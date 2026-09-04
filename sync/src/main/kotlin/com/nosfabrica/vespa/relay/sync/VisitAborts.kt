/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Why visits end early: one counter per [Reason], summing to `abortedVisits`,
 * and one log line per abort naming the relay, the stream, the ask, the
 * reason and what the relay said. Rows are published even at zero, so an
 * absent row cannot be read as a forgotten counter.
 *
 * A line is spoken once per (stream, relay, reason) and re-said no more than
 * every [resayAfterMs]. The dedup map stops growing at [MAX_SPOKEN], past
 * which aborts are still counted but no longer narrated.
 */
internal class VisitAborts(
    /** How long before the same (stream, relay, reason) is worth a line again. */
    private val resayAfterMs: Long = DEFAULT_RESAY_AFTER_MS,
    /** A parameter so a test can age an abort without waiting. */
    private val now: () -> Long = System::currentTimeMillis,
) {
    /**
     * The ways a visit ends early, with the counter each is published under
     * and the sentence it is said in. Five are quartz's own walk endings
     * ([VisitPool.refusedOutright] decides which abort at all); three are the
     * pool's. The counter names are a wire contract the glossary defines.
     */
    enum class Reason(
        val count: String,
        val says: String,
        /** Describes this mirror, not the relay: counted and spoken, never written on the relay's row. */
        val ours: Boolean = false,
    ) {
        /** The relay refused with `auth-required:` and would not accept the identity our signer answered with. */
        AUTH_REQUIRED("abortedAuthRequired", "the relay would not accept our NIP-42 identity"),

        /** The relay ended the subscription for a reason of its own; the sentence beside the line tells which. */
        CLOSED("abortedClosed", "the relay closed the subscription"),

        /** The relay went quiet inside a page and never ended it. */
        QUIET("abortedQuiet", "the relay went quiet inside the page"),

        /** The dial never landed, so nothing was ever asked. */
        UNREACHABLE("abortedUnreachable", "the dial never landed"),

        /** The relay answered but ignored the paging cursor, so the walk cannot advance. */
        UNPAGEABLE("abortedUnpageable", "the relay ignored the paging cursor"),

        /** The sequence of asks went quiet past `LEG_QUIET_GIVE_UP_MS`; the remaining asks wait for the revisit. */
        GAVE_UP("abortedGaveUp", "the visit went quiet for too long — the revisit takes the remaining asks"),

        /** The visit threw. The class and message are on the line. */
        FAILED("abortedFailed", "the visit failed"),

        /**
         * A hook of ours was parked in the full ingest queue when the walk gave
         * up, so the ending quartz reported was manufactured on our side of the
         * socket. Read beside the ingest row's `queued` against `capacity`.
         */
        BACKPRESSURED("abortedBackpressured", "our own ingest queue held the socket — nothing the relay did", ours = true),
    }

    private val counters = Reason.entries.associateWith { AtomicLong() }
    private val totalAborts = AtomicLong()

    /**
     * The last abort per (stream, relay), for the status table's per-row
     * question, asked hours after the line scrolled away. Bounded by
     * [MAX_SPOKEN] like the narration map beside it.
     */
    private val lastByUnit = ConcurrentHashMap<String, Last>()

    /** The last time this unit ended early, and what the relay said about it. */
    class Last(
        val reason: Reason,
        val said: String?,
        /** Epoch seconds, because everything it is published beside is. */
        val atSec: Long,
    )

    /** The last abort for one unit, or null for a unit that has never aborted. */
    fun last(
        stream: String,
        url: NormalizedRelayUrl,
    ): Last? = lastByUnit[unitKey(stream, url)]

    /**
     * Forgets the unit's last abort when a visit comes back clean. The row is
     * about where a pair stands now; the counters are the lifetime record.
     */
    fun cleared(
        stream: String,
        url: NormalizedRelayUrl,
    ) {
        lastByUnit.remove(unitKey(stream, url))
    }

    /** The last time each (stream, relay, reason) was spoken; see [MAX_SPOKEN]. */
    private val spokenAt = ConcurrentHashMap<String, Long>()

    /** Every abort since boot, which the reason rows partition. */
    val total: Long get() = totalAborts.get()

    /**
     * Counts one abort and returns the line to print, or null when the line
     * is rationed. The count always happens. A sentence rather than a boolean,
     * so every abort line in this router reads the same way.
     */
    fun record(
        stream: String,
        url: NormalizedRelayUrl,
        reason: Reason,
        asked: String,
        said: String?,
        /** What the socket carried during the refused ask, from [RelayPages]; null when nothing was sampled. */
        sent: String? = null,
    ): String? {
        counters[reason]?.incrementAndGet()
        totalAborts.incrementAndGet()
        val at = now()
        // Recorded before the narration gate: the gate rations lines, and the status row
        // must not go blank because this abort fell inside a re-say window.
        val key = unitKey(stream, url)
        // A stall of ours neither writes the relay's row nor clears it.
        if (!reason.ours && (lastByUnit.containsKey(key) || lastByUnit.size < MAX_SPOKEN)) {
            lastByUnit[key] = Last(reason, said, at / 1000)
        }
        if (!worthSaying(key, reason, at)) return null
        return "router: visit $stream ${url.url} aborted — ${reason.says} [$asked]" +
            (said?.let { " — the relay said: $it" } ?: "") +
            (sent?.let { " — $it" } ?: "")
    }

    private fun worthSaying(
        unit: String,
        reason: Reason,
        at: Long,
    ): Boolean {
        val key = "$unit ${reason.name}"
        val last = spokenAt[key]
        if (last != null && at - last < resayAfterMs) return false
        // Past the bound nothing new is narrated, but a pair already in the map keeps its re-say.
        if (last == null && spokenAt.size >= MAX_SPOKEN) return false
        spokenAt[key] = at
        return true
    }

    /** The rows the visits processor publishes: the total, then its partition in the enum's order. */
    fun counts(): List<Processors.Count> =
        buildList {
            add(Processors.Count("abortedVisits", totalAborts.get()))
            for (reason in Reason.entries) add(Processors.Count(reason.count, counters[reason]?.get() ?: 0L))
        }

    private fun unitKey(
        stream: String,
        url: NormalizedRelayUrl,
    ) = "$stream ${url.url}"

    companion object {
        /**
         * Which reason a quartz walk ending is. Exhaustive rather than
         * defaulting, so an ending quartz adds later must be decided.
         */
        fun of(end: PagedFetchResult.End): Reason =
            when (end) {
                PagedFetchResult.End.AUTH_REQUIRED -> Reason.AUTH_REQUIRED

                PagedFetchResult.End.CLOSED -> Reason.CLOSED

                PagedFetchResult.End.IDLE -> Reason.QUIET

                PagedFetchResult.End.CANNOT_CONNECT -> Reason.UNREACHABLE

                PagedFetchResult.End.UNPAGEABLE -> Reason.UNPAGEABLE

                // Not refusals; see [VisitPool.refusedOutright]. Named so the `when` stays exhaustive.
                PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED -> Reason.CLOSED
            }

        /**
         * The refused ask, short enough for a log line. Kinds are counted past
         * [KINDS_LISTED] rather than listed: the width is the fact that matters.
         */
        fun asked(filter: Filter): String =
            buildList {
                filter.kinds?.takeIf { it.isNotEmpty() }?.let {
                    add(if (it.size > KINDS_LISTED) "${it.size} kinds" else "kinds ${it.joinToString(",")}")
                }
                filter.authors
                    ?.size
                    ?.takeIf { it > 0 }
                    ?.let { add("$it author(s)") }
                filter.since?.let { add("since $it") }
                filter.until?.let { add("until $it") }
            }.joinToString(", ").ifEmpty { "everything" }

        /** Above this many, the width is printed instead of the list; see [asked]. */
        const val KINDS_LISTED = 8

        /** Half an hour: a handful of lines a minute on a roster of hundreds, readable across a shift. */
        const val DEFAULT_RESAY_AFTER_MS = 30 * 60 * 1000L

        /** How many distinct (stream, relay, reason) triples are ever narrated. */
        const val MAX_SPOKEN = 20_000
    }
}
