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
 * WHY VISITS ARE ENDING EARLY — the instrument `abortedVisits` needed and did
 * not have.
 *
 * ## The question this exists to answer
 *
 * `abortedVisits` is a single number, and on `vespa-eventstore-staging` it read
 * 92.5% of visits over one incremental minute. A visit that aborts leaves its
 * relay unreconciled, so a relay that refuses any single ask never completes
 * however often it is visited — which means that number was the whole
 * explanation for a resync that had stopped converging, and it could not be
 * acted on, because nothing said which relays or why.
 *
 * Worse, almost none of it was even visible. Over a 20-minute window the visit
 * side emitted TWO log lines against roughly 4,400 aborts: the `!clean` path
 * returned without naming the relay, the stream, the ask or the reason, and the
 * ~406 refusals an operator could see came from the relay client's own
 * connection logging — a tenfold undercount, and attributable to no visit.
 *
 * This is the same shape as the `oldestBatchSec` complaint that produced
 * [com.nosfabrica.vespa.relay.progress.StoreCalls]: a number that reports a
 * problem exists with no instrument that can name it. That fixed the store
 * side; this is the visit side's.
 *
 * ## What it publishes, and why it is a partition
 *
 * One counter per [Reason], summing to `abortedVisits`. The partition is the
 * point: "92.5% aborted" and "92.5% aborted, all of it `abortedAuthRequired`"
 * are the same measurement and different work, and only the second can be
 * costed. The rows are published even at zero — a reason that has never fired
 * is a fact about this deployment, and an absent row reads as a counter that
 * was forgotten.
 *
 * ## …and what it SAYS
 *
 * One line per abort, naming the relay, the stream, the ask and the reason —
 * plus, where the relay explained itself, the sentence it used
 * ([RelayComplaints]). Spoken once per (stream, relay, reason) and re-said no
 * more than every [resayAfterMs], for the reason `StoreCalls` re-warns rather
 * than warning once: a wedge is watched over hours and the log is the only
 * thing awake for it, but one line per abort would be four a second on this
 * deployment and would bury everything beside it.
 *
 * The dedup map is bounded at [MAX_SPOKEN] and stops growing there — past it
 * aborts are still COUNTED, they are just no longer narrated. A roster of
 * thousands times a handful of streams times seven reasons is the real ceiling
 * and sits under it; the bound exists so that a pathological roster costs a
 * quiet log rather than a heap.
 */
internal class VisitAborts(
    /** How long before the same (stream, relay, reason) is worth a line again. */
    private val resayAfterMs: Long = DEFAULT_RESAY_AFTER_MS,
    /**
     * Where the clock comes from — a parameter only so a test can age an abort
     * without waiting half an hour, the same seam [com.nosfabrica.vespa.relay.progress.StoreCalls] takes for
     * the same reason.
     */
    private val now: () -> Long = System::currentTimeMillis,
) {
    /**
     * The ways a visit ends early, each with the counter it is published under
     * and the sentence it is said in.
     *
     * Five of them are quartz's own walk endings, believed and passed through
     * ([VisitPool.refusedOutright] decides which endings abort at all); the
     * other three are this pool's — the sequence-level quiet give-up, an
     * exception escaping the visit, and a walk our own ingest queue stalled,
     * which quartz can only report as one of the first five.
     *
     * The counter NAMES are a wire contract like the pool words: the status
     * document publishes them and the glossary defines each one, so renaming a
     * constant here without the glossary leaves a number a reader meets
     * undefined.
     */
    enum class Reason(
        val count: String,
        val says: String,
        /**
         * The abort is OURS — it describes this mirror's state, not the
         * relay's — so it is counted and spoken but never written on the
         * relay's row. See [BACKPRESSURED], the only one so far.
         */
        val ours: Boolean = false,
    ) {
        /**
         * The relay refused with `auth-required:` and the NIP-42 exchange did
         * not satisfy it. This router DOES answer challenges — the signer is
         * attached whenever `RELAY_NSEC` is set — so reaching here means the
         * relay wants an identity of ours it will not accept, not that nobody
         * replied. Fifty relays on staging.
         */
        AUTH_REQUIRED("abortedAuthRequired", "the relay would not accept our NIP-42 identity"),

        /**
         * The relay ended the subscription for a reason of its own: a policy
         * refusal, a rate limit, or a filter it will not serve. The sentence
         * beside the line is the only thing that tells those apart, which is
         * why [RelayComplaints] exists.
         */
        CLOSED("abortedClosed", "the relay closed the subscription"),

        /** The relay went quiet inside a page and never ended it — silence, which is not an answer. */
        QUIET("abortedQuiet", "the relay went quiet inside the page"),

        /** The dial never landed, so nothing was ever asked. */
        UNREACHABLE("abortedUnreachable", "the dial never landed"),

        /**
         * The relay answered but ignored the paging cursor, so the walk cannot
         * advance and proves nothing about what the relay holds.
         */
        UNPAGEABLE("abortedUnpageable", "the relay ignored the paging cursor"),

        /**
         * The SEQUENCE of asks went quiet past `LEG_QUIET_GIVE_UP_MS`, so the
         * remaining asks were left to the revisit. Not one ask's refusal —
         * a relay answering hundreds of bound authors with a full empty idle
         * window apiece, which was measured at five hours of one worker.
         */
        GAVE_UP("abortedGaveUp", "the visit went quiet for too long — the revisit takes the remaining asks"),

        /** The visit threw. The class and message are on the line. */
        FAILED("abortedFailed", "the visit failed"),

        /**
         * A hook of OURS was suspended in the full ingest queue when the walk
         * gave up, so the ending quartz reported — silence, or a page received
         * and not delivered — was manufactured on our side of the socket. See
         * `VisitPool.holding` for the mechanism. Before this reason existed,
         * every one of these was `abortedQuiet` or `abortedUnpageable`, with a
         * sentence blaming the relay's cursor; on staging that was 90% of all
         * aborts, on relays the monitor had correctly graded `prime`.
         *
         * Nothing to fix at the relay, so nothing is written on its row. The
         * number to read beside this one is the ingest processor's `queued`
         * against its `capacity`.
         */
        BACKPRESSURED("abortedBackpressured", "our own ingest queue held the socket — nothing the relay did", ours = true),
    }

    private val counters = Reason.entries.associateWith { AtomicLong() }
    private val totalAborts = AtomicLong()

    /**
     * THE LAST ABORT PER UNIT, which the counters cannot hold and the log line
     * scrolls away.
     *
     * A count answers "how much of this is happening"; a status table answers
     * "what is wrong with THIS relay", and that question is asked of one row at
     * a time, hours after the line was printed. `RelayStatusReport` reads this
     * to turn a unit with no band into the two different findings it can be —
     * *never visited* and *visited, and the relay would not have it* — which
     * are the same absence in the band file.
     *
     * One entry per (stream, relay), overwritten, and bounded by [MAX_SPOKEN]
     * like the narration map beside it: the roster is the real ceiling and sits
     * far under it.
     */
    private val lastByUnit = ConcurrentHashMap<String, Last>()

    /** The last time this unit ended early, and what the relay said about it. */
    class Last(
        val reason: Reason,
        val said: String?,
        /** Epoch SECONDS, because everything it is published beside is. */
        val atSec: Long,
    )

    /** …read back for one unit. Null for a unit that has never aborted. */
    fun last(
        stream: String,
        url: NormalizedRelayUrl,
    ): Last? = lastByUnit[unitKey(stream, url)]

    /**
     * …and FORGOTTEN, when this unit's visit comes back clean.
     *
     * The row is about where a pair stands NOW, and without this it never
     * stopped being about where it once stood: a pair that met a transient
     * `CANNOT_CONNECT` at boot and has written no band since — a relay that is
     * simply empty for this filter is the ordinary case — read `refused`,
     * `fault: true` and carried a stale sentence at the top of a worst-first
     * table for the life of the process. The COUNTERS are the lifetime record
     * and are untouched; this is the live one.
     */
    fun cleared(
        stream: String,
        url: NormalizedRelayUrl,
    ) {
        lastByUnit.remove(unitKey(stream, url))
    }

    /** The last time each (stream, relay, reason) was spoken — see [MAX_SPOKEN]. */
    private val spokenAt = ConcurrentHashMap<String, Long>()

    /** Every abort since boot, which is what the reason rows partition. */
    val total: Long get() = totalAborts.get()

    /**
     * Count one abort, and answer whether it is worth a line.
     *
     * The count always happens; only the narration is rationed. Returns the
     * SENTENCE to print rather than a boolean so the caller cannot compose a
     * different one — every abort line in this router reads the same way, which
     * is what makes them greppable.
     */
    fun record(
        stream: String,
        url: NormalizedRelayUrl,
        reason: Reason,
        asked: String,
        said: String?,
        /**
         * WHAT THE SOCKET ACTUALLY CARRIED during the refused ask — see
         * [RelayPages], and #187 for why no other instrument in this process
         * can see it.
         *
         * The third of the three things an abort needs to be actionable, and
         * the one that was missing: [asked] is what we sent, [said] is what the
         * relay answered in words, and this is what it answered in EVENTS. A
         * walk aborts on `downloaded == 0`, which means every event it received
         * failed to match — so the page is the only place the reason lives, and
         * it was being discarded by the match that produced the abort.
         *
         * Null when nothing was sampled: another walk held the sampler, the
         * socket carried nothing, or this pool has none. Absent rather than
         * "0 events", which would read as a finding.
         */
        sent: String? = null,
    ): String? {
        counters[reason]?.incrementAndGet()
        totalAborts.incrementAndGet()
        val at = now()
        // Recorded BEFORE the narration gate, and that ordering is the point:
        // the gate rations LINES, and a status row that went blank because this
        // abort was inside a re-say window would report a relay as never
        // visited while the log said otherwise.
        val key = unitKey(stream, url)
        // OUR OWN STALLS NEVER REACH THE ROW. The row answers "what is wrong
        // with THIS relay", and a stall of ours is not an answer to that: it
        // would mark a healthy relay `refused` and at fault for as long as the
        // queue stays full, which on staging was every relay on the roster for
        // the life of the process. Neither is the previous entry cleared —
        // whatever the relay last said for itself still stands.
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
        // Past the bound nothing NEW is narrated, but a pair already in the map
        // keeps its re-say: the relays that abort first are the ones an
        // operator is already reading about, and evicting them to make room for
        // the tail of a huge roster would trade a readable log for a complete
        // one nobody can read.
        if (last == null && spokenAt.size >= MAX_SPOKEN) return false
        spokenAt[key] = at
        return true
    }

    /**
     * The rows the visits processor publishes: the total, then the partition of
     * it, in the enum's own order.
     */
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
         * Which reason a quartz walk ending is — for the endings that abort a
         * visit at all. `DRAINED` and `LIMIT_REACHED` are not refusals and
         * never reach here, which is why this is exhaustive over the enum
         * rather than defaulting: an ending quartz adds later must be decided,
         * not silently filed under [Reason.CLOSED].
         */
        fun of(end: PagedFetchResult.End): Reason =
            when (end) {
                PagedFetchResult.End.AUTH_REQUIRED -> Reason.AUTH_REQUIRED

                PagedFetchResult.End.CLOSED -> Reason.CLOSED

                PagedFetchResult.End.IDLE -> Reason.QUIET

                PagedFetchResult.End.CANNOT_CONNECT -> Reason.UNREACHABLE

                PagedFetchResult.End.UNPAGEABLE -> Reason.UNPAGEABLE

                // Not refusals — see [VisitPool.refusedOutright]. Named so the
                // `when` stays exhaustive and a new ending is a compile error.
                PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED -> Reason.CLOSED
            }

        /**
         * WHAT THE REFUSED ASK WAS, short enough for a log line.
         *
         * The kinds are COUNTED past [KINDS_LISTED] rather than listed, which
         * is not a nicety here: the ask this router aborts on most carries 139
         * of them, and a line that spelled them out would be 700 characters of
         * numbers with the relay's own explanation at the far end of it. The
         * WIDTH is the fact that matters for that ask anyway — it is why the
         * relay refused.
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

        /** Above this many, the width is printed instead of the list — see [asked]. */
        const val KINDS_LISTED = 8

        /**
         * How long before the same (stream, relay, reason) earns another line.
         *
         * Half an hour, which on a roster of hundreds is a handful of lines a
         * minute at worst and a timeline an operator can read across a shift.
         * The counters are the continuous record; these lines are the index
         * into them.
         */
        const val DEFAULT_RESAY_AFTER_MS = 30 * 60 * 1000L

        /** How many distinct (stream, relay, reason) triples are ever narrated — see the class header. */
        const val MAX_SPOKEN = 20_000
    }
}
