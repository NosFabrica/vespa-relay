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
     * other two are this pool's — the sequence-level quiet give-up, and an
     * exception escaping the visit.
     *
     * The counter NAMES are a wire contract like the pool words: the status
     * document publishes them and the glossary defines each one, so renaming a
     * constant here without the glossary leaves a number a reader meets
     * undefined.
     */
    enum class Reason(
        val count: String,
        val says: String,
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
    }

    private val counters = Reason.entries.associateWith { AtomicLong() }
    private val totalAborts = AtomicLong()

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
    ): String? {
        counters[reason]?.incrementAndGet()
        totalAborts.incrementAndGet()
        if (!worthSaying(stream, url, reason)) return null
        return "router: visit $stream ${url.url} aborted — ${reason.says} [$asked]" +
            (said?.let { " — the relay said: $it" } ?: "")
    }

    private fun worthSaying(
        stream: String,
        url: NormalizedRelayUrl,
        reason: Reason,
    ): Boolean {
        val key = "$stream ${url.url} ${reason.name}"
        val at = now()
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
