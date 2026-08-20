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
package com.nosfabrica.vespa.relay.progress

import java.util.concurrent.ConcurrentHashMap

/**
 * WHEN EACH RELAY WAS LAST SYNCED, AND WHAT HAPPENED THE LAST TIME IT WAS TRIED.
 *
 * ## The question nothing in this process could answer
 *
 * Everything the mirror published about a relay was either a LIVE position or
 * an AGGREGATE. `inFlight` names the relays a worker is on RIGHT NOW and drops
 * a row the instant the visit ends; the pool's counters say how many visits ran
 * across the whole roster; the coverage report says how far back a band reaches
 * but not when anything last touched it — a band whose walk finished in March
 * and one that finished a minute ago publish the identical row. So "when was
 * wss://relay.example last synced, and if it is not being synced, why" had no
 * answer here at all. It had one in the log, for as long as the container kept
 * it, spread across a `visit … failed` line, an `audit …` line and silence for
 * the relays that worked.
 *
 * This is the missing half: one row per relay, holding what the LAST visit did
 * and when anything last arrived. It is the only per-relay state in this
 * process that outlives the visit it describes.
 *
 * ## What a row can and cannot claim
 *
 * **`syncedAt` is the last visit that ENDED CLEANLY — it is not a completeness
 * claim.** A clean visit walked every outstanding leg of every ask the roster
 * had for that relay and left a tail; whether the relay's history is fully
 * covered is the band's question ([SyncCoverageReport]'s `settled`), and the
 * two are deliberately different members on two different cards. A relay can be
 * synced every five minutes and still be `paged` forever, and a relay settled
 * to its floor can have failed every visit since.
 *
 * **A clean visit that carried nothing is still a clean visit.** Most are:
 * a tailed relay's catch-up normally finds nothing, because the tail already
 * delivered it. That is why `events` sits beside `syncedAt` rather than
 * deciding it — "synced" and "delivered" are separate readings, and folding
 * them would report every quiet relay as broken.
 *
 * **`lastEventAt` counts arrivals from any path** — a catch-up page, an audit's
 * diff, or the live tail — because for a tailed relay the tail IS the sync, and
 * a freshness reading taken only at visit boundaries would call a relay
 * streaming events right now half an hour stale.
 *
 * ## Why a relay is not being synced, in the row itself
 *
 * Four endings, and each is a different fault with a different fix — see
 * [Ending]. Beside them sits [Row.onRoster], which is the answer for a relay
 * that is not being VISITED at all: the roster is the monitor's certified set,
 * so a url that leaves it stops being dialled while its row stays here saying
 * so. That distinction is the one an operator gets wrong first — "it stopped
 * syncing" reads as a broken relay, and it is as often this router declining to
 * dial one.
 *
 * ## Bounded, and it never drops a relay it is still being asked about
 *
 * Two different bounds, because the two halves have different failure modes.
 * What this HOLDS is trimmed to [cap] by dropping OFF-ROSTER rows only,
 * least-recently-touched first: those are a fact with a shelf life, and the
 * roster is the ledger's whole subject. A roster larger than the cap therefore
 * grows past it rather than forgetting relays it is being asked about — the
 * rows are a few hundred bytes each, and a ledger that silently forgets the
 * relay someone is looking up is worth more heap than it saves.
 *
 * What it PUBLISHES is capped separately and says so: the list is ordered
 * worst-first, so the cut falls on the healthiest rows and never on the ones
 * being looked for, and what was left out is published as `omitted` on the same
 * terms as every other bounded list in this document.
 */
class VisitLedger(
    /**
     * How many rows this keeps, and the default size of the list it publishes.
     *
     * Four thousand: comfortably above the largest roster measured here (a
     * fan-out reaching ~3,300 urls on 850 hosts), so in practice the only rows
     * it ever drops are for relays the roster no longer wants.
     */
    private val cap: Int = DEFAULT_CAP,
) {
    /**
     * HOW A VISIT ENDED — the four exits [VisitPool.visit] has, and they are
     * different faults rather than degrees of one.
     *
     * The words are what the document publishes and the page draws, so they are
     * fixed here rather than spelled at each call site.
     */
    enum class Ending(
        val word: String,
        /** Said on the row when the ending itself is the whole story. */
        val why: String?,
    ) {
        /** Every ask walked, the heal queue drained, the tail (re)opened. */
        SYNCED("synced", null),

        /**
         * The relay answered a leg with nothing and an ending that says it will
         * not answer the next one either — a closed subscription, an auth
         * demand, a connect that never landed. The visit stops at the first
         * one: the next ask is the same conversation with the same relay.
         */
        REFUSED(
            "refused",
            "The relay ended a walk with nothing delivered and no more to give — closed, auth-gated, unpageable or " +
                "unreachable. The visit stopped there rather than re-opening the same conversation per remaining ask; " +
                "the monitor's next sweep is what decides whether it stays certified.",
        ),

        /**
         * Nothing arrived for long enough that the visit gave up on the
         * remaining asks. Not a refusal — the relay may be answering, slowly —
         * so the revisit simply takes what was left.
         */
        QUIET(
            "quiet",
            "Nothing arrived for long enough that the visit gave up its remaining asks. The relay never refused, so " +
                "this is a slow or wedged conversation rather than a closed one; the revisit picks up what was left.",
        ),

        /** The visit threw. The exception is the whole message, so it is carried per row. */
        FAILED("failed", null),
    }

    /**
     * One relay's row, flattened for a reader outside this process.
     *
     * A snapshot, not a view: [SyncProgress] serialises these on a timer while
     * the pool keeps moving, and handing out the live entry would publish a row
     * whose members were read at different instants.
     */
    class Row(
        val relay: String,
        /** [Ending.word], or `never` for a relay no visit has finished yet. */
        val outcome: String,
        /** Why, in the router's own words — absent when the outcome needs none. */
        val detail: String?,
        /** The last visit that ended cleanly, in epoch seconds. Null before the first. */
        val syncedAt: Long?,
        /** When the last visit STARTED, cleanly or not. Null before the first. */
        val lastVisitAt: Long?,
        /** When anything last arrived from this relay — page, audit diff or tail. */
        val lastEventAt: Long?,
        /** What the last visit received. */
        val events: Long,
        /** Visits that have not ended cleanly since the last one that did. */
        val failures: Int,
        /** Whether the roster still wants this relay at all. */
        val onRoster: Boolean,
        /** Whether a live tail is carrying its present. */
        val tailed: Boolean,
        /** The armed revisit, when one is armed — absent while the relay is queued or being visited. */
        val nextVisitInSec: Long?,
        /** How long a worker has held it, when a visit is running right now. */
        val heldForSec: Long?,
        /** The streams asking for it, as the last roster rebuild had them. */
        val streams: List<String>,
    )

    /** Every row this ledger publishes, and how many the publish cap left out. */
    class Snapshot(
        val relays: List<Row>,
        val omitted: Int,
    )

    /**
     * The live entry, mutated in place. Volatile rather than locked: the writers
     * are one visit worker per url plus the roster rebuild, the readers are the
     * status tick, and every member is a word or a long.
     */
    private class Entry(
        val relay: String,
    ) {
        @Volatile var outcome: Ending? = null

        @Volatile var detail: String? = null

        @Volatile var syncedAtMs: Long = 0

        @Volatile var lastVisitAtMs: Long = 0

        @Volatile var lastEventAtMs: Long = 0

        @Volatile var events: Long = 0

        @Volatile var failures: Int = 0

        @Volatile var onRoster: Boolean = true

        @Volatile var streams: List<String> = emptyList()

        /**
         * The eviction clock: any write touches it, so the rows that go first
         * are the ones nothing has said anything about for longest.
         */
        @Volatile var touchedMs: Long = System.currentTimeMillis()
    }

    private val entries = ConcurrentHashMap<String, Entry>()

    /**
     * `computeIfAbsent`, not `getOrPut`: the latter is the plain map extension
     * — a get, then a put — and two visit workers racing it on one url would
     * each build an entry and one would overwrite the other's, losing whatever
     * the loser had already recorded.
     */
    private fun entry(relay: String): Entry = entries.computeIfAbsent(relay) { Entry(relay) }

    /**
     * WHAT THE ROSTER WANTS, as of this rebuild.
     *
     * Called with the whole roster rather than per url so the relays that have
     * LEFT it are marked in the same pass — a row that silently kept
     * `onRoster: true` after decertification would report a relay as waiting
     * for a visit that is never coming.
     */
    fun roster(streamsByRelay: Map<String, List<String>>) {
        val nowMs = System.currentTimeMillis()
        for ((relay, streams) in streamsByRelay) {
            val e = entry(relay)
            e.onRoster = true
            e.streams = streams
            e.touchedMs = nowMs
        }
        for ((relay, e) in entries) {
            if (relay !in streamsByRelay) {
                // The streams stay on the row: "it was in `content`'s roster
                // until the monitor stopped certifying it" is most of what
                // makes a dropped relay findable at all.
                e.onRoster = false
            }
        }
        trim()
    }

    /**
     * An event arrived from [relay], by whatever path.
     *
     * Called per event, so it does exactly one map lookup and one volatile
     * write — the same order of cost as the yield fold the pool already runs
     * beside it. No trim here: a row that is receiving is a row that exists.
     */
    fun received(
        relay: String,
        nowMs: Long = System.currentTimeMillis(),
    ) {
        // ONE volatile write, and deliberately not two: the eviction clock is
        // not touched here. Only OFF-ROSTER rows are ever evicted and nothing
        // off the roster has a tail or a visit to deliver through, so an
        // arrival can never be what saves a row from the trim — and this is
        // the one method on this class that runs per event.
        entry(relay).lastEventAtMs = nowMs
    }

    /**
     * One visit, recorded as it ends.
     *
     * [startedMs] is the visit's own start rather than the moment this is
     * called, so `lastVisitAt` and the in-flight row's `heldForSec` describe
     * the same instant, and a five-hour visit does not read as having begun
     * when it finally gave up.
     */
    fun visited(
        relay: String,
        startedMs: Long,
        ending: Ending,
        events: Long,
        detail: String? = null,
        nowMs: Long = System.currentTimeMillis(),
    ) {
        val e = entry(relay)
        e.lastVisitAtMs = startedMs
        e.outcome = ending
        e.detail = detail ?: ending.why
        e.events = events
        if (ending == Ending.SYNCED) {
            e.syncedAtMs = nowMs
            e.failures = 0
        } else {
            // Saturating rather than wrapping, and counted per row: this is the
            // number that separates a relay that failed once from one that has
            // failed every visit for a week, which is the difference between a
            // blip and a relay to stop dialling.
            e.failures = (e.failures + 1).coerceAtMost(Int.MAX_VALUE)
        }
        e.touchedMs = nowMs
        trim()
    }

    /**
     * The rows, worst first, with the live half of each supplied by the pool.
     *
     * The three lookups are passed in rather than stored because they are the
     * pool's own live state — a tail can open and a timer can be re-armed
     * between two ticks — and reading them at snapshot time is what keeps the
     * row internally consistent. Defaults answer "nothing known", which is what
     * a test or a probe with no pool behind it can honestly say.
     *
     * ORDERED WORST FIRST: never-synced before long-ago-synced before
     * just-synced, and on-roster before off, so the cap falls on the rows
     * nobody is looking for. The page re-sorts for its own views; this order is
     * the one that decides what survives.
     */
    fun snapshot(
        nowMs: Long = System.currentTimeMillis(),
        tailed: (String) -> Boolean = { false },
        nextVisitInSec: (String) -> Long? = { null },
        heldForSec: (String) -> Long? = { null },
        limit: Int = cap,
    ): Snapshot {
        val ordered =
            entries.values
                .sortedWith(
                    compareByDescending<Entry> { it.onRoster }
                        .thenBy { it.syncedAtMs }
                        .thenBy { it.relay },
                )
        val rows =
            ordered.take(limit).map { e ->
                Row(
                    relay = e.relay,
                    outcome = e.outcome?.word ?: NEVER,
                    detail = e.detail,
                    syncedAt = e.syncedAtMs.takeIf { it > 0 }?.div(1000),
                    lastVisitAt = e.lastVisitAtMs.takeIf { it > 0 }?.div(1000),
                    lastEventAt = e.lastEventAtMs.takeIf { it > 0 }?.div(1000),
                    events = e.events,
                    failures = e.failures,
                    onRoster = e.onRoster,
                    tailed = tailed(e.relay),
                    // A relay being visited RIGHT NOW has no armed timer, and
                    // one waiting in the queue has none either — the timer is
                    // armed when the visit finishes. Both publish nothing here
                    // rather than a zero, which would read as "due now".
                    nextVisitInSec = nextVisitInSec(e.relay),
                    heldForSec = heldForSec(e.relay),
                    streams = e.streams,
                )
            }
        return Snapshot(relays = rows, omitted = ordered.size - rows.size)
    }

    /**
     * Back to [cap] by dropping OFF-ROSTER rows only, least-recently-touched
     * first — see the class header for why an over-cap roster is kept whole
     * instead.
     */
    private fun trim() {
        if (entries.size <= cap) return
        val droppable = entries.values.filter { !it.onRoster }
        if (droppable.isEmpty()) return
        for (e in droppable.sortedBy { it.touchedMs }.take(minOf(droppable.size, entries.size - cap))) {
            entries.remove(e.relay, e)
        }
    }

    companion object {
        /** What a row says before its first visit has finished — see [Row.outcome]. */
        const val NEVER = "never"

        /** See the [cap] parameter for the sizing. */
        const val DEFAULT_CAP = 4_000
    }
}
