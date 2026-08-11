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
package com.nosfabrica.vespa.relay.router.progress

import com.nosfabrica.vespa.relay.util.fmtCount
import com.nosfabrica.vespa.relay.util.fmtDay
import com.nosfabrica.vespa.relay.util.fmtDuration
import java.util.concurrent.ConcurrentHashMap

/**
 * What each stream is doing right now, so an operator can tell a stream that
 * is working from one that never started.
 *
 * Two rules, both violated by the completion-only logging this replaced:
 *
 *  - **A phase in progress reports elapsed time.** The two longest phases —
 *    walking the local id set, discovering relays — produce no completion for
 *    minutes and used to print nothing at all.
 *  - **A configured stream is never absent.** It appears on every tick even
 *    when idle, so silence can never be read as "not configured".
 *
 * A third rule arrived later, from a 45-minute ingest drain that produced no
 * output at all:
 *
 *  - **Every phase reports elapsed time, including the ones that are not
 *    "progress".** `Idle` and `Failed` used not to, so a stream that went idle
 *    forty-five minutes ago and one that finished a second ago printed the
 *    identical line. Silence and stillness are different states and only the
 *    clock tells them apart.
 *
 * It also holds each stream's [CycleTally], because the phase and the
 * disposition are two halves of one answer — what a stream is doing, and what
 * became of everything it took on. [snapshot] hands both to [SyncProgress],
 * which is what publishes them off this process.
 */
class StreamPhases {
    sealed interface Phase {
        /** Registered, not yet started — the only honest thing to say before the first phase. */
        data object Starting : Phase

        /** Configured, nothing to do yet — its sources have named no relays. */
        data class Waiting(
            val sources: String,
            val retrySec: Long,
        ) : Phase

        /** Ready to reconcile, waiting for another stream to release the gate. */
        data class Queued(
            val relays: Int,
        ) : Phase

        /** Reading relay urls out of the store. */
        data class Discovering(
            val sources: String,
        ) : Phase

        /**
         * Building the local id set the reconcile compares against.
         * [total] is null when it could not be counted — an unknown
         * denominator is better than a wrong one.
         */
        data class Snapshotting(
            val collected: Int,
            val total: Int?,
            val relays: Int,
        ) : Phase

        /** Paging relays that are not reconciling — no id set involved. */
        data class Fetching(
            val done: Int,
            val total: Int,
            val events: Long,
            /** Time-axis progress of the relays still walking — see [PagingProgress]. */
            val fraction: Double? = null,
            val etaMs: Long? = null,
            /**
             * Oldest `created_at` the walk has reached. The percentage alone
             * cannot show a deep walk moving — days of progress on an
             * unbounded walk round to 0% — but the date moves every page.
             */
            val reachedSeconds: Long? = null,
            /**
             * Relays with a WORKER on them right now, across every pass —
             * probing, queued for a transfer slot, or transferring.
             *
             * Beside [done], never instead of it, because the two stopped
             * agreeing when the fan-out became a rotation: [done] is how far
             * the current WALK got over its list, and this is how much of the
             * admission gate is committed — including legs handed out by passes
             * that ended long ago. A walk that has finished handing out while
             * ten relays are still transferring is neither "done" nor "idle".
             */
            val running: Int = 0,
            /**
             * …and of those, how many are actually on a socket.
             *
             * Published separately because the gap is large and reporting the
             * wider number alone overstated the work: a stream with 8 transfer
             * slots routinely shows 128 workers, of which 120 are waiting on a
             * connect to a host that will never answer. One number for both
             * read as "128 relays syncing" on a stream that cannot sync more
             * than 8.
             */
            val transferring: Int = 0,
        ) : Phase

        /** Fanning out. */
        data class Syncing(
            val done: Int,
            val total: Int,
            val events: Long,
            val skipped: Long,
            val unreachable: Long,
            /** Relays with a worker right now, across every pass — see [Fetching.running]. */
            val running: Int = 0,
            /** …and of those, how many are on a socket — see [Fetching.transferring]. */
            val transferring: Int = 0,
        ) : Phase

        /** Cycle finished; nothing more until the next refresh. */
        data class Idle(
            val events: Long,
            /**
             * Seconds until the next cycle, or null when there is no next cycle.
             *
             * A dynamic stream re-runs on a timer and can say when. A STATIC one
             * cannot: it backfills once and then live-tails, so there is nothing
             * to count down to. That case used to pass 0, which rendered as
             * "next in 0s" — indistinguishable from a stream about to re-run
             * immediately, and it was read as a busy loop more than once.
             */
            val nextInSec: Long?,
            /**
             * Relays with a worker STILL RUNNING from the pass that just ended.
             *
             * The gap between passes is not quiet on a rotation: the walk ends
             * when the last url is handed out, and the slowest legs run on
             * through the wait. Reported here because "idle" otherwise claims
             * the opposite of what is happening, and a hung leg would be
             * invisible for exactly as long as it lasted.
             */
            val running: Int = 0,
            /** …and of those, how many are on a socket — see [Fetching.transferring]. */
            val transferring: Int = 0,
        ) : Phase

        /** The last attempt threw. */
        data class Failed(
            val reason: String,
            val retrySec: Long,
        ) : Phase
    }

    private class Entry(
        @Volatile var phase: Phase,
        @Volatile var sinceMs: Long,
        /** The cycle in progress, or the last one that ran. Null before the first. */
        @Volatile var tally: CycleTally? = null,
        /** When that cycle started, in epoch seconds. */
        @Volatile var cycleStartedSec: Long? = null,
        /** When it ended; null while it is running. */
        @Volatile var cycleEndedSec: Long? = null,
        /** `running` / `completed` / `failed` — see [Stream.outcome]. */
        @Volatile var outcome: String? = null,
        /**
         * Which half of the router owns the cycle in this slot.
         *
         * ONE stream name can carry both `urls` and `relaySource`, so
         * `StaticBackfill` and `DynamicSync` both open a cycle under it, at boot,
         * at the same time. Without an owner the second `beginCycle` overwrote
         * the first's tally and the first `endCycle` stamped `completed` on the
         * other's still-running fan-out. Publishing one of the two is
         * incomplete; publishing a blend of them is wrong.
         */
        @Volatile var owner: String? = null,
    )

    /**
     * One stream's state, flattened for a reader outside this process.
     *
     * A snapshot, not a view: [SyncProgress] serialises it on a timer while the
     * fan-out keeps moving, and handing out the live [Entry] would publish a
     * document whose members were read at different instants.
     */
    class Stream(
        val name: String,
        /** The phase's own word — `fetching`, `syncing`, `idle`, … — never the rendered line. */
        val phase: String,
        /** How long it has been in that phase, in seconds. */
        val phaseForSec: Long,
        val tally: CycleTally?,
        val cycleStartedSec: Long?,
        val cycleEndedSec: Long?,
        /**
         * What became of the cycle: `running` while it is, `completed` when it
         * reached its end, `failed` when it threw. Published because a static
         * backfill that finished and a stream whose cycle aborted at 80% left
         * the identical trace — both simply stopped saying anything.
         */
        val outcome: String?,
    )

    private val phases = ConcurrentHashMap<String, Entry>()

    /** Ordered, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    @Synchronized
    fun register(name: String) {
        if (phases.putIfAbsent(name, Entry(Phase.Starting, System.currentTimeMillis())) == null) {
            order += name
        }
    }

    /**
     * A cycle has started; [tally] is what it will fill in as urls settle.
     *
     * Replaces the previous cycle's outright. Keeping a history here would be a
     * second, unbounded thing to reason about on a router that already writes
     * its durable state to two files; the last cycle plus the one running is
     * what an operator asks about.
     */
    @Synchronized
    fun beginCycle(
        name: String,
        owner: String,
        tally: CycleTally,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        register(name)
        phases[name]?.let {
            // First writer wins while a cycle is live. The loser keeps counting
            // into its own tally and simply does not publish this round — which
            // is the honest outcome for a stream whose work has two sources and
            // one slot to describe it in.
            if (it.outcome == "running" && it.owner != null && it.owner != owner) return
            it.owner = owner
            it.tally = tally
            it.cycleStartedSec = nowSeconds
            it.cycleEndedSec = null
            it.outcome = "running"
        }
    }

    /**
     * The cycle ended. [outcome] is `completed` or `failed` — the caller knows
     * which, and guessing from the counters would report a cycle that legitimately
     * reached nothing as a failure.
     */
    @Synchronized
    fun endCycle(
        name: String,
        owner: String,
        outcome: String,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        phases[name]?.let {
            // Only for a cycle that actually started, and only by whoever
            // started it. A stream that fails during DISCOVERY has no tally, and
            // stamping an end on the previous cycle's would age a finished run
            // every time the next one threw; a static backfill finishing must
            // not stamp `completed` on a dynamic fan-out still running under the
            // same name.
            if (it.outcome == "running" && it.owner == owner) {
                it.cycleEndedSec = nowSeconds
                it.outcome = outcome
            }
        }
    }

    /** The two halves of the router that can own a stream's cycle slot. */
    companion object {
        const val STATIC = "static"
        const val DYNAMIC = "dynamic"
    }

    /** Every registered stream, in registration order, as of now. */
    @Synchronized
    fun snapshot(): List<Stream> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            Stream(
                name = name,
                phase = word(e.phase),
                phaseForSec = (System.currentTimeMillis() - e.sinceMs) / 1000,
                tally = e.tally,
                cycleStartedSec = e.cycleStartedSec,
                cycleEndedSec = e.cycleEndedSec,
                outcome = e.outcome,
            )
        }

    /**
     * The phase's machine-readable name.
     *
     * Spelled out rather than derived from the class name: these strings are
     * published, and a reader charting them must not have a series renamed by a
     * Kotlin refactor.
     */
    private fun word(phase: Phase): String =
        when (phase) {
            is Phase.Starting -> "starting"
            is Phase.Waiting -> "waiting"
            is Phase.Queued -> "queued"
            is Phase.Discovering -> "discovering"
            is Phase.Snapshotting -> "snapshotting"
            is Phase.Fetching -> "fetching"
            is Phase.Syncing -> "syncing"
            is Phase.Idle -> "idle"
            is Phase.Failed -> "failed"
        }

    fun set(
        name: String,
        phase: Phase,
    ) {
        val existing = phases[name]
        if (existing == null) {
            register(name)
            phases[name]?.phase = phase
            return
        }
        // The clock restarts only when the KIND of phase changes: snapshotting
        // that reports a new count every page is still the same phase, and
        // resetting elapsed there would hide exactly the duration worth seeing.
        if (existing.phase::class != phase::class) existing.sinceMs = System.currentTimeMillis()
        existing.phase = phase
    }

    /** One line per stream, in registration order. */
    @Synchronized
    fun report(): List<String> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            val elapsed = System.currentTimeMillis() - e.sinceMs
            "router: $name ${describe(e.phase, elapsed)}"
        }

    private fun describe(
        phase: Phase,
        elapsedMs: Long,
    ): String {
        val elapsed = fmtDuration(elapsedMs)
        return when (phase) {
            is Phase.Starting -> {
                "starting ($elapsed elapsed)"
            }

            is Phase.Waiting -> {
                "waiting — no relays in [${phase.sources}] yet, retry in ${phase.retrySec}s ($elapsed elapsed)"
            }

            is Phase.Queued -> {
                "queued behind another stream — ${phase.relays} relay(s) ready ($elapsed elapsed)"
            }

            is Phase.Discovering -> {
                "discovering relays from [${phase.sources}] ($elapsed elapsed)"
            }

            is Phase.Snapshotting -> {
                val of = phase.total?.let { "/${fmtCount(it)}" } ?: ""
                val pct = phase.total?.takeIf { it > 0 }?.let { " (${(phase.collected * 100L / it)}%)" } ?: ""
                "snapshotting ${fmtCount(phase.collected)}$of local ids$pct for ${phase.relays} relay(s) ($elapsed elapsed)"
            }

            is Phase.Fetching -> {
                // `returned`, not `done`. This counts fan-out legs that came
                // BACK — including the ones that came back unreachable, capped
                // or out of budget — and reading it as progress is the single
                // most misread number this router publishes. What settled is in
                // the cycle's disposition ([CycleTally]); what is COVERED is in
                // the bands. Three different questions, and only this one is
                // cheap enough to tick every second.
                "fetching ${phase.done}/${phase.total} relay(s) returned, ${phase.events} event(s) received" +
                    rate(phase.events, elapsedMs) +
                    (if (phase.running > 0) ", ${phase.running} in flight (${phase.transferring} transferring)" else "") +
                    (phase.reachedSeconds?.let { " — back to ${fmtDay(it)}" } ?: "") +
                    (phase.fraction?.let { ", %.1f%% of the window walked".format(it * 100) } ?: "") +
                    (phase.etaMs?.let { ", ETA ~${fmtDuration(it)}" } ?: "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Syncing -> {
                "syncing ${phase.done}/${phase.total} relay(s) returned, ${phase.events} event(s) received" +
                    rate(phase.events, elapsedMs) +
                    (if (phase.running > 0) ", ${phase.running} in flight (${phase.transferring} transferring)" else "") +
                    // "skipped as dead" said nothing about what dead MEANT or
                    // when it is retried. It is a NIP-66 record this router (or
                    // another signing with the same key) wrote earlier saying a
                    // relay could not be reached; the set is re-read at the top
                    // of every cycle, so the retry is the refresh interval.
                    (if (phase.skipped > 0) ", ${phase.skipped} not dialled (struck out, no route, or no transport)" else "") +
                    (if (phase.unreachable > 0) ", ${phase.unreachable} dialled and failed" else "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Idle -> {
                // The elapsed clock is the point here, not decoration: a stream
                // that went idle forty-five minutes ago and one that finished a
                // second ago printed the same line, and an ingest queue draining
                // behind a finished cycle looked exactly like a stalled router.
                val tail = if (phase.running > 0) ", ${phase.running} relay(s) still running (${phase.transferring} transferring)" else ""
                phase.nextInSec?.let { "idle — ${phase.events} event(s) received last pass$tail, next in ${it}s ($elapsed ago)" }
                    ?: "backfilled ${phase.events} event(s); live tail only — no further cycles ($elapsed ago)"
            }

            is Phase.Failed -> {
                "failed: ${phase.reason} — retry in ${phase.retrySec}s ($elapsed ago)"
            }
        }
    }

    /**
     * `, 2350/s received` — throughput for the phase, or nothing when it is too
     * early to mean anything.
     *
     * Two things this is NOT, both of which it has been read as. The clock is
     * the PHASE's, so it is the rate of the work being described rather than a
     * lifetime average. And the numerator is events RECEIVED FROM UPSTREAMS by
     * this one stream — the health line's `ev/s` is events reaching ingest
     * across every stream, after dedup drops the copies the other relays already
     * delivered, so the two are counted differently on purpose and disagreeing
     * is not a fault in either.
     */
    private fun rate(
        events: Long,
        elapsedMs: Long,
    ): String {
        // Under a second the divisor is noise and the answer is a wild number.
        if (elapsedMs < 1_000 || events <= 0) return ""
        return ", ${events * 1000 / elapsedMs}/s received"
    }
}
