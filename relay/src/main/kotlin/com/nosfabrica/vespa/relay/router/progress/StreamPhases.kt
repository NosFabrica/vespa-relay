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
        ) : Phase

        /** Fanning out. */
        data class Syncing(
            val done: Int,
            val total: Int,
            val events: Long,
            val skipped: Long,
            val unreachable: Long,
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
                "fetching ${phase.done}/${phase.total} relay(s), ${phase.events} event(s)${rate(phase.events, elapsedMs)}" +
                    (phase.reachedSeconds?.let { " — back to ${fmtDay(it)}" } ?: "") +
                    (phase.fraction?.let { ", %.1f%% through the window".format(it * 100) } ?: "") +
                    (phase.etaMs?.let { ", ETA ~${fmtDuration(it)}" } ?: "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Syncing -> {
                "syncing ${phase.done}/${phase.total} relay(s), ${phase.events} event(s)${rate(phase.events, elapsedMs)}" +
                    (if (phase.skipped > 0) ", ${phase.skipped} skipped as dead" else "") +
                    (if (phase.unreachable > 0) ", ${phase.unreachable} unreachable" else "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Idle -> {
                phase.nextInSec?.let { "idle — ${phase.events} event(s) last cycle, next in ${it}s" }
                    ?: "backfilled ${phase.events} event(s); live tail only — no further cycles"
            }

            is Phase.Failed -> {
                "failed: ${phase.reason} — retry in ${phase.retrySec}s"
            }
        }
    }

    /**
     * `, 2350/s` — throughput for the phase, or nothing when it is too early
     * to mean anything. The clock is the PHASE's, so this is the rate of the
     * work being described rather than a lifetime average.
     */
    private fun rate(
        events: Long,
        elapsedMs: Long,
    ): String {
        // Under a second the divisor is noise and the answer is a wild number.
        if (elapsedMs < 1_000 || events <= 0) return ""
        return ", ${events * 1000 / elapsedMs}/s"
    }
}
