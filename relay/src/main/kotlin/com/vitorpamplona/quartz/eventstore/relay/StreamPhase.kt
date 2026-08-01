/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import java.util.concurrent.ConcurrentHashMap

/** `h:mm:ss` past an hour, `m:ss` below it. Shared by every progress line. */
internal fun fmtDuration(ms: Long): String {
    val s = ms / 1000
    val h = s / 3600
    val m = (s % 3600) / 60
    val sec = s % 60
    return if (h > 0) "%d:%02d:%02d".format(h, m, sec) else "%d:%02d".format(m, sec)
}

/**
 * What each stream is doing right now, so an operator can tell a stream that is
 * working from one that never started.
 *
 * Every line the router used to print reported an event that had FINISHED. That
 * is the wrong shape for this system: its two longest phases — walking the local
 * id set, and discovering relays from the store — produce no completion for
 * minutes, so they printed nothing at all. A dynamic stream could sit for hours
 * having said one word, and the static backfill reported
 * `0/12 relay(s) done, 0/0 events (0%), 0/s avg` throughout, which reads as idle
 * and means "still measuring".
 *
 * Two rules follow, and both are what the old output violated:
 *
 *  - **A phase in progress reports elapsed time.** "How long has it been doing
 *    that" is the question, and only the phase itself can answer it.
 *  - **A configured stream is never absent.** It appears on every tick even when
 *    idle, so silence can never be mistaken for "not configured" — which is
 *    exactly how two dynamic streams went unnoticed.
 */
class StreamPhases {
    sealed interface Phase {
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
         *
         * [collected] is the running count and [total] what the engine says the
         * filter matches, so this reports real progress rather than just that
         * time is passing. [total] is null when it could not be counted — an
         * unknown denominator is better than a wrong one.
         */
        data class Snapshotting(
            val collected: Int,
            val total: Int?,
            val relays: Int,
        ) : Phase

        /**
         * Paging relays that are not reconciling — no id set involved.
         *
         * A stream where every relay fetches used to report nothing at all: the
         * paging path set no phase, so the line sat on its registration
         * placeholder and read `discovering relays from []` for 46 minutes while
         * twelve relays moved 14M events. The count was on the very next line
         * the whole time.
         */
        data class Fetching(
            val done: Int,
            val total: Int,
            val events: Long,
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
            val nextInSec: Long,
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
        if (phases.putIfAbsent(name, Entry(Phase.Discovering(""), System.currentTimeMillis())) == null) {
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
        // The clock restarts only when the KIND of phase changes. Snapshotting
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
                "fetching ${phase.done}/${phase.total} relay(s), ${phase.events} event(s) ($elapsed elapsed)"
            }

            is Phase.Syncing -> {
                "syncing ${phase.done}/${phase.total} relay(s), ${phase.events} event(s)" +
                    (if (phase.skipped > 0) ", ${phase.skipped} skipped as dead" else "") +
                    (if (phase.unreachable > 0) ", ${phase.unreachable} unreachable" else "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Idle -> {
                "idle — ${phase.events} event(s) last cycle, next in ${phase.nextInSec}s"
            }

            is Phase.Failed -> {
                "failed: ${phase.reason} — retry in ${phase.retrySec}s"
            }
        }
    }

    companion object {
        /** 24.8M rather than 24819118: the magnitude is the point, not the digits. */
        fun fmtCount(n: Int): String =
            when {
                n >= 1_000_000 -> "%.1fM".format(n / 1_000_000.0)
                n >= 1_000 -> "%.0fk".format(n / 1_000.0)
                else -> n.toString()
            }
    }
}
