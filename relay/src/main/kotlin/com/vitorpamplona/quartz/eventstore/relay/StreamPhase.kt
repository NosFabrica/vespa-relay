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
 * A `created_at` as a UTC day, for saying how far back a walk has reached.
 *
 * Day resolution on purpose: this answers "is it moving, and roughly where is
 * it" over minutes of walking, and a timestamp to the second would change on
 * every line without making either answer clearer.
 */
internal fun fmtDay(seconds: Long): String =
    java.time.Instant
        .ofEpochSecond(seconds)
        .atZone(java.time.ZoneOffset.UTC)
        .toLocalDate()
        .toString()

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
        /**
         * Registered, not yet started. The only honest thing to say before a
         * stream's first phase arrives.
         *
         * The default used to be `Discovering("")`, which had a static stream —
         * one whose relays are written in the config and are never discovered at
         * all — reporting `discovering relays from []` for its first minute.
         */
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
            /** Time-axis progress of the relays still walking — see [PagingProgress]. */
            val fraction: Double? = null,
            val etaMs: Long? = null,
            /**
             * Oldest `created_at` the walk has reached, in seconds.
             *
             * The percentage alone cannot show a deep walk moving: a paged fetch
             * with no `since` runs back to [SyncCursors.PLAUSIBLE_FLOOR], so days
             * of real progress round to `0%` and the line looks identical to a
             * stalled one. The date moves every page.
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
                "idle — ${phase.events} event(s) last cycle, next in ${phase.nextInSec}s"
            }

            is Phase.Failed -> {
                "failed: ${phase.reason} — retry in ${phase.retrySec}s"
            }
        }
    }

    /**
     * `, 2350/s` — throughput for the phase, or nothing when it is too early to
     * mean anything.
     *
     * Only the static backfill line ever carried a rate; the dynamic streams
     * reported a running total against an elapsed clock and left the division
     * to whoever was reading. A total that is still climbing and one that has
     * stalled look identical that way, which is the thing worth seeing.
     *
     * The clock is the PHASE's, and it restarts when the phase kind changes, so
     * this is the rate of the work being described rather than a lifetime
     * average diluted by everything before it.
     */
    private fun rate(
        events: Long,
        elapsedMs: Long,
    ): String {
        // Under a second the divisor is noise and the answer is a wild number.
        if (elapsedMs < 1_000 || events <= 0) return ""
        return ", ${events * 1000 / elapsedMs}/s"
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

/**
 * How far a paged walk has got, and when it will finish — measured on the time
 * axis, because that is the only axis whose end is known in advance.
 *
 * A paged fetch has no event denominator. It asks for the newest events below a
 * moving `until` and walks backwards; how many exist is exactly what it is
 * trying to find out. Every attempt to report a percentage from event counts
 * therefore reported `downloaded/downloaded` — a permanent `100%` and an
 * `ETA ~0:00` from the first event, printed for hours against a relay that had
 * barely started. Five numbers on that line and not one of them measured
 * anything.
 *
 * The time axis has what the event axis lacks: both ends are known before the
 * first request. The walk starts at the filter's `until` (or now) and finishes
 * at its `since` (or [SyncCursors.PLAUSIBLE_FLOOR], below which no real event
 * can exist), and `fetchAllPages` reports each page's new `until` — the exact
 * position of the walk between them. It needs no COUNT support, which matters:
 * the relay holding the scores we most want does not implement NIP-45.
 *
 * The estimate assumes events are spread evenly over time, which they are not —
 * a relay busier this month than in 2021 finishes its last stretch faster than
 * predicted. So this errs pessimistic on the tail and is honest about what it
 * is: a bound that only ever moves forward, never a promise.
 */
class PagingProgress {
    private class Walk(
        val top: Long,
        val bottom: Long,
        val startedMs: Long,
        @Volatile var current: Long,
    )

    private val walks = ConcurrentHashMap<String, Walk>()

    /** Begin a walk over `[bottom, top]` seconds. */
    fun begin(
        key: String,
        top: Long,
        bottom: Long,
    ) {
        if (top > bottom) walks[key] = Walk(top, bottom, System.currentTimeMillis(), top)
    }

    /** The walk reached [until]; monotonic, so a page that jumps back cannot un-advance it. */
    fun mark(
        key: String,
        until: Long,
    ) {
        walks[key]?.let {
            // Clamped to the walk's own floor. A page cursor comes from the
            // oldest `created_at` a relay returned, and relays serve events
            // stamped 0 — one of those dragged the cursor to epoch and the line
            // read `back to 1969-12-31` while the walk was in fact somewhere in
            // 2026. The floor is the oldest second this walk can legitimately
            // reach, so anything below it means the walk is done, not that it
            // has travelled to 1969.
            val reached = until.coerceAtLeast(it.bottom)
            if (reached < it.current) it.current = reached
        }
    }

    fun finish(key: String) {
        walks.remove(key)
    }

    /**
     * Fraction of the walk complete, averaged over every relay still walking.
     *
     * Averaged rather than summed: relays run concurrently and each covers its
     * own span, so "half the relays are done and half are at zero" is 50% — the
     * thing an operator actually wants to know.
     */
    fun fraction(stream: String? = null): Double? {
        val live = live(stream)
        if (live.isEmpty()) return null
        return live.sumOf { w ->
            val span = (w.top - w.bottom).coerceAtLeast(1)
            ((w.top - w.current).toDouble() / span).coerceIn(0.0, 1.0)
        } / live.size
    }

    /**
     * The walks belonging to [stream], or every walk when it is null.
     *
     * One PagingProgress serves the whole router, so an unscoped question is
     * answered over every stream at once: both streams printed the SAME `33.4%`
     * and the same `back to …` date while one was walking 2026 and the other had
     * bottomed out at the floor. A key is `"stream|url"`, so the stream name
     * scopes it.
     */
    private fun live(stream: String?): List<Walk> =
        if (stream == null) {
            walks.values.toList()
        } else {
            walks.entries.filter { it.key.startsWith("$stream|") }.map { it.value }
        }

    /** The oldest second [stream] has reached, or null when it is not walking. */
    fun reached(stream: String? = null): Long? = live(stream).minOfOrNull { it.current }

    /** Milliseconds left at the rate achieved so far, or null before it means anything. */
    fun etaMs(stream: String? = null): Long? {
        val f = fraction(stream) ?: return null
        // Under a few percent the extrapolation is dominated by connect time and
        // produces numbers like "ETA 9 days" that are worse than saying nothing.
        if (f < 0.02) return null
        val oldestStart = live(stream).minOfOrNull { it.startedMs } ?: return null
        val elapsed = System.currentTimeMillis() - oldestStart
        if (elapsed < 5_000) return null
        return ((elapsed / f) - elapsed).toLong()
    }
}
