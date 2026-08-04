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
package com.nosfabrica.vespa.relay.router

import java.util.concurrent.ConcurrentHashMap

/**
 * How far a paged walk has got, measured on the time axis — the only axis
 * whose end is known in advance. A paged fetch has no event denominator (how
 * many events exist is exactly what it is finding out), so every count-based
 * percentage degenerated to `downloaded/downloaded = 100%`. The time axis has
 * both ends before the first request: the filter's `until` (or now) down to
 * its `since` (or [SyncCursors.PLAUSIBLE_FLOOR]), with each page reporting
 * its new position between them.
 *
 * The estimate assumes events are spread evenly over time, which they are
 * not — so it errs pessimistic on the tail, and is a bound, not a promise.
 */
class PagingProgress {
    private class Walk(
        val top: Long,
        val bottom: Long,
        val startedMs: Long,
        @Volatile var current: Long,
    )

    private val walks = ConcurrentHashMap<String, Walk>()

    /** Begin a walk over `[bottom, top]` seconds. Keys are `"stream|url"`. */
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
            // Clamped to the walk's own floor: relays serve events stamped 0,
            // and one of those once dragged the line to `back to 1969-12-31`.
            // Below the floor means the walk is done, not time travel.
            val reached = until.coerceAtLeast(it.bottom)
            if (reached < it.current) it.current = reached
        }
    }

    fun finish(key: String) {
        walks.remove(key)
    }

    /**
     * Fraction of the walk complete, averaged over every relay still walking —
     * averaged rather than summed because each covers its own span, so "half
     * the relays done and half at zero" is 50%.
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
     * The walks belonging to [stream], or every walk when null. One
     * PagingProgress serves the whole router, so the stream name scopes the
     * question — unscoped, two streams once printed each other's numbers.
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
        // Under a few percent the extrapolation is dominated by connect time
        // and produces numbers worse than saying nothing.
        if (f < 0.02) return null
        val oldestStart = live(stream).minOfOrNull { it.startedMs } ?: return null
        val elapsed = System.currentTimeMillis() - oldestStart
        if (elapsed < 5_000) return null
        return ((elapsed / f) - elapsed).toLong()
    }
}
