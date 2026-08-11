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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import java.util.concurrent.ConcurrentHashMap

/**
 * How far a paged walk has got, measured on the time axis — the only axis
 * whose end is known in advance. A paged fetch has no event denominator (how
 * many events exist is exactly what it is finding out), so every count-based
 * percentage degenerated to `downloaded/downloaded = 100%`. The time axis has
 * both ends before the first request: the filter's `until` (or now) down to
 * its `since` (or [SyncCoverage.PLAUSIBLE_FLOOR]), with each page reporting
 * its new position between them.
 *
 * The estimate assumes events are spread evenly over time, which they are
 * not — so it errs pessimistic on the tail, and is a bound, not a promise.
 *
 * ## Why a finished walk stays in the denominator
 *
 * It used to be removed, and that made the percentage run BACKWARDS: the
 * average was over the walks still live, so every relay that finished left the
 * numerator and the denominator together, and a stream whose fast relays
 * drained first fell from 60% to 20% while strictly gaining ground. A fraction
 * that decreases as work completes is not a slow fraction, it is a wrong one —
 * an operator reads it as the sync having lost what it had.
 *
 * So [finish] RETAINS the walk for the rest of the cycle and [reset] clears the
 * stream at the start of the next one. The retained walk contributes what it
 * actually achieved: `covered = true` — the relay EOSE'd an empty page, so there
 * is nothing below where it stopped — contributes 1.0, and anything else (a
 * capped walk, a socket that closed, a throw between [begin] and [finish])
 * contributes the share it had really reached. That is also the only place in
 * the progress line where a walk that FAILED and one that SUCCEEDED stop looking
 * identical.
 */
class PagingProgress {
    private class Walk(
        val top: Long,
        val bottom: Long,
        val startedMs: Long,
        @Volatile var current: Long,
        /** Ended, and still counted — see the class header. */
        @Volatile var done: Boolean = false,
        /** Ended having proved there is nothing below it, i.e. worth a full share. */
        @Volatile var covered: Boolean = false,
    ) {
        /**
         * The share of this walk's window it has got through.
         *
         * A settled walk is 1.0 by fiat rather than by arithmetic: `mark` is fed
         * the cursor of each page RECEIVED, so the last thing a drained walk
         * reports is the oldest event the relay actually held — routinely well
         * above the filter's floor. Measured against the floor it would sit at
         * about 70% forever, and a stream of exhausted relays would report a
         * number that could never reach 100% however complete it was.
         */
        fun share(): Double {
            if (covered) return 1.0
            val span = (top - bottom).coerceAtLeast(1)
            return ((top - current).toDouble() / span).coerceIn(0.0, 1.0)
        }
    }

    private val walks = ConcurrentHashMap<String, Walk>()

    /**
     * Begin a walk over `[bottom, top]` seconds. Keys are `"stream|url"`.
     * An inverted window is not a walk; a single-second one (`top == bottom`)
     * is — a band's re-read edge leg is exactly that shape.
     */
    fun begin(
        key: String,
        top: Long,
        bottom: Long,
    ) {
        if (top >= bottom) walks[key] = Walk(top, bottom, System.currentTimeMillis(), top)
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

    /**
     * The walk ended. It stays in the average until [reset] — see the class
     * header for why removing it made the percentage run backwards.
     *
     * [covered] is the walk's own verdict, not a guess: `true` only for a drain
     * (an EOSE on an empty page, which proves there is nothing older). Every
     * call site closes in a `finally`, so the default has to be the honest one —
     * a throw between [begin] and [finish] settled nothing.
     */
    fun finish(
        key: String,
        covered: Boolean = false,
    ) {
        walks[key]?.let {
            it.covered = covered
            it.done = true
        }
    }

    /**
     * Forget what [stream] has already FINISHED walking, at the start of its
     * next cycle.
     *
     * Called by the cycle, never by a leg: the retained walks ARE the finished
     * share of the cycle in progress, so clearing them any earlier is the
     * backwards-running fraction again by another route.
     *
     * Finished ones only, and that is not caution. One stream name can carry
     * both `urls` and `relaySource` — `StaticBackfill` and `DynamicSync` then
     * write walks under the same prefix — so a blanket clear would delete a
     * static backfill's live walk out from under it on the first dynamic cycle,
     * and every `mark` and `finish` on a removed key is silently a no-op. The
     * walk would simply stop existing while it was still running.
     */
    fun reset(stream: String) {
        walks.entries.removeIf { it.key.startsWith("$stream|") && it.value.done }
    }

    /**
     * Fraction of the cycle's paged work complete, averaged over every relay it
     * has walked — averaged rather than summed because each covers its own span,
     * so "half the relays done and half at zero" is 50%.
     *
     * The denominator is every walk BEGUN this cycle, finished or not. That is
     * what makes it monotonic: a relay that completes moves from its own partial
     * share to 1.0 and stays in the count.
     */
    fun fraction(stream: String? = null): Double? {
        val all = all(stream)
        if (all.isEmpty()) return null
        return all.sumOf { it.share() } / all.size
    }

    /**
     * The walks belonging to [stream], or every walk when null. One
     * PagingProgress serves the whole router, so the stream name scopes the
     * question — unscoped, two streams once printed each other's numbers.
     */
    private fun all(stream: String?): List<Walk> =
        if (stream == null) {
            walks.values.toList()
        } else {
            walks.entries.filter { it.key.startsWith("$stream|") }.map { it.value }
        }

    /**
     * The oldest second [stream] is CURRENTLY reading, or null when nothing of
     * it is walking.
     *
     * Live walks only, unlike [fraction]. This one is a position, not a score:
     * the line prints it as "back to <date>", and a finished walk's floor would
     * pin that date at the deepest thing the cycle ever touched while the relays
     * still walking are nowhere near it.
     */
    fun reached(stream: String? = null): Long? = all(stream).filter { !it.done }.minOfOrNull { it.current }

    /** Milliseconds left at the rate achieved so far, or null before it means anything. */
    fun etaMs(stream: String? = null): Long? {
        val f = fraction(stream) ?: return null
        // Under a few percent the extrapolation is dominated by connect time
        // and produces numbers worse than saying nothing.
        if (f < 0.02) return null
        // Nothing left to wait for; extrapolating 0 seconds from a division is
        // the same answer and one rounding error away from a negative one.
        if (f >= 1.0) return 0L
        val oldestStart = all(stream).minOfOrNull { it.startedMs } ?: return null
        val elapsed = System.currentTimeMillis() - oldestStart
        if (elapsed < 5_000) return null
        return ((elapsed / f) - elapsed).toLong()
    }
}
