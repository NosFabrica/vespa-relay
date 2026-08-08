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

import java.util.concurrent.atomic.AtomicLong

/**
 * Whether a probe is still earning its round trip, learned from what it has
 * been dropping rather than declared.
 *
 * The alternative was to tell ingest which phase a stream is in — catch-up
 * versus live tail — and probe only while backfilling. This measures the thing
 * that phase was a proxy for. It needs no flag threaded through seven submit
 * sites, it is right about legs nobody classified (a negentropy reconcile
 * delivers only events we are known to LACK, so both probes are dead weight
 * there and this notices), and it cannot be wrong about a stream whose
 * behaviour changed since someone labelled it.
 *
 * Never latches off: past the threshold it still samples one batch in
 * [RESAMPLE_EVERY], so a stream that starts producing duplicates again is
 * picked back up. The counters decay so that a backfill's 94% cannot keep a
 * probe switched on through the quiet months that follow it.
 */
internal class ProbeGate(
    /** Drop rate below which the round trip stops paying — see each caller for the arithmetic. */
    private val minHitRate: Double,
) {
    private val judged = AtomicLong()
    private val dropped = AtomicLong()
    private val skipped = AtomicLong()

    fun worthIt(): Boolean {
        val seen = judged.get()
        if (seen < LEARN_EVENTS) return true
        if (dropped.get().toDouble() / seen >= minHitRate) return true
        return skipped.incrementAndGet() % RESAMPLE_EVERY == 0L
    }

    fun record(
        judgedNow: Int,
        droppedNow: Int,
    ) {
        dropped.addAndGet(droppedNow.toLong())
        // Halving both keeps the RATE and forgets the age, so the window
        // slides without holding a ring buffer. Racy at the boundary by a
        // batch or two, which a heuristic can afford.
        if (judged.addAndGet(judgedNow.toLong()) > DECAY_ABOVE) {
            judged.set(judged.get() / 2)
            dropped.set(dropped.get() / 2)
        }
    }

    /** Drop rate so far, for the stats line — an operator reading "probe off" wants to see why. */
    fun hitRate(): Double = judged.get().takeIf { it > 0 }?.let { dropped.get().toDouble() / it } ?: 0.0

    /** Whether this gate has any evidence yet, so a status line can stay quiet rather than print 0%. */
    fun hasJudged(): Boolean = judged.get() > 0

    /**
     * [worthIt] without the side effect. Reporting must never call [worthIt]:
     * it advances the resample counter, so a status line printed once a minute
     * would quietly change which batches get probed.
     */
    fun paying(): Boolean {
        val seen = judged.get()
        return seen < LEARN_EVENTS || dropped.get().toDouble() / seen >= minHitRate
    }

    private companion object {
        /** Events judged before the rate is trusted — one batch's worth is noise. */
        const val LEARN_EVENTS = 50_000L

        /** Batches skipped per sampled one once a probe is judged not to pay. */
        const val RESAMPLE_EVERY = 32L

        /** Judged events after which the window halves, so the rate tracks the present. */
        const val DECAY_ABOVE = 1_000_000L
    }
}
