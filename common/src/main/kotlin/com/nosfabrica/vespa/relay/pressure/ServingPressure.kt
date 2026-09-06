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
package com.nosfabrica.vespa.relay.pressure

import java.util.concurrent.atomic.AtomicLong

/**
 * How slow the relay's own reads have become, so the mirror can stop filling the engine's queue.
 * The mean is exponentially weighted (alpha = 1/8): one slow query is absorbed, a sustained rise
 * moves it within a handful of reads.
 */
class ServingPressure(
    /** Above this mean read latency (ms), ingest starts yielding. */
    private val thresholdMs: Long = DEFAULT_THRESHOLD_MS,
    private val maxBackoffMs: Long = 2_000,
) {
    private val meanMicros = AtomicLong(0)

    private val samples = AtomicLong(0)

    /** Records a completed read; on the serving path, so it must stay cheap. */
    fun record(durationMs: Long) {
        // Floored at 1 so a mean of zero stays unreachable; the counter, not the mean, says which
        // sample is first.
        val micros = durationMs.coerceAtLeast(1) * 1_000
        val first = samples.getAndIncrement() == 0L
        meanMicros.updateAndGet { prev ->
            if (first) micros else prev + (micros - prev) / 8
        }
    }

    /** The current mean read latency in milliseconds, or 0 before any read. */
    fun meanMs(): Long = meanMicros.get() / 1_000

    fun sampleCount(): Long = samples.get()

    /**
     * Overwrites the mean with one measured elsewhere, replacing rather than smoothing: the EWMA
     * already happened there. An instance is recorded into or adopted into, never both;
     * `adopt(0, 0)` is the reset.
     */
    fun adopt(
        meanMs: Long,
        sampleCount: Long,
    ) {
        meanMicros.set(meanMs.coerceAtLeast(0) * 1_000)
        samples.set(sampleCount.coerceAtLeast(0))
    }

    /**
     * How long ingest should wait before its next batch: zero while reads are healthy or
     * under-sampled, then the overshoot.
     */
    fun backoffMs(): Long {
        if (samples.get() < MIN_SAMPLES) return 0
        val mean = meanMs()
        if (mean <= thresholdMs) return 0
        return (mean - thresholdMs).coerceAtMost(maxBackoffMs)
    }

    /** For the health line: whether ingest is currently yielding, and by how much. */
    fun describe(): String? {
        val backoff = backoffMs()
        return if (backoff <= 0) null else "reads ${meanMs()}ms — ingest yielding ${backoff}ms/batch"
    }

    companion object {
        const val DEFAULT_THRESHOLD_MS = 2_000L

        /** Below this, the mean is one client's cold first query rather than a trend. */
        const val MIN_SAMPLES = 20
    }
}
