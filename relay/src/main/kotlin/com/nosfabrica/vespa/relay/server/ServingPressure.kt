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
package com.nosfabrica.vespa.relay.server

import java.util.concurrent.atomic.AtomicLong

/**
 * How slow the relay's own reads have become, so the mirror can get out of
 * their way. A client's REQ queues behind ingest's queries inside the engine
 * and nothing can reorder that queue — the only lever is to stop filling it.
 *
 * Latency is the steering signal (not a fixed ingest rate) because it needs no
 * per-deployment tuning. The mean is exponentially weighted (alpha = 1/8): an
 * ordinary slow query is absorbed, a sustained rise moves it within a handful
 * of reads, and it decays back as soon as reads are healthy.
 */
class ServingPressure(
    /**
     * Above this mean read latency (ms), ingest starts yielding. Comfortably
     * above a healthy read (~400ms against 52M documents) and far below the
     * point a client gives up.
     */
    private val thresholdMs: Long = 2_000,
    /** Never pause a batch longer than this, however bad it gets. */
    private val maxBackoffMs: Long = 2_000,
) {
    // Fixed-point millis: updated from many threads, and an AtomicLong keeps
    // that lock-free without a full histogram.
    private val meanMicros = AtomicLong(0)

    private val samples = AtomicLong(0)

    /** Record a completed read. Called on the serving path, so it must stay cheap. */
    fun record(durationMs: Long) {
        val micros = durationMs * 1_000
        samples.incrementAndGet()
        meanMicros.updateAndGet { prev ->
            if (prev == 0L) micros else prev + (micros - prev) / 8
        }
    }

    /** The current mean read latency in milliseconds, or 0 before any read. */
    fun meanMs(): Long = meanMicros.get() / 1_000

    fun sampleCount(): Long = samples.get()

    /**
     * How long ingest should wait before its next batch, in milliseconds. Zero
     * while reads are healthy or before there are enough samples to mean
     * anything — a relay nobody is querying must mirror at full speed. Past
     * the threshold it grows with the overshoot.
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
        /**
         * Below this, the mean is one client's cold first query rather than a
         * trend, and throttling the mirror on it would be superstition.
         */
        const val MIN_SAMPLES = 20
    }
}
