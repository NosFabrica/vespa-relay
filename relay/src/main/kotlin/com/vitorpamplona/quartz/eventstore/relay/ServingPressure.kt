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

import java.util.concurrent.atomic.AtomicLong

/**
 * How slow the relay's own reads have become, so ingest can get out of their way.
 *
 * A relay exists to answer clients. Mirroring is what it does with the time left
 * over, and this makes that ordering true rather than aspirational: measured on
 * this deployment, a REQ that takes **381ms** while ingest is idle returned
 * **nothing in 20 seconds** while the ingest queue was full and Vespa sat at
 * 1087% CPU. A client cannot tell that relay from a broken one.
 *
 * The contention is not in our HTTP client — its dispatcher allows 1024
 * concurrent requests — but inside the engine, where a client's REQ queues
 * behind ingest's dedup and projection queries. Nothing on the client side can
 * reorder that queue, so the only lever is to stop filling it.
 *
 * ## Why latency, and not a fixed budget
 *
 * A fixed ingest rate is wrong on both sides: too low and an idle relay mirrors
 * at a fraction of what the hardware allows, too high and it still buries reads
 * on a bad day. Read latency is the thing we actually care about, so it is the
 * thing to steer by — and it needs no tuning per deployment, which every
 * hard-coded rate in this router has eventually needed.
 *
 * The signal is an exponentially weighted mean (alpha = 1/8), not a max. An
 * ordinary slow query — a cold cache, an expensive filter — is absorbed, and a
 * sustained rise moves the mean within a handful of reads. A single
 * CATASTROPHIC read still trips it, because an eighth of thirty seconds clears
 * any sane threshold on its own. That is intended rather than a rough edge: a
 * read that slow means a client has already given up, and the mean decays back
 * as soon as reads are healthy again.
 */
class ServingPressure(
    /**
     * Above this mean read latency, ingest starts yielding. Comfortably above a
     * healthy read here (~400ms against 52M documents) so ordinary variance
     * never throttles the mirror, and far below the point a client gives up.
     */
    private val thresholdMs: Long = 2_000,
    /** Never pause a batch longer than this, however bad it gets. */
    private val maxBackoffMs: Long = 2_000,
) {
    // Fixed-point millis: the mean is updated from many threads and an
    // AtomicLong keeps that lock-free without pulling in a full histogram.
    private val meanMicros = AtomicLong(0)

    private val samples = AtomicLong(0)

    /** Record a completed read. Called on the serving path, so it must stay cheap. */
    fun record(durationMs: Long) {
        val micros = durationMs * 1_000
        samples.incrementAndGet()
        meanMicros.updateAndGet { prev ->
            // alpha = 1/8: a sustained change moves the mean within a handful of
            // reads, a single outlier moves it by an eighth.
            if (prev == 0L) micros else prev + (micros - prev) / 8
        }
    }

    /** The current mean read latency in milliseconds, or 0 before any read. */
    fun meanMs(): Long = meanMicros.get() / 1_000

    fun sampleCount(): Long = samples.get()

    /**
     * How long ingest should wait before its next batch, in milliseconds.
     *
     * Zero while reads are healthy — the common case must cost nothing. Past the
     * threshold it grows with the overshoot, so a relay that is merely busy
     * slows a little and one that is drowning slows a lot.
     *
     * Returns 0 until there are enough samples to mean anything: a relay nobody
     * is querying has no serving latency to protect, and must mirror at full
     * speed.
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
