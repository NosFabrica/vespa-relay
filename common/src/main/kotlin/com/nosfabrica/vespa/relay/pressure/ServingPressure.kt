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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.pow

/**
 * How slow the relay's own reads have become, so the mirror can stop filling the engine's queue.
 * The mean is exponentially weighted (alpha = 1/8): one slow query is absorbed, a sustained rise
 * moves it within a handful of reads. It decays only once no read is in flight and none has
 * ended for [QUIET_GRACE_MS], and a read in flight past the threshold counts at its age.
 */
class ServingPressure(
    /** Above this mean read latency (ms), ingest starts yielding. */
    private val thresholdMs: Long = DEFAULT_THRESHOLD_MS,
    private val maxBackoffMs: Long = 2_000,
    /** Monotonic milliseconds. */
    private val clockMs: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    private val meanMicros = AtomicLong(0)

    private val samples = AtomicLong(0)

    /** When a read last ended, answered or not. */
    @Volatile private var lastReadMs = clockMs()

    private val inFlight = ConcurrentHashMap.newKeySet<Read>()

    /** One read from [begin] to its first end; a second end is a no-op, so two paths may both end it. */
    inner class Read internal constructor(
        internal val startedMs: Long,
    ) {
        private val ended = AtomicBoolean(false)

        /** The read answered: its latency becomes a sample. */
        fun finish() {
            if (!ended.compareAndSet(false, true)) return
            // Recorded while still in flight, so the quiet it spent hanging does not age the mean.
            record(clockMs() - startedMs)
            inFlight.remove(this)
        }

        /** The read ended unanswered (cancelled, failed, closed before its EOSE): no sample. */
        fun abandon() {
            if (!ended.compareAndSet(false, true)) return
            lastReadMs = clockMs()
            inFlight.remove(this)
        }
    }

    /** Starts timing one read; every [Read] must be ended, or it holds the mean up for good. */
    fun begin(): Read = Read(clockMs()).also { inFlight.add(it) }

    /** Records a completed read; on the serving path, so it must stay cheap. */
    fun record(durationMs: Long) {
        // Floored at 1 so a mean of zero stays unreachable; the counter, not the mean, says which
        // sample is first.
        val micros = durationMs.coerceAtLeast(1) * 1_000
        val now = clockMs()
        val first = samples.getAndIncrement() == 0L
        meanMicros.updateAndGet { prev ->
            if (first) micros else aged(prev, now).let { it + (micros - it) / 8 }
        }
        lastReadMs = now
    }

    /** The current mean read latency in milliseconds, decayed for any quiet since, or a stalled read's age; 0 before any read. */
    fun meanMs(): Long {
        val now = clockMs()
        val mean = aged(meanMicros.get(), now) / 1_000
        val stalledMs = inFlight.minOfOrNull { it.startedMs }?.let { now - it } ?: 0
        return if (stalledMs > thresholdMs) maxOf(mean, stalledMs) else mean
    }

    /** [micros] halved every [DECAY_HALF_LIFE_MS] of quiet past the grace: a relay nobody reads has no readers to protect. */
    private fun aged(
        micros: Long,
        now: Long,
    ): Long {
        // A read that hangs has not finished to say so.
        if (inFlight.isNotEmpty()) return micros
        val quietMs = now - lastReadMs - QUIET_GRACE_MS
        if (quietMs <= 0) return micros
        return (micros * 0.5.pow(quietMs.toDouble() / DECAY_HALF_LIFE_MS)).toLong()
    }

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
        lastReadMs = clockMs()
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

        /** How long the mean holds after the last read ends, before it starts to fade. */
        const val QUIET_GRACE_MS = 60_000L

        /** How fast the mean fades once the grace is spent. */
        const val DECAY_HALF_LIFE_MS = 30_000L
    }
}
