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
package com.nosfabrica.vespa.relay.ingest

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select

/** Where a [BatchFill] takes events from: the pipeline's channel, or a test's scripted arrivals. */
internal interface BatchSource<T> {
    /** The next event, however long it takes; null once the source is closed and empty. */
    suspend fun take(): T?

    /** An event already waiting, or null. */
    fun poll(): T?

    /** The next event within [ms], or null when none came or the source closed. Never loses an event to the timeout. */
    suspend fun takeWithin(ms: Long): T?
}

/** A channel as a [BatchSource]. The timed take is a select, so a timeout never strands an event it had already taken. */
internal fun <T> ReceiveChannel<T>.asBatchSource(): BatchSource<T> =
    object : BatchSource<T> {
        override suspend fun take(): T? = receiveCatching().getOrNull()

        override fun poll(): T? = tryReceive().getOrNull()

        @OptIn(ExperimentalCoroutinesApi::class)
        override suspend fun takeWithin(ms: Long): T? =
            select {
                onReceiveCatching { it.getOrNull() }
                onTimeout(ms) { null }
            }
    }

/**
 * How a worker fills one batch: the first event, everything already waiting, and then, while the
 * batch is narrower than [target], up to [lingerMs] more for events still arriving. A batch leaves
 * at most [lingerMs] after its first event was taken. Not thread-safe: one worker fills at a time.
 *
 * It lingers only while lingering has been fed. Publishers that each await their OK stop sending
 * while a batch waits, so its wait only delays them; that shows as a linger whose arrivals end early.
 */
internal class BatchFill(
    private val max: Int,
    private val lingerMs: Long,
    private val target: Int = (max / 2).coerceAtLeast(1),
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    /** How late into recent lingers events were still arriving, as a share of each wait; 1 is a stream that never paused. */
    private var fed = 1.0

    /** Batches that could have lingered since the last that did, while [fed] says lingering does not pay; one in [SAMPLE_EVERY] does, to notice a stream. */
    private var skipped = 0

    /**
     * Fills [batch], cleared first; false once [source] is closed and empty. [mayGrow] false stops
     * the wait: nothing can arrive while the producers are parked on a spent budget.
     */
    suspend fun <T> fill(
        source: BatchSource<T>,
        batch: MutableList<T>,
        mayGrow: () -> Boolean = { true },
    ): Boolean {
        batch.clear()
        batch.add(source.take() ?: return false)
        val deadline = nowMs() + lingerMs
        takeWaiting(source, batch)
        // A lone event is not waited on: nothing behind it says another is coming.
        if (lingerMs <= 0 || batch.size < 2 || batch.size >= target) return true
        val sampling = fed < FED_ENOUGH
        if (sampling && ++skipped < SAMPLE_EVERY) return true
        skipped = 0
        val startedAt = nowMs()
        var lastArrival = startedAt
        while (batch.size < target && mayGrow()) {
            val left = deadline - nowMs()
            if (left <= 0) break
            batch.add(source.takeWithin(left) ?: break)
            lastArrival = nowMs()
            takeWaiting(source, batch)
        }
        val waited = nowMs() - startedAt
        val share = if (batch.size >= target || waited <= 0) 1.0 else (lastArrival - startedAt).toDouble() / waited
        // Slow to turn off, since a stream's arrivals clump; quick to turn on, since a sample is rare.
        fed += (share - fed) * (if (sampling) SAMPLE_WEIGHT else STEADY_WEIGHT)
        return true
    }

    private fun <T> takeWaiting(
        source: BatchSource<T>,
        batch: MutableList<T>,
    ) {
        while (batch.size < max) batch.add(source.poll() ?: return)
    }

    private companion object {
        /** Below this, arrivals stopped early in most lingers: the publishers were waiting on the batch. */
        const val FED_ENOUGH = 0.5

        const val STEADY_WEIGHT = 0.125

        const val SAMPLE_WEIGHT = 0.5

        const val SAMPLE_EVERY = 256
    }
}
