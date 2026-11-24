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

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import java.util.PriorityQueue
import java.util.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * How [BatchFill] shapes batches, on a clock the test owns: arrivals are scripted in milliseconds
 * and a fake store spends a fixed time per write plus a time per event, as the real one does.
 */
class BatchFillTest {
    private class Clock {
        var now = 0L
    }

    /** Events released as the clock passes their arrival time; each carries a [Long] the test chooses. */
    private class Arrivals(
        private val clock: Clock,
    ) : BatchSource<Long> {
        private val pending = PriorityQueue<Pair<Long, Long>>(compareBy { it.first })

        fun add(
            at: Long,
            value: Long = at,
        ) {
            pending.add(at to value)
        }

        override suspend fun take(): Long? {
            val (at, value) = pending.poll() ?: return null
            clock.now = maxOf(clock.now, at)
            return value
        }

        override fun poll(): Long? = if (pending.peek()?.let { it.first <= clock.now } == true) pending.poll().second else null

        override suspend fun takeWithin(ms: Long): Long? {
            yield()
            val next = pending.peek()
            if (next != null && next.first <= clock.now + ms) {
                pending.poll()
                clock.now = maxOf(clock.now, next.first)
                return next.second
            }
            clock.now += ms
            return null
        }
    }

    private class Written(
        val width: Int,
        /** When the batch's first event was taken: the earliest it could have been written. */
        val takenAt: Long,
        val leftAt: Long,
    )

    /** The fake store's time for one write of [width] events. */
    private fun passMs(width: Int): Long = 4L + width / 10

    /** Every batch a single worker writes from events arriving at [times], whatever the worker does. */
    private fun writes(
        fill: BatchFill,
        clock: Clock,
        times: List<Long>,
    ): List<Written> =
        runBlocking {
            val source = Arrivals(clock).apply { times.forEach { add(it) } }
            val batch = ArrayList<Long>()
            val out = ArrayList<Written>()
            while (true) {
                val before = clock.now
                if (!fill.fill(source, batch)) break
                out.add(Written(batch.size, maxOf(before, batch[0]), clock.now))
                clock.now += passMs(batch.size)
            }
            out
        }

    /**
     * Events written in [ms] by [clients] publishers that each send one event, await its OK and
     * send the next a millisecond later: publishers whose rate is the pipeline's own.
     */
    private fun closedLoop(
        fill: BatchFill,
        clock: Clock,
        clients: Int,
        ms: Long,
    ): Int =
        runBlocking {
            val source = Arrivals(clock).apply { (0 until clients).forEach { add(0, it.toLong()) } }
            val batch = ArrayList<Long>()
            var written = 0
            while (clock.now < ms && fill.fill(source, batch)) {
                clock.now += passMs(batch.size)
                written += batch.size
                batch.forEach { client -> source.add(clock.now + 1 + client % 3, client) }
            }
            written
        }

    /** [perMs] arrivals in every millisecond of [ms]. */
    private fun steady(
        perMs: Int,
        ms: Long,
    ): List<Long> = (0 until ms).flatMap { t -> List(perMs) { t } }

    private fun fill(
        clock: Clock,
        lingerMs: Long,
        target: Int = 500,
    ) = BatchFill(max = 1_000, lingerMs = lingerMs, target = target, nowMs = { clock.now })

    @Test
    fun `a steady trickle is written in wider batches when it may linger`() {
        val trickle = steady(perMs = 3, ms = 3_000)
        val eager = Clock().let { writes(fill(it, lingerMs = 0), it, trickle) }
        val lingering = Clock().let { writes(fill(it, lingerMs = 10), it, trickle) }
        assertEquals(trickle.size, eager.sumOf { it.width })
        assertEquals(trickle.size, lingering.sumOf { it.width })
        val eagerMean = trickle.size.toDouble() / eager.size
        val lingeringMean = trickle.size.toDouble() / lingering.size
        assertTrue(lingeringMean >= 2 * eagerMean, "mean width $lingeringMean lingering against $eagerMean without")
    }

    @Test
    fun `a stream that reaches the pipeline in uneven clumps keeps its linger`() {
        // Clumps 1-12ms apart, as a publisher's stream reaches the channel: some lingers end in a pause.
        val random = Random(7)
        var at = 0L
        val clumped = ArrayList<Long>()
        while (at < 3_000) {
            val pause = 1L + random.nextInt(12)
            at += pause
            repeat(3 * pause.toInt()) { clumped.add(at) }
        }
        val eager = Clock().let { writes(fill(it, lingerMs = 0), it, clumped) }
        val lingering = Clock().let { writes(fill(it, lingerMs = 20), it, clumped) }
        assertTrue(eager.size >= 2 * lingering.size, "${lingering.size} writes lingering against ${eager.size} without")
    }

    @Test
    fun `no batch leaves later than the linger after its first event was taken`() {
        val clock = Clock()
        val written = writes(fill(clock, lingerMs = 10), clock, steady(perMs = 1, ms = 2_000))
        written.forEach { assertTrue(it.leftAt - it.takenAt <= 10, "a batch left ${it.leftAt - it.takenAt}ms after its first event was taken") }
        assertTrue(written.any { it.leftAt - it.takenAt == 10L }, "a trickle that never fills the batch waits out the whole linger")
    }

    @Test
    fun `publishers awaiting their OKs are not held back by the linger`() {
        val eager = Clock().let { closedLoop(fill(it, lingerMs = 0), it, clients = 20, ms = 10_000) }
        val lingering = Clock().let { closedLoop(fill(it, lingerMs = 20), it, clients = 20, ms = 10_000) }
        assertTrue(lingering >= eager * 0.9, "$lingering events written lingering against $eager without")
    }

    @Test
    fun `a stream that follows waiting publishers is lingered on again`() {
        val clock = Clock()
        val batching = fill(clock, lingerMs = 10)
        closedLoop(batching, clock, clients = 20, ms = 2_000)
        clock.now = 0
        val trickle = steady(perMs = 3, ms = 6_000)
        val written = writes(batching, clock, trickle)
        val late = written.filter { it.takenAt >= 4_000 }
        assertTrue(late.sumOf { it.width }.toDouble() / late.size >= 40, "mean width ${late.sumOf { it.width }.toDouble() / late.size} four seconds into the stream")
    }

    @Test
    fun `a lone event on an idle pipeline leaves at once`() {
        val clock = Clock()
        val written = writes(fill(clock, lingerMs = 1_000), clock, listOf(5L, 5_000L))
        assertEquals(listOf(1, 1), written.map { it.width })
        written.forEach { assertEquals(it.takenAt, it.leftAt) }
    }

    @Test
    fun `a batch already half full does not wait`() {
        val clock = Clock()
        val written = writes(fill(clock, lingerMs = 1_000), clock, List(600) { 0L } + listOf(1L))
        assertEquals(600, written.first().width)
        assertEquals(0L, written.first().leftAt)
    }

    @Test
    fun `a full batch does not wait`() {
        val clock = Clock()
        val written = writes(fill(clock, lingerMs = 1_000, target = 1_000), clock, List(1_500) { 0L })
        assertEquals(listOf(1_000, 500), written.map { it.width })
        assertEquals(0L, written.first().leftAt)
    }

    @Test
    fun `nothing is waited for while the producers cannot submit`() =
        runBlocking {
            val clock = Clock()
            val batch = ArrayList<Long>()
            val source = Arrivals(clock).apply { listOf(0L, 0L, 1L).forEach { add(it) } }
            assertTrue(fill(clock, lingerMs = 1_000).fill(source, batch) { false })
            assertEquals(2, batch.size)
            assertEquals(0L, clock.now)
        }

    @Test
    fun `a closed and empty source ends the worker`() =
        runBlocking {
            val clock = Clock()
            assertFalse(fill(clock, lingerMs = 10).fill(Arrivals(clock), ArrayList()))
        }
}
