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

import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** What a client that stops reading can make this relay hold for it, and that one still reading is never cut off. */
class OutboundQueueTest {
    /** A whole REQ answer lands at store speed while a slow link drains it; that is not a slow consumer. */
    @Test
    fun `a client draining slowly but steadily is never closed however far over the budget it runs`() =
        runBlocking(Dispatchers.Default) {
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000, stallMs = 500)
            val event = "x".repeat(1_000)
            val frames = 300
            val written = AtomicInteger()
            val writer =
                launch {
                    queue.drain {
                        delay(2)
                        written.incrementAndGet()
                    }
                }
            repeat(frames) {
                assertTrue(queue.offer(event), "frame $it refused while the client was reading")
                if (it % 2 == 0) delay(1)
            }
            assertTrue(frames - written.get() > 10, "the backlog never passed the budget, so this proved nothing")
            while (written.get() < frames) {
                assertFalse(queue.stalled(), "a client that is reading reported as stalled")
                delay(10)
            }
            assertEquals(frames, written.get())
            queue.close()
            writer.join()
        }

    @Test
    fun `a client that never reads is closed once the writer has waited out the stall window`() =
        runBlocking {
            var now = 0L
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000, stallMs = 30_000, nowMs = { now })
            val event = "x".repeat(4_000)
            val writer = launch { queue.drain { awaitCancellation() } }
            assertTrue(queue.offer(event))
            yield()
            repeat(3) { assertTrue(queue.offer(event), "inside the stall window the budget alone closes nobody") }
            assertFalse(queue.stalled())

            now += 30_000
            assertTrue(queue.stalled(), "over the budget with no frame handed on for the whole window")
            assertFalse(queue.offer(event))
            queue.close()
            writer.cancel()
        }

    /** A writer stuck behind a small backlog is a quiet client, not one the relay must protect itself from. */
    @Test
    fun `a stalled writer under the character budget is not a slow consumer`() =
        runBlocking {
            var now = 0L
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000, stallMs = 30_000, nowMs = { now })
            val writer = launch { queue.drain { awaitCancellation() } }
            assertTrue(queue.offer("x".repeat(4_000)))
            yield()
            now += 60_000
            assertFalse(queue.stalled())
            assertTrue(queue.offer("x".repeat(4_000)))
            queue.close()
            writer.cancel()
        }

    /** An idle connection whose queue was empty has not stalled, however long ago it last sent. */
    @Test
    fun `a burst after a long idle spell is not a stall`() =
        runBlocking {
            var now = 0L
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000, stallMs = 30_000, nowMs = { now })
            val writer = launch { queue.drain { } }
            assertTrue(queue.offer("[]"))
            yield()
            now += 600_000
            repeat(5) { assertTrue(queue.offer("x".repeat(4_000))) }
            assertFalse(queue.stalled())
            queue.close()
            writer.join()
        }

    /**
     * A page no single connection is cut for, held by many slow readers at once, is what exhausts a heap: past the
     * process budget, a connection over its own character budget is a slow consumer even while still reading, and
     * what a closed connection held leaves the budget.
     */
    @Test
    fun `past the process budget a connection over its own budget is a slow consumer`() {
        val budget = OutboundBudget(limitChars = 150)
        val first = OutboundQueue(maxFrames = 1_000, maxChars = 100, budget = budget)
        val second = OutboundQueue(maxFrames = 1_000, maxChars = 100, budget = budget)

        assertTrue(first.offer("x".repeat(120)), "over its own budget, the process under its: still served")
        assertFalse(first.slowConsumer())
        assertTrue(second.offer("x".repeat(60)), "under its own budget: served even with the process over")
        assertTrue(budget.exhausted())
        assertTrue(first.slowConsumer(), "the process is over: the connection holding most gives it back")
        assertFalse(first.offer("x"), "and is refused, which ends it")
        assertFalse(second.slowConsumer(), "one under its own budget is not cut for the process")

        first.close()
        assertEquals(60, budget.queued(), "what the closed connection held left the budget")
        assertFalse(budget.exhausted())
        second.close()
        assertEquals(0, budget.queued())
    }

    @Test
    fun `raised limits do not unbound the frame count`() {
        assertEquals(MAX_OUTGOING_FRAMES, outboundFrameCap(RelayLimits(maxFilters = 1_000, maxLimit = Int.MAX_VALUE)))
        assertEquals(10 * 5_000 + MAX_OUTGOING_BUFFER, outboundFrameCap(RelayLimits(maxFilters = 10, maxLimit = 5_000)))
    }

    @Test
    fun `many small frames still trip the frame count`() {
        val queue = OutboundQueue(maxFrames = 3, maxChars = 10_000)

        repeat(3) { assertTrue(queue.offer("[]")) }
        assertFalse(queue.offer("[]"))
    }

    @Test
    fun `what the writer sends no longer counts against the client`() =
        runBlocking {
            var now = 0L
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000, stallMs = 30_000, nowMs = { now })
            val sent = CompletableDeferred<Unit>()
            val writer =
                launch {
                    queue.drain {
                        if (sent.isCompleted) awaitCancellation()
                        sent.complete(Unit)
                    }
                }
            assertTrue(queue.offer("x".repeat(8_000)))
            sent.await()
            assertTrue(queue.offer("x".repeat(3_000)))
            yield()
            now += 30_000
            assertFalse(queue.stalled(), "eight thousand characters already sent were still counted")
            queue.close()
            writer.cancel()
        }

    /** The connection is already ending; a second verdict would start a second close. */
    @Test
    fun `a closed queue never reports a slow consumer`() {
        val queue = OutboundQueue(maxFrames = 1, maxChars = 10)
        queue.close()

        assertTrue(queue.offer("x".repeat(100)))
        assertFalse(queue.stalled())
    }
}
