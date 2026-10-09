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

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** What a client that stops reading can make this relay hold for it. */
class OutboundQueueTest {
    @Test
    fun `a few large frames trip the slow-consumer bound long before the frame count does`() {
        val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000)
        val event = "x".repeat(4_000)

        assertTrue(queue.offer(event))
        assertTrue(queue.offer(event))
        assertFalse(queue.offer(event), "three frames of a thousand allowed, but twelve thousand characters of ten")
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
            val queue = OutboundQueue(maxFrames = 1_000, maxChars = 10_000)
            val event = "x".repeat(4_000)
            val sent = CompletableDeferred<Unit>()
            assertTrue(queue.offer(event))
            assertTrue(queue.offer(event))

            var written = 0
            val writer =
                launch {
                    queue.drain {
                        if (++written == 2) sent.complete(Unit)
                    }
                }
            sent.await()
            assertTrue(queue.offer(event), "the frame already sent no longer counts")
            queue.close()
            writer.join()
        }

    /** The connection is already ending; a second verdict would start a second close. */
    @Test
    fun `a closed queue never reports a slow consumer`() {
        val queue = OutboundQueue(maxFrames = 1, maxChars = 10)
        queue.close()

        assertTrue(queue.offer("x".repeat(100)))
    }
}
