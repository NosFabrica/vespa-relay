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
package com.nosfabrica.vespa.relay.sync

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A periodic job outlives a bad tick and ends only on cancellation; a shutdown runs every step. */
class GuardsTest {
    @Test
    fun `a shutdown step that throws does not keep the next from running`() {
        val ran = mutableListOf<String>()
        shutdownStep("close the engine") {
            ran += "engine"
            error("the client pool would not close")
        }
        shutdownStep("close the bands") { ran += "bands" }
        shutdownStep("close the store") { ran += "store" }
        assertEquals(listOf("engine", "bands", "store"), ran, "the state files still get their last write")
    }

    @Test
    fun `a tick that throws does not end the loop`() =
        runBlocking {
            val ticks = AtomicInteger()
            val job =
                CoroutineScope(SupervisorJob()).launch {
                    repeatEvery(5, "test loop") {
                        when (ticks.incrementAndGet()) {
                            1 -> error("one bad tick")
                            2 -> throw CancellationException("a stray cancellation, not a shutdown")
                        }
                    }
                }
            withTimeout(5_000) { while (ticks.get() < 4) delay(5) }
            assertTrue(job.isActive, "the loop must still be running after a failed tick")

            job.cancelAndJoin()
            val stopped = ticks.get()
            delay(50)
            assertEquals(stopped, ticks.get(), "cancellation, and only cancellation, ends it")
        }
}
