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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

/** The live gate's reads for cold authors: bounded across subscriptions, and never silent when they drop an event. */
class ColdAuthorChecksTest {
    @Test
    fun `a burst of cold authors runs at most the bound of store reads, and the rest are counted`(): Unit =
        runBlocking {
            val said = Collections.synchronizedList(mutableListOf<String>())
            val checks = ColdAuthorChecks(max = 3, log = { said += it })
            val running = AtomicInteger(0)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                repeat(10) {
                    checks.start(scope, {
                        running.incrementAndGet()
                        awaitCancellation()
                    }) { error("a check that never answers delivered") }
                }
                withTimeout(5_000) { while (running.get() < 3) delay(5) }
                delay(50)
                assertEquals(3, running.get(), "no more reads than the bound")
                assertEquals(7, checks.shedding)
                assertEquals(1, said.size, "said once, not once per event: $said")
                assertContains(said[0], "shed past 3 in flight")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a slot frees when its check ends, and an admitted event is delivered`(): Unit =
        runBlocking {
            val checks = ColdAuthorChecks(max = 1, log = {})
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                repeat(3) {
                    val delivered = CompletableDeferred<Unit>()
                    checks.start(scope, { true }) { delivered.complete(Unit) }
                    withTimeout(5_000) { delivered.await() }
                    // The slot is released on completion, just after send.
                    delay(20)
                }
                assertEquals(0, checks.shedding, "one at a time never meets the bound")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a failed check drops its event, is counted and said, and cancellation is neither`(): Unit =
        runBlocking {
            val said = Collections.synchronizedList(mutableListOf<String>())
            var clock = 0L
            val checks = ColdAuthorChecks(max = 8, everyMillis = 60_000, now = { clock }, log = { said += it })
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val ended = AtomicInteger(0)
                repeat(2) {
                    checks.start(scope, {
                        try {
                            throw IllegalStateException("store timed out")
                        } finally {
                            ended.incrementAndGet()
                        }
                    }) { error("a failed check delivered") }
                }
                withTimeout(5_000) { while (ended.get() < 2 || said.isEmpty()) delay(5) }
                delay(50)
                assertEquals(2, checks.failures)
                assertEquals(1, said.size, "throttled: $said")
                assertContains(said[0], "IllegalStateException")

                val parked = CompletableDeferred<Unit>()
                val doomed = CoroutineScope(SupervisorJob() + Dispatchers.Default)
                checks.start(doomed, {
                    parked.complete(Unit)
                    awaitCancellation()
                }) { error("a cancelled check delivered") }
                withTimeout(5_000) { parked.await() }
                doomed.cancel()
                delay(50)
                assertEquals(2, checks.failures, "a subscription closing is not a failed check")

                clock = 61_000
                checks.start(scope, { error("again") }) { }
                withTimeout(5_000) { while (said.size < 2) delay(5) }
                assertEquals(3, checks.failures)
                assertContains(said[1], "3 cold-author check(s) failed", message = "said again once the window passed")
            } finally {
                scope.cancel()
            }
        }
}
