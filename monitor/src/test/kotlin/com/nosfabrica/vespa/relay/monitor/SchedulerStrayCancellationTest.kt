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
package com.nosfabrica.vespa.relay.monitor

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A CancellationException does not mean the scheduler's own scope is closing: awaiting a Deferred someone
 * else cancelled throws one into a live coroutine. That is one failed pass, never the end of the scheduler.
 */
class SchedulerStrayCancellationTest {
    private val a = RelayUrlNormalizer.normalize("wss://nos.lol")

    @Test
    fun `a cancellation the scheduler's own scope did not ask for does not end sweeping`(): Unit =
        runBlocking {
            val sweeps = AtomicInteger()
            val pass =
                AliasMonitor.Pass { _, _ ->
                    if (sweeps.incrementAndGet() == 1) {
                        // A shared read another caller abandoned: awaiting it throws CancellationException here.
                        val shared = CompletableDeferred<Int>()
                        shared.cancel()
                        shared.await()
                    }
                    0
                }
            val source =
                object : AliasMonitor.CandidateSource {
                    override suspend fun candidates(): List<NormalizedRelayUrl> = listOf(a)
                }
            val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            try {
                AliasMonitor(listOf(pass), scope, intervalMs = 20, startupDelayMs = 0, emptyRetryMs = 20, source = source, retryEveryMs = null).start()
                withTimeoutOrNull(3_000) { while (sweeps.get() < 2) delay(10) }
            } finally {
                scope.cancel()
            }
            assertTrue(sweeps.get() >= 2, "the scheduler died on a stray CancellationException after ${sweeps.get()} sweep(s); nothing measures again")
        }

    @Test
    fun `a stray cancellation in one pass does not cost the passes after it`(): Unit =
        runBlocking {
            val ran = CopyOnWriteArrayList<String>()
            val first =
                AliasMonitor.Pass { _, _ ->
                    ran += "first"
                    CompletableDeferred<Int>().apply { cancel() }.await()
                }
            val second = AliasMonitor.Pass { _, _ -> 0.also { ran += "second" } }
            val source =
                object : AliasMonitor.CandidateSource {
                    override suspend fun candidates(): List<NormalizedRelayUrl> = listOf(a)
                }
            val m = AliasMonitor(listOf(first, second), CoroutineScope(Dispatchers.Unconfined), source = source)
            // The caller is active throughout: this cancellation is not its own.
            runCatching { m.runPass() }
            assertEquals(listOf("first", "second"), ran.toList(), "a pass's stray CancellationException skipped every pass after it")
        }
}
