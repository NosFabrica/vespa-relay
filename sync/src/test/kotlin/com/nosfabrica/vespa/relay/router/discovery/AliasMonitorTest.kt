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
package com.nosfabrica.vespa.relay.router.discovery

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The monitor is what keeps a probe pass off a sync cycle, so what matters
 * about it is bookkeeping rather than folding: what it holds between passes,
 * and what it does when one stream's pass blows up.
 */
class AliasMonitorTest {
    private val a = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val b = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val c = RelayUrlNormalizer.normalize("wss://nos.lol/delta-echo")

    /** Records what each pass was asked to measure, and can be told to fail. */
    private class Recording(
        private val throwOn: Map<String, () -> Throwable> = emptyMap(),
    ) : AliasMonitor.Pass {
        val passes = mutableListOf<Pair<String, List<NormalizedRelayUrl>>>()
        val calls = AtomicInteger()

        override suspend fun measure(
            label: String,
            candidates: List<NormalizedRelayUrl>,
            canDial: suspend (NormalizedRelayUrl) -> Boolean,
            onEvent: suspend (Event) -> Unit,
        ): Int {
            calls.incrementAndGet()
            passes += label to candidates
            throwOn[label]?.let { throw it() }
            return 0
        }
    }

    private fun monitor(pass: AliasMonitor.Pass) = AliasMonitor(pass, CoroutineScope(Dispatchers.Unconfined))

    @Test
    fun `a stream's later submission replaces its earlier one`() =
        runBlocking {
            // A stream re-discovers its whole world every cycle. Appending would
            // make the monitor re-read the same urls once per cycle forever.
            val pass = Recording()
            val m = monitor(pass)
            m.submit("outbox", listOf(a, b), canDial = { true })
            m.submit("outbox", listOf(a, b, c), canDial = { true })

            m.runPass()

            assertEquals(1, pass.passes.size, "a stream was measured more than once in a single pass")
            assertEquals(listOf(a, b, c), pass.passes.single().second)
        }

    @Test
    fun `each stream keeps its own work`() =
        runBlocking {
            val pass = Recording()
            val m = monitor(pass)
            m.submit("outbox", listOf(a, b), canDial = { true })
            m.submit("assertions", listOf(a, c), canDial = { true })

            m.runPass()

            assertEquals(setOf("outbox", "assertions"), pass.passes.map { it.first }.toSet())
        }

    @Test
    fun `one stream's failure does not skip the streams after it`() =
        runBlocking {
            // A probe pass talks to arbitrary third-party relays; any of them
            // can fail in ways this cannot enumerate. Losing the rest of the
            // pass to one of them is how a fold silently stops progressing.
            val pass = Recording(throwOn = mapOf("outbox" to { IllegalStateException("upstream went away") }))
            val m = monitor(pass)
            m.submit("outbox", listOf(a, b), canDial = { true })
            m.submit("assertions", listOf(a, c), canDial = { true })

            m.runPass()

            assertEquals(2, pass.calls.get(), "the pass stopped at the failing stream")
        }

    @Test
    fun `cancellation ends the pass rather than being logged`() =
        runBlocking {
            // Swallowing this would keep the monitor grinding through every
            // remaining stream after the scope has been told to stop.
            val pass =
                Recording(
                    throwOn =
                        mapOf(
                            "outbox" to { CancellationException("scope closing") },
                            "assertions" to { CancellationException("scope closing") },
                        ),
                )
            val m = monitor(pass)
            m.submit("outbox", listOf(a, b), canDial = { true })
            m.submit("assertions", listOf(a, c), canDial = { true })

            assertFailsWith<CancellationException> { m.runPass() }
            assertEquals(1, pass.calls.get(), "the pass continued past a cancellation")
        }

    @Test
    fun `a single url is not worth a pass`() =
        runBlocking {
            val pass = Recording()
            val m = monitor(pass)
            m.submit("outbox", listOf(a), canDial = { true })

            m.runPass()

            assertTrue(pass.passes.isEmpty(), "a lone url has nothing to be compared against")
        }
}
