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
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals

class HttpRelayGateTest {
    /** Holds a slot until [release] completes; answers how the gate decided. */
    private fun kotlinx.coroutines.CoroutineScope.hold(
        gate: HttpRelayGate,
        release: CompletableDeferred<Unit>,
    ) = async { gate.through(refused = { "refused" }) { release.await().let { "ran" } } }

    private suspend fun HttpRelayGate.tryOnce() = through(refused = { "refused" }) { "ran" }

    @Test
    fun `the relay is capped at its total`() =
        runBlocking {
            val gate = HttpRelayGate(total = 3)
            val release = CompletableDeferred<Unit>()
            val held = (1..3).map { hold(gate, release) }
            while (gate.inFlight < 3) yield()

            assertEquals("refused", gate.tryOnce(), "a fourth command past the total")

            release.complete(Unit)
            assertEquals(listOf("ran", "ran", "ran"), held.map { it.await() })
            assertEquals(0, gate.inFlight)
            assertEquals("ran", gate.tryOnce(), "every slot frees when its command ends")
        }

    @Test
    fun `zero lifts the cap`() =
        runBlocking {
            val gate = HttpRelayGate(total = 0)
            val release = CompletableDeferred<Unit>()
            val held = (1..10).map { hold(gate, release) }
            while (gate.inFlight < 10) yield()
            assertEquals("ran", async { gate.tryOnce() }.await())
            release.complete(Unit)
            held.forEach { it.await() }
        }

    @Test
    fun `a command that throws still frees its slot`() =
        runBlocking {
            val gate = HttpRelayGate(total = 1)
            runCatching { gate.through(refused = { "refused" }) { error("boom") } }
            assertEquals("ran", gate.tryOnce())
        }
}
