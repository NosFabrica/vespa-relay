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

class HttpReadGateTest {
    /** Holds a slot for [client] until [release] completes; answers how the gate decided. */
    private fun kotlinx.coroutines.CoroutineScope.hold(
        gate: HttpReadGate,
        client: String,
        release: CompletableDeferred<Unit>,
    ) = async { gate.through(client, refused = { it.name }) { release.await().let { "ran" } } }

    private suspend fun HttpReadGate.tryOnce(client: String) = through(client, refused = { it.name }) { "ran" }

    @Test
    fun `one client is capped at its share and the relay at its total`() =
        runBlocking {
            val gate = HttpReadGate(perClient = 2, total = 3)
            val release = CompletableDeferred<Unit>()
            val held = listOf(hold(gate, "a", release), hold(gate, "a", release), hold(gate, "b", release))
            while (gate.inFlight < 3) yield()

            assertEquals("CLIENT_BUSY", gate.tryOnce("a"), "a third read from one address")
            assertEquals("RELAY_BUSY", gate.tryOnce("c"), "a fresh address past the total")

            release.complete(Unit)
            assertEquals(listOf("ran", "ran", "ran"), held.map { it.await() })
            assertEquals(0, gate.inFlight)
            assertEquals("ran", gate.tryOnce("a"), "every slot frees when its read ends")
        }

    @Test
    fun `zero lifts a cap`() =
        runBlocking {
            val gate = HttpReadGate(perClient = 0, total = 0)
            val release = CompletableDeferred<Unit>()
            val held = (1..10).map { hold(gate, "a", release) }
            while (gate.inFlight < 10) yield()
            assertEquals("ran", async { gate.tryOnce("a") }.await())
            release.complete(Unit)
            held.forEach { it.await() }
        }

    @Test
    fun `a read that throws still frees its slot`() =
        runBlocking {
            val gate = HttpReadGate(perClient = 1, total = 1)
            runCatching { gate.through("a", refused = { "refused" }) { error("boom") } }
            assertEquals("ran", gate.tryOnce("a"))
        }
}
