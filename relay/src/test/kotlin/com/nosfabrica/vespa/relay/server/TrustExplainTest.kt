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

import io.ktor.client.request.get
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** `/trust/explain/{pubkey}` is anonymous, so what one caller can make the store do is bounded. */
class TrustExplainTest {
    private val key = "ab".repeat(32)

    @Test
    fun `asking about one pubkey again within the ttl costs no store read`(): Unit =
        runBlocking {
            val reads = AtomicInteger(0)
            var clock = 0L
            val gate = ExplainGate({ k -> buildJsonObject { put("n", reads.incrementAndGet()) } }, ttlMillis = 30_000, now = { clock })
            repeat(5) { gate.answer(key) }
            gate.answer(key.uppercase())
            assertEquals(1, reads.get(), "one read for one pubkey, whatever its case")
            clock = 31_000
            gate.answer(key)
            assertEquals(2, reads.get(), "and a fresh one once the answer is stale")
        }

    @Test
    fun `a malformed key is never cached, so junk cannot evict real answers`(): Unit =
        runBlocking {
            val reads = AtomicInteger(0)
            val gate = ExplainGate({ buildJsonObject { put("n", reads.incrementAndGet()) } }, maxEntries = 1)
            gate.answer(key)
            repeat(3) { gate.answer("not-a-key-$it") }
            gate.answer(key)
            assertEquals(4, reads.get(), "the real answer stayed cached through the junk")
        }

    @Test
    fun `past the bound a caller is told to retry rather than queued`(): Unit =
        runBlocking {
            val parked = CompletableDeferred<Unit>()
            val entered = AtomicInteger(0)
            val gate =
                ExplainGate({
                    entered.incrementAndGet()
                    parked.await()
                    buildJsonObject { put("ok", true) }
                }, maxInFlight = 1)
            val first = async { gate.answer("cd".repeat(32)) }
            withTimeout(5_000) { while (entered.get() == 0) delay(5) }
            assertNull(gate.answer(key), "a second pubkey while the slot is taken is refused")
            parked.complete(Unit)
            first.await()
            assertEquals(1, entered.get())
        }

    @Test
    fun `a refused ask is a 503 with a retry hint`() =
        testApplication {
            val parked = CompletableDeferred<Unit>()
            val entered = AtomicInteger(0)
            val gate =
                ExplainGate({
                    entered.incrementAndGet()
                    parked.await()
                    buildJsonObject { put("ok", true) }
                }, maxInFlight = 1)
            application { routing { trustHealth(null, null, gate) } }
            coroutineScope {
                val first = async { client.get("/trust/explain/${"cd".repeat(32)}") }
                withTimeout(5_000) { while (entered.get() == 0) delay(5) }
                val busy = client.get("/trust/explain/$key")
                assertEquals(HttpStatusCode.ServiceUnavailable, busy.status)
                assertEquals("1", busy.headers[HttpHeaders.RetryAfter])
                parked.complete(Unit)
                assertEquals(HttpStatusCode.OK, first.await().status)
            }
        }
}
