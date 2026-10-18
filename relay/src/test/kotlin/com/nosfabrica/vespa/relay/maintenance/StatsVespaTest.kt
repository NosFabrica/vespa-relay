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
package com.nosfabrica.vespa.relay.maintenance

import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.InetSocketAddress
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

/** What the stats tiers see of an engine that accepted a query and never answered it. */
class StatsVespaTest {
    @Test
    fun `an engine that stops answering fails the query instead of holding the tier`() {
        val never = CountDownLatch(1)
        val engine =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/search/") { never.await() }
                executor = Executors.newCachedThreadPool()
                start()
            }
        try {
            val vespa = StatsVespa("http://127.0.0.1:${engine.address.port}", requestTimeout = Duration.ofMillis(300))
            runBlocking {
                withTimeout(10_000) {
                    val failed = assertFailsWith<StatsQueryFailed> { vespa.group(StatsYql.TOTAL) }
                    assertContains(failed.message.orEmpty(), HttpTimeoutException::class.simpleName!!)
                }
            }
        } finally {
            never.countDown()
            engine.stop(0)
        }
    }

    /** `/stats.json` is public; a proxy's error page or the engine's own body names hosts and ports. */
    @Test
    fun `what the engine or a proxy answered never reaches the published message`() {
        val secret = "content-node-7.internal:19107"
        for ((status, body) in listOf(
            200 to "<html>bad gateway at $secret</html>",
            200 to """["not an object", "$secret"]""",
            200 to """{"no-root": "$secret"}""",
            502 to "upstream $secret refused",
        )) {
            val engine =
                HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                    createContext("/search/") { exchange ->
                        val bytes = body.encodeToByteArray()
                        exchange.sendResponseHeaders(status, bytes.size.toLong())
                        exchange.responseBody.use { it.write(bytes) }
                    }
                    start()
                }
            try {
                val failed = assertFailsWith<StatsQueryFailed> { runBlocking { StatsVespa("http://127.0.0.1:${engine.address.port}").group(StatsYql.TOTAL) } }
                val published = publicReason(failed)
                assertFalse(secret in published, "leaked: $published")
                assertContains(published, "engine detail in the relay log")
            } finally {
                engine.stop(0)
            }
        }
    }

    @Test
    fun `an engine that cannot be reached is named by the failure's class, not its message`() {
        val closed = java.net.ServerSocket(0).use { it.localPort }
        val failed = assertFailsWith<StatsQueryFailed> { runBlocking { StatsVespa("http://127.0.0.1:$closed").group(StatsYql.TOTAL) } }
        assertFalse("127.0.0.1" in publicReason(failed), publicReason(failed))
        assertContains(publicReason(failed), "did not answer")
        assertEquals("IllegalArgumentException (detail in the relay log)", publicReason(IllegalArgumentException("/var/lib/secret")))
    }
}
