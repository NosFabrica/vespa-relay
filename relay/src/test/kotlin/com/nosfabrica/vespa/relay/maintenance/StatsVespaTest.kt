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
import kotlin.test.assertFailsWith

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
                    assertFailsWith<HttpTimeoutException> { vespa.group(StatsYql.TOTAL) }
                }
            }
        } finally {
            never.countDown()
            engine.stop(0)
        }
    }
}
