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

import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * A live feed's mean reaches the pressure instance, and a feed that dies or
 * never connects resets it rather than throttling on a number from the past.
 */
class PressurePollerTest {
    private fun awaitTrue(
        what: String,
        // A miss can cost the poller's whole request timeout, and three are needed.
        timeoutMs: Long = 30_000,
        condition: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            Thread.sleep(20)
        }
        fail("timed out waiting for $what")
    }

    private fun pressureServer(
        meanMs: Long,
        samples: Long,
    ): HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/pressure") { exchange ->
                val body = """{"meanMs":$meanMs,"samples":$samples}""".toByteArray()
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            start()
        }

    @Test
    fun `a served mean is adopted, and a feed that dies resets the throttle`() {
        val server = pressureServer(meanMs = 5_000, samples = ServingPressure.MIN_SAMPLES.toLong())
        val pressure = ServingPressure(thresholdMs = 2_000)
        val poller = PressurePoller("http://127.0.0.1:${server.address.port}/pressure", pressure, intervalMs = 25).start()
        try {
            awaitTrue("the served mean to be adopted") {
                pressure.meanMs() == 5_000L && pressure.backoffMs() > 0
            }

            server.stop(0)

            // Three consecutive misses clear it: a relay that is down has no clients to protect.
            awaitTrue("the reset after the feed died") {
                pressure.backoffMs() == 0L && pressure.sampleCount() == 0L
            }
        } finally {
            poller.close()
            server.stop(0)
        }
    }

    @Test
    fun `a feed that never yields a sample resets instead of standing on a stale claim`() {
        // A 503 rather than a closed port: a non-200 is a deterministic miss in every environment.
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/pressure") { exchange ->
                    exchange.sendResponseHeaders(503, -1)
                    exchange.close()
                }
                start()
            }
        val pressure = ServingPressure(thresholdMs = 2_000)
        // Pre-loaded, so the never-fed path must actively clear it.
        pressure.adopt(10_000, 100)
        val poller = PressurePoller("http://127.0.0.1:${server.address.port}/pressure", pressure, intervalMs = 25).start()
        try {
            awaitTrue("the reset after three failed polls") {
                pressure.backoffMs() == 0L && pressure.sampleCount() == 0L
            }
            assertEquals(0, pressure.meanMs())
        } finally {
            poller.close()
            server.stop(0)
        }
    }
}
