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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * One permit gate per transport, so a slow one cannot spend a fast one's budget.
 * Written as deadlocks, not stopwatches: a timing assertion on a loaded CI box measures the box.
 */
class DialGateTest {
    private val onions =
        (0 until 4).map {
            RelayUrlNormalizer.normalize("ws://vespa${it}iexampleonionaddressthatisnotreal7abcdefghij.onion")
        }
    private val clearnet = RelayUrlNormalizer.normalize("wss://relay.example.com")

    private fun tor(
        maxSockets: Int,
        routeAll: Boolean = false,
    ) = TorTransport(
        TorSettings(
            socksHost = "127.0.0.1",
            socksPort = 9050,
            routeAll = routeAll,
            connectTimeoutSec = 90,
            maxSockets = maxSockets,
        ),
        OkHttpClient(),
    )

    @Test
    fun `onions holding every Tor permit cannot stop a clearnet dial starting`() =
        runBlocking {
            // Two permits each, four onions, one clearnet url: the onions over-subscribe their own gate.
            val gate = DialGate.over(concurrency = 2, tor = tor(maxSockets = 2))
            // Only the clearnet job completes this. Under one shared gate it queues
            // behind the onions and the timeout below is the old behaviour failing.
            val clearnetRan = CompletableDeferred<Unit>()
            val onionsSeen = AtomicInteger()

            withTimeout(10_000) {
                coroutineScope {
                    // Launched first, so they hold the permits before the clearnet job asks for one.
                    for (url in onions) {
                        launch {
                            gate.withPermit(url) {
                                onionsSeen.incrementAndGet()
                                clearnetRan.await()
                            }
                        }
                    }
                    launch {
                        gate.withPermit(clearnet) {
                            clearnetRan.complete(Unit)
                        }
                    }
                }
            }
            assertEquals(4, onionsSeen.get(), "every onion should still have been probed — this splits the budget, it does not drop work")
        }

    /**
     * The shape this replaced, one semaphore over both transports, so the test
     * above is known to assert something. Half a second, because the outcome is a deadlock.
     */
    @Test
    fun `one gate over both transports is what deadlocks`() =
        runBlocking {
            val shared = DialGate(clearnetPermits = 2, torPermits = 2, routesTor = { false })
            val clearnetRan = CompletableDeferred<Unit>()
            val finished =
                withTimeoutOrNull(500) {
                    coroutineScope {
                        for (url in onions) {
                            launch { shared.withPermit(url) { clearnetRan.await() } }
                        }
                        launch { shared.withPermit(clearnet) { clearnetRan.complete(Unit) } }
                    }
                    true
                }
            assertNull(finished, "two permits shared between five urls should not have got the clearnet one started")
        }

    @Test
    fun `the clearnet gate still bounds clearnet dials`() =
        runBlocking {
            val gate = DialGate.over(concurrency = 3, tor = tor(maxSockets = 2))
            val inFlight = AtomicInteger()
            val peak = AtomicInteger()
            coroutineScope {
                repeat(24) {
                    launch {
                        gate.withPermit(clearnet) {
                            val now = inFlight.incrementAndGet()
                            peak.updateAndGet { seen -> maxOf(seen, now) }
                            // A suspension point, so the permits are contended rather than each job running to completion.
                            kotlinx.coroutines.yield()
                            inFlight.decrementAndGet()
                        }
                    }
                }
            }
            assertTrue(peak.get() <= 3, "the clearnet gate let ${peak.get()} dials run against a limit of 3")
        }

    /**
     * A Tor permit has to mean a socket, not a place in the Tor dispatcher's
     * queue, so it is sized from the dispatcher and capped by the operator's knob.
     */
    @Test
    fun `the Tor gate is the Tor dispatcher's width, and never wider than the operators knob`() {
        DialGate.over(concurrency = 100, tor = tor(maxSockets = TorSettings.DEFAULT_MAX_SOCKETS)).let {
            assertEquals(100, it.clearnetPermits)
            assertEquals(32, it.torPermits)
        }
        DialGate.over(concurrency = 4, tor = tor(maxSockets = 32)).let {
            assertEquals(4, it.clearnetPermits)
            assertEquals(4, it.torPermits)
        }
    }

    /**
     * At `dialConcurrency = 16` against the default 32 sockets both gates are 16,
     * and the boot line is the only place an operator sees the split.
     */
    @Test
    fun `the boot line tells a proxied gate from an unproxied one, even at the same width`() {
        assertEquals(
            "16 clearnet dial(s), 16 over Tor",
            DialGate.over(concurrency = 16, tor = tor(maxSockets = 32)).describe(),
        )
        assertEquals("16 dial(s)", DialGate.over(concurrency = 16, tor = null).describe())
        assertEquals(
            "100 clearnet dial(s), 32 over Tor",
            DialGate.over(concurrency = 100, tor = tor(maxSockets = 32)).describe(),
        )
    }

    @Test
    fun `no proxy means one gate, exactly as before`() {
        val gate = DialGate.over(concurrency = 16, tor = null)
        assertEquals(16, gate.clearnetPermits)
        assertEquals("16 dial(s)", gate.describe())
        // A `.onion` with no Tor fails on the clearnet gate: there is no second transport to charge it to.
        runBlocking {
            val one = DialGate.over(concurrency = 1, tor = null)
            val order = mutableListOf<String>()
            coroutineScope {
                launch { one.withPermit(onions.first()) { order += "onion" } }
                launch { one.withPermit(clearnet) { order += "clearnet" } }
            }
            assertEquals(listOf("onion", "clearnet"), order, "one permit, so the second job waited for the first")
        }
    }

    /**
     * The gate asks [TorTransport.routes], the predicate that picks the OkHttp
     * client, so the gate a url waits on and the dispatcher it lands in cannot disagree.
     */
    @Test
    fun `SYNC_TOR_ALL puts clearnet urls on the Tor gate too`() =
        runBlocking {
            val gate = DialGate.over(concurrency = 8, tor = tor(maxSockets = 1, routeAll = true))
            assertEquals(1, gate.torPermits)
            val order = mutableListOf<String>()
            coroutineScope {
                launch { gate.withPermit(onions.first()) { order += "onion" } }
                launch { gate.withPermit(clearnet) { order += "clearnet" } }
            }
            assertEquals(listOf("onion", "clearnet"), order, "routeAll means the clearnet url queued behind the onion on Tor's single permit")
        }
}
