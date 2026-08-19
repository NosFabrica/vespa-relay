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
 * **A SLOW TRANSPORT MUST NOT SPEND A FAST ONE'S BUDGET.**
 *
 * The fold, the stability gate and the fitness pass each used to bound their
 * dials with one `Semaphore(dialConcurrency)` covering both transports. On
 * staging that gate ran saturated at 100 with `.onion` — 10% of the candidate
 * urls, and allowed a circuit budget on top of the clearnet one by
 * [probeIdleMs] — holding 60-74% of every permit, 30-40 of them queued behind a
 * Tor dispatcher only [TorSettings.maxSockets] wide and therefore holding a
 * probe slot with no socket under it.
 *
 * The property is scheduling, not sizing, so the first test is the one that
 * fails on the old shape: a clearnet dial must be able to START while every
 * onion permit is held. It is written as a deadlock rather than as a stopwatch —
 * the clearnet job is what releases the onion jobs — because a timing assertion
 * on a loaded CI box measures the box.
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
            // Two permits each, four onions, one clearnet url — so the onions
            // over-subscribe their own gate exactly as the measured pass did.
            val gate = DialGate.over(concurrency = 2, tor = tor(maxSockets = 2))
            // Nothing completes this but the clearnet job. Under one shared gate
            // of two permits the first two onions take both, the clearnet url
            // queues behind them, and nothing ever completes it: the timeout
            // below is the old behaviour failing, not a slow machine.
            val clearnetRan = CompletableDeferred<Unit>()
            val onionsSeen = AtomicInteger()

            withTimeout(10_000) {
                coroutineScope {
                    // Launched FIRST, so they hold the permits before the
                    // clearnet job asks for one.
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
     * The same staging, on the shape this replaced — one undifferentiated
     * semaphore over both transports. It is here so the test above is known to
     * be asserting something: the clearnet job never gets a permit, so nothing
     * ever completes the onions, and the run has to be cut rather than
     * finishing. Half a second, because the outcome is a deadlock and not a
     * slow machine.
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
            // The other half of the same property: separating the two must not
            // quietly let either one run unbounded.
            val gate = DialGate.over(concurrency = 3, tor = tor(maxSockets = 2))
            val inFlight = AtomicInteger()
            val peak = AtomicInteger()
            coroutineScope {
                repeat(24) {
                    launch {
                        gate.withPermit(clearnet) {
                            val now = inFlight.incrementAndGet()
                            peak.updateAndGet { seen -> maxOf(seen, now) }
                            // A suspension point, so the permits are actually
                            // contended rather than each job running to
                            // completion before the next one starts.
                            kotlinx.coroutines.yield()
                            inFlight.decrementAndGet()
                        }
                    }
                }
            }
            assertTrue(peak.get() <= 3, "the clearnet gate let ${peak.get()} dials run against a limit of 3")
        }

    /**
     * A permit on the Tor gate has to mean a SOCKET, not a place in the Tor
     * dispatcher's queue — which is the whole of the "30-40 permits with nothing
     * behind them" half of the report. So it is sized from the dispatcher, and
     * capped by the operator's knob, which is a ceiling on the plane's whole
     * appetite rather than a clearnet-only number.
     */
    @Test
    fun `the Tor gate is the Tor dispatcher's width, and never wider than the operators knob`() {
        DialGate.over(concurrency = 100, tor = tor(maxSockets = TorSettings.DEFAULT_MAX_SOCKETS)).let {
            assertEquals(100, it.clearnetPermits)
            assertEquals(32, it.torPermits)
        }
        // …and a deployment that asked for four dials did not ask for
        // thirty-two onion ones.
        DialGate.over(concurrency = 4, tor = tor(maxSockets = 32)).let {
            assertEquals(4, it.clearnetPermits)
            assertEquals(4, it.torPermits)
        }
    }

    /**
     * TWO EQUAL NUMBERS IS NOT ONE NUMBER. At `dialConcurrency = 16` against
     * the default 32 sockets both gates are 16, and a line that collapsed to
     * "16 dial(s)" there would read exactly like the no-Tor deployment while
     * the real ceiling is 32. The boot line is the only place an operator sees
     * this split, so it has to say which shape it is.
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
        // A `.onion` on a deployment with no Tor is a url that will fail, and it
        // fails on the clearnet gate: there is no second transport to charge it
        // to. Asserted by saturating the only gate there is.
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
     * Under `SYNC_TOR_ALL` every url is dialled through the proxy, so every url
     * must wait on the proxy's permits. The gate asks [TorTransport.routes] —
     * the same predicate that picks the OkHttp client — precisely so the gate a
     * url waits on and the dispatcher it lands in cannot disagree.
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
