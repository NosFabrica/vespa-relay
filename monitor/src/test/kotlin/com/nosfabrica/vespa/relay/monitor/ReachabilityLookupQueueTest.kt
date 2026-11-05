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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.Socket
import java.net.UnknownHostException
import java.util.concurrent.CountDownLatch
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A lookup's budget starts when a thread runs it, not when it is submitted: the pre-probe asks for every url
 * of a pass at once, and a live host queued behind dead names is not UNEXPLAINED for the wait. A resolver
 * that never returns costs the lookups behind it a bounded wait, never a hang.
 */
class ReachabilityLookupQueueTest {
    private fun url(raw: String) = RelayUrlNormalizer.normalize(raw)

    @Test
    fun `a live host's lookup is not charged the wait behind other hosts' slow lookups`(): Unit =
        runBlocking {
            val probe =
                ReachabilityProbe(
                    tor = null,
                    threads = 2,
                    resolve = { host ->
                        if (host.startsWith("dead")) {
                            Thread.sleep(3_000)
                            throw UnknownHostException("$host: Temporary failure in name resolution")
                        }
                        arrayOf(InetAddress.getByAddress(host, byteArrayOf(127, 0, 0, 1)))
                    },
                    // Any socket: the connect is not what is under test.
                    connect = { _, _ -> Socket() },
                    lookupMs = 1_000,
                )
            // Two dead names take both lookup threads, as a sweep's dead relays do.
            val dead = (1..2).map { async(Dispatchers.Default) { probe.reach(url("wss://dead$it.example")) } }
            delay(200)
            val live = probe.reach(url("wss://live.example"))
            dead.awaitAll()
            assertEquals(Reach.REACHABLE, live, "the resolver answered live.example at once; only the queue was slow")
        }

    @Test
    fun `a resolver that never returns costs later lookups a bounded wait, and frees its thread when it does`(): Unit =
        runBlocking {
            val hung = CountDownLatch(1)
            val probe =
                ReachabilityProbe(
                    tor = null,
                    threads = 2,
                    resolve = { host ->
                        if (host.startsWith("hung")) hung.await()
                        arrayOf(InetAddress.getByAddress(host, byteArrayOf(127, 0, 0, 1)))
                    },
                    connect = { _, _ -> Socket() },
                    lookupMs = 200,
                    starvedMs = 500,
                )
            try {
                // Both threads blocked in the resolver for good, as a truly hung getAllByName holds them.
                for (i in 1..2) assertEquals(Reach.UNEXPLAINED, probe.reach(url("wss://hung$i.example")))
                val startedMs = System.currentTimeMillis()
                val starved = (1..20).map { async(Dispatchers.Default) { probe.reach(url("wss://waits$it.example")) } }.awaitAll()
                assertEquals(List(20) { Reach.UNEXPLAINED }, starved, "a lookup that never got a thread is our side, never an answer")
                assertTrue(System.currentTimeMillis() - startedMs < 5_000, "a starved pool held its waiters past the starvation window")
            } finally {
                hung.countDown()
            }
            assertEquals(Reach.REACHABLE, probe.reach(url("wss://after.example")), "the threads did not come back once the resolver returned")
        }
}
