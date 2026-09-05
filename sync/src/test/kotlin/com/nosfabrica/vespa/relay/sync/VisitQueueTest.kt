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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The rotation invariants. Each visit announces itself and waits to be
 * released, so the interleavings under test are staged, not hoped for.
 */
class VisitQueueTest {
    private val url = RelayUrlNormalizer.normalize("wss://a.example")

    @Test
    fun `disarming a revisit lets the next completion arm the cadence the url now has`() =
        runBlocking {
            // The delay is read once, when the timer is armed.
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<NormalizedRelayUrl>(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            val tailed = AtomicInteger(1)
            val visits = AtomicInteger()
            scope.launch {
                q.visitLoop(
                    stillWanted = { true },
                    revisitDelayMs = { if (tailed.get() == 1) 3_600_000L else 150L },
                ) {
                    visits.incrementAndGet()
                    entered.send(Unit)
                }
            }
            try {
                withTimeout(10_000) {
                    q.offer(url)
                    entered.receive()
                    // Visit one is done and an hour-long timer is armed.
                    delay(200)
                    assertEquals(1, visits.get())

                    // Eviction: the cadence is now the short one.
                    tailed.set(0)
                    q.disarm(url)
                    q.offer(url)
                    entered.receive()

                    // The completion after eviction arms the untailed cadence.
                    entered.receive()
                    assertEquals(3, visits.get())
                }
            } finally {
                scope.cancel()
            }
        }

    /**
     * `launch` parents a job at creation and LAZY defers only the body, so the timer that loses
     * `putIfAbsent` must be cancelled, not dropped. The race is staged by blocking `revisitDelayMs`.
     */
    @Test
    fun `a revisit timer that loses the slot is cancelled, not left parented forever`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val q = VisitQueue<NormalizedRelayUrl>(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            val inArmRevisit = Channel<Unit>(Channel.UNLIMITED)
            val release = java.util.concurrent.CountDownLatch(1)
            val armCalls = AtomicInteger()
            repeat(2) {
                scope.launch {
                    q.visitLoop(
                        stillWanted = { true },
                        revisitDelayMs = {
                            // The first worker to finish parks here, before the slot is claimed.
                            if (armCalls.incrementAndGet() == 1) {
                                inArmRevisit.trySend(Unit)
                                release.await(10, java.util.concurrent.TimeUnit.SECONDS)
                            }
                            3_600_000L
                        },
                    ) { entered.send(Unit) }
                }
            }
            try {
                withTimeout(20_000) {
                    q.offer(url)
                    entered.receive()
                    // Worker A is now held inside armRevisit with no slot taken.
                    inArmRevisit.receive()

                    // Worker B visits and arms the url, winning the slot A is about to ask for.
                    q.offer(url)
                    entered.receive()
                    while (armCalls.get() < 2) delay(10)
                    delay(100)

                    // A asks, and loses.
                    release.countDown()
                    delay(300)

                    // Two worker loops plus the one timer that won. `isCompleted`, not `isActive`:
                    // a LAZY job never started is New, which `isActive` would hide.
                    val children = scope.coroutineContext[Job]!!.children.count { !it.isCompleted }
                    assertEquals(
                        3,
                        children,
                        "the timer that lost the slot is still parented to the scope — one leaked Job per lost race",
                    )
                }
            } finally {
                release.countDown()
                scope.cancel()
            }
        }

    @Test
    fun `disarming a url with nothing armed is a no-op`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                VisitQueue<NormalizedRelayUrl>(scope).disarm(url)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a requeue during a visit is parked and re-sent the moment it finishes`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<NormalizedRelayUrl>(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            val release = Channel<Unit>(Channel.UNLIMITED)
            val visits = AtomicInteger()
            repeat(2) {
                scope.launch {
                    q.visitLoop(stillWanted = { true }, revisitDelayMs = { 3_600_000L }) {
                        visits.incrementAndGet()
                        entered.send(Unit)
                        release.receive()
                    }
                }
            }
            try {
                withTimeout(10_000) {
                    q.offer(url)
                    entered.receive()
                    // A rebuild wants the running url again; the second worker draws it and parks it.
                    assertTrue(q.offer(url), "a running url can be wanted again")
                    delay(200)
                    assertEquals(1, visits.get(), "the collision must not start a second concurrent visit")
                    release.send(Unit)
                    // The parked requeue comes back as the next visit, not after the revisit timer.
                    entered.receive()
                    release.send(Unit)
                    assertEquals(2, visits.get())
                }
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `offering a waiting url twice is one visit`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<NormalizedRelayUrl>(scope)
            val visits = AtomicInteger()
            assertTrue(q.offer(url))
            assertFalse(q.offer(url), "already waiting — the offer dedups")
            scope.launch {
                q.visitLoop(stillWanted = { true }, revisitDelayMs = { 3_600_000L }) {
                    visits.incrementAndGet()
                }
            }
            try {
                withTimeout(10_000) {
                    while (visits.get() < 1) delay(10)
                    delay(200)
                    assertEquals(1, visits.get())
                }
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a url nobody wants any more dies quietly, on the visit and on the requeue`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<NormalizedRelayUrl>(scope)
            val visits = AtomicInteger()
            scope.launch {
                q.visitLoop(stillWanted = { false }, revisitDelayMs = { 1L }) {
                    visits.incrementAndGet()
                }
            }
            try {
                q.offer(url)
                delay(300)
                assertEquals(0, visits.get(), "a dropped url is drawn, skipped, and never revisited")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `two units on one relay run at the same time, the same unit never twice`() =
        runBlocking {
            // Admission is per key, and the key is a (relay, stream) pair.
            data class Unit2(
                val url: String,
                val stream: String,
            )

            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<Unit2>(scope)
            val entered = Channel<Unit2>(Channel.UNLIMITED)
            val release = Channel<Unit>(Channel.UNLIMITED)
            val running = AtomicInteger()
            val peak = AtomicInteger()
            // Three workers against two units, so a worker is always free to draw the re-offer.
            repeat(3) {
                scope.launch {
                    q.visitLoop(stillWanted = { true }, revisitDelayMs = { 3_600_000L }) { key ->
                        // Count first, then the peak: `updateAndGet` re-runs its lambda on a lost CAS.
                        val now = running.incrementAndGet()
                        peak.updateAndGet { was -> maxOf(was, now) }
                        entered.send(key)
                        release.receive()
                        running.decrementAndGet()
                    }
                }
            }
            try {
                val content = Unit2("wss://a.example/", "content")
                q.offer(content)
                q.offer(Unit2("wss://a.example/", "indexers"))
                withTimeout(5_000) {
                    assertEquals(
                        setOf(content, Unit2("wss://a.example/", "indexers")),
                        setOf(entered.receive(), entered.receive()),
                        "one relay, two streams, both visiting at once",
                    )
                }
                assertEquals(2, running.get())

                assertTrue(q.offer(content), "wanting it again is allowed while it runs")
                repeat(20) { delay(10) }
                assertEquals(2, running.get(), "still two — the third draw parked")
                assertEquals(2, peak.get(), "and never ran a third visit at all")

                // The park is a promise, not a drop: finishing re-offers it.
                release.send(Unit)
                release.send(Unit)
                withTimeout(5_000) { assertEquals(content, entered.receive(), "the parked unit runs as soon as its own visit ends") }
                release.send(Unit)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `the queue splits by group, counting what waits and never what runs`() =
        runBlocking {
            // A running unit is not waiting; counted, it would appear both in `queued` and in flight.
            data class Unit2(
                val url: String,
                val stream: String,
            )

            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue<Unit2>(scope)
            val entered = Channel<Unit2>(Channel.UNLIMITED)
            val release = Channel<Unit>(Channel.UNLIMITED)
            // One worker, so exactly one unit runs and the rest are provably queued.
            scope.launch {
                q.visitLoop(stillWanted = { true }, revisitDelayMs = { 3_600_000L }) { key ->
                    entered.send(key)
                    release.receive()
                }
            }
            try {
                assertEquals(emptyMap(), q.waitingBy { it.stream }, "an empty queue is an empty split, not a zero per stream")

                q.offer(Unit2("wss://a.example/", "content"))
                withTimeout(5_000) { entered.receive() }
                q.offer(Unit2("wss://b.example/", "content"))
                q.offer(Unit2("wss://c.example/", "content"))
                q.offer(Unit2("wss://a.example/", "indexers"))

                assertEquals(
                    mapOf("content" to 2, "indexers" to 1),
                    q.waitingBy { it.stream },
                    "the two still queued and the one on a worker counted apart",
                )
                assertEquals(3, q.waiting, "and the split adds up to the number the pool publishes")

                // A stream nothing has queued is absent, not zero; the caller knows its own streams.
                assertEquals(null, q.waitingBy { it.stream }["idle"])
            } finally {
                release.trySend(Unit)
                scope.cancel()
            }
        }
}
