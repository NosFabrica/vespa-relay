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
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The rotation invariants, hermetically — the choreography that was
 * probe-only when two of its race bugs were found. The visits here are
 * controllable: each one announces itself and waits to be released, so the
 * interleavings under test are STAGED, not hoped for.
 */
class VisitQueueTest {
    private val url = RelayUrlNormalizer.normalize("wss://a.example")

    @Test
    fun `disarming a revisit lets the next completion arm the cadence the url now has`() =
        runBlocking {
            // THE SIX-TIMES FRESHNESS GAP. The delay is read once, when the
            // timer is armed, so a url armed while TAILED carries the tailed
            // cadence — half an hour against five minutes untailed. Eviction
            // requeues it promptly, but the visit that followed found the old
            // timer still standing and armed nothing, so the relay that had
            // just lost its live feed waited out the cadence it earned while it
            // still had one.
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            // Long while "tailed", short once not — the pool's own shape.
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

                    // The tail is evicted: the cadence is now the short one,
                    // and the pool requeues promptly as it always did.
                    tailed.set(0)
                    q.disarm(url)
                    q.offer(url)
                    entered.receive()

                    // …and THIS is what the stale timer used to swallow: the
                    // completion after eviction arms the untailed cadence, so a
                    // third visit lands on the short clock rather than an hour
                    // out.
                    entered.receive()
                    assertEquals(3, visits.get())
                }
            } finally {
                scope.cancel()
            }
        }

    /**
     * **THE TIMER THAT LOST ITS SLOT USED TO BE LEAKED, NOT CANCELLED.**
     *
     * `armRevisit` builds the timer with `scope.launch(start = LAZY)` before
     * claiming the url's slot, and deliberately so — the body clears its own
     * entry, so a job that could run before the map knew about it would clear a
     * successor's. But `launch` parents the job at CREATION; LAZY defers only
     * the body. The loser of `putIfAbsent` was then dropped on the floor: never
     * started, never cancelled, and an incomplete child of a scope that lives
     * as long as the router.
     *
     * The race is staged rather than hoped for, in this class's usual way.
     * `revisitDelayMs` is the caller's lambda and runs INSIDE `armRevisit`
     * before the slot is claimed, so blocking the first call holds one worker
     * exactly there while the other arms the same url and wins.
     */
    @Test
    fun `a revisit timer that loses the slot is cancelled, not left parented forever`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val q = VisitQueue(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            val inArmRevisit = Channel<Unit>(Channel.UNLIMITED)
            val release = java.util.concurrent.CountDownLatch(1)
            val armCalls = AtomicInteger()
            repeat(2) {
                scope.launch {
                    q.visitLoop(
                        stillWanted = { true },
                        revisitDelayMs = {
                            // The FIRST worker to finish parks here, inside
                            // armRevisit and before the slot is claimed.
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

                    // Worker B takes the whole url through a visit and arms it,
                    // winning the slot A is about to ask for.
                    q.offer(url)
                    entered.receive()
                    while (armCalls.get() < 2) delay(10)
                    delay(100)

                    // …and now A asks, and loses.
                    release.countDown()
                    delay(300)

                    // Two worker loops plus the ONE armed timer that won. The
                    // loser must not still be here: before the fix this counted
                    // four, and grew by one for every lost race the router ran.
                    // NOT `isActive`, which is the trap this assertion fell
                    // into first: a LAZY job that was never started is in the
                    // New state, so `isActive` is false for it and the leak is
                    // exactly what such a filter hides. `isCompleted` is the
                    // question — a cancelled job reaches it, an abandoned one
                    // never does.
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
    fun `an armed revisit says when it is due, and a url with none says nothing`() =
        runBlocking {
            // The countdown the status page draws per relay. Absent is the
            // answer for a url being visited or waiting in the queue — the
            // timer is armed when a visit FINISHES — and a `0` there would read
            // as "due now" on a relay nothing is counting down to.
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            scope.launch {
                q.visitLoop(stillWanted = { true }, revisitDelayMs = { 600_000L }) {
                    entered.send(Unit)
                    // Held open, so the assertion below is taken while the
                    // visit is genuinely running rather than after it.
                    delay(60_000)
                }
            }
            try {
                withTimeout(10_000) {
                    assertEquals(emptyMap(), q.revisitsDueInSec(), "nothing has finished, so nothing is armed")
                    q.offer(url)
                    entered.receive()
                    assertEquals(emptyMap(), q.revisitsDueInSec(), "a url being visited is counting down to nothing")
                }
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a revisit that is due but has not fired counts down to zero, never past it`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue(scope)
            val entered = Channel<Unit>(Channel.UNLIMITED)
            scope.launch {
                q.visitLoop(stillWanted = { true }, revisitDelayMs = { 600_000L }) { entered.send(Unit) }
            }
            try {
                withTimeout(10_000) {
                    q.offer(url)
                    entered.receive()
                    // The visit has finished and a ten-minute timer is armed.
                    delay(200)
                    val due = q.revisitsDueInSec()[url.url]
                    assertNotNull(due)
                    assertTrue(due in 590..600, "ten minutes out, read in seconds: was $due")
                    // …and read from an instant past the deadline, which is
                    // reachable whenever the dispatcher is busy: a countdown
                    // running backwards would draw a revisit overdue by hours.
                    assertEquals(0L, q.revisitsDueInSec(System.currentTimeMillis() + 3_600_000)[url.url])
                }
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `disarming a url with nothing armed is a no-op`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                VisitQueue(scope).disarm(url)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a requeue during a visit is parked and re-sent the moment it finishes`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val q = VisitQueue(scope)
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
                    // The visit is running; a rebuild wants it again. The
                    // second worker draws it, collides, and parks it.
                    assertTrue(q.offer(url), "a running url can be wanted again")
                    delay(200)
                    assertEquals(1, visits.get(), "the collision must not start a second concurrent visit")
                    release.send(Unit)
                    // The parked requeue comes back as the NEXT visit — one
                    // queue wait, not the hour-long revisit timer above.
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
            val q = VisitQueue(scope)
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
            val q = VisitQueue(scope)
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
}
