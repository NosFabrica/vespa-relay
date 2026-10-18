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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The live gate's reads for cold authors: one per author in flight, bounded across subscriptions, and never silent when they drop an event. */
class ColdAuthorChecksTest {
    /** A trust cache over one author: unknown until a read warms it. */
    private class Cell {
        @Volatile var warm: Boolean? = null
        val reads = AtomicInteger(0)
        val running = AtomicInteger(0)
        val peak = AtomicInteger(0)

        fun known(): Boolean? = warm

        suspend fun read(
            hold: suspend () -> Unit,
            answer: Boolean = true,
        ): Boolean {
            reads.incrementAndGet()
            peak.accumulateAndGet(running.incrementAndGet(), ::maxOf)
            try {
                hold()
            } finally {
                running.decrementAndGet()
            }
            warm = answer
            return answer
        }
    }

    @Test
    fun `subscriptions on one cold author share one read and every one is delivered`(): Unit =
        runBlocking {
            val checks = ColdAuthorChecks(max = 1, log = {})
            val cell = Cell()
            val release = CompletableDeferred<Unit>()
            val delivered = AtomicInteger(0)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
            try {
                repeat(100) {
                    checks.start(scope, "alice", cell::known, { cell.read({ release.await() }) }) { delivered.incrementAndGet() }
                }
                assertEquals(1, cell.reads.get(), "one store read for one author, however many subscriptions wait on it")
                release.complete(Unit)
                withTimeout(5_000) { while (delivered.get() < 100) delay(5) }
                assertEquals(1, cell.reads.get())
                assertEquals(0, checks.shedding, "waiting on another's read takes no read slot")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `checks the read ahead did not answer read for themselves within the bound, queued not dropped`(): Unit =
        runBlocking {
            val checks = ColdAuthorChecks(max = 2, log = {})
            val cells = List(5) { Cell() }
            val delivered = AtomicInteger(0)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                // Each cell stays cold for every waiter, as a rule reading another rank service's cell would.
                cells.forEachIndexed { i, cell ->
                    repeat(4) {
                        checks.start(scope, "author$i", { null }, { cell.read({ delay(20) }) }) { delivered.incrementAndGet() }
                    }
                }
                withTimeout(10_000) { while (delivered.get() < 20) delay(5) }
                assertTrue(cells.maxOf { it.peak.get() } <= 2, "never more reads at once than the bound")
                assertEquals(20, cells.sumOf { it.reads.get() }, "a cold cell is read for every check it leaves unanswered")
                assertEquals(0, checks.shedding, "a check waits its turn for a slot")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a check that gets no read slot in time is dropped, counted and said once`(): Unit =
        runBlocking {
            val said = Collections.synchronizedList(mutableListOf<String>())
            val checks = ColdAuthorChecks(max = 3, queueMillis = 100, log = { said += it })
            val running = AtomicInteger(0)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                repeat(10) { i ->
                    checks.start(scope, "author$i", { null }, {
                        running.incrementAndGet()
                        awaitCancellation()
                    }) { error("a check that never answers delivered") }
                }
                withTimeout(5_000) { while (checks.shedding < 7) delay(5) }
                delay(50)
                assertEquals(3, running.get(), "no more reads than the bound")
                assertEquals(7, checks.shedding)
                assertEquals(1, said.size, "said once, not once per event: $said")
                assertContains(said[0], "shed waiting on 3 reads in flight")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `checks past the waiting bound are dropped at once`(): Unit =
        runBlocking {
            val checks = ColdAuthorChecks(max = 1, maxWaiting = 2, log = {})
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
            try {
                repeat(5) { checks.start(scope, "alice", { null }, { awaitCancellation() }) { error("delivered") } }
                assertEquals(3, checks.shedding)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a slot frees when its check ends, and an admitted event is delivered`(): Unit =
        runBlocking {
            val checks = ColdAuthorChecks(max = 1, queueMillis = 0, log = {})
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                repeat(3) { i ->
                    val delivered = CompletableDeferred<Unit>()
                    checks.start(scope, "author$i", { null }, { true }) { delivered.complete(Unit) }
                    withTimeout(5_000) { delivered.await() }
                }
                assertEquals(0, checks.shedding, "one at a time never meets the bound")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a failed read drops its event and every one waiting on it, counted and said, and cancellation is neither`(): Unit =
        runBlocking {
            val said = Collections.synchronizedList(mutableListOf<String>())
            var clock = 0L
            val checks = ColdAuthorChecks(max = 8, everyMillis = 60_000, now = { clock }, log = { said += it })
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
            try {
                val release = CompletableDeferred<Unit>()
                repeat(3) {
                    checks.start(scope, "alice", { null }, {
                        release.await()
                        throw IllegalStateException("store timed out")
                    }) { error("a failed check delivered") }
                }
                release.complete(Unit)
                withTimeout(5_000) { while (checks.failures < 3) delay(5) }
                assertEquals(3, checks.failures, "the waiters fail closed with the read they waited on")
                assertEquals(1, said.size, "throttled: $said")
                assertContains(said[0], "IllegalStateException")

                val doomed = CoroutineScope(SupervisorJob() + Dispatchers.Unconfined)
                checks.start(doomed, "bob", { null }, { awaitCancellation() }) { error("a cancelled check delivered") }
                doomed.cancel()
                delay(50)
                assertEquals(3, checks.failures, "a subscription closing is not a failed check")

                val delivered = CompletableDeferred<Unit>()
                checks.start(scope, "bob", { null }, { true }) { delivered.complete(Unit) }
                withTimeout(5_000) { delivered.await() }

                clock = 61_000
                checks.start(scope, "carol", { null }, { error("again") }) { }
                withTimeout(5_000) { while (said.size < 2) delay(5) }
                assertEquals(4, checks.failures)
                assertContains(said[1], "4 cold-author check(s) failed", message = "said again once the window passed")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `the relay's pressure endpoint reports what the live gate dropped`() {
        val url = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = url), url)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), pressure = ServingPressure(), wait = false)
        try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            val body =
                HttpClient
                    .newHttpClient()
                    .send(HttpRequest.newBuilder(URI("http://localhost:$port/pressure")).build(), HttpResponse.BodyHandlers.ofString())
                    .body()
            val gate =
                Json
                    .parseToJsonElement(body)
                    .jsonObject
                    .getValue("liveGate")
                    .jsonObject
            assertEquals(0, gate.getValue("failed").jsonPrimitive.long)
            assertEquals(0, gate.getValue("shed").jsonPrimitive.long)
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }
}
