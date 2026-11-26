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
import com.nosfabrica.vespa.relay.server.config.replayGateFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip77Negentropy.NegOpenCmd
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The relay runs at most so many plain REQ replays against the store at once, whoever sends them; the rest wait
 * in arrival order, past a bound a new one is refused `rate-limited:`, and ranked reads keep their own lanes.
 */
class ReplayGateTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)

    /** Replays whose filter carries a `#t` tag wait here, by tag, until the test releases them. */
    private val holds = ConcurrentHashMap<String, CompletableDeferred<Unit>>()
    private val started: MutableList<String> = Collections.synchronizedList(mutableListOf())

    private val store =
        object : IEventStore by inner {
            override suspend fun rawQuery(
                filters: List<Filter>,
                onEach: (RawEvent) -> Unit,
            ) {
                filters.firstNotNullOfOrNull { it.tags?.get("t")?.firstOrNull() }?.let { tag ->
                    started += tag
                    holds.computeIfAbsent(tag) { CompletableDeferred() }.await()
                }
                inner.rawQuery(filters, onEach)
            }
        }

    private var server: NostrRelayServer? = null

    @AfterTest
    fun tearDown() {
        holds.values.forEach { it.complete(Unit) }
        server?.close()
    }

    private fun server(gate: ReplayGate) = NostrRelayServer(store, relayUrl, requireReadLens = false, replays = gate).also { server = it }

    private fun NostrRelayServer.open(): Pair<RelaySession, MutableList<String>> {
        val out = Collections.synchronizedList(mutableListOf<String>())
        return connect { out.add(it) } to out
    }

    private fun Pair<RelaySession, MutableList<String>>.req(tag: String) = runBlocking { first.receive("""["REQ","$tag",{"kinds":[1],"#t":["$tag"]}]""") }

    private fun release(tag: String) = holds.computeIfAbsent(tag) { CompletableDeferred() }.complete(Unit)

    @Test
    fun `replays past the bound wait for a permit relay-wide, and run in arrival order as each EOSE frees one`() {
        val gate = ReplayGate(permits = 2, maxWaiting = 10)
        val server = server(gate)
        val sockets = List(4) { server.open() }
        sockets.forEachIndexed { i, s -> s.req("r$i") }
        awaitTrue { started.size == 2 }
        settle()
        assertEquals(listOf("r0", "r1"), started.toList(), "two permits, so two replays reach the store, the first two sent")
        assertEquals(2, gate.running)
        assertEquals(2, gate.waiting, "the other two connections wait, though each sent only one REQ")

        release("r1")
        awaitMessage(sockets[1].second) { it.startsWith("""["EOSE","r1"]""") }
        awaitTrue { started.size == 3 }
        assertEquals("r2", started[2], "the first waiting is the next admitted")
        release("r0")
        awaitTrue { started.size == 4 }
        assertEquals(listOf("r0", "r1", "r2", "r3"), started.toList())
        release("r2")
        release("r3")
        sockets.forEachIndexed { i, s -> awaitMessage(s.second) { it.startsWith("""["EOSE","r$i"]""") } }
        awaitTrue { gate.running == 0 }
        assertEquals(2, gate.waited)
    }

    @Test
    fun `a REQ past a full queue is refused rate-limited with a retry-after, and a CLOSE while waiting frees its place`() {
        val gate = ReplayGate(permits = 1, maxWaiting = 1)
        val server = server(gate)
        val (a, b, c) = List(3) { server.open() }
        a.req("held")
        awaitTrue { started == listOf("held") }
        b.req("waiting")
        awaitTrue { gate.waiting == 1 }

        c.req("refused")
        val closed = awaitMessage(c.second) { it.startsWith("""["CLOSED","refused"""") }
        assertTrue(""""rate-limited:""" in closed, closed)
        assertTrue(closed.endsWith(""","${ReplayGate.RETRY_AFTER_MS}"]"""), "the client is told when to ask again: $closed")
        assertEquals(1, gate.refused)

        runBlocking { b.first.receive("""["CLOSE","waiting"]""") }
        awaitTrue { gate.waiting == 0 }
        c.req("after")
        awaitTrue { gate.waiting == 1 }
        release("held")
        awaitTrue { started.size == 2 }
        assertEquals(listOf("held", "after"), started.toList(), "the closed REQ never reached the store")
        release("after")
        awaitMessage(c.second) { it.startsWith("""["EOSE","after"]""") }
    }

    @Test
    fun `a ranked read takes no relay-wide permit, and a full queue does not refuse it or a reconcile`() {
        val gate = ReplayGate(permits = 1, maxWaiting = 1)
        val server = server(gate)
        val (a, b, c) = List(3) { server.open() }
        a.req("held")
        b.req("waiting")
        awaitTrue { gate.waiting == 1 }
        runBlocking { c.first.receive("""["REQ","ranked",{"kinds":[1],"search":"alpha"}]""") }
        awaitMessage(c.second) { it.startsWith("""["EOSE","ranked"]""") }
        assertFalse(c.second.any { it.startsWith("""["CLOSED","ranked"""") })

        val reconcile = NegOpenCmd("neg", Filter(kinds = listOf(1)), "")
        assertTrue(gate.policy().accept(reconcile) is PolicyResult.Accepted, "a NEG-OPEN is not a replay of this gate")
    }

    @Test
    fun `live events past the bound end a replay still waiting for a relay-wide permit`(): Unit =
        runBlocking(Dispatchers.Default) {
            val ingest = IngestQueue(store, SupervisorJob())
            val gate = ReplayGate(permits = 1)
            val backend = ObserverBackend(LiveEventStore(store, ingest), replays = gate)
            val first = backend.openSubscription(context(1), listOf(Filter(kinds = listOf(1), tags = mapOf("t" to listOf("held")))), LiveListener { _, _ -> })
            val delivered = AtomicInteger()
            val second = backend.openSubscription(context(2), listOf(Filter(kinds = listOf(1))), LiveListener { _, _ -> delivered.incrementAndGet() })
            try {
                launch { first.replayRaw({}, {}) }
                withTimeout(10_000) { while (started.isEmpty()) delay(10) }
                val waiting = async { runCatching { second.replayRaw({}, {}) } }
                withTimeout(10_000) { while (gate.waiting == 0) delay(10) }
                val signer = NostrSignerSync()
                val now = System.currentTimeMillis() / 1000
                repeat(LIVE_EVENTS) { i -> backend.submit(signer.sign<Event>(now, 1, emptyArray(), "live $i")) {} }
                val outcome = withTimeout(30_000) { waiting.await() }

                assertTrue(outcome.isFailure, "the waiting replay must end, not hold every live event until its turn")
                assertTrue("live" in outcome.exceptionOrNull()?.message.orEmpty())
                assertEquals(0, delivered.get(), "nothing it held was sent ungated")
                assertEquals(0, gate.waiting, "and it left the queue")
            } finally {
                release("held")
                first.cancel()
                second.cancel()
                ingest.close()
            }
        }

    @Test
    fun `REPLAY_CONCURRENCY and REPLAY_QUEUE size the gate, and a malformed one stops the boot`() {
        val defaults = replayGateFromEnv(emptyMap())
        assertEquals(ReplayGate.DEFAULT_PERMITS to ReplayGate.DEFAULT_WAITING, defaults.permits to defaults.maxWaiting)
        val set = replayGateFromEnv(mapOf("REPLAY_CONCURRENCY" to "32", "REPLAY_QUEUE" to "0"))
        assertEquals(32 to 0, set.permits to set.maxWaiting)
        val off = replayGateFromEnv(mapOf("REPLAY_CONCURRENCY" to "0"))
        assertFalse(off.gates(listOf(Filter(kinds = listOf(1)))), "0 turns the gate off")
        for (bad in listOf("REPLAY_CONCURRENCY" to "-1", "REPLAY_QUEUE" to "many")) {
            assertFailsWith<IllegalStateException>(bad.toString()) { replayGateFromEnv(mapOf(bad)) }
        }
    }

    @Test
    fun `with no queue a REQ is refused only when no permit is free`() {
        val gate = ReplayGate(permits = 1, maxWaiting = 0)
        val plain = listOf(Filter(kinds = listOf(1)))
        assertFalse(gate.refuses(plain), "a free permit admits it")
        val permit = gate.tryEnter(plain)
        assertTrue(gate.refuses(plain), "none free and nowhere to wait")
        permit!!.release()
        assertFalse(gate.refuses(plain))
        assertEquals(1, gate.refused)
    }

    @Test
    fun `the pressure endpoint reports the replay gate`() {
        val relay = NostrRelayServer(store, relayUrl, replays = ReplayGate(permits = 3))
        val http = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), pressure = ServingPressure(), wait = false)
        try {
            val port =
                runBlocking {
                    http.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            val body =
                HttpClient
                    .newHttpClient()
                    .send(HttpRequest.newBuilder(URI("http://localhost:$port/pressure")).build(), HttpResponse.BodyHandlers.ofString())
                    .body()
            val replays =
                Json
                    .parseToJsonElement(body)
                    .jsonObject
                    .getValue("replays")
                    .jsonObject
            assertEquals(setOf("running", "waiting", "waited", "refused"), replays.keys)
            assertTrue(replays.values.all { it.jsonPrimitive.long == 0L }, body)
        } finally {
            http.stop(0, 0)
            relay.close()
        }
    }

    private fun context(id: Long) =
        object : RequestContext {
            override val connectionId = id
            override val policy: IRelayPolicy get() = error("not asked")
            override val authenticatedUsers = emptySet<String>()
        }

    /** Long enough for a replay that was going to reach the store to have reached it. */
    private fun settle() = Thread.sleep(200)

    private fun awaitTrue(condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 10_000
        while (!condition()) {
            if (System.currentTimeMillis() > deadline) fail("timed out; started $started")
            Thread.sleep(10)
        }
    }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }

    private companion object {
        /** Past quartz's own hold before EOSE and past ours. */
        const val LIVE_EVENTS = 3_000
    }
}
