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
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.server.config.relayIngestFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * HEAD-OF-LINE BLOCKING ACROSS CONNECTIONS. Every connection's EVENTs share the relay's one ingest
 * queue (production default: 1 worker, 1,000-event batches, a 1,000-slot queue, 20 ms linger), and a
 * connection reads its frames one at a time, so a connection whose EVENT waits for queue room reads
 * nothing else. One connection (A) flooding EVENTs while the store is slow must not stall another
 * connection's (B's) work.
 *
 * The store here costs 2 ms per event written: a full 1,000-event pass takes ~2 s, B's single event
 * ~2 ms. B's own work is one EVENT (a 2 ms write, at most a 20 ms linger) and one REQ against an
 * in-memory index (milliseconds). The bounds, 1 s for B's OK and 500 ms for B's EOSE, are ~25x what
 * that work costs and still well under one of A's passes, so they fail only when B waits on A.
 */
class PublishFloodIsolationTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private class Relay(
        val inner: InMemoryEventIndex,
        val pipeline: IngestPipeline,
        val server: NostrRelayServer,
        val scope: CoroutineScope,
    )

    private fun slowRelay(msPerEvent: Long = 2): Relay {
        val inner = InMemoryEventIndex()
        val slow =
            object : EventIndex by inner {
                override suspend fun put(doc: EventDoc) {
                    delay(msPerEvent)
                    inner.put(doc)
                }

                override suspend fun putAll(docs: List<EventDoc>) {
                    delay(msPerEvent * docs.size)
                    inner.putAll(docs)
                }
            }
        val store = NostrSemanticsStore(slow, relay = relayUrl)
        val scope = CoroutineScope(Job())
        val pipeline = IngestPipeline(store, relayIngestFromEnv(emptyMap()).tuning!!, audit = null, servingPressure = null, scope = scope, knownIds = inner::existingIds).also { it.start() }
        return Relay(inner, pipeline, NostrRelayServer(store, relayUrl, requireReadLens = false, ingest = pipeline), scope)
    }

    private class Timings(
        val okMs: Long?,
        val eoseMs: Long?,
        val queuedAtStart: Int,
        val capacity: Int,
    )

    /**
     * A floods [flood] EVENTs from its own reader (sequential receives, as NativeWebSocket's one reader
     * coroutine per connection does); once A waits for room (the queue full, or A at its connection's bound), B sends [bEvent] (if any) then a REQ.
     */
    private fun measure(bSendsEvent: Boolean): Timings =
        runBlocking {
            val relay = slowRelay()
            val flooder = NostrSignerSync()
            val now = System.currentTimeMillis() / 1000
            val flood = (0 until 3_000).map { flooder.sign<Event>(now, 1, emptyArray(), "flood $it") }
            val bSigner = NostrSignerSync()
            val bEvent: Event = bSigner.sign(now, 1, emptyArray(), "B's one note")

            val aOut = Collections.synchronizedList(mutableListOf<String>())
            val a = relay.server.connect { aOut.add(it) }
            val bOut = Collections.synchronizedList(mutableListOf<String>())
            val b = relay.server.connect { bOut.add(it) }
            val aReader = launch(Dispatchers.Default) { for (e in flood) a.receive("""["EVENT",${e.toJson()}]""") }
            try {
                // A is flooding once it waits on room: in the shared queue, or under its connection's own bound.
                val fullBy = System.currentTimeMillis() + 10_000
                while (!relay.pipeline.isFull() && relay.server.publishGate.held == 0) {
                    if (System.currentTimeMillis() > fullBy) fail("A's flood never filled the ingest queue nor reached its connection's bound")
                    delay(5)
                }
                delay(50)
                val queuedAtStart = relay.pipeline.queued.get()

                val startedAt = System.nanoTime()
                val bReader =
                    launch(Dispatchers.Default) {
                        if (bSendsEvent) b.receive("""["EVENT",${bEvent.toJson()}]""")
                        b.receive("""["REQ","mine",{"kinds":[1],"authors":["${bSigner.pubKey}"],"limit":10}]""")
                    }
                var okMs: Long? = null
                var eoseMs: Long? = null
                val deadline = System.currentTimeMillis() + 20_000
                while (System.currentTimeMillis() < deadline && (eoseMs == null || (bSendsEvent && okMs == null))) {
                    val elapsed = (System.nanoTime() - startedAt) / 1_000_000
                    synchronized(bOut) {
                        if (okMs == null && bOut.any { it.startsWith("""["OK","${bEvent.id}",true""") }) okMs = elapsed
                        if (eoseMs == null && bOut.any { it.startsWith("""["EOSE","mine"""") }) eoseMs = elapsed
                    }
                    delay(2)
                }
                bReader.cancel()
                Timings(okMs, eoseMs, queuedAtStart, relay.pipeline.capacity)
            } finally {
                aReader.cancel()
                a.close()
                b.close()
                relay.server.close()
                relay.pipeline.closeIntake()
                relay.scope.cancel()
                relay.pipeline.close()
            }
        }

    @Test
    fun `another connection's REQ is not delayed by one connection's EVENT flood`() {
        val t = measure(bSendsEvent = true)
        val eose = t.eoseMs ?: fail("B's REQ never reached EOSE within 20 s (queue ${t.queuedAtStart}/${t.capacity} when B sent)")
        assertTrue(
            eose < 500,
            "B's REQ (sent right after B's one EVENT) reached EOSE after ${eose}ms while A's flood held the shared ingest queue " +
                "at ${t.queuedAtStart}/${t.capacity}; B's own work needs milliseconds (B's OK came at ${t.okMs}ms)",
        )
    }

    @Test
    fun `another connection's EVENT is not delayed by one connection's EVENT flood`() {
        val t = measure(bSendsEvent = true)
        val ok = t.okMs ?: fail("B's EVENT was never answered OK true within 20 s (queue ${t.queuedAtStart}/${t.capacity} when B sent)")
        assertTrue(
            ok < 1_000,
            "B's one EVENT was answered after ${ok}ms while A's flood held the shared ingest queue at ${t.queuedAtStart}/${t.capacity}; " +
                "its own write costs ~2 ms plus at most a 20 ms linger",
        )
    }

    /** CONTROL (expected to pass): a REQ that is not queued behind its own connection's EVENT is not slowed by A's flood. */
    @Test
    fun `control - another connection's REQ alone is served promptly during the flood`() {
        val t = measure(bSendsEvent = false)
        val eose = t.eoseMs ?: fail("B's REQ never reached EOSE within 20 s")
        assertTrue(eose < 500, "B's lone REQ reached EOSE after ${eose}ms (queue ${t.queuedAtStart}/${t.capacity} when B sent)")
    }
}
