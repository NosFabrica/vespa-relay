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
import com.nosfabrica.vespa.eventstore.engine.client.EngineRefusesWrites
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.server.config.relayIngestFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The whole publish path (session, policies, LiveEventStore, the relay's default ingest pipeline,
 * NostrSemanticsStore) during a FEED BLOCK: a NIP-46 request (kind 24133, ephemeral) published right
 * behind a storable note must still be answered `OK true` and reach the signer subscribed to it, as it
 * is when it reaches the store alone. It needs no write; only the note does.
 */
class EphemeralFeedBlockTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerSync()

    // Reproduces a defect not fixed yet; fails against main 2f8ae4df. The fix removes this @Ignore.
    @Ignore
    @Test
    fun `an ephemeral publish is answered and fanned out while the store refuses writes`() =
        runBlocking<Unit> {
            val writing = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val inner = InMemoryEventIndex()
            // The engine refuses every write. The first one is also held until released, so the next
            // publishes queue behind it and drain as one batch, as a burst does on a busy relay.
            val refusing =
                object : EventIndex by inner {
                    private suspend fun refuse(): Nothing {
                        writing.complete(Unit)
                        release.await()
                        throw IllegalStateException("batch insert failed", EngineRefusesWrites("the engine is refusing writes (507): its feed is blocked for disk or memory"))
                    }

                    override suspend fun put(doc: EventDoc) = refuse()

                    override suspend fun putAll(docs: List<EventDoc>) = refuse()
                }
            val store = NostrSemanticsStore(refusing, relay = relayUrl)
            val scope = CoroutineScope(Job())
            val pipeline = IngestPipeline(store, relayIngestFromEnv(emptyMap()).tuning!!, audit = null, servingPressure = null, scope = scope, knownIds = inner::existingIds).also { it.start() }
            val server = NostrRelayServer(store, relayUrl, requireReadLens = false, ingest = pipeline)

            val now = System.currentTimeMillis() / 1000
            val remoteSigner = "b2".repeat(32)
            val blocker: Event = signer.sign(now, 1, emptyArray(), "first note, its write held")
            val note: Event = signer.sign(now, 1, emptyArray(), "a note in the same burst")
            val request: Event = signer.sign(now, 24133, arrayOf(arrayOf("p", remoteSigner)), "nip-44 ciphertext")

            val signerOut = Collections.synchronizedList(mutableListOf<String>())
            val bunker = server.connect { signerOut.add(it) }
            val out = Collections.synchronizedList(mutableListOf<String>())
            val client = server.connect { out.add(it) }
            try {
                bunker.receive("""["REQ","nip46",{"kinds":[24133],"#p":["$remoteSigner"]}]""")
                awaitMessage(signerOut) { it.startsWith("""["EOSE","nip46"""") }

                client.receive("""["EVENT",${blocker.toJson()}]""")
                withTimeout(10_000) { writing.await() }
                client.receive("""["EVENT",${note.toJson()}]""")
                client.receive("""["EVENT",${request.toJson()}]""")
                release.complete(Unit)

                // The storable notes rightly hear the feed block.
                val noteOk = awaitMessage(out) { it.startsWith("""["OK","${note.id}",""") }
                assertTrue(noteOk.startsWith("""["OK","${note.id}",false,"error:"""), "the note should hear the feed block: $noteOk")

                val requestOk = awaitMessage(out) { it.startsWith("""["OK","${request.id}",""") }
                assertTrue(
                    requestOk.startsWith("""["OK","${request.id}",true"""),
                    "an ephemeral event needs no store write, so a feed block must not refuse it; the relay answered $requestOk",
                )
                awaitMessage(signerOut, timeoutMs = 2_000) { it.startsWith("""["EVENT","nip46",""") && request.id in it }
            } finally {
                release.complete(Unit)
                client.close()
                bunker.close()
                server.close()
                pipeline.closeIntake()
                scope.cancel()
                pipeline.close()
            }
        }

    private fun awaitMessage(
        out: List<String>,
        timeoutMs: Long = 10_000,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
