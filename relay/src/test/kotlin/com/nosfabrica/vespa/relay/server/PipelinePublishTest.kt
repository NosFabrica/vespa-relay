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
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.server.config.relayIngestFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/** With the ingest pipeline plugged in, a publisher hears what it heard from quartz's queue, and live subscribers still see what lands. */
class PipelinePublishTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private fun forge(event: Event) = Event(event.id, event.pubKey, event.createdAt, event.kind, event.tags, event.content, "f".repeat(128))

    private fun oks(
        out: List<String>,
        success: Boolean,
    ) = synchronized(out) { out.count { it.startsWith("""["OK",""") && ",$success," in it } }

    @Test
    fun `publishes are answered and fanned out through the pipeline`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val scope = CoroutineScope(Job())
            val pipeline = IngestPipeline(store, IngestTuning(2, 500), audit = null, servingPressure = null, scope = scope, knownIds = index::existingIds).also { it.start() }
            val server = NostrRelayServer(store, relayUrl, requireReadLens = false, ingest = pipeline)

            val watcherOut = Collections.synchronizedList(mutableListOf<String>())
            val watcher = server.connect { watcherOut.add(it) }
            watcher.receive("""["REQ","live",{"kinds":[1]}]""")
            awaitCount(watcherOut, 1) { it.startsWith("""["EOSE","live"""") }

            val out = Collections.synchronizedList(mutableListOf<String>())
            val publisher = server.connect { out.add(it) }
            val events = (0 until 300).map { note(it) }
            for (e in events) publisher.receive("""["EVENT",${e.toJson()}]""")
            awaitCount(out, 300) { it.startsWith("""["OK",""") }
            assertEquals(300, oks(out, true))
            assertEquals(300, store.count(Filter(kinds = listOf(1))))
            awaitCount(watcherOut, 300) { it.startsWith("""["EVENT","live"""") }

            // A resend is acknowledged as a duplicate, and a forgery is refused; neither reaches the subscriber.
            out.clear()
            for (e in events.take(150)) publisher.receive("""["EVENT",${e.toJson()}]""")
            publisher.receive("""["EVENT",${forge(note(999)).toJson()}]""")
            awaitCount(out, 151) { it.startsWith("""["OK",""") }
            assertEquals(150, synchronized(out) { out.count { ",true,\"duplicate:" in it } })
            assertTrue(synchronized(out) { out.any { ",false,\"invalid:" in it } }, "the forgery was not refused: $out")
            assertEquals(300, synchronized(watcherOut) { watcherOut.count { it.startsWith("""["EVENT","live"""") } })

            publisher.close()
            watcher.close()
            server.close()
            pipeline.closeIntake()
            scope.cancel()
            pipeline.close()
        }

    @Test
    fun `RELAY_INGEST picks the writer and the pipeline's shape`() {
        // One batch per worker may wait: the relay's publishes are buffered by their senders.
        // And a byte bound over them, since every publish may be a full frame.
        assertEquals(IngestTuning(2, 1_000, queue = 2_000, queueBytes = 512L shl 20), relayIngestFromEnv(emptyMap()).tuning)
        assertEquals(
            IngestTuning(4, 4_096, queue = 16_384, queueBytes = 64L shl 20),
            relayIngestFromEnv(mapOf("RELAY_INGEST_CONCURRENCY" to "4", "RELAY_INGEST_BATCH" to "4096", "RELAY_INGEST_QUEUE_MB" to "64")).tuning,
        )
        assertFailsWith<IllegalStateException> { relayIngestFromEnv(mapOf("RELAY_INGEST_QUEUE_MB" to "8")) }
        val queue = relayIngestFromEnv(mapOf("RELAY_INGEST" to "queue"))
        assertNull(queue.tuning)
        assertEquals(64, queue.batch)
        assertFailsWith<IllegalStateException> { relayIngestFromEnv(mapOf("RELAY_INGEST" to "sqlite")) }
        // Strict in either mode: a value the pipeline would refuse stops a queue boot too.
        assertFailsWith<IllegalStateException> { relayIngestFromEnv(mapOf("RELAY_INGEST" to "queue", "RELAY_INGEST_CONCURRENCY" to "abc")) }
    }

    private fun awaitCount(
        out: List<String>,
        n: Int,
        match: (String) -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            if (synchronized(out) { out.count(match) } >= n) return
            Thread.sleep(20)
        }
        fail("timed out waiting for $n frames; got ${synchronized(out) { out.count(match) }}")
    }
}
