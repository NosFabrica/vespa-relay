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
import com.nosfabrica.vespa.relay.server.config.ingestBatchFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Published events reach the store in batches of up to `ingestBatch`, not quartz's SQLite-sized 64:
 * every Vespa write pays its round trips however few events it carries.
 */
class IngestBatchTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerSync()

    /** Records each write's size and holds the first one until [release], so the rest queue behind it. */
    private class GatedStore(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        val sizes: MutableList<Int> = Collections.synchronizedList(mutableListOf())
        val release = CompletableDeferred<Unit>()

        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
            sizes.add(events.size)
            release.await()
            return inner.batchInsert(events)
        }
    }

    /** Publishes [n] notes while the first write is held, then lets them all through. */
    private fun publishBehindAHeldWrite(
        n: Int,
        ingestBatch: Int?,
    ): GatedStore =
        runBlocking {
            val store = GatedStore(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl))
            val server =
                if (ingestBatch == null) NostrRelayServer(store, relayUrl) else NostrRelayServer(store, relayUrl, ingestBatch = ingestBatch)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            val events = (0 until n).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "note $it") }
            for (e in events) session.receive("""["EVENT",${e.toJson()}]""")
            store.release.complete(Unit)
            awaitCount(out, n) { it.startsWith("""["OK",""") && ",true," in it }
            assertEquals(n, store.count(Filter(kinds = listOf(1))))
            session.close()
            server.close()
            store
        }

    @Test
    fun `unset keeps quartz's 64-event batches`() {
        val store = publishBehindAHeldWrite(300, ingestBatch = null)
        assertEquals(IngestQueue.DEFAULT_MAX_BATCH, store.sizes.max(), "batches: ${store.sizes}")
    }

    @Test
    fun `a larger ingest batch writes what queued behind a slow write in one batch`() {
        val store = publishBehindAHeldWrite(300, ingestBatch = 500)
        assertTrue(store.sizes.max() > IngestQueue.DEFAULT_MAX_BATCH, "no batch exceeded 64: ${store.sizes}")
        assertEquals(300, store.sizes.sum())
    }

    @Test
    fun `the queue holds four batches and never less than quartz's default`() {
        assertEquals(IngestQueue.DEFAULT_CAPACITY, ingestCapacity(IngestQueue.DEFAULT_MAX_BATCH))
        assertEquals(4_000, ingestCapacity(1_000))
    }

    @Test
    fun `RELAY_INGEST_BATCH parses strictly`() {
        assertEquals(IngestQueue.DEFAULT_MAX_BATCH, ingestBatchFromEnv(emptyMap()))
        assertEquals(1_000, ingestBatchFromEnv(mapOf("RELAY_INGEST_BATCH" to " 1000 ")))
        assertFailsWith<IllegalStateException> { ingestBatchFromEnv(mapOf("RELAY_INGEST_BATCH" to "0")) }
        assertFailsWith<IllegalStateException> { ingestBatchFromEnv(mapOf("RELAY_INGEST_BATCH" to "lots")) }
        assertFailsWith<IllegalStateException> { ingestBatchFromEnv(mapOf("RELAY_INGEST_BATCH" to "100000")) }
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
        fail("timed out waiting for $n accepted OKs; got ${synchronized(out) { out.count(match) }}")
    }
}
