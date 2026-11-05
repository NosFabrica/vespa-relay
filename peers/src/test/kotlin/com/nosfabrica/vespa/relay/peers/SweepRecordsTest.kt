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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A sweep reads our own records off one walk, and writes and reads exactly what reading the store each time did. */
class SweepRecordsTest {
    private val relay = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val urls = (1..6).map { RelayUrlNormalizer.normalize("wss://r$it.example") }
    private val measuredAt = nowSeconds() - 600

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)

    /** Our records as their tags, with the stamps a write takes from the clock blanked. */
    private suspend fun recordsOf(store: IEventStore): Map<String, List<List<String>>> =
        store
            .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey)))
            .associate { e -> e.tags.first { it[0] == "d" }[1] to e.tags.map(::unstamped) }

    private fun unstamped(tag: Array<String>): List<String> = tag.mapIndexed { i, v -> if (i == 3 && tag[0] in setOf(RelayVerdictRecord.SAME_AS_TAG, RelayVerdictRecord.SELF_CONSISTENT_TAG)) "*" else v }

    /** The same records before either run: graded, folded, and one whose every tag aged out long ago. */
    private suspend fun seed(
        store: NostrSemanticsStore,
        record: RelayVerdictRecord,
    ) {
        record.publish(urls[0], urls[1], 100, 80)
        record.publishFitness(urls[1], "prime", "answered", pageable = Fact(true, "drained"), nip77 = null, measuredAt = measuredAt)
        record.publishConsistency(urls[3], true, 100, 100, 99, 0.99, 7)
        record.publishFitness(urls[4], "prime", "answered", pageable = null, nip77 = Fact(false, "declined"), measuredAt = measuredAt)
        val old = nowSeconds() - RelayVerdictRecord.DEFAULT_TTL_SECONDS - 1_000
        store.insert(
            signer.sign(
                EventTemplate<Event>(old, RelayDiscoveryEvent.KIND, arrayOf(arrayOf("d", urls[5].url), arrayOf(RelayVerdictRecord.SAME_AS_TAG, urls[5].url, "old", old.toString(), "1")), ""),
            ),
        )
    }

    /** One sweep's reads and writes, three passes' worth, returning everything it read. */
    private suspend fun sweep(record: RelayVerdictRecord): List<Any> {
        val read = ArrayList<Any>()
        read += record.loadAll()
        read.add(record.ownRecordsSince(nowSeconds() - RelayVerdictRecord.DEFAULT_TTL_SECONDS).map { it.tags.first { t -> t[0] == "d" }[1] }.sorted())
        // The fold.
        record.publish(urls[2], urls[1], 50, 40)
        record.publishDistinct(urls[1], 60, "1 compared endpoint(s) on this host", 3)
        read += record.load(urls)
        // Stability.
        record.publishConsistency(urls[1], true, 100, 98, 97, 0.97, 7)
        record.publishConsistency(urls[2], false, 100, 30, 10, 0.1, 7)
        read += record.load(urls + urls)
        // Fitness, a withdrawal among it.
        for (url in urls.take(3)) record.publishFitness(url, "prime", "answered", pageable = Fact(true, "drained"), nip77 = Fact(true, "spoke"), measuredAt = measuredAt)
        record.retireFitness(urls[4])
        read += record.load(urls)
        read += record.loadAll()
        return read
    }

    @Test
    fun `a sweep off one walk reads and writes what a sweep reading the store each time did`(): Unit =
        runBlocking {
            val plainStore = newStore()
            val plain = RelayVerdictRecord(plainStore, signer)
            seed(plainStore, plain)
            val plainCalls = StoreCalls()
            val plainRead = withContext(plainCalls) { sweep(plain) }

            val sweptStore = newStore()
            val swept = RelayVerdictRecord(sweptStore, signer)
            seed(sweptStore, swept)
            val sweptCalls = StoreCalls()
            swept.beginSweep()
            val sweptRead =
                try {
                    withContext(sweptCalls) { sweep(swept) }
                } finally {
                    swept.endSweep()
                }

            assertEquals(plainRead, sweptRead, "a read inside the sweep differed")
            assertEquals(recordsOf(plainStore), recordsOf(sweptStore), "the records written differed")
            val u2 = recordsOf(sweptStore).getValue(urls[1].url).map { it[0] }
            assertTrue(RelayVerdictRecord.SAME_AS_TAG in u2 && RelayVerdictRecord.SELF_CONSISTENT_TAG in u2 && "l" in u2, "three passes' edits did not merge: $u2")

            fun issued(
                calls: StoreCalls,
                caller: String,
            ) = calls
                .snapshot()
                .callers
                .firstOrNull { it.caller == caller }
                ?.issued ?: 0
            val writes = 8L
            assertEquals(2 * writes, issued(plainCalls, StoreCalls.CALLER_MONITOR_PUBLISH), "each edit read its record, then wrote it")
            assertEquals(writes, issued(sweptCalls, StoreCalls.CALLER_MONITOR_PUBLISH), "an edit inside the sweep read its record from the store")
            assertEquals(1L, issued(sweptCalls, StoreCalls.CALLER_MONITOR_VERDICTS), "the sweep read its records more than once")
        }

    /** Stores every write, and reports the first write of [url] once [armed] as refused anyway. */
    private class LandsThenRefuses(
        private val store: IEventStore,
        private val url: NormalizedRelayUrl,
    ) : StoreWriter by StoreWriter.local(store) {
        var armed = false

        override suspend fun insert(event: Event) {
            store.insert(event)
            val subject = event.tags.first { it[0] == "d" }[1]
            if (armed && subject == url.url) {
                armed = false
                error("the relay went silent before its OK")
            }
        }
    }

    @Test
    fun `a write reported refused is read afresh on the next edit, so whatever landed is kept`(): Unit =
        runBlocking {
            val store = newStore()
            val writer = LandsThenRefuses(store, urls[0])
            val record = RelayVerdictRecord(store, signer, writer = writer)
            record.publishFitness(urls[0], "prime", "answered", pageable = null, nip77 = null, measuredAt = measuredAt)
            writer.armed = true
            record.beginSweep()
            try {
                // Taken before the refused write, so only a fresh read can know it landed.
                record.loadAll()
                assertNull(record.publishConsistency(urls[0], true, 100, 100, 99, 0.99, 7))
                record.publish(urls[0], urls[1], 100, 90)
                val inSweep = record.load(listOf(urls[0]))
                assertTrue(urls[0] in inSweep.consistent, "the sweep's read missed a write that landed")
            } finally {
                record.endSweep()
            }
            val tags = recordsOf(store).getValue(urls[0].url).map { it[0] }
            assertTrue(RelayVerdictRecord.SELF_CONSISTENT_TAG in tags && RelayVerdictRecord.SAME_AS_TAG in tags && "l" in tags, "an edit after a refused write lost what landed: $tags")
        }
}
