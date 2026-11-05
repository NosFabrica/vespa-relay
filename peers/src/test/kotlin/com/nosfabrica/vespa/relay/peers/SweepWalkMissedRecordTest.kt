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
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * A record of ours superseded while the sweep's walk is between pages is in neither page; the next edit
 * of its url still reads the record by `#d`, and keeps every tag on it.
 */
class SweepWalkMissedRecordTest {
    private val relay = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val subject = RelayUrlNormalizer.normalize("wss://subject.example")
    private val canonical = RelayUrlNormalizer.normalize("wss://canonical.example")

    /** Before the walk's second page, lands [late] in the store, as the outbox's re-send would. */
    private class LandsBetweenPages(
        private val inner: IEventStore,
        private val self: String,
        private val late: Event,
    ) : IEventStore by inner {
        private var landed = false

        override suspend fun <T : Event> query(filter: Filter): List<T> {
            if (!landed && filter.authors == listOf(self) && filter.tags == null && filter.until != null) {
                landed = true
                inner.insert(late)
            }
            return inner.query(filter)
        }
    }

    /** The store and a record over it, the subject's record set to land again between the walk's pages, as an outbox re-send does. */
    private suspend fun midWalk(): Pair<IEventStore, RelayVerdictRecord> {
        val now = nowSeconds()
        val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
        val consistency = arrayOf(RelayVerdictRecord.SELF_CONSISTENT_TAG, "true", "stored sweeps ago", (now - 3_600).toString(), RelayVerdictRecord.CONSISTENCY_EPOCH)
        // The subject's record, older than everything else, so the walk reaches it on its second page.
        inner.insert(
            signer.sign(EventTemplate<Event>(now - 10_000, RelayDiscoveryEvent.KIND, arrayOf(arrayOf("d", subject.url), consistency), "")),
        )
        // A first page's worth (RECORD_PAGE = 2,000) of newer records of ours.
        for (i in 0 until 2_000) {
            inner.insert(signer.sign(EventTemplate<Event>(now - 5_000 + i, RelayDiscoveryEvent.KIND, arrayOf(arrayOf("d", "wss://filler$i.example/")), "")))
        }
        // An earlier edit of the subject that was cut at its deadline, re-sent and landing now.
        val late =
            signer.sign(
                EventTemplate<Event>(
                    now - 100,
                    RelayDiscoveryEvent.KIND,
                    arrayOf(
                        arrayOf("d", subject.url),
                        consistency,
                        arrayOf("l", "prime", RelayVerdictRecord.FITNESS_NAMESPACE, "answered", (now - 100).toString(), RelayVerdictRecord.FITNESS_EPOCH),
                        arrayOf("L", RelayVerdictRecord.FITNESS_NAMESPACE),
                    ),
                    "",
                ),
            )
        val store = LandsBetweenPages(inner, signer.pubKey, late)
        return inner to RelayVerdictRecord(store, signer)
    }

    @Test
    fun `a record superseded mid-walk is not read as no record, so the next edit keeps its tags`(): Unit =
        runBlocking {
            val (inner, record) = midWalk()
            record.beginSweep()
            try {
                // The derivation's read takes the walk, as AliasMonitor.runPass's first read does.
                record.ownRecordsSince(0)
                assertTrue(record.publishFold(subject, canonical, "contained") != null, "the fold's write did not land")
            } finally {
                record.endSweep()
            }
            val tags =
                inner
                    .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(subject.url))))
                    .maxBy { it.createdAt }
                    .tags
                    .map { it[0] }
            assertTrue(RelayVerdictRecord.SELF_CONSISTENT_TAG in tags, "the edit signed a fresh record over one the walk missed, dropping a verdict stored long ago: $tags")
            assertTrue("l" in tags, "the edit dropped the fitness grade that landed during the walk: $tags")
        }

    @Test
    fun `a record superseded mid-walk still reads back its verdicts within the sweep`(): Unit =
        runBlocking {
            val (_, record) = midWalk()
            record.beginSweep()
            val held =
                try {
                    record.ownRecordsSince(0)
                    record.load(listOf(subject))
                } finally {
                    record.endSweep()
                }
            assertTrue(subject in held.consistent, "the sweep read a url the walk missed as having no consistency verdict")
            assertTrue(held.fitness[subject]?.value == "prime", "the sweep read a url the walk missed as ungraded: ${held.fitness}")
        }
}
