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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.peers.GatedResponder
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The retraction audit deletes on evidence: a record a peer's reconcile lacks is removed only by its
 * author's own deletion request, a vanish aimed at this relay, or a newer version, each ingested so
 * the store's NIP-09/62 rules do the deleting and keep the tombstone. Absence alone keeps it.
 */
class RetractionEvidenceTest {
    private val url = RelayUrlNormalizer.normalize("wss://provider.example")
    private val self = RelayUrlNormalizer.normalize("wss://us.example")
    private val provider = NostrSignerSync(KeyPair())
    private val stranger = NostrSignerSync(KeyPair())
    private val ask = Filter(kinds = listOf(30382), authors = listOf(provider.pubKey))
    private val now = nowSeconds()

    private fun subject(n: Int) = n.toString(16).padStart(64, '0')

    private val cards = (1..20).map { n -> provider.sign<Event>(now - 3_600 + n, 30382, arrayOf(arrayOf("d", subject(n)), arrayOf("rank", "50")), "") }
    private val gone = cards.first()
    private val address = "30382:${provider.pubKey}:${subject(1)}"

    private val stream =
        SyncStream(
            name = "scores",
            dir = SyncDirection.DOWN,
            filter = Filter(kinds = listOf(30382)),
            urls = listOf(url),
            trusted = false,
            negentropySyncThePastSeconds = 86_400,
            deleteMissing = DeleteMissing.ON,
            ownedKinds = setOf(30382),
        )

    private fun deletion(
        by: NostrSignerSync,
        tag: Array<String>,
        at: Long = now,
    ) = by.sign<Event>(at, 5, arrayOf(tag), "")

    private class Outcome(
        val store: NostrSemanticsStore,
        val audit: RetractionAudit,
    ) {
        suspend fun held(filter: Filter) = store.query<Event>(filter)
    }

    /** One enforcing audit of a store holding [cards] and [alsoHeld] against a peer serving [peerHolds]. */
    private fun audit(
        peerHolds: List<Event>,
        alsoHeld: List<Event> = emptyList(),
        audited: SyncStream = stream,
    ): Outcome = auditOf(runBlocking { NostrSemanticsStore(InMemoryEventIndex(), relay = self).also { it.batchInsert(cards + alsoHeld) } }, peerHolds, audited)

    private fun auditOf(
        store: NostrSemanticsStore,
        peerHolds: List<Event>,
        audited: SyncStream = stream,
    ): Outcome =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val peer = GatedPeer(scope, peerHolds, gated = false)
                val client = NostrClient(peer, scope)
                val waivers = LensWaivers(log = {})
                GatedResponder(client, scope, NostrSignerInternal(KeyPair()), waivers)
                client.connect()
                val audit =
                    RetractionAudit(
                        ClientRetractionReads(client, waivers),
                        store,
                        SyncBands(null),
                        IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null).also { it.start() },
                        RefusedIds.disabled(),
                    )
                withTimeout(30_000) { audit.reconcileAndDelete(audited, url, ask, emptySet(), verdictWaitMs = 5_000) }
                Outcome(store, audit)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a record the peer lacks is kept when nothing says it was deleted`() =
        runBlocking {
            val out = audit(cards.drop(1))
            assertEquals(cards.size, out.held(ask).size, "absence alone is not a retraction")
        }

    @Test
    fun `its author's kind 5 at the peer deletes it through ingest and is mirrored as the tombstone`() =
        runBlocking {
            val request = deletion(provider, arrayOf("e", gone.id))
            val out = audit(cards.drop(1) + request)
            assertEquals(cards.size - 1, out.held(ask).size)
            assertTrue(out.held(Filter(ids = listOf(gone.id))).isEmpty(), "the one its author deleted")
            assertEquals(listOf(request.id), out.held(Filter(kinds = listOf(5))).map { it.id }, "the request is stored, so a re-download is refused")
        }

    @Test
    fun `a kind 5 naming it by someone else deletes nothing`() =
        runBlocking {
            val out = audit(cards.drop(1) + deletion(stranger, arrayOf("e", gone.id)))
            assertEquals(cards.size, out.held(ask).size, "only an event's author may delete it")
            assertTrue(out.held(Filter(kinds = listOf(5))).isEmpty(), "and nobody else's request is evidence")
        }

    @Test
    fun `an address deletion at or after the record removes it`() =
        runBlocking {
            val request = deletion(provider, arrayOf("a", address))
            val out = audit(cards.drop(1) + request)
            assertTrue(out.held(Filter(ids = listOf(gone.id))).isEmpty())
            assertEquals(listOf(request.id), out.held(Filter(kinds = listOf(5))).map { it.id })
        }

    @Test
    fun `an address deletion older than the record leaves it`() =
        runBlocking {
            val out = audit(cards.drop(1) + deletion(provider, arrayOf("a", address), at = gone.createdAt - 60))
            assertEquals(cards.size, out.held(ask).size, "the record is newer than the request that names its address")
        }

    @Test
    fun `a vanish aimed at this relay removes everything by its author`() =
        runBlocking {
            val note = provider.sign<Event>(now - 100, 1, emptyArray(), "hello")
            val profile = provider.sign<Event>(now - 100, 0, emptyArray(), "{}")
            val vanish = provider.sign<Event>(now, 62, arrayOf(arrayOf("relay", self.url)), "")
            val out = audit(cards.drop(1) + vanish, alsoHeld = listOf(note, profile))
            val left = out.held(Filter(authors = listOf(provider.pubKey))).map { it.kind }
            assertEquals(listOf(62), left, "the author is gone from this relay, the request kept")
        }

    @Test
    fun `a vanish aimed at another relay deletes nothing here`() =
        runBlocking {
            val vanish = provider.sign<Event>(now, 62, arrayOf(arrayOf("relay", url.url)), "")
            val out = audit(cards.drop(1) + vanish)
            assertEquals(cards.size, out.held(ask).size, "it asked the peer to forget, not us")
        }

    /** NIP-40 is the store's: it serves no expired record, so the comparison never names one to judge. */
    @Test
    fun `an expired record is never judged, the store omits it`() =
        runBlocking {
            val lapsing = provider.sign<Event>(now - 7_200, 30382, arrayOf(arrayOf("d", subject(99)), arrayOf("expiration", (now - 60).toString())), "")
            val out = audit(cards, alsoHeld = listOf(lapsing))
            assertTrue(out.held(Filter(ids = listOf(lapsing.id))).isEmpty(), "not served")
            assertEquals(cards.size, out.held(ask).size, "and nothing else touched")
            assertEquals(0L, out.audit.missingKept.get(), "never among what the peer lacks")
            assertEquals(0L, out.audit.retractedOnEvidence.get())
        }

    @Test
    fun `a record the peer replaced with a newer version is gone and counted as evidence`() =
        runBlocking {
            val newer = provider.sign<Event>(gone.createdAt + 600, 30382, gone.tags, "")
            val out = audit(cards.drop(1) + newer)
            assertEquals(listOf(newer.id), out.held(Filter(kinds = listOf(30382), authors = listOf(provider.pubKey), tags = mapOf("d" to listOf(subject(1))))).map { it.id })
            assertEquals(1L, out.audit.retractedOnEvidence.get(), "superseded on ingest")
            assertEquals(0L, out.audit.missingKept.get())
        }

    @Test
    fun `a dry run fetches the evidence and applies none of it`() =
        runBlocking {
            val out = audit(cards.drop(1) + deletion(provider, arrayOf("e", gone.id)), audited = stream.copy(deleteMissing = DeleteMissing.DRY_RUN))
            assertEquals(cards.size, out.held(ask).size)
            assertTrue(out.held(Filter(kinds = listOf(5))).isEmpty(), "nothing ingested")
            assertEquals(1L, out.audit.evidenceFetched.get())
        }

    @Test
    fun `the counters say what was found, applied and kept`() =
        runBlocking {
            val out = audit(cards.drop(2) + deletion(provider, arrayOf("e", gone.id)))
            assertEquals(1L, out.audit.evidenceFetched.get())
            assertEquals(1L, out.audit.retractedOnEvidence.get())
            assertEquals(1L, out.audit.missingKept.get(), "the second missing card had no request")
            assertEquals(0L, out.audit.deleted.get(), "nothing on absence")
        }

    /**
     * A request the store already holds for a record it still serves: two writers whose guard caches
     * disagreed let the record in after its tombstone. Its author's own word, so it is applied here too.
     */
    @Test
    fun `a request already stored here deletes a record it names that slipped past it`() =
        runBlocking {
            val request = deletion(provider, arrayOf("e", gone.id))
            val index = InMemoryEventIndex()
            val blind = NostrSemanticsStore(index, relay = self, writers = WriterTopology.SINGLE_WRITER)
            blind.insert(stranger.sign<Event>(now - 10, 1, emptyArray(), "warm the cache"))
            val store = NostrSemanticsStore(index, relay = self)
            store.insert(request)
            blind.batchInsert(cards)
            assertEquals(cards.size, store.query<Event>(ask).size, "the premise: the tombstoned record got in")
            val out = auditOf(store, peerHolds = cards.drop(1))
            assertTrue(out.held(Filter(ids = listOf(gone.id))).isEmpty())
            assertEquals(1L, out.audit.retractedOnEvidence.get())
            assertEquals(0L, out.audit.evidenceFetched.get(), "nothing was asked of the peer for it")
        }
}
