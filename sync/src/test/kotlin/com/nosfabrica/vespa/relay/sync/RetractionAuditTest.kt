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
import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyIdDiff
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** What the retraction audit may claim: a band is verified only by a comparison it saw through to the end. */
class RetractionAuditTest {
    private val relay = RelayUrlNormalizer.normalize("wss://provider.example")
    private val author = "a".repeat(64)

    private val stream =
        SyncStream(
            name = "scores",
            dir = SyncDirection.DOWN,
            filter = Filter(kinds = listOf(30382)),
            urls = emptyList(),
            trusted = false,
            negentropySyncThePastSeconds = 86_400,
            deleteMissing = DeleteMissing.DRY_RUN,
            ownedKinds = setOf(30382),
        )
    private val ask = Filter(kinds = listOf(30382), authors = listOf(author))

    /** A relay that reconciles cleanly and owes us one id; how it answers the by-id ask is the variable. */
    private class Relay(
        private val answersInFull: Boolean,
    ) : RetractionReads {
        var asked = 0

        override suspend fun reconcileIds(
            url: NormalizedRelayUrl,
            filter: Filter,
            local: List<IdAndTime>,
        ) = NegentropyIdDiff(needIds = listOf("b".repeat(64)), haveIds = emptyList(), windows = 1)

        override suspend fun fetchIds(
            url: NormalizedRelayUrl,
            ids: List<String>,
            onEvent: suspend (Event) -> Unit,
        ): Boolean {
            asked++
            return answersInFull
        }
    }

    private val period = stream.negentropySyncThePastSeconds!!
    private val spacing = SyncBands.attemptSpacingSeconds(period)

    /** One audit of [ask] against [reads], recorded into [bands]; returns the owned ask the clock is kept on. */
    private fun audit(
        reads: Relay,
        bands: SyncBands,
    ): Filter {
        val scope = CoroutineScope(SupervisorJob())
        try {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val audit =
                RetractionAudit(
                    reads = reads,
                    store = store,
                    bands = bands,
                    ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1), null, null, scope, null, null),
                    refusedIds = RefusedIds.disabled(),
                )
            val before = reads.asked
            runBlocking { audit.reconcileAndDelete(stream, relay, ask, sharedAuthors = emptySet()) }
            assertEquals(before + 1, reads.asked, "the premise: the diff owed an id, so a by-id ask went out")
            return assertNotNull(audit.ownedAskOf(stream, ask))
        } finally {
            scope.cancel()
        }
    }

    private fun verifiedAfter(reads: Relay): Long? {
        val bands = SyncBands(null)
        return bands.verifiedAt(stream.name, relay, audit(reads, bands))
    }

    @Test
    fun `an audit that keeps ending short backs off and never waits past its own period`() {
        val bands = SyncBands(null)
        val short = Relay(answersInFull = false)
        val owned = audit(short, bands)
        val t = nowSeconds()
        assertFalse(bands.claimAudit(stream.name, relay, owned, period, now = t + spacing + 60), "one short audit already waits past the plain spacing")

        repeat(4) { audit(short, bands) }
        val after = nowSeconds()
        assertFalse(bands.claimAudit(stream.name, relay, owned, period, now = t + period - 120), "five in a row wait longer still")
        assertTrue(bands.claimAudit(stream.name, relay, owned, period, now = after + period + 60), "but never longer than the audit's own period")
    }

    @Test
    fun `a restart does not re-run an audit its backoff is holding off`() {
        val f = File.createTempFile("sync-bands", ".json").also { it.delete() }
        try {
            val owned = SyncBands(f).use { audit(Relay(answersInFull = false), it) }
            val rebooted = SyncBands(f)
            assertFalse(rebooted.claimAudit(stream.name, relay, owned, period, now = nowSeconds() + 60), "the backoff is on disk, not only in memory")
            assertNull(rebooted.verifiedAt(stream.name, relay, owned), "and it claims no coverage the download never earned")
        } finally {
            f.delete()
        }
    }

    @Test
    fun `a verified audit starts the backoff over`() {
        val bands = SyncBands(null)
        val owned = audit(Relay(answersInFull = false), bands)
        audit(Relay(answersInFull = true), bands)
        assertEquals(1, bands.noteIncompleteAudit(stream.name, relay, owned), "the next short audit is the first in a row again")
    }

    @Test
    fun `a by-id fetch that stalls or is refused leaves the band unverified`() {
        assertNull(verifiedAfter(Relay(answersInFull = false)), "an id never answered for is not a mirrored one")
    }

    /** A relay that no longer serves one of our ids, and sends back [recovered] for the one it owes us. */
    private class Retracting(
        private val recovered: Event?,
    ) : RetractionReads {
        override suspend fun reconcileIds(
            url: NormalizedRelayUrl,
            filter: Filter,
            local: List<IdAndTime>,
        ) = NegentropyIdDiff(needIds = listOf("b".repeat(64)), haveIds = listOf("c".repeat(64)), windows = 1)

        override suspend fun fetchIds(
            url: NormalizedRelayUrl,
            ids: List<String>,
            onEvent: suspend (Event) -> Unit,
        ): Boolean {
            recovered?.let { onEvent(it) }
            return true
        }
    }

    /** Records [reads] deleted in one enforcing audit, through an ingest that is never started. */
    private fun deletedBy(reads: Retracting): Long {
        val scope = CoroutineScope(SupervisorJob())
        try {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val audit =
                RetractionAudit(
                    reads = reads,
                    store = store,
                    bands = SyncBands(null),
                    ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1), null, null, scope, null, null),
                    refusedIds = RefusedIds.disabled(),
                )
            val enforcing = stream.copy(deleteMissing = DeleteMissing.ON)
            runBlocking { audit.reconcileAndDelete(enforcing, relay, ask, sharedAuthors = emptySet(), verdictWaitMs = 200) }
            return audit.deleted.get()
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `an audit whose recovered events the store never answers deletes nothing`() {
        val recovered = Event("b".repeat(64), author, nowSeconds(), 30382, emptyArray(), "", "d".repeat(128))
        assertEquals(0L, deletedBy(Retracting(recovered)), "a store that has not taken what we recovered is not one to delete from")
        assertEquals(1L, deletedBy(Retracting(recovered = null)), "the premise: with every verdict in, the same audit deletes")
    }

    @Test
    fun `a by-id fetch answered in full verifies the band`() {
        assertNotNull(verifiedAfter(Relay(answersInFull = true)), "a clean comparison and a full answer earn the claim")
    }
}
