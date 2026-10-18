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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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

    private fun verifiedAfter(reads: Relay): Long? {
        val scope = CoroutineScope(SupervisorJob())
        try {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val bands = SyncBands(null)
            val audit =
                RetractionAudit(
                    reads = reads,
                    store = store,
                    bands = bands,
                    ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1), null, null, scope, null, null),
                    refusedIds = RefusedIds.disabled(),
                )
            runBlocking { audit.reconcileAndDelete(stream, relay, ask, sharedAuthors = emptySet()) }
            assertEquals(1, reads.asked, "the premise: the diff owed an id, so a by-id ask went out")
            return bands.verifiedAt(stream.name, relay, assertNotNull(audit.ownedAskOf(stream, ask)))
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `a by-id fetch that stalls or is refused leaves the band unverified`() {
        assertNull(verifiedAfter(Relay(answersInFull = false)), "an id never answered for is not a mirrored one")
    }

    @Test
    fun `a by-id fetch answered in full verifies the band`() {
        assertNotNull(verifiedAfter(Relay(answersInFull = true)), "a clean comparison and a full answer earn the claim")
    }
}
