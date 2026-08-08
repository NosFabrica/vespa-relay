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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Verification is the most expensive thing ingest does per event (~48µs of
 * schnorr) and a mirror is offered the same event once per relay that holds it.
 * These pin the property that makes that affordable: **an event the store
 * already holds is dropped without being verified**, and — the other half,
 * which is what keeps that safe — **nothing unverified is ever written**.
 */
class IngestDedupTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    /** The same event with its signature replaced by junk: same id, unverifiable. */
    private fun forge(event: Event) =
        Event(
            id = event.id,
            pubKey = event.pubKey,
            createdAt = event.createdAt,
            kind = event.kind,
            tags = event.tags,
            content = event.content,
            sig = "f".repeat(128),
        )

    /**
     * Runs one batch through a pipeline over a real in-memory store, wired to
     * the same existence check production hands it. [preload] goes into the
     * store first; [offer] is what the upstream then delivers.
     */
    private fun ingest(
        preload: List<Event>,
        offer: List<Event>,
        probe: Boolean = true,
    ): Triple<IngestPipeline, NostrSemanticsStore, Long> =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            preload.forEach { store.insert(it) }
            val scope = CoroutineScope(Job())
            val pipeline =
                IngestPipeline(
                    store,
                    // One worker: two would split the offer into halves that
                    // each fall under the probe's width gate, and this asserts
                    // what ONE batch does.
                    RouterConfig(connectionTimeoutSec = 10, streams = emptyList(), ingestConcurrency = 1),
                    audit = null,
                    servingPressure = null,
                    scope = scope,
                    knownIds = if (probe) index::existingIds else null,
                )
            // Queued BEFORE the workers start, so the whole offer is drained as
            // one batch — this asserts what a batch does, and a batch split
            // three ways would test the channel instead.
            offer.forEach { pipeline.submit(it, skipVerify = false) }
            pipeline.start()
            // Every offered event lands in exactly one of the two counters —
            // accepted, or rejected by dedup, by verify, or by the store — so
            // their sum is the settled condition. A fixed sleep here would be a
            // guess about a loaded CI box.
            var waitedMs = 0
            while (pipeline.accepted.get() + pipeline.rejected.get() < offer.size && waitedMs < SETTLE_TIMEOUT_MS) {
                delay(5)
                waitedMs += 5
            }
            val stored = store.count(Filter(kinds = listOf(1))).toLong()
            scope.cancel()
            pipeline.close()
            Triple(pipeline, store, stored)
        }

    @Test
    fun `an event the store already holds is dropped without being verified`() {
        // Wide enough to earn the probe round trip, which is the case that
        // matters: this is the fan-out, not a live tail.
        val held = (0 until 200).map { note(it) }
        // Every offered copy is FORGED — same ids, junk signatures. If the
        // pipeline verified them it would say `bad signature`; dropping them as
        // duplicates is the proof it never looked.
        val (pipeline, _, stored) = ingest(preload = held, offer = held.map { forge(it) })

        val breakdown = pipeline.rejectionBreakdown()
        assertFalse(breakdown.contains("bad signature"), "verified an event it already held: $breakdown")
        assertTrue(breakdown.contains("duplicate"), "expected the store's own duplicate wording, got: $breakdown")
        assertEquals(200, pipeline.rejected.get())
        assertEquals(0, pipeline.accepted.get())
        assertEquals(200, stored, "the held events must still be the only ones stored")
    }

    @Test
    fun `copies of one event inside a batch cost a single verification`() {
        val real = (0 until 150).map { note(it) }
        // Each event once, then every one of them again — the shape a fan-out
        // across two relays delivers. The repeats are forged, so a second
        // verification would be visible.
        val (pipeline, _, stored) = ingest(preload = emptyList(), offer = real + real.map { forge(it) })

        assertFalse(
            pipeline.rejectionBreakdown().contains("bad signature"),
            "verified a copy of an event already in the same batch",
        )
        assertEquals(150, pipeline.accepted.get())
        assertEquals(150, pipeline.rejected.get())
        assertEquals(150, stored)
    }

    @Test
    fun `a bad signature on an event we do NOT hold is still rejected and never written`() {
        val fresh = (0 until 200).map { note(it) }
        val (pipeline, _, stored) = ingest(preload = emptyList(), offer = fresh.map { forge(it) })

        assertTrue(pipeline.rejectionBreakdown().contains("bad signature"), pipeline.rejectionBreakdown())
        assertEquals(200, pipeline.rejected.get())
        assertEquals(0, pipeline.accepted.get())
        assertEquals(0, stored, "an unverified event reached the store")
    }

    @Test
    fun `with no probe wired the pipeline still ingests, verifying every copy`() {
        val fresh = (0 until 50).map { note(it) }
        val (pipeline, _, stored) = ingest(preload = emptyList(), offer = fresh, probe = false)

        assertEquals(50, pipeline.accepted.get())
        assertEquals(50, stored)
    }

    private companion object {
        /** Long enough that only a hang reaches it, so a slow box fails no test. */
        const val SETTLE_TIMEOUT_MS = 30_000
    }
}
