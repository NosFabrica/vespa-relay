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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
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
                    newestVersions =
                        if (!probe) {
                            null
                        } else {
                            { kind, authors ->
                                index
                                    .search(EventQuery(kinds = listOf(kind), authors = authors))
                                    .groupBy { it.pubkey }
                                    .mapValues { (_, docs) ->
                                        docs
                                            .maxWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })
                                            .let { Version(it.createdAt, it.id) }
                                    }
                            }
                        },
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
            // Every kind, not just the notes: the supersession cases store
            // kind 0, and a filter that cannot see them would read as "nothing
            // was written" for the wrong reason.
            val stored = store.count(Filter()).toLong()
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

    // ---- supersession ------------------------------------------------------

    /** kind 0 for [author] at [at] — a later `at` is a NEWER version of the SAME address, with a different id. */
    private fun profile(
        author: NostrSignerSync,
        at: Long,
    ): Event = author.sign(at, 0, emptyArray(), """{"name":"a","at":$at}""")

    @Test
    fun `a replaceable the store already beats is dropped without being verified`() {
        val people = (0 until 200).map { NostrSignerSync() }
        val newest = people.map { profile(it, 1_700_001_000L) }
        // Older versions of the same addresses, forged. Different ids, so the
        // id probe cannot see them — only the version probe can. If they were
        // verified, the breakdown would say so.
        val stale = people.map { forge(profile(it, 1_700_000_000L)) }

        val (pipeline, _, stored) = ingest(preload = newest, offer = stale)

        val breakdown = pipeline.rejectionBreakdown()
        assertFalse(breakdown.contains("bad signature"), "verified a version the store already beats: $breakdown")
        assertTrue(breakdown.contains("replaced"), "expected the store's own wording for it, got: $breakdown")
        assertEquals(200, pipeline.rejected.get())
        assertEquals(0, pipeline.accepted.get())
        assertEquals(200, stored, "the newer versions must be the only ones stored")
    }

    @Test
    fun `a NEWER replaceable is never dropped by the version probe`() {
        val people = (0 until 200).map { NostrSignerSync() }
        val older = people.map { profile(it, 1_700_000_000L) }
        val newer = people.map { profile(it, 1_700_001_000L) }

        val (pipeline, _, stored) = ingest(preload = older, offer = newer)

        assertEquals(200, pipeline.accepted.get(), pipeline.rejectionBreakdown())
        assertEquals(200, stored, "one document per address — the newer version replaced the older")
    }

    @Test
    fun `of several versions in one batch only the newest is verified and written`() {
        val people = (0 until 200).map { NostrSignerSync() }
        // Three generations per address, all in one batch, oldest first. Only
        // the newest can survive NIP-01, so the other two must never be
        // verified — they are forged, which is how the test can tell.
        val offer =
            people.flatMap {
                listOf(
                    forge(profile(it, 1_700_000_000L)),
                    forge(profile(it, 1_700_000_500L)),
                    profile(it, 1_700_001_000L),
                )
            }

        val (pipeline, _, stored) = ingest(preload = emptyList(), offer = offer)

        assertFalse(pipeline.rejectionBreakdown().contains("bad signature"), pipeline.rejectionBreakdown())
        assertEquals(200, pipeline.accepted.get())
        assertEquals(400, pipeline.rejected.get())
        assertEquals(200, stored)
    }

    @Test
    fun `an addressable is left to the store, whose version query the router does not reproduce`() {
        val author = NostrSignerSync()
        // kind 30382 at one address (same d tag), older then newer. The router
        // must not touch these: dropping one on a query shape it got wrong
        // would be a lost event, not a slow one.
        val d = arrayOf(arrayOf("d", "rank"))
        val older = author.sign<Event>(1_700_000_000L, 30382, d, "old")
        val newer = author.sign<Event>(1_700_001_000L, 30382, d, "new")

        val (pipeline, _, _) = ingest(preload = listOf(newer), offer = listOf(older), probe = false)

        // Rejected by the STORE as replaced, having been verified — the
        // behaviour that existed before the supersession pre-filter.
        assertEquals(1, pipeline.rejected.get())
        assertTrue(pipeline.rejectionBreakdown().contains("replaced"), pipeline.rejectionBreakdown())
    }

    @Test
    fun `a batch carrying a deletion keeps every version, because position decides their fate`() {
        val author = NostrSignerSync()
        val v1 = profile(author, 1_700_000_000L)
        val v2 = profile(author, 1_700_001_000L)
        // v2 lands on its own tombstone and is rejected; v1 is what survives.
        // Collapsing the batch to "v2, the newest" would leave this address
        // EMPTY — so the collapse must stand down for a batch like this.
        val delete = author.sign<Event>(1_700_002_000L, 5, arrayOf(arrayOf("e", v2.id)), "")

        val (pipeline, store, _) = ingest(preload = emptyList(), offer = listOf(v1, delete, v2))

        val kept = runBlocking { store.query<Event>(Filter(kinds = listOf(0))) }
        assertEquals(listOf(v1.id), kept.map { it.id }, "the replay's outcome was changed by collapsing the batch")
        assertTrue(pipeline.accepted.get() >= 2, pipeline.rejectionBreakdown())
    }

    private companion object {
        /** Long enough that only a hang reaches it, so a slow box fails no test. */
        const val SETTLE_TIMEOUT_MS = 30_000
    }
}
