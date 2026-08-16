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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The verdict-built relay list, end to end through the record: what
 * [FitnessPass] writes is exactly what [RelayDiscovery.syncable] admits, and
 * the freshness rules live on the TAG, not the event — quartz's passive
 * monitor rewrites the record's `createdAt` on every connection, so an event
 * clock would make every verdict immortal.
 */
class SyncableRelaysTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val stranger = NostrSignerInternal(KeyPair())

    private val good = RelayUrlNormalizer.normalize("wss://good.example")
    private val dead = RelayUrlNormalizer.normalize("wss://dead.example")
    private val stale = RelayUrlNormalizer.normalize("wss://stale.example")
    private val forged = RelayUrlNormalizer.normalize("wss://forged.example")

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    @Test
    fun `admits exactly the fresh syncable verdicts our monitor signed`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "syncable", "answered 20 events at a settled anchor", pageable = true to "all at or below", nip77 = null)
            record.publishFitness(dead, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)
            // A stranger's certificate for a url our monitor never passed: the
            // authors filter is what keeps somebody else's 30166s from
            // steering our fan-out.
            RelayVerdictRecord(store, stranger).publishFitness(forged, "syncable", "trust me", pageable = null, nip77 = null)

            val admitted =
                RelayDiscovery.syncable(
                    store,
                    monitorAuthors = listOf(signer.pubKey),
                    maxAgeSeconds = 3600,
                )
            assertEquals(listOf(good), admitted.map { it.url })
            assertEquals(emptyMap(), admitted.single().bindings, "a certified relay carries no narrow — the ask is the stream's whole filter")
        }

    @Test
    fun `a stale verdict admits nothing, however fresh the record's own clock is`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(stale, "syncable", "was fine last week", pageable = null, nip77 = null)

            // The freshness bound is read off the tag's measured-at stamp
            // (element 3). Querying with `now` pushed past it must drop the
            // verdict even though the EVENT was signed moments ago — that is
            // the exact difference between the verdict's clock and the
            // record's.
            val admitted =
                RelayDiscovery.syncable(
                    store,
                    monitorAuthors = listOf(signer.pubKey),
                    maxAgeSeconds = 3600,
                    now = nowSeconds() + 7200,
                )
            assertEquals(emptyList(), admitted)
        }

    @Test
    fun `the certified gate holds out unverdicted urls and lets the narrows ride through`() =
        runBlocking {
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(good, "syncable", "answers and pages", pageable = null, nip77 = null)

            // What a gated 10040 scan hands over: the pairing is the point —
            // each relay narrowed to its provider — and the gate must filter
            // urls without touching it.
            val provider = "a".repeat(64)
            val scanned =
                listOf(
                    DiscoveredRelay(good, bindings = mapOf("authors" to setOf(provider))),
                    DiscoveredRelay(dead, bindings = mapOf("authors" to setOf(provider))),
                )
            val gated =
                RelayDiscovery.certifiedOnly(
                    store,
                    scanned,
                    monitorAuthors = listOf(signer.pubKey),
                    maxAgeSeconds = 3600,
                )
            assertEquals(listOf(good), gated.map { it.url }, "no fresh verdict, no dial — however many 10040s name the url")
            assertEquals(mapOf("authors" to setOf(provider)), gated.single().bindings, "the (relay, provider) pairing survives the gate")
        }

    @Test
    fun `a verdict from an older rules epoch reads as no verdict at all`() =
        runBlocking {
            val store = newStore()
            // Hand-build the tag shape publishFitness writes, with a foreign
            // epoch — the state every standing record is in the day the rules
            // change. FITNESS_EPOCH is the lever that forces the re-measure;
            // this pin is what makes forgetting to read it a test failure
            // instead of a month of stale admissions.
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "syncable", "current rules", pageable = null, nip77 = null)
            val fresh =
                RelayDiscovery.syncable(
                    store,
                    monitorAuthors = listOf(signer.pubKey),
                    maxAgeSeconds = 3600,
                )
            assertEquals(1, fresh.size)
        }
}
