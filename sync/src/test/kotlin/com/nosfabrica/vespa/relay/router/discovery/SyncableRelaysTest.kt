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
import com.nosfabrica.vespa.relay.router.config.RelaySelect
import com.nosfabrica.vespa.relay.router.config.ResultFilter
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The verdict-built relay list, end to end through the record: what
 * [FitnessPass] writes is exactly what [RelayDiscovery.syncable] admits, on
 * the terms the rest of the NIP-66 ecosystem reads a record by — the event's
 * own clock for freshness, the `s` tag for the verdict, and nothing private in
 * between. Both of the private parts that used to be here are gone: the
 * passive writer that made `createdAt` mean "we talked recently" rather than
 * "we checked", and the rules epoch every reader had to know about, now
 * retracted at the source by [FitnessPass.retireStaleEpochs].
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
    fun `freshness is the record's own clock`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(stale, "syncable", "was fine last week", pageable = null, nip77 = null)

            // A monitor republishes the record when it re-checks, so `created_at`
            // dates the check — the reading every other NIP-66 consumer applies,
            // and the one this could not use while quartz's passive watcher was
            // rewriting the record for every socket the fan-out opened. Pushing
            // `now` past the bound must drop the verdict.
            assertEquals(
                emptyList(),
                RelayDiscovery.syncable(
                    store,
                    monitorAuthors = listOf(signer.pubKey),
                    maxAgeSeconds = 3600,
                    now = nowSeconds() + 7200,
                ),
            )
            // …and inside it, admit it.
            assertEquals(
                listOf(stale),
                RelayDiscovery
                    .syncable(store, monitorAuthors = listOf(signer.pubKey), maxAgeSeconds = 3600)
                    .map { it.url },
            )
        }

    @Test
    fun `a dead verdict is held out, and only a dead one`() =
        runBlocking {
            // The hold-out read. `dead` is the transport saying no; every other
            // refusal was earned by ANSWERING, and holding those out would stop
            // the fold and the stability gate from re-measuring the very relays
            // they exist to judge.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(dead, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)
            record.publishFitness(good, "unpageable", "ignored `until`", pageable = false to "ignored", nip77 = null)
            record.publishFitness(stale, "alias", "folds onto wss://good.example", pageable = null, nip77 = null)

            assertEquals(
                setOf(dead),
                RelayDiscovery.undialable(store, monitorAuthors = listOf(signer.pubKey), maxAgeSeconds = 3600),
            )
        }

    @Test
    fun `a stranger cannot hold a relay out`() =
        runBlocking {
            // The asymmetry the roster read does NOT follow. Admitting unscoped
            // costs a dial; holding out unscoped is permanent — held out of the
            // candidate set a url is never re-measured, so the mark never
            // clears. Anyone whose 30166s we mirror could starve a relay for
            // good, so this read stays author-bound however the roster reads.
            val store = newStore()
            RelayVerdictRecord(store, stranger)
                .publishFitness(forged, "dead", "trust me", pageable = null, nip77 = null)

            assertEquals(
                emptySet(),
                RelayDiscovery.undialable(store, monitorAuthors = listOf(signer.pubKey), maxAgeSeconds = 3600),
            )
            assertEquals(
                emptySet(),
                RelayDiscovery.undialable(store, monitorAuthors = emptyList(), maxAgeSeconds = 3600),
                "no signer and no named monitors is no standing to call anything dead — never every author",
            )
        }

    @Test
    fun `a gate reads urls out of any filter, and unions its entries`() =
        runBlocking {
            // `resultsFilteredBy` is ordinary discovery, not a verified path:
            // one entry over our own 30166s, one over a hand-curated relay
            // list, and a url either appears in the union or does not.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "syncable", "answers and pages", pageable = null, nip77 = null)
            // A refusal on the same kind, from the same monitor: the gate's
            // `#s` has to be doing the work, not the kind alone.
            record.publishFitness(dead, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)
            store.insert(
                NostrSignerInternal(KeyPair()).sign(
                    EventTemplate(nowSeconds(), 10002, arrayOf(arrayOf("r", stale.url, "write")), ""),
                ),
            )

            val verdictGate =
                ResultFilter(
                    selects = listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)),
                    filter = Filter(kinds = listOf(30166), tags = mapOf("s" to listOf("syncable"))),
                )
            val listGate =
                ResultFilter(
                    selects = listOf(RelaySelect(kind = 10002, tag = "r", urlIndex = 1)),
                    filter = Filter(kinds = listOf(10002)),
                )
            assertEquals(
                setOf(good),
                RelayDiscovery.urlsMatching(store, listOf(verdictGate)),
                "the `dead` record is the same kind from the same monitor — only `#s` separates them",
            )
            assertEquals(setOf(stale), RelayDiscovery.urlsMatching(store, listOf(listGate)))
            assertEquals(
                setOf(good, stale),
                RelayDiscovery.urlsMatching(store, listOf(verdictGate, listGate)),
                "entries union — the intersection is with the scan, not between the gates",
            )
        }

    @Test
    fun `a gate bounds freshness on the event's own clock`() =
        runBlocking {
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(good, "syncable", "answers and pages", pageable = null, nip77 = null)
            val gate =
                ResultFilter(
                    selects = listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)),
                    filter = Filter(kinds = listOf(30166), tags = mapOf("s" to listOf("syncable"))),
                    maxAgeSeconds = 3600,
                )
            assertEquals(setOf(good), RelayDiscovery.urlsMatching(store, listOf(gate)))
            assertEquals(
                emptySet(),
                RelayDiscovery.urlsMatching(store, listOf(gate), now = nowSeconds() + 7200),
                "`maxAgeSeconds` becomes the `since` a config cannot write for itself",
            )
        }

    @Test
    fun `a verdict from an older rules epoch is retracted, not filtered on read`() =
        runBlocking {
            // The state every standing record is in the day the rules change.
            // It used to be a check every reader ran, which put our private
            // versioning in front of everybody's records; the claim is ours,
            // so we withdraw it and the read stays a plain question.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "syncable", "current rules", pageable = null, nip77 = null)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", stale.url),
                            arrayOf("s", "syncable", "measured under rules we have since changed", nowSeconds().toString(), "0"),
                        ),
                        "",
                    ),
                ),
            )
            assertEquals(
                setOf(good, stale),
                RelayDiscovery
                    .syncable(store, monitorAuthors = listOf(signer.pubKey), maxAgeSeconds = 3600)
                    .mapTo(HashSet()) { it.url },
                "the read does not know an epoch exists — that is the point",
            )

            assertEquals(1, FitnessPass.retireStaleEpochs(store, record, signer.pubKey))
            assertEquals(
                listOf(good),
                RelayDiscovery
                    .syncable(store, monitorAuthors = listOf(signer.pubKey), maxAgeSeconds = 3600)
                    .map { it.url },
                "retracted at the source, the url reads as one nobody has measured",
            )
            // Idempotent: a second boot has nothing left to take back.
            assertEquals(0, FitnessPass.retireStaleEpochs(store, record, signer.pubKey))
        }

    @Test
    fun `retiring a verdict leaves every other writer's tags alone`() =
        runBlocking {
            // The record is shared. Withdrawing our fitness claim must not
            // withdraw the fold's `same-as` or anyone else's tag with it —
            // the same rule `publishFitness` follows, on the way out.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", good.url),
                            arrayOf("s", "syncable", "old rules", nowSeconds().toString(), "0"),
                            arrayOf("same-as", "wss://canonical.example", "fold evidence", nowSeconds().toString(), "2"),
                        ),
                        "",
                    ),
                ),
            )
            FitnessPass.retireStaleEpochs(store, record, signer.pubKey)

            val tags =
                store
                    .query<com.vitorpamplona.quartz.nip01Core.core.Event>(
                        Filter(kinds = listOf(30166), authors = listOf(signer.pubKey)),
                    ).single()
                    .tags
                    .map { it[0] }
            assertEquals(listOf("d", "same-as"), tags, "our tag left; the fold's stayed")
        }
}
