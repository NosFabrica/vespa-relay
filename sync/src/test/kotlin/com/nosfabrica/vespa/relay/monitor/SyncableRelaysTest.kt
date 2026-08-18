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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.shared.RelayDiscovery
import com.nosfabrica.vespa.relay.shared.RelayVerdictRecord
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The verdict-built relay list, end to end through the record: what
 * [FitnessPass] writes is what a stream admits, on the terms the rest of the
 * NIP-66 ecosystem reads a record by — the event's own clock for freshness,
 * the `s` tag for the verdict, and nothing private in between.
 *
 * Everything private is gone, and with it the separate read that enforced it.
 * A verdict query has no verified path of its own any more: it is a source
 * whose select is NIP-66's `d` tag, run through [RelayDiscovery.discover] like
 * a 10002 scan, so these tests go through that too.
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

    /** The `d`-tag select the loader writes for a kind-30166 source. */
    private fun verdictSource(
        authors: List<String>,
        maxAgeSeconds: Long,
        verdict: String,
    ) = RelaySource(
        selects = listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)),
        filter =
            Filter(
                kinds = listOf(30166),
                authors = authors.takeIf { it.isNotEmpty() },
                tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(verdict)),
            ),
        maxAgeSeconds = maxAgeSeconds,
    )

    private suspend fun admitted(
        store: NostrSemanticsStore,
        authors: List<String> = emptyList(),
        maxAgeSeconds: Long = 3600,
        now: Long = nowSeconds(),
        verdict: String = "prime",
    ): List<NormalizedRelayUrl> =
        RelayDiscovery
            .discover(
                store,
                RelayDiscoveryConfig(
                    sources = listOf(verdictSource(authors, maxAgeSeconds, verdict)),
                    refreshSeconds = 3600,
                    exclude = RelayExcludes.NONE,
                ),
                now = now,
            ).map { it.url }

    @Test
    fun `admits exactly the fresh prime grades our monitor signed`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "prime", "answered 20 events at a settled anchor", pageable = true to "all at or below", nip77 = null)
            record.publishFitness(dead, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)
            // A stranger's certificate for a url our monitor never passed: the
            // authors filter is what keeps somebody else's 30166s from
            // steering our fan-out.
            RelayVerdictRecord(store, stranger).publishFitness(forged, "prime", "trust me", pageable = null, nip77 = null)

            assertEquals(listOf(good), admitted(store, authors = listOf(signer.pubKey)))
            assertEquals(
                setOf(good, forged),
                admitted(store).toSet(),
                "unscoped admits every monitor in the store — the operator's absent `authors`, honoured",
            )
        }

    @Test
    fun `freshness is the record's own clock`() =
        runBlocking {
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(stale, "prime", "was fine last week", pageable = null, nip77 = null)

            // A monitor republishes the record when it re-checks, so `created_at`
            // dates the check — the reading every other NIP-66 consumer applies,
            // and the one this could not use while quartz's passive watcher was
            // rewriting the record for every socket the fan-out opened.
            assertEquals(listOf(stale), admitted(store, authors = listOf(signer.pubKey)))
            assertEquals(
                emptyList(),
                admitted(store, authors = listOf(signer.pubKey), now = nowSeconds() + 7200),
                "`maxAgeSeconds` becomes the `since` a config cannot write for itself",
            )
        }

    @Test
    fun `a refusal is not an admission`() =
        runBlocking {
            // Only the label VALUE separates these: same kind, same monitor, same clock.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "prime", "answers and pages", pageable = null, nip77 = null)
            record.publishFitness(dead, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)

            assertEquals(listOf(good), admitted(store, authors = listOf(signer.pubKey)))
            assertEquals(listOf(dead), admitted(store, authors = listOf(signer.pubKey), verdict = "dead"))
        }

    @Test
    fun `a dead verdict is held out, and only a dead one`() =
        runBlocking {
            // The hold-out read — the one place a typed read survives, because
            // it is not config-driven. `dead` is the transport saying no; every
            // other refusal was earned by ANSWERING, and holding those out would
            // stop the fold and the stability gate from re-measuring the very
            // relays they exist to judge.
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
    fun `a verdict from an older rules epoch is retracted, not filtered on read`() =
        runBlocking {
            // The state every standing record is in the day the rules change.
            // It used to be a check every reader ran, which put our private
            // versioning in front of everybody's records; the claim is ours,
            // so we withdraw it and the read stays a plain question.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(good, "prime", "current rules", pageable = null, nip77 = null)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", stale.url),
                            // NIP-32's shape: value, namespace, then the
                            // house's evidence / measured-at / epoch, one
                            // place right of where the fold carries them.
                            arrayOf(
                                "l",
                                "prime",
                                RelayVerdictRecord.FITNESS_NAMESPACE,
                                "measured under rules we have since changed",
                                nowSeconds().toString(),
                                "0",
                            ),
                        ),
                        "",
                    ),
                ),
            )
            assertEquals(
                setOf(good, stale),
                admitted(store, authors = listOf(signer.pubKey)).toSet(),
                "the read does not know an epoch exists — that is the point",
            )

            assertEquals(1, FitnessPass.retireStaleEpochs(store, record, signer.pubKey))
            assertEquals(
                listOf(good),
                admitted(store, authors = listOf(signer.pubKey)),
                "retracted at the source, the url reads as one nobody has measured",
            )
            // Idempotent: a second boot has nothing left to take back.
            assertEquals(0, FitnessPass.retireStaleEpochs(store, record, signer.pubKey))

            // A label carrying one of our VALUES under somebody else's
            // namespace is not a stale grade of ours. The tag index answers on
            // the value alone, so the query returns these; retiring on one
            // would edit a record we hold no verdict on.
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(arrayOf("d", "wss://labelled.example/"), arrayOf("l", "prime", "somebody.else")),
                        "",
                    ),
                ),
            )
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
                            arrayOf("l", "prime", RelayVerdictRecord.FITNESS_NAMESPACE, "old rules", nowSeconds().toString(), "0"),
                            // Another labeller on the same record. `l` is
                            // shared ground — nostr.watch puts country and ASN
                            // labels here — so owning the NAME would delete
                            // theirs on every sweep.
                            arrayOf("l", "CA", "countryCode"),
                            arrayOf("same-as", "wss://canonical.example", "fold evidence", nowSeconds().toString(), "2"),
                        ),
                        "",
                    ),
                ),
            )
            FitnessPass.retireStaleEpochs(store, record, signer.pubKey)

            val tags =
                store
                    .query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey)))
                    .single()
                    .tags
                    .map { it[0] }
            assertEquals(listOf("d", "l", "same-as"), tags, "our label left; the fold's tag and the foreign label stayed")
            val labels =
                store
                    .query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey)))
                    .single()
                    .tags
                    .filter { it[0] == "l" }
            assertEquals(listOf("CA"), labels.map { it[1] }, "the label that survived is the one under somebody else's namespace")
        }

    @Test
    fun `a grade still written on the s tag is retracted at boot`() =
        runBlocking {
            // THE MIGRATION. Every record this deployment signed before the
            // move carries its grade on `s` — 4,000 of 4,000, measured — and
            // `s` is the SOFTWARE field to every other NIP-66 reader. Left
            // alone it does not merely stop being read as a grade: it is read
            // as the relay running a piece of software called `dead`, and
            // `edit` would carry it forward forever.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", good.url),
                            // `syncable`, NOT `prime`: the fixture has to be
                            // what the OLD build signed, or it tests the
                            // migration against records that never existed.
                            // It did, and the migration missed the only grade
                            // that admits anything.
                            arrayOf("s", "syncable", "answered at a settled anchor", nowSeconds().toString(), "1"),
                            arrayOf("same-as", "wss://canonical.example", "fold evidence", nowSeconds().toString(), "2"),
                        ),
                        "",
                    ),
                ),
            )
            assertEquals(1, FitnessPass.retireLegacyGrades(store, record, signer.pubKey))

            val tags =
                store
                    .query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey)))
                    .single()
                    .tags
                    .map { it[0] }
            assertEquals(listOf("d", "same-as"), tags, "the stale grade left `s` free for the software string")
            assertEquals(
                emptyList(),
                admitted(store, authors = listOf(signer.pubKey)),
                "and the url reads as unmeasured, which is what gets it re-graded",
            )
            // Idempotent, and it stays cheap once no legacy records remain.
            assertEquals(0, FitnessPass.retireLegacyGrades(store, record, signer.pubKey))
        }

    @Test
    fun `every word the old build could have signed is one the migration looks for`() =
        runBlocking {
            // The pin that would have caught the miss. `syncable` is the word
            // that changed, so it is the one a query derived from today's
            // `Verdict` cannot ask for — and it was the admitting grade, which
            // makes it both the largest group in the store and the only one
            // whose survival keeps a relay wrongly out of a roster.
            assertTrue("syncable" in FitnessPass.LEGACY_GRADES, "the previous admitting grade must stay findable")
            for (verdict in FitnessPass.Verdict.entries) {
                assertTrue(verdict.value in FitnessPass.LEGACY_GRADES, "`${verdict.value}` is spelled the same today and must still be swept")
            }

            // …and it holds end to end, one record per legacy word.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            for ((i, word) in FitnessPass.LEGACY_GRADES.withIndex()) {
                store.insert(
                    signer.sign(
                        EventTemplate(
                            nowSeconds(),
                            30166,
                            arrayOf(arrayOf("d", "wss://legacy$i.example/"), arrayOf("s", word, "old", nowSeconds().toString(), "1")),
                            "",
                        ),
                    ),
                )
            }
            assertEquals(
                FitnessPass.LEGACY_GRADES.size,
                FitnessPass.retireLegacyGrades(store, record, signer.pubKey),
                "a grade left on `s` is published as the relay's software until it is retracted",
            )
        }

    @Test
    fun `a relay genuinely running software we grade by is not mistaken for a legacy record`() =
        runBlocking {
            // The migration queries `s` for OUR vocabulary, so the one way it
            // can be wrong is a relay whose software is honestly called
            // `alias`. It is signed by somebody else here, which is the same
            // boundary every other read on this record uses.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(arrayOf("d", good.url), arrayOf("s", "git+https://github.com/hoytech/strfry.git")),
                        "",
                    ),
                ),
            )
            assertEquals(0, FitnessPass.retireLegacyGrades(store, record, signer.pubKey), "a repository url is not one of our grades")
            val tags = store.query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey))).single().tags
            assertEquals(listOf("d", "s"), tags.map { it[0] }, "the software string is left exactly where it is")
        }
}
