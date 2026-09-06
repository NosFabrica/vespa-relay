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
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayReachabilityStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The kind-30166 record is addressable on the relay url, so the monitor and the
 * fold write the same slot; every edit must keep the other writer's tags.
 */
class RelayVerdictRecordTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val canonical = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val signer = NostrSignerInternal(KeyPair())

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    private suspend fun recordFor(
        store: NostrSemanticsStore,
        url: String,
    ): Event? =
        store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url))),
            ).maxByOrNull { it.createdAt }

    private fun tagNames(event: Event?): Set<String> =
        event
            ?.tags
            ?.mapNotNull { it.firstOrNull() }
            ?.toSet()
            .orEmpty()

    @Test
    fun `the fold's verdict survives the monitor's next dial`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            // The passive path, run every time a connection opens: it rewrites the same address.
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds() + 1)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(
                RelayVerdictRecord.SAME_AS_TAG in after,
                "the monitor erased the fold's verdict: left $after",
            )
            assertTrue("rtt-open" in after, "the monitor recorded nothing: left $after")
        }

    @Test
    fun `a cleared url reads back as cleared, not as a fold onto itself`() =
        runBlocking {
            // A self-fold in `aliases` would have the fan-out resolve the url to itself.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publishDistinct(alias, sampled = 500, comparedAgainst = "3 compared peer(s)", bestShared = 2)

            val held = record.load(listOf(alias))
            assertEquals(setOf(alias), held.distinct)
            assertTrue(held.aliases.isEmpty(), "a cleared url was read back as an alias: ${held.aliases}")
        }

    @Test
    fun `the two forms are told apart after normalisation, not by string`() =
        runBlocking {
            // Compared as strings, `wss://nos.lol` cleared reads as a fold onto `wss://nos.lol/`.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val unslashed = RelayUrlNormalizer.normalize("wss://nos.lol")

            record.publishDistinct(unslashed, sampled = 500, comparedAgainst = "2 compared peer(s)", bestShared = 0)

            assertEquals(setOf(canonical), record.load(listOf(canonical)).distinct)
        }

    @Test
    fun `clearing a url replaces an earlier fold on the same address`() =
        runBlocking {
            // One owned tag name, so the newer verdict replaces the older rather than contradicting it.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publishDistinct(alias, sampled = 500, comparedAgainst = "1 compared peer(s)", bestShared = 3)

            val held = record.load(listOf(alias))
            assertEquals(setOf(alias), held.distinct)
            assertTrue(held.aliases.isEmpty(), "the stale fold outlived the verdict that replaced it")
            assertEquals(1, recordFor(store, alias.url)?.tags?.count { it.firstOrNull() == RelayVerdictRecord.SAME_AS_TAG })
        }

    @Test
    fun `a cleared verdict carries the evidence it rests on`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publishDistinct(alias, sampled = 500, comparedAgainst = "19 compared peer(s)", bestShared = 2)

            assertEquals(
                "500 newest events, best 2 shared with 19 compared peer(s)",
                recordFor(store, alias.url)?.tags?.first { it.firstOrNull() == RelayVerdictRecord.SAME_AS_TAG }?.get(2),
            )
        }

    @Test
    fun `a monitor observation survives the fold's verdict`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            monitor.recordProbed(mapOf(alias to 120L), emptySet(), 1_700_000_000L)
            val before = tagNames(recordFor(store, alias.url))
            record.publish(alias, canonical, sampled = 500, shared = 498)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(
                after.containsAll(before - RelayVerdictRecord.SAME_AS_TAG),
                "publishing the verdict dropped the monitor's tags: had $before, left $after",
            )
            assertTrue(RelayVerdictRecord.SAME_AS_TAG in after)
        }

    @Test
    fun `re-probing replaces the verdict rather than appending a second one`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publish(alias, canonical, sampled = 500, shared = 500)

            val held = recordFor(store, alias.url)
            assertEquals(1, held?.tags?.count { it.firstOrNull() == RelayVerdictRecord.SAME_AS_TAG })
            assertEquals(
                "500 newest events, 500 shared with ${canonical.url}",
                held?.tags?.first { it.firstOrNull() == RelayVerdictRecord.SAME_AS_TAG }?.get(2),
            )
        }

    @Test
    fun `an edit lands even when the record it replaces is newer than the clock`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)
            // A record stamped in the future: a peer's clock, or two writers inside one second.
            monitor.recordProbed(mapOf(alias to 99L), emptySet(), nowSeconds() + 3_600)

            val written = record.publish(alias, canonical, sampled = 500, shared = 500)

            assertTrue(written != null, "the edit was silently lost to replaceable-event ordering")
            assertTrue(RelayVerdictRecord.SAME_AS_TAG in tagNames(recordFor(store, alias.url)))
        }

    @Test
    fun `an edit keeps tags it does not own, whoever wrote them`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds())
            val theirs = tagNames(recordFor(store, alias.url)) - "d"

            record.publish(alias, canonical, sampled = 500, shared = 500)
            record.publish(alias, canonical, sampled = 500, shared = 499)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(after.containsAll(theirs), "an edit dropped $theirs, leaving $after")
        }

    @Test
    fun `the fitness writer owns its own label namespace and nobody else's`() =
        runBlocking {
            // `l` is shared ground, so ownership is a predicate over the namespace, not the tag name.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", alias.url),
                            arrayOf("l", "CA", "countryCode"),
                            arrayOf("L", "countryCode"),
                            arrayOf("l", "prime", RelayVerdictRecord.FITNESS_NAMESPACE, "an older pass", nowSeconds().toString(), "1"),
                        ),
                        "",
                    ),
                ),
            )
            record.publishFitness(alias, "dead", "no TCP answer", pageable = null, nip77 = null)

            val tags = recordFor(store, alias.url)!!.tags
            val labels = tags.filter { it[0] == "l" }.map { it[1] to it.getOrNull(2) }
            assertEquals(
                setOf("CA" to "countryCode", "dead" to RelayVerdictRecord.FITNESS_NAMESPACE),
                labels.toSet(),
                "our grade was replaced; the country label was not touched",
            )
            assertEquals(
                setOf("countryCode", RelayVerdictRecord.FITNESS_NAMESPACE),
                tags.filter { it[0] == "L" }.map { it[1] }.toSet(),
                "and both namespaces are still declared — ours re-stated, theirs carried",
            )
        }

    @Test
    fun `the NIP-77 measurement reads back, and an unmeasured relay is not a no`(): Unit =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(canonical, "prime", "answered", pageable = null, nip77 = true to "answered a NEG-OPEN")
            record.publishFitness(alias, "prime", "answered", pageable = null, nip77 = false to "no NEG-OPEN")
            record.publishFitness(self, "prime", "answered", pageable = null, nip77 = null)

            val read = record.load(listOf(canonical, alias, self)).speaksNegentropy
            assertEquals(true, read[canonical])
            assertEquals(false, read[alias], "a measured refusal is a verdict, and the audit must not be attempted")
            assertEquals(null, read[self], "unmeasured is not a verdict — the ask tries and finds out")
        }

    @Test
    fun `a label carrying no namespace is dropped rather than carried forever`() =
        runBlocking {
            // Nobody can attribute it, so there is no writer it could be taken from.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(arrayOf("d", alias.url), arrayOf("l", "orphan"), arrayOf("L"), arrayOf("l", "CA", "countryCode")),
                        "",
                    ),
                ),
            )
            record.publishFitness(alias, "prime", "answered", pageable = null, nip77 = null)

            val tags = recordFor(store, alias.url)!!.tags
            assertTrue(tags.none { it[0] == "l" && it.size < 3 }, "a namespace-less label survived: ${tags.map { it.toList() }}")
            assertTrue(tags.none { it[0] == "L" && it.size < 2 })
            assertTrue(tags.any { it[0] == "l" && it.getOrNull(2) == "countryCode" }, "and the attributable one is untouched")
        }

    @Test
    fun `the measured facts are replaced on every pass, not accumulated`() =
        runBlocking {
            // Carried forward, a stale `rtt-open` draws as a current reading of a socket nobody has opened.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(
                alias,
                "prime",
                "answered",
                pageable = null,
                nip77 = null,
                facts = RelayFacts(network = "clearnet", rttOpenMs = 40, software = "strfry", version = "1.0.3"),
            )
            assertEquals("40", recordFor(store, alias.url)!!.tags.single { it[0] == "rtt-open" }[1])

            // A pass that learned nothing clears them rather than leaving the old reading beside a new verdict.
            record.publishFitness(alias, "dead", "no TCP answer", pageable = null, nip77 = null)
            val names = tagNames(recordFor(store, alias.url))
            for (gone in listOf("rtt-open", "s", "n")) {
                assertTrue(gone !in names, "`$gone` outlived the dial that measured it: $names")
            }
        }

    @Test
    fun `the grade carries its clock and rules one place right of the fold's`() =
        runBlocking {
            // NIP-32 spends index 2 on the namespace, so the house shape sits at 3/4/5 here and 2/3/4 on `same-as`.
            val store = newStore()
            RelayVerdictRecord(store, signer)
                .publishFitness(alias, "prime", "answered 20 events", pageable = null, nip77 = null)

            val grade = recordFor(store, alias.url)!!.tags.single { it[0] == "l" }
            assertEquals("prime", grade[1])
            assertEquals(RelayVerdictRecord.FITNESS_NAMESPACE, grade[RelayVerdictRecord.LABEL_NAMESPACE_INDEX])
            assertEquals("answered 20 events", grade[3])
            assertTrue(grade[RelayVerdictRecord.LABEL_MEASURED_AT_INDEX].toLong() > 0)
            assertEquals(RelayVerdictRecord.FITNESS_EPOCH, grade[RelayVerdictRecord.LABEL_EPOCH_INDEX])
        }

    @Test
    fun `the verdict is readable back as an alias`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publish(alias, canonical, sampled = 500, shared = 498)

            assertEquals(mapOf(alias to canonical), record.load(listOf(alias)).aliases)
        }

    @Test
    fun `a verdict still reads as CURRENT after the other writers have edited the record`() =
        runBlocking {
            // Other writers copy the tag forward, so this asserts what `load` decides, not what tags exist.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds() + 1)
            // An edit of ours that owns the other tag, which carries this one forward the same way.
            record.publishConsistency(alias, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)

            val held = record.load(listOf(alias))
            assertEquals(
                mapOf(alias to canonical),
                held.aliases,
                "the verdict survived as a tag but stopped reading as current — its clock or its rules version was dropped",
            )
            assertEquals(setOf(alias), held.consistent, "the stability verdict did not survive its own round trip")
        }

    @Test
    fun `a verdict measured under superseded rules is no verdict at all`() =
        runBlocking {
            // Fresh measured-at, so only the epoch can make it stale.
            val store = newStore()
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(alias, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayVerdictRecord.SAME_AS_TAG,
                                canonical.url,
                                "500 newest events, 498 shared with ${canonical.url}",
                                nowSeconds().toString(),
                                "1",
                            ),
                        )
                    },
                ),
            )

            val held = RelayVerdictRecord(store, signer).load(listOf(alias))
            assertTrue(
                held.aliases.isEmpty() && held.distinct.isEmpty(),
                "a verdict from superseded rules was still being acted on: ${held.aliases}${held.distinct}",
            )
        }

    @Test
    fun `a verdict written before the epoch existed is re-measured, not trusted forever`() =
        runBlocking {
            // The event's clock is rewritten on every connect, so a fallback to it would never age out.
            val store = newStore()
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(alias, "", nowSeconds()) {
                        add(arrayOf(RelayVerdictRecord.SAME_AS_TAG, canonical.url, "old-style, no timestamp"))
                    },
                ),
            )

            val held = RelayVerdictRecord(store, signer).load(listOf(alias))
            assertTrue(held.aliases.isEmpty(), "a pre-epoch verdict on a freshly rewritten record read as current")
        }

    @Test
    fun `what this build publishes reads back as current`() =
        runBlocking {
            // A constant bumped on the writer's side only would re-probe the whole fan-out every pass.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publishConsistency(canonical, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)

            assertEquals(mapOf(alias to canonical), record.load(listOf(alias)).aliases)
            assertEquals(setOf(canonical), record.load(listOf(canonical)).consistent)
        }
}
