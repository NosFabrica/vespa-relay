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
 * One relay url, one kind-30166 address, two writers.
 *
 * NIP-66's relay record is ADDRESSABLE on `d` = the relay url, so the passive
 * monitor (which updates a record every time a connection is opened or used)
 * and the fold (which adds what a probe proved) are aiming at the same slot.
 * A writer that rebuilds the record from only its own tags silently erases the
 * other's, and the loss is invisible: the event is still there, still signed,
 * still parses.
 *
 * These tests hold the merge from both directions.
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
            // The monitor sees this url again — an ordinary dial, on the passive
            // path that runs every time a connection opens. It rewrites the same
            // address, and the only thing keeping our verdict alive is that it
            // edits rather than rebuilds.
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds() + 1)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(
                RelayVerdictRecord.SAME_AS_TAG in after,
                "the monitor erased the fold's verdict: left $after",
            )
            // And it did write what it came to write, so this is a merge rather
            // than a monitor that gave up on the record.
            assertTrue("rtt-open" in after, "the monitor recorded nothing: left $after")
        }

    @Test
    fun `a cleared url reads back as cleared, not as a fold onto itself`() =
        runBlocking {
            // The self-form: `same-as` pointing at the record's own url. It has
            // to come back in `distinct` and never in `aliases`, or the fan-out
            // would resolve the url to itself through a verdict and the whole
            // point — not re-probing it — would be lost.
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
            // `wss://nos.lol` and `wss://nos.lol/` are one url and the store
            // holds whichever the normalizer produced. Comparing the raw strings
            // would read a cleared verdict as a fold of a url onto itself, which
            // `RelayAliases.adopt` silently drops — leaving the url unmeasured
            // and re-probed every pass, the exact bug this half exists to fix.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val unslashed = RelayUrlNormalizer.normalize("wss://nos.lol")

            record.publishDistinct(unslashed, sampled = 500, comparedAgainst = "2 compared peer(s)", bestShared = 0)

            assertEquals(setOf(canonical), record.load(listOf(canonical)).distinct)
        }

    @Test
    fun `clearing a url replaces an earlier fold on the same address`() =
        runBlocking {
            // A host that split one endpoint into two real relays: the url used
            // to be a duplicate and is not any more. One owned tag name means
            // the newer verdict replaces the older rather than sitting beside it
            // and contradicting it.
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
    fun `an attempt that decided nothing is written down as an attempt, not as a verdict`() =
        runBlocking {
            // THE THIRD VALUE, and the reason it is a value of `self-consistent`
            // rather than a tag of its own: it answers the same question. What
            // this monitor thinks of a relay's stability is `true`, `false`, or
            // "we asked and could not tell", and the third one is the state four
            // thousand urls of a discovered corpus are actually in.
            //
            // It must NEVER read back as `false`. That verdict costs a relay its
            // place in the fan-out, and nothing writes it from silence.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publishUnmeasured(alias, "the TLS handshake failed")

            val held = record.load(listOf(alias))
            assertEquals(mapOf(alias to "the TLS handshake failed"), held.unmeasured)
            assertTrue(held.inconsistent.isEmpty(), "an attempt is not a refusal: ${held.inconsistent}")
            assertTrue(held.consistent.isEmpty())
        }

    @Test
    fun `an attempt that decided nothing takes an aged-out verdict away with it`() =
        runBlocking {
            // The url answered once and cannot be re-measured now. Leaving the
            // old `true` standing beside a note about why it could not be
            // re-measured would have the record asserting a verdict and
            // explaining its own absence at the same time — so [edit]'s
            // ownership takes it, which is the whole reason this rides the tag
            // it does.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publishConsistency(alias, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)
            record.publishUnmeasured(alias, "never answered a REQ")

            val held = record.load(listOf(alias))
            assertTrue(held.consistent.isEmpty(), "the stale verdict outlived the attempt that replaced it")
            assertEquals(mapOf(alias to "never answered a REQ"), held.unmeasured)
            assertEquals(
                1,
                recordFor(store, alias.url)?.tags?.count { it.firstOrNull() == RelayVerdictRecord.SELF_CONSISTENT_TAG },
                "one answer per url, replaced rather than appended",
            )
        }

    @Test
    fun `an attempt with nothing to say is not read back as one`() =
        runBlocking {
            // The reason IS the state — a row that cannot say why it could not
            // decide has nothing a card could draw and nothing a reader could
            // act on, so it is dropped rather than counted namelessly.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)

            record.publishUnmeasured(alias, "")

            assertTrue(record.load(listOf(alias)).unmeasured.isEmpty())
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
            // A record stamped well into the future — a peer's clock, or simply
            // two writers inside the same second. `now` alone would be rejected
            // as older and the edit would vanish into a caught exception.
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

            // Two edits later, everything the other writer put there is intact.
            val after = tagNames(recordFor(store, alias.url))
            assertTrue(after.containsAll(theirs), "an edit dropped $theirs, leaving $after")
        }

    @Test
    fun `the fitness writer owns its own label namespace and nobody else's`() =
        runBlocking {
            // `l` IS SHARED GROUND — nostr.watch labels the same relay with its
            // country, ISP and ASN, all on `l` — which is the whole reason
            // ownership here is a predicate over the NAMESPACE rather than the
            // tag name. Owning the name would delete every other labeller's
            // work on each sweep, which is the mistake this record type exists
            // to make impossible.
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
            // The pass has always published this; the sync plane reads it now,
            // and what it decides is whether an ask is reconciled or re-fetched.
            // Three states, and the third is the one worth a test: a url NOBODY
            // measured must not read as "does not speak negentropy", or the
            // router would give up NIP-77 for every relay a monitor has not
            // reached yet.
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
            // Nobody can attribute it — not us, not the monitor that wrote it —
            // so there is no writer it could be taken from, and leaving it
            // costs a tag on the record for the life of the address.
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
            // The residue this is written against: `n` and `rtt-open` written
            // by a passive monitor that no longer exists, carried forward by
            // every edit since, and drawn on the stats panel as current
            // readings of a socket nobody had opened in months.
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

            // A pass that learned nothing must CLEAR them rather than leave the
            // old reading standing beside a new verdict.
            record.publishFitness(alias, "dead", "no TCP answer", pageable = null, nip77 = null)
            val names = tagNames(recordFor(store, alias.url))
            for (gone in listOf("rtt-open", "s", "n")) {
                assertTrue(gone !in names, "`$gone` outlived the dial that measured it: $names")
            }
        }

    @Test
    fun `the grade carries its clock and rules one place right of the fold's`() =
        runBlocking {
            // NIP-32 spends index 2 on the namespace, so the house shape sits
            // at 3/4/5 here and 2/3/4 on `same-as`. A reader using the fold's
            // offsets would date every grade by its own evidence string.
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
            // The tag-name check two tests up is not enough, and the gap it
            // leaves is the one that would hurt most. Expiry now lives in the
            // tag's 4th and 5th elements, and every other writer on this
            // address — quartz's passive monitor, our own stability pass —
            // carries our tag forward by copying it. If any of them rebuilt it
            // at a fixed arity, the tag would survive with its NAME and lose its
            // clock and its rules version, which reads as a stale verdict: the
            // url would be re-fingerprinted every pass, forever, while the
            // record on screen looked perfectly healthy.
            //
            // So this asserts what `load` DECIDES, not what the tags are called.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds() + 1)
            // And an edit of ours that owns the OTHER tag, which carries this
            // one forward the same way.
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
            // The forcing lever, and the only one that works on records already
            // signed. The fold's rules have changed repeatedly — comparing a
            // host's urls to each other rather than only to whichever one led is
            // the largest of them — and a record from before such a change is
            // not an old reading of today's rule, it is a reading of a different
            // rule. Waiting out the TTL does not make it agree; it only means a
            // month of applying conclusions we would no longer draw.
            //
            // Written HERE with a fresh measured-at, so nothing but the epoch can
            // be what makes it stale.
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
            // Every record signed before this element existed, which on a live
            // store is all of them. They carry no measured-at either, and THAT is
            // the trap: the reader used to fall back to the event's clock, which
            // quartz's monitor rewrites every time we connect — so a verdict on
            // any relay still in the fan-out could never age out under any TTL.
            // Measure once, trust forever, with no way to force otherwise short
            // of deleting records.
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
            // The other half of the two above: the epoch check must reject the
            // past and nothing else. A constant that disagreed with the writer —
            // bumped on one side of the file only — would silently re-probe the
            // entire fan-out every pass and never converge, and every test here
            // that goes through `publish` would still pass.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publishConsistency(canonical, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)

            assertEquals(mapOf(alias to canonical), record.load(listOf(alias)).aliases)
            assertEquals(setOf(canonical), record.load(listOf(canonical)).consistent)
        }
}
