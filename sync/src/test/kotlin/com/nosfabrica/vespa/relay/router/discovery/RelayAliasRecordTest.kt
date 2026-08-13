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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
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
class RelayAliasRecordTest {
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
            val record = RelayAliasRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            // The monitor sees this url again — an ordinary dial, on the passive
            // path that runs every time a connection opens. It rewrites the same
            // address, and the only thing keeping our verdict alive is that it
            // edits rather than rebuilds.
            monitor.recordProbed(mapOf(alias to 120L), emptySet(), nowSeconds() + 1)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(
                RelayAliasRecord.SAME_AS_TAG in after,
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
            val record = RelayAliasRecord(store, signer)

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
            val record = RelayAliasRecord(store, signer)
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
            val record = RelayAliasRecord(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publishDistinct(alias, sampled = 500, comparedAgainst = "1 compared peer(s)", bestShared = 3)

            val held = record.load(listOf(alias))
            assertEquals(setOf(alias), held.distinct)
            assertTrue(held.aliases.isEmpty(), "the stale fold outlived the verdict that replaced it")
            assertEquals(1, recordFor(store, alias.url)?.tags?.count { it.firstOrNull() == RelayAliasRecord.SAME_AS_TAG })
        }

    @Test
    fun `a cleared verdict carries the evidence it rests on`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)

            record.publishDistinct(alias, sampled = 500, comparedAgainst = "19 compared peer(s)", bestShared = 2)

            assertEquals(
                "500 newest events, best 2 shared with 19 compared peer(s)",
                recordFor(store, alias.url)?.tags?.first { it.firstOrNull() == RelayAliasRecord.SAME_AS_TAG }?.get(2),
            )
        }

    @Test
    fun `a monitor observation survives the fold's verdict`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)

            monitor.recordProbed(mapOf(alias to 120L), emptySet(), 1_700_000_000L)
            val before = tagNames(recordFor(store, alias.url))
            record.publish(alias, canonical, sampled = 500, shared = 498)

            val after = tagNames(recordFor(store, alias.url))
            assertTrue(
                after.containsAll(before - RelayAliasRecord.SAME_AS_TAG),
                "publishing the verdict dropped the monitor's tags: had $before, left $after",
            )
            assertTrue(RelayAliasRecord.SAME_AS_TAG in after)
        }

    @Test
    fun `re-probing replaces the verdict rather than appending a second one`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)

            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publish(alias, canonical, sampled = 500, shared = 500)

            val held = recordFor(store, alias.url)
            assertEquals(1, held?.tags?.count { it.firstOrNull() == RelayAliasRecord.SAME_AS_TAG })
            assertEquals(
                "500 newest events, 500 shared with ${canonical.url}",
                held?.tags?.first { it.firstOrNull() == RelayAliasRecord.SAME_AS_TAG }?.get(2),
            )
        }

    @Test
    fun `an edit lands even when the record it replaces is newer than the clock`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            val monitor = RelayReachabilityStore(store, signer)
            // A record stamped well into the future — a peer's clock, or simply
            // two writers inside the same second. `now` alone would be rejected
            // as older and the edit would vanish into a caught exception.
            monitor.recordProbed(mapOf(alias to 99L), emptySet(), nowSeconds() + 3_600)

            val written = record.publish(alias, canonical, sampled = 500, shared = 500)

            assertTrue(written != null, "the edit was silently lost to replaceable-event ordering")
            assertTrue(RelayAliasRecord.SAME_AS_TAG in tagNames(recordFor(store, alias.url)))
        }

    @Test
    fun `an edit keeps tags it does not own, whoever wrote them`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
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
    fun `the verdict is readable back as an alias`() =
        runBlocking {
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            record.publish(alias, canonical, sampled = 500, shared = 498)

            assertEquals(mapOf(alias to canonical), record.load(listOf(alias)).aliases)
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
                                RelayAliasRecord.SAME_AS_TAG,
                                canonical.url,
                                "500 newest events, 498 shared with ${canonical.url}",
                                nowSeconds().toString(),
                                "1",
                            ),
                        )
                    },
                ),
            )

            val held = RelayAliasRecord(store, signer).load(listOf(alias))
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
                        add(arrayOf(RelayAliasRecord.SAME_AS_TAG, canonical.url, "old-style, no timestamp"))
                    },
                ),
            )

            val held = RelayAliasRecord(store, signer).load(listOf(alias))
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
            val record = RelayAliasRecord(store, signer)
            record.publish(alias, canonical, sampled = 500, shared = 498)
            record.publishConsistency(canonical, consistent = true, first = 500, second = 500, shared = 500, score = 1.0)

            assertEquals(mapOf(alias to canonical), record.load(listOf(alias)).aliases)
            assertEquals(setOf(canonical), record.load(listOf(canonical)).stable)
        }
}
