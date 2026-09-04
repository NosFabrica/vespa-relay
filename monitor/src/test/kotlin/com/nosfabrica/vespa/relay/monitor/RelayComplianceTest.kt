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
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Does the answer match the ask. The fake is [Liar], a relay that passes every
 * other check the monitor makes, so only the compliance pass can tell it apart.
 */
class RelayComplianceTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val url = RelayUrlNormalizer.normalize("wss://liar.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** Well below any anchor the pass computes, so nothing here is off-window by accident. */
    private val settled = nowSeconds() - 30L * 24 * 60 * 60

    private fun signed(
        kind: Int,
        at: Long,
        n: Int,
        /** Seconds between events; a corpus packed tighter than [AliasProbe.WINDOW_SLACK_SECONDS] cannot test the cursor. */
        step: Long = 1,
    ): List<Event> = (0 until n).map { events.sign(at - it * step, kind, emptyArray(), "e$kind-$it") }

    // ------------------------------------------------------------------------
    // The rules, with no relay and no socket in sight.
    // ------------------------------------------------------------------------

    private val judge = RelayCompliance()

    private fun tally(
        seen: Int,
        offFilter: Int,
    ) = AliasProbe.Compliance(seen = seen, offKind = offFilter, offFilter = offFilter, kindsAsked = true)

    @Test
    fun `an answer that matches is compliant, and an answer that is mostly not is not`() {
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(tally(seen = 20, offFilter = 0)))
        assertEquals(RelayCompliance.Verdict.NONCOMPLIANT, judge.decide(tally(seen = 20, offFilter = 20)))
    }

    @Test
    fun `nothing came back is not a pass`() {
        // A drain is evidence of nothing about the filter, the same rule as [RelayConsistency.Verdict.UNMEASURABLE].
        assertEquals(RelayCompliance.Verdict.UNMEASURABLE, judge.decide(AliasProbe.Compliance()))
    }

    @Test
    fun `both bars have to be crossed, and each alone is a relay this must not refuse`() {
        // The share alone refuses a thin answer; the count alone refuses a firehose with a stray.
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(tally(seen = 2, offFilter = 1)), "over the share, under the count")
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(tally(seen = 500, offFilter = 3)), "over the count, under the share")
        assertEquals(RelayCompliance.Verdict.NONCOMPLIANT, judge.decide(tally(seen = 20, offFilter = 3)), "over both")

        val strict = RelayCompliance(minOffFilterShare = 0.0, minOffFilterEvents = 1)
        assertEquals(RelayCompliance.Verdict.NONCOMPLIANT, strict.decide(tally(seen = 500, offFilter = 1)))
    }

    @Test
    fun `over-serving the limit is a fact and never a refusal`() {
        // An over-served event still matches the filter; it is measured for bandwidth, not graded on.
        val greedy = AliasProbe.Compliance(seen = 500, overLimit = 490, kindsAsked = true)
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(greedy))
        assertTrue(judge.evidence(greedy).contains("490 beyond the `limit`"), judge.evidence(greedy))
    }

    @Test
    fun `a stray below the bars is still named in the evidence`() {
        // The bars are provisional; the evidence corpus is how they get re-taken.
        val evidence = judge.evidence(tally(seen = 500, offFilter = 3))
        assertTrue(evidence.contains("3 off-filter"), evidence)
        assertTrue(evidence.contains("3 of a kind the filter did not ask for"), evidence)
    }

    @Test
    fun `an event that is both wrong-kind and out-of-window is one off-filter event`() {
        // Adding the two counters would put the share past 1.0 on a relay answering a narrow ask with its newest firehose.
        val both = signed(kind = 7, at = nowSeconds(), n = 10)
        val reading = AliasProbe.Compliance.of(both, limit = 10, until = settled, kinds = listOf(1))
        assertEquals(10, reading.offKind)
        assertEquals(10, reading.offWindow)
        assertEquals(10, reading.offFilter, "ten events, not twenty")
        assertEquals(1.0, judge.share(reading))
    }

    // ------------------------------------------------------------------------
    // The tally, taken on the walk that was happening anyway.
    // ------------------------------------------------------------------------

    /**
     * A relay that answers every REQ with [serves], whatever was asked. Not
     * broken, not slow, not lying about who it is; it simply does not read the
     * filter. With [honoursCursor] it pages properly, so any refusal it earns is
     * about the content of its answers rather than the cursor.
     */
    private inner class Liar(
        val serves: List<Event>,
        val honoursCursor: Boolean = false,
        /**
         * Never answers a bare filter, which is the only way the ladder reaches
         * its `kinds` rung. Silence rather than an empty page: `dialVerdict`
         * breaks on any non-null window, and an empty bare answer ends the climb
         * as a drain.
         */
        val refusesBare: Boolean = false,
    ) {
        var asks = 0

        @Suppress("UNUSED_PARAMETER")
        suspend fun fetch(
            at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            kinds: List<Int>?,
        ): AliasProbe.Page {
            asks++
            if (refusesBare && kinds == null) return AliasProbe.Page(events = null, reason = null)
            if (!honoursCursor) return AliasProbe.Page(serves)
            return AliasProbe.Page(serves.filter { until == null || it.createdAt <= until }.take(want))
        }
    }

    @Test
    fun `the walk counts what the relay served against the filter that asked for it`(): Unit =
        runBlocking {
            // Kind 7 to a `kinds=[1]` ask, stamped now against a settled `until`: every event fails both checks.
            val liar = Liar(signed(kind = 7, at = nowSeconds(), n = 10))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val window = probe.window(url, anchor = settled, kinds = listOf(1)) {}

            assertEquals(10, window.compliance.seen)
            assertEquals(10, window.compliance.offKind)
            assertEquals(10, window.compliance.offWindow)
            assertTrue(window.compliance.kindsAsked)
        }

    @Test
    fun `a bare ask constrains no kind, and page two of a bare walk stays bare`(): Unit =
        runBlocking {
            // A zero here means the question was never put, which must not read as the relay getting it right.
            val liar = Liar(signed(kind = 7, at = settled, n = 10))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val bare = probe.window(url, anchor = settled, kinds = null) {}

            assertEquals(0, bare.compliance.offKind)
            assertTrue(!bare.compliance.kindsAsked, "nothing was asked about kinds, so nothing was learned about them")

            // Page two must be page two of page one: a `kinds=[1]` page two under a bare page one
            // drains honestly on a relay holding no kind 1, and the drain would read as a terminating walk.
            val second = assertNotNull(probe.pageBelow(url, until = settled, kinds = null) {})
            assertEquals(0, second.offKind)
            assertTrue(!second.kindsAsked, "page two of a bare walk is bare, so it learns nothing about kinds either")
        }

    @Test
    fun `the tally is against each page's own cursor, not the anchor the walk started at`(): Unit =
        runBlocking {
            // One second apart: pages after the first carry a cursor the relay supplied and are tallied with no slack,
            // so a relay re-serving its page against a stepped cursor cannot hide inside the anchor's slack.
            val page = signed(kind = 1, at = settled, n = 5)
            val liar = Liar(page)
            val probe = AliasProbe(fetch = liar::fetch, target = 20, page = 5, fallbackPage = 5)
            val window = probe.window(url, anchor = settled, kinds = null) {}

            assertTrue(liar.asks > 1, "the walk has to have paged for this to be measuring anything")
            assertTrue(
                window.compliance.offWindow > 0,
                "every page after the first came back above the cursor it was asked under",
            )
        }

    @Test
    fun `everything the narrow ask downloads goes to ingest, like every other window`(): Unit =
        runBlocking {
            val liar = Liar(signed(kind = 1, at = settled, n = 4))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val delivered = mutableListOf<Event>()
            probe.pageBelow(url, until = settled, kinds = null) { delivered += it }
            assertEquals(4, delivered.size)
        }

    // ------------------------------------------------------------------------
    // The verdict, end to end, against a real record.
    // ------------------------------------------------------------------------

    private suspend fun grade(
        store: NostrSemanticsStore,
        serves: List<Event>,
        honoursCursor: Boolean = false,
        refusesBare: Boolean = false,
    ): Pair<String?, Map<String, Array<String>>> {
        val record = RelayVerdictRecord(store, signer)
        val liar = Liar(serves, honoursCursor, refusesBare)
        FitnessPass(
            record = record,
            probe =
                AliasProbe(
                    fetch = liar::fetch,
                    target = FitnessPass.FITNESS_TARGET,
                    page = FitnessPass.FITNESS_TARGET,
                    fallbackPage = FitnessPass.FITNESS_TARGET,
                    idleMs = { 200L },
                ),
            client = EmptyNostrClient(),
            foldedAway = { emptyMap() },
            inconsistent = { emptySet() },
            progress = Processors().of("fitness"),
        ).measure("compliance", listOf(url), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)

        val published =
            store
                .query<Event>(
                    Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
                ).maxByOrNull { it.createdAt }
        val label = published?.tags?.firstOrNull { it[0] == RelayVerdictRecord.LABEL_TAG }?.getOrNull(1)
        val facts = published?.tags?.associateBy { it[0] }.orEmpty()
        return label to facts
    }

    @Test
    fun `a relay that answers with the wrong kind is refused, however consistently it does it`(): Unit =
        runBlocking {
            // Deep enough that page two has something below page one, and silent on the bare rung so the
            // ladder reaches a `kinds` filter at all. See [Liar.refusesBare].
            val (label, facts) =
                grade(newStore(), signed(kind = 7, at = settled, n = 40), honoursCursor = true, refusesBare = true)

            assertEquals("noncompliant", label)
            val compliant = assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])
            assertEquals("false", compliant[1])
            assertTrue(compliant[2].contains("of a kind the filter did not ask for"), compliant[2])
        }

    @Test
    fun `a relay that serves what it was asked for is prime, and says why on the record`(): Unit =
        runBlocking {
            val (label, facts) = grade(newStore(), signed(kind = 1, at = settled, n = 40), honoursCursor = true)

            assertEquals("prime", label)
            assertEquals("true", assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])[1])
            val pageable = assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])
            assertEquals("true", pageable[1])
            assertTrue(pageable[2].startsWith("walked two pages"), pageable[2])
        }

    @Test
    fun `a page two our own clock cuts costs the FACT and never the verdict`(): Unit =
        runBlocking {
            // A url cut during page two counts as `abandoned`, which feeds the batch guard's blind share;
            // a paging check must not be able to cost a pass its output.
            val store = newStore()
            val serves = signed(kind = 1, at = settled, n = 40)
            val record = RelayVerdictRecord(store, signer)
            val parked =
                java.util.concurrent.atomic
                    .AtomicInteger()
            val fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page = { _, want, until, _ ->
                // Page one is asked at the anchor; page two is asked one below page one's floor, `settled - 20`.
                if (until != null && until < settled - 15) {
                    parked.incrementAndGet()
                    kotlinx.coroutines.CompletableDeferred<AliasProbe.Page>().await()
                } else {
                    AliasProbe.Page(serves.filter { until == null || it.createdAt <= until }.take(want))
                }
            }
            FitnessPass(
                record = record,
                probe =
                    AliasProbe(
                        fetch = fetch,
                        target = FitnessPass.FITNESS_TARGET,
                        page = FitnessPass.FITNESS_TARGET,
                        fallbackPage = FitnessPass.FITNESS_TARGET,
                        idleMs = { 60L },
                    ),
                client = EmptyNostrClient(),
                foldedAway = { emptyMap() },
                inconsistent = { emptySet() },
                progress = Processors().of("fitness"),
            ).measure("cut page two", listOf(url), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)

            val published =
                store
                    .query<Event>(
                        Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
                    ).maxByOrNull { it.createdAt }
            val facts = published?.tags?.associateBy { it[0] }.orEmpty()

            assertTrue(parked.get() > 0, "page two has to have been reached, or this proves nothing")
            assertEquals(
                "prime",
                published?.tags?.firstOrNull { it[0] == RelayVerdictRecord.LABEL_TAG }?.getOrNull(1),
                "the ladder earned this before the second page was asked; our clock does not un-earn it",
            )
            assertNull(facts[RelayVerdictRecord.PAGEABLE_TAG], "…and the fact it could not take is simply absent")
        }

    @Test
    fun `a relay ignoring the cursor entirely is still unpageable, and now says the other half too`(): Unit =
        runBlocking {
            val (label, facts) = grade(newStore(), signed(kind = 1, at = nowSeconds(), n = 20))

            assertEquals("unpageable", label)
            assertEquals("false", assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])[1])
            assertEquals("false", assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])[1])
        }

    @Test
    fun `an empty relay is graded on what it answered and never on what it did not`(): Unit =
        runBlocking {
            val (label, facts) = grade(newStore(), emptyList())

            assertEquals("prime", label)
            assertNull(facts[RelayVerdictRecord.COMPLIANT_TAG], "no events came back, so there is no claim to make")
            assertNull(
                facts[RelayVerdictRecord.PAGEABLE_TAG],
                "#187's second half: `pageable: true` on an empty anchored page was 26% of the 137, and an honest " +
                    "EOSE proves the relay ANSWERS — there was never anything here to page from",
            )
        }

    // ------------------------------------------------------------------------
    // #187 — one page is not a walk.
    // ------------------------------------------------------------------------

    @Test
    fun `a relay that honours the anchor and then ignores the cursor is unpageable`(): Unit =
        runBlocking {
            // Page one is at or below the anchor; page two, asked strictly below it, is the same events again.
            val (label, facts) = grade(newStore(), signed(kind = 1, at = settled, n = 20), honoursCursor = false)

            assertEquals("unpageable", label)
            val pageable = assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])
            assertEquals("false", pageable[1])
            assertTrue(pageable[2].contains("page two"), pageable[2])
        }

    @Test
    fun `the five-minute slack does not apply to a cursor the relay itself supplied`(): Unit =
        runBlocking {
            // Events one second apart, as at a firehose: twenty of them span seconds, so with the anchor's slack
            // applied to page two a cursor-ignoring relay would land inside it every time.
            val page = signed(kind = 1, at = settled, n = 20)
            val liar = Liar(page, honoursCursor = false)
            val probe = AliasProbe(fetch = liar::fetch, target = 20, page = 20, fallbackPage = 20)
            val second = assertNotNull(probe.pageBelow(url, until = settled - 20, kinds = null) {})

            assertEquals(second.seen, second.offWindow, "every event came back above a cursor one second below them")
        }

    @Test
    fun `a page two that drains is the strongest proof of all, and is published as one`(): Unit =
        runBlocking {
            // A relay holding exactly one page: a cursor-ignoring relay could not have drained page two.
            val (label, facts) = grade(newStore(), signed(kind = 1, at = settled, n = 20), honoursCursor = true)

            assertEquals("prime", label)
            val pageable = assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])
            assertEquals("true", pageable[1])
            assertTrue(pageable[2].contains("drained"), pageable[2])
        }
}
