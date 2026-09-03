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
 * DOES THE ANSWER MATCH THE ASK — the check the stability gate structurally
 * cannot make, and the relay it exists for.
 *
 * The fake at the centre of this file is `Liar`: a relay that answers every REQ
 * with the SAME events regardless of what was asked. It is deliberately built
 * to pass everything else the monitor measures — it is reachable, it answers
 * promptly, it EOSEs, its events are real and signed, and asked the same filter
 * twice it returns exactly the same window, so [RelayConsistency] scores it
 * 1.000 and certifies it. Every assertion here is about the one pass that can
 * tell it apart from a relay.
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
        /**
         * Seconds between events, and a parameter because
         * [AliasProbe.WINDOW_SLACK_SECONDS] is real: a corpus packed one second
         * apart sits inside the slack whatever the cursor says, so a test about
         * the cursor has to spread wider than the slack to be about anything.
         */
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
        // A drain is the relay's honest answer to a narrow ask and it is
        // evidence of NOTHING about the filter — the same rule
        // [RelayConsistency.Verdict.UNMEASURABLE] carries. Publishing
        // `compliant true` off an empty page would put our signature to a claim
        // no event supports.
        assertEquals(RelayCompliance.Verdict.UNMEASURABLE, judge.decide(AliasProbe.Compliance()))
    }

    @Test
    fun `both bars have to be crossed, and each alone is a relay this must not refuse`() {
        // The share alone would refuse a thin answer: one wrong event out of
        // two is 0.500, and a relay that served two events has told us almost
        // nothing. The count alone would refuse a firehose: three wrong out of
        // five hundred is 0.006, which is a stray and not a policy.
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(tally(seen = 2, offFilter = 1)), "over the share, under the count")
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(tally(seen = 500, offFilter = 3)), "over the count, under the share")
        assertEquals(RelayCompliance.Verdict.NONCOMPLIANT, judge.decide(tally(seen = 20, offFilter = 3)), "over both")

        // …and both are the PASS's to set, which is what makes the probe that
        // has to measure them able to sweep a bar without a second copy of the
        // rules living inside it.
        val strict = RelayCompliance(minOffFilterShare = 0.0, minOffFilterEvents = 1)
        assertEquals(RelayCompliance.Verdict.NONCOMPLIANT, strict.decide(tally(seen = 500, offFilter = 1)))
    }

    @Test
    fun `over-serving the limit is a fact and never a refusal`() {
        // An over-served event MATCHES the filter — it was simply not asked for
        // yet. It costs bandwidth, which is why it is measured; it does not make
        // the answer wrong, which is why it cannot cost the relay its place.
        val greedy = AliasProbe.Compliance(seen = 500, overLimit = 490, kindsAsked = true)
        assertEquals(RelayCompliance.Verdict.COMPLIANT, judge.decide(greedy))
        assertTrue(judge.evidence(greedy).contains("490 beyond the `limit`"), judge.evidence(greedy))
    }

    @Test
    fun `a stray below the bars is still named in the evidence`() {
        // The bars are provisional — see the class header — and this is how they
        // get re-taken: a corpus of records saying how far short of the line
        // each relay fell is the measurement. A record that only said
        // `compliant true` would have thrown it away.
        val evidence = judge.evidence(tally(seen = 500, offFilter = 3))
        assertTrue(evidence.contains("3 off-filter"), evidence)
        assertTrue(evidence.contains("3 of a kind the filter did not ask for"), evidence)
    }

    @Test
    fun `an event that is both wrong-kind and out-of-window is one off-filter event`() {
        // Adding the two counters would put the share past 1.0 on exactly the
        // relay this exists for — the one answering a narrow ask with its
        // newest firehose, where every event fails both checks at once.
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
     * A relay that answers every REQ with [serves], whatever was asked.
     *
     * The point of the fake: it is not broken, not slow and not lying about who
     * it is. It simply does not read the filter.
     */
    private inner class Liar(
        val serves: List<Event>,
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
            return AliasProbe.Page(serves)
        }
    }

    @Test
    fun `the walk counts what the relay served against the filter that asked for it`(): Unit =
        runBlocking {
            // Kind 7 to a `kinds=[1]` ask, stamped now against a settled
            // `until`: every event fails both per-event checks, and the walk
            // reports it without a second REQ, because the events were already
            // in hand.
            val liar = Liar(signed(kind = 7, at = nowSeconds(), n = 10))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val window = probe.window(url, anchor = settled, kinds = listOf(1)) {}

            assertEquals(10, window.compliance.seen)
            assertEquals(10, window.compliance.offKind)
            assertEquals(10, window.compliance.offWindow)
            assertTrue(window.compliance.kindsAsked)
        }

    @Test
    fun `a bare ask constrains no kind, and the tally says so rather than reporting a clean sheet`(): Unit =
        runBlocking {
            // THE HOLE THE NARROW ASK EXISTS FOR. Most relays answer the bare
            // rung, and a bare filter cannot be violated on `kinds` — so a zero
            // here means the question was never put, which is not the same
            // finding as "the relay got it right" and must not read as one.
            val liar = Liar(signed(kind = 7, at = settled, n = 10))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val bare = probe.window(url, anchor = settled, kinds = null) {}

            assertEquals(0, bare.compliance.offKind)
            assertTrue(!bare.compliance.kindsAsked, "nothing was asked about kinds, so nothing was learned about them")

            // …and the narrow ask closes it, on the same relay, in one REQ.
            val narrow = probe.complianceAsk(url, anchor = settled) {}
            assertEquals(10, narrow.offKind)
            assertTrue(narrow.kindsAsked)
        }

    @Test
    fun `the tally is against each page's own cursor, not the anchor the walk started at`(): Unit =
        runBlocking {
            // `until` steps down as a walk pages backwards. An event above page
            // two's cursor is the cursor being ignored even when it sits below
            // the anchor — and a tally kept against the anchor alone scores that
            // relay clean. The `Liar` above serves the SAME page every time, so
            // page two is asked with a lower `until` and answered with the same
            // events, every one of them now above it.
            val page = signed(kind = 1, at = settled, n = 5, step = 3600)
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
            // The bargain the whole probe is built on: a check that also syncs.
            // Nothing is fetched twice to pay for a verdict.
            val liar = Liar(signed(kind = 1, at = settled, n = 4))
            val probe = AliasProbe(fetch = liar::fetch, target = 10, page = 10, fallbackPage = 10)
            val delivered = mutableListOf<Event>()
            probe.complianceAsk(url, anchor = settled) { delivered += it }
            assertEquals(4, delivered.size)
        }

    // ------------------------------------------------------------------------
    // The verdict, end to end, against a real record.
    // ------------------------------------------------------------------------

    private suspend fun grade(
        store: NostrSemanticsStore,
        serves: List<Event>,
    ): Pair<String?, Map<String, Array<String>>> {
        val record = RelayVerdictRecord(store, signer)
        val liar = Liar(serves)
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
            // THE RELAY THIS WHOLE FILE IS FOR. Asked the same filter twice it
            // answers identically, so the stability gate scores it 1.000 and
            // certifies it; asked ANYTHING it answers with kind 7. Before this
            // check the monitor published `prime` about it and every visit-mode
            // stream dialled it forever.
            val (label, facts) = grade(newStore(), signed(kind = 7, at = settled, n = 20))

            assertEquals("noncompliant", label)
            val compliant = assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])
            assertEquals("false", compliant[1])
            assertTrue(compliant[2].contains("of a kind the filter did not ask for"), compliant[2])
        }

    @Test
    fun `a relay that serves what it was asked for is prime, and says why on the record`(): Unit =
        runBlocking {
            val (label, facts) = grade(newStore(), signed(kind = 1, at = settled, n = 20))

            assertEquals("prime", label)
            assertEquals("true", assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])[1])
            assertEquals("true", assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])[1])
        }

    @Test
    fun `a relay ignoring the cursor entirely is still unpageable, and now says the other half too`(): Unit =
        runBlocking {
            // The older verdict is unchanged — a walk against this cannot
            // terminate, which is a fact about paging and not about filters —
            // and the compliance tag now rides beside it saying what the events
            // were.
            val (label, facts) = grade(newStore(), signed(kind = 1, at = nowSeconds(), n = 20))

            assertEquals("unpageable", label)
            assertEquals("false", assertNotNull(facts[RelayVerdictRecord.PAGEABLE_TAG])[1])
            assertEquals("false", assertNotNull(facts[RelayVerdictRecord.COMPLIANT_TAG])[1])
        }

    @Test
    fun `an empty relay is graded on what it answered and never on what it did not`(): Unit =
        runBlocking {
            // A drain is honest. It is `prime` — it answered, at a settled
            // anchor, with the truth — and it carries NO compliance fact,
            // because there was nothing to check.
            val (label, facts) = grade(newStore(), emptyList())

            assertEquals("prime", label)
            assertNull(facts[RelayVerdictRecord.COMPLIANT_TAG], "no events came back, so there is no claim to make")
        }
}
