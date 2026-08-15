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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The stability gate, which is the only thing here that REMOVES a relay from a
 * fan-out on the router's own evidence.
 *
 * That makes the asymmetry the point of nearly every test below: a wrong
 * "inconsistent" silently stops mirroring a working relay and nobody notices,
 * while a wrong "consistent" costs one relay's worth of duplicate downloads
 * until the next monthly re-measure. So the bar for excluding has to be a
 * positive measurement and nothing less — not silence, not a thin window, not a
 * failed store read.
 */
class RelayConsistencyTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val steady = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val shuffler = RelayUrlNormalizer.normalize("wss://fiatjaf.com")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    private fun ids(
        from: Int,
        count: Int,
    ): Set<String> = (from until from + count).map { "id$it" }.toSet()

    @Test
    fun `two identical answers are consistent`() {
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 500), ids(0, 500)))
    }

    @Test
    fun `a truncated but agreeing answer is still the same answer`() {
        // Containment, not equality: a relay may serve one of the two asks more
        // shallowly at its own default_limit, and a prefix of the same answer is
        // the same answer. This is the case that must not be excluded.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 100), ids(0, 500)))
    }

    @Test
    fun `the measured shuffler is refused and the measured steady relays are not`() {
        // The live numbers as verdicts. nos.lol, nostr.oxtr.dev and
        // relay.lightning.pub returned 500 of 500 at every anchor depth in both
        // runs — the good actors are never near the bar.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 500), ids(0, 500)))
        // A relay a few events behind on one of the two asks still passes: 482
        // of 500 is 0.964. This is the margin that keeps ordinary replication
        // lag from reading as misbehaviour.
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 500), ids(18, 500)))
        // fiatjaf.com, 128 shared of a 179/203 pair = 0.715. It scored between
        // 0.618 and 0.826 across eight measurements and never once passed.
        assertEquals(RelayConsistency.Verdict.INCONSISTENT, c.decide(ids(0, 179), ids(51, 203)))
        // multiplexer.huszonegy.world, 327 of 500 = 0.654 at the seven-day
        // anchor. An earlier run scored the SAME relay at the SAME depth 0.964,
        // which is the finding: its disagreement with itself is not even
        // reproducible, so "it just needs time to replicate" does not hold.
        assertEquals(RelayConsistency.Verdict.INCONSISTENT, c.decide(ids(0, 500), ids(173, 500)))
    }

    @Test
    fun `a thin window decides nothing rather than deciding against the relay`() {
        // A relay holding nineteen events is small, not dishonest. Calling this
        // INCONSISTENT would drop working relays for the crime of being quiet.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(ids(0, 19), ids(500, 19)))
        // …even when the two windows share nothing at all, which is the shape
        // that most looks like misbehaviour and is the least evidence of it.
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(emptySet(), emptySet()))
    }

    @Test
    fun `silence decides nothing`() {
        // A relay that could not be asked has proved nothing about how it
        // answers. That is HostStrikes' business, and a verdict here would
        // exclude every relay that happened to be down during one pass.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(null, ids(0, 500)))
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(ids(0, 500), null))
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(null, null))
    }

    @Test
    fun `an unmeasured url is dialled, because a fan-out is never narrowed by silence`() {
        val c = RelayConsistency()
        assertTrue(c.usable(steady), "a url nothing has measured must still be dialled")
        assertFalse(c.measured(steady))
        assertEquals(emptyList(), c.unusable(listOf(steady, shuffler)))
    }

    @Test
    fun `only a positive failure removes a url`() {
        val c = RelayConsistency()
        c.learn(steady, RelayConsistency.Verdict.CONSISTENT)
        c.learn(shuffler, RelayConsistency.Verdict.UNMEASURABLE)
        assertEquals(emptyList(), c.unusable(listOf(steady, shuffler)))

        c.learn(shuffler, RelayConsistency.Verdict.INCONSISTENT)
        assertEquals(listOf(shuffler), c.unusable(listOf(steady, shuffler)))
    }

    @Test
    fun `a verdict that lapses puts the relay back in the fan-out`() {
        // The monthly re-measure, as a property. A relay refused today must come
        // back on its own once the record ages out — otherwise one bad afternoon
        // removes a relay permanently and no operator ever learns why.
        val c = RelayConsistency()
        c.learn(shuffler, RelayConsistency.Verdict.INCONSISTENT)
        assertFalse(c.usable(shuffler))

        c.forget(listOf(shuffler))
        assertTrue(c.usable(shuffler), "a forgotten verdict still refused the relay")
        assertFalse(c.measured(shuffler))
    }

    @Test
    fun `a relay that starts behaving is un-refused by the new verdict`() {
        val c = RelayConsistency()
        c.learn(shuffler, RelayConsistency.Verdict.INCONSISTENT)
        c.learn(shuffler, RelayConsistency.Verdict.CONSISTENT)
        assertTrue(c.usable(shuffler))
        assertEquals(0, c.refusedCount())
    }

    @Test
    fun `the verdict survives a round trip through the signed record`() =
        runBlocking {
            // The whole point of writing it to NIP-66 rather than a file: a
            // restart must not re-dial every relay it already measured, and the
            // evidence has to be readable by whoever asks why we stopped.
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            record.publishConsistency(shuffler, consistent = false, first = 203, second = 179, shared = 128, score = 0.715)
            record.publishConsistency(steady, consistent = true, first = 500, second = 500, shared = 500, score = 1.0)

            val held = record.load(listOf(steady, shuffler))
            assertEquals(setOf(shuffler), held.unstable)
            assertEquals(setOf(steady), held.stable)
        }

    @Test
    fun `a stability verdict does not need a fold to be readable`() =
        runBlocking {
            // The record carries two independent verdicts, and a url that was
            // never folded carries no `same-as` at all. Reading the event as a
            // fold first and giving up when there is none would make every
            // stability verdict on an unfolded url invisible — which is most of
            // them.
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            record.publishConsistency(shuffler, consistent = false, first = 203, second = 179, shared = 128, score = 0.715)

            val held = record.load(listOf(shuffler))
            assertEquals(setOf(shuffler), held.unstable)
            assertTrue(held.aliases.isEmpty())
            assertTrue(held.distinct.isEmpty())
        }

    @Test
    fun `a fold and a stability verdict coexist on one record`() =
        runBlocking {
            // Two writers, one addressable event. Each owns its own tag and must
            // carry the other's forward untouched — the same rule the passive
            // NIP-66 monitor already forced on `same-as`.
            val store = newStore()
            val record = RelayAliasRecord(store, signer)
            val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")

            record.publish(alias, steady, 500, 498)
            record.publishConsistency(alias, consistent = true, first = 500, second = 500, shared = 500, score = 1.0)

            val held = record.load(listOf(alias))
            assertEquals(steady, held.aliases[alias], "publishing the stability verdict dropped the fold")
            assertEquals(setOf(alias), held.stable)
        }

    @Test
    fun `the pass refuses the shuffler, keeps the steady relay and dials each once`() =
        runBlocking {
            // End to end through the real pass: two urls, one that answers the
            // same way twice and one that does not.
            val store = newStore()
            val consistency = RelayConsistency()
            val dials = AtomicInteger()
            val drift = AtomicInteger()
            val corpus: List<Event> = (0 until 60).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val pass =
                ConsistencyPass(
                    consistency = consistency,
                    record = RelayAliasRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { at, want, _, _ ->
                                dials.incrementAndGet()
                                // The shuffler hands back a window that walks
                                // forward on every ask, so no two answers agree.
                                val from = if (at == shuffler) drift.getAndAdd(40) else 0
                                AliasProbe.Page(corpus.drop(from).take(want))
                            },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                        ),
                )

            val decided = pass.measure("t", listOf(steady, shuffler), canDial = { true })

            assertEquals(2, decided, "both urls should have reached a verdict")
            assertEquals(listOf(shuffler), pass.apply(listOf(steady, shuffler)))

            // …and a second pass costs nothing: the verdicts are in the store.
            val afterFirst = dials.get()
            assertEquals(0, pass.measure("t", listOf(steady, shuffler), canDial = { true }))
            assertEquals(afterFirst, dials.get(), "a measured url was re-dialled")
        }

    @Test
    fun `apply never dials`() =
        runBlocking {
            // It runs in front of every fan-out, on the cycle's critical path —
            // the same rule AliasFolding.apply is held to.
            val dials = AtomicInteger()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayAliasRecord(newStore(), signer),
                    probe =
                        AliasProbe(
                            fetch = { _, _, _, _ ->
                                dials.incrementAndGet()
                                AliasProbe.Page(emptyList())
                            },
                        ),
                )

            assertEquals(emptyList(), pass.apply(listOf(steady, shuffler)))
            assertEquals(0, dials.get(), "apply() opened ${dials.get()} socket(s)")
        }

    @Test
    fun `a relay that says nothing is not refused`() =
        runBlocking {
            // The commonest way to fail a measurement is to be down, and the
            // fan-out must not shrink for it.
            val store = newStore()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayAliasRecord(store, signer),
                    probe = AliasProbe(fetch = { _, _, _, _ -> AliasProbe.Page(null) }),
                )

            assertEquals(0, pass.measure("t", listOf(steady, shuffler), canDial = { true }))
            assertEquals(emptyList(), pass.apply(listOf(steady, shuffler)))
        }

    @Test
    fun `a verdict ages on when it was MEASURED, not on the record it rides on`() =
        runBlocking {
            // The re-measure schedule, and the bug it used to have. Kind 30166 is
            // shared: quartz's RelayMonitor rewrites the record for every relay
            // this client connects to, on a 5-minute flush, carrying our tags
            // forward. So the event's createdAt tracks the last time we TALKED to
            // the relay — which for a relay still in the fan-out is always
            // minutes ago — and ageing the verdict on it meant a KEPT relay was
            // measured once and trusted forever, while only a REFUSED one (never
            // dialled, so never refreshed) actually expired.
            //
            // Here the record is written NOW, exactly as that flush leaves it,
            // over a verdict measured a month and a day ago. It must read stale.
            val store = newStore()
            val month = 30L * 24 * 60 * 60
            val record = RelayAliasRecord(store, signer, ttlSeconds = month)
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(steady, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayAliasRecord.SELF_CONSISTENT_TAG,
                                RelayAliasRecord.CONSISTENT_YES,
                                "500 + 500 events at a 7d anchor, 500 shared -> 1.000",
                                (nowSeconds() - month - 1).toString(),
                                // Current rules, so age is the only thing left
                                // that can make this stale — which is what the
                                // test is about.
                                RelayAliasRecord.CONSISTENCY_EPOCH,
                            ),
                        )
                    },
                ),
            )

            val held = record.load(listOf(steady))
            assertTrue(
                held.stable.isEmpty(),
                "a verdict measured over a month ago read as current because the record had just been rewritten",
            )
        }

    @Test
    fun `a record written before measured-at existed is re-measured, not believed forever`() =
        runBlocking {
            // This used to fall back to the event's clock, on the reasoning that
            // it is the only reading available for such a record. It is also the
            // clock quartz's monitor rewrites every time we connect — so the
            // fallback dated the verdict as minutes old for as long as the relay
            // stayed in the fan-out, and no TTL could ever expire it. The
            // fallback WAS the bug it was written around.
            //
            // Refusing it costs one re-measure per url, once. Keeping it cost
            // every pre-stamp verdict, permanently.
            val store = newStore()
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(shuffler, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayAliasRecord.SELF_CONSISTENT_TAG,
                                RelayAliasRecord.CONSISTENT_NO,
                                "old-style, no timestamp",
                            ),
                        )
                    },
                ),
            )

            val held = RelayAliasRecord(store, signer).load(listOf(shuffler))
            assertTrue(held.unstable.isEmpty(), "a verdict with no measurement time read as current off the record's clock")
        }

    @Test
    fun `the anchor is a week back, because a shallower one accuses a replicating relay`() {
        // Measured: multiplexer.huszonegy.world scores 0.446 at one minute and
        // 0.964 at seven days. Anything shallower than this excludes a relay for
        // replicating rather than for misbehaving.
        assertEquals(7L * 24 * 60 * 60, RelayConsistency.ANCHOR_LAG_SECONDS)
        assertTrue(
            RelayConsistency.ANCHOR_LAG_SECONDS > AliasProbe.ANCHOR_LAG_SECONDS,
            "the stability anchor must be deeper than the fold's — it is making a costlier claim",
        )
        assertEquals(1_000L - 7 * 24 * 60 * 60, RelayConsistency.settledAnchor(1_000L))
    }
}
