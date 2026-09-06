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

/** The stability gate excludes a relay only on a positive measurement: not silence, not a thin window. */
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
        // Containment, not equality: a prefix served at the relay's own default_limit is the same answer.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 100), ids(0, 500)))
    }

    @Test
    fun `the measured shuffler is refused and the measured steady relays are not`() {
        // Live readings as verdicts; the margin keeps ordinary replication lag from reading as misbehaviour.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 500), ids(0, 500)))
        assertEquals(RelayConsistency.Verdict.CONSISTENT, c.decide(ids(0, 500), ids(18, 500)))
        assertEquals(RelayConsistency.Verdict.INCONSISTENT, c.decide(ids(0, 179), ids(51, 203)))
        assertEquals(RelayConsistency.Verdict.INCONSISTENT, c.decide(ids(0, 500), ids(173, 500)))
    }

    @Test
    fun `a thin window decides nothing rather than deciding against the relay`() {
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(ids(0, 19), ids(500, 19)))
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(emptySet(), emptySet()))
    }

    @Test
    fun `silence decides nothing`() {
        // A relay that could not be asked is HostStrikes' business.
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
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishConsistency(shuffler, consistent = false, first = 203, second = 179, shared = 128, score = 0.715, anchorDays = 7)
            record.publishConsistency(steady, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)

            val held = record.load(listOf(steady, shuffler))
            assertEquals(setOf(shuffler), held.inconsistent)
            assertEquals(setOf(steady), held.consistent)
        }

    @Test
    fun `a stability verdict does not need a fold to be readable`() =
        runBlocking {
            // Most urls were never folded and carry no `same-as`.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            record.publishConsistency(shuffler, consistent = false, first = 203, second = 179, shared = 128, score = 0.715, anchorDays = 7)

            val held = record.load(listOf(shuffler))
            assertEquals(setOf(shuffler), held.inconsistent)
            assertTrue(held.aliases.isEmpty())
            assertTrue(held.distinct.isEmpty())
        }

    @Test
    fun `a fold and a stability verdict coexist on one record`() =
        runBlocking {
            // Two writers, one addressable event: each owns its tag and carries the other's forward.
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")

            record.publish(alias, steady, 500, 498)
            record.publishConsistency(alias, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)

            val held = record.load(listOf(alias))
            assertEquals(steady, held.aliases[alias], "publishing the stability verdict dropped the fold")
            assertEquals(setOf(alias), held.consistent)
        }

    @Test
    fun `the pass refuses the shuffler, keeps the steady relay and dials each once`() =
        runBlocking {
            val store = newStore()
            val consistency = RelayConsistency()
            val dials = AtomicInteger()
            val drift = AtomicInteger()
            val corpus: List<Event> = (0 until 60).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val pass =
                ConsistencyPass(
                    consistency = consistency,
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { at, want, _, _ ->
                                dials.incrementAndGet()
                                // The shuffler's window walks forward on every ask.
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
            assertEquals(listOf(shuffler), pass.applyVerdicts(listOf(steady, shuffler)))

            val afterFirst = dials.get()
            assertEquals(0, pass.measure("t", listOf(steady, shuffler), canDial = { true }))
            assertEquals(afterFirst, dials.get(), "a measured url was re-dialled")
        }

    @Test
    fun `apply never dials`() =
        runBlocking {
            val dials = AtomicInteger()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(newStore(), signer),
                    probe =
                        AliasProbe(
                            fetch = { _, _, _, _ ->
                                dials.incrementAndGet()
                                AliasProbe.Page(emptyList())
                            },
                        ),
                )

            assertEquals(emptyList(), pass.applyVerdicts(listOf(steady, shuffler)))
            assertEquals(0, dials.get(), "apply() opened ${dials.get()} socket(s)")
        }

    @Test
    fun `a relay that says nothing is not refused`() =
        runBlocking {
            val store = newStore()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(store, signer),
                    probe = AliasProbe(fetch = { _, _, _, _ -> AliasProbe.Page(null) }),
                )

            assertEquals(0, pass.measure("t", listOf(steady, shuffler), canDial = { true }))
            assertEquals(emptyList(), pass.applyVerdicts(listOf(steady, shuffler)))
        }

    @Test
    fun `a verdict ages on when it was MEASURED, not on the record it rides on`() =
        runBlocking {
            // Quartz rewrites the shared kind 30166 on every connection, so the event's createdAt is
            // always fresh; a verdict measured past the TTL must still read stale.
            val store = newStore()
            val month = 30L * 24 * 60 * 60
            val record = RelayVerdictRecord(store, signer, ttlSeconds = month)
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(steady, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayVerdictRecord.SELF_CONSISTENT_TAG,
                                RelayVerdictRecord.CONSISTENT_YES,
                                "500 + 500 events at a 7d anchor, 500 shared -> 1.000",
                                (nowSeconds() - month - 1).toString(),
                                // Current rules, so age is the only thing left that can make this stale.
                                RelayVerdictRecord.CONSISTENCY_EPOCH,
                            ),
                        )
                    },
                ),
            )

            val held = record.load(listOf(steady))
            assertTrue(
                held.consistent.isEmpty(),
                "a verdict measured over a month ago read as current because the record had just been rewritten",
            )
        }

    @Test
    fun `a record written before measured-at existed is re-measured, not believed forever`() =
        runBlocking {
            // Falling back to the event's clock would keep the verdict current forever.
            val store = newStore()
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(shuffler, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayVerdictRecord.SELF_CONSISTENT_TAG,
                                RelayVerdictRecord.CONSISTENT_NO,
                                "old-style, no timestamp",
                            ),
                        )
                    },
                ),
            )

            val held = RelayVerdictRecord(store, signer).load(listOf(shuffler))
            assertTrue(held.inconsistent.isEmpty(), "a verdict with no measurement time read as current off the record's clock")
        }

    @Test
    fun `the anchor is a week back, because a shallower one accuses a replicating relay`() {
        assertEquals(7L * 24 * 60 * 60, RelayConsistency.ANCHOR_LAG_SECONDS)
        assertTrue(
            RelayConsistency.ANCHOR_LAG_SECONDS > AliasProbe.ANCHOR_LAG_SECONDS,
            "the stability anchor must be deeper than the fold's — it is making a costlier claim",
        )
        assertEquals(1_000L - 7 * 24 * 60 * 60, RelayConsistency.settledAnchor(1_000L))
    }
}
