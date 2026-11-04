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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
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
    fun `a decision carries the two windows it was decided on, and silence carries none`() {
        val c = RelayConsistency()
        val decided = c.judge(ids(0, 500), ids(0, 499))
        assertEquals(RelayConsistency.Verdict.CONSISTENT, decided?.verdict)
        assertEquals(500 to 499, decided?.let { it.first.size to it.second.size })
        assertEquals(null, c.judge(null, ids(0, 500)))
        assertEquals(null, c.judge(ids(0, 19), ids(0, 19)))
    }

    @Test
    fun `silence decides nothing`() {
        // A relay that could not be asked proves nothing about how it answers.
        val c = RelayConsistency()
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(null, ids(0, 500)))
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(ids(0, 500), null))
        assertEquals(RelayConsistency.Verdict.UNMEASURABLE, c.decide(null, null))
    }

    @Test
    fun `an unmeasured url is dialled, because a fan-out is never narrowed by silence`() =
        runBlocking {
            val dialled = java.util.Collections.synchronizedSet(HashSet<String>())
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(newStore(), signer),
                    probe =
                        AliasProbe(fetch = { at, _, _, _ ->
                            dialled += at.url
                            AliasProbe.Page(null)
                        }),
                )
            pass.measure(RunKind.SWEEP, listOf(steady, shuffler))
            assertEquals(setOf(steady.url, shuffler.url), dialled.toSet(), "a url nothing has measured must still be dialled")
        }

    @Test
    fun `only a positive failure removes a url`() =
        runBlocking {
            val store = newStore()
            val corpus: List<Event> = (0 until 60).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val drift = AtomicInteger()
            val shuffles =
                java.util.concurrent.atomic
                    .AtomicBoolean(false)
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { at, want, _, _ ->
                                when {
                                    at != shuffler -> AliasProbe.Page(corpus.take(want))
                                    !shuffles.get() -> AliasProbe.Page(null)
                                    else -> AliasProbe.Page(corpus.drop(drift.getAndAdd(40)).take(want))
                                }
                            },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                        ),
                )
            pass.measure(RunKind.SWEEP, listOf(steady, shuffler))
            assertEquals(emptySet(), RelayVerdictRecord(store, signer).load(listOf(steady, shuffler)).inconsistent)

            shuffles.set(true)
            pass.measure(RunKind.SWEEP, listOf(steady, shuffler))
            assertEquals(setOf(shuffler), RelayVerdictRecord(store, signer).load(listOf(steady, shuffler)).inconsistent)
        }

    @Test
    fun `a verdict that lapses puts the relay back in the fan-out`() =
        runBlocking {
            val store = newStore()
            val month = 30L * 24 * 60 * 60
            store.insert(
                signer.sign(
                    RelayDiscoveryEvent.build(shuffler, "", nowSeconds()) {
                        add(
                            arrayOf(
                                RelayVerdictRecord.SELF_CONSISTENT_TAG,
                                RelayVerdictRecord.CONSISTENT_NO,
                                "203 + 179 events at a 7d anchor, 128 shared -> 0.715",
                                (nowSeconds() - month - 1).toString(),
                                RelayVerdictRecord.CONSISTENCY_EPOCH,
                            ),
                        )
                    },
                ),
            )
            val dials = AtomicInteger()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(store, signer, ttlSeconds = month),
                    probe =
                        AliasProbe(fetch = { _, _, _, _ ->
                            dials.incrementAndGet()
                            AliasProbe.Page(null)
                        }),
                )
            pass.measure(RunKind.SWEEP, listOf(shuffler))
            assertTrue(dials.get() > 0, "a lapsed verdict still refused the relay")
        }

    @Test
    fun `a relay that starts behaving is un-refused by the new verdict`() =
        runBlocking {
            val record = RelayVerdictRecord(newStore(), signer)
            record.publishConsistency(shuffler, consistent = false, first = 203, second = 179, shared = 128, score = 0.715, anchorDays = 7)
            record.publishConsistency(shuffler, consistent = true, first = 500, second = 500, shared = 500, score = 1.0, anchorDays = 7)
            val held = record.load(listOf(shuffler))
            assertFalse(shuffler in held.inconsistent)
            assertEquals(0, held.inconsistent.size)
        }

    @Test
    fun `a decision counts the shared ids once, and its score is their share of the smaller window`() {
        val decided = RelayConsistency().judge(ids(0, 100), ids(50, 200))
        assertEquals(50, decided?.shared)
        assertEquals(0.5, decided?.score)
        assertEquals(RelayConsistency.Verdict.INCONSISTENT, decided?.verdict)
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

            val decided = pass.measure(RunKind.SWEEP, listOf(steady, shuffler))

            assertEquals(2, decided, "both urls should have reached a verdict")
            assertEquals(setOf(shuffler), RelayVerdictRecord(store, signer).load(listOf(steady, shuffler)).inconsistent)

            val afterFirst = dials.get()
            assertEquals(0, pass.measure(RunKind.SWEEP, listOf(steady, shuffler)))
            assertEquals(afterFirst, dials.get(), "a measured url was re-dialled")
        }

    @Test
    fun `a store slower than the relay's deadline still gets the verdict the relay earned`() =
        runBlocking {
            // The write is the store's time, not the relay's; it must not be cut by the dial's clock.
            val inner = newStore()
            val slow =
                object : IEventStore by inner {
                    override suspend fun insert(event: Event) {
                        kotlinx.coroutines.delay(AliasProbe.WINDOWS_PER_URL * 20L * 3)
                        inner.insert(event)
                    }
                }
            val corpus: List<Event> = (0 until 60).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val processors = Processors()
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(slow, signer),
                    probe =
                        AliasProbe(
                            fetch = { _, want, _, _ -> AliasProbe.Page(corpus.take(want)) },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { 20L },
                        ),
                    progress = processors.of("consistency"),
                )

            assertEquals(1, pass.measure(RunKind.SWEEP, listOf(steady)))
            assertEquals(setOf(steady), RelayVerdictRecord(inner, signer).load(listOf(steady)).consistent)
            val row =
                processors
                    .snapshot()
                    .single()
                    .work
                    .single()
            assertTrue(row.undecided.isEmpty(), "a decided url was also filed as undecided: ${row.undecided.map { it.reason }}")
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

            assertEquals(0, pass.measure(RunKind.SWEEP, listOf(steady, shuffler)))
            assertEquals(emptySet(), RelayVerdictRecord(store, signer).load(listOf(steady, shuffler)).inconsistent)
        }

    @Test
    fun `a connect that ran out of time is walked once, not again through the kinds fallback`() =
        runBlocking {
            val asked = java.util.Collections.synchronizedList(ArrayList<List<Int>?>())
            val pass =
                ConsistencyPass(
                    consistency = RelayConsistency(),
                    record = RelayVerdictRecord(newStore(), signer),
                    probe =
                        AliasProbe(fetch = { _, _, _, kinds ->
                            asked += kinds
                            AliasProbe.Page(null, reason = "cannot:WebSocket Failure: Connect timed out (SocketTimeoutException)")
                        }),
                )

            assertEquals(0, pass.measure(RunKind.SWEEP, listOf(steady)))
            assertEquals(listOf(null, null), asked.toList(), "the kinds pair would wait on the same dial's clock")
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
