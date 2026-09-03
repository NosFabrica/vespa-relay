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
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * THE FAST LANE, AND WHAT IT IS ALLOWED TO HAND A RELAY THAT NOBODY HAS
 * CHECKED YET.
 *
 * The lane exists because a new relay waits on `prime` — that grade is the
 * whole admission decision for every visit-mode stream — and the sweep runs on
 * a six-hour clock. So the lane looks every [MonitorConfig.fastLaneSeconds] at
 * the urls named since its last look and grades them straight away.
 *
 * **It used to run FITNESS ALONE, and that is the hole this pins shut.** The
 * stability gate rode the sweep, so a url could be graded `prime` and admitted
 * to every roster before anything had asked whether it answers the same
 * question twice. [Verdict.INCONSISTENT] exists because such a relay "would
 * poison bands and coverage", and the window was up to a whole sweep of one
 * doing exactly that.
 *
 * The FOLD is deliberately still not here: it elects a leader out of a host's
 * whole group, and a since-bound set holds whichever urls of that host happened
 * to be named in the last two minutes. Per-url work belongs in the lane; per-host
 * work does not.
 */
class FastLaneTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val steady = RelayUrlNormalizer.normalize("wss://steady.example")
    private val shuffler = RelayUrlNormalizer.normalize("wss://shuffler.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** Deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE] as one page. */
    private val corpus: List<Event> = (0 until 60).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /**
     * The world the lane reads from: [candidatesSince] is the lane's door and
     * [candidates] is the sweep's, so a test can drive either.
     */
    private class Fresh(
        private val urls: List<NormalizedRelayUrl>,
    ) : AliasMonitor.CandidateSource {
        override suspend fun candidates() = urls

        override suspend fun candidatesSince(since: Long) = urls

        override suspend fun canDial(url: NormalizedRelayUrl) = true

        override suspend fun onEvent(event: Event) = Unit

        override val sockets = Sockets.NONE
    }

    /**
     * A fetch where [shuffler] walks its window forward on every ask, so no two
     * answers agree — the same shape `RelayConsistencyTest` measures the gate
     * with — and everything else is an ordinary relay that pages.
     *
     * The cursor is honoured for everything but the shuffler, and since #187
     * that is what "ordinary" means: the fitness pass asks a second page below
     * the first and grades a relay that ignores it `unpageable`. This test is
     * about the LANE admitting and refusing, so its good relays have to page or
     * they are all refused for a reason it is not measuring.
     */
    private fun shufflingFetch(
        dials: AtomicInteger,
        drift: AtomicInteger,
    ): suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
        { at, want, until, _ ->
            dials.incrementAndGet()
            if (at == shuffler) {
                AliasProbe.Page(corpus.drop(drift.getAndAdd(40)).take(want))
            } else {
                AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want))
            }
        }

    /** The two real passes the lane runs, wired the way [MonitorEngine] wires them. */
    private class Lane(
        val stability: ConsistencyPass,
        val fitness: FitnessPass,
        val monitor: AliasMonitor,
    )

    private fun lane(
        store: NostrSemanticsStore,
        urls: List<NormalizedRelayUrl>,
        fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page,
        /** False stages the lane as it ran BEFORE the gate joined it — see the control below. */
        gateInLane: Boolean = true,
    ): Lane {
        val processors = Processors()
        val consistency = RelayConsistency()
        val stability =
            ConsistencyPass(
                consistency = consistency,
                record = RelayVerdictRecord(store, signer),
                probe = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40),
                progress = processors.of("consistency"),
            )
        val fitness =
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40),
                client = EmptyNostrClient(),
                foldedAway = { emptyMap() },
                // THE WIRE UNDER TEST. Fitness reads the gate's standing
                // verdicts through this, and until the lane ran the gate there
                // was never one to read on a url this new.
                inconsistent = { u -> stability.applyVerdicts(u).toSet() },
                progress = processors.of("fitness"),
            )
        val entry = { handle: Processors.Handle?, run: suspend (String, List<NormalizedRelayUrl>, suspend (NormalizedRelayUrl) -> Boolean, suspend (Event) -> Unit, Sockets) -> Int ->
            object : AliasMonitor.Pass {
                override val progress = handle

                override suspend fun measure(
                    label: String,
                    candidates: List<NormalizedRelayUrl>,
                    canDial: suspend (NormalizedRelayUrl) -> Boolean,
                    onEvent: suspend (Event) -> Unit,
                    sockets: Sockets,
                ): Int = run(label, candidates, canDial, onEvent, sockets)
            }
        }
        return Lane(
            stability,
            fitness,
            AliasMonitor(
                passes = listOf(entry(fitness.progress, fitness::measure)),
                scope = CoroutineScope(Dispatchers.Unconfined),
                source = Fresh(urls),
                fastLaneEveryMs = 1_000L,
                fastLanePasses =
                    if (gateInLane) {
                        listOf(entry(stability.progress, stability::measure), entry(fitness.progress, fitness::measure))
                    } else {
                        listOf(entry(fitness.progress, fitness::measure))
                    },
            ),
        )
    }

    @Test
    fun `a relay that cannot answer twice is refused by the lane, not admitted by it`() =
        runBlocking {
            val store = newStore()
            val l = lane(store, listOf(steady, shuffler), shufflingFetch(AtomicInteger(), AtomicInteger()))

            l.monitor.runFastLane(0)

            // The whole point: the shuffler never sees `prime`, on the very
            // tick that discovered it. Before the gate joined this lane it was
            // graded off the fitness ladder alone, which the shuffler passes —
            // it answers, and it pages.
            assertEquals(Verdict.INCONSISTENT.value, gradeOf(store, shuffler))
            assertNotEquals(Verdict.PRIME.value, gradeOf(store, shuffler))
            // …and the relay that IS steady still gets its grade on the same
            // tick. A lane that bought safety by making everyone wait for the
            // sweep would have defeated its own reason to exist.
            assertEquals(Verdict.PRIME.value, gradeOf(store, steady))
        }

    /**
     * THE CONTROL, and it is the reason the test above is known to assert
     * something: the shuffler passes the fitness ladder on its own. It answers
     * and it pages, which is all `prime` is measured from — so with the gate
     * back on the sweep the lane admits it, and every visit-mode stream selects
     * on that grade until the next sweep replaces it.
     */
    @Test
    fun `fitness alone in the lane is what let the shuffler through`() =
        runBlocking {
            val store = newStore()
            val l = lane(store, listOf(steady, shuffler), shufflingFetch(AtomicInteger(), AtomicInteger()), gateInLane = false)

            l.monitor.runFastLane(0)

            assertEquals(
                Verdict.PRIME.value,
                gradeOf(store, shuffler),
                "the ladder cannot see instability, so a lane without the gate had to admit this relay",
            )
        }

    @Test
    fun `the work moves earlier and does not repeat, so the sweep re-dials nothing the lane measured`() =
        runBlocking {
            // The cost argument for putting the gate here at all. A stability
            // verdict stands for RelayVerdictRecord.DEFAULT_TTL_SECONDS, so a
            // url the lane measured is one the sweep skips: over a month this
            // is the same number of dials, taken sooner.
            val store = newStore()
            val dials = AtomicInteger()
            val l = lane(store, listOf(steady, shuffler), shufflingFetch(dials, AtomicInteger()))

            l.monitor.runFastLane(0)
            val afterLane = dials.get()
            assertTrue(afterLane > 0, "the lane dialled nothing at all")

            assertEquals(0, l.stability.measure("sweep", listOf(steady, shuffler), canDial = { true }))
            assertEquals(afterLane, dials.get(), "the sweep re-dialled a url the lane had already decided")
        }

    @Test
    fun `one lane pass failing does not cost the next one its turn`() =
        runBlocking {
            // A new relay's first `prime` must not be lost to a fault in the
            // pass ahead of it — the same rule `runPass` holds each stream to.
            val ran = mutableListOf<String>()
            val boom =
                AliasMonitor.Pass { label, _, _, _, _ ->
                    ran += "stability:$label"
                    error("the gate blew up")
                }
            val after =
                AliasMonitor.Pass { label, _, _, _, _ ->
                    ran += "fitness:$label"
                    0
                }
            val monitor =
                AliasMonitor(
                    passes = listOf(after),
                    scope = CoroutineScope(Dispatchers.Unconfined),
                    source = Fresh(listOf(steady)),
                    fastLaneEveryMs = 1_000L,
                    fastLanePasses = listOf(boom, after),
                )

            monitor.runFastLane(0)

            assertEquals(
                listOf("stability:${AliasMonitor.FAST_LANE}", "fitness:${AliasMonitor.FAST_LANE}"),
                ran,
                "the lane runs its passes in order, and the second still runs when the first throws",
            )
        }

    private suspend fun gradeOf(
        store: NostrSemanticsStore,
        url: NormalizedRelayUrl,
    ): String? =
        store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
            ).flatMap { it.tags.toList() }
            .firstOrNull { it.size >= 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
            ?.get(1)
}
