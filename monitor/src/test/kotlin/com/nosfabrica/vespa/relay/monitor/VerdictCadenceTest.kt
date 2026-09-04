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
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * How often a relay is re-graded depends on nothing but when it was last
 * graded: the fitness pass's write loop and per-url clock, driven by fakes.
 */
class VerdictCadenceTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /** A relay that honours the cursor; one that ignores it is graded `unpageable` before any clock fires. */
    private fun paged(
        events: List<Event>,
        want: Int,
        until: Long?,
    ) = AliasProbe.Page(events.filter { until == null || it.createdAt <= until }.take(want))

    /** Twelve of these is a whole per-url deadline in test time. */
    private val tinyIdleMs = 20L

    private fun deadlineMs() = AliasProbe.WINDOWS_PER_URL * tinyIdleMs

    /** Answers every ask promptly and in full, so the ladder settles `prime` on the first rung. */
    private fun answeringProbe(idleMs: Long = tinyIdleMs) =
        AliasProbe(
            fetch = { _, want, until, _ -> paged(corpus(), want, until) },
            target = 40,
            page = 40,
            fallbackPage = 40,
            idleMs = { idleMs },
        )

    /** A NEG-OPEN parked with no timer under it, the shape of a reconciliation whose idle window keeps re-arming. */
    private fun parkingNegOpen(hits: AtomicInteger? = null): suspend (NormalizedRelayUrl, Filter) -> Unit =
        { _, _ ->
            hits?.incrementAndGet()
            CompletableDeferred<Unit>().await()
        }

    @Test
    fun `a job the clock cuts AFTER the ladder earned a verdict still publishes it`() =
        runBlocking {
            val store = newStore()
            val slow = RelayUrlNormalizer.normalize("wss://slow.example")
            val hits = AtomicInteger()
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    // Longer than the per-url deadline, so the outer clock fires first, as in production.
                    nip77DeadlineMs = deadlineMs() * 100,
                    reconcile = parkingNegOpen(hits),
                )

            val captured = ByteArrayOutputStream()
            val realErr = System.err
            System.setErr(PrintStream(captured, true))
            try {
                withTimeout(deadlineMs() * 40) {
                    pass.measure("cut late", listOf(slow), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                }
            } finally {
                System.setErr(realErr)
            }

            assertTrue(hits.get() > 0, "the NEG-OPEN has to have been reached, or this proves nothing")
            assertEquals(
                Verdict.PRIME.value,
                gradeOf(store, slow),
                "a verdict the dial had already earned must survive the wall clock that cut the job",
            )
            val err = captured.toString()
            assertTrue(
                "gave up on" !in err,
                "a url cut with a verdict in hand is not abandoned; got: $err",
            )
            assertTrue(
                "ran past the per-url deadline AFTER earning a verdict" in err,
                "the pass must still say the budget was spent — that is the warning #172 had none of; got: $err",
            )
        }

    @Test
    fun `the NEG-OPEN cannot spend the url's whole budget`() =
        runBlocking {
            val store = newStore()
            val slow = RelayUrlNormalizer.normalize("wss://slow.example")
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    // A per-url deadline out of reach, so only the NEG-OPEN's own clock can end the job.
                    probe = answeringProbe(idleMs = 60_000L),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    nip77DeadlineMs = 100L,
                    reconcile = parkingNegOpen(),
                )

            withTimeout(30_000) {
                pass.measure("neg-open clock", listOf(slow), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            assertEquals(Verdict.PRIME.value, gradeOf(store, slow))
            // Our own clock is not the relay declining, so no nip77 fact in either direction.
            assertNull(
                tagOf(store, slow, RelayVerdictRecord.NIP77_TAG),
                "a NEG-OPEN our own clock cut must publish no nip77 fact in either direction",
            )
        }

    /** A store whose writes never answer, which is what ends a batch on the wedge limit. */
    private class WedgedWrites(
        private val inner: NostrSemanticsStore,
    ) : IEventStore by inner {
        @Volatile
        var wedged = true

        override suspend fun insert(event: Event) {
            if (wedged) {
                CompletableDeferred<Unit>().await()
                error("unreachable")
            }
            inner.insert(event)
        }
    }

    @Test
    fun `a batch the store wedged does not drop the same urls again next pass`() =
        runBlocking {
            // More urls than the wedge limit can reach, so pass two has to start where pass one stopped.
            val urls = (0 until 12).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val store = WedgedWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = self))
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                    reconcile = { _, _ -> },
                )

            withTimeout(30_000) {
                pass.measure("wedged", urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            store.wedged = false
            withTimeout(30_000) {
                pass.measure("recovered", urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            for (url in urls) {
                assertEquals(
                    Verdict.PRIME.value,
                    gradeOf(store, url),
                    "${url.url} was earned twice and written never — a cut batch must not drop the same tail every pass",
                )
            }
        }

    @Test
    fun `the write loop resumes where the wedge stopped it`() =
        runBlocking {
            val urls = (0 until 12).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val attempted = mutableListOf<String>()
            val recording =
                object : IEventStore by store {
                    override suspend fun insert(event: Event) {
                        event.tags
                            .firstOrNull { it.firstOrNull() == "d" }
                            ?.getOrNull(1)
                            ?.let { synchronized(attempted) { attempted += it } }
                        CompletableDeferred<Unit>().await()
                        error("unreachable")
                    }
                }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(recording, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                    reconcile = { _, _ -> },
                )

            // One label for both sweeps: the resume cursor is per label. See [FitnessPass.writeCursors].
            withTimeout(30_000) { pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE) }
            val first = synchronized(attempted) { attempted.toList() }
            synchronized(attempted) { attempted.clear() }
            withTimeout(30_000) { pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE) }
            val second = synchronized(attempted) { attempted.toList() }

            assertEquals(FitnessPass.PUBLISH_WEDGE_LIMIT, first.size, "a wedged store must cost the wedge limit and no more")
            assertEquals(FitnessPass.PUBLISH_WEDGE_LIMIT, second.size)
            // The write that tripped the limit did not land, so it is retried; the ones before it are behind us.
            assertEquals(
                first.last(),
                second.first(),
                "the next batch must pick up at the write the wedge stopped on: $first then $second",
            )
            assertTrue(
                first.dropLast(1).none { it in second },
                "urls a wedged batch already wrote off must not be retried ahead of the ones it never reached: $first then $second",
            )
        }

    @Test
    fun `a fast lane tick does not clear the sweep's resume point`() =
        runBlocking {
            // One pass object, two callers on two clocks, as `MonitorEngine.fitnessEntry` is wired.
            val corpusUrls = (0 until 12).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val laneUrls = listOf(RelayUrlNormalizer.normalize("wss://fresh.example"))
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val attempted = mutableListOf<String>()

            // The write loop runs on whatever dispatcher the insert suspends onto; a plain local has no visibility guarantee there.
            val wedged = AtomicBoolean(true)
            val recording =
                object : IEventStore by store {
                    override suspend fun insert(event: Event) {
                        event.tags
                            .firstOrNull { it.firstOrNull() == "d" }
                            ?.getOrNull(1)
                            ?.let { synchronized(attempted) { attempted += it } }
                        if (wedged.get()) {
                            CompletableDeferred<Unit>().await()
                            error("unreachable")
                        }
                        store.insert(event)
                    }
                }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(recording, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                    reconcile = { _, _ -> },
                )

            withTimeout(30_000) {
                pass.measure(AliasMonitor.ALL_STREAMS, corpusUrls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            val sweptFirst = synchronized(attempted) { attempted.toList() }
            synchronized(attempted) { attempted.clear() }

            // A healthy lane tick writes its whole batch and so has no resume point of its own.
            wedged.set(false)
            withTimeout(30_000) {
                pass.measure(AliasMonitor.FAST_LANE, laneUrls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            synchronized(attempted) { attempted.clear() }

            wedged.set(true)
            withTimeout(30_000) {
                pass.measure(AliasMonitor.ALL_STREAMS, corpusUrls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            val sweptAgain = synchronized(attempted) { attempted.toList() }

            assertEquals(
                sweptFirst.last(),
                sweptAgain.first(),
                "a lane tick between two sweeps must not move the sweep's cursor: $sweptFirst then $sweptAgain",
            )
        }

    @Test
    fun `a store that alternates timeout and decline is not a wedged store`() =
        runBlocking {
            // A decline is the store answering, so it ends a run exactly as a success does.
            val urls = (0 until 30).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val n = AtomicInteger()
            val alternating =
                object : IEventStore by store {
                    override suspend fun insert(event: Event) {
                        if (n.getAndIncrement() % 2 == 0) {
                            CompletableDeferred<Unit>().await()
                            error("unreachable")
                        }
                        error("store declines this one")
                    }
                }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(alternating, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                    reconcile = { _, _ -> },
                )
            withTimeout(60_000) {
                pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            // Thirty urls is under the total time budget, so only the consecutive limit could have ended this early.
            assertEquals(
                urls.size,
                n.get(),
                "a store answering every other write is not wedged; the batch must not end on the consecutive limit",
            )
        }

    @Test
    fun `an inherited verdict is re-signed only when it would change the record`() =
        runBlocking {
            // The `measured-at` stamp is how a verdict ages; stamping an untested inheritance claims a measurement nothing took.
            val alias = RelayUrlNormalizer.normalize("wss://alias.example")
            val canonical = RelayUrlNormalizer.normalize("wss://canonical.example")
            val dialled = RelayUrlNormalizer.normalize("wss://dialled.example")
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val inserts = AtomicInteger()
            val counting =
                object : IEventStore by store {
                    override suspend fun insert(event: Event) {
                        inserts.incrementAndGet()
                        store.insert(event)
                    }
                }

            fun pass() =
                FitnessPass(
                    record = RelayVerdictRecord(counting, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { mapOf(alias to canonical) },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    reconcile = { _, _ -> },
                )

            withTimeout(30_000) {
                pass().measure(AliasMonitor.ALL_STREAMS, listOf(alias, dialled), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(Verdict.ALIAS.value, gradeOf(store, alias))
            assertEquals(Verdict.PRIME.value, gradeOf(store, dialled))
            val afterFirst = inserts.get()
            assertEquals(2, afterFirst, "a first pass has to put both verdicts down")
            val aliasStamp = stampOf(store, alias)

            inserts.set(0)
            withTimeout(30_000) {
                pass().measure(AliasMonitor.ALL_STREAMS, listOf(alias, dialled), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(1, inserts.get(), "a pass must re-sign what it dialled and only that")
            assertEquals(Verdict.PRIME.value, gradeOf(store, dialled), "the dialled url is still re-graded every pass")
            assertEquals(
                aliasStamp,
                stampOf(store, alias),
                "an untested verdict must keep the stamp of the pass that actually took it",
            )

            inserts.set(0)
            withTimeout(30_000) {
                FitnessPass(
                    record = RelayVerdictRecord(counting, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    reconcile = { _, _ -> },
                ).measure(AliasMonitor.ALL_STREAMS, listOf(alias, dialled), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(Verdict.PRIME.value, gradeOf(store, alias), "a verdict that CHANGED must be written whatever the record said")
        }

    @Test
    fun `a url that re-folds onto a different canonical is re-signed, grade unchanged`() =
        runBlocking {
            // The grade alone does not identify the claim: `alias` names a canonical, and the record must name the current one.
            val alias = RelayUrlNormalizer.normalize("wss://alias.example")
            val first = RelayUrlNormalizer.normalize("wss://first-canonical.example")
            val second = RelayUrlNormalizer.normalize("wss://second-canonical.example")
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

            fun passFolding(onto: NormalizedRelayUrl) =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { mapOf(alias to onto) },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    reconcile = { _, _ -> },
                )

            withTimeout(30_000) {
                passFolding(first).measure(AliasMonitor.ALL_STREAMS, listOf(alias), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertTrue(first.url in (evidenceOf(store, alias) ?: ""), "the first fold has to name the first canonical")

            withTimeout(30_000) {
                passFolding(second).measure(AliasMonitor.ALL_STREAMS, listOf(alias), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(Verdict.ALIAS.value, gradeOf(store, alias))
            assertTrue(
                second.url in (evidenceOf(store, alias) ?: ""),
                "a re-fold onto a different canonical must be written even though the grade did not change; got ${evidenceOf(store, alias)}",
            )

            val stamp = stampOf(store, alias)
            withTimeout(30_000) {
                passFolding(second).measure(AliasMonitor.ALL_STREAMS, listOf(alias), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(stamp, stampOf(store, alias), "an unchanged inherited verdict must still be left standing")
        }

    @Test
    fun `a NEG-OPEN the clock cuts is counted and said out loud`() =
        runBlocking {
            val slow = RelayUrlNormalizer.normalize("wss://slow.example")
            val store = newStore()
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = answeringProbe(idleMs = 60_000L),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    nip77DeadlineMs = 100L,
                    reconcile = parkingNegOpen(),
                )
            val captured = ByteArrayOutputStream()
            val realErr = System.err
            System.setErr(PrintStream(captured, true))
            try {
                withTimeout(30_000) {
                    pass.measure(AliasMonitor.ALL_STREAMS, listOf(slow), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                }
            } finally {
                System.setErr(realErr)
            }

            val err = captured.toString()
            assertTrue(
                "NEG-OPEN(s) cut at the" in err,
                "a spent NEG-OPEN budget must be reported, not swallowed as `no fact`; got: $err",
            )
            assertEquals(Verdict.PRIME.value, gradeOf(store, slow))
            assertNull(tagOf(store, slow, RelayVerdictRecord.NIP77_TAG), "a cut NEG-OPEN publishes no fact in either direction")
        }

    /** The sentence the fitness label publishes beside its grade. */
    private suspend fun evidenceOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
    ): String? = tagOf(store, url, "l")?.getOrNull(RelayVerdictRecord.LABEL_EVIDENCE_INDEX)

    /** The `measured-at` the fitness label carries. */
    private suspend fun stampOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
    ): String? = tagOf(store, url, "l")?.getOrNull(RelayVerdictRecord.LABEL_MEASURED_AT_INDEX)

    @Test
    fun `the batch's ceiling on a store that alternates is a time budget, not a count`() =
        runBlocking {
            val urls = (0 until 60).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val attempts = AtomicInteger()
            val alternating =
                object : IEventStore by store {
                    override suspend fun insert(event: Event) {
                        if (attempts.getAndIncrement() % 2 == 0) {
                            CompletableDeferred<Unit>().await()
                            error("unreachable")
                        }
                        store.insert(event)
                    }
                }
            val captured = ByteArrayOutputStream()
            val realErr = System.err
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(alternating, signer),
                    probe = answeringProbe(),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                    // Five deadlines' worth: enough for real progress, little enough for a test.
                    publishWedgeBudgetMs = 500L,
                    reconcile = { _, _ -> },
                )
            System.setErr(PrintStream(captured, true))
            try {
                withTimeout(60_000) {
                    pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                }
            } finally {
                System.setErr(realErr)
            }

            val err = captured.toString()
            assertTrue(
                "spent waiting on writes that never came back" in err,
                "an alternating store must end the batch on the time budget, not on the consecutive limit; got: $err",
            )
            // A count of three would have stopped at the sixth write.
            assertTrue(
                attempts.get() > 6,
                "the budget must buy more than the consecutive limit would; only ${attempts.get()} write(s) attempted",
            )
            assertTrue(
                "the next batch's writes START at" in err,
                "a batch the budget stopped must name where the next one resumes; got: $err",
            )
        }

    private suspend fun gradeOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
    ): String? = tagOf(store, url, "l")?.let { if (it.size >= 3 && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE) it[1] else null }

    private suspend fun tagOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
        name: String,
    ): Array<String>? =
        store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
            ).flatMap { it.tags.toList() }
            .firstOrNull { it.firstOrNull() == name }
}
