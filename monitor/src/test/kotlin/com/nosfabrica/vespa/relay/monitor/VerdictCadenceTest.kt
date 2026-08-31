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
 * HOW OFTEN A RELAY IS RE-GRADED MUST NOT DEPEND ON ANYTHING BUT WHEN IT WAS
 * LAST GRADED — the rule #172 was the absence of.
 *
 * ## The failure being pinned
 *
 * On `vespa-eventstore-staging` the two relays carrying the most NIP-85
 * delegations fell out of every `assertions` roster. Both were `prime`,
 * `self-consistent 1.000`, `pageable`, and opened in under 20ms; they were
 * excluded because their verdicts had aged 68.4h past a 48h `gatedBy` bound and
 * nothing was re-taking them. Measured 1.7h apart they went 66.7h -> 68.4h:
 * ageing, not oscillating.
 *
 * The correlation across every kind-30166 this monitor has signed carrying an
 * `rtt-read` was with the READ, and only the read:
 *
 * ```
 * rtt-read >= 10s : n=74    median verdict age 530.5h   (22 days)
 * rtt-read <  1s  : n=1567  median verdict age  25.7h
 * ```
 *
 * `rtt-open` predicted nothing — the two stalled relays open FASTER than
 * `nos.lol`, which was re-graded every few hours. Raising the bound had already
 * been tried once (14h -> 48h) for the same class of problem, and at a 22-day
 * median no bound short of "never expire" holds, while each bump weakens the
 * gate for every relay that genuinely went bad.
 *
 * ## What it actually was, and what it was not
 *
 * **THE CAUSE: the write loop's early exits dropped the SAME urls every pass.**
 * The wedge limits end the batch, and the loop walked a
 * [java.util.concurrent.ConcurrentHashMap] — hash order, arbitrary but STABLE
 * across passes. "Every url is measured again next pass" was therefore true of
 * the head and false forever of the tail. [WriteOrderForensicProbe] rebuilds
 * that map from the 20,075 records staging was serving and shows the staircase:
 * three contiguous slices at 1.2-1.5h, 29.1-29.9h and 72.3h, 735 crossings
 * where a per-url cause predicts ~9,700, and the same fresh share in every
 * grade. Three sweeps, each from position zero, each reaching less far.
 *
 * **NOT the cause, though it is a real hole and is fixed here too: a wall clock
 * that fires after the verdict is EARNED threw it away.** The last step of a
 * url's job is the NEG-OPEN, an idle-bounded reconciliation that every round
 * re-arms, so it is the one step able to spend the whole per-url budget — and
 * when it did, [AliasProbe.deadlineMs] cut the job and the pass published
 * NOTHING, including the `prime` the ladder had already settled. That predicts
 * exactly the reported `rtt-read` correlation, which is why it was the first
 * theory. [FitnessBudgetLiveProbe] measured it against the real relays and it
 * does not happen: a whole job runs 1.1-11.9s against a 240s budget. Binned by
 * verdict AGE rather than by rtt, the median read is flat across the cohorts
 * (664ms fresh, 937ms at 48-96h) — the 20x tail in the issue is what binning by
 * the rtt itself surfaces out of 74 records.
 */
class VerdictCadenceTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /** [ProbeDeadlineTest.tinyIdleMs]'s reason: twelve of these is a whole per-url deadline in test time. */
    private val tinyIdleMs = 20L

    private fun deadlineMs() = AliasProbe.WINDOWS_PER_URL * tinyIdleMs

    /** Answers every ask promptly and in full — the ladder settles a `prime` on the first rung. */
    private fun answeringProbe(idleMs: Long = tinyIdleMs) =
        AliasProbe(
            fetch = { _, _, _, _ -> AliasProbe.Page(corpus()) },
            target = 40,
            page = 40,
            fallbackPage = 40,
            idleMs = { idleMs },
        )

    /**
     * A NEG-OPEN that never comes back — parked on a `CompletableDeferred`
     * nobody completes rather than on a `delay`, for [ProbeDeadlineTest]'s
     * reason: a sleeper only proves that a timeout cancels a sleeper, where
     * this has no timer under it at all, which is the shape of a
     * reconciliation whose idle window keeps being re-armed.
     */
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
                    // Longer than the per-url deadline, so the OUTER clock is
                    // the one that fires — which is exactly the production
                    // ordering: the NEG-OPEN's own bound is generous and the
                    // url's whole budget is what runs out first.
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
            // THE WHOLE POINT. The relay answered the ladder; our clock firing
            // one step later is not a fact about it, and the verdict it earned
            // must be on the record. Before this, the url carried no grade and
            // aged another whole sweep — every sweep, for as long as it stayed
            // slow.
            assertEquals(
                Verdict.PRIME.value,
                gradeOf(store, slow),
                "a verdict the dial had already earned must survive the wall clock that cut the job",
            )
            // …and it must not be counted as a url the pass lost: an abandoned
            // url is one that told us nothing, and this one told us everything
            // `prime` asserts.
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
                    // A per-url deadline far out of a test's reach, so the only
                    // thing that can end this job is the NEG-OPEN's own clock.
                    probe = answeringProbe(idleMs = 60_000L),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    nip77DeadlineMs = 100L,
                    reconcile = parkingNegOpen(),
                )

            // The bound this test is about: an idle window is not a wall clock,
            // so without one this call does not return.
            withTimeout(30_000) {
                pass.measure("neg-open clock", listOf(slow), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            assertEquals(Verdict.PRIME.value, gradeOf(store, slow))
            // A cut NEG-OPEN is NO FACT, not a false one: the relay never
            // declined anything, and publishing `nip77 false` off our own clock
            // would tell the network a reconciling relay does not reconcile.
            assertNull(
                tagOf(store, slow, RelayVerdictRecord.NIP77_TAG),
                "a NEG-OPEN our own clock cut must publish no nip77 fact in either direction",
            )
        }

    /** A store whose writes never answer — [PublishDeadlineTest]'s wedge, which is what ends a batch early. */
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
            // Enough urls that the wedge limit cannot reach the end of the
            // batch: whatever the first pass fails to write, the second one has
            // to start from, or those urls are never re-graded however often
            // the sweep runs.
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

            // Pass one: the store takes nothing, and the wedge limit ends the
            // batch after [FitnessPass.PUBLISH_WEDGE_LIMIT] writes.
            withTimeout(30_000) {
                pass.measure("wedged", urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            // Pass two, store healthy again. The urls the wedge cost pass one
            // are the ones pass two must reach FIRST — and with the whole batch
            // writable it reaches all of them, so the assertion that matters is
            // that no url is left ungraded by two passes over the same set.
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
            // The direct statement of the rule, on a batch big enough that the
            // wedge limit leaves most of it unwritten: pass one attempts the
            // first few urls in url order, pass two must NOT attempt the same
            // ones again first.
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

            // ONE LABEL for both, because the cursor is per label — two sweeps
            // of the same corpus are one batch stream, and a lane tick is
            // another. See [FitnessPass.writeCursors].
            withTimeout(30_000) { pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE) }
            val first = synchronized(attempted) { attempted.toList() }
            synchronized(attempted) { attempted.clear() }
            withTimeout(30_000) { pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE) }
            val second = synchronized(attempted) { attempted.toList() }

            assertEquals(FitnessPass.PUBLISH_WEDGE_LIMIT, first.size, "a wedged store must cost the wedge limit and no more")
            assertEquals(FitnessPass.PUBLISH_WEDGE_LIMIT, second.size)
            // The one that tripped the limit is retried — it is the write that
            // did not land, and the limit is a statement about the store rather
            // than about that url — but the ones BEFORE it are behind us.
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
            // ONE PASS OBJECT, TWO CALLERS ON TWO CLOCKS — which is how the
            // router builds it: `MonitorEngine.fitnessEntry` is in both
            // `passes` and `fastLanePasses`. The lane runs every 120s over the
            // handful of urls named since its last look; the sweep runs every
            // few hours over the whole corpus. A cursor shared between them is
            // cleared by ~180 lane ticks before the next sweep ever reads it,
            // so the rotation would be dead code that looks alive.
            val corpusUrls = (0 until 12).map { RelayUrlNormalizer.normalize("wss://relay%02d.example".format(it)) }
            val laneUrls = listOf(RelayUrlNormalizer.normalize("wss://fresh.example"))
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val attempted = mutableListOf<String>()

            // An AtomicBoolean rather than a captured `var`: the write loop
            // runs on whatever dispatcher the store's insert suspends onto, and
            // a plain local carries no visibility guarantee across it.
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

            // A sweep the store wedges, leaving most of the corpus unwritten…
            withTimeout(30_000) {
                pass.measure(AliasMonitor.ALL_STREAMS, corpusUrls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            val sweptFirst = synchronized(attempted) { attempted.toList() }
            synchronized(attempted) { attempted.clear() }

            // …then a healthy lane tick over one fresh url, which writes its
            // whole batch and therefore has no resume point of its own.
            wedged.set(false)
            withTimeout(30_000) {
                pass.measure(AliasMonitor.FAST_LANE, laneUrls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            synchronized(attempted) { attempted.clear() }

            // The next sweep must still pick up where the wedge stopped it. On
            // a shared cursor it starts at the top instead, and the corpus's
            // tail is dropped again — the same tail, every sweep, which is the
            // starvation this rotation exists to end.
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
            // The consecutive limit looks for a store that has STOPPED
            // ANSWERING. A decline is the store answering — promptly, and
            // refusing — so it ends a run exactly as a success does. Before
            // that, an alternating store reached "three consecutive timeouts"
            // having never timed out twice in a row, and ended the batch at the
            // third write of a store that was demonstrably alive.
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

            // Every write was ATTEMPTED: the consecutive limit never trips, so
            // the batch runs to its end rather than stopping at the third url.
            // What still bounds an alternating store is the total limit, and 30
            // urls is under it — the point here is that the run counter no
            // longer fires on a store that keeps answering.
            assertEquals(
                urls.size,
                n.get(),
                "a store answering every other write is not wedged; the batch must not end on the consecutive limit",
            )
        }

    @Test
    fun `an inherited verdict is re-signed only when it would change the record`() =
        runBlocking {
            // A MEASURED verdict is always written; an INHERITED one only when
            // it says something the record does not. The two free refusals cost
            // this pass no socket — the fold proved the alias, the stability
            // gate proved the inconsistency — so stamping `measured-at = now`
            // on them claims a measurement nothing took, and the stamp is the
            // whole mechanism by which a verdict ages.
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

            // PASS ONE writes both: the record carries no grade for the alias
            // yet, so the inherited verdict changes something.
            withTimeout(30_000) {
                pass().measure(AliasMonitor.ALL_STREAMS, listOf(alias, dialled), canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }
            assertEquals(Verdict.ALIAS.value, gradeOf(store, alias))
            assertEquals(Verdict.PRIME.value, gradeOf(store, dialled))
            val afterFirst = inserts.get()
            assertEquals(2, afterFirst, "a first pass has to put both verdicts down")
            val aliasStamp = stampOf(store, alias)

            // PASS TWO writes only the one it dialled. The alias verdict is
            // unchanged and untested, so it is left exactly as it was — same
            // stamp, not merely the same grade.
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

            // …AND A CHANGE STILL LANDS. The same url now folds nowhere and is
            // dialled instead, so the record has to move off `alias`.
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

    /** The `measured-at` the fitness label carries — index 4, per NIP-32's shape. */
    private suspend fun stampOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
    ): String? = tagOf(store, url, "l")?.getOrNull(RelayVerdictRecord.LABEL_MEASURED_AT_INDEX)

    @Test
    fun `the batch's ceiling on a store that alternates is a time budget, not a count`() =
        runBlocking {
            // A store that times out every other write never trips the
            // consecutive limit — a success clears the run each time — so
            // something else has to stop it, and what is worth bounding is the
            // TIME. A count said something different on every corpus: its cost
            // is every verdict after the trip, so at twenty it let a fraction
            // of a percent of a 20,000-url batch forfeit the rest.
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
                    // Five deadlines' worth: enough that the alternating store
                    // makes real progress first, little enough to spend inside
                    // a test. The production number is twenty minutes and this
                    // line is the same code.
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

            // It stopped, and it stopped on the BUDGET rather than on a run —
            // the store answered every other write throughout, so a run never
            // reached three.
            val err = captured.toString()
            assertTrue(
                "spent waiting on writes that never came back" in err,
                "an alternating store must end the batch on the time budget, not on the consecutive limit; got: $err",
            )
            // …and it made real progress before stopping: roughly the budget
            // divided by the deadline, which is the whole point of pricing this
            // in time. A count of three would have stopped at the sixth write.
            assertTrue(
                attempts.get() > 6,
                "the budget must buy more than the consecutive limit would; only ${attempts.get()} write(s) attempted",
            )
            // The urls it did not reach are the next batch's head, not a tail
            // dropped again — the rotation is what makes stopping affordable.
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
