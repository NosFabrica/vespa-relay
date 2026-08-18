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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The chunker's contract, which is easier to state than to eyeball in a log:
 *
 *  - every window is sized before it is asked for, from BOTH sides;
 *  - windows are walked newest-first, because the one-timestamp cursor is only
 *    correct while the finished region stays contiguous;
 *  - a crash resumes where the cursor points, and a completed sweep leaves none;
 *  - a second nobody can reconcile costs that second, not the sweep.
 *
 * The peer here is a model of a relay with a `max_sync_events`, including the
 * part quartz does for us: it halves an over-cap window itself and only refuses
 * when a one-second slice is still over.
 */
class NegentropyPagerTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val notes = Filter(kinds = listOf(1))
    private val mirror = "notes"

    /** Events per second on one side of the wire, plus optional dense seconds. */
    private class Density(
        val perSecond: Int,
        val spikes: Map<Long, Int> = emptyMap(),
    ) {
        fun count(range: LongRange): Int {
            val seconds = range.last - range.first + 1
            val base = seconds * perSecond
            val extra = spikes.entries.filter { it.key in range }.sumOf { it.value.toLong() }
            return (base + extra).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        }
    }

    private class FakeIndex(
        val density: Density,
    ) : NegentropyLocalIndex {
        val counted = mutableListOf<LongRange>()
        val read = mutableListOf<LongRange>()
        var answersCount = true

        override suspend fun count(window: Filter): Int? {
            val range = window.since!!..window.until!!
            counted += range
            return if (answersCount) density.count(range) else null
        }

        override suspend fun entriesFor(window: Filter): List<IdAndTime> {
            val range = window.since!!..window.until!!
            read += range
            return List(density.count(range)) { IdAndTime(range.first, it.toString().padStart(64, '0')) }
        }
    }

    /**
     * A relay with a cap, wrapped in the quartz that now talks to it: it splits
     * an over-cap window itself, hands a slice no window size can fit to the
     * caller's hook instead of throwing, and reports the cap it refused with.
     */
    private class FakePeer(
        val density: Density,
        val cap: Int = Int.MAX_VALUE,
        // Windows (in call order) the relay drops instead of answering.
        val failAt: Set<Int> = emptySet(),
        // Refuse through the exception rather than the hook — the defensive
        // path, for a quartz that could not hand the window over.
        val throwsOverMax: Boolean = false,
        // A relay whose refusal carries no number: nothing to fit to, so the
        // caller is left halving.
        val statesCap: Boolean = true,
    ) : WindowSync {
        val asked = mutableListOf<LongRange>()
        val reconciled = mutableListOf<LongRange>()
        val pagedRanges = mutableListOf<LongRange>()
        val kindsAsked = mutableListOf<List<Int>?>()
        val targetsSeen = mutableListOf<Int>()
        var calls = 0

        override suspend fun reconcile(
            url: NormalizedRelayUrl,
            window: Filter,
            local: NegentropyLocalIndex,
            targetWindow: Int,
            onProgress: ((Int, Int) -> Unit)?,
            onUnreconcilable: suspend (Filter) -> Unit,
            onEvent: suspend (Event) -> Unit,
        ): NegentropySyncResult {
            val range = window.since!!..window.until!!
            asked += range
            kindsAsked += window.kinds
            targetsSeen += targetWindow
            if (calls++ in failAt) {
                throw NegentropySyncException(url, window, NegentropySyncException.Reason.UNAVAILABLE, "relay disconnected")
            }
            val done = mutableListOf<LongRange>()
            var refused = false

            // quartz's own loop: split what does not fit, hand over what cannot
            // be split, and keep going.
            suspend fun walk(r: LongRange) {
                if (density.count(r) <= cap) {
                    done += r
                    return
                }
                refused = true
                if (r.last - r.first <= NegentropyPager.MIN_WINDOW_SECONDS) {
                    if (throwsOverMax) {
                        throw NegentropySyncException(
                            url,
                            window.copy(since = r.first, until = r.last),
                            NegentropySyncException.Reason.OVER_MAX_SYNC_EVENTS,
                            "created_at window [${r.first}, ${r.last}] still exceeds the relay's max_sync_events",
                            cap.toLong(),
                        )
                    }
                    onUnreconcilable(window.copy(since = r.first, until = r.last))
                    return
                }
                val mid = r.first + (r.last - r.first) / 2
                walk(r.first..mid)
                walk((mid + 1)..r.last)
            }
            walk(range)
            reconciled += done
            val downloaded = done.sumOf { density.count(it) }
            return NegentropySyncResult(
                needCount = downloaded,
                haveCount = 0,
                downloaded = downloaded,
                windows = done.size.coerceAtLeast(1),
                peerCap = if (refused && statesCap) cap.toLong() else null,
            )
        }

        override suspend fun page(
            url: NormalizedRelayUrl,
            window: Filter,
            onEvent: suspend (Event) -> Unit,
        ): Int {
            val range = window.since!!..window.until!!
            pagedRanges += range
            kindsAsked += window.kinds
            return density.count(range)
        }
    }

    private fun pager(
        index: NegentropyLocalIndex,
        peer: WindowSync,
        state: SweepState = SweepState(null),
        tuning: NegPageTuning = NegPageTuning(target = 1_000, minTarget = 10, maxTarget = 100_000, slackSeconds = 60),
    ) = NegentropyPager(index, peer, state, tuning)

    private fun leg(
        since: Long,
        until: Long,
    ) = notes.copy(since = since, until = until)

    /** Windows must tile the leg exactly: no gap, no overlap. */
    private fun assertTiles(
        covered: List<LongRange>,
        since: Long,
        until: Long,
    ) {
        val sorted = covered.sortedBy { it.first }
        assertEquals(since, sorted.first().first, "sweep starts at the leg's floor")
        assertEquals(until, sorted.last().last, "sweep ends at the leg's ceiling")
        sorted.zipWithNext { a, b -> assertEquals(a.last + 1, b.first, "windows must be contiguous: $a then $b") }
    }

    // ---- (1) our side --------------------------------------------------------

    @Test
    fun `a store denser than the target is cut before any round trip`() =
        runBlocking {
            // 100/s over 1000s = 100_000 local events against a 1000-event target.
            val index = FakeIndex(Density(perSecond = 100))
            val peer = FakePeer(Density(perSecond = 100))
            val out = pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            // The leg holds 100x the target, so it cannot have gone in one ask,
            // and the FIRST window — the one asked before anything is learned —
            // must fit the starting target. Later windows are allowed to be
            // bigger: clean ones grow the target, which is the point of it.
            assertTrue(peer.asked.size > 1, "a leg 100x the target must be cut")
            assertTrue(
                index.density.count(peer.asked.first()) <= 1_000,
                "first window holds ${index.density.count(peer.asked.first())} local events, over the starting target",
            )
            // Every window was counted before it was asked for — that is what
            // "before any round trip" means.
            peer.asked.forEach { assertTrue(it in index.counted, "window $it was asked for without being counted first") }
            assertTiles(peer.reconciled, 1_000, 1_999)
            // Nothing was refused: the pre-split did the work, so the peer never
            // had to split anything itself.
            assertEquals(peer.asked.size, peer.reconciled.size)
        }

    @Test
    fun `a store that cannot be counted lets the peer decide`() =
        runBlocking {
            val index = FakeIndex(Density(perSecond = 100)).apply { answersCount = false }
            // The peer's own cap is what cuts it up instead.
            val peer = FakePeer(Density(perSecond = 100), cap = 5_000)
            val out = pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertTiles(peer.reconciled, 1_000, 1_999)
            peer.reconciled.forEach { assertTrue(peer.density.count(it) <= 5_000) }
        }

    // `: Unit` is load-bearing on the two tests below and not decoration:
    // an expression-bodied test whose last statement HAS a value —
    // `zipWithNext` returns the list of its lambda's results — compiles to a
    // method returning that type, and JUnit 5 silently does not run a
    // non-void @Test. `windows are walked newest first` had been skipped that
    // way, unnoticed, since it was written. Declaring Unit discards the value
    // and the method comes back void.
    @Test
    fun `windows are walked newest first`(): Unit =
        runBlocking {
            val index = FakeIndex(Density(perSecond = 100))
            val peer = FakePeer(Density(perSecond = 1))
            pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            peer.asked.zipWithNext { a, b ->
                assertTrue(b.last < a.first, "windows must descend: asked $a then $b")
            }
        }

    @Test
    fun `the window cursor is the older edge, announced after the cut, and only descends`(): Unit =
        runBlocking {
            // The in-flight row draws this as `back to <date>` beside
            // `auditing history (negentropy)`, in the same direction as a paged
            // leg's cursor. Two ways that reading used to be wrong: the newer
            // edge (a sweep with years left reporting today's date), and
            // announcing a window BEFORE it is cut (the whole leg, then a
            // cursor walking upwards as each bisection narrowed it).
            val index = FakeIndex(Density(perSecond = 100))
            val peer = FakePeer(Density(perSecond = 1))
            val announced = mutableListOf<LongRange>()
            pager(index, peer)
                .sweep(mirror, relay, notes, leg(1_000, 1_999), onWindow = { since, until -> announced += since..until }) {}

            assertEquals(peer.asked, announced.toList(), "only the windows actually reconciled are announced")
            assertTrue(announced.size > 1, "a leg 100x the target must be cut, or this test proves nothing")
            assertEquals(1_999L, announced.first().last, "the first window announced is the newest")
            assertEquals(1_000L, announced.last().first, "the last one reaches the leg's floor")
            announced.zipWithNext { a, b ->
                assertTrue(b.first < a.first, "the cursor must only go back: announced $a then $b")
            }
        }

    @Test
    fun `a store that cannot count still hands the peer a bound`() =
        runBlocking {
            // Without a count there is nothing to pre-split on, so the window
            // goes to the peer whole — but never unbounded: the target rides
            // along, and quartz splits and reads inside it against that number.
            val index = FakeIndex(Density(perSecond = 100)).apply { answersCount = false }
            val peer = FakePeer(Density(perSecond = 100))
            val out = pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertTrue(peer.targetsSeen.isNotEmpty())
            peer.targetsSeen.forEach { assertTrue(it in 10..100_000, "target $it is outside the configured bounds") }
            assertTrue(index.read.isEmpty(), "the pager must not materialise ids itself any more")
        }

    @Test
    fun `the count the pager took is not asked for twice`() =
        runBlocking {
            // quartz sizes the window it is handed before splitting it, using
            // the same count this layer just took. Priming the index is what
            // keeps that from being a second store round trip per window.
            val index = FakeIndex(Density(perSecond = 1))
            val peer =
                object : WindowSync by FakePeer(Density(perSecond = 1)) {
                    var seen: Int? = null

                    override suspend fun reconcile(
                        url: NormalizedRelayUrl,
                        window: Filter,
                        local: NegentropyLocalIndex,
                        targetWindow: Int,
                        onProgress: ((Int, Int) -> Unit)?,
                        onUnreconcilable: suspend (Filter) -> Unit,
                        onEvent: suspend (Event) -> Unit,
                    ): NegentropySyncResult {
                        // What quartz does first for the window it is given.
                        seen = local.count(window)
                        return NegentropySyncResult(0, 0, 0, 1, null)
                    }
                }
            pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertEquals(1_000, peer.seen, "the primed count must come back without another query")
            assertEquals(1, index.counted.size, "one count for the window, not two")
        }

    // ---- (2) their side ------------------------------------------------------

    @Test
    fun `a cap below the target pulls it down, a cap above it does not`() =
        runBlocking {
            // The number is the point: a peer that will take 400 must be asked
            // for less than we planned, and a peer that will take 40,000 must
            // not drag us down just because it happened to split this window.
            val tight = SweepState(null)
            pager(FakeIndex(Density(perSecond = 0)), FakePeer(Density(perSecond = 100), cap = 500), tight)
                .sweep(mirror, relay, notes, leg(1_000, 1_999)) {}
            assertEquals(400, tight.target(relay, 1_000), "the target must come down to the cap, with margin")

            val roomy = SweepState(null)
            pager(FakeIndex(Density(perSecond = 0)), FakePeer(Density(perSecond = 100), cap = 50_000), roomy)
                .sweep(mirror, relay, notes, leg(1_000, 1_999)) {}
            assertTrue(roomy.target(relay, 1_000) >= 1_000, "a cap above the target is not a reason to shrink")
        }

    @Test
    fun `a refusal with no number leaves us halving`() =
        runBlocking {
            // Nothing to fit to, so the only move is to ask for less next time.
            val state = SweepState(null)
            val peer = FakePeer(Density(perSecond = 100), cap = 5_000, statesCap = false)
            pager(FakeIndex(Density(perSecond = 0)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(state.target(relay, 1_000) < 1_000, "a split we cannot explain must still shrink the ask")
            assertNull(state.peer(relay)?.cap, "and it must not invent a cap it was never told")
        }

    @Test
    fun `a clean sweep grows the window back`() =
        runBlocking {
            val state = SweepState(null)
            state.setTarget(relay, 100)
            val index = FakeIndex(Density(perSecond = 0))
            val peer = FakePeer(Density(perSecond = 0))
            pager(index, peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(state.target(relay, 1_000) > 100, "clean windows must grow the learned size")
        }

    @Test
    fun `a stated cap sizes the window in one step and is remembered`() =
        runBlocking {
            val state = SweepState(null)
            // The peer refuses once and says what it will take; quartz parses
            // that off the refusal and reports it, so no ladder is walked.
            val peer = FakePeer(Density(perSecond = 100), cap = 1_000)
            pager(FakeIndex(Density(perSecond = 0)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertEquals(1_000, state.peer(relay)?.cap, "the cap outlives the sweep that learned it")
            assertTrue(state.target(relay, 999_999) <= 800, "the target must fit under the cap with margin")
            // And it never grows past it, however many clean windows follow.
            pager(FakeIndex(Density(perSecond = 0)), FakePeer(Density(perSecond = 0)), state)
                .sweep(mirror, relay, notes, leg(2_000, 2_999)) {}
            assertTrue(state.target(relay, 999_999) <= 800)
        }

    // ---- the cursor ----------------------------------------------------------

    @Test
    fun `the cursor tracks the contiguous finished region and a completed sweep clears it`() =
        runBlocking {
            val state = SweepState(null)
            val peer = FakePeer(Density(perSecond = 1), failAt = setOf(2))
            // Fails mid-sweep: the failed window is paged, so the sweep still
            // finishes and the cursor is cleared. What we want to see is that
            // while it ran, the cursor only ever described one contiguous slice.
            val index = FakeIndex(Density(perSecond = 100))
            val out = pager(index, peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)), "a completed sweep leaves no cursor behind")
        }

    @Test
    fun `a stopped sweep keeps its cursor and a resume skips what it covers`() =
        runBlocking {
            val state = SweepState(null)
            // Three failures in a row stops it, with everything above still done.
            val stopping = FakePeer(Density(perSecond = 1), failAt = setOf(1, 2, 3, 4, 5, 6))
            val index = FakeIndex(Density(perSecond = 100))
            val first = pager(index, stopping, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertFalse(first.complete)
            val mark = assertNotNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)), "a stopped sweep must leave a cursor")
            assertEquals(1_999, mark.upTo, "the finished region starts at the leg's ceiling")

            val second = FakePeer(Density(perSecond = 1))
            pager(FakeIndex(Density(perSecond = 100)), second, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            second.asked.forEach {
                assertTrue(it.last < mark.downTo, "resumed windows must stay below the cursor, asked $it against $mark")
            }
        }

    @Test
    fun `a cursor from a different filter shape is not reused`() =
        runBlocking {
            val state = SweepState(null)
            state.advance(SweepState.keyFor(mirror, relay, notes), 1_500, 1_999)
            val profiles = Filter(kinds = listOf(0))
            val peer = FakePeer(Density(perSecond = 1))
            pager(FakeIndex(Density(perSecond = 0)), peer, state).sweep(mirror, relay, profiles, profiles.copy(since = 1_000, until = 1_999)) {}

            assertTiles(peer.reconciled, 1_000, 1_999)
        }

    // ---- the escape hatch ----------------------------------------------------

    @Test
    fun `a second the peer will not reconcile is split by kind`() =
        runBlocking {
            val shape = Filter(kinds = listOf(1, 7))
            // One second holds more than the cap all by itself.
            val hot = Density(perSecond = 1, spikes = mapOf(1_500L to 10_000))
            val peer = FakePeer(hot, cap = 5_000)
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, shape, shape.copy(since = 1_000, until = 1_999)) {}

            assertTrue(out.complete, "one un-splittable second must not cost the sweep")
            assertTrue(
                peer.kindsAsked.any { it == listOf(1) } && peer.kindsAsked.any { it == listOf(7) },
                "the dense second must be retried per kind",
            )
        }

    @Test
    fun `a single-kind second the peer will not reconcile is paged`() =
        runBlocking {
            val hot = Density(perSecond = 1, spikes = mapOf(1_500L to 10_000))
            val peer = FakePeer(hot, cap = 5_000)
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertEquals(1, out.pagedWindows)
            assertTrue(peer.pagedRanges.any { 1_500L in it }, "the dense second must be paged: ${peer.pagedRanges}")
            // And the rest of the leg is still reconciled, not paged with it.
            assertTiles(peer.reconciled + peer.pagedRanges, 1_000, 1_999)
        }

    @Test
    fun `the cursor never reaches past a window that is still pending`() =
        runBlocking {
            // The invariant everything else rests on, checked at every step
            // rather than at the end: whatever the cursor claims must be
            // strictly above the window about to be asked for. A crash between
            // any two windows resumes from that cursor, so a claim that reaches
            // past pending work silently skips events — the one failure mode
            // here that loses data instead of repeating it.
            //
            // Driven through the escape hatch (a second the peer refuses at any
            // size), because that is the path that takes a slice out of the
            // MIDDLE of a window and leaves a piece above it pending.
            val state = SweepState(null)
            val shape = notes
            val hot = Density(perSecond = 10, spikes = mapOf(1_500L to 100_000))
            val peer = FakePeer(hot, cap = 20_000)
            val checking =
                object : NegentropyLocalIndex {
                    val inner = FakeIndex(Density(perSecond = 10))

                    override suspend fun count(window: Filter): Int? {
                        val claimed = state.reconciled(SweepState.keyFor(mirror, relay, shape))
                        if (claimed != null) {
                            assertTrue(
                                claimed.downTo > window.until!!,
                                "cursor claims down to ${claimed.downTo} while ${window.since}..${window.until} is still pending",
                            )
                        }
                        return inner.count(window)
                    }

                    override suspend fun entriesFor(window: Filter): List<IdAndTime> = inner.entriesFor(window)
                }
            val out = pager(checking, peer, state).sweep(mirror, relay, shape, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertEquals(1, out.pagedWindows)
        }

    // ---- when negentropy is not on offer at all -------------------------------

    @Test
    fun `a peer that fails the first window is reported unusable`() =
        runBlocking {
            val peer = FakePeer(Density(perSecond = 1), failAt = setOf(0))
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertFalse(out.negentropyUsable)
            assertFalse(out.complete)
            assertEquals(1, peer.asked.size, "a relay that cannot reconcile must not be asked window after window")
            assertTrue(peer.pagedRanges.isEmpty(), "paging the whole leg is the caller's decision, not the pager's")
        }

    @Test
    fun `a resumed sweep that cannot reconcile hands back only what is left`() =
        runBlocking {
            // A relay that stops speaking NIP-77 between runs must not cost the
            // caller the ground the cursor was holding: what comes back is the
            // un-swept remainder, not the whole leg.
            val state = SweepState(null)
            state.advance(SweepState.keyFor(mirror, relay, notes), 1_500, 1_999)
            val peer = FakePeer(Density(perSecond = 1), failAt = setOf(0))
            val out = pager(FakeIndex(Density(perSecond = 0)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertFalse(out.negentropyUsable)
            val rest = assertNotNull(out.outstanding, "the caller needs to know what to page")
            assertEquals(1_000, rest.since)
            assertEquals(1_499, rest.until, "everything the cursor already claims stays claimed")
        }

    @Test
    fun `a fresh sweep that cannot reconcile hands back the whole leg`() =
        runBlocking {
            val peer = FakePeer(Density(perSecond = 1), failAt = setOf(0))
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            val rest = assertNotNull(out.outstanding)
            assertEquals(1_000, rest.since)
            assertEquals(1_999, rest.until)
        }

    // ---- the live head -------------------------------------------------------

    @Test
    fun `nothing inside the slack window is swept`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val peer = FakePeer(Density(perSecond = 1))
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, notes, notes.copy(since = now - 3_600, until = now)) {}

            assertTrue(out.complete)
            peer.asked.forEach {
                assertTrue(it.last <= now - 60, "swept $it, which reaches into the live head at ${now - 60}")
            }
        }

    @Test
    fun `a leg entirely inside the live head does nothing and claims nothing`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val peer = FakePeer(Density(perSecond = 1))
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, notes, notes.copy(since = now - 10, until = now)) {}

            assertFalse(out.complete, "the live tail owns that range; a sweep must not claim it")
            assertTrue(peer.asked.isEmpty())
        }
}
