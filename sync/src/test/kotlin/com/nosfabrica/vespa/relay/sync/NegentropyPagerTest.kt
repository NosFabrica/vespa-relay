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
package com.nosfabrica.vespa.relay.sync

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
 * The chunker's contract. The peer models a relay with a `max_sync_events`,
 * including the halving quartz does on its behalf before refusing.
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
     * A relay with a cap, behind the quartz that splits an over-cap window itself
     * and hands a slice no size can fit to the caller's hook.
     */
    private class FakePeer(
        val density: Density,
        val cap: Int = Int.MAX_VALUE,
        // Windows (in call order) the relay drops instead of answering.
        val failAt: Set<Int> = emptySet(),
        val throwsOverMax: Boolean = false,
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

            // quartz's own loop: split what does not fit, hand over what cannot be.
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

        /** Whether the fallback page is refused: a `CLOSED` with nothing delivered. */
        var refusesPages = false

        /**
         * Events a refused page still delivered: a kind-chunked page can serve
         * one chunk and be turned away on the next.
         */
        var refusedPagesStillDeliver = 0

        override suspend fun page(
            url: NormalizedRelayUrl,
            window: Filter,
            onEvent: suspend (Event) -> Unit,
        ): PagedWindow {
            val range = window.since!!..window.until!!
            pagedRanges += range
            kindsAsked += window.kinds
            if (refusesPages) return PagedWindow(refusedPagesStillDeliver, refused = true)
            return PagedWindow(density.count(range), refused = false)
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
            // Only the first window must fit the starting target; clean ones grow it.
            assertTrue(peer.asked.size > 1, "a leg 100x the target must be cut")
            assertTrue(
                index.density.count(peer.asked.first()) <= 1_000,
                "first window holds ${index.density.count(peer.asked.first())} local events, over the starting target",
            )
            peer.asked.forEach { assertTrue(it in index.counted, "window $it was asked for without being counted first") }
            assertTiles(peer.reconciled, 1_000, 1_999)
            assertEquals(peer.asked.size, peer.reconciled.size)
        }

    @Test
    fun `a store that cannot be counted lets the peer decide`() =
        runBlocking {
            val index = FakeIndex(Density(perSecond = 100)).apply { answersCount = false }
            val peer = FakePeer(Density(perSecond = 100), cap = 5_000)
            val out = pager(index, peer).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertTiles(peer.reconciled, 1_000, 1_999)
            peer.reconciled.forEach { assertTrue(peer.density.count(it) <= 5_000) }
        }

    // `: Unit` is load-bearing on the two tests below: `zipWithNext` returns a list,
    // and JUnit 5 silently does not run a non-void @Test.
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
            // Priming the index with the count just taken spares quartz a second store round trip.
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
            val peer = FakePeer(Density(perSecond = 100), cap = 1_000)
            pager(FakeIndex(Density(perSecond = 0)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertEquals(1_000, state.peer(relay)?.cap, "the cap outlives the sweep that learned it")
            assertTrue(state.target(relay, 999_999) <= 800, "the target must fit under the cap with margin")
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
            // The failed window falls to paging, so the sweep still completes.
            val index = FakeIndex(Density(perSecond = 100))
            val out = pager(index, peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.complete)
            assertNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)), "a completed sweep leaves no cursor behind")
        }

    @Test
    fun `a stopped sweep keeps its cursor and a resume skips what it covers`() =
        runBlocking {
            val state = SweepState(null)
            // Three failures in a row stop the sweep, with everything above still done.
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
    fun `a per-kind slice the peer still refuses falls to paging, never to the floor`() =
        runBlocking {
            // Our side of the second is thin, so each per-kind reconcile hands the
            // slice straight back through `onUnreconcilable`.
            val shape = Filter(kinds = listOf(1, 7))
            val hot = Density(perSecond = 1, spikes = mapOf(1_500L to 10_000))
            val peer = FakePeer(hot, cap = 5_000)
            val out = pager(FakeIndex(Density(perSecond = 0)), peer).sweep(mirror, relay, shape, shape.copy(since = 1_000, until = 1_999)) {}

            assertTrue(out.complete)
            assertTrue(
                peer.pagedRanges.any { 1_500L in it },
                "a per-kind slice refused at any window size must be paged over REQ: ${peer.pagedRanges}",
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
            assertTiles(peer.reconciled + peer.pagedRanges, 1_000, 1_999)
        }

    @Test
    fun `the cursor never reaches past a window that is still pending`() =
        runBlocking {
            // Checked at every step: the cursor must stay strictly above the window about
            // to be asked. The escape hatch takes a slice out of the middle of a window,
            // leaving a piece above it pending.
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

    // ---- when the fallback page is refused too --------------------------------

    @Test
    fun `a window whose fallback page was refused is NOT claimed by the cursor`() =
        runBlocking {
            // Mid-sweep: the first window reconciles and the second falls to a refused page.
            val state = SweepState(null)
            // Dense enough to cut the leg into several windows, so there is a second one to drop.
            val peer = FakePeer(Density(perSecond = 100), failAt = setOf(1)).apply { refusesPages = true }
            val out = pager(FakeIndex(Density(perSecond = 100)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(peer.pagedRanges.isNotEmpty(), "the fallback page was attempted")
            assertEquals(1, out.refusedWindows, "…and counted as a window this sweep could not read")
            assertFalse(out.complete, "a sweep holding an unread window has not verified the history")
            // The cursor is the durable half: it may only cover ground that was read.
            val held = state.reconciled(SweepState.keyFor(mirror, relay, notes))
            assertTrue(
                held == null || held.downTo > peer.pagedRanges.first().first,
                "the cursor claimed ${held?.downTo} but the window at ${peer.pagedRanges.first()} was refused",
            )
        }

    @Test
    fun `a fallback page that delivered SOME events and was still refused is not claimed`() =
        runBlocking {
            val state = SweepState(null)
            val peer =
                FakePeer(Density(perSecond = 100), failAt = setOf(1)).apply {
                    refusesPages = true
                    refusedPagesStillDeliver = 25
                }
            val out = pager(FakeIndex(Density(perSecond = 100)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(out.downloaded >= 25, "what the served chunk delivered is kept")
            assertEquals(1, out.refusedWindows, "…and the refusal is still seen")
            assertFalse(out.complete, "so the leg cannot claim its history was verified")
        }

    @Test
    fun `a window whose fallback page was SERVED is claimed exactly as before`() =
        runBlocking {
            val state = SweepState(null)
            val peer = FakePeer(Density(perSecond = 100), failAt = setOf(1))
            val out = pager(FakeIndex(Density(perSecond = 100)), peer, state).sweep(mirror, relay, notes, leg(1_000, 1_999)) {}

            assertTrue(peer.pagedRanges.isNotEmpty(), "the fallback page was attempted")
            assertEquals(0, out.refusedWindows)
            assertTrue(out.complete, "every window was read, one way or the other")
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
