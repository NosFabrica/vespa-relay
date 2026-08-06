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

import com.nosfabrica.vespa.relay.util.fmtCount
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySync
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/**
 * Sizes for the window stack. All three are counts of EVENTS; the pager turns
 * them into `created_at` boundaries, because seconds are the only axis a Nostr
 * filter can actually be cut on.
 *
 * @param target    where a peer we have never asked starts. Big enough that a
 *   healthy relay reconciles it in one NEG-OPEN, small enough that our side of
 *   it is a few megabytes of packed ids rather than the whole corpus.
 * @param minTarget the floor the learned target may shrink to. A peer that
 *   refuses even this is refusing negentropy, not sizing it.
 * @param maxTarget the ceiling it may grow to.
 * @param slackSeconds how far below `now` a sweep stops. A window has to be
 *   IMMUTABLE to be checkpointable — events are still arriving at the top of
 *   the range, and a cursor written across them would claim ground that moved.
 *   The live subscription is what covers the head; this is the seam between them.
 */
internal data class NegPageTuning(
    val target: Int = 100_000,
    val minTarget: Int = 1_000,
    val maxTarget: Int = 1_000_000,
    val slackSeconds: Long = 60,
)

/** Our own side of a window: how many we hold there, and which ids. */
internal interface LocalIndex {
    /** Null when the store could not answer — the pager then lets the peer decide. */
    suspend fun count(window: Filter): Int?

    suspend fun ids(window: Filter): List<IdAndTime>
}

/** One window against one peer: reconcile it, or (the escape hatch) page it. */
internal interface WindowSync {
    /** @throws NegentropySyncException when NIP-77 cannot enumerate this window. */
    suspend fun reconcile(
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        onProgress: ((Int, Int) -> Unit)?,
        onEvent: suspend (Event) -> Unit,
    ): NegentropySyncResult

    suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Int
}

internal class StoreLocalIndex(
    private val store: IEventStore,
) : LocalIndex {
    override suspend fun count(window: Filter): Int? = runCatching { store.count(window) }.getOrNull()

    override suspend fun ids(window: Filter): List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(window))
}

internal class ClientWindowSync(
    private val client: NostrClient,
    private val idleTimeoutMs: Long = NEG_IDLE_MS,
) : WindowSync {
    override suspend fun reconcile(
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        onProgress: ((Int, Int) -> Unit)?,
        onEvent: suspend (Event) -> Unit,
    ): NegentropySyncResult =
        client.negentropySync(
            relay = url,
            filter = window,
            idleTimeoutMs = idleTimeoutMs,
            localEntries = local,
            onProgress = onProgress,
            onEvent = onEvent,
        )

    override suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Int = client.fetchAllPages(url, listOf(window), idleTimeoutMs, onEvent = onEvent)
}

/** What one [NegentropyPager.sweep] did with one leg of one peer. */
internal class SweepOutcome(
    val downloaded: Int,
    val reconciledWindows: Int,
    val pagedWindows: Int,
    /** The leg is fully covered — the caller may record a band for it. */
    val complete: Boolean,
    /**
     * False when the peer could not reconcile the FIRST window at all (no
     * NIP-77, refused, unreachable). Nothing was learned about the rest of the
     * leg, so the caller should page the leg itself — the same fallback
     * `negentropySyncOrFetch` makes, kept in the caller's hands.
     */
    val negentropyUsable: Boolean,
    val failure: NegentropySyncException?,
)

/**
 * Negentropy paging that sizes itself: one work stack of `created_at` windows,
 * split from BOTH ends, checkpointed per window.
 *
 * The problem it solves is that a NEG-OPEN is all-or-nothing at both ends of
 * the wire. Ours: a reconcile needs our matching ids up front, and on a corpus
 * this size that snapshot is the most expensive thing the router does (measured
 * elsewhere in this package at 14.9M ids for one stream, and three concurrent
 * streams heading for ~9 GiB — which is why they are serialised behind a
 * semaphore at all). Theirs: a relay refuses outright past its `max_sync_events`
 * rather than answering partially. Neither side can see the other's size, and
 * nothing in the protocol negotiates it.
 *
 * So the boundary is expressed as a timestamp but decided by a COUNT, and two
 * independent sources may split a window:
 *
 *  1. **We are dense.** [LocalIndex.count] over the window, before a round trip
 *    is spent. Cheap against an indexed store, and it is what bounds our own
 *    snapshot: a window that passes this check is at most `target` ids, so peak
 *    memory is a property of the target rather than of the corpus.
 *  2. **They are dense.** The peer refuses, or quartz's own splitter had to cut
 *    the window up to get an answer ([NegentropySyncResult.windows] > 1). Either
 *    way the target shrinks, so the NEXT window is asked at a size they will
 *    take — and if they told us the number ([NegErrWatcher]), it shrinks to
 *    theirs in one step instead of a halving ladder.
 *
 * Neither source knows anything about the other, and the same stack absorbs
 * both. That is what makes this automatic rather than tuned.
 *
 * **The division of labour with quartz.** quartz already halves a window on
 * overflow and retries, down to one second — that is its job and this does not
 * duplicate it. What it cannot do is any of the three things that make paging
 * work at scale: it never sees OUR count (it slices the local list we already
 * built, so our snapshot is whatever we handed it), it forgets everything
 * between calls, and it has nowhere to write a cursor. This layer supplies
 * exactly those: the pre-split, the learned per-peer size, and the checkpoint.
 *
 * **Ordering is not incidental.** Windows are popped strictly newest-first, so
 * the finished region is always a single contiguous slice growing downward from
 * the leg's ceiling — which is the only reason a cursor can be one timestamp
 * instead of a set of intervals. Every push below keeps that invariant.
 */
internal class NegentropyPager(
    private val local: LocalIndex,
    private val peer: WindowSync,
    private val state: SweepState,
    private val tuning: NegPageTuning,
) {
    /**
     * Reconcile [leg] against [url], one right-sized window at a time.
     *
     * [shape] is the stream's filter — the cursor's identity and the shape every
     * window keeps. Only `since`/`until` vary across windows, deliberately:
     * strfry matches a declared negentropy tree by comparing canonicalised
     * filter JSON, so a shape that stays byte-identical rides their index across
     * the whole sweep, while sub-partitioning on any other axis drops the
     * window onto the capped snapshot path. Which is why the kind split below
     * is an escape hatch and not a strategy.
     */
    suspend fun sweep(
        url: NormalizedRelayUrl,
        shape: Filter,
        leg: Filter,
        onProgress: ((Int, Int) -> Unit)? = null,
        onEvent: suspend (Event) -> Unit,
    ): SweepOutcome {
        val floor = leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR
        // Immutability, and the seam with the live tail: nothing within
        // `slackSeconds` of now is swept, because a window that is still
        // receiving events cannot be checkpointed honestly.
        val head = nowSeconds() - tuning.slackSeconds
        val ceiling = minOf(leg.until ?: head, head)
        if (ceiling < floor) {
            // The whole leg is inside the live head. Not an error and not
            // complete: the subscription owns that range, not this sweep.
            return SweepOutcome(0, 0, 0, complete = false, negentropyUsable = true, failure = null)
        }

        var target = state.target(url, tuning.target).coerceIn(tuning.minTarget, tuning.maxTarget)
        val startedTarget = target
        val stack = ArrayDeque<LongRange>()
        pushResumed(stack, url, shape, floor, ceiling)

        var downloaded = 0
        var reconciled = 0
        var paged = 0
        var consecutiveFailures = 0
        var lastFailure: NegentropySyncException? = null

        while (stack.isNotEmpty()) {
            // Newest first — the invariant the cursor rests on.
            val w = stack.removeLast()
            val minimal = w.last - w.first <= MIN_WINDOW_SECONDS

            // (1) Our side, before a round trip is spent. Skipped on a minimal
            // window: there is nothing left to cut, and reconciling a dense
            // second still beats re-downloading it.
            if (!minimal) {
                val ours = local.count(window(shape, w))
                if (ours != null && ours > target) {
                    bisect(stack, w)
                    continue
                }
            }

            val ids = local.ids(window(shape, w))
            try {
                val result = peer.reconcile(url, window(shape, w), ids, onProgress) { onEvent(it) }
                downloaded += result.downloaded
                reconciled++
                consecutiveFailures = 0
                // (2) Their side. `windows > 1` means quartz had to cut this
                // window up to get an answer — the peer is denser than our count
                // suggested, and the next window should be asked smaller.
                target = if (result.windows > 1) shrink(url, target) else grow(url, target)
                complete(url, shape, w)
            } catch (e: NegentropySyncException) {
                lastFailure = e
                when (e.reason) {
                    NegentropySyncException.Reason.OVER_MAX_SYNC_EVENTS -> {
                        // The peer refused a slice even after quartz halved this
                        // window down to the second. Learn from it, drain that
                        // second by other means, and keep the rest of the window
                        // on the stack.
                        target = shrink(url, target)
                        val (got, pagedHere) = drainOverflow(url, shape, w, e, stack, onEvent)
                        downloaded += got
                        paged += pagedHere
                        consecutiveFailures = 0
                    }

                    NegentropySyncException.Reason.UNAVAILABLE -> {
                        if (reconciled == 0 && paged == 0) {
                            // The first window never landed: this peer does not
                            // do negentropy for us at all. Say so and let the
                            // caller page the leg — a sweep of windows that each
                            // fail the same way would be the same paging with
                            // extra round trips.
                            return SweepOutcome(downloaded, 0, 0, complete = false, negentropyUsable = false, failure = e)
                        }
                        // Mid-sweep: one window's worth of trouble. Page it so
                        // the sweep keeps its contiguity and moves on.
                        System.err.println(
                            "router: sweep ${url.url} window [${w.first}, ${w.last}] could not reconcile (${e.detail}) — paging this window",
                        )
                        downloaded += peer.page(url, window(shape, w), onEvent)
                        paged++
                        complete(url, shape, w)
                        if (++consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                            System.err.println(
                                "router: sweep ${url.url} gave up after $consecutiveFailures window(s) in a row failed" +
                                    " — ${fmtCount(downloaded)} event(s) kept, cursor holds at ${cursorLow(url, shape) ?: w.first}",
                            )
                            return SweepOutcome(downloaded, reconciled, paged, complete = false, negentropyUsable = true, failure = e)
                        }
                    }
                }
            }
        }

        // The leg is done; the band the caller records now is the durable
        // statement, so the working cursor goes.
        state.finish(url, shape)
        if (target != startedTarget) {
            System.err.println(
                "router: sweep ${url.url} window size ${fmtCount(startedTarget)} → ${fmtCount(target)} event(s)" +
                    (state.peer(url)?.cap?.let { " (their cap ${fmtCount(it)})" } ?: ""),
            )
        }
        return SweepOutcome(downloaded, reconciled, paged, complete = true, negentropyUsable = true, failure = lastFailure)
    }

    /**
     * [NegErrWatcher]'s hook: the peer stated its `max_sync_events` in a
     * refusal. Recorded against the peer, and the window size drops to fit it
     * immediately — the whole point of reading the number is not having to find
     * it by halving. Never RAISES the target: the cap is a ceiling the peer
     * enforces, not evidence that a larger window would have worked.
     */
    fun learnCap(
        url: NormalizedRelayUrl,
        cap: Int,
    ) {
        val fitted = (cap * CAP_MARGIN).toInt().coerceIn(tuning.minTarget, tuning.maxTarget)
        state.learnCap(url, cap, minOf(state.target(url, tuning.target), fitted))
    }

    /**
     * The stack's starting state: the leg, minus whatever a previous run
     * already reconciled.
     *
     * Pushed low-then-high so the first pop is the newest window — every push
     * in this class does, because the cursor is only one number for as long as
     * the finished region stays contiguous.
     */
    private fun pushResumed(
        stack: ArrayDeque<LongRange>,
        url: NormalizedRelayUrl,
        shape: Filter,
        floor: Long,
        ceiling: Long,
    ) {
        val done = state.reconciled(url, shape)
        if (done == null || done.upTo < floor || done.downTo > ceiling) {
            stack.addLast(floor..ceiling)
            return
        }
        System.err.println(
            "router: sweep ${url.url} resuming — [${done.downTo}, ${done.upTo}] already reconciled",
        )
        if (done.downTo > floor) stack.addLast(floor..(done.downTo - 1))
        if (done.upTo < ceiling) stack.addLast((done.upTo + 1)..ceiling)
    }

    /** Halve a window in time; the halves are pushed newest-last so they pop newest-first. */
    private fun bisect(
        stack: ArrayDeque<LongRange>,
        w: LongRange,
    ) {
        val mid = w.first + (w.last - w.first) / 2
        stack.addLast(w.first..mid)
        stack.addLast((mid + 1)..w.last)
    }

    /**
     * A slice the peer will not reconcile at any window size.
     *
     * `created_at` has second granularity and is author-controlled, so a single
     * second holding more than a relay's cap is reachable and cannot be cut on
     * the time axis — the loop would spin forever. Two ways out, in order of
     * how much they cost:
     *
     *  1. **Split by kind.** Only available to a multi-kind filter, and it
     *     changes the filter shape, which on strfry means the window stops
     *     matching their declared tree and falls onto the capped snapshot path.
     *     A deliberate downgrade, taken only here.
     *  2. **Page that second over REQ.** Always available. `fetchAllPages`
     *     steps past a second denser than one page rather than looping on it,
     *     so this terminates — at the cost of that second's unreachable tail.
     *
     * The rest of the failed window goes back on the stack around the slice, so
     * a sweep loses one second's guarantee rather than the window.
     */
    private suspend fun drainOverflow(
        url: NormalizedRelayUrl,
        shape: Filter,
        w: LongRange,
        e: NegentropySyncException,
        stack: ArrayDeque<LongRange>,
        onEvent: suspend (Event) -> Unit,
    ): Pair<Int, Int> {
        // The slice quartz actually failed on, clamped into this window: it
        // reports the exact sub-range, so there is no need to guess which
        // second is the dense one.
        val badFrom = (e.window.since ?: w.first).coerceIn(w.first, w.last)
        val badTo = (e.window.until ?: w.last).coerceIn(badFrom, w.last)

        var downloaded = 0
        var paged = 0
        val kinds = shape.kinds
        if (kinds != null && kinds.size > 1) {
            var stillOver = false
            for (kind in kinds) {
                val perKind = window(shape, badFrom..badTo).copy(kinds = listOf(kind))
                try {
                    downloaded += peer.reconcile(url, perKind, local.ids(perKind), null) { onEvent(it) }.downloaded
                } catch (_: NegentropySyncException) {
                    stillOver = true
                    downloaded += peer.page(url, perKind, onEvent)
                    paged++
                }
            }
            System.err.println(
                "router: sweep ${url.url} [$badFrom, $badTo] over the peer's cap at ${badTo - badFrom + 1}s" +
                    " — split by kind${if (stillOver) ", $paged kind(s) still over and paged" else ""}",
            )
        } else {
            System.err.println(
                "router: sweep ${url.url} [$badFrom, $badTo] over the peer's cap and un-splittable" +
                    " (single kind, ${badTo - badFrom + 1}s) — paging it",
            )
            downloaded += peer.page(url, window(shape, badFrom..badTo), onEvent)
            paged++
        }

        // Rebuild the window around the drained slice, newest-last. The drained
        // slice itself is NOT re-pushed; it is done, by whichever route.
        if (badFrom > w.first) stack.addLast(w.first..(badFrom - 1))
        if (badTo < w.last) stack.addLast((badTo + 1)..w.last)
        // The cursor moves ONLY if the drained slice reaches the top of the
        // window, because only then is everything above it finished. A slice
        // taken out of the middle leaves a pending piece ABOVE it, and a cursor
        // that reached down past that piece would claim ground no one has
        // compared — the one way this design can lose events rather than repeat
        // them. Nothing is lost by staying quiet: the pieces around it move the
        // cursor as they finish, and their claim covers the drained slice
        // between them, which by then is true.
        if (badTo >= w.last) state.advance(url, shape, badFrom, w.last)
        return downloaded to paged
    }

    /** One window finished, by any route: move the cursor. */
    private fun complete(
        url: NormalizedRelayUrl,
        shape: Filter,
        w: LongRange,
    ) = state.advance(url, shape, w.first, w.last)

    private fun cursorLow(
        url: NormalizedRelayUrl,
        shape: Filter,
    ): Long? = state.reconciled(url, shape)?.downTo

    /**
     * Shrink toward what the peer will take: their own number when they sent
     * one, halving when they did not. The 0.8 leaves room for the set to have
     * grown between their refusal and our next ask.
     */
    private fun shrink(
        url: NormalizedRelayUrl,
        target: Int,
    ): Int {
        val cap = state.peer(url)?.cap
        val next =
            if (cap != null) {
                minOf(target, (cap * CAP_MARGIN).toInt())
            } else {
                target / 2
            }
        return next.coerceIn(tuning.minTarget, tuning.maxTarget).also { state.setTarget(url, it) }
    }

    /**
     * Grow after a clean single-window reconcile — gently, and never past what
     * the peer has already told us it will take. A sweep that starts too small
     * would otherwise pay its round trip per window forever.
     */
    private fun grow(
        url: NormalizedRelayUrl,
        target: Int,
    ): Int {
        val cap = state.peer(url)?.cap?.let { (it * CAP_MARGIN).toInt() } ?: tuning.maxTarget
        val next = (target * GROWTH).toInt().coerceAtMost(minOf(cap, tuning.maxTarget)).coerceAtLeast(tuning.minTarget)
        if (next != target) state.setTarget(url, next)
        return next
    }

    private fun window(
        shape: Filter,
        w: LongRange,
    ): Filter = shape.copy(since = w.first, until = w.last, limit = null)

    companion object {
        /**
         * Where bisection bottoms out. NIP-01 timestamps are seconds, so this
         * is not a tuning choice — it is the smallest window that exists.
         */
        const val MIN_WINDOW_SECONDS = 1L

        /** How much of a peer's stated cap we actually ask for. */
        private const val CAP_MARGIN = 0.8

        /** Multiplicative growth per clean window. */
        private const val GROWTH = 1.25

        /**
         * How many windows may fail in a row before the sweep stops. A relay
         * that has started refusing everything is not going to be talked round
         * by another 400 windows, and the cursor keeps what was already earned.
         */
        private const val MAX_CONSECUTIVE_FAILURES = 3
    }
}
