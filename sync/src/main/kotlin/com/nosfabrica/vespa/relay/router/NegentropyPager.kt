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

import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.fmtCount
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
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

/**
 * One window against one peer: reconcile it, or (the escape hatch) page it.
 *
 * The reconcile is handed the INDEX rather than a materialised id list — quartz
 * reads what it needs per window now, so nothing here has to hold a snapshot at
 * all — plus the size that bounds those reads.
 */
internal interface WindowSync {
    /** @throws NegentropySyncException when NIP-77 cannot enumerate this window. */
    suspend fun reconcile(
        url: NormalizedRelayUrl,
        window: Filter,
        local: NegentropyLocalIndex,
        targetWindow: Int,
        onProgress: ((Int, Int) -> Unit)?,
        onUnreconcilable: suspend (Filter) -> Unit,
        onEvent: suspend (Event) -> Unit,
    ): NegentropySyncResult

    suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Int
}

/** [NegentropyLocalIndex] over this relay's store: counts and id reads by range. */
internal class StoreWindowIndex(
    private val store: IEventStore,
) : NegentropyLocalIndex {
    // A count that fails is not fatal — quartz falls back to letting the peer's
    // refusal do the splitting — so it must not take the sweep down with it.
    override suspend fun count(window: Filter): Int? = runCatching { store.count(window) }.getOrNull()

    override suspend fun entriesFor(window: Filter): List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(window))
}

/**
 * The count the pager already took, handed to quartz instead of a second query.
 *
 * quartz asks for the count of the window it is given before deciding whether to
 * sub-split it — the same window this class just counted to decide the
 * checkpoint size. Priming it saves exactly one store round trip per window,
 * which at tens of thousands of windows is worth the fifteen lines.
 */
internal class PrimedIndex(
    private val inner: NegentropyLocalIndex,
    private val window: Filter,
    private val known: Int?,
) : NegentropyLocalIndex {
    override suspend fun count(window: Filter): Int? =
        if (known != null && window.since == this.window.since && window.until == this.window.until) {
            known
        } else {
            inner.count(window)
        }

    override suspend fun entriesFor(window: Filter): List<IdAndTime> = inner.entriesFor(window)
}

internal class ClientWindowSync(
    private val client: NostrClient,
    private val idleTimeoutMs: Long = NEG_IDLE_MS,
    /**
     * The twice-refused ids, so the reconcile can decline them before the REQ
     * that would fetch them. Disabled by default, which passes quartz a null
     * predicate rather than one that always says yes — see [wantIdFor].
     */
    private val refused: RefusedIds = RefusedIds.disabled(),
) : WindowSync {
    /**
     * The predicate quartz consults for every id the reconcile names, or null.
     *
     * **Null when suppression is off, never a lambda returning true.** quartz's
     * `NeedGate` hands the batch straight back when the predicate is absent —
     * same list instance, no copy per batch — and a trivially-true lambda would
     * forfeit that on every sync for nothing.
     *
     * Keyed on the WINDOW rather than the event's own `created_at`, because at
     * this point an id is all we have: the reconcile names ids and the bodies
     * have not arrived. [RefusedIds.suppressedInWindow] therefore consults
     * every epoch the window overlaps.
     *
     * Called from the reconciler coroutines, possibly several at once, so it
     * has to be cheap and thread-safe: it is a lock-free cuckoo probe over the
     * handful of epochs the window touches.
     */
    private fun wantIdFor(window: Filter): ((String) -> Boolean)? =
        if (!refused.enabled) {
            null
        } else {
            { id -> !refused.suppressedInWindow(id, window.since, window.until) }
        }

    override suspend fun reconcile(
        url: NormalizedRelayUrl,
        window: Filter,
        local: NegentropyLocalIndex,
        targetWindow: Int,
        onProgress: ((Int, Int) -> Unit)?,
        onUnreconcilable: suspend (Filter) -> Unit,
        onEvent: suspend (Event) -> Unit,
    ): NegentropySyncResult =
        client.negentropySync(
            relay = url,
            filter = window,
            idleTimeoutMs = idleTimeoutMs,
            localIndex = local,
            targetWindow = targetWindow,
            onUnreconcilableWindow = onUnreconcilable,
            wantId = wantIdFor(window),
            onProgress = onProgress,
            onEvent = onEvent,
        )

    // The count only. A page here is a SUB-WINDOW of a leg — one slice the
    // reconcile could not compare, or one dense second split by kind — so
    // draining it means "nothing below this slice's own floor", which is a
    // point inside the leg rather than the leg's bottom. Only a walk that
    // reached the filter's floor may settle history, and that judgement lives
    // at the leg's call site in `drainSettlesThePast`.
    override suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Int = client.fetchAllPages(url, listOf(window), idleTimeoutMs, onEvent = onEvent).downloaded
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
     * leg, so the caller should page it — the same fallback
     * `negentropySyncOrFetch` makes, kept in the caller's hands.
     */
    val negentropyUsable: Boolean,
    /**
     * What the caller should page when [negentropyUsable] is false: the part of
     * the leg this sweep has NOT already covered.
     *
     * Not the whole leg, because a sweep that resumed from a cursor may be most
     * of the way down it already — one transient failure on the window after a
     * restart would otherwise re-download everything the cursor was keeping.
     */
    val outstanding: Filter?,
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
 *  1. **We are dense.** [NegentropyLocalIndex.count] over the window, before a
 *    round trip is spent. That number sizes the CHECKPOINT — how much a crash
 *    costs — and is handed to quartz as its own read bound for the same window.
 *  2. **They are dense.** The peer states its cap in a refusal, which quartz
 *    parses and reports as [NegentropySyncResult.peerCap]; failing that, a
 *    window it had to split ([NegentropySyncResult.windows] > 1) says the same
 *    thing more vaguely. A stated cap fits the target in one step; a split
 *    halves it.
 *
 * Neither source knows anything about the other, and the same stack absorbs
 * both. That is what makes this automatic rather than tuned.
 *
 * **The division of labour with quartz.** quartz owns everything INSIDE one
 * call: splitting a window it cannot reconcile, bounding what it reads from the
 * index, draining a second no window size will fit (through the hook this class
 * passes it), and reporting the peer's cap. What it cannot do is remember
 * anything between calls. So this layer owns exactly what survives a call: the
 * checkpoint cursor, the per-peer size learned across syncs, and the order the
 * windows are walked in.
 *
 * **Ordering is not incidental.** Windows are popped strictly newest-first, so
 * the finished region is always a single contiguous slice growing downward from
 * the leg's ceiling — which is the only reason a cursor can be one timestamp
 * instead of a set of intervals. Every push below keeps that invariant.
 */
internal class NegentropyPager(
    private val local: NegentropyLocalIndex,
    private val peer: WindowSync,
    private val state: SweepState,
    private val tuning: NegPageTuning,
) {
    /** How far below `now` a sweep stops — the head its claims must not over-run. */
    internal val slackSeconds: Long get() = tuning.slackSeconds

    /**
     * Reconcile [leg] against [url], one right-sized window at a time.
     *
     * [stream] and [shape] are the cursor's identity: the stream asking, and
     * the filter it asks. [shape] is also the shape every window keeps — only
     * `since`/`until` vary across windows, deliberately:
     * strfry matches a declared negentropy tree by comparing canonicalised
     * filter JSON, so a shape that stays byte-identical rides their index across
     * the whole sweep, while sub-partitioning on any other axis drops the
     * window onto the capped snapshot path. Which is why the kind split below
     * is an escape hatch and not a strategy.
     */
    suspend fun sweep(
        stream: String,
        url: NormalizedRelayUrl,
        shape: Filter,
        leg: Filter,
        onProgress: ((Int, Int) -> Unit)? = null,
        /**
         * WHERE IN TIME the sweep is: called with (since, until) of each window
         * the sweep actually RECONCILES, after any bisection has cut it to a
         * size both sides will take. Windows are taken newest-first, so `since`
         * — the older edge — is the depth reached, and it only descends; that
         * is the number a reader wants beside `auditing history (negentropy)`,
         * and it is the same reading as a paging leg's cursor rather than the
         * opposite end of the range. The progress callback above counts events,
         * and a sweep that finds nothing missing delivers none — this is the
         * only signal that moves on a clean audit.
         */
        onWindow: ((Long, Long) -> Unit)? = null,
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
            return SweepOutcome(0, 0, 0, complete = false, negentropyUsable = true, outstanding = null, failure = null)
        }

        var target = state.target(url, tuning.target).coerceIn(tuning.minTarget, tuning.maxTarget)
        val startedTarget = target
        // Once per sweep, not per window: building it serialises the filter, and
        // a discovery stream's filter carries thousands of authors.
        val cursor = SweepState.keyFor(stream, url, shape)
        val stack = ArrayDeque<LongRange>()
        pushResumed(stack, url, cursor, floor, ceiling)

        var downloaded = 0
        var reconciled = 0
        var paged = 0
        var consecutiveFailures = 0
        var lastFailure: NegentropySyncException? = null

        while (stack.isNotEmpty()) {
            // Newest first — the invariant the cursor rests on.
            val sweepWindow = stack.removeLast()
            val minimal = sweepWindow.last - sweepWindow.first <= MIN_WINDOW_SECONDS

            // (1) Our side, before the round trip: this is what sizes the
            // CHECKPOINT — how much a crash costs — and quartz re-uses the same
            // number to bound its own reads inside the window.
            val ours = if (minimal) null else local.count(windowFilter(shape, sweepWindow))
            if (ours != null && ours > target) {
                bisect(stack, sweepWindow)
                continue
            }
            // AFTER the cut, not before it: the first window off the stack is
            // the WHOLE leg — a decade wide on a deep history — and announcing
            // it would report a floor the sweep has not reached, then walk the
            // cursor UPWARDS as each bisection narrowed it. Announced here the
            // reported windows descend, because only windows that are actually
            // reconciled are announced and those are popped newest-first.
            onWindow?.invoke(sweepWindow.first, sweepWindow.last)

            var pagedHere = 0
            try {
                // Offset by what the sweep already has: quartz counts from zero
                // per NEG-OPEN, so passing its numbers straight through would
                // walk the progress line backwards at every window boundary.
                val base = downloaded
                val progress = onProgress?.let { report -> { need: Int, got: Int -> report(need, base + got) } }
                val result =
                    peer.reconcile(
                        url = url,
                        window = windowFilter(shape, sweepWindow),
                        // Primed so quartz's own sizing check does not re-ask the
                        // store for the count we just took.
                        local = PrimedIndex(local, windowFilter(shape, sweepWindow), ours),
                        targetWindow = target,
                        onProgress = progress,
                        // A second the peer will not reconcile at any size is
                        // drained HERE, inside the call, so everything around it
                        // in this window still reconciles — it never becomes an
                        // exception and the window never has to be re-tried.
                        onUnreconcilable = { dense ->
                            downloaded += drainDense(url, shape, dense, target, onEvent)
                            pagedHere++
                        },
                        onEvent = onEvent,
                    )
                downloaded += result.downloaded
                reconciled++
                paged += pagedHere
                consecutiveFailures = 0
                // (2) Their side. A stated cap is the peer telling us its size
                // outright; a split with no cap means something made quartz cut
                // the window up, so be cautious. Anything else earns growth.
                target =
                    when {
                        result.peerCap != null -> fitToCap(url, result.peerCap!!)
                        result.windows > 1 -> shrink(url, target)
                        else -> grow(url, target)
                    }
                complete(cursor, sweepWindow)
            } catch (e: NegentropySyncException) {
                lastFailure = e
                when (e.reason) {
                    NegentropySyncException.Reason.OVER_MAX_SYNC_EVENTS -> {
                        // Defensive: quartz hands an un-reconcilable window to
                        // the hook above rather than throwing, so reaching here
                        // means it could not even do that. Drain the slice it
                        // named and keep the rest of the window on the stack.
                        e.cap?.let { fitToCap(url, it) } ?: shrink(url, target)
                        target = state.target(url, tuning.target)
                        val badFrom = (e.window.since ?: sweepWindow.first).coerceIn(sweepWindow.first, sweepWindow.last)
                        val badTo = (e.window.until ?: sweepWindow.last).coerceIn(badFrom, sweepWindow.last)
                        downloaded += drainDense(url, shape, windowFilter(shape, badFrom..badTo), target, onEvent)
                        paged++
                        if (badFrom > sweepWindow.first) stack.addLast(sweepWindow.first..(badFrom - 1))
                        if (badTo < sweepWindow.last) stack.addLast((badTo + 1)..sweepWindow.last)
                        // Only claimable when the drained slice reaches the top
                        // of the window: a slice out of the middle leaves a
                        // pending piece ABOVE it, and a cursor that reached down
                        // past that piece would claim ground no one compared.
                        if (badTo >= sweepWindow.last) state.advance(cursor, badFrom, sweepWindow.last)
                        consecutiveFailures = 0
                    }

                    NegentropySyncException.Reason.UNAVAILABLE -> {
                        if (reconciled == 0 && paged == 0) {
                            // The first window never landed: this peer does not
                            // do negentropy for us at all. Say so and let the
                            // caller page the leg — a sweep of windows that each
                            // fail the same way would be the same paging with
                            // extra round trips.
                            return SweepOutcome(
                                downloaded,
                                0,
                                0,
                                complete = false,
                                negentropyUsable = false,
                                outstanding = outstanding(shape, floor, ceiling, cursor),
                                failure = e,
                            )
                        }
                        // Mid-sweep: one window's worth of trouble. Page it so
                        // the sweep keeps its contiguity and moves on.
                        System.err.println(
                            "router: sweep ${url.url} window [${sweepWindow.first}, ${sweepWindow.last}] could not reconcile (${e.detail}) — paging this window",
                        )
                        downloaded += peer.page(url, windowFilter(shape, sweepWindow), onEvent)
                        paged++
                        complete(cursor, sweepWindow)
                        if (++consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                            System.err.println(
                                "router: sweep ${url.url} gave up after $consecutiveFailures window(s) in a row failed" +
                                    " — ${fmtCount(downloaded)} event(s) kept, cursor holds at ${state.reconciled(cursor)?.downTo ?: sweepWindow.first}",
                            )
                            return SweepOutcome(downloaded, reconciled, paged, complete = false, negentropyUsable = true, outstanding = null, failure = e)
                        }
                    }
                }
            }
        }

        // The leg is done; the band the caller records now is the durable
        // statement, so the working cursor goes.
        state.finish(cursor)
        if (target != startedTarget) {
            System.err.println(
                "router: sweep ${url.url} window size ${fmtCount(startedTarget)} → ${fmtCount(target)} event(s)" +
                    (state.peer(url)?.cap?.let { " (their cap ${fmtCount(it)})" } ?: ""),
            )
        }
        return SweepOutcome(downloaded, reconciled, paged, complete = true, negentropyUsable = true, outstanding = null, failure = lastFailure)
    }

    /**
     * The peer stated its `max_sync_events` — in a refusal quartz parsed for us
     * and reported through [NegentropySyncResult.peerCap]. Recorded against the
     * peer, and the window size drops to fit it immediately: the whole point of
     * having the number is not having to find it by halving. Never RAISES the
     * target — a cap is a ceiling the peer enforces, not evidence that a larger
     * window would have worked.
     */
    private fun fitToCap(
        url: NormalizedRelayUrl,
        cap: Long,
    ): Int {
        val fitted =
            (cap * CAP_MARGIN)
                .coerceIn(tuning.minTarget.toDouble(), tuning.maxTarget.toDouble())
                .toInt()
        val next = minOf(state.target(url, tuning.target), fitted)
        state.learnCap(url, cap.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(), next)
        return next
    }

    /**
     * The slice of the leg nothing has covered yet — everything below what the
     * cursor claims, since the sweep walks newest-first and the claim is
     * contiguous from the ceiling down.
     */
    private fun outstanding(
        shape: Filter,
        floor: Long,
        ceiling: Long,
        cursor: SweepState.Cursor,
    ): Filter {
        val done = state.reconciled(cursor)
        val top = if (done != null && done.downTo > floor) done.downTo - 1 else ceiling
        return shape.copy(since = floor, until = minOf(top, ceiling), limit = null)
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
        cursor: SweepState.Cursor,
        floor: Long,
        ceiling: Long,
    ) {
        val done = state.reconciled(cursor)
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
        sweepWindow: LongRange,
    ) {
        val mid = sweepWindow.first + (sweepWindow.last - sweepWindow.first) / 2
        stack.addLast(sweepWindow.first..mid)
        stack.addLast((mid + 1)..sweepWindow.last)
    }

    /**
     * A slice the peer will not reconcile at any window size.
     *
     * `created_at` has second granularity and is author-controlled, so a single
     * second holding more than a relay's cap is reachable and cannot be cut on
     * the time axis. Two ways out, in order of how much they cost:
     *
     *  1. **Split by kind.** Only available to a multi-kind filter, and it
     *     changes the filter shape, which on strfry means the slice stops
     *     matching their declared tree and falls onto the capped snapshot path.
     *     A deliberate downgrade, taken only here.
     *  2. **Page it over REQ.** Always available. `fetchAllPages` steps past a
     *     second denser than one page rather than looping on it, so this
     *     terminates — at the cost of that second's unreachable tail.
     *
     * Everything AROUND the slice is quartz's problem now, not this method's:
     * the hook is called mid-reconcile, so the rest of the window carries on
     * being reconciled once this returns.
     */
    private suspend fun drainDense(
        url: NormalizedRelayUrl,
        shape: Filter,
        dense: Filter,
        target: Int,
        onEvent: suspend (Event) -> Unit,
    ): Int {
        var downloaded = 0
        val kinds = shape.kinds
        val span = (dense.until ?: 0L) - (dense.since ?: 0L) + 1
        if (kinds != null && kinds.size > 1) {
            var stillOver = 0
            for (kind in kinds) {
                val perKind = dense.copy(kinds = listOf(kind), limit = null)
                // Counted, not read: this is the one slice we already know is
                // dense at the PEER, and there is no reason to assume our side
                // of it is small either. A minimal window is the end of the
                // line for splitting, so over the target — or uncountable —
                // paging is the only move left that stays bounded.
                val mine = local.count(perKind)
                if (mine == null || mine > target) {
                    stillOver++
                    downloaded += peer.page(url, perKind, onEvent)
                    continue
                }
                try {
                    downloaded +=
                        peer
                            .reconcile(url, perKind, PrimedIndex(local, perKind, mine), target, null, { }, onEvent)
                            .downloaded
                } catch (_: NegentropySyncException) {
                    stillOver++
                    downloaded += peer.page(url, perKind, onEvent)
                }
            }
            System.err.println(
                "router: sweep ${url.url} [${dense.since}, ${dense.until}] over the peer's cap at ${span}s" +
                    " — split by kind${if (stillOver > 0) ", $stillOver kind(s) still over and paged" else ""}",
            )
        } else {
            System.err.println(
                "router: sweep ${url.url} [${dense.since}, ${dense.until}] over the peer's cap and un-splittable" +
                    " (single kind, ${span}s) — paging it",
            )
            downloaded += peer.page(url, dense, onEvent)
        }
        return downloaded
    }

    /** One window finished, by any route: move the cursor. */
    private fun complete(
        cursor: SweepState.Cursor,
        sweepWindow: LongRange,
    ) = state.advance(cursor, sweepWindow.first, sweepWindow.last)

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

    private fun windowFilter(
        shape: Filter,
        sweepWindow: LongRange,
    ): Filter = shape.copy(since = sweepWindow.first, until = sweepWindow.last, limit = null)

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
