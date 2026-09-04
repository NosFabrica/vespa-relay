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

import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
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
import kotlinx.coroutines.CancellationException

/**
 * Sizes for the window stack, all counts of events; the pager turns them into
 * `created_at` boundaries.
 *
 * @param target where a peer never asked before starts.
 * @param minTarget the floor the learned target may shrink to; a peer refusing this refuses negentropy.
 * @param maxTarget the ceiling it may grow to.
 * @param slackSeconds how far below `now` a sweep stops; a window must be immutable to be checkpointed.
 */
internal data class NegPageTuning(
    val target: Int = 100_000,
    val minTarget: Int = 1_000,
    val maxTarget: Int = 1_000_000,
    val slackSeconds: Long = 60,
)

/** One window against one peer: reconcile it, or page it. The reconcile reads the index per window. */
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

    /**
     * The terminal fallback: walk this window over a plain REQ. Says whether
     * it was refused as its own fact; a refusal delivers zero events exactly
     * as an empty window does.
     */
    suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): PagedWindow
}

/** What one fallback walk delivered, and whether the relay turned any part of it away. */
internal class PagedWindow(
    val downloaded: Int,
    val refused: Boolean,
)

/** [NegentropyLocalIndex] over this relay's store: counts and id reads by range. */
internal class StoreWindowIndex(
    private val store: IEventStore,
) : NegentropyLocalIndex {
    // A failed count is not fatal (quartz lets the peer's refusal split), but cancellation is not a failed count.
    override suspend fun count(window: Filter): Int? =
        try {
            storeCall(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.OP_COUNT, StoreCalls.summarise(window)) {
                store.count(window)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            null
        }

    // Booked apart from the count: this materialises every id in the window, the largest allocation the router makes.
    override suspend fun entriesFor(window: Filter): List<IdAndTime> =
        storeCall(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.OP_SNAPSHOT_IDS, StoreCalls.summarise(window)) {
            store.snapshotIdsForNegentropy(listOf(window))
        }
}

/** The count the pager already took, handed to quartz's own sizing check instead of a second query. */
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
    /** Per-relay kind caps, the same instance the pool learns into. No default, so the sharing cannot regress silently. */
    private val widths: FilterWidths,
    private val idleTimeoutMs: Long = NEG_IDLE_MS,
    /** The twice-refused ids, declined before the REQ that would fetch them. */
    private val refused: RefusedIds = RefusedIds.disabled(),
) : WindowSync {
    /**
     * The predicate quartz consults for every id the reconcile names. Null
     * when suppression is off, since quartz skips the per-batch copy for an
     * absent predicate. Keyed on the window because only the id is known yet.
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

    /**
     * Pages a window in chunks of the width this relay takes. A page here is a
     * sub-window of a leg, so its drain settles nothing; that judgement is
     * [drainSettlesThePast] at the leg's call site. The NEG-OPEN itself is not
     * chunked: a width-refused one throws `UNAVAILABLE` and lands here.
     */
    override suspend fun page(
        url: NormalizedRelayUrl,
        window: Filter,
        onEvent: suspend (Event) -> Unit,
    ): PagedWindow {
        var downloaded = 0
        for (chunk in widths.chunk(url, window)) {
            val walked = client.fetchAllPages(url, listOf(chunk), idleTimeoutMs, onEvent = onEvent)
            downloaded += walked.downloaded
            // The first refusal ends it; what was delivered is kept, so the refusal cannot ride on the count.
            if (VisitPool.refusedOutright(walked)) return PagedWindow(downloaded, refused = true)
        }
        return PagedWindow(downloaded, refused = false)
    }
}

/** What one [NegentropyPager.sweep] did with one leg of one peer. */
internal class SweepOutcome(
    val downloaded: Int,
    val reconciledWindows: Int,
    val pagedWindows: Int,
    /** The leg is fully covered; the caller may record a band for it. */
    val complete: Boolean,
    /** False when the peer could not reconcile the first window at all; the caller should page [outstanding]. */
    val negentropyUsable: Boolean,
    /** The part of the leg this sweep has not covered, when [negentropyUsable] is false. */
    val outstanding: Filter?,
    val failure: NegentropySyncException?,
    /** Windows the relay refused the last-resort REQ for, and so were not claimed. */
    val refusedWindows: Int = 0,
)

/**
 * Negentropy paging that sizes itself: one stack of `created_at` windows,
 * split from both ends, checkpointed per window.
 *
 * A NEG-OPEN is all-or-nothing at both ends: our id snapshot must fit in
 * memory, and a relay refuses outright past its `max_sync_events`. So the
 * boundary is a timestamp decided by a count, from two sources: our own
 * [NegentropyLocalIndex.count] before the round trip, and the peer's cap
 * from its refusal ([NegentropySyncResult.peerCap]) or, failing that, a
 * window quartz had to split. quartz owns everything inside one call; this
 * layer owns what survives a call: the cursor, the per-peer size, the order.
 *
 * Windows pop strictly newest-first, so the finished region is one contiguous
 * slice growing down from the ceiling and the cursor can be one timestamp.
 * Every push here keeps that invariant.
 */
internal class NegentropyPager(
    private val local: NegentropyLocalIndex,
    private val peer: WindowSync,
    private val state: SweepState,
    private val tuning: NegPageTuning,
    /** What each relay said when it refused. Deaf for probes and tests. */
    private val complaints: RelayComplaints = RelayComplaints.DEAF,
) {
    /** How far below `now` a sweep stops. */
    internal val slackSeconds: Long get() = tuning.slackSeconds

    /**
     * Reconcile [leg] against [url], one right-sized window at a time.
     * [stream] and [shape] are the cursor's identity. Only `since`/`until`
     * vary across windows: strfry matches a declared negentropy tree by
     * canonical filter JSON, so any other split drops onto its capped path.
     */
    suspend fun sweep(
        stream: String,
        url: NormalizedRelayUrl,
        shape: Filter,
        leg: Filter,
        onProgress: ((Int, Int) -> Unit)? = null,
        /** (since, until) of each window actually reconciled; `since` is the depth reached and only descends. */
        onWindow: ((Long, Long) -> Unit)? = null,
        onEvent: suspend (Event) -> Unit,
    ): SweepOutcome {
        val floor = leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR
        // Nothing within `slackSeconds` of now is swept; the live tail owns the head.
        val head = nowSeconds() - tuning.slackSeconds
        val ceiling = minOf(leg.until ?: head, head)
        if (ceiling < floor) {
            // The whole leg is inside the live head: not an error and not complete.
            return SweepOutcome(0, 0, 0, complete = false, negentropyUsable = true, outstanding = null, failure = null, refusedWindows = 0)
        }

        var target = state.target(url, tuning.target).coerceIn(tuning.minTarget, tuning.maxTarget)
        val startedTarget = target
        // Once per sweep: the key serialises the filter.
        val cursor = SweepState.keyFor(stream, url, shape)
        val stack = ArrayDeque<LongRange>()
        pushResumed(stack, url, cursor, floor, ceiling)

        var downloaded = 0
        var reconciled = 0
        var paged = 0
        var refusedWindows = 0
        var consecutiveFailures = 0
        var lastFailure: NegentropySyncException? = null

        while (stack.isNotEmpty()) {
            // Newest first, the invariant the cursor rests on.
            val sweepWindow = stack.removeLast()
            val minimal = sweepWindow.last - sweepWindow.first <= MIN_WINDOW_SECONDS

            // Our side first: this sizes the checkpoint, and quartz re-uses the number to bound its reads.
            val ours = if (minimal) null else local.count(windowFilter(shape, sweepWindow))
            if (ours != null && ours > target) {
                bisect(stack, sweepWindow)
                continue
            }
            // After the cut, so the reported depth only descends.
            onWindow?.invoke(sweepWindow.first, sweepWindow.last)

            var pagedHere = 0
            // The cursor may only advance over ground that was read; a refused REQ read nothing.
            var refusedHere = false
            // The floor for reading what the relay says about this window. See [sayRefused].
            val askedAtMs = System.currentTimeMillis()
            try {
                // quartz counts from zero per NEG-OPEN; offset so the progress line never walks backwards.
                val base = downloaded
                val progress = onProgress?.let { report -> { need: Int, got: Int -> report(need, base + got) } }
                val result =
                    peer.reconcile(
                        url = url,
                        window = windowFilter(shape, sweepWindow),
                        local = PrimedIndex(local, windowFilter(shape, sweepWindow), ours),
                        targetWindow = target,
                        onProgress = progress,
                        // A second the peer will not reconcile at any size is drained inside the call.
                        onUnreconcilable = { dense ->
                            val drained = drainDense(url, shape, dense, target, onEvent)
                            downloaded += drained.downloaded
                            if (drained.refused) refusedHere = true
                            pagedHere++
                        },
                        onEvent = onEvent,
                    )
                downloaded += result.downloaded
                reconciled++
                paged += pagedHere
                consecutiveFailures = 0
                // Their side: a stated cap fits in one step, a split shrinks, anything else grows.
                target =
                    when {
                        result.peerCap != null -> fitToCap(url, result.peerCap!!)
                        result.windows > 1 -> shrink(url, target)
                        else -> grow(url, target)
                    }
                if (refusedHere) {
                    refusedWindows++
                    sayRefused(url, sweepWindow, "a dense slice was refused", askedAtMs)
                } else {
                    complete(cursor, sweepWindow)
                }
            } catch (e: NegentropySyncException) {
                lastFailure = e
                when (e.reason) {
                    NegentropySyncException.Reason.OVER_MAX_SYNC_EVENTS -> {
                        // Defensive: quartz normally hands an unreconcilable window to the hook above.
                        e.cap?.let { fitToCap(url, it) } ?: shrink(url, target)
                        target = state.target(url, tuning.target)
                        val badFrom = (e.window.since ?: sweepWindow.first).coerceIn(sweepWindow.first, sweepWindow.last)
                        val badTo = (e.window.until ?: sweepWindow.last).coerceIn(badFrom, sweepWindow.last)
                        val drained = drainDense(url, shape, windowFilter(shape, badFrom..badTo), target, onEvent)
                        downloaded += drained.downloaded
                        paged++
                        if (badFrom > sweepWindow.first) stack.addLast(sweepWindow.first..(badFrom - 1))
                        if (badTo < sweepWindow.last) stack.addLast((badTo + 1)..sweepWindow.last)
                        // Claimable only when the drained slice reaches the top of the
                        // window (a pending piece above it was never compared) and
                        // only when the drain was served.
                        if (drained.refused) {
                            refusedWindows++
                            sayRefused(url, badFrom..sweepWindow.last, "the dense slice was refused", askedAtMs)
                        } else if (badTo >= sweepWindow.last) {
                            state.advance(cursor, badFrom, sweepWindow.last)
                        }
                        consecutiveFailures = 0
                    }

                    NegentropySyncException.Reason.UNAVAILABLE -> {
                        if (reconciled == 0 && paged == 0) {
                            // The first window never landed: this peer does not do negentropy for us. Let the caller page.
                            return SweepOutcome(
                                downloaded,
                                0,
                                0,
                                complete = false,
                                negentropyUsable = false,
                                outstanding = outstanding(shape, floor, ceiling, cursor),
                                failure = e,
                                refusedWindows = refusedWindows,
                            )
                        }
                        // Mid-sweep: page this window so the sweep keeps its contiguity.
                        System.err.println(
                            "router: sweep ${url.url} window [${sweepWindow.first}, ${sweepWindow.last}] could not reconcile (${e.detail}) — paging this window",
                        )
                        val walked = peer.page(url, windowFilter(shape, sweepWindow), onEvent)
                        downloaded += walked.downloaded
                        paged++
                        if (walked.refused) {
                            refusedWindows++
                            sayRefused(url, sweepWindow, "the fallback page was refused too", askedAtMs)
                        } else {
                            complete(cursor, sweepWindow)
                        }
                        if (++consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                            System.err.println(
                                "router: sweep ${url.url} gave up after $consecutiveFailures window(s) in a row failed" +
                                    " — ${fmtCount(downloaded)} event(s) kept, cursor holds at ${state.reconciled(cursor)?.downTo ?: sweepWindow.first}",
                            )
                            return SweepOutcome(downloaded, reconciled, paged, complete = false, negentropyUsable = true, outstanding = null, failure = e, refusedWindows = refusedWindows)
                        }
                    }
                }
            }
        }

        // The band the caller records is the durable statement; the working cursor goes.
        state.finish(cursor)
        if (target != startedTarget) {
            System.err.println(
                "router: sweep ${url.url} window size ${fmtCount(startedTarget)} → ${fmtCount(target)} event(s)" +
                    (state.peer(url)?.cap?.let { " (their cap ${fmtCount(it)})" } ?: ""),
            )
        }
        // An empty stack means every window was visited, not that every window was answered.
        return SweepOutcome(
            downloaded,
            reconciled,
            paged,
            complete = refusedWindows == 0,
            negentropyUsable = true,
            outstanding = null,
            failure = lastFailure,
            refusedWindows = refusedWindows,
        )
    }

    /** The peer stated its `max_sync_events`: fit the target to it now. Never raises the target. */
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

    /** The slice of the leg below what the cursor claims. */
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

    /** The stack's starting state: the leg minus what a previous run reconciled. Pushed low-then-high. */
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

    /** What one fallback drain delivered, and whether the relay turned any of it away. */
    private class Paged(
        val downloaded: Int,
        val refused: Boolean,
    )

    /**
     * A slice the peer will not reconcile at any window size: one second can
     * hold more than a relay's cap. Split by kind where the filter allows (a
     * shape change strfry answers off its capped path), else page it over REQ.
     * Called mid-reconcile, so the rest of the window carries on.
     */
    private suspend fun drainDense(
        url: NormalizedRelayUrl,
        shape: Filter,
        dense: Filter,
        target: Int,
        onEvent: suspend (Event) -> Unit,
    ): Paged {
        var downloaded = 0
        var refused = false
        val kinds = shape.kinds
        val span = (dense.until ?: 0L) - (dense.since ?: 0L) + 1
        if (kinds != null && kinds.size > 1) {
            var stillOver = 0
            for (kind in kinds) {
                val perKind = dense.copy(kinds = listOf(kind), limit = null)
                // Over the target or uncountable, paging is the only bounded move left.
                val mine = local.count(perKind)
                if (mine == null || mine > target) {
                    stillOver++
                    val walked = peer.page(url, perKind, onEvent)
                    downloaded += walked.downloaded
                    if (walked.refused) refused = true
                    continue
                }
                try {
                    // A slice the peer still refuses inside the per-kind reconcile lands on this hook, not in the catch.
                    var pagedSlices = 0
                    downloaded +=
                        peer
                            .reconcile(url, perKind, PrimedIndex(local, perKind, mine), target, null, { slice ->
                                pagedSlices++
                                val walked = peer.page(url, slice, onEvent)
                                downloaded += walked.downloaded
                                if (walked.refused) refused = true
                            }, onEvent)
                            .downloaded
                    if (pagedSlices > 0) stillOver++
                } catch (_: NegentropySyncException) {
                    stillOver++
                    val walked = peer.page(url, perKind, onEvent)
                    downloaded += walked.downloaded
                    if (walked.refused) refused = true
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
            val walked = peer.page(url, dense, onEvent)
            downloaded += walked.downloaded
            if (walked.refused) refused = true
        }
        return Paged(downloaded, refused)
    }

    /** A window the sweep could not read, said once with the relay's own words. */
    private suspend fun sayRefused(
        url: NormalizedRelayUrl,
        sweepWindow: LongRange,
        why: String,
        /** When this window's own asks went out, so an older refusal is not printed as its cause. */
        askedAtMs: Long,
    ) {
        System.err.println(
            "router: sweep ${url.url} window [${sweepWindow.first}, ${sweepWindow.last}] NOT claimed — $why" +
                (complaints.awaitSince(url, askedAtMs)?.let { "; the relay said: $it" } ?: "") +
                " — the cursor holds and the next audit re-reads it",
        )
    }

    /** One window finished, by any route: move the cursor. */
    private fun complete(
        cursor: SweepState.Cursor,
        sweepWindow: LongRange,
    ) = state.advance(cursor, sweepWindow.first, sweepWindow.last)

    /** Shrink toward what the peer will take: their number when they sent one, halving when they did not. */
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

    /** Grow after a clean single-window reconcile, never past a cap the peer has stated. */
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
        /** Where bisection bottoms out: NIP-01 timestamps are seconds. */
        const val MIN_WINDOW_SECONDS = 1L

        /** How much of a peer's stated cap we ask for, leaving room for the set to grow. */
        private const val CAP_MARGIN = 0.8

        private const val GROWTH = 1.25

        /** Windows that may fail in a row before the sweep stops; the cursor keeps what was earned. */
        private const val MAX_CONSECUTIVE_FAILURES = 3
    }
}
