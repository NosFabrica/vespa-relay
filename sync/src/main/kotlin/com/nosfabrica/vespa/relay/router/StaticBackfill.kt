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

import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.SyncMode
import com.nosfabrica.vespa.relay.router.config.SyncUpstream
import com.nosfabrica.vespa.relay.router.heal.Healer
import com.nosfabrica.vespa.relay.router.progress.CycleTally
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.fmtCount
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.count
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The one-shot historical catch-up for statically configured upstreams.
 *
 * Per stream, relays are partitioned by [worthReconciling]: reconcilers share
 * ONE local id snapshot (per-relay snapshots walked the identical range once
 * per url), pagers walk their windows with no snapshot at all and start
 * immediately — they never wait for an id walk they will not read. Cursor
 * bands ([SyncBands]) keep every walk from repeating what a previous run
 * already covered.
 */
internal class StaticBackfill(
    private val client: NostrClient,
    private val store: IEventStore,
    private val config: RouterConfig,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val phases: StreamPhases,
    private val paging: PagingProgress,
    // Windowed reconciliation for streams too big to snapshot in one piece.
    private val pager: NegentropyPager,
    // One stream reconciles at a time, across static and dynamic both: a
    // stream holds its whole id set from snapshot start to its last relay, so
    // concurrent streams would hold their sets simultaneously (measured: three
    // at once heading for ~9 GiB). Serialising costs little wall clock — they
    // were contending for the same engine anyway.
    private val streamGate: Semaphore,
    private val transferring: AtomicInteger,
    private val scope: CoroutineScope,
    // Drained after each upstream's history catch-up, off the sweep's path.
    private val healer: Healer,
    // Declines twice-refused ids before the download REQ — see [wantIdFor].
    private val refusedIds: RefusedIds,
) {
    private val progress = BackfillProgress()

    /**
     * The disposition of each stream's configured upstreams, keyed by stream
     * name so `pageOne` and `reconcileOne` can settle a url without threading
     * the tally through five signatures.
     *
     * A static stream folds nothing and discovers nothing — its urls are in
     * `router.conf` — so `discovered` is the configured count and
     * `foldedOntoAnother` is 0. The partition is otherwise the dynamic one, and
     * that is the point: a reader must not need to know which kind of stream it
     * is looking at to read the numbers.
     */
    private val tallies = ConcurrentHashMap<String, CycleTally>()

    fun begin(totalUpstreams: Int) = progress.begin(totalUpstreams)

    /**
     * Record what became of one configured upstream, exactly once.
     *
     * Called beside every `progress.done` — the two answer different questions
     * (how far through the boot are we, and what happened to this relay) and
     * the one place they agree on is that a upstream reaches each of them once.
     * `nothingNew` covers both "answered, we were in sync" and "its band already
     * covers the filter, so nothing was asked": neither downloaded anything and
     * neither is a failure, and splitting them would need a fourth outcome the
     * dynamic side has no equivalent for.
     */
    private fun settle(
        upstream: SyncUpstream,
        events: Int,
        failed: Boolean = false,
    ) {
        val tally = tallies[upstream.streamName] ?: return
        when {
            failed -> {
                tally.transferFailed.incrementAndGet()
            }

            events > 0 -> {
                tally.delivered.incrementAndGet()
                tally.received.addAndGet(events.toLong())
            }

            else -> {
                tally.nothingNew.incrementAndGet()
            }
        }
    }

    /**
     * Backfill every down upstream, grouped by stream — one group is one
     * stream's relay list, and a stream carries exactly one filter.
     *
     * Grouped by the FILTER until bands became per stream, which read the same
     * for every config anyone had and is now wrong for the one that motivated
     * the change: two streams sharing a filter landed in one group, whose
     * snapshot was narrowed from `group.first()`'s bands and then recorded into
     * each upstream's OWN stream. The second stream's relays would be compared
     * against a window sized for the first stream's progress — every event
     * outside it re-downloaded, every cycle, while both bands claimed to be
     * fine. The cost of splitting them is one id snapshot each instead of one
     * shared, serialised behind the same gate.
     */
    suspend fun run(upstreams: List<SyncUpstream>) {
        coroutineScope {
            upstreams
                .withIndex()
                .groupBy { it.value.streamName }
                .forEach { (_, group) ->
                    launch { backfillStream(group.first().value.filter, group) }
                }
        }
    }

    private suspend fun backfillStream(
        filter: Filter,
        group: List<IndexedValue<SyncUpstream>>,
    ) {
        val name = group.first().value.streamName
        // A backfill runs once per process, so this clears nothing it wrote —
        // it clears whatever a same-named stream left behind, which is exactly
        // the case a config carrying both `urls` and `relaySource` produces.
        // See [PagingProgress.reset].
        paging.reset(name)
        val tally =
            CycleTally(
                discovered = group.size,
                foldedOntoAnother = 0,
                hosts =
                    group
                        .map {
                            it.value.url.url
                                .substringAfter("://")
                                .substringBefore("/")
                        }.distinct()
                        .size,
            ).also {
                tallies[name] = it
                // Always pass 1: a static backfill runs once per process and
                // then live-tails, so there is no second walk to number.
                phases.beginCycle(name, StreamPhases.STATIC, 1, it)
            }
        // Our own count, taken once for the stream — the cheap half of the
        // reconcile-or-page decision.
        val ours = runCatching { store.count(filter) }.getOrNull() ?: 0
        val (reconcilers, pagers) =
            group.partitionSuspend { worthReconciling(it.value, filter, ours) }
        System.err.println(
            "router: $name ${reconcilers.size} relay(s) will reconcile, ${pagers.size} will fetch" +
                " [sync=${group.first().value.sync.wire}]" +
                " (we hold ${fmtCount(ours)} matching event(s), floor ${config.negMinEvents})",
        )
        val eventsEarly = AtomicLong()
        // The pagers own the phase line only when nobody else does: when
        // relays also reconcile, that path is the long pole and reports the
        // id walk, which an operator cannot otherwise see.
        val pagersReport = reconcilers.isEmpty()
        val pagedDone = AtomicInteger()
        val early =
            if (pagers.isEmpty()) {
                null
            } else {
                if (pagersReport) phases.set(name, StreamPhases.Phase.Fetching(0, pagers.size, 0))
                scope.launch { pageAll(name, pagers, eventsEarly, pagedDone, pagersReport) }
            }
        if (reconcilers.isEmpty()) {
            early?.join()
            phases.endCycle(name, StreamPhases.STATIC, "completed")
            phases.set(name, StreamPhases.Phase.Idle(eventsEarly.get(), null))
            return
        }
        phases.set(name, StreamPhases.Phase.Queued(reconcilers.size))
        // One snapshot for the stream, or one right-sized window at a time?
        //
        // Decided by OUR count, because that is what the shared snapshot costs:
        // below the target the whole set fits in one window anyway and sharing
        // it across the group is strictly cheaper (one id walk, not one per
        // relay). Above it, that single walk is the multi-gigabyte hold this
        // package's stream gate exists to contain — and it is also the thing a
        // crash throws away. See [NegentropyPager].
        val sweeping = config.negPageTarget > 0 && ours > config.negPageTarget
        try {
            streamGate.withPermit {
                if (sweeping) {
                    System.err.println(
                        "router: static backfill $name reconciling in windows of ~${fmtCount(config.negPageTarget)} event(s)" +
                            " — ${fmtCount(ours)} local event(s) is past the point where one shared snapshot pays for itself",
                    )
                    sweepAll(name, reconcilers, eventsEarly, early)
                    return@withPermit
                }
                val snapshot = snapshotForStream(reconcilers.map { it.value }, filter)
                // Awaited inside the permit: the id set stays live until the last
                // relay is done with it, so releasing at the fan-out would let the
                // next stream allocate its own on top of this one.
                val done = AtomicInteger()
                val events = AtomicLong()
                phases.set(name, StreamPhases.Phase.Syncing(0, reconcilers.size, 0, 0, 0))
                coroutineScope {
                    reconcilers.forEach { (idx, upstream) ->
                        launch {
                            val got = reconcileOne(idx, upstream, snapshot)
                            events.addAndGet(got.toLong())
                            phases.set(
                                name,
                                StreamPhases.Phase.Syncing(done.incrementAndGet(), reconcilers.size, events.get(), 0, 0),
                            )
                        }
                    }
                }
                early?.join()
                phases.endCycle(name, StreamPhases.STATIC, "completed")
                phases.set(name, StreamPhases.Phase.Idle(events.get() + eventsEarly.get(), null))
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // The snapshot is the one step without its own catch
            // (reconcileOne contains per-relay failures). Contain a throw to
            // THIS stream: siblings share run()'s coroutineScope and a
            // propagating failure would cancel their backfills too, and
            // relays never marked done would tick the progress loop forever.
            System.err.println("router: $name backfill failed before reconcile (${e.message}) — ${reconcilers.size} relay(s) skipped until next boot")
            reconcilers.forEach { (idx, up) ->
                progress.done(idx, 0)
                settle(up, 0, failed = true)
            }
            early?.join()
            // `failed`, and the distinction is the whole point of publishing an
            // outcome: this stream stopped before it reconciled anything, and
            // the Idle line below it is byte-identical to the one a stream that
            // finished cleanly leaves.
            phases.endCycle(name, StreamPhases.STATIC, "failed")
            phases.set(name, StreamPhases.Phase.Idle(eventsEarly.get(), null))
        }
    }

    /** Page every non-reconciling relay, with a ticker refreshing the phase line. */
    private suspend fun pageAll(
        name: String,
        pagers: List<IndexedValue<SyncUpstream>>,
        eventsEarly: AtomicLong,
        pagedDone: AtomicInteger,
        pagersReport: Boolean,
    ) {
        // Refreshed on a tick, not only as relays finish: twelve relays paging
        // for an hour complete almost never, and a percentage that only moves
        // on completion is the stale-phase bug this exists to end.
        val tick =
            if (!pagersReport) {
                null
            } else {
                scope.launch {
                    while (true) {
                        delay(PROGRESS_INTERVAL_MS)
                        phases.set(name, fetchingPhase(name, pagedDone.get(), pagers.size, eventsEarly.get()))
                    }
                }
            }
        // coroutineScope, because `forEach { launch }` returns the instant the
        // children START — cancelling the ticker after it would kill the
        // ticker microseconds in. This awaits them.
        try {
            coroutineScope {
                pagers.forEach { (idx, upstream) ->
                    launch {
                        // pageOne feeds [eventsEarly] as events land, so the
                        // ticker has a number that moves.
                        pageOne(idx, upstream, eventsEarly)
                        if (pagersReport) {
                            phases.set(name, fetchingPhase(name, pagedDone.incrementAndGet(), pagers.size, eventsEarly.get()))
                        }
                    }
                }
            }
        } finally {
            tick?.cancel()
        }
    }

    private fun fetchingPhase(
        name: String,
        done: Int,
        total: Int,
        events: Long,
    ) = StreamPhases.Phase.Fetching(done, total, events, paging.fraction(name), paging.etaMs(name), paging.reached(name))

    /**
     * Page a relay's whole window, with no local id set involved — same leg
     * walk and band bookkeeping as the reconcile path, but it runs while a
     * snapshot is still being built.
     */
    private suspend fun pageOne(
        idx: Int,
        upstream: SyncUpstream,
        live: AtomicLong,
    ): Int {
        val legs = bands.legs(upstream.streamName, upstream.url, upstream.filter)
        if (legs.isEmpty()) {
            progress.done(idx, 0)
            settle(upstream, 0)
            return 0
        }
        var downloaded = 0
        // Invariant for this upstream: hoisted out of the per-event
        // callback, which allocated an identical object per event.
        val origin = originFor(upstream)
        transferring.incrementAndGet()
        return try {
            for (leg in legs) {
                // Floored before it is walked, or an upstream holding an event
                // stamped `created_at = 0` drives this walk past zero and it
                // never returns. See [flooredForPaging].
                val window = leg.flooredForPaging()
                var seenMin: Long? = null
                var seenMax: Long? = null
                // Per-kind spans, which quartz's SyncCoverage requires before it
                // will record a band for a multi-kind filter at all.
                val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
                val walk = PagingProgress.Walked(upstream.streamName, upstream.url.url)
                // Counted as events arrive — not from fetchAllPages' return
                // value, which only lands when the walk ends; that once read
                // `0 event(s)` through a 17-minute, 7.5M-event walk.
                var seenSoFar = 0
                val cursor = paging.begin(walk, window.until ?: nowSeconds(), window.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
                // The walk's own account of why it stopped. Only `DRAINED` — the
                // relay EOSEd an empty page, so there is nothing older — earns the
                // band the right to close its older leg. See `drainSettlesThePast`.
                // In a try/finally for the same reason as DynamicSync's: the
                // caller catches Exception, and a throw between begin and
                // finish strands this walk in PagingProgress at 0%, which
                // `fraction` then averages into the stream's number forever.
                // Assigned inside the try rather than taken from its value:
                // `finally` runs first, and it is what tells PagingProgress
                // whether this walk drained — see [PagingProgress.finish].
                var walked: PagedFetchResult? = null
                try {
                    walked =
                        client.fetchAllPages(
                            upstream.url,
                            listOf(window),
                            NEG_IDLE_MS,
                            onNewPage = { until -> cursor?.reached(until) },
                        ) { event ->
                            if (upstream.filter.match(event)) {
                                if (SyncCoverage.isPlausible(event.createdAt)) {
                                    seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                    seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                    // Where the walk is, as it moves — `onNewPage`
                                    // above fires only at page boundaries, so
                                    // without this a leg inside its first page
                                    // reports the day it opened at. See
                                    // [PagingProgress.Walk.reached].
                                    cursor?.reached(event.createdAt)
                                }
                                // Per KIND too: a band holding one interval for a
                                // multi-kind filter lets a long-lived kind vouch
                                // for a short-lived one, and quartz now declines to
                                // record such a band at all rather than over-claim.
                                // Without this, every multi-kind stream here would
                                // stop resuming.
                                SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                                ingest.submit(event, upstream.trusted, origin)
                            }
                            seenSoFar++
                            live.incrementAndGet()
                            progress.update(idx, downloaded + seenSoFar, downloaded + seenSoFar)
                        }
                } finally {
                    paging.finish(walk, covered = walked?.drained == true)
                }
                downloaded += walked?.downloaded ?: 0
                // paged = true: this walked a span, it did not reconcile a
                // range, so the band it earns is the span it saw.
                bands.record(
                    upstream.streamName,
                    upstream.url,
                    upstream.filter,
                    seenMin,
                    seenMax,
                    paged = true,
                    observedByKind = seenByKind,
                    drained = drainSettlesThePast(walked, window, upstream.filter),
                )
            }
            progress.done(idx, downloaded)
            settle(upstream, downloaded)
            System.err.println("router: static backfill ${upstream.url.url} paged $downloaded (no snapshot needed)")
            downloaded
        } catch (e: CancellationException) {
            // Shutdown: rethrow before the catch below reports it as a failed
            // fetch and zeroes the relay's real count.
            throw e
        } catch (e: Exception) {
            progress.done(idx, 0)
            settle(upstream, 0, failed = true)
            System.err.println("router: static backfill ${upstream.url.url} paged fetch failed: ${e.message}")
            0
        } finally {
            transferring.decrementAndGet()
        }
    }

    /**
     * Reconcile against our id set, or just page the relay?
     *
     * Negentropy is worth its id exchange exactly to the extent that the two
     * sides already share data, and nothing else decides it. A stream that
     * KNOWS says so via [SyncMode] — sharing is a property of the kind, not
     * of the volume, and counts cannot see it (NIP-85 assertions put millions
     * on both sides and share essentially none).
     *
     * For `auto`, two counts on the same filter stand in, assuming overlap
     * tracks volume: ours below the floor → fetch (bootstrap: nothing to
     * reconcile against); theirs below the floor → a small relay is cheaper
     * to fetch outright; both large → reconcile (the 20,000-relay sweep,
     * where paging once cost 14.4M downloaded events to keep 9,878).
     *
     * A relay that does not answer COUNT is reconciled: our side is already
     * known large by then, and `negentropySyncOrFetch` falls back to paging
     * on its own if the relay lacks NIP-77 too. Guessing wrong here costs one
     * redundant id exchange; guessing wrong the other way re-downloads
     * everything.
     */
    private suspend fun worthReconciling(
        upstream: SyncUpstream,
        filter: Filter,
        ours: Int,
    ): Boolean {
        // Declared beats measured.
        when (upstream.sync) {
            SyncMode.NEGENTROPY -> return true

            SyncMode.FETCH -> return false

            // A `live` upstream never reaches here — RouterConfig.backfillUpstreams
            // filters it out before this class is handed anything, because the
            // whole point of the mode is that no history is walked. Answered
            // rather than left to an `else` so that a future caller passing one
            // in gets the cheap branch instead of a store walk it must not do.
            SyncMode.LIVE -> return false

            SyncMode.AUTO -> Unit
        }
        // OUR count decides it, and nothing else.
        //
        // This used to ask the relay for a NIP-45 COUNT too, and reconcile only
        // when both sides cleared the floor. That cost a round trip per relay
        // per cycle across a 16,000-relay fan-out, and bought a worse answer:
        // COUNT is optional and widely unimplemented (one upstream here replies
        // `unknown cmd`), and where it IS implemented it can be slow — measured
        // at a 13.5s median on a loaded relay, against a 5s budget. A relay that
        // did not answer was assumed reconcilable anyway, so most of that
        // waiting changed nothing.
        //
        // What actually decides whether reconciling pays is how much WE hold: a
        // reconcile transfers the difference, so it wins when our set is already
        // most of theirs, and loses when we are starting from nothing and the
        // difference is everything. That is answerable from our own store, for
        // free, without asking anyone.
        return ours >= config.negMinEvents
    }

    /**
     * The local id set every relay in one stream reconciles against, narrowed
     * to what the hungriest of them still needs ([SyncBands.coveringWindow]).
     */
    private suspend fun snapshotForStream(
        group: List<SyncUpstream>,
        filter: Filter,
    ): StreamSnapshot {
        val urls = group.map { it.url }
        // Nobody in this group has anything outside its band, so every
        // reconcileOne below returns at its own `legs.isEmpty()` check without
        // ever reading the snapshot. Build it anyway and the cycle pays the
        // most expensive thing this router does for no work at all —
        // coveringWindow cannot help here, because once no relay needs anything
        // there is no window to narrow TO and it correctly hands back the whole
        // filter. Asking first is the only place the saving exists.
        if (!bands.anyOutstanding(group.first().streamName, urls, filter)) {
            System.err.println(
                "router: static backfill ${group.first().streamName} — all ${group.size} relay(s) already cover the filter, skipping the snapshot",
            )
            return StreamSnapshot(emptyList(), nowSeconds())
        }
        val window = bands.coveringWindow(group.first().streamName, urls, filter)
        val startedMs = System.currentTimeMillis()
        val takenAt = startedMs / 1000
        val name = group.first().streamName
        // The denominator, asked for once — it turns "4.2M ids so far" into
        // "4.2M/14.9M (28%)". Null rather than a guess if it fails.
        val expected = runCatching { store.count(window) }.getOrNull()
        phases.set(name, StreamPhases.Phase.Snapshotting(0, expected, group.size))
        val local =
            store.snapshotIdsReporting(window) { collected ->
                phases.set(name, StreamPhases.Phase.Snapshotting(collected, expected, group.size))
            }
        System.err.println(
            "router: static backfill $name local snapshot ${local.size} id(s) in ${fmtDuration(System.currentTimeMillis() - startedMs)}" +
                (window.since?.let { ", since $it" } ?: ", full filter (no relay is caught up yet)") +
                " — shared by ${group.size} relay(s)",
        )
        return StreamSnapshot(local, takenAt)
    }

    /**
     * The windowed path: every relay in the group sweeps its own legs, each
     * against ids read one window at a time. No shared snapshot exists here —
     * the whole point is that it never gets built.
     */
    private suspend fun sweepAll(
        name: String,
        reconcilers: List<IndexedValue<SyncUpstream>>,
        eventsEarly: AtomicLong,
        early: Job?,
    ) {
        val done = AtomicInteger()
        val events = AtomicLong()
        phases.set(name, StreamPhases.Phase.Syncing(0, reconcilers.size, 0, 0, 0))
        coroutineScope {
            reconcilers.forEach { (idx, upstream) ->
                launch {
                    val got = sweepOne(idx, upstream)
                    events.addAndGet(got.toLong())
                    phases.set(
                        name,
                        StreamPhases.Phase.Syncing(done.incrementAndGet(), reconcilers.size, events.get(), 0, 0),
                    )
                }
            }
        }
        early?.join()
        phases.endCycle(name, StreamPhases.STATIC, "completed")
        phases.set(name, StreamPhases.Phase.Idle(events.get() + eventsEarly.get(), null))
    }

    /**
     * One relay, swept leg by leg through [NegentropyPager].
     *
     * The band bookkeeping is the same as [reconcileOne]'s and for the same
     * reasons; what differs is where the coverage claim comes from. A leg is
     * only recorded once the sweep reports it COMPLETE — a sweep that stopped
     * early has already written its cursor, and recording a band from a partial
     * leg would let the next boot skip the part that never happened.
     */
    private suspend fun sweepOne(
        idx: Int,
        upstream: SyncUpstream,
    ): Int {
        val legs = bands.legs(upstream.streamName, upstream.url, upstream.filter)
        if (legs.isEmpty()) {
            progress.done(idx, 0)
            settle(upstream, 0)
            System.err.println("router: static backfill ${upstream.url.url} already covers its filter — nothing outside the synced band")
            return 0
        }
        // Invariant for this upstream: hoisted out of the per-event
        // callback, which allocated an identical object per event.
        val origin = originFor(upstream)
        transferring.incrementAndGet()
        return try {
            var downloaded = 0
            var windows = 0
            var pagedWindows = 0
            var legsDone = 0
            for (leg in legs) {
                var seenMin: Long? = null
                var seenMax: Long? = null
                val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
                // Stamped before the first window: every window reconciles
                // against ids read at or after this moment, so it is the
                // conservative claim. Erring early costs a re-fetch, never a gap.
                val startedAt = nowSeconds()
                // Hoisted so the callback can report where a paged walk has got
                // to. The fallback below rebuilt the identical string.
                val walk = PagingProgress.Walked(upstream.streamName, upstream.url.url)
                // Set only if this leg falls back to paging below — the sweep
                // reconciles and has no cursor — and read by `onEvent`, which
                // both halves share. See [PagingProgress.Walk].
                var cursor: PagingProgress.Walk? = null
                val onEvent: suspend (Event) -> Unit = { event ->
                    if (upstream.filter.match(event)) {
                        if (SyncCoverage.isPlausible(event.createdAt)) {
                            seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                            seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                            // Null for the reconciling half of this callback's
                            // life: the SWEEP has no cursor, and only the paging
                            // fallback below sets one. See [PagingProgress.Walk].
                            cursor?.reached(event.createdAt)
                        }
                        SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                        ingest.submit(event, upstream.trusted, origin)
                    }
                }
                val here = downloaded
                val outcome =
                    pager.sweep(
                        stream = upstream.streamName,
                        url = upstream.url,
                        shape = upstream.filter,
                        leg = leg,
                        onProgress = { need, got -> progress.update(idx, need, here + got) },
                        onEvent = onEvent,
                    )
                downloaded += outcome.downloaded
                windows += outcome.reconciledWindows + outcome.pagedWindows
                pagedWindows += outcome.pagedWindows
                if (!outcome.negentropyUsable) {
                    // The relay never reconciled a single window — the same
                    // "try negentropy, else page" fallback the non-windowed path
                    // gets from `negentropySyncOrFetch`, made here because the
                    // pager deliberately leaves the choice to its caller.
                    // Only what the sweep has NOT already covered: a resumed
                    // sweep may be most of the way down the leg, and paging the
                    // whole thing would throw that away.
                    // The `?: leg` is unreachable today — the ONE path that sets
                    // `negentropyUsable = false` always supplies `outstanding` —
                    // but the field is nullable, so this is the type's required
                    // handling rather than dead code. Do not "simplify" it to
                    // `!!`: falling back to the whole leg re-pages, which is
                    // wasteful, while `!!` would crash the stream.
                    val rest = (outcome.outstanding ?: leg).flooredForPaging()
                    cursor = paging.begin(walk, rest.until ?: nowSeconds(), rest.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
                    // finally: see the paged site above — an orphaned walk is
                    // averaged into `fraction` at 0% for as long as the process
                    // lives.
                    // Inside the try, so `finally` can report the drain — see
                    // [PagingProgress.finish].
                    var walked: PagedFetchResult? = null
                    try {
                        walked =
                            client.fetchAllPages(
                                upstream.url,
                                listOf(rest),
                                NEG_IDLE_MS,
                                onNewPage = { until -> cursor?.reached(until) },
                                onEvent = onEvent,
                            )
                    } finally {
                        paging.finish(walk, covered = walked?.drained == true)
                    }
                    downloaded += walked?.downloaded ?: 0
                    pagedWindows++
                    legsDone++
                    // Against `rest`, not `leg`: a resumed sweep leaves only the
                    // part it had not reached, and that remainder is what was
                    // actually paged. Its floor is the leg's whenever the sweep
                    // was working downward, which is the only case that matters.
                    bands.record(
                        upstream.streamName,
                        upstream.url,
                        upstream.filter,
                        seenMin,
                        seenMax,
                        paged = true,
                        observedByKind = seenByKind,
                        drained = drainSettlesThePast(walked, rest, upstream.filter),
                    )
                    continue
                }
                if (!outcome.complete) continue
                legsDone++
                // A leg with any paged window in it is recorded as PAGED: the
                // reconcile claim covers a whole range compared at once, and one
                // second that had to be walked instead is enough to make that
                // claim untrue for the leg.
                val paged = outcome.pagedWindows > 0
                bands.record(
                    upstream.streamName,
                    upstream.url,
                    upstream.filter,
                    seenMin,
                    seenMax,
                    paged,
                    reconciledThrough = startedAt.takeUnless { paged },
                    observedByKind = seenByKind,
                )
            }
            progress.done(idx, downloaded)
            settle(upstream, downloaded)
            System.err.println(
                "router: static backfill ${upstream.url.url} downloaded $downloaded over $windows window(s)" +
                    (if (pagedWindows > 0) " ($pagedWindows paged)" else "") +
                    " [$legsDone/${legs.size} leg(s) complete]",
            )
            downloaded
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            progress.done(idx, 0)
            settle(upstream, 0, failed = true)
            System.err.println("router: static backfill ${upstream.url.url} sweep failed: ${e.message}")
            0
        } finally {
            transferring.decrementAndGet()
        }
    }

    private suspend fun reconcileOne(
        idx: Int,
        upstream: SyncUpstream,
        snapshot: StreamSnapshot,
    ): Int {
        val legs = bands.legs(upstream.streamName, upstream.url, upstream.filter)
        if (legs.isEmpty()) {
            progress.done(idx, 0)
            settle(upstream, 0)
            System.err.println("router: static backfill ${upstream.url.url} already covers its filter — nothing outside the synced band")
            return 0
        }
        // Invariant for this upstream: hoisted out of the per-event
        // callback, which allocated an identical object per event.
        val origin = originFor(upstream)
        transferring.incrementAndGet()
        try {
            var downloaded = 0
            var paged = false
            for (window in legs) {
                // Track the span this leg actually saw: the client reports how
                // many events came back, not when they were from.
                var seenMin: Long? = null
                var seenMax: Long? = null
                // Per-kind spans, which quartz's SyncCoverage requires before it
                // will record a band for a multi-kind filter at all.
                val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
                // No wall-clock deadline anywhere here — see [NEG_IDLE_MS].
                val result =
                    client.negentropySyncOrFetch(
                        relay = upstream.url,
                        filter = window,
                        idleTimeoutMs = NEG_IDLE_MS,
                        localEntries = snapshot.ids,
                        // Declined before the REQ, so a twice-refused id never
                        // crosses the wire. Null when suppression is off, which
                        // is what keeps quartz's no-predicate path (batch handed
                        // back uncopied) for everyone who has not opted in.
                        wantId = wantIdFor(window),
                        onProgress = { needSoFar, done -> progress.update(idx, needSoFar, downloaded + done) },
                        onEvent = { event ->
                            if (upstream.filter.match(event)) {
                                // Only PLAUSIBLE stamps widen the band: one
                                // future-dated event among 700k once discarded
                                // a whole upstream's band.
                                if (SyncCoverage.isPlausible(event.createdAt)) {
                                    seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                    seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                }
                                SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                                ingest.submit(event, upstream.trusted, origin)
                            }
                        },
                    )
                downloaded += result.downloaded
                paged = paged || result.pagedFallback
                // Per leg, not once at the end: a crash between legs keeps the
                // ground the first one gained. Coverage is stamped from when
                // the SNAPSHOT was read — that is the state the relay was
                // compared against, and erring early only costs a small
                // re-fetch, never a gap.
                bands.record(
                    upstream.streamName,
                    upstream.url,
                    upstream.filter,
                    seenMin,
                    seenMax,
                    result.pagedFallback,
                    reconciledThrough = snapshot.takenAt.takeUnless { result.pagedFallback },
                    // Only read on the paged-fallback branch; a finished
                    // reconcile compares the whole filter at once and earns the
                    // same span for every kind in it.
                    observedByKind = seenByKind,
                    // No `drained` here, deliberately: the paging happens inside
                    // quartz's `negentropySyncOrFetch`, which returns its own
                    // result and does not carry the walk's `end` out. So a NIP-77-less
                    // relay reached through this path keeps the old behaviour and
                    // re-asks its older leg — the conservative direction, and not
                    // the path a `sync = "fetch"` stream takes anyway. Thread the
                    // signal through that helper upstream to close this last gap.
                )
            }
            progress.done(idx, downloaded)
            settle(upstream, downloaded)
            val band = bands.band(upstream.streamName, upstream.url, upstream.filter)
            System.err.println(
                "router: static backfill ${upstream.url.url} downloaded $downloaded" +
                    (if (paged) " (paged REQ fallback — no NIP-77)" else " (negentropy)") +
                    (if (legs.size > 1) " [resumed: ${legs.size} leg(s) outside the synced band]" else "") +
                    (if (band != null) " [synced ${band.minCreatedAt}..${band.maxCreatedAt}]" else ""),
            )
            return downloaded
        } catch (e: CancellationException) {
            // Shutdown: rethrow before the catch below reports it as a failed
            // backfill and zeroes the relay's real count.
            throw e
        } catch (e: Exception) {
            progress.done(idx, 0)
            settle(upstream, 0, failed = true)
            System.err.println("router: static backfill ${upstream.url.url} failed: ${e.message}")
            return 0
        } finally {
            transferring.decrementAndGet()
        }
    }

    /** Print overall progress until every upstream is done, then a closing line. */
    suspend fun progressLoop(dynamicStreams: Int) {
        while (scope.isActive) {
            delay(PROGRESS_INTERVAL_MS)
            val s = progress.snapshot()
            if (s.allDone) {
                bands.flush()
                // "backfill complete" must not read as "caught up" while the
                // dynamic streams — the larger half of the fill — still run.
                System.err.println(
                    "router: static backfill complete — ${s.downloaded} events from ${s.total} relay(s)" +
                        " in ${fmtDuration(s.elapsedMs)}; live tail now streaming" +
                        if (dynamicStreams > 0) "; $dynamicStreams dynamic stream(s) still syncing" else "",
                )
                return
            }
            // ETA from the average rate since start — steadier than an
            // instantaneous window, which flickers to zero between pages.
            val elapsedSec = s.elapsedMs / 1000.0
            val avgRate = if (elapsedSec > 0) s.downloaded / elapsedSec else 0.0
            val remaining = (s.need - s.downloaded).coerceAtLeast(0)
            val etaSec = if (avgRate > 1) (remaining / avgRate).toLong() else -1
            System.err.println(
                "router: static backfill ${s.done}/${s.total} relay(s) done, ${s.downloaded}/${s.need} events (${s.percent()}%)" +
                    ", ${"%.0f".format(avgRate)}/s avg" +
                    (if (etaSec >= 0) ", ETA ~${fmtDuration(etaSec * 1000)} to useful" else ", ETA —"),
            )
        }
    }

    /**
     * The local id set one stream's reconcilers share. [takenAt] is when the
     * ids were read: the coverage a reconcile earns is measured from HERE,
     * not from when a relay's leg happened to start — stamping later would
     * claim we compared a window we never looked at.
     */
    private class StreamSnapshot(
        val ids: List<IdAndTime>,
        val takenAt: Long,
    )

    /**
     * Aggregate backfill progress across upstreams. `needSoFar` grows as
     * reconciliation discovers ids, so early percentages are estimates that
     * firm up as the run proceeds.
     */
    private class BackfillProgress {
        private val need = ConcurrentHashMap<Int, Long>()
        private val got = ConcurrentHashMap<Int, Long>()
        private val finished = ConcurrentHashMap<Int, Long>()

        @Volatile private var totalUpstreams = 0

        @Volatile private var startMs = 0L

        fun begin(n: Int) {
            totalUpstreams = n
            startMs = System.currentTimeMillis()
        }

        fun update(
            idx: Int,
            needSoFar: Int,
            downloaded: Int,
        ) {
            need[idx] = needSoFar.toLong()
            got[idx] = downloaded.toLong()
        }

        fun done(
            idx: Int,
            downloaded: Int,
        ) {
            finished[idx] = downloaded.toLong()
            got[idx] = downloaded.toLong()
            // A finished upstream's need is exactly what it downloaded.
            need[idx] = maxOf(need[idx] ?: 0L, downloaded.toLong())
        }

        fun snapshot(): Snapshot {
            val need = need.values.sum()
            val got = got.values.sum()
            return Snapshot(
                need = need,
                downloaded = got,
                done = finished.size,
                total = totalUpstreams,
                allDone = finished.size >= totalUpstreams && totalUpstreams > 0,
                elapsedMs = System.currentTimeMillis() - startMs,
            )
        }
    }

    private data class Snapshot(
        val need: Long,
        val downloaded: Long,
        val done: Int,
        val total: Int,
        val allDone: Boolean,
        val elapsedMs: Long,
    ) {
        fun percent(): Int = if (need <= 0) 0 else ((downloaded * 100) / need).coerceIn(0, 100).toInt()
    }

    /**
     * The suppression predicate for one window, or null when it is off.
     *
     * Note the limit quartz documents and this inherits: it does NOT cover a
     * window handed to the paged fallback. A REQ names no ids before it streams
     * bodies, so a declined event inside a paged window arrives anyway. Rare,
     * and the ingest-side check still drops it before the store.
     */
    private fun wantIdFor(window: Filter): ((String) -> Boolean)? =
        if (!refusedIds.enabled) {
            null
        } else {
            { id -> !refusedIds.suppressedInWindow(id, window.since, window.until) }
        }

    /** What this upstream's stream lets the healer do about a stale copy. */
    private fun originFor(upstream: SyncUpstream) = IngestOrigin(upstream.url, healContent = upstream.healContent, healRetractions = upstream.healRetractions)
}

/**
 * [List.partition] where the predicate suspends, evaluated concurrently — the
 * predicate this exists for is a NIP-45 COUNT round trip with its own
 * timeout, and serially, twelve silent relays would be a minute of dead wait.
 */
private suspend fun <T> List<T>.partitionSuspend(predicate: suspend (T) -> Boolean): Pair<List<T>, List<T>> =
    coroutineScope {
        val marked = map { item -> async { item to predicate(item) } }.awaitAll()
        marked.filter { it.second }.map { it.first } to marked.filterNot { it.second }.map { it.first }
    }
