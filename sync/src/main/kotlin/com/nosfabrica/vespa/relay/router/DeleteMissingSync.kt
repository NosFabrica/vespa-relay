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

import com.nosfabrica.vespa.relay.router.config.DeleteMissing
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.progress.LegProgress
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import java.util.concurrent.atomic.AtomicLong

/**
 * The `deleteMissing` path of a dynamic stream: the upstream is the source of
 * truth for its records (a NIP-85 provider's own relay for its own scores),
 * so a sync here goes BOTH ways — download what it has and we lack, delete
 * what we hold and it no longer serves. Nothing is ever published upstream.
 */
internal class DeleteMissingSync(
    private val client: NostrClient,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val paging: PagingProgress,
    private val refusedIds: RefusedIds,
) {
    /**
     * Records dropped because the upstream that owns them stopped serving
     * them — the only number in the router that goes DOWN, so it is reported
     * on the health line rather than buried in a sync's rounding.
     */
    val deleted = AtomicLong()

    /**
     * Reconcile one narrow ask both ways.
     *
     * The ids are read for THIS ask, never a cycle's shared snapshot — the
     * shared snapshot spans every service on the stream, and handing it to a
     * one-service reconcile would delete almost the whole corpus. This is the
     * one place in the router where a wrong filter destroys data, so the ids
     * are derived from the ask itself and there is no parameter to pass the
     * wrong thing in.
     *
     * The ask splits by kind first: only [SyncStream.ownedKinds] is reconciled
     * and deleted from. The rest is ATTACHED — downloaded from the same relay,
     * never judged by its absence there, and dropped only by [cascade] when
     * the owned set is retracted wholesale.
     *
     * [sharedAuthors] are authors this cycle found at more than one relay. One
     * relay's empty answer does not retract what a sibling relay may still be
     * serving, so they are downloaded from and never deleted for.
     */
    suspend fun reconcileAndDelete(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        sharedAuthors: Set<String>,
        /**
         * The caller's liveness counter for this leg, ticked as events arrive.
         *
         * Threaded in rather than derived from the return value because this
         * call is where a `deleteMissing` stream spends its hours: a reconcile
         * against a provider holding ~150k score events is minutes on a good
         * day, and a counter that moves only when the call returns says nothing
         * at all about the call that never does. Null for a caller with nothing
         * to report to — the probe, and the tests.
         */
        legProgress: LegProgress? = null,
    ): Int {
        val askKinds = ask.kinds.orEmpty()
        val attachedKinds = askKinds.filter { it !in stream.ownedKinds }
        val ownedKinds = askKinds.filter { it in stream.ownedKinds }

        // Attached kinds ride along as an ordinary paged mirror: we still want
        // the provider's own kind 0/10002 if it serves them, we just never
        // read their absence as a retraction.
        var attachedDownloaded = 0
        if (attachedKinds.isNotEmpty()) {
            attachedDownloaded = pageAsk(stream, url, ask.copy(kinds = attachedKinds), legProgress)
        }
        if (ownedKinds.isEmpty()) return attachedDownloaded

        // A relay this author is not alone at cannot prove a retraction; keep
        // mirroring it, decide nothing from it.
        if (ask.authors?.any { it in sharedAuthors } == true) {
            return attachedDownloaded + pageAsk(stream, url, ask.copy(kinds = ownedKinds), legProgress)
        }

        val ownedAsk = ask.copy(kinds = ownedKinds)
        val mine = store.snapshotIdsForNegentropy(listOf(ownedAsk))
        // NOT an early return when we hold nothing: an ask we have no records
        // for is exactly a service we have never fetched, and reconciling
        // against an empty local set is precisely "give me everything"; the
        // delete side then no-ops on its own.

        // Stamped BEFORE the comparison, never after: the band below claims
        // coverage through this moment, and anything the provider published
        // while the reconcile ran is outside what was compared. Same reading
        // as the ordinary path's `syncStartedAt`.
        val startedAt = nowSeconds()

        // A COMPLETED reconcile is the whole licence to delete. quartz throws
        // rather than falling back, so a normal return means every window was
        // compared and an empty answer is the relay's answer, not its
        // silence. Failing that, page the ask so the mirror still fills, and
        // delete nothing.
        val diff =
            try {
                client.negentropyReconcileIds(url, ownedAsk, mine, idleTimeoutMs = NEG_IDLE_MS)
            } catch (e: NegentropySyncException) {
                System.err.println(
                    "router: ${stream.name} ${url.url} could not reconcile (${e.reason}) — paging instead, deleting nothing",
                )
                return attachedDownloaded + pageAsk(stream, url, ownedAsk, legProgress)
            }

        // Whether this reconcile compared a RANGE at all. Read by both things
        // that need the licence — the band below and the delete further down —
        // and named once so they cannot drift apart: two copies of `windows <
        // 1` is one edit away from a band claiming coverage a delete would not
        // dare act on, or the reverse.
        val compared = diff.windows >= 1

        // fetchAll, not fetchAllPages: an id set is not a time range, and
        // paging it by `until` re-asks for events it just received.
        var downloaded = attachedDownloaded
        // Invariant for this (stream, relay): hoisted out of the fetch loop
        // below rather than rebuilt per event.
        val origin = IngestOrigin(url, stream.healContent, stream.healRetractions)
        // The one place suppression saves BANDWIDTH rather than CPU: the diff
        // names the ids before anything is fetched, so an id we have twice
        // refused never becomes a REQ at all. Everywhere else on the down path
        // a REQ streams bodies without naming them first, and the earliest
        // possible hook is already past the wire.
        val wanted = diff.needIds.filterNot { refusedIds.suppressedInWindow(it, ownedAsk.since, ownedAsk.until) }
        val skipped = diff.needIds.size - wanted.size
        if (skipped > 0) {
            System.err.println("router: ${stream.name} ${url.url} skipped $skipped id(s) already twice refused")
        }
        // The edges of what came back, on the same terms as every other sync
        // path: screened per event, because one misdated stamp among a
        // thousand honest ones must not cost the relay its whole band.
        var seenMin: Long? = null
        var seenMax: Long? = null
        val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
        for (chunk in wanted.chunked(ID_FETCH_CHUNK)) {
            for (event in client.fetchAll(url, listOf(Filter(ids = chunk)), NEG_IDLE_MS)) {
                // Where the events ARRIVE — see the parameter's own note. This
                // loop is the long half of a big provider's reconcile.
                legProgress?.received()
                if (stream.filter.match(event)) {
                    if (SyncCoverage.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                    ingest.submit(event, stream.trusted, origin)
                    downloaded++
                }
            }
        }

        // The band this path used to leave unwritten — the reason a relay that
        // reconciled CLEANLY with nothing to send was invisible on the coverage
        // card while one that happened to have an event was drawn.
        //
        // A reconcile is exactly the walk that can say something when nothing
        // came back: the comparison itself proves the two sides are level, so
        // the claim rests on `reconciledThrough` rather than on event times,
        // which is why an empty PAGE records nothing and an empty RECONCILE
        // records this. The ordinary streams have always recorded it
        // ([DynamicSync.syncOneFilter]); only the deleteMissing path, which
        // reconciles through a different quartz call to get at `haveIds`, never
        // carried the line across.
        //
        // Gated on [compared], the same licence the delete below runs on: a
        // band is a claim that a range WAS compared, which is the thing that
        // makes an empty answer mean anything in either direction.
        //
        // What this now narrows, stated plainly: [pageAsk] reads bands
        // ([SyncBands.legs]), so a later reconcile that FAILS pages from this
        // moment instead of walking the provider's whole history again. That is
        // the same claim every other stream makes off a clean reconcile, and it
        // is the intended second effect — the fallback re-walked everything,
        // every cycle, because nothing was ever recorded here. It reaches
        // nothing else: the delete side deletes BY ID out of `ownedAsk`, which
        // is the stream's filter narrowed by discovery alone, and this stream
        // holds no shared snapshot to narrow ([DynamicSync.holdsIdSet]).
        if (compared) {
            bands.record(
                stream.name,
                url,
                ownedAsk,
                seenMin,
                seenMax,
                paged = false,
                reconciledThrough = startedAt,
                observedByKind = seenByKind,
            )
        }
        if (diff.haveIds.isEmpty()) return downloaded

        // A reconcile that split into no windows compared no range. It cannot
        // have returned a meaningful diff, whatever it says.
        if (!compared) {
            System.err.println(
                "router: ${stream.name} ${url.url} reconciled 0 window(s) — nothing was compared, deleting nothing",
            )
            return downloaded
        }

        val retracted = retracts(mine.size, diff.needIds.size, diff.haveIds.size, diff.windows)

        // NO SIZE GUARD, deliberately. A provider that retracts a subject
        // usually does it because the subject turned out to be a scammer —
        // exactly the score that must not survive — and a mass retraction is
        // precisely the case that matters. Volume guards blocked that case
        // while the harmless ones sailed through; the completed reconcile
        // above is what makes "empty" trustworthy enough to act on.
        val share = diff.haveIds.size.toDouble() / mine.size
        if (stream.deleteMissing == DeleteMissing.DRY_RUN) {
            System.err.println(
                "router: ${stream.name} would delete ${diff.haveIds.size}/${mine.size} record(s) (${(share * 100).toInt()}%)" +
                    " for ${url.url} after a clean ${diff.windows}-window reconcile — set deleteMissing = true to apply",
            )
            if (retracted) cascade(stream, url, ask, attachedKinds, apply = false)
            return downloaded
        }
        // Deleted BY ID and inside the ask: the filter that found them is the
        // filter that removes them, so a delete can never reach past the
        // records this reconcile actually compared.
        for (chunk in diff.haveIds.chunked(ID_FETCH_CHUNK)) {
            store.delete(ownedAsk.copy(ids = chunk, since = null, until = null, limit = null))
        }
        deleted.addAndGet(diff.haveIds.size.toLong())
        System.err.println(
            "router: ${stream.name} deleted ${diff.haveIds.size}/${mine.size} record(s) (${(share * 100).toInt()}%)" +
                " ${url.url} no longer serves, after a clean ${diff.windows}-window reconcile",
        )
        if (retracted) cascade(stream, url, ask, attachedKinds, apply = true)
        return downloaded
    }

    /**
     * The attached kinds go when the owned set does.
     *
     * A NIP-85 service key exists to sign scores. Once every score it ever
     * published is retracted, its kind 0 and 10002 describe a provider that no
     * longer provides anything — we would be holding, and serving in search, a
     * profile kept alive by nothing but our own copy of it. They are meant to
     * go together.
     *
     * Scoped by [ask], so this reaches exactly the authors the reconcile just
     * judged and only the kinds that reconcile was never allowed to speak for.
     */
    private suspend fun cascade(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        attachedKinds: List<Int>,
        apply: Boolean,
    ) {
        if (attachedKinds.isEmpty()) return
        val cascadeAsk = ask.copy(kinds = attachedKinds, ids = null, since = null, until = null, limit = null)
        val held = runCatching { store.count(cascadeAsk) }.getOrDefault(0)
        if (held <= 0) return
        val what = "$held attached record(s) $attachedKinds for ${ask.authors?.size ?: 0} retracted service(s) via ${url.url}"
        if (!apply) {
            System.err.println("router: ${stream.name} would cascade — $what")
            return
        }
        store.delete(cascadeAsk)
        deleted.addAndGet(held.toLong())
        System.err.println("router: ${stream.name} cascaded — deleted $what")
    }

    /**
     * Page one ask, for a relay that could not reconcile it — the mirror
     * still wants the events; only the DELETE side needs a reconcile. Same
     * band bookkeeping as every other paged path, or the relay would
     * re-walk its whole history every cycle.
     */
    private suspend fun pageAsk(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        legProgress: LegProgress? = null,
    ): Int {
        var downloaded = 0
        // Invariant for this (stream, relay), as in the reconcile path above.
        val origin = IngestOrigin(url, stream.healContent, stream.healRetractions)
        for (unfloored in bands.legs(stream.name, url, ask)) {
            // Every walk below this one is paged, so the floor goes on here —
            // without it an upstream holding a `created_at = 0` event walks past
            // zero and never comes back. See [flooredForPaging].
            val leg = unfloored.flooredForPaging()
            var seenMin: Long? = null
            var seenMax: Long? = null
            // Per-kind spans, which quartz's SyncCoverage requires before it
            // will record a band for a multi-kind filter at all.
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            // Same time-axis reporting as every other paged walk: without it
            // these walks are the one hole in the stream's fraction/ETA line.
            val walk = "${stream.name}|${url.url}"
            var walked: PagedFetchResult? = null
            paging.begin(walk, leg.until ?: nowSeconds(), leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
            try {
                // Assigned inside the try, so `finally` can tell PagingProgress
                // whether the walk drained — see [PagingProgress.finish].
                walked =
                    client.fetchAllPages(
                        url,
                        listOf(leg),
                        NEG_IDLE_MS,
                        onNewPage = { until -> paging.mark(walk, until) },
                    ) { event ->
                        legProgress?.received()
                        if (stream.filter.match(event)) {
                            if (SyncCoverage.isPlausible(event.createdAt)) {
                                seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                            }
                            SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                            ingest.submit(event, stream.trusted, origin)
                        }
                    }
                downloaded += walked?.downloaded ?: 0
            } finally {
                paging.finish(walk, covered = walked?.drained == true)
            }
            bands.record(
                stream.name,
                url,
                ask,
                seenMin,
                seenMax,
                paged = true,
                observedByKind = seenByKind,
                drained = drainSettlesThePast(walked, leg, ask),
            )
        }
        return downloaded
    }

    companion object {
        /** Ids per by-id REQ, and per delete. The store's own bulk chunk. */
        private const val ID_FETCH_CHUNK = 500

        /**
         * Does this reconcile say the author's owned set was RETRACTED, as
         * opposed to rewritten, partly dropped, or never held? Only a
         * retraction may take the attached kinds down with it.
         *
         * An addressable record a provider replaces arrives as its old id
         * retracted and a new id offered, which is why [need] must be zero:
         * that one field is the whole difference between "this provider
         * published a fresh score" and "this provider is gone". [mine] > 0
         * keeps it to real losses — a service we never held scores for has
         * retracted nothing, whatever its relay serves today — and [windows]
         * repeats the reconcile-completed check the caller already made,
         * because the cost of getting this wrong is someone else's profile.
         */
        internal fun retracts(
            mine: Int,
            need: Int,
            have: Int,
            windows: Int,
        ): Boolean = windows >= 1 && mine > 0 && need == 0 && have == mine
    }
}
