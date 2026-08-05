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
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
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
    ): Int {
        val askKinds = ask.kinds.orEmpty()
        val attachedKinds = askKinds.filter { it !in stream.ownedKinds }
        val ownedKinds = askKinds.filter { it in stream.ownedKinds }

        // Attached kinds ride along as an ordinary paged mirror: we still want
        // the provider's own kind 0/10002 if it serves them, we just never
        // read their absence as a retraction.
        var attachedDownloaded = 0
        if (attachedKinds.isNotEmpty()) {
            attachedDownloaded = pageAsk(stream, url, ask.copy(kinds = attachedKinds))
        }
        if (ownedKinds.isEmpty()) return attachedDownloaded

        // A relay this author is not alone at cannot prove a retraction; keep
        // mirroring it, decide nothing from it.
        if (ask.authors?.any { it in sharedAuthors } == true) {
            return attachedDownloaded + pageAsk(stream, url, ask.copy(kinds = ownedKinds))
        }

        val ownedAsk = ask.copy(kinds = ownedKinds)
        val mine = store.snapshotIdsForNegentropy(listOf(ownedAsk))
        // NOT an early return when we hold nothing: an ask we have no records
        // for is exactly a service we have never fetched, and reconciling
        // against an empty local set is precisely "give me everything"; the
        // delete side then no-ops on its own.

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
                return attachedDownloaded + pageAsk(stream, url, ownedAsk)
            }

        // fetchAll, not fetchAllPages: an id set is not a time range, and
        // paging it by `until` re-asks for events it just received.
        var downloaded = attachedDownloaded
        for (chunk in diff.needIds.chunked(ID_FETCH_CHUNK)) {
            for (event in client.fetchAll(url, listOf(Filter(ids = chunk)), NEG_IDLE_MS)) {
                if (stream.filter.match(event)) {
                    ingest.submit(event, stream.trusted)
                    downloaded++
                }
            }
        }
        if (diff.haveIds.isEmpty()) return downloaded

        // A reconcile that split into no windows compared no range. It cannot
        // have returned a meaningful diff, whatever it says.
        if (diff.windows < 1) {
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
    ): Int {
        var downloaded = 0
        for (leg in bands.legs(url, ask)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            // Same time-axis reporting as every other paged walk: without it
            // these walks are the one hole in the stream's fraction/ETA line.
            val walk = "${stream.name}|${url.url}"
            paging.begin(walk, leg.until ?: nowSeconds(), leg.since ?: SyncBands.PLAUSIBLE_FLOOR)
            try {
                downloaded +=
                    client.fetchAllPages(url, listOf(leg), NEG_IDLE_MS, onNewPage = { until -> paging.mark(walk, until) }) { event ->
                        if (stream.filter.match(event)) {
                            if (SyncBands.isPlausible(event.createdAt)) {
                                seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                            }
                            ingest.submit(event, stream.trusted)
                        }
                    }
            } finally {
                paging.finish(walk)
            }
            bands.record(url, ask, seenMin, seenMax, paged = true)
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
