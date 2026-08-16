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
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyIdDiff
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The `deleteMissing` comparison, ON THE AUDIT'S CLOCK — the pool's port of
 * [DeleteMissingSync]'s core. The upstream is the source of truth for its own
 * records (a NIP-85 provider's relay for its own scores), so the ask's audit
 * goes both ways: download what it has and we lack, delete what we hold and
 * it no longer serves. Nothing is ever published upstream.
 *
 * ## What did NOT come across, and why
 *
 * The legacy class also paged — attached kinds, and any ask whose reconcile
 * failed — because in the cycle model this call was the ask's only visit. In
 * the pool the CATCH-UP has already paged the ask's whole filter before the
 * audit runs, so the mirror side is covered whatever happens here, and a
 * failed reconcile simply decides nothing: the events are in, the deletion
 * waits for a reconcile that completes. That asymmetry is the point — the
 * download must never depend on the comparison, and the comparison must never
 * be inferred from the download.
 *
 * ## The cadence
 *
 * `verifySeconds`, the same knob that schedules every other ask's history
 * audit — a deletion decision deserves the audited full-history comparison,
 * not a quick pass, and each `(relay, provider)` band ages on its own clock
 * so the reconciles stagger themselves.
 */
internal class RetractionAudit(
    private val client: NostrClient,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val refusedIds: RefusedIds,
) {
    /**
     * Records dropped because the upstream that owns them stopped serving
     * them — the only number in the router that goes DOWN, so it is reported
     * on the health line rather than buried in a sync's rounding.
     */
    val deleted = AtomicLong()

    /**
     * The ask's owned-kind projection — the filter the audit clock, the
     * reconcile and the deletes all run on. Derived HERE and only here: the
     * clock that schedules the comparison and the band the comparison stamps
     * must read one filter, and two derivations in two files is how they
     * drift apart. Null when the ask carries no owned kind at all — nothing
     * to compare, nothing to schedule.
     */
    private fun ownedAskOf(
        stream: SyncStream,
        ask: Filter,
    ): Filter? {
        val ownedKinds = ask.kinds.orEmpty().filter { it in stream.ownedKinds }
        if (ownedKinds.isEmpty()) return null
        return ask.copy(kinds = ownedKinds)
    }

    /**
     * When each ask's reconcile last RAN, complete or failed — see
     * [VisitPool.attemptSpacingSeconds]. Without it a relay whose reconcile
     * keeps failing was retried on every revisit, and every retry paid the
     * full [IEventStore.snapshotIdsForNegentropy] walk of the provider's
     * owned set before the reconcile that would fail again.
     */
    private val attempts = ConcurrentHashMap<String, Long>()

    /**
     * Is this ask's comparison due — the owned ask's verified-at clock aged
     * past [verifySeconds] (falling back to the band's `fullAt`, which a
     * fresh catch-up stamps and quartz thereafter freezes — see
     * [SyncBands.verifiedAt]), and no attempt inside the spacing window?
     * [reconcileAndDelete]'s band record is what advances the clock, on the
     * same [ownedAskOf] filter.
     */
    fun due(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        verifySeconds: Long,
    ): Boolean {
        val ownedAsk = ownedAskOf(stream, ask) ?: return false
        val now = nowSeconds()
        val verifiedAt = bands.verifiedAt(stream.name, url, ownedAsk) ?: bands.band(stream.name, url, ownedAsk)?.fullAt ?: 0L
        if (!VisitPool.auditDue(verifiedAt, now, verifySeconds)) return false
        return now - (attempts[attemptKey(stream, url, ownedAsk)] ?: 0L) >= VisitPool.attemptSpacingSeconds(verifySeconds)
    }

    private fun attemptKey(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ownedAsk: Filter,
    ): String = "${stream.name}|${url.url}|${ownedAsk.toJson()}"

    /**
     * Reconcile one ask's OWNED kinds both ways, and act on the difference.
     *
     * The ids are read for THIS ask alone — this is the one place in the
     * router where a wrong filter destroys data, so they are derived from the
     * ask itself and there is no parameter to pass the wrong thing in. Kinds
     * outside [SyncStream.ownedKinds] are not touched here at all: the
     * catch-up mirrors them, and only [cascade] may drop them, when the owned
     * set is retracted wholesale.
     *
     * [sharedAuthors] are authors the roster found at more than one relay.
     * One relay's empty answer does not retract what a sibling relay may
     * still be serving, so their asks are never judged here.
     *
     * [onActivity] ticks on any sign of life (the reconcile's long silence is
     * computation, not a hang) and [onEvent] sees every downloaded event —
     * the pool's counters and quiet clocks.
     */
    suspend fun reconcileAndDelete(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        sharedAuthors: Set<String>,
        onActivity: () -> Unit = {},
        onEvent: suspend (Event) -> Unit = {},
    ) {
        val ownedAsk = ownedAskOf(stream, ask) ?: return
        // NEVER an unbound ask. The loader refuses configs that could produce
        // one, but this is the line where a wrong filter destroys data, so
        // the boundary is enforced where the deletion happens too: an ask
        // with no authors would reconcile EVERY provider's owned records
        // against this one relay and delete whatever it happens not to hold.
        val bound = ask.authors
        if (bound.isNullOrEmpty()) {
            System.err.println(
                "router: ${stream.name} ${url.url} retraction ask binds no authors — refusing to judge every provider at once",
            )
            return
        }
        // A relay this author is not alone at cannot prove a retraction; the
        // catch-up keeps mirroring it, this decides nothing from it.
        if (bound.any { it in sharedAuthors }) return

        attempts[attemptKey(stream, url, ownedAsk)] = nowSeconds()
        val mine = store.snapshotIdsForNegentropy(listOf(ownedAsk))
        // NOT an early return when we hold nothing: an ask we have no records
        // for is exactly a service we have never fetched, and reconciling
        // against an empty local set is precisely "give me everything"; the
        // delete side then no-ops on its own.

        // Stamped BEFORE the comparison, never after: the band below claims
        // coverage through this moment, and anything the provider published
        // while the reconcile ran is outside what was compared.
        val startedAt = nowSeconds()

        // A COMPLETED reconcile is the whole licence to delete. quartz throws
        // rather than falling back, so a normal return means every window was
        // compared and an empty answer is the relay's answer, not its
        // silence. Failing that, nothing is decided — the catch-up already
        // mirrored the ask, and the deletion waits for a clean comparison.
        val diff =
            try {
                client.negentropyReconcileIds(url, ownedAsk, mine, idleTimeoutMs = NEG_IDLE_MS)
            } catch (e: NegentropySyncException) {
                System.err.println(
                    "router: ${stream.name} ${url.url} could not reconcile (${e.reason}) — deleting nothing; the catch-up already mirrors this ask",
                )
                return
            }
        onActivity()

        // Whether this reconcile compared a RANGE at all — the one licence
        // both the band and the delete run on, named once so they cannot
        // drift apart.
        val compared = diff.windows >= 1

        val observed = mirrorNeeded(stream, url, ownedAsk, diff.needIds, onEvent)

        // The comparison itself proves the two sides are level, so the claim
        // rests on `reconciledThrough` rather than on event times — which is
        // what schedules the NEXT audit of this ask on `verifySeconds`, and
        // why an empty reconcile records this where an empty page records
        // nothing.
        if (compared) {
            bands.record(
                stream.name,
                url,
                ownedAsk,
                observed.min,
                observed.max,
                paged = false,
                reconciledThrough = startedAt,
                observedByKind = observed.byKind,
            )
        }
        if (diff.haveIds.isEmpty()) return
        if (!compared) {
            System.err.println(
                "router: ${stream.name} ${url.url} reconciled 0 window(s) — nothing was compared, deleting nothing",
            )
            return
        }
        deleteRetracted(stream, url, ask, ownedAsk, mine.size, diff)
    }

    /** What the download half saw, for the band record. */
    private class Observed {
        var min: Long? = null
        var max: Long? = null
        val byKind = mutableMapOf<Int, SyncCoverage.Span>()
    }

    /**
     * THE DOWNLOAD HALF: fetch what the provider has that we lack, and ingest
     * it. Runs whatever the delete half later decides — the file's KDoc calls
     * that independence the point, and a private seam is what makes it
     * structural. fetchAll, not fetchAllPages: an id set is not a time range,
     * and paging it by `until` re-asks for events it just received. The
     * suppression saves BANDWIDTH: the diff names ids before anything is
     * fetched, so a twice-refused id never becomes a REQ.
     */
    private suspend fun mirrorNeeded(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ownedAsk: Filter,
        needIds: List<String>,
        onEvent: suspend (Event) -> Unit,
    ): Observed {
        val observed = Observed()
        val origin = IngestOrigin(url, stream.healContent, stream.healRetractions)
        val wanted = needIds.filterNot { refusedIds.suppressedInWindow(it, ownedAsk.since, ownedAsk.until) }
        val skipped = needIds.size - wanted.size
        if (skipped > 0) {
            System.err.println("router: ${stream.name} ${url.url} skipped $skipped id(s) already twice refused")
        }
        for (chunk in wanted.chunked(ID_FETCH_CHUNK)) {
            for (event in client.fetchAll(url, listOf(Filter(ids = chunk)), NEG_IDLE_MS)) {
                onEvent(event)
                // The OWNED ask's scope, not the stream's whole filter: the
                // ids came from this ask's reconcile, and a buggy relay that
                // answers a by-id REQ with extras must not widen what a
                // trusted stream ingests past the ask's author binding.
                if (ownedAsk.match(event)) {
                    if (SyncCoverage.isPlausible(event.createdAt)) {
                        observed.min = minOf(observed.min ?: event.createdAt, event.createdAt)
                        observed.max = maxOf(observed.max ?: event.createdAt, event.createdAt)
                    }
                    SyncCoverage.observe(observed.byKind, event.kind, event.createdAt)
                    ingest.submit(event, stream.trusted, origin)
                }
            }
        }
        return observed
    }

    /**
     * THE DELETE HALF: act on what we hold that a completed reconcile says
     * the provider no longer serves — dry-run or enforce, cascade on a
     * wholesale retraction. Only ever called after `compared` held; never
     * feeds the download.
     */
    private suspend fun deleteRetracted(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        ownedAsk: Filter,
        mine: Int,
        diff: NegentropyIdDiff,
    ) {
        val retracted = retracts(mine, diff.needIds.size, diff.haveIds.size, diff.windows)

        // NO SIZE GUARD, deliberately. A provider that retracts a subject
        // usually does it because the subject turned out to be a scammer —
        // exactly the score that must not survive — and a mass retraction is
        // precisely the case that matters. The completed reconcile above is
        // what makes "empty" trustworthy enough to act on.
        val share = diff.haveIds.size.toDouble() / mine
        if (stream.deleteMissing == DeleteMissing.DRY_RUN) {
            System.err.println(
                "router: ${stream.name} would delete ${diff.haveIds.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                    " for ${url.url} after a clean ${diff.windows}-window reconcile — set deleteMissing = true to apply",
            )
            if (retracted) cascade(stream, url, ask, apply = false)
            return
        }
        // Deleted BY ID and inside the ask: the filter that found them is the
        // filter that removes them, so a delete can never reach past the
        // records this reconcile actually compared.
        for (chunk in diff.haveIds.chunked(ID_FETCH_CHUNK)) {
            store.delete(ownedAsk.copy(ids = chunk, since = null, until = null, limit = null))
        }
        deleted.addAndGet(diff.haveIds.size.toLong())
        System.err.println(
            "router: ${stream.name} deleted ${diff.haveIds.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                " ${url.url} no longer serves, after a clean ${diff.windows}-window reconcile",
        )
        if (retracted) cascade(stream, url, ask, apply = true)
    }

    /**
     * The attached kinds go when the owned set does.
     *
     * A NIP-85 service key exists to sign scores. Once every score it ever
     * published is retracted, its kind 0 and 10002 describe a provider that
     * no longer provides anything — a profile kept alive by nothing but our
     * own copy of it. Scoped by [ask], so this reaches exactly the authors
     * the reconcile just judged and only the kinds it was never allowed to
     * speak for.
     */
    private suspend fun cascade(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        apply: Boolean,
    ) {
        val attachedKinds = ask.kinds.orEmpty().filter { it !in stream.ownedKinds }
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
