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

import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
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
import java.util.concurrent.atomic.AtomicLong

/**
 * The `deleteMissing` comparison, run in a retracting ask's audit slot. The
 * upstream is the source of truth for the kinds it owns, so the audit goes
 * both ways: download what it has and we lack, delete what we hold and it no
 * longer serves. The catch-up has already paged the ask, so a failed
 * reconcile decides nothing; the download never depends on the comparison
 * and the comparison is never inferred from the download.
 */
internal class RetractionAudit(
    private val client: NostrClient,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val refusedIds: RefusedIds,
) {
    /** Records dropped because their owning upstream stopped serving them; the one counter that goes down. */
    val deleted = AtomicLong()

    /**
     * The ask's owned-kind projection: the one filter the audit clock, the
     * reconcile and the deletes all run on. Null when the ask carries no owned kind.
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
     * When this ask's comparison next comes due, read-only. On the owned
     * projection, because that is the filter the reconcile stamps. An ask
     * with no owned kind is [AuditClock.NOT_SCHEDULED], not due.
     */
    fun auditClock(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        negentropySyncThePastSeconds: Long,
    ): AuditClock {
        val ownedAsk = ownedAskOf(stream, ask) ?: return AuditClock.NOT_SCHEDULED
        return AuditClock.of(bands.auditDueAt(stream.name, url, ownedAsk, negentropySyncThePastSeconds))
    }

    /** [SyncBands.claimAudit] on the owned ask. True commits the caller to running the reconcile. */
    fun claimAudit(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        negentropySyncThePastSeconds: Long,
    ): Boolean {
        val ownedAsk = ownedAskOf(stream, ask) ?: return false
        return bands.claimAudit(stream.name, url, ownedAsk, negentropySyncThePastSeconds)
    }

    /**
     * Reconcile one ask's owned kinds both ways and act on the difference.
     * The ids are read for this ask alone; kinds outside
     * [SyncStream.ownedKinds] are never touched. [sharedAuthors] are found at
     * more than one relay, so their asks are never judged: one relay's empty
     * answer does not retract what a sibling still serves.
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
        // Enforced where the deletion happens, not only in the loader: an
        // unbound ask would judge every provider against this one relay.
        val bound = ask.authors
        if (bound.isNullOrEmpty()) {
            System.err.println(
                "router: ${stream.name} ${url.url} retraction ask binds no authors — refusing to judge every provider at once",
            )
            return
        }
        if (bound.any { it in sharedAuthors }) return

        val mine =
            storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_SNAPSHOT_IDS, StoreCalls.summarise(ownedAsk)) {
                store.snapshotIdsForNegentropy(listOf(ownedAsk))
            }
        // No early return on an empty local set: that is "give me everything", and the delete side no-ops.

        // Stamped before the comparison: the band claims coverage through this moment.
        val startedAt = nowSeconds()

        // A completed reconcile is the whole licence to delete; quartz throws rather than falling back.
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

        // The one licence both the band and the delete run on.
        val compared = diff.windows >= 1

        val observed = mirrorNeeded(stream, url, ownedAsk, diff.needIds, onEvent)

        // The claim rests on `reconciledThrough`, not event times, so an empty reconcile still records.
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
     * The download half: fetch what the provider has that we lack and ingest
     * it. fetchAll, not fetchAllPages: an id set is not a time range. Twice
     * refused ids are dropped before any REQ is sent.
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
                // Matched against the owned ask, so a relay answering a by-id REQ with extras cannot widen a trusted ingest.
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

    /** The delete half, dry-run or enforce. Only called after `compared` held. */
    private suspend fun deleteRetracted(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        ownedAsk: Filter,
        mine: Int,
        diff: NegentropyIdDiff,
    ) {
        // No size guard: a mass retraction is precisely the case that matters, and the completed reconcile is what makes it trustworthy.
        val share = diff.haveIds.size.toDouble() / mine
        if (stream.deleteMissing == DeleteMissing.DRY_RUN) {
            System.err.println(
                "router: ${stream.name} would delete ${diff.haveIds.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                    " for ${url.url} after a clean ${diff.windows}-window reconcile — set deleteMissing = true to apply",
            )
            return
        }
        // Deleted by id and inside the ask, so a delete can never reach past what this reconcile compared.
        for (chunk in diff.haveIds.chunked(ID_FETCH_CHUNK)) {
            storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_DELETE, StoreCalls.ids(chunk.size)) {
                store.delete(ownedAsk.copy(ids = chunk, since = null, until = null, limit = null))
            }
        }
        deleted.addAndGet(diff.haveIds.size.toLong())
        System.err.println(
            "router: ${stream.name} deleted ${diff.haveIds.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                " ${url.url} no longer serves, after a clean ${diff.windows}-window reconcile",
        )
    }

    companion object {
        /** Ids per by-id REQ, and per delete. The store's own bulk chunk. */
        private const val ID_FETCH_CHUNK = 500
    }
}
