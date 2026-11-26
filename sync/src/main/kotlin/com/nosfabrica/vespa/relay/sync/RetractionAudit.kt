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
import com.nosfabrica.vespa.relay.ingest.IngestAcks
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.addressTagOrNull
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.DONE_REASON_EOSE
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyIdDiff
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllWithHooks
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.deletionsCovering
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import java.util.concurrent.atomic.AtomicLong

/** The reads the retraction audit makes of a relay, and the seam its tests stand in for. */
internal interface RetractionReads {
    /** The id diff of [filter] against [local]; throws [NegentropySyncException] when no reconcile completed. */
    suspend fun reconcileIds(
        url: NormalizedRelayUrl,
        filter: Filter,
        local: List<IdAndTime>,
    ): NegentropyIdDiff

    /** Delivers what [url] serves of [ids]. True only when the relay ended the answer with an EOSE. */
    suspend fun fetchIds(
        url: NormalizedRelayUrl,
        ids: List<String>,
        onEvent: suspend (Event) -> Unit,
    ): Boolean

    /** Delivers what [url] serves of [filter], paged past its cap. True only when the walk drained. */
    suspend fun fetchMatching(
        url: NormalizedRelayUrl,
        filter: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Boolean
}

/** Quartz, unchanged: a by-id answer reports how it ended, so a stall never reads as an empty answer. */
internal class ClientRetractionReads(
    private val client: NostrClient,
    private val waivers: LensWaivers = LensWaivers.none(),
) : RetractionReads {
    override suspend fun reconcileIds(
        url: NormalizedRelayUrl,
        filter: Filter,
        local: List<IdAndTime>,
    ): NegentropyIdDiff = client.negentropyReconcileIds(url, waivers.onWire(url, filter), local, idleTimeoutMs = NEG_IDLE_MS)

    override suspend fun fetchIds(
        url: NormalizedRelayUrl,
        ids: List<String>,
        onEvent: suspend (Event) -> Unit,
    ): Boolean {
        val result =
            client.fetchAllWithHooks(filters = mapOf(url to listOf(waivers.onWire(url, Filter(ids = ids)))), idleTimeoutMs = NEG_IDLE_MS) { relay, event ->
                if (relay == url) onEvent(event)
                // Handed on above; true would have quartz keep every event in a result list no one reads.
                false
            }
        return result.doneReasons[url] == DONE_REASON_EOSE && url !in result.stalled && url !in result.dead
    }

    override suspend fun fetchMatching(
        url: NormalizedRelayUrl,
        filter: Filter,
        onEvent: suspend (Event) -> Unit,
    ): Boolean = client.fetchAllPages(url, listOf(waivers.onWire(url, filter)), NEG_IDLE_MS) { onEvent(it) }.end == PagedFetchResult.End.DRAINED
}

/**
 * The `deleteMissing` comparison, run in a retracting ask's audit slot: download what the
 * upstream has and we lack, and for what we hold and it no longer serves, fetch the evidence that
 * it was deleted and apply that. The catch-up has already paged the ask, so a failed reconcile
 * decides nothing, and absence alone deletes only where the stream sets `deleteOnAbsence`.
 */
internal class RetractionAudit(
    private val reads: RetractionReads,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val refusedIds: RefusedIds,
    /** Where the deletes go: the store, or the serving relay as signed `MirrorDelete` requests. */
    private val writer: StoreWriter = StoreWriter.local(store),
) {
    /** Holds back absence deletes on an answer that shrank too far to believe. */
    private val shrink = ShrinkGuard()

    /** Records dropped on absence alone, on a stream that sets `deleteOnAbsence`. */
    val deleted = AtomicLong()

    /** Audits whose absence deletes were held back because the peer's answer shrank too far to believe. */
    val withheld = AtomicLong()

    /** Deletion requests and vanishes a peer served that name a record we hold, each counted once per audit. */
    val evidenceFetched = AtomicLong()

    /** Records a peer stopped serving that are gone on evidence: their author's request, or a newer version. */
    val retractedOnEvidence = AtomicLong()

    /** Records a peer stopped serving that nothing says were deleted, kept. */
    val missingKept = AtomicLong()

    /** Forgets the shrink baselines of (stream, relay) units not [wanted]. */
    fun retain(wanted: (String, NormalizedRelayUrl) -> Boolean) = shrink.retain { wanted(it.stream, it.url) }

    /**
     * The ask's owned-kind projection, the one filter the audit clock, the reconcile and the
     * deletes all run on. Null when the ask carries no owned kind.
     */
    internal fun ownedAskOf(
        stream: SyncStream,
        ask: Filter,
    ): Filter? {
        val ownedKinds = ask.kinds.orEmpty().filter { it in stream.ownedKinds }
        if (ownedKinds.isEmpty()) return null
        return ask.copy(kinds = ownedKinds)
    }

    /**
     * When this ask's comparison next comes due, read-only. An ask with no owned kind is
     * [AuditClock.NOT_SCHEDULED], not due.
     */
    fun auditClock(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        negentropySyncThePastSeconds: Long,
        band: String = "",
    ): AuditClock {
        val ownedAsk = ownedAskOf(stream, ask) ?: return AuditClock.NOT_SCHEDULED
        return AuditClock.of(bands.auditDueAt(stream.name, url, ownedAsk, negentropySyncThePastSeconds, band))
    }

    /** [SyncBands.claimAudit] on the owned ask. True commits the caller to running the reconcile. */
    fun claimAudit(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        negentropySyncThePastSeconds: Long,
        band: String = "",
    ): Boolean {
        val ownedAsk = ownedAskOf(stream, ask) ?: return false
        return bands.claimAudit(stream.name, url, ownedAsk, negentropySyncThePastSeconds, band = band)
    }

    /**
     * Reconcile one ask's owned kinds both ways and act on the difference. [sharedAuthors] are
     * found at more than one relay, so their asks are never judged: one relay's empty answer
     * does not retract what a sibling still serves.
     */
    suspend fun reconcileAndDelete(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ask: Filter,
        sharedAuthors: Set<String>,
        onActivity: () -> Unit = {},
        /** How long the recovered events may wait for the store's verdicts; past it nothing is recorded or deleted. */
        verdictWaitMs: Long = LEG_QUIET_GIVE_UP_MS,
        onEvent: suspend (Event) -> Unit = {},
    ) {
        val ownedAsk = ownedAskOf(stream, ask) ?: return
        // Enforced here, not only in the loader: an unbound ask would judge every provider at once.
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

        // A completed reconcile is the whole licence to delete.
        val diff =
            try {
                reads.reconcileIds(url, ownedAsk, mine)
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

        // The claim rests on `reconciledThrough`, not event times, so an empty reconcile still records;
        // a by-id answer that stalled or was refused leaves ids unfetched, so it claims nothing, and
        // neither does one whose events did not all reach the store ([IngestAcks]).
        val verdicts = observed.acks.awaitWithin(verdictWaitMs)
        // The wait was ours, not the relay's silence.
        onActivity()
        if (verdicts is IngestAcks.Verdicts.Unanswered) {
            // A store that has not taken what we recovered is not one to delete from.
            System.err.println(
                "router: ${stream.name} ${url.url} — ${verdicts.count} recovered event(s) had no verdict from the store after " +
                    "${verdictWaitMs / 1000}s; the band stays unverified and nothing is deleted until the next audit",
            )
            return
        }
        val stored = verdicts == IngestAcks.Verdicts.Stored
        if (compared && observed.drained && verdicts is IngestAcks.Verdicts.Failed) {
            System.err.println(
                "router: ${stream.name} ${url.url} — ${verdicts.count} recovered event(s) did not reach the store; " +
                    "the band stays unverified and the next audit fetches them again",
            )
        }
        if (compared && observed.drained && stored) {
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
        if (compared && !observed.drained) {
            // The clock stays put, so without a backoff the whole audit would re-run at every attempt spacing.
            val inARow = bands.noteIncompleteAudit(stream.name, url, ownedAsk)
            System.err.println(
                "router: ${stream.name} ${url.url} did not answer every by-id ask in full — the band stays unverified" +
                    " ($inARow audit(s) in a row; the next waits longer)",
            )
        }
        val peerHeld = mine.size - diff.haveIds.size + diff.needIds.size
        // Settled on every compared audit, so the absence path judges against the peer's last answer.
        val believable = compared && stream.deleteOnAbsence && shrink.mayDelete(ShrinkGuard.Key(stream.name, url, ownedAsk), mine.size, peerHeld)
        if (diff.haveIds.isEmpty()) return
        if (!compared) {
            System.err.println(
                "router: ${stream.name} ${url.url} reconciled 0 window(s) — nothing was compared, deleting nothing",
            )
            return
        }
        val judged = retractOnEvidence(stream, url, ownedAsk, diff.haveIds, verdictWaitMs, onActivity)
        val unproven = judged.unproven.size
        if (judged.unchecked > 0) {
            missingKept.addAndGet((judged.unchecked + unproven).toLong())
            System.err.println(
                "router: ${stream.name} ${url.url} — ${judged.unchecked} of the ${diff.haveIds.size} record(s) it no longer serves " +
                    "could not be checked for a deletion request; keeping them and the $unproven with none until the next audit",
            )
            return
        }
        if (unproven == 0) return
        if (!stream.deleteOnAbsence) {
            missingKept.addAndGet(unproven.toLong())
            System.err.println(
                "router: ${stream.name} ${url.url} no longer serves $unproven of our ${mine.size} record(s) and holds no deletion " +
                    "request or newer version for them — kept (missingKept)",
            )
            return
        }
        // Fail closed: a peer reading us through a trust lens answers exactly this, and refuses nothing.
        if (!believable) {
            withheld.incrementAndGet()
            missingKept.addAndGet(unproven.toLong())
            System.err.println(
                "router: ${stream.name} ${url.url} answered $peerHeld of the ${mine.size} record(s) we hold — too few to believe; " +
                    "deleting none of the $unproven it lacks with no evidence (retractionsWithheld)",
            )
            return
        }
        deleteAbsent(stream, url, ownedAsk, mine.size, judged.unproven, diff.windows)
    }

    /** What the download half saw, for the band record, and whether every chunk was answered in full. */
    private class Observed {
        var min: Long? = null
        var max: Long? = null
        val byKind = mutableMapOf<Int, SyncCoverage.Span>()
        var drained = true

        /** The store's answers to what this download handed ingest. */
        val acks = IngestAcks()
    }

    /**
     * The download half, by id rather than paged: an id set is not a time range. Twice refused
     * ids are dropped before any REQ is sent.
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
            val answered =
                reads.fetchIds(url, chunk) { event ->
                    onEvent(event)
                    // Matched against the owned ask, so extras in a by-id answer cannot widen a trusted ingest.
                    if (ownedAsk.match(event)) {
                        if (SyncCoverage.isPlausible(event.createdAt)) {
                            observed.min = minOf(observed.min ?: event.createdAt, event.createdAt)
                            observed.max = maxOf(observed.max ?: event.createdAt, event.createdAt)
                        }
                        SyncCoverage.observe(observed.byKind, event.kind, event.createdAt)
                        ingest.submit(event, stream.trusted, origin, observed.acks.track())
                    }
                }
            if (!answered) observed.drained = false
        }
        return observed
    }

    /** The ids an audit could not settle on evidence: still held with none, or never answered for. */
    private class Judged {
        val unproven = ArrayList<String>()
        var unchecked = 0
    }

    /** One audit's evidence asks of one peer: a filter asked twice is answered once, and the asks stop after [MAX_SHORT_EVIDENCE] short in a row. */
    private inner class PeerEvidence(
        private val url: NormalizedRelayUrl,
    ) {
        private val answered = HashMap<String, List<Event>>()
        private var shortInARow = 0

        /** Set by any ask of the current chunk that was not answered in full. */
        var short = false

        val gaveUp: Boolean get() = shortInARow >= MAX_SHORT_EVIDENCE

        suspend fun fetch(filter: Filter): List<Event> {
            val key = filter.toJson()
            answered[key]?.let { return it }
            if (gaveUp) {
                short = true
                return emptyList()
            }
            val got = ArrayList<Event>()
            // Matched and verified here: only a signed request this filter names may count, or be ingested.
            val drained = reads.fetchMatching(url, filter) { event -> if (filter.match(event) && event.verify()) got += event }
            if (!drained) {
                shortInARow++
                short = true
                return got
            }
            shortInARow = 0
            answered[key] = got
            return got
        }
    }

    /**
     * The deletion evidence for [missing] ids, applied: a newer version already took the record's place
     * (the download half ingested it), the store holds its author's request, or the peer serves one, which
     * is ingested so the store's NIP-09/62 rules delete and keep the tombstone. Dry-run only reports. An
     * expired record never gets here: the store's snapshot omits it. What is left is the caller's.
     */
    private suspend fun retractOnEvidence(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ownedAsk: Filter,
        missing: List<String>,
        verdictWaitMs: Long,
        onActivity: () -> Unit,
    ): Judged {
        val judged = Judged()
        val enforce = stream.deleteMissing == DeleteMissing.ON
        val origin = IngestOrigin(url, stream.healContent, stream.healRetractions)
        val acks = IngestAcks()
        val peer = PeerEvidence(url)
        val ingested = HashSet<String>()
        // Under enforce, the ids whose fate the ingested evidence decides; the store's answer is read after.
        val awaiting = ArrayList<String>()
        var superseded = 0
        var requestedHere = 0
        var namedThere = 0
        for (chunk in missing.chunked(EVIDENCE_CHUNK)) {
            val held = storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_QUERY, StoreCalls.ids(chunk.size)) { store.query<Event>(byIds(ownedAsk, chunk)) }
            superseded += chunk.size - held.size
            val local = coveredBy(held, covering(held) { f -> storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_QUERY, StoreCalls.summarise(f)) { store.query<Event>(f) } })
            requestedHere += local.size
            if (enforce) deleteByIds(ownedAsk, local.toList())
            val rest = held.filterNot { it.id in local }
            if (rest.isEmpty()) continue
            peer.short = false
            val found = covering(rest) { f -> peer.fetch(f) }
            onActivity()
            if (peer.short) {
                judged.unchecked += rest.size
                continue
            }
            val named = coveredBy(rest, found)
            namedThere += named.size
            val fresh = found.filter { ingested.add(it.id) }
            evidenceFetched.addAndGet(fresh.size.toLong())
            if (enforce) {
                for (event in fresh) ingest.submit(event, skipVerify = true, origin = origin, onOutcome = acks.track())
                awaiting += rest.map { it.id }
            } else {
                rest.filterNot { it.id in named }.mapTo(judged.unproven) { it.id }
            }
        }
        var removedThere = 0
        if (awaiting.isNotEmpty()) {
            val verdicts = acks.awaitWithin(verdictWaitMs)
            onActivity()
            if (verdicts == IngestAcks.Verdicts.Stored) {
                val still = HashSet<String>()
                for (chunk in awaiting.chunked(EVIDENCE_CHUNK)) {
                    storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_QUERY, StoreCalls.ids(chunk.size)) { store.query<Event>(byIds(ownedAsk, chunk)) }.mapTo(still) { it.id }
                }
                removedThere = awaiting.size - still.size
                awaiting.filterTo(judged.unproven) { it in still }
            } else {
                // A store that has not taken the evidence has not judged by it.
                judged.unchecked += awaiting.size
            }
        }
        val gone = superseded + if (enforce) requestedHere + removedThere else 0
        retractedOnEvidence.addAndGet(gone.toLong())
        if (superseded + requestedHere + namedThere == 0) return judged
        val evidence = "$superseded superseded by a newer version, $requestedHere by a request we hold"
        System.err.println(
            if (enforce) {
                "router: ${stream.name} ${url.url} — of the ${missing.size} record(s) it no longer serves, $gone are gone on evidence " +
                    "($evidence, $removedThere by the ${ingested.size} request(s) it served, ingested)"
            } else {
                "router: ${stream.name} ${url.url} — of the ${missing.size} record(s) it no longer serves, evidence would remove " +
                    "${superseded + requestedHere + namedThere} ($evidence, $namedThere by the ${ingested.size} request(s) it serves) " +
                    "— set deleteMissing = true to apply"
            },
        )
        return judged
    }

    /** Quartz's covering requests for [events], through [query]; a store that names no relay is in no vanish's scope. */
    private suspend fun covering(
        events: List<Event>,
        query: suspend (Filter) -> List<Event>,
    ): List<Event> {
        if (events.isEmpty()) return emptyList()
        val self = store.relay ?: return deletionsCovering(events, NO_RELAY) { f -> if (f.kinds == VANISH_KINDS) emptyList() else query(f) }
        return deletionsCovering(events, self, query)
    }

    private suspend fun deleteByIds(
        ownedAsk: Filter,
        ids: List<String>,
    ) {
        for (chunk in ids.chunked(ID_FETCH_CHUNK)) {
            storeCall(StoreCalls.CALLER_AUDIT_RETRACTION, StoreCalls.OP_DELETE, StoreCalls.ids(chunk.size)) { writer.delete(byIds(ownedAsk, chunk)) }
        }
    }

    /** The absence delete, dry-run or enforce: [ids] were compared, checked for evidence, and had none. */
    private suspend fun deleteAbsent(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        ownedAsk: Filter,
        mine: Int,
        ids: List<String>,
        windows: Int,
    ) {
        // A retraction past ShrinkGuard's share is held back above, and left to the operator.
        val share = ids.size.toDouble() / mine
        if (stream.deleteMissing == DeleteMissing.DRY_RUN) {
            System.err.println(
                "router: ${stream.name} would delete ${ids.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                    " for ${url.url} on absence after a clean $windows-window reconcile — set deleteMissing = true to apply",
            )
            return
        }
        // By id and inside the ask, so a delete never reaches past what this reconcile compared.
        deleteByIds(ownedAsk, ids)
        deleted.addAndGet(ids.size.toLong())
        System.err.println(
            "router: ${stream.name} deleted ${ids.size}/$mine record(s) (${(share * 100).toInt()}%)" +
                " ${url.url} no longer serves, on absence (deleteOnAbsence), after a clean $windows-window reconcile",
        )
    }

    companion object {
        /** Ids per by-id REQ, and per delete. The store's own bulk chunk. */
        private const val ID_FETCH_CHUNK = 500

        /** Records per evidence ask: one `#e` and one `#a` filter, each well under a relay's message cap. */
        internal const val EVIDENCE_CHUNK = 100

        /** Evidence asks in a row answered short before the audit stops asking; quartz's `DownloadPace` gives up at three. */
        private const val MAX_SHORT_EVIDENCE = 3

        private val VANISH_KINDS = listOf(RequestToVanishEvent.KIND)

        /** Only a stand-in for [covering]'s signature where the store names no relay; its vanish asks are never sent. */
        private val NO_RELAY = RelayUrlNormalizer.normalize("wss://no-relay.invalid")

        private fun byIds(
            ownedAsk: Filter,
            ids: List<String>,
        ) = ownedAsk.copy(ids = ids, since = null, until = null, limit = null)

        /** The ids of [targets] that [evidence], already scoped by quartz's `deletionsCovering`, removes under NIP-09 and NIP-62. */
        internal fun coveredBy(
            targets: List<Event>,
            evidence: List<Event>,
        ): Set<String> {
            val out = HashSet<String>()
            for (request in evidence) {
                val own = targets.filter { it.pubKey == request.pubKey }
                when (request) {
                    is DeletionRequestEvent -> {
                        val ids = request.deleteEventIds().toHashSet()
                        val addresses = request.deleteAddresses().filter { it.pubKeyHex == request.pubKey }.mapTo(HashSet()) { it.toValue() }
                        own.filterTo(ArrayList()) { it.id in ids || (it.createdAt <= request.createdAt && it.addressTagOrNull() in addresses) }.mapTo(out) { it.id }
                    }

                    is RequestToVanishEvent -> {
                        own.filter { it.createdAt < request.createdAt }.mapTo(out) { it.id }
                    }
                }
            }
            return out
        }
    }
}
