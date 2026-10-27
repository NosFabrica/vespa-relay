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
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.config.SyncTier
import com.nosfabrica.vespa.relay.ingest.IngestAcks
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.status.RelayStatusReport
import com.nosfabrica.vespa.relay.status.StreamPhases
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * The sync plane: a rotating queue of (relay, stream) units worked by a fixed set of workers.
 * A worker takes a unit, dials the relay, runs that stream's jobs in order (catch-up, audit
 * where due, heal drain, tail), and the unit re-enters the queue on a revisit delay.
 */
internal class VisitPool(
    /** The reads this pool makes of a relay. */
    private val reads: RelayReads,
    /** What a relay said when it refused an ask; [RelayComplaints.DEAF] when nobody listens. */
    private val complaints: RelayComplaints = RelayComplaints.DEAF,
    /** What the socket carried across a refused ask; [RelayPages.DEAF] when nobody listens. */
    private val pages: RelayPages = RelayPages.DEAF,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val pager: NegentropyPager,
    private val healer: Healer,
    /** The deleteMissing comparison, run in a retracting stream's audit slot; null for the probes. */
    private val retraction: RetractionAudit? = null,
    private val sockets: Sockets,
    private val scope: CoroutineScope,
    /** Decides what to sync; the pool asks it to [RosterSource.rebuild] on the roster clock. */
    private val rosterBuilder: RosterSource,
    /** The visit-mode streams: every relaySource entry a kind-30166 verdict source. */
    private val streams: List<SyncStream>,
    private val progress: Processors.Handle,
    /** The streams' rows in the progress document; null for the probes. */
    private val phases: StreamPhases? = null,
    /** Worker count, the sum of the streams' dial widths. */
    private val workers: Int = DEFAULT_VISIT_CONCURRENCY,
    /** Per-stream caps on each of the pool's jobs; uncapped by default. */
    private val limits: PoolLimits = PoolLimits(emptyMap()),
    /**
     * How many kinds each relay accepts in one filter, learned from its refusals.
     * Shared with the pager so the audit's fallback REQs are chunked the same way.
     */
    private val widths: FilterWidths = FilterWidths(),
    /** Silence between asks that ends a visit; a seam for tests. */
    private val quietGiveUpMs: Long = LEG_QUIET_GIVE_UP_MS,
    /** How long, in all, one walk our ingest queue stalled may wait for it to drain; a seam for tests. */
    private val pauseLimitMs: Long = LEG_QUIET_GIVE_UP_MS,
) {
    /** When each stream's audits and re-fetches come due. */
    private val schedule = AuditSchedule(streams, bands, retraction)

    /** The current roster, swapped as one reference so asks and shared authors never mix generations. */
    @Volatile
    private var currentRoster: RosterBuilder.Roster = RosterBuilder.Roster(asks = emptyMap(), sharedAuthors = emptyMap())

    /** url → stream → asks. */
    private val roster: Map<NormalizedRelayUrl, Map<String, RosterBuilder.UnitAsks>> get() = currentRoster.asks

    /**
     * The unit of work: one stream's asks against one relay. Two streams on one relay touch
     * disjoint bands and may run concurrently; one stream's jobs on a relay are serialised.
     */
    internal data class VisitKey(
        val url: NormalizedRelayUrl,
        val stream: String,
    )

    /** Offers, collisions and revisit timers. */
    private val queue = VisitQueue<VisitKey>(scope)

    /** The visits in progress, by unit; [status] reads them. */
    private val ongoing = ConcurrentHashMap<VisitKey, OngoingVisit>()

    /** The pool's running totals. */
    private val counters = PoolCounters()

    /** Why visits end early, counted by reason. */
    private val aborts = VisitAborts()

    /** Set when a stream's phase row is stale; the flush loop clears it. */
    private val phasesDirty = AtomicBoolean(false)

    private fun phasesChanged() {
        phasesDirty.set(true)
    }

    /** The live tails and the yields that rank them. */
    private val tails =
        TailBook(
            reads = reads,
            sockets = sockets,
            limits = limits,
            widths = widths,
            ingest = ingest,
            queue = queue,
            roster = { currentRoster },
            onArrived = { counters.poolReceived.incrementAndGet() },
            onChange = ::phasesChanged,
        )

    /** The pool as the status page reads it. */
    private val status =
        PoolStatus(
            streams = streams,
            bands = bands,
            limits = limits,
            widths = widths,
            schedule = schedule,
            retraction = retraction,
            queue = queue,
            tails = tails,
            ongoing = ongoing,
            aborts = aborts,
            counters = counters,
            phases = phases,
            roster = { currentRoster },
        )

    /**
     * Is a producer of ours parked in the full ingest queue on this relay's events? A parked
     * hook silences every subscription on that socket.
     */
    private fun heldByUs(url: NormalizedRelayUrl): Boolean = ingest.parkedOn(url) > 0

    /** Where a unit's next visit starts, after one cut short; absent starts at the first ask. */
    private val resumeAt = ConcurrentHashMap<VisitKey, Int>()

    /** Units turned away before dialling: they retry after [TURNED_AWAY_RETRY_MS], not a revisit's wait. */
    private val turnedAway = ConcurrentHashMap.newKeySet<VisitKey>()

    /** Counts one event a visit delivered: the pool, the relay's yield, and the visit's own row. */
    private fun arrived(
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
    ) {
        counters.poolReceived.incrementAndGet()
        tails.credit(url)
        ongoingVisit.events.incrementAndGet()
        ongoingVisit.lastActivityMs = System.currentTimeMillis()
    }

    /** Every (relay, stream) unit on the roster, for the status page's per-relay table. */
    internal fun primeUnits(): List<RelayStatusReport.PrimeUnit> = status.primeUnits()

    /** Every open tail subscription, quietest first. */
    internal fun livePool(): InFlight = status.livePool()

    fun start() {
        if (streams.isEmpty()) return
        warnOnSocketBudget(streams)
        progress.phase("rotating")
        progress.counts { status.counts() }
        status.registerPhases()
        status.flushPhases()
        scope.launch { rosterLoop() }
        scope.launch {
            repeatEvery(PHASE_FLUSH_MS, "phase flush") { if (phasesDirty.getAndSet(false)) status.flushPhases() }
        }
        repeat(workers) { launchWorker() }
    }

    /** Workers currently drawing units, and how many were replaced after dying. */
    internal fun workerHealth(): WorkerHealth = WorkerHealth(queue.workersAlive, workers, workersRespawned.get())

    internal data class WorkerHealth(
        val alive: Int,
        val configured: Int,
        val respawned: Long,
    )

    private val workersRespawned = AtomicLong()

    /** One worker, replaced if anything but shutdown ends its loop. */
    private fun launchWorker() {
        scope
            .launch {
                queue.visitLoop(
                    stillWanted = { key -> wantedBy(currentRoster, key) },
                    // Read, never getOrPut: a finishing visit must not resurrect a pruned yield.
                    revisitDelayMs = { key ->
                        if (turnedAway.remove(key)) {
                            TURNED_AWAY_RETRY_MS
                        } else {
                            revisitDelayMs(tails.yieldScore(key.url, System.currentTimeMillis()) ?: 0.0, tails.has(key))
                        }
                    },
                    onEscape = ::recordFailure,
                    visit = ::visit,
                )
            }.invokeOnCompletion { cause ->
                if (cause == null || cause is CancellationException || !scope.isActive) return@invokeOnCompletion
                workersRespawned.incrementAndGet()
                System.err.println("router: a visit worker died (${cause.javaClass.simpleName}: ${cause.message?.take(80)}); starting a replacement")
                launchWorker()
            }
    }

    /** Rebuilds the roster on the tightest `refreshSeconds` any source asks for, floored at a minute. */
    private suspend fun rosterLoop() {
        val cadence =
            streams
                .flatMap { s -> s.discovery?.let { d -> (d.sources + d.gatedBy).map { it.refreshSeconds ?: d.refreshSeconds } }.orEmpty() }
                .minOrNull()
                ?.times(1000L)
                ?.coerceAtLeast(60_000L) ?: 300_000L
        while (scope.isActive) {
            try {
                rebuildRoster()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: visit roster rebuild failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            }
            // An empty roster usually means the monitor has not published yet; retry sooner.
            delay(if (roster.isEmpty()) EMPTY_ROSTER_RETRY_MS else cadence)
        }
    }

    private suspend fun rebuildRoster() {
        val built = rosterBuilder.rebuild()
        val next = built.asks
        val previous = currentRoster.asks
        currentRoster = built
        tails.retain(wanted = { wantedBy(built, it) }, goneUrls = previous.keys - next.keys)
        resumeAt.keys.removeIf { !wantedBy(built, it) }
        turnedAway.removeIf { !wantedBy(built, it) }
        var enqueued = 0
        for (url in next.keys) {
            // Queue a unit when its ask set is news: new to the roster, or its asks changed.
            for ((stream, unit) in built.asks[url].orEmpty()) {
                if (previous[url]?.get(stream)?.identity == unit.identity) continue
                if (queue.offer(VisitKey(url, stream))) enqueued++
            }
        }
        if (enqueued > 0 || previous.size != next.size) {
            System.err.println(
                "router: visit roster — ${next.size} prime relay(s) across ${streams.size} stream(s)" +
                    (if (enqueued > 0) ", $enqueued newly queued" else ""),
            )
        }
        phasesChanged()
        // Last, and on its own: a failed prune must not keep the new roster's units unqueued.
        try {
            bands.retain(ownedState(built))
            pager.retain(next.keys.mapTo(HashSet()) { it.url })
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("router: could not prune state off the roster: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
        }
    }

    /** Everything a unit of [roster] files state under: its catch-up bands, its audit clocks, its retraction's ask. */
    private fun ownedState(roster: RosterBuilder.Roster): Set<SyncBands.Held> {
        val keysOf = streams.associate { it.name to (SyncBands.coverageKeys(it) + it.name).distinct() }
        val out = HashSet<SyncBands.Held>()
        for ((url, byStream) in roster.asks) {
            for ((name, unit) in byStream) {
                val keys = keysOf[name] ?: listOf(name)
                val filters = unit.identity + unit.asks.mapNotNull { retraction?.ownedAskOf(it.stream, it.filter)?.toJson() }
                for (key in keys) for (filter in filters) out += SyncBands.Held(key, filter, url.url)
            }
        }
        return out
    }

    /** A visit that threw, recorded as an abort; the worker carries on. */
    private fun recordFailure(
        key: VisitKey,
        e: Throwable,
    ) {
        aborts
            .record(
                key.stream,
                key.url,
                VisitAborts.Reason.FAILED,
                asked = "${e.javaClass.simpleName}: ${e.message?.take(80)}",
                said = null,
            )?.let(System.err::println)
    }

    private fun asksFor(
        snapshot: RosterBuilder.Roster,
        key: VisitKey,
    ): List<RosterBuilder.Ask> =
        snapshot.asks[key.url]
            ?.get(key.stream)
            ?.asks
            .orEmpty()

    private fun wantedBy(
        snapshot: RosterBuilder.Roster,
        key: VisitKey,
    ): Boolean = snapshot.asks[key.url]?.containsKey(key.stream) == true

    /**
     * One stream's turn on one relay: catch-up, the audit where due, the heal drain, then its
     * tail. Other streams may visit the same relay at the same time over the same socket.
     */
    private suspend fun visit(key: VisitKey) {
        val url = key.url
        // One roster generation for the whole visit.
        val snapshot = currentRoster
        val wanted = asksFor(snapshot, key)
        // A download into a full queue parks its first event and silences the
        // socket; skipped like a refused permit, and retried shortly.
        if (ingest.isFull()) {
            counters.visitsHeldByIngest.incrementAndGet()
            turnAway(key)
            return
        }
        // Taken before the socket claim, so `visitConcurrency` bounds simultaneous dials.
        // Workers are shared across streams, so a full share is routine and retried shortly.
        val permit =
            limits.tryHold(key.stream, JOB_VISITING) ?: run {
                turnAway(key)
                return
            }
        counters.visitsRun.incrementAndGet()
        // A prompt requeue can reach here past an earlier turn-away; this visit's own wait applies.
        turnedAway.remove(key)
        val mayEvict = !tails.takeEvicted(key)
        val ongoingVisit = OngoingVisit(System.currentTimeMillis())
        ongoing[key] = ongoingVisit
        sockets.claim(url)
        try {
            // A cut visit resumes where it stopped, so the asks behind the cut are not starved.
            val start = if (wanted.isEmpty()) 0 else (resumeAt.remove(key) ?: 0) % wanted.size
            for (step in wanted.indices) {
                val at = (start + step) % wanted.size
                val ask = wanted[at]
                // Give up on silence, not on a deadline: a delivering visit is never cut.
                if (System.currentTimeMillis() - ongoingVisit.lastActivityMs > quietGiveUpMs) {
                    resumeAt[key] = at
                    aborts
                        .record(
                            key.stream,
                            url,
                            VisitAborts.Reason.GAVE_UP,
                            asked = "${quietGiveUpMs / 60_000} quiet minute(s), ${VisitAborts.asked(ask.filter)}",
                            said = null,
                        )?.let(System.err::println)
                    return
                }
                // Reset per ask: an ask with no outstanding legs never reaches the code that would.
                ongoingVisit.pagingUntil = null
                ongoingVisit.stage = ASKING
                val refusal = catchUp(ask, url, ongoingVisit)
                // A refusal ends this stream's visit; the monitor's next sweep decides re-admission.
                if (refusal != null) {
                    resumeAt[key] = at + 1
                    aborts
                        .record(
                            key.stream,
                            url,
                            refusal.reason,
                            asked = VisitAborts.asked(refusal.filter),
                            // A stall of ours has no sentence from the relay to wait for.
                            said = if (refusal.ours) null else complaints.awaitSince(url, refusal.askedAtMs),
                            sent = refusal.sent,
                        )?.let(System.err::println)
                    return
                }
                auditIfDue(
                    ask,
                    url,
                    ongoingVisit,
                    snapshot.sharedAuthors[ask.stream.name].orEmpty(),
                    snapshot.speaksNegentropy[url],
                )
            }
            // Before the tail: a tail budget turning us away is not the relay refusing us.
            aborts.cleared(key.stream, url)
            ongoingVisit.stage = FINISHING
            healer.drain(url)
            tails.open(key, mayEvict)
        } finally {
            ongoing.remove(key)
            sockets.release(url)
            // The heal drain and the tail open still count as visiting.
            permit.release()
        }
    }

    /** Nothing was dialled, so the next try needs no revisit's wait; a standing timer would hold it to one. */
    private fun turnAway(key: VisitKey) {
        turnedAway += key
        queue.disarm(key)
    }

    /** One refused walk: the ending, the chunk the relay actually saw, and when it was asked. */
    class Refusal(
        val end: PagedFetchResult.End,
        val filter: Filter,
        val askedAtMs: Long,
        /** What the socket carried while this ask was out. */
        val sent: String? = null,
        /** A hook of ours was parked in the ingest queue when the walk gave up. */
        val ours: Boolean = false,
    ) {
        val reason: VisitAborts.Reason
            get() = if (ours) VisitAborts.Reason.BACKPRESSURED else VisitAborts.of(end)
    }

    /**
     * Walks the band's outstanding legs. Returns the refusal that ended the walk with nothing
     * delivered, or null when every leg came back clean. A leg refused on width is narrowed
     * and re-walked, at most [MAX_NARROWINGS] times per leg.
     */
    private suspend fun catchUp(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val tiers = SyncBands.catchUpBands(ask.stream)
        val now = nowSeconds()
        // Oldest-last, so the catch-up of recent history is never queued behind a re-page of
        // the tail. Each band keeps its own coverage: a band expires, and only its legs re-open.
        for ((tier, olderEdge, newerEdge) in SyncTier.tile(tiers)) {
            val (since, until) = SyncTier.windowAt(now, olderEdge, newerEdge)
            val refusal = catchUpBand(ask, url, SyncTier.keyFor(ask.stream.name, tiers, tier), since, until, ongoingVisit)
            if (refusal != null) return refusal
        }
        return null
    }

    /**
     * One band's outstanding legs, under the coverage filed at [key]. The band bounds the legs,
     * never the key: a filter carrying the window would be re-keyed every time `now` moved it.
     */
    private suspend fun catchUpBand(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        key: String,
        bandSince: Long?,
        bandUntil: Long?,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val stream = ask.stream
        // Read before the first `record` below widens it; it tells a catch-up from a re-fetch.
        val covered = bands.band(key, url, ask.filter)
        val floor = ask.filter.bandFloor(bandSince)
        for (leg in bands.legs(key, url, ask.filter).mapNotNull { it.clampedTo(bandSince, bandUntil) }) {
            val stage = if (rewalksCovered(leg, covered)) REFETCHING else CATCHING_UP
            // Only a re-fetch pays a cap; a catch-up is already bounded by the dial width.
            // A refused permit skips the leg: it stays outstanding for the next visit.
            val hold =
                if (stage.pool == POOL_REFETCHING) {
                    limits.tryHold(stream.name, POOL_REFETCHING) ?: continue
                } else {
                    null
                }
            ongoingVisit.stage = stage
            val flooredLeg = leg.flooredForPaging()
            try {
                var narrowings = 0
                while (true) {
                    val refusal = walkLeg(ask, url, key, flooredLeg, floor, ongoingVisit) ?: break
                    // The relay's complaint arrives on a different listener than the refusal, so await it.
                    if (!refusal.ours &&
                        narrowings < MAX_NARROWINGS &&
                        widths.learn(url, complaints.awaitSince(url, refusal.askedAtMs), refusal.filter.kinds?.size ?: 0)
                    ) {
                        narrowings++
                        System.err.println(
                            "router: visit ${stream.name} ${url.url} — the relay refused a " +
                                "${refusal.filter.kinds?.size}-kind filter; asking in chunks of ${widths.capFor(url)} from here",
                        )
                        continue
                    }
                    return refusal
                }
            } finally {
                hold?.release()
            }
        }
        return null
    }

    /**
     * Whether every event a walk handed to ingest reached the store. A band is recorded only when
     * they all did ([IngestAcks]); otherwise the ground is walked again at the next visit.
     */
    private suspend fun storedAll(
        acks: IngestAcks,
        stream: String,
        url: NormalizedRelayUrl,
    ): Boolean {
        if (acks.await()) return true
        counters.bandsWithheld.incrementAndGet()
        System.err.println(
            "router: $stream ${url.url} — ${acks.failures} event(s) of this walk did not reach the store; " +
                "its band is not recorded, and the next visit walks the ground again",
        )
        return false
    }

    /**
     * Walks one leg as the REQs the relay will take: itself, or its kinds in chunks. Returns
     * the first refused chunk, or null. Each chunk records its own band, which is safe because
     * bands are per kind and chunking splits on kinds and nothing else.
     */
    private suspend fun walkLeg(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        key: String,
        flooredLeg: Filter,
        /** The ask bounded below by its band, whose floor a drained chunk must reach to settle it. */
        floor: Filter,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val stream = ask.stream
        for (chunk in widths.chunk(url, flooredLeg)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            // Per chunk: the depth only decreases, so a stale value would hide a shallower walk.
            ongoingVisit.pagingUntil = null
            // The band waits for the store: see [IngestAcks].
            val acks = IngestAcks()
            val onEvent: suspend (Event) -> Unit = { event ->
                arrived(url, ongoingVisit)
                // The walk is newest-first, so the oldest event seen is the cursor's depth.
                if (SyncCoverage.isPlausible(event.createdAt) && event.createdAt < (ongoingVisit.pagingUntil ?: Long.MAX_VALUE)) {
                    ongoingVisit.pagingUntil = event.createdAt
                }
                if (ask.filter.match(event)) {
                    if (SyncCoverage.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions), acks.track())
                }
            }
            // Stamped before the REQ goes out: a complaint older than this belongs to an earlier ask.
            val askedAtMs = System.currentTimeMillis()
            val sampling = pages.arm(url, chunk)
            val paged =
                try {
                    pageThroughStalls(url, chunk, ongoingVisit, onEvent)
                } finally {
                    pages.free(sampling)
                }
            val walked = paged.walked
            // OUR STALL ENDS THE VISIT, delivered events or not. The pause could not get the walk
            // going again, so every further chunk, leg and band would park against the same queue
            // — each another idle window and pause while holding this visit's permit and socket
            // claim. No band for the cut either: awaiting its acks would wait on that same queue
            // (the hook that parked is at its back), and a catch-up's cut keeps a band only where
            // it touches the one held, which is rarely. The ground is asked again next visit.
            if (paged.ours || refusedOutright(walked)) {
                // No band for a refused chunk: nothing was walked.
                return Refusal(walked.end, chunk, askedAtMs, pages.render(sampling, walked.downloaded), ours = paged.ours)
            }
            if (!storedAll(acks, stream.name, url)) {
                // Read to the end or not, ground whose events did not reach the store is not covered.
                ongoingVisit.lastActivityMs = System.currentTimeMillis()
            } else if (readToTheEnd(walked)) {
                // An answered ask is the relay responding, whether or not it had anything to send.
                ongoingVisit.lastActivityMs = System.currentTimeMillis()
                bands.record(
                    key,
                    url,
                    ask.filter,
                    seenMin,
                    seenMax,
                    paged = true,
                    observedByKind = seenByKind,
                    drained = drainSettlesThePast(walked, chunk, floor),
                )
            } else {
                bands.recordCut(key, url, ask.filter, seenByKind, walkedTop = chunk.until ?: (askedAtMs / 1000))
            }
        }
        return null
    }

    /** A walk through [pageThroughStalls]: the pager's last answer, and whether our own queue ended it. */
    private class Paged(
        val walked: PagedFetchResult,
        /**
         * The walk stopped on a stall of OURS — a hook parked in the full ingest queue when it went
         * quiet — that the pause could not get past. Decided when each stall is seen, before any
         * waiting: by the time a pause gives up, the hook may have got in while the queue stayed
         * above half, and reading `heldByUs` then filed our own stall as the relay's silence.
         */
        val ours: Boolean,
    )

    /**
     * [RelayReads.page], paused rather than ended while our own ingest queue holds the socket:
     * the walk waits for the queue to drain, then re-asks from the oldest second it reached, so a
     * burst costs one page again instead of the whole walk.
     *
     * BOUNDED, both ways a resume can fail to help. Each resume must start OLDER than the last
     * (or be the first): a burst denser in one `created_at` second than half the queue re-stalls
     * inside that second, and a page whose events are all implausible never moves the cursor, so
     * without the rule both re-asked the same ground forever, refilling the queue with
     * duplicates. And every pause of ONE walk draws on the one [pauseLimitMs] budget, never a
     * fresh one per pause. Either way the walk then ends as ours ([Paged.ours]).
     *
     * The count is of DISTINCT events: the resume is inclusive at the second it starts from, so
     * the relay serves that second's delivered events again, and they are dropped here instead of
     * being counted, credited to the relay and submitted a second time.
     */
    private suspend fun pageThroughStalls(
        url: NormalizedRelayUrl,
        chunk: Filter,
        ongoingVisit: OngoingVisit,
        onEvent: suspend (Event) -> Unit,
    ): Paged {
        val floor = BoundarySecond()
        var resumedAt: Long? = null
        val distinct: suspend (Event) -> Unit = { event ->
            if (!floor.repeats(event, resumedAt)) {
                onEvent(event)
                floor.delivered(event)
            }
        }
        var ask = chunk
        var pausedMs = 0L
        var paused = false
        // The pager's own counts, summed over the asks, less the re-served copies dropped above
        // (the pager counted them: to it they were delivered).
        var downloaded = 0
        while (true) {
            val page = reads.page(url, ask, NEG_IDLE_MS, distinct)
            downloaded += page.downloaded
            val walked = page.copy(downloaded = downloaded - floor.dropped())
            // A fact of this instant: whether a hook of ours holds the socket right now.
            if (!(stalledByUs(walked.end) && heldByUs(url))) return Paged(walked, ours = false)
            val cursor = ongoingVisit.pagingUntil
            // A limit'd ask is not resumed: a re-ask from the cursor would count its limit again.
            val moved = cursor != null && (resumedAt == null || cursor < resumedAt!!)
            if (chunk.limit != null || !moved) return Paged(walked, ours = true)
            val before = ongoingVisit.stage
            ongoingVisit.stage = Stage(before.pool, STAGE_PAUSED)
            val waited =
                try {
                    awaitIngestRoom(pauseLimitMs - pausedMs)
                } finally {
                    ongoingVisit.stage = before
                }
            // The wait was ours, not the relay's silence, whichever way it ended: left stale, the
            // visit's next ask read it as the relay quiet past `quietGiveUpMs` (abortedGaveUp).
            ongoingVisit.lastActivityMs = System.currentTimeMillis()
            if (waited == null) return Paged(walked, ours = true)
            pausedMs += waited
            if (!paused) counters.walksPausedByIngest.incrementAndGet()
            paused = true
            resumedAt = cursor
            // Inclusive: the second the walk stopped in is asked again, and [distinct] drops its repeats.
            ask = chunk.copy(until = cursor)
        }
    }

    /**
     * The events a walk delivered in the OLDEST second it reached — enough to recognise a
     * resume's re-served copies, at O(one second) of ids — and how many such copies it dropped.
     * Only plausible seconds count, as for the walk's cursor (`pagingUntil`), so the two name the
     * same second.
     * Locked: a hook that was parked when its page ended returns on its own coroutine, while the
     * resumed page delivers.
     */
    private class BoundarySecond {
        private var second = Long.MAX_VALUE
        private val ids = HashSet<String>()
        private var dropped = 0

        @Synchronized
        fun repeats(
            event: Event,
            resumedAt: Long?,
        ): Boolean {
            val repeat = resumedAt != null && event.createdAt == resumedAt && event.createdAt == second && event.id in ids
            if (repeat) dropped++
            return repeat
        }

        @Synchronized
        fun delivered(event: Event) {
            if (!SyncCoverage.isPlausible(event.createdAt)) return
            if (event.createdAt < second) {
                second = event.createdAt
                ids.clear()
            }
            if (event.createdAt == second) ids += event.id
        }

        /** Re-served copies dropped so far. */
        @Synchronized
        fun dropped(): Int = dropped
    }

    /**
     * Waits until the queue has room for a page, so the re-ask does not park again at once — and
     * with it the parked hook has got in, since a hook parks only on a full queue. Returns how long
     * it waited, or null when that would take longer than [budgetMs].
     */
    private suspend fun awaitIngestRoom(budgetMs: Long): Long? {
        val start = System.currentTimeMillis()
        while (!ingest.hasHeadroom()) {
            if (System.currentTimeMillis() - start >= budgetMs) return null
            delay(PAUSE_POLL_MS)
        }
        return System.currentTimeMillis() - start
    }

    /**
     * Runs the audit this ask is due, if any: the deleteMissing comparison for a retracting
     * stream, the plain history sweep otherwise. A relay the monitor measured as not answering
     * NEG-OPEN is skipped; unmeasured relays are still tried.
     */
    private suspend fun auditIfDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
        sharedAuthors: Set<String>,
        speaksNegentropy: Boolean?,
    ) {
        val tiers = ask.stream.negentropySchedule
        if (tiers.isEmpty()) return
        if (speaksNegentropy == false) {
            counters.auditsSkipped.incrementAndGet()
            return
        }
        val now = nowSeconds()
        // Dueness (read-only), then the cap, then the claim: any other order spends a permit or an
        // attempt on work that never runs. Youngest band first, one band per visit.
        val due =
            SyncTier.tile(tiers).firstOrNull { (tier, _, _) ->
                schedule.isDue(ask, url, tier.everySeconds, now, SyncTier.bandIdOf(tiers, tier))
            } ?: return
        val (tier, olderEdge, newerEdge) = due
        val band = SyncTier.bandIdOf(tiers, tier)
        val hold = limits.tryHold(ask.stream.name, POOL_NEGENTROPY) ?: return
        try {
            if (ask.stream.deleteMissing != DeleteMissing.OFF) {
                retractionIfDue(ask, url, tier.everySeconds, ongoingVisit, sharedAuthors, band)
            } else {
                sweepAudit(ask, url, tier, olderEdge, newerEdge, band, now, ongoingVisit)
            }
        } finally {
            hold.release()
        }
    }

    /**
     * Reconciles the ask's whole past in windows and downloads only the diff. The filter goes
     * to the pager verbatim: a sweep narrowed to what the band does not cover could never find
     * what a relay back-filled behind a catch-up.
     */
    private suspend fun sweepAudit(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        tier: SyncTier,
        olderEdge: Long?,
        newerEdge: Long?,
        band: String,
        now: Long,
        ongoingVisit: OngoingVisit,
    ) {
        val stream = ask.stream
        // Read before the claim, which would stamp this audit's own clock.
        val verifiedBefore = bands.verifiedAt(stream.name, url, ask.filter, band)
        if (!bands.claimAudit(stream.name, url, ask.filter, tier.everySeconds, now, band)) return
        val auditStarted = now
        var received = 0
        ongoingVisit.stage = NEGENTROPY
        val window = ask.filter.windowed(olderEdge, newerEdge, now)
        val acks = IngestAcks()
        val outcome =
            pager.sweep(
                // The cursor's identity. Band-qualified, or every band of a stream would resume
                // on one cursor: `SweepState.keyFor` strips `since`/`until`, which is all a band has.
                if (band.isEmpty()) stream.name else "${stream.name}#$band",
                url,
                // Shape stays the whole ask, so the cursor survives the window sliding with `now`.
                ask.filter,
                window,
                // A clean audit downloads nothing, so frames must count as activity.
                onProgress = { _, _ -> ongoingVisit.lastActivityMs = System.currentTimeMillis() },
                // The window's `since`: how far back the audit has got, like a paging cursor.
                onWindow = { since, _ ->
                    ongoingVisit.lastActivityMs = System.currentTimeMillis()
                    ongoingVisit.pagingUntil = since
                },
            ) { event ->
                received++
                arrived(url, ongoingVisit)
                if (ask.filter.match(event)) {
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions), acks.track())
                }
            }
        counters.auditsRun.incrementAndGet()
        counters.auditsRefusedWindows.addAndGet(outcome.refusedWindows.toLong())
        // The band's third state: no walk could start, so it is neither complete nor incomplete,
        // and recording that keeps it out of a backlog it could never leave.
        if (!outcome.negentropyUsable) {
            val why = outcome.failure?.detail ?: "the relay would not open negentropy"
            val strikes = bands.noteCannotReconcile(stream.name, url, ask.filter, band, why, auditStarted)
            if (strikes == SyncBands.STRIKES_BEFORE_IMPOSSIBLE) {
                counters.auditsCannotRun.incrementAndGet()
                System.err.println(
                    "router: audit ${stream.name}${if (band.isEmpty()) "" else " [$band]"} ${url.url} — the relay refused" +
                        " to open negentropy $strikes time(s) in a row ($why); this band is neither verified nor" +
                        " outstanding — negentropy CANNOT do it. Re-fetching covers it; re-tried in" +
                        " ${SyncBands.CANNOT_RECONCILE_TTL_SECONDS / 86_400}d.",
                )
            }
        } else if (outcome.reconciledWindows > 0) {
            // A window did reconcile, so the band is ordinary work again whatever it looked like before.
            bands.clearCannotReconcile(stream.name, url, ask.filter, band)
        }
        if (outcome.complete && storedAll(acks, stream.name, url)) {
            // The sweep stops `slackSeconds` short of its start, so the claim does too.
            bands.recordAudit(
                stream.name,
                url,
                ask.filter,
                window,
                reachesFloor = olderEdge == null,
                verifiedAt = auditStarted - pager.slackSeconds,
                band = band,
            )
        }
        System.err.println(
            "router: audit ${stream.name}${if (band.isEmpty()) "" else " [$band]"} ${url.url} — $received event(s) recovered, " +
                (
                    if (outcome.complete) {
                        "history verified"
                    } else if (outcome.negentropyUsable) {
                        "incomplete"
                    } else {
                        "incomplete — the relay would not open negentropy (${outcome.failure?.detail?.take(80)})"
                    }
                ) +
                (if (outcome.refusedWindows > 0) ", ${outcome.refusedWindows} window(s) REFUSED and not claimed" else "") +
                ", last verified ${verifiedBefore?.let { "${auditStarted - it}s ago" } ?: "never"}",
        )
    }

    /** [this] with the band's older edge as its floor: the ground below belongs to an older band. */
    private fun Filter.bandFloor(bandSince: Long?): Filter = if (bandSince == null) this else copy(since = maxOf(since ?: bandSince, bandSince))

    /** [this] intersected with a band's window, or null where nothing is left of it. */
    private fun Filter.clampedTo(
        bandSince: Long?,
        bandUntil: Long?,
    ): Filter? {
        val lo = listOfNotNull(since, bandSince).maxOrNull()
        val hi = listOfNotNull(until, bandUntil).minOrNull()
        if (lo != null && hi != null && lo > hi) return null
        return copy(since = lo, until = hi)
    }

    /**
     * This ask narrowed to one age band. The band's edges are intersected with whatever bounds
     * the ask already carries, so a band can only narrow a stream's window, never widen it past
     * what the stream asked for.
     */
    private fun Filter.windowed(
        olderEdge: Long?,
        newerEdge: Long?,
        now: Long,
    ): Filter = SyncTier.windowedFilter(this, now, olderEdge, newerEdge)

    /** The retraction audit for one ask, on the same clock as every other audit. */
    private suspend fun retractionIfDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
        sharedAuthors: Set<String>,
        band: String = "",
    ) {
        val retraction = retraction ?: return
        if (!retraction.claimAudit(ask.stream, url, ask.filter, negentropySyncThePastSeconds, band)) return
        ongoingVisit.stage = RETRACTING
        retraction.reconcileAndDelete(
            ask.stream,
            url,
            ask.filter,
            sharedAuthors,
            onActivity = { ongoingVisit.lastActivityMs = System.currentTimeMillis() },
        ) { arrived(url, ongoingVisit) }
        counters.retractionsRun.incrementAndGet()
    }

    companion object {
        /** Every down stream with declared urls or discovery sources rides the pool. */
        internal fun ridesThePool(stream: SyncStream): Boolean = stream.dir != SyncDirection.UP && (stream.urls.isNotEmpty() || stream.discovery?.sources?.isNotEmpty() == true)

        /**
         * Whether a walk ended in a way that makes the next leg futile: nothing delivered, and
         * an ending that is the relay declining rather than an empty page or our own limit.
         */
        internal fun refusedOutright(walked: PagedFetchResult): Boolean = walked.downloaded == 0 && !readToTheEnd(walked)

        /**
         * Whether a walk read its filter to the end: the relay drained it, or the filter's own
         * limit stopped it. Any other ending leaves ground below the oldest event unread.
         */
        internal fun readToTheEnd(walked: PagedFetchResult): Boolean =
            when (walked.end) {
                PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED -> true

                PagedFetchResult.End.IDLE, PagedFetchResult.End.CLOSED,
                PagedFetchResult.End.AUTH_REQUIRED, PagedFetchResult.End.CANNOT_CONNECT,
                PagedFetchResult.End.UNPAGEABLE,
                -> false
            }

        /**
         * The endings a socket parked in one of our hooks can manufacture: silence, and a first
         * page received but never counted as delivered.
         */
        internal fun stalledByUs(end: PagedFetchResult.End): Boolean =
            when (end) {
                PagedFetchResult.End.IDLE, PagedFetchResult.End.UNPAGEABLE -> true

                PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED,
                PagedFetchResult.End.CLOSED, PagedFetchResult.End.AUTH_REQUIRED,
                PagedFetchResult.End.CANNOT_CONNECT,
                -> false
            }

        /**
         * The tail's filters: the asks merged by shape, single-author asks folded into one
         * filter naming all their authors. Safe for the tail alone, because trust and heal are
         * re-derived per event.
         */
        internal fun tailFilters(
            asks: List<RosterBuilder.Ask>,
            since: Long,
        ): List<Filter> {
            val byShape = LinkedHashMap<String, MutableList<Filter>>()
            for (ask in asks) byShape.getOrPut(ask.filter.copy(authors = null).toJson()) { mutableListOf() } += ask.filter
            return byShape.values.map { group ->
                val unbound = group.firstOrNull { it.authors.isNullOrEmpty() }
                val authors = if (unbound != null) null else group.flatMap { it.authors.orEmpty() }.distinct().sorted()
                (unbound ?: group.first()).copy(authors = authors, since = since)
            }
        }

        const val DEFAULT_VISIT_CONCURRENCY = RouterConfig.DEFAULT_VISIT_CONCURRENCY

        /** How often a paused walk looks at the ingest queue again. */
        const val PAUSE_POLL_MS = 250L

        /** The `doing` sentences. Each names what the visit is for and how, since neither implies the other. */
        const val STAGE_PAGING = "catching up (paging)"
        const val STAGE_REFETCHING = "re-fetching the past (paging)"
        const val STAGE_NEGENTROPY = "negentropy sync of the past"
        const val STAGE_RETRACTING = "negentropy sync of the provider's own records"
        const val STAGE_TAILING = "holding a live tail"
        const val STAGE_CLAIMING = "claiming the socket"
        const val STAGE_ASKING = "checking what this ask still owes"
        const val STAGE_FINISHING = "draining queued heals, then the tail"
        const val STAGE_PAUSED = "paused: waiting for our ingest queue to drain"

        /** The pool's four workloads, as the `pool` word on every published row. */
        const val POOL_LIVE = "live"
        const val POOL_CATCHING_UP = "catching-up"
        const val POOL_REFETCHING = "re-fetching"
        const val POOL_NEGENTROPY = "negentropy"

        /** The fifth budgeted job: a stream's dial width. Appears in the limits, never on a row. */
        const val JOB_VISITING = "visiting"

        internal val CLAIMING = Stage(null, STAGE_CLAIMING)
        internal val ASKING = Stage(null, STAGE_ASKING)
        internal val CATCHING_UP = Stage(POOL_CATCHING_UP, STAGE_PAGING)
        internal val REFETCHING = Stage(POOL_REFETCHING, STAGE_REFETCHING)
        internal val NEGENTROPY = Stage(POOL_NEGENTROPY, STAGE_NEGENTROPY)
        internal val RETRACTING = Stage(POOL_NEGENTROPY, STAGE_RETRACTING)
        internal val FINISHING = Stage(null, STAGE_FINISHING)
        internal val TAILING = Stage(POOL_LIVE, STAGE_TAILING)

        /**
         * Whether a leg walks time the band already covers (a re-fetch) rather than time
         * outside it (a catch-up). Judged against the leg's own kinds, because a band holds one
         * span per kind; strict on both edges, because ordinary legs touch the band at its edges.
         */
        internal fun rewalksCovered(
            leg: Filter,
            covered: SyncCoverage.Band?,
        ): Boolean {
            if (covered == null || covered.spans.isEmpty()) return false
            val mine = leg.kinds?.mapNotNull { covered.spans[it] } ?: covered.spans.values.toList()
            if (mine.isEmpty()) return false
            val from = leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR
            val to = leg.until ?: Long.MAX_VALUE
            return from < mine.maxOf { it.max } && to > mine.minOf { it.min }
        }

        internal const val SCHEDULE_CACHE_MS = 60_000L

        /**
         * The sum of the streams' dial widths: fewer workers would leave a configured share
         * unreachable, more could never get a permit.
         */
        internal fun workersFor(streams: List<SyncStream>): Int =
            streams
                .sumOf { it.visitConcurrency ?: RouterConfig.UNCAPPED_STREAM_VISITS }
                .coerceAtLeast(1)

        /**
         * Warns once at boot when the streams' dials and tails together could crowd the OkHttp
         * dispatcher. An upper bound: a relay two streams want is one socket charged to both.
         */
        internal fun warnOnSocketBudget(streams: List<SyncStream>) {
            val dials = workersFor(streams)
            val tails = streams.sumOf { it.liveBudget }
            if (dials + tails <= SOCKET_HEADROOM) return
            System.err.println(
                "router: the streams together ask for up to $dials simultaneous dial(s) and $tails tail(s) — " +
                    "at most ${dials + tails} sockets against a dispatcher ceiling of $DISPATCHER_CEILING, which " +
                    "leaves little for the static upstreams, the monitor's probes and the healer. An UPPER bound: " +
                    "one relay two streams both want is one socket charged to both budgets, so the real count is " +
                    "lower by however much their rosters overlap. Lower `visitConcurrency` or `maxLiveConcurrency` on the " +
                    "streams that need them least",
            )
        }

        /** OkHttp's dispatcher ceiling, shared by every plane in this process. */
        internal const val DISPATCHER_CEILING = 1_024

        /** What the visit pool may take of it, leaving the other planes theirs. */
        internal const val SOCKET_HEADROOM = 900

        /**
         * The revisit delay a relay has earned. A tailed relay's base is longer, since its
         * revisit only serves the audit clock; recent yield shrinks the wait.
         */
        internal fun revisitDelayMs(
            yieldScore: Double,
            tailed: Boolean,
        ): Long {
            val base = if (tailed) REVISIT_TAILED_MS else REVISIT_UNTAILED_MS
            return (base / (1.0 + yieldScore / YIELD_HALVES_THE_WAIT)).toLong().coerceAtLeast(REVISIT_FLOOR_MS)
        }

        const val REVISIT_TAILED_MS = 30L * 60 * 1000
        const val REVISIT_UNTAILED_MS = 5L * 60 * 1000
        const val REVISIT_FLOOR_MS = 60_000L

        /** A visit turned away before dialling retries this soon; it cost nothing, so it earns no wait. */
        const val TURNED_AWAY_RETRY_MS = 5_000L

        const val YIELD_HALVES_THE_WAIT = 50.0

        const val YIELD_HALF_LIFE_MS = 60L * 60 * 1000

        const val EMPTY_ROSTER_RETRY_MS = 60_000L

        const val PHASE_FLUSH_MS = 1_000L

        /** Events land at a relay after their `created_at`, so a tail opened at `now` would miss some. */
        const val TAIL_OVERLAP_SECONDS = 60L
    }
}
