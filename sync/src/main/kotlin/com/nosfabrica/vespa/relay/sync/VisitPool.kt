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
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.status.RelayStatusReport
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
import java.util.concurrent.atomic.AtomicInteger
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
    /** Decides what to sync; the pool asks it to [RosterBuilder.rebuild] on the roster clock. */
    private val rosterBuilder: RosterBuilder,
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

    /** One held live subscription and the socket claim that rides with it. */
    private class Tail(
        val subId: String,
        /** The roster's ask identity when the tail was opened; a change re-opens the tail. */
        val wantsAtOpen: Set<String>,
        /** The kind cap the tail was opened at; a newly learned cap re-opens the tail. */
        val capAtOpen: Int? = null,
        val openedMs: Long = System.currentTimeMillis(),
        /** The live permit, held for the tail's life and released by [dropTail]. */
        val hold: PoolLimits.Hold,
    ) {
        val events = AtomicLong()

        @Volatile var lastEventMs: Long = openedMs
    }

    private val tails = ConcurrentHashMap<VisitKey, Tail>()
    private val tailSeq = AtomicInteger()

    /**
     * What a relay has delivered lately: a score that halves every [YIELD_HALF_LIFE_MS].
     * Decides which relays keep tails when the budget is short and how soon a relay is revisited.
     */
    private class Yield {
        val arrived = AtomicLong()

        @Volatile var score: Double = 0.0

        @Volatile var foldedAtMs: Long = System.currentTimeMillis()

        /** Folds what arrived into the decayed score and returns it. Racy by design: a priority hint. */
        fun foldedScore(nowMs: Long): Double {
            val fresh = arrived.getAndSet(0)
            val dtMs = (nowMs - foldedAtMs).coerceAtLeast(0)
            if (fresh > 0 || dtMs > YIELD_HALF_LIFE_MS / 8) {
                score = score * Math.pow(0.5, dtMs.toDouble() / YIELD_HALF_LIFE_MS) + fresh
                foldedAtMs = nowMs
            }
            return score
        }
    }

    private val yields = ConcurrentHashMap<NormalizedRelayUrl, Yield>()

    private fun yieldOf(url: NormalizedRelayUrl): Yield = yields.getOrPut(url) { Yield() }

    /**
     * Is a producer of ours parked in the full ingest queue on this relay's events? A parked
     * hook silences every subscription on that socket.
     */
    private fun heldByUs(url: NormalizedRelayUrl): Boolean = ingest.parkedOn(url) > 0

    /** Visits not dialled because the ingest queue was full at the claim. */
    private val visitsHeldByIngest = AtomicLong()

    /** What a row is doing: the pool word the page groups by, and the sentence it prints. */
    private class Stage(
        /** One of the `POOL_` constants, or null for a visit between jobs. */
        val pool: String?,
        val word: String,
    )

    /** One visit in progress, for the in-flight list; tails are listed separately. */
    private class OngoingVisit(
        val startedMs: Long,
    ) {
        @Volatile var stream: String? = null

        @Volatile var stage: Stage = CLAIMING

        /** The `created_at` second the walk or audit window has reached. */
        @Volatile var pagingUntil: Long? = null

        val events = AtomicLong()

        /** Any sign of life: an event, a negentropy frame, a window opening. */
        @Volatile var lastActivityMs: Long = startedMs
    }

    private val ongoing = ConcurrentHashMap<VisitKey, OngoingVisit>()

    /** Counts one arrived event: the pool, the relay's yield, and the visit or tail it came by. */
    private fun arrived(
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit?,
        tail: Tail? = null,
    ) {
        poolReceived.incrementAndGet()
        yieldOf(url).arrived.incrementAndGet()
        ongoingVisit?.let {
            it.events.incrementAndGet()
            it.lastActivityMs = System.currentTimeMillis()
        }
        tail?.let {
            it.events.incrementAndGet()
            it.lastEventMs = System.currentTimeMillis()
        }
    }

    /**
     * The visits currently serving one stream, quietest first. Published whole: a row is a
     * worker, so the list is bounded by the worker count.
     */
    private fun inFlightFor(stream: String): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            ongoing
                .entries
                .filter { it.key.stream == stream }
                .map { (key, row) ->
                    val heldForSec = ((nowMs - row.startedMs) / 1000).coerceAtLeast(0)
                    InFlight.Relay(
                        relay = key.url.url,
                        heldForSec = heldForSec,
                        // A visit holds its socket from its first moment, so the two clocks agree.
                        transferringForSec = heldForSec,
                        events = row.events.get(),
                        quietForSec = ((nowMs - row.lastActivityMs) / 1000).coerceAtLeast(0),
                        stage = row.stage.word,
                        pool = row.stage.pool,
                        pagingUntil = row.pagingUntil,
                    )
                }.sortedWith(QUIETEST_FIRST)
        return InFlight(relays = rows, omitted = 0)
    }

    /**
     * Every (relay, stream) unit on the roster, for the status page's per-relay table, read
     * off one roster snapshot.
     */
    internal fun primeUnits(): List<RelayStatusReport.PrimeUnit> {
        val snapshot = currentRoster
        val out = ArrayList<RelayStatusReport.PrimeUnit>(snapshot.asks.size)
        for ((url, byStream) in snapshot.asks) {
            for ((stream, unit) in byStream) {
                val key = VisitKey(url, stream)
                val abort = aborts.last(stream, url)
                out +=
                    RelayStatusReport.PrimeUnit(
                        relay = url.url,
                        stream = stream,
                        // The same strings the bands are keyed under, so the report joins on them.
                        askKeys = unit.identity,
                        visiting = ongoing.containsKey(key),
                        live = tails.containsKey(key),
                        speaksNegentropy = snapshot.speaksNegentropy[url],
                        kindCap = widths.capFor(url),
                        abortReason = abort?.reason?.says,
                        abortSaid = abort?.said,
                        abortAtSec = abort?.atSec ?: 0,
                    )
            }
        }
        return out
    }

    /**
     * Every open tail subscription, quietest first, in the row shape of [inFlightFor].
     * Published whole: the set is bounded by the streams' live budgets.
     */
    internal fun livePool(): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            tails
                .entries
                .map { (key, tail) ->
                    val heldForSec = ((nowMs - tail.openedMs) / 1000).coerceAtLeast(0)
                    InFlight.Relay(
                        relay = key.url.url,
                        stream = key.stream,
                        heldForSec = heldForSec,
                        // A tail holds its socket for its whole life, so the two clocks agree.
                        transferringForSec = heldForSec,
                        events = tail.events.get(),
                        quietForSec = ((nowMs - tail.lastEventMs) / 1000).coerceAtLeast(0),
                        stage = TAILING.word,
                        pool = TAILING.pool,
                    )
                }.sortedWith(QUIETEST_FIRST)
        return InFlight(relays = rows, omitted = 0)
    }

    /** One stream's cap, use and deferrals per job, including the uncapped ones. */
    private fun limitsFor(stream: String): List<StreamPhases.Limit> =
        PoolLimits.JOBS.map { (job, _) ->
            StreamPhases.Limit(
                job = job,
                cap = limits.capFor(stream, job),
                inUse = limits.heldBy(stream, job),
                deferred = limits.deferred(stream, job),
            )
        }

    private class ScheduleCache(
        val atMs: Long,
        val rows: Map<String, List<StreamPhases.Scheduled>>,
    )

    @Volatile
    private var scheduleCache: ScheduleCache? = null

    /** When one stream's scheduled re-reads come due, cached for [SCHEDULE_CACHE_MS]. */
    private fun scheduleFor(stream: String): List<StreamPhases.Scheduled> {
        val nowMs = System.currentTimeMillis()
        val cached = scheduleCache
        if (cached != null && nowMs - cached.atMs < SCHEDULE_CACHE_MS) return cached.rows[stream].orEmpty()
        val fresh = schedule.rows(currentRoster.asks, nowSeconds())
        scheduleCache = ScheduleCache(nowMs, fresh)
        return fresh[stream].orEmpty()
    }

    private fun phasesChanged() {
        phasesDirty.set(true)
    }

    /** Publishes each stream's relay, tail and queue counts, one walk per collection. */
    private fun flushPhases() {
        val phases = phases ?: return
        val queuedByStream = queue.waitingBy { it.stream }
        val tailedByStream = tails.keys.groupingBy { it.stream }.eachCount()
        val relaysByStream = HashMap<String, Int>()
        for (byStream in currentRoster.asks.values) {
            for (name in byStream.keys) relaysByStream.merge(name, 1, Int::plus)
        }
        for (stream in streams) {
            phases.set(
                stream.name,
                StreamPhases.Phase.Rotating(
                    relays = relaysByStream[stream.name] ?: 0,
                    tailed = tailedByStream[stream.name] ?: 0,
                    queued = queuedByStream[stream.name] ?: 0,
                ),
            )
        }
    }

    private val phasesDirty =
        java.util.concurrent.atomic
            .AtomicBoolean(false)

    private val evictedTails = AtomicLong()

    private val poolReceived = AtomicLong()
    private val visitsRun = AtomicLong()
    private val auditsRun = AtomicLong()

    /** Audits not attempted because the monitor measured the relay as not answering NEG-OPEN. */
    private val auditsSkipped = AtomicLong()

    /** Why visits end early, counted by reason. */
    private val aborts = VisitAborts()

    /** Windows an audit could not read and did not claim. */
    private val auditsRefusedWindows = AtomicLong()

    fun start() {
        if (streams.isEmpty()) return
        warnOnSocketBudget(streams)
        progress.phase("rotating")
        progress.counts {
            // Names must not collide with anything else on the progress document.
            listOf(
                Processors.Count("roster", roster.size.toLong()),
                // Units of work, not relays: `visiting + awaitingVisit + between` sums to this.
                Processors.Count(
                    "rosterVisits",
                    currentRoster.asks.values
                        .sumOf { it.size }
                        .toLong(),
                ),
                Processors.Count("awaitingVisit", queue.waiting.toLong()),
                Processors.Count("visiting", queue.visiting.toLong()),
                Processors.Count("liveHeld", tails.size.toLong()),
                Processors.Count("visitsRun", visitsRun.get()),
                Processors.Count("visitsHeldByIngest", visitsHeldByIngest.get()),
                Processors.Count("negentropyRunning", ongoing.values.count { it.stage.pool == POOL_NEGENTROPY }.toLong()),
                Processors.Count("negentropyRuns", auditsRun.get()),
                Processors.Count("negentropySkipped", auditsSkipped.get()),
                Processors.Count("negentropyRefused", auditsRefusedWindows.get()),
                Processors.Count("retracted", retraction?.deleted?.get() ?: 0L),
                Processors.Count("liveEvicted", evictedTails.get()),
                Processors.Count("poolReceived", poolReceived.get()),
                Processors.Count("narrowedRelays", widths.narrowed.toLong()),
            ) + aborts.counts()
        }
        for (stream in streams) {
            phases?.names(
                stream.name,
                inFlight = { inFlightFor(stream.name) },
                limits = { limitsFor(stream.name) },
                schedule = { scheduleFor(stream.name) },
            )
        }
        flushPhases()
        scope.launch { rosterLoop() }
        scope.launch {
            while (scope.isActive) {
                delay(PHASE_FLUSH_MS)
                if (phasesDirty.getAndSet(false)) flushPhases()
            }
        }
        repeat(workers) {
            scope.launch {
                queue.visitLoop(
                    stillWanted = { key -> wantedBy(currentRoster, key) },
                    // Read, never getOrPut: a finishing visit must not resurrect a pruned yield.
                    revisitDelayMs = { key ->
                        revisitDelayMs(yields[key.url]?.foldedScore(System.currentTimeMillis()) ?: 0.0, tails.containsKey(key))
                    },
                    visit = ::guardedVisit,
                )
            }
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
        // Per unit: a relay can leave one stream's roster and stay on another's.
        for (key in tails.keys.filter { !wantedBy(built, it) }) {
            dropTail(key)
        }
        for (url in previous.keys - next.keys) {
            yields.remove(url)
        }
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
    }

    /** One visit with its failure recorded as an abort. */
    private suspend fun guardedVisit(key: VisitKey) {
        try {
            visit(key)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            aborts
                .record(
                    key.stream,
                    key.url,
                    VisitAborts.Reason.FAILED,
                    asked = "${e.javaClass.simpleName}: ${e.message?.take(80)}",
                    said = null,
                )?.let(System.err::println)
        }
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
        // socket; skipped like a refused permit, and the revisit brings it back.
        if (ingest.isFull()) {
            visitsHeldByIngest.incrementAndGet()
            return
        }
        // Taken before the socket claim, so `visitConcurrency` bounds simultaneous dials.
        val permit = limits.tryHold(key.stream, JOB_VISITING) ?: return
        visitsRun.incrementAndGet()
        val ongoingVisit = OngoingVisit(System.currentTimeMillis())
        ongoingVisit.stream = key.stream
        ongoing[key] = ongoingVisit
        sockets.claim(url)
        try {
            for (ask in wanted) {
                // Give up on silence, not on a deadline: a delivering visit is never cut.
                if (System.currentTimeMillis() - ongoingVisit.lastActivityMs > LEG_QUIET_GIVE_UP_MS) {
                    aborts
                        .record(
                            key.stream,
                            url,
                            VisitAborts.Reason.GAVE_UP,
                            asked = "${LEG_QUIET_GIVE_UP_MS / 60_000} quiet minute(s), ${VisitAborts.asked(ask.filter)}",
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
            openTail(key)
        } finally {
            ongoing.remove(key)
            sockets.release(url)
            // The heal drain and the tail open still count as visiting.
            permit.release()
        }
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
        val stream = ask.stream
        // Read before the first `record` below widens it; it tells a catch-up from a re-fetch.
        val covered = bands.band(stream.name, url, ask.filter)
        for (leg in bands.legs(stream.name, url, ask.filter)) {
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
                    val refusal = walkLeg(ask, url, flooredLeg, ongoingVisit) ?: break
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
     * Walks one leg as the REQs the relay will take: itself, or its kinds in chunks. Returns
     * the first refused chunk, or null. Each chunk records its own band, which is safe because
     * bands are per kind and chunking splits on kinds and nothing else.
     */
    private suspend fun walkLeg(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        flooredLeg: Filter,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val stream = ask.stream
        for (chunk in widths.chunk(url, flooredLeg)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            // Per chunk: the depth only decreases, so a stale value would hide a shallower walk.
            ongoingVisit.pagingUntil = null
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
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions))
                }
            }
            // Stamped before the REQ goes out: a complaint older than this belongs to an earlier ask.
            val askedAtMs = System.currentTimeMillis()
            val sampling = pages.arm(url, chunk)
            val walked =
                try {
                    reads.page(url, chunk, NEG_IDLE_MS, onEvent)
                } finally {
                    pages.free(sampling)
                }
            if (refusedOutright(walked)) {
                // No band for a refused chunk: nothing was walked. Whose refusal
                // is decided now: the parked hook is a fact of this instant.
                return Refusal(
                    walked.end,
                    chunk,
                    askedAtMs,
                    pages.render(sampling, walked.downloaded),
                    ours = stalledByUs(walked.end) && heldByUs(url),
                )
            }
            bands.record(
                stream.name,
                url,
                ask.filter,
                seenMin,
                seenMax,
                paged = true,
                observedByKind = seenByKind,
                drained = drainSettlesThePast(walked, chunk, ask.filter),
            )
        }
        return null
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
        val negentropySyncThePastSeconds = ask.stream.negentropySyncThePastSeconds ?: return
        if (speaksNegentropy == false) {
            auditsSkipped.incrementAndGet()
            return
        }
        // Dueness first (read-only), then the cap, then the claim, which stamps the attempt clock.
        // Any other order spends a permit or an attempt on work that never runs.
        if (!schedule.isDue(ask, url, negentropySyncThePastSeconds, nowSeconds())) return
        val hold = limits.tryHold(ask.stream.name, POOL_NEGENTROPY) ?: return
        try {
            if (ask.stream.deleteMissing != DeleteMissing.OFF) {
                retractionIfDue(ask, url, negentropySyncThePastSeconds, ongoingVisit, sharedAuthors)
            } else {
                sweepAudit(ask, url, negentropySyncThePastSeconds, ongoingVisit)
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
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
    ) {
        val stream = ask.stream
        val now = nowSeconds()
        // Read before the claim, which would stamp this audit's own clock.
        val verifiedBefore = bands.verifiedAt(stream.name, url, ask.filter)
        if (!bands.claimAudit(stream.name, url, ask.filter, negentropySyncThePastSeconds, now)) return
        val auditStarted = now
        var received = 0
        ongoingVisit.stage = NEGENTROPY
        val outcome =
            pager.sweep(
                stream.name,
                url,
                ask.filter,
                ask.filter,
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
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions))
                }
            }
        auditsRun.incrementAndGet()
        auditsRefusedWindows.addAndGet(outcome.refusedWindows.toLong())
        if (outcome.complete) {
            // The sweep stops `slackSeconds` short of its start, so the claim does too.
            bands.record(
                stream.name,
                url,
                ask.filter,
                observedMin = null,
                observedMax = null,
                paged = false,
                reconciledThrough = auditStarted - pager.slackSeconds,
            )
        }
        System.err.println(
            "router: audit ${stream.name} ${url.url} — $received event(s) recovered, " +
                (if (outcome.complete) "history verified" else "incomplete (negentropy usable: ${outcome.negentropyUsable})") +
                (if (outcome.refusedWindows > 0) ", ${outcome.refusedWindows} window(s) REFUSED and not claimed" else "") +
                ", last verified ${verifiedBefore?.let { "${auditStarted - it}s ago" } ?: "never"}",
        )
    }

    /** The retraction audit for one ask, on the same clock as every other audit. */
    private suspend fun retractionIfDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
        sharedAuthors: Set<String>,
    ) {
        val retraction = retraction ?: return
        if (!retraction.claimAudit(ask.stream, url, ask.filter, negentropySyncThePastSeconds)) return
        ongoingVisit.stage = RETRACTING
        retraction.reconcileAndDelete(
            ask.stream,
            url,
            ask.filter,
            sharedAuthors,
            onActivity = { ongoingVisit.lastActivityMs = System.currentTimeMillis() },
        ) { arrived(url, ongoingVisit) }
        auditsRun.incrementAndGet()
    }

    /**
     * Opens this unit's live tail, `since` [TAIL_OVERLAP_SECONDS] behind now so the seam with
     * the catch-up cannot drop an event. A sitting tail whose asks or kind cap changed is
     * re-opened. The socket claim lives until the roster drops the unit.
     */
    private suspend fun openTail(key: VisitKey) {
        val url = key.url
        val snapshot = currentRoster
        val urlAsks = asksFor(snapshot, key)
        if (urlAsks.isEmpty()) return
        val wantsNow = snapshot.asks[url]?.get(key.stream)?.identity ?: return
        val capNow = widths.capFor(url)
        val sitting = tails[key]
        if (sitting != null) {
            if (sitting.wantsAtOpen == wantsNow && sitting.capAtOpen == capNow) return
            dropTail(key)
        }
        // A spare permit within the stream's budget, or one earned by evicting its weakest tail.
        // `trySpare` does not count a deferral: a full live budget is normal, not refused work.
        val hold = limits.trySpare(key.stream, POOL_LIVE) ?: earnTail(key) ?: return
        val subId = "visit-tail-${tailSeq.incrementAndGet()}"
        // Built before the listener closes over it, so the first burst lands on the counters.
        val tail = Tail(subId, wantsNow, capAtOpen = capNow, hold = hold)

        // Every way out before the publish hands back exactly what was taken.
        fun abandon(untail: Boolean = false) {
            if (untail) reads.untail(subId)
            sockets.release(url)
            hold.release()
        }
        // Claim and subscribe before publishing, so a concurrent dropTail meets a fully formed tail.
        sockets.claim(url)
        try {
            reads.tail(subId, url, widths.chunkAll(url, tailFilters(urlAsks, nowSeconds() - TAIL_OVERLAP_SECONDS))) { event ->
                arrived(url, ongoingVisit = null, tail = tail)
                // Scope is re-checked per event against the live roster, so a relay cannot widen ingest.
                var any = false
                var allTrusted = true
                var healContent = false
                var healRetractions = false
                for (ask in currentRoster.asks[url]
                    ?.get(key.stream)
                    ?.asks
                    .orEmpty()) {
                    if (!ask.filter.match(event)) continue
                    any = true
                    allTrusted = allTrusted && ask.stream.trusted
                    healContent = healContent || ask.stream.healContent
                    healRetractions = healRetractions || ask.stream.healRetractions
                }
                if (any) ingest.submit(event, allTrusted, IngestOrigin(url, healContent, healRetractions))
            }
        } catch (e: CancellationException) {
            abandon()
            throw e
        } catch (e: Exception) {
            abandon()
            System.err.println("router: tail ${key.stream} ${url.url} failed to open: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            return
        }
        if (tails.putIfAbsent(key, tail) != null) {
            abandon(untail = true)
            return
        }
        // The roster may have dropped this unit between the read above and the publish.
        if (!wantedBy(currentRoster, key)) {
            dropTail(key)
            return
        }
        phasesChanged()
    }

    /**
     * Earns this unit a live permit by evicting the same stream's weakest tail, or returns
     * null. The candidate must win on yield, not tie, so a pool of equals does not churn.
     */
    private fun earnTail(candidate: VisitKey): PoolLimits.Hold? {
        val nowMs = System.currentTimeMillis()
        val mine = yieldOf(candidate.url).foldedScore(nowMs)
        // One fold per tail: `foldedScore` drains what arrived, so a second read differs.
        var weakest: VisitKey? = null
        var weakestScore = Double.MAX_VALUE
        for (key in tails.keys) {
            if (key == candidate || key.stream != candidate.stream) continue
            val score = yieldOf(key.url).foldedScore(nowMs)
            if (score < weakestScore) {
                weakest = key
                weakestScore = score
            }
        }
        if (weakest == null || weakestScore >= mine) return null
        evictedTails.incrementAndGet()
        dropTail(weakest)
        if (wantedBy(currentRoster, weakest)) queue.offer(weakest)
        return limits.tryHold(candidate.stream, POOL_LIVE)
    }

    private fun dropTail(key: VisitKey) {
        val tail = tails.remove(key) ?: return
        reads.untail(tail.subId)
        sockets.release(key.url)
        tail.hold.release()
        // The revisit timer was armed on the tailed cadence; let the next visit arm the untailed one.
        queue.disarm(key)
        phasesChanged()
    }

    companion object {
        /** Every down stream with declared urls or discovery sources rides the pool. */
        internal fun ridesThePool(stream: SyncStream): Boolean = stream.dir != SyncDirection.UP && (stream.urls.isNotEmpty() || stream.discovery?.sources?.isNotEmpty() == true)

        /**
         * Whether a walk ended in a way that makes the next leg futile: nothing delivered, and
         * an ending that is the relay declining rather than an empty page or our own limit.
         */
        internal fun refusedOutright(walked: PagedFetchResult): Boolean =
            walked.downloaded == 0 &&
                when (walked.end) {
                    PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED -> false

                    PagedFetchResult.End.IDLE, PagedFetchResult.End.CLOSED,
                    PagedFetchResult.End.AUTH_REQUIRED, PagedFetchResult.End.CANNOT_CONNECT,
                    PagedFetchResult.End.UNPAGEABLE,
                    -> true
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

        /** Quietest first, then longest held, then url. The front end applies the same order. */
        private val QUIETEST_FIRST =
            compareByDescending<InFlight.Relay> { it.quietForSec }
                .thenByDescending { it.heldForSec }
                .thenBy { it.relay }

        const val DEFAULT_VISIT_CONCURRENCY = RouterConfig.DEFAULT_VISIT_CONCURRENCY

        /** The `doing` sentences. Each names what the visit is for and how, since neither implies the other. */
        const val STAGE_PAGING = "catching up (paging)"
        const val STAGE_REFETCHING = "re-fetching the past (paging)"
        const val STAGE_NEGENTROPY = "negentropy sync of the past"
        const val STAGE_RETRACTING = "negentropy sync of the provider's own records"
        const val STAGE_TAILING = "holding a live tail"
        const val STAGE_CLAIMING = "claiming the socket"
        const val STAGE_ASKING = "checking what this ask still owes"
        const val STAGE_FINISHING = "draining queued heals, then the tail"

        /** The pool's four workloads, as the `pool` word on every published row. */
        const val POOL_LIVE = "live"
        const val POOL_CATCHING_UP = "catching-up"
        const val POOL_REFETCHING = "re-fetching"
        const val POOL_NEGENTROPY = "negentropy"

        /** The fifth budgeted job: a stream's dial width. Appears in the limits, never on a row. */
        const val JOB_VISITING = "visiting"

        private val CLAIMING = Stage(null, STAGE_CLAIMING)
        private val ASKING = Stage(null, STAGE_ASKING)
        private val CATCHING_UP = Stage(POOL_CATCHING_UP, STAGE_PAGING)
        private val REFETCHING = Stage(POOL_REFETCHING, STAGE_REFETCHING)
        private val NEGENTROPY = Stage(POOL_NEGENTROPY, STAGE_NEGENTROPY)
        private val RETRACTING = Stage(POOL_NEGENTROPY, STAGE_RETRACTING)
        private val FINISHING = Stage(null, STAGE_FINISHING)
        private val TAILING = Stage(POOL_LIVE, STAGE_TAILING)

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

        const val DEFAULT_MAX_LIVE_CONCURRENCY = RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY

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

        const val YIELD_HALVES_THE_WAIT = 50.0

        const val YIELD_HALF_LIFE_MS = 60L * 60 * 1000

        const val EMPTY_ROSTER_RETRY_MS = 60_000L

        const val PHASE_FLUSH_MS = 1_000L

        /** Events land at a relay after their `created_at`, so a tail opened at `now` would miss some. */
        const val TAIL_OVERLAP_SECONDS = 60L
    }
}
