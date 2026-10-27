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

import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.status.RelayStatusReport
import com.nosfabrica.vespa.relay.status.StreamPhases
import com.nosfabrica.vespa.relay.sync.VisitPool.VisitKey
import com.nosfabrica.vespa.relay.util.nowSeconds
import java.util.concurrent.atomic.AtomicLong

/** What a row is doing: the pool word the page groups by, and the sentence it prints. */
internal class Stage(
    /** One of the `POOL_` constants, or null for a visit between jobs. */
    val pool: String?,
    val word: String,
)

/** One visit in progress, for the in-flight list; tails are listed separately. */
internal class OngoingVisit(
    val startedMs: Long,
) {
    @Volatile var stage: Stage = VisitPool.CLAIMING

    /** The `created_at` second the walk or audit window has reached. */
    @Volatile var pagingUntil: Long? = null

    val events = AtomicLong()

    /** Any sign of life: an event, an answered ask, a negentropy frame, a window opening. */
    @Volatile var lastActivityMs: Long = startedMs
}

/** The pool's running totals since boot, published on its progress row. */
internal class PoolCounters {
    val poolReceived = AtomicLong()
    val visitsRun = AtomicLong()

    /** Visits not dialled because the ingest queue was full at the claim. */
    val visitsHeldByIngest = AtomicLong()

    /** Walks our full ingest queue silenced that waited for it and resumed from their cursor, each counted once however often it paused. */
    val walksPausedByIngest = AtomicLong()
    val auditsRun = AtomicLong()

    /** Audits not attempted because the monitor measured the relay as not answering NEG-OPEN. */
    val auditsSkipped = AtomicLong()

    /** Windows an audit could not read and did not claim. */
    val auditsRefusedWindows = AtomicLong()

    /** Bands that read as impossible: the relay will not open negentropy for that ask. */
    val auditsCannotRun = AtomicLong()

    /** Retraction audits: the sweeps' clock and pool, counted apart because they are not sweeps. */
    val retractionsRun = AtomicLong()

    /** Walks that read their ground but did not record it, because an event of theirs failed to reach the store. */
    val bandsWithheld = AtomicLong()
}

/**
 * The pool as the status page reads it: the in-flight and live rows, the per-relay table, each
 * stream's limits, schedule and phase, and the progress counters. Reads the pool, never writes it.
 */
internal class PoolStatus(
    private val streams: List<SyncStream>,
    private val bands: SyncBands,
    private val limits: PoolLimits,
    private val widths: FilterWidths,
    private val schedule: AuditSchedule,
    private val retraction: RetractionAudit?,
    private val queue: VisitQueue<VisitKey>,
    private val tails: TailBook,
    private val ongoing: Map<VisitKey, OngoingVisit>,
    private val aborts: VisitAborts,
    private val counters: PoolCounters,
    /** The streams' rows in the progress document; null for the probes. */
    private val phases: StreamPhases?,
    private val roster: () -> RosterBuilder.Roster,
) {
    /**
     * The visits currently serving one stream, quietest first. Published whole: a row is a
     * worker, so the list is bounded by the worker count.
     */
    fun inFlightFor(stream: String): InFlight {
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

    /** Every (relay, stream) unit on the roster, for the status page's per-relay table, off one roster snapshot. */
    fun primeUnits(): List<RelayStatusReport.PrimeUnit> {
        val snapshot = roster()
        // One pass for the whole table; asked per row, it would scan the map once per row.
        val cannotReconcile = bands.cannotReconcileByUnit()
        val coverageKeys = streams.associate { it.name to SyncBands.coverageKeys(it) }
        // A banded stream's audit files its reconcile under the bare name, which no band key is.
        val auditKeys = streams.associate { it.name to it.name.takeIf { name -> name !in coverageKeys.getValue(name) } }
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
                        coverageKeys = coverageKeys[stream] ?: listOf(stream),
                        askKeys = unit.identity,
                        visiting = ongoing.containsKey(key),
                        live = tails.has(key),
                        speaksNegentropy = snapshot.speaksNegentropy[url],
                        // Any band of this unit that negentropy cannot walk; the per-band split
                        // is on the stream's schedule rows, which is where a band is the subject.
                        negentropyRefusedWhy = cannotReconcile[stream to url.url],
                        watched = snapshot.watches(url, stream),
                        kindCap = widths.capFor(url),
                        abortReason = abort?.reason?.says,
                        abortSaid = abort?.said,
                        abortAtSec = abort?.atSec ?: 0,
                        auditKey = auditKeys[stream],
                    )
            }
        }
        return out
    }

    /**
     * Every open tail subscription, quietest first, in the row shape of [inFlightFor].
     * Published whole: the set is bounded by the streams' live budgets.
     */
    fun livePool(): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            tails
                .open()
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
                        stage = VisitPool.TAILING.word,
                        pool = VisitPool.TAILING.pool,
                    )
                }.sortedWith(QUIETEST_FIRST)
        return InFlight(relays = rows, omitted = 0)
    }

    /** One stream's cap, use and deferrals per job, including the uncapped ones. */
    fun limitsFor(stream: String): List<StreamPhases.Limit> =
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

    /** When one stream's scheduled re-reads come due, cached for [VisitPool.SCHEDULE_CACHE_MS]. */
    fun scheduleFor(stream: String): List<StreamPhases.Scheduled> {
        val nowMs = System.currentTimeMillis()
        val cached = scheduleCache
        if (cached != null && nowMs - cached.atMs < VisitPool.SCHEDULE_CACHE_MS) return cached.rows[stream].orEmpty()
        val fresh = schedule.rows(roster().asks, nowSeconds())
        scheduleCache = ScheduleCache(nowMs, fresh)
        return fresh[stream].orEmpty()
    }

    /** Registers each stream's row readers on the phase document. */
    fun registerPhases() {
        for (stream in streams) {
            phases?.names(
                stream.name,
                inFlight = { inFlightFor(stream.name) },
                limits = { limitsFor(stream.name) },
                schedule = { scheduleFor(stream.name) },
            )
        }
    }

    /** Publishes each stream's relay, tail and queue counts, one walk per collection. */
    fun flushPhases() {
        val phases = phases ?: return
        val queuedByStream = queue.waitingBy { it.stream }
        val tailedByStream =
            tails
                .open()
                .keys
                .groupingBy { it.stream }
                .eachCount()
        val relaysByStream = HashMap<String, Int>()
        for (byStream in roster().asks.values) {
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

    /** The pool's progress row. Names must not collide with anything else on the progress document. */
    fun counts(): List<Processors.Count> {
        val asks = roster().asks
        return listOf(
            Processors.Count("roster", asks.size.toLong()),
            // Units of work, not relays: `visiting + awaitingVisit + between` sums to this.
            Processors.Count("rosterVisits", asks.values.sumOf { it.size }.toLong()),
            Processors.Count("awaitingVisit", queue.waiting.toLong()),
            Processors.Count("visiting", queue.visiting.toLong()),
            Processors.Count("liveHeld", tails.size.toLong()),
            Processors.Count("visitsRun", counters.visitsRun.get()),
            Processors.Count("visitsHeldByIngest", counters.visitsHeldByIngest.get()),
            Processors.Count("walksPausedByIngest", counters.walksPausedByIngest.get()),
            Processors.Count("bandsWithheld", counters.bandsWithheld.get()),
            Processors.Count("negentropyRunning", ongoing.values.count { it.stage.pool == VisitPool.POOL_NEGENTROPY }.toLong()),
            Processors.Count("negentropyRuns", counters.auditsRun.get()),
            Processors.Count("negentropySkipped", counters.auditsSkipped.get()),
            Processors.Count("negentropyRefused", counters.auditsRefusedWindows.get()),
            Processors.Count("negentropyCannotRun", counters.auditsCannotRun.get()),
            Processors.Count("retractionRuns", counters.retractionsRun.get()),
            Processors.Count("retracted", retraction?.deleted?.get() ?: 0L),
            Processors.Count("liveEvicted", tails.evictions.get()),
            Processors.Count("poolReceived", counters.poolReceived.get()),
            Processors.Count("narrowedRelays", widths.narrowed.toLong()),
        ) + aborts.counts()
    }

    companion object {
        /** Quietest first, then longest held, then url. The front end applies the same order. */
        private val QUIETEST_FIRST =
            compareByDescending<InFlight.Relay> { it.quietForSec }
                .thenByDescending { it.heldForSec }
                .thenBy { it.relay }
    }
}
