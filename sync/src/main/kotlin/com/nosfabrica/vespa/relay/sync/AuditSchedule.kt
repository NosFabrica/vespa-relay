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
import com.nosfabrica.vespa.relay.status.StreamPhases
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** When an ask's audit comes due, as one value: not scheduled, never run, or a time. */
@JvmInline
internal value class AuditClock private constructor(
    private val at: Long,
) {
    /** Whether this ask is audited at all. */
    val scheduled: Boolean get() = at != UNSCHEDULED

    /** Scheduled but never yet run, so always due. */
    val neverRun: Boolean get() = at == NEVER_RUN

    /** Due by [now]? Never for an unscheduled ask; always for one never run. */
    fun dueBy(now: Long): Boolean = scheduled && (neverRun || at <= now)

    /** The time it comes due, for a countdown; null where there is no one time to count down to. */
    val dueAt: Long? get() = if (!scheduled || neverRun) null else at

    companion object {
        private const val UNSCHEDULED = Long.MAX_VALUE
        private const val NEVER_RUN = Long.MIN_VALUE

        /** Nothing audits this ask: never due, and never a backlog either. */
        val NOT_SCHEDULED = AuditClock(UNSCHEDULED)

        /** Scheduled and never run. */
        val NEVER_AUDITED = AuditClock(NEVER_RUN)

        /** A time, or [NEVER_AUDITED] where the band has no clock yet. */
        fun of(dueAt: Long?): AuditClock = if (dueAt == null) NEVER_AUDITED else AuditClock(dueAt)
    }
}

/**
 * When each stream's scheduled re-reads of the past come due, and how much is waiting behind
 * them. The one place `deleteMissing` decides which clock schedules an ask.
 */
internal class AuditSchedule(
    private val streams: List<SyncStream>,
    private val bands: SyncBands,
    private val retraction: RetractionAudit?,
) {
    /**
     * Which clock schedules this ask's audit. A `deleteMissing` stream stamps its clock on the
     * ask's owned-kind projection, so the full ask's band clock would never advance.
     */
    fun clockFor(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
    ): AuditClock {
        if (ask.stream.deleteMissing == DeleteMissing.OFF) {
            return AuditClock.of(bands.auditDueAt(ask.stream.name, url, ask.filter, negentropySyncThePastSeconds))
        }
        val r = retraction ?: return AuditClock.NOT_SCHEDULED
        return r.auditClock(ask.stream, url, ask.filter, negentropySyncThePastSeconds)
    }

    /**
     * Would an audit run for this ask now, without stamping anything? Ignores
     * `attemptSpacingSeconds`, so `claimAudit` may still turn the ask away a moment later.
     */
    fun isDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        now: Long,
    ): Boolean = clockFor(ask, url, negentropySyncThePastSeconds).dueBy(now)

    /** One pass over the roster for every stream at once; the caller caches the result. */
    fun rows(
        roster: Map<NormalizedRelayUrl, Map<String, RosterBuilder.UnitAsks>>,
        nowSec: Long,
    ): Map<String, List<StreamPhases.Scheduled>> {
        val audits = HashMap<String, Tally>()
        val refetches = HashMap<String, Tally>()
        val refetchPeriods = HashMap<String, Long>()
        for ((url, byStream) in roster) {
            for ((name, unit) in byStream) {
                // Per stream, not per ask: the knob and the tally are the stream's.
                val refetching = refetchPeriods.getOrPut(name) { bands.refetchThePastSecondsFor(name) } != SyncBands.NEVER
                for (ask in unit.asks) {
                    ask.stream.negentropySyncThePastSeconds?.let { period ->
                        // An ask nothing schedules is left out rather than counted due forever.
                        val clock = clockFor(ask, url, period)
                        if (clock.scheduled) audits.getOrPut(name) { Tally(nowSec) }.add(clock)
                    }
                    if (refetching) {
                        refetches.getOrPut(name) { Tally(nowSec) }.add(AuditClock.of(bands.refetchDueAt(name, url, ask.filter)))
                    }
                }
            }
        }
        val rows = HashMap<String, List<StreamPhases.Scheduled>>()
        for (stream in streams) {
            val out = mutableListOf<StreamPhases.Scheduled>()
            stream.negentropySyncThePastSeconds?.let { period ->
                out += (audits[stream.name] ?: Tally(nowSec)).row(VisitPool.POOL_NEGENTROPY, period)
            }
            val refetchPeriod = bands.refetchThePastSecondsFor(stream.name)
            if (refetchPeriod != SyncBands.NEVER) {
                out += (refetches[stream.name] ?: Tally(nowSec)).row(VisitPool.POOL_REFETCHING, refetchPeriod)
            }
            rows[stream.name] = out
        }
        return rows
    }

    /** One job's asks sorted into due, never run and waiting, with the soonest of the waiting. */
    private class Tally(
        private val nowSec: Long,
    ) {
        var due = 0
        var neverRun = 0
        var waiting = 0
        var soonest: Long? = null

        fun add(clock: AuditClock) {
            val dueAt = clock.dueAt
            when {
                // Never audited: due by definition, and counted apart from the overdue.
                dueAt == null -> {
                    neverRun++
                }

                dueAt <= nowSec -> {
                    due++
                }

                else -> {
                    waiting++
                    soonest = minOf(soonest ?: dueAt, dueAt)
                }
            }
        }

        fun row(
            job: String,
            everySec: Long,
        ) = StreamPhases.Scheduled(
            job = job,
            everySec = everySec,
            due = due,
            neverRun = neverRun,
            waiting = waiting,
            // Absent when nothing is waiting; a 0 would read as "due now".
            nextInSec = soonest?.let { (it - nowSec).coerceAtLeast(0) },
        )
    }
}
