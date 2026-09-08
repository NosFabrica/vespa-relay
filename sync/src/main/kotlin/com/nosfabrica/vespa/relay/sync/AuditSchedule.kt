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
import com.nosfabrica.vespa.relay.config.SyncTier
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
        band: String = "",
    ): AuditClock {
        if (ask.stream.deleteMissing == DeleteMissing.OFF) {
            return AuditClock.of(bands.auditDueAt(ask.stream.name, url, ask.filter, negentropySyncThePastSeconds, band))
        }
        val r = retraction ?: return AuditClock.NOT_SCHEDULED
        return r.auditClock(ask.stream, url, ask.filter, negentropySyncThePastSeconds, band)
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
        band: String = "",
    ): Boolean = clockFor(ask, url, negentropySyncThePastSeconds, band).dueBy(now)

    /** One pass over the roster for every stream at once; the caller caches the result. */
    fun rows(
        roster: Map<NormalizedRelayUrl, Map<String, RosterBuilder.UnitAsks>>,
        nowSec: Long,
    ): Map<String, List<StreamPhases.Scheduled>> {
        // Keyed by stream and the band inside it: a banded stream gets a row per band, and its
        // bands are on different clocks, so one tally over all of them would report neither.
        val audits = HashMap<Pair<String, String>, Tally>()
        val refetches = HashMap<Pair<String, String>, Tally>()
        val byName = streams.associateBy { it.name }
        for ((url, byStream) in roster) {
            for ((name, unit) in byStream) {
                val stream = byName[name] ?: continue
                val auditTiers = stream.negentropySchedule
                val refetchTiers = refetchBandsOf(stream)
                for (ask in unit.asks) {
                    for (tier in auditTiers) {
                        val band = SyncTier.bandIdOf(auditTiers, tier)
                        // An ask nothing schedules is left out rather than counted due forever.
                        val clock = clockFor(ask, url, tier.everySeconds, band)
                        if (clock.scheduled) audits.getOrPut(name to band) { Tally(nowSec) }.add(clock)
                    }
                    for ((tier, older, newer) in SyncTier.tile(refetchTiers)) {
                        val key = SyncTier.keyFor(name, refetchTiers, tier)
                        // Per band, not per stream: the knob and the tally are the band's.
                        if (bands.refetchThePastSecondsFor(key) == SyncBands.NEVER) continue
                        val bandAsk = SyncTier.windowedFilter(ask.filter, nowSec, older, newer)
                        refetches.getOrPut(name to tier.id) { Tally(nowSec) }.add(AuditClock.of(bands.refetchDueAt(key, url, bandAsk)))
                    }
                }
            }
        }
        val rows = HashMap<String, List<StreamPhases.Scheduled>>()
        for (stream in streams) {
            val out = mutableListOf<StreamPhases.Scheduled>()
            val auditTiers = stream.negentropySchedule
            for (tier in auditTiers) {
                val band = SyncTier.bandIdOf(auditTiers, tier)
                out += (audits[stream.name to band] ?: Tally(nowSec)).row(jobOf(VisitPool.POOL_NEGENTROPY, band), tier.everySeconds)
            }
            val refetchTiers = refetchBandsOf(stream)
            for (tier in refetchTiers) {
                val key = SyncTier.keyFor(stream.name, refetchTiers, tier)
                val period = bands.refetchThePastSecondsFor(key)
                if (period == SyncBands.NEVER) continue
                val band = SyncTier.bandIdOf(refetchTiers, tier)
                out += (refetches[stream.name to tier.id] ?: Tally(nowSec)).row(jobOf(VisitPool.POOL_REFETCHING, band), period)
            }
            rows[stream.name] = out
        }
        return rows
    }

    /**
     * A stream's re-fetch bands as the pool walks them: its own, else the one unbounded band
     * that a bare period — the stream's or the router's — resolves to. Must match `catchUp`'s,
     * or the page reports on a coverage nothing writes.
     */
    private fun refetchBandsOf(stream: SyncStream): List<SyncTier> = stream.refetchSchedule.ifEmpty { listOf(SyncTier(maxAgeSeconds = null, everySeconds = SyncBands.NEVER)) }

    /** The pool word, band-qualified where a stream has more than one. */
    private fun jobOf(
        pool: String,
        band: String,
    ): String = if (band.isEmpty()) pool else "$pool $band"

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
