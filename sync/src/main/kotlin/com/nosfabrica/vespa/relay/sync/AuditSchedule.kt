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
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * WHEN AN ASK'S AUDIT COMES DUE — the three answers, in one value.
 *
 * The engine gate and the status row both ask this, and both used to read a
 * `Long?` carrying two conventions at once: `null` for "never run, so due by
 * definition" and a sentinel for "nothing schedules this at all". Two magic
 * values in one nullable Long, meaning OPPOSITE things — always due and never
 * due — with nothing in the type to say so.
 *
 * A value class, so it is still a bare `long` at runtime: this is read once
 * per ask per visit on a roster of thousands, and the cost of saying what it
 * means should be nothing.
 */
@JvmInline
internal value class AuditClock private constructor(
    private val at: Long,
) {
    /** Is this ask audited at ALL — see [NOT_SCHEDULED]. */
    val scheduled: Boolean get() = at != UNSCHEDULED

    /**
     * Scheduled, but never yet run — always due, which is what makes a relay's
     * first audit happen on its first visit rather than a period later.
     *
     * Counted apart from [dueBy] by the status row, because that is what makes
     * a fresh deployment's audit storm read as scheduled work whose schedule
     * has not started rather than a period being ignored.
     */
    val neverRun: Boolean get() = at == NEVER_RUN

    /** Due by [now]? Never for an unscheduled ask; always for one never run. */
    fun dueBy(now: Long): Boolean = scheduled && (neverRun || at <= now)

    /**
     * The time it comes due, for a countdown — null wherever there is no one
     * time to count down to, which is both of the answers above.
     */
    val dueAt: Long? get() = if (!scheduled || neverRun) null else at

    companion object {
        private const val UNSCHEDULED = Long.MAX_VALUE
        private const val NEVER_RUN = Long.MIN_VALUE

        /**
         * NOTHING AUDITS THIS ASK: a `deleteMissing` stream whose `ownedKinds`
         * does not reach it, or a router with no retraction plane at all.
         * Never due — and never a backlog either, which is why the status row
         * leaves it out rather than counting it due forever.
         */
        val NOT_SCHEDULED = AuditClock(UNSCHEDULED)

        /** Scheduled and never run — see [neverRun]. */
        val NEVER_AUDITED = AuditClock(NEVER_RUN)

        /** …and the ordinary answer: a time, or [NEVER_AUDITED] where the band has no clock yet. */
        fun of(dueAt: Long?): AuditClock = if (dueAt == null) NEVER_AUDITED else AuditClock(dueAt)
    }
}

/**
 * WHEN EACH STREAM'S SCHEDULED RE-READS OF THE PAST COME DUE, and how much is
 * waiting behind them.
 *
 * ## What it is for
 *
 * "Do the audits only run when they are scheduled to" is a question the
 * counters cannot answer. `negentropyRuns` climbing says work happened;
 * nothing said whether it was DUE. The two are told apart here: work only ever
 * leaves [StreamPhases.Scheduled.waiting] by its clock running out, so a
 * `waiting` that holds steady while the counters climb is a rule being broken,
 * and one that drains at the period is the schedule working.
 *
 * ## Why it is its own class
 *
 * Because it is the one place `deleteMissing` decides WHICH CLOCK schedules an
 * ask, and that decision was written in three places before it was written
 * here — the engine's gate, the status walk, and the retraction plane's own
 * copy. The copies disagreed, and the panel built to certify that audits run
 * only when due read permanently in arrears while the audits ran perfectly
 * well. One class owns the decision; [VisitPool] asks it both questions.
 *
 * It is also the piece with no store, no sockets and no coroutines behind it —
 * a roster in, rows out — so pulling it out of the pool is what makes the
 * schedule assertable at all.
 */
internal class AuditSchedule(
    private val streams: List<SyncStream>,
    private val bands: SyncBands,
    private val retraction: RetractionAudit?,
) {
    /**
     * WHICH CLOCK SCHEDULES THIS ASK'S AUDIT, and when it next comes due.
     *
     * A `deleteMissing` stream's comparison runs on the ask's owned-kind
     * projection and stamps its clock THERE, so reading the full ask's filter
     * finds a key nothing ever writes and falls back to a band `fullAt` no
     * reconcile advances. That is the branch this method exists to hold once.
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
     * Would an audit run for this ask right now, without stamping anything to
     * find out?
     *
     * Deliberately WEAKER than the claim it guards: it reads the band clock
     * and not `attemptSpacingSeconds`, so an ask that recently attempted and
     * failed still passes here and is turned away by `claimAudit` a moment
     * later, having briefly held a permit. That is the cheap direction to be
     * wrong in — the alternative is a second copy of the spacing rule, kept in
     * step by hand with the one that matters.
     */
    fun isDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        now: Long,
    ): Boolean = clockFor(ask, url, negentropySyncThePastSeconds).dueBy(now)

    /**
     * ONE PASS OVER THE ROSTER for every stream at once — the walk is the same
     * walk for all of them, and doing it per stream would multiply the cost by
     * the stream count for identical work.
     *
     * Dueness is per (stream, relay, filter): every ask ages on its own clock,
     * which is what makes the audits a trickle rather than a herd, and the
     * price of that is that no single number knows the answer. The caller
     * caches — see `VisitPool.scheduleFor`.
     */
    fun rows(
        roster: Map<NormalizedRelayUrl, Map<String, RosterBuilder.UnitAsks>>,
        nowSec: Long,
    ): Map<String, List<StreamPhases.Scheduled>> {
        val audits = HashMap<String, Tally>()
        val refetches = HashMap<String, Tally>()
        val refetchPeriods = HashMap<String, Long>()
        for ((url, byStream) in roster) {
            for ((name, unit) in byStream) {
                // Per stream, not per ask: the knob and the tally are the
                // stream's, and the roster's nesting hands them over grouped.
                val refetching = refetchPeriods.getOrPut(name) { bands.refetchThePastSecondsFor(name) } != SyncBands.NEVER
                for (ask in unit.asks) {
                    ask.stream.negentropySyncThePastSeconds?.let { period ->
                        // An ask nothing schedules is left OUT of the tally
                        // rather than counted due: a backlog that can never
                        // drain is the one reading this row must not invent.
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

    /**
     * One job's asks, sorted into the three states a schedule has — and the
     * soonest of the ones still counting down.
     */
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
                // Never audited: due by definition, and counted apart so a
                // fresh deployment's storm reads as a schedule that has not
                // started rather than one being ignored.
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
            // Absent when nothing is waiting — there is no next one to count
            // down to, and a 0 would read as "due now".
            nextInSec = soonest?.let { (it - nowSec).coerceAtLeast(0) },
        )
    }
}
