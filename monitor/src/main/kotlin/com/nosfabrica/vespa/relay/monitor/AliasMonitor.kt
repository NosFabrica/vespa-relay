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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select

/** Which run a pass is part of. [label] is published: charts and the status page read it. */
enum class RunKind(
    val label: String,
) {
    /** Every candidate, on the sweep's clock. */
    SWEEP("all streams"),

    /** Only the urls named since the lane last looked. */
    FAST_LANE("fast lane"),
}

/**
 * The monitor's clock: a sweep of every [passes] entry over the candidate set, a fast lane over the
 * urls named since its last look, and a replay of the writes the store did not take. All three run on
 * one scheduler coroutine, so none overlaps another: the passes edit one record per url by read-modify-write.
 */
class AliasMonitor(
    /** Run over each sweep's urls in this order. */
    private val passes: List<Pass>,
    private val scope: CoroutineScope,
    private val intervalMs: Long = DEFAULT_INTERVAL_MS,
    private val startupDelayMs: Long = DEFAULT_STARTUP_DELAY_MS,
    private val emptyRetryMs: Long = DEFAULT_EMPTY_RETRY_MS,
    /** Where the candidate set comes from; null measures nothing. */
    private val source: CandidateSource? = null,
    /** How often the fast lane looks for newly named urls. Null, or an empty [fastLanePasses], turns it off. */
    private val fastLaneEveryMs: Long? = null,
    private val fastLanePasses: List<Pass> = emptyList(),
    /** How often a pass's writes the store did not take are replayed; null turns it off. */
    private val retryEveryMs: Long? = DEFAULT_RETRY_MS,
    /** Brackets each sweep, so its reads of our own records come off one walk; the lane and the retry read the store. */
    private val sweepRecords: SweepRecords = SweepRecords.NONE,
) {
    /** Starts and ends what one sweep reads our own records from. */
    interface SweepRecords {
        fun begin()

        fun end()

        companion object {
            val NONE =
                object : SweepRecords {
                    override fun begin() = Unit

                    override fun end() = Unit
                }
        }
    }

    /** One pass over one run's urls, returning how many verdicts it learned. */
    fun interface Pass {
        suspend fun measure(
            kind: RunKind,
            candidates: List<NormalizedRelayUrl>,
        ): Int

        /**
         * The pass's progress row, or null. The monitor writes the clock to it and the pass writes
         * the work.
         */
        val progress: Processors.Handle? get() = null

        /** Replay the writes the store did not take; returns how many landed. Never dials. */
        suspend fun flushPending(): Int = 0
    }

    /** Where the candidate set comes from. */
    interface CandidateSource {
        /** Every url worth measuring, across every configured stream. */
        suspend fun candidates(): List<NormalizedRelayUrl>

        /** The derivation's own progress row, or null; [candidates] walks the whole store. */
        val progress: Processors.Handle? get() = null

        /**
         * Only the urls named by relay-list events whose `created_at` is at or after [since], so a list
         * arriving backdated is left to the sweep. Throws when the lists could not be read; the default is a no-op.
         */
        suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> = emptyList()

        /** When the last [candidates] that read every relay list began, in epoch seconds; null before one has. */
        val listsReadFromSec: Long? get() = null
    }

    /** What the scheduler runs when its clock comes due. */
    private enum class Job { SWEEP, FAST_LANE, RETRY }

    /** When the next sweep is due, in epoch millis, or null while one runs. Read by the status page. */
    @Volatile
    private var dueAtMs: Long? = null

    /** Start the scheduler. Calling it twice launches two, which would overlap. */
    fun start(): AliasMonitor {
        val firstSweepAt = System.currentTimeMillis() + startupDelayMs
        // Registered before the first sleep, so silence never reads as "not configured".
        dueAtMs = firstSweepAt
        source?.progress?.let { p ->
            p.nextPassAt { dueAtMs }
            p.phase(Processors.IDLE)
        }
        for (pass in passes) {
            pass.progress?.nextPassAt { dueAtMs }
            pass.progress?.phase(Processors.IDLE)
        }
        scope.launch { schedule(firstSweepAt) }
        return this
    }

    /**
     * Every sweep, lane tick and retry, one at a time, the earliest due first. A clock that comes due
     * while another job runs is one tick, run once afterwards, however many periods it missed.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun schedule(firstSweepAt: Long) {
        var sweepAt = firstSweepAt
        val laneEveryMs = fastLaneEveryMs?.takeIf { fastLanePasses.isNotEmpty() && source != null }
        var laneAt = laneEveryMs?.let { firstSweepAt + it }
        // From the boot, not from zero: everything older is the sweep's.
        var lastLookSec = firstSweepAt / 1000
        // Off the sweep's clock: a store that recovers minutes after a batch failed should not wait hours.
        val retryMs = retryEveryMs
        var retryAt = retryMs?.let { System.currentTimeMillis() + it }
        while (true) {
            // Biased to the first clause, so a sweep due beside a lane tick runs first.
            val job =
                select {
                    onTimeout(untilMs(sweepAt)) { Job.SWEEP }
                    laneAt?.let { onTimeout(untilMs(it)) { Job.FAST_LANE } }
                    retryAt?.let { onTimeout(untilMs(it)) { Job.RETRY } }
                }
            when (job) {
                Job.SWEEP -> {
                    // Unset while it runs, so a running sweep does not read as a late one.
                    dueAtMs = null
                    val hadWork = guarded("sweep") { runPass() } == true
                    sweepAt = System.currentTimeMillis() + if (hadWork) intervalMs else emptyRetryMs
                    dueAtMs = sweepAt
                }

                Job.FAST_LANE -> {
                    // Taken before the read: a list created during it is in the next window, whether or not this one saw it.
                    val lookingAt = System.currentTimeMillis() / 1000
                    // Advanced only past a tick that ran, so a failed one's window is looked at again.
                    if (guarded("fast lane") { runFastLane(lastLookSec) } != null) lastLookSec = lookingAt
                    laneAt = laneEveryMs?.let { System.currentTimeMillis() + it }
                }

                Job.RETRY -> {
                    guarded("pending write retry") { flushPending() }
                    retryAt = retryMs?.let { System.currentTimeMillis() + it }
                }
            }
        }
    }

    private fun untilMs(atMs: Long): Long = atMs - System.currentTimeMillis()

    /** Runs [work], logging anything it throws but a cancellation; null when it threw. */
    private suspend fun <T> guarded(
        what: String,
        work: suspend () -> T,
    ): T? =
        try {
            work()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("monitor: $what failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            null
        }

    /** Every pass's pending writes, one pass at a time and each guarded. */
    suspend fun flushPending(): Int {
        var landed = 0
        for (pass in (passes + fastLanePasses).distinct()) {
            landed += guarded("pending write retry") { pass.flushPending() } ?: 0
        }
        return landed
    }

    /** One lane tick: the urls named since [sinceSec], through every [fastLanePasses] entry in order. */
    suspend fun runFastLane(sinceSec: Long): Int {
        val src = source ?: return 0
        // Never behind the last sweep's read: what it saw is graded, and a read outage must not grow the window without bound.
        val fresh = src.candidatesSince(maxOf(sinceSec, src.listsReadFromSec ?: sinceSec))
        if (fresh.isEmpty()) return 0
        System.err.println("monitor: fast lane — ${fresh.size} url(s) named since the last look")
        return runEach(fastLanePasses, RunKind.FAST_LANE, fresh)
    }

    /** One sweep: derive the candidate set, then run every pass over it in order. True when there was any. */
    suspend fun runPass(): Boolean {
        sweepRecords.begin()
        try {
            // Derived now, so no stream's discovery clock decides what this pass sees.
            val urls = source?.let { derive(it) }.orEmpty()
            runEach(passes, RunKind.SWEEP, urls)
            return urls.isNotEmpty()
        } finally {
            sweepRecords.end()
        }
    }

    /** The sweep's candidate set off [src], bracketed on its row; empty when the derivation failed. */
    private suspend fun derive(src: CandidateSource): List<NormalizedRelayUrl> {
        src.progress?.begin(Processors.COLLECTING)
        return try {
            src.candidates()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("monitor: alias source failed to derive: ${e.message}")
            emptyList()
        } finally {
            src.progress?.finish()
        }
    }

    /**
     * [pass]es over [urls] in order, each guarded so one failing does not cost the passes after it.
     * Every row's clock is stamped, an empty sweep's included; only urls are measured.
     */
    private suspend fun runEach(
        passes: List<Pass>,
        kind: RunKind,
        urls: List<NormalizedRelayUrl>,
    ): Int {
        var learned = 0
        for (pass in passes) {
            learned += guarded("${kind.label} pass") {
                pass.progress?.begin()
                try {
                    // Empty, not "fewer than two": the per-url passes grade a lone url, and the fold refuses a world of one itself.
                    if (urls.isEmpty()) 0 else pass.measure(kind, urls)
                } finally {
                    // A cancelled or thrown pass still stamps its clock.
                    pass.progress?.finish()
                }
            } ?: 0
        }
        return learned
    }

    companion object {
        /** Matches the default stream refresh. */
        const val DEFAULT_INTERVAL_MS = 6L * 60 * 60 * 1000

        /** So the first pass competes with downloads rather than every stream's opening burst. */
        const val DEFAULT_STARTUP_DELAY_MS = 2L * 60 * 1000

        /** The retry when a cold store has nothing to derive from yet. */
        const val DEFAULT_EMPTY_RETRY_MS = 60L * 1000

        /** How often unwritten verdicts are replayed: short against a sweep, long against a write's own deadline. */
        const val DEFAULT_RETRY_MS = 5L * 60 * 1000
    }
}
