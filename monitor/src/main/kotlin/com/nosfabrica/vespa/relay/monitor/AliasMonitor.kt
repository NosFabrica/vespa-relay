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

import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong

/**
 * The monitor's clock: a sweep of every [passes] entry over the candidate set on its own
 * schedule, and a fast lane over the urls named since the last look. Passes never overlap,
 * sweep or lane: they edit one record per url by read-modify-write, so [passGate] serialises them.
 */
class AliasMonitor(
    /** Run over each sweep's urls in this order, never concurrently. */
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
) {
    /** One pass over one label's urls, returning how many verdicts it learned. */
    fun interface Pass {
        suspend fun measure(
            label: String,
            candidates: List<NormalizedRelayUrl>,
            canDial: suspend (NormalizedRelayUrl) -> Boolean,
            onEvent: suspend (Event) -> Unit,
            sockets: Sockets,
        ): Int

        /**
         * The pass's progress row, or null. The monitor writes the clock to it and the pass writes
         * the work.
         */
        val progress: Processors.Handle? get() = null
    }

    /** Where the candidate set comes from. */
    interface CandidateSource {
        /** Every url worth measuring, across every configured stream. */
        suspend fun candidates(): List<NormalizedRelayUrl>

        /** The derivation's own progress row, or null; [candidates] walks the whole store. */
        val progress: Processors.Handle? get() = null

        /**
         * Only the urls named by relay-list events ingested at or after [since]; the default makes
         * the lane a no-op.
         */
        suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> = emptyList()

        /** Whether this process can reach that url at all: transport, not policy. */
        suspend fun canDial(url: NormalizedRelayUrl): Boolean

        /** A probe's events are still events: offered to whichever streams want them. */
        suspend fun onEvent(event: Event)

        /** The cross-stream socket refcount, so a probe cannot close a live transfer. */
        val sockets: Sockets
    }

    /** One label's work: what it sees, and how to reach it. */
    private class Work(
        val candidates: List<NormalizedRelayUrl>,
        val canDial: suspend (NormalizedRelayUrl) -> Boolean,
        val onEvent: suspend (Event) -> Unit,
        val sockets: Sockets,
    )

    private val learnedTotal = AtomicLong()

    /**
     * How many verdicts the sweep has learned since boot: a version for a cached relay list, not a
     * statistic.
     */
    fun generation(): Long = learnedTotal.get()

    /** Start the sweep loop and, when configured, the fast lane. Calling it twice launches two loops. */
    fun start(): AliasMonitor {
        // Registered before the first sleep, so silence never reads as "not configured".
        dueAtMs = System.currentTimeMillis() + startupDelayMs
        source?.progress?.let { p ->
            p.nextPassAt { dueAtMs }
            p.phase(Processors.IDLE)
        }
        for (pass in passes) {
            pass.progress?.nextPassAt { dueAtMs }
            pass.progress?.phase(Processors.IDLE)
        }
        scope.launch {
            delay(startupDelayMs)
            while (scope.isActive) {
                // Unset while a pass runs, so a running pass does not read as a late one.
                dueAtMs = null
                passGate.withLock { runPass() }
                val wait = if (lastPassHadWork) intervalMs else emptyRetryMs
                dueAtMs = System.currentTimeMillis() + wait
                delay(wait)
            }
        }
        // A lane tick that lands mid-sweep waits on the gate; its since-bound still covers those minutes.
        val everyMs = fastLaneEveryMs
        if (everyMs != null && fastLanePasses.isNotEmpty() && source != null) {
            scope.launch {
                delay(startupDelayMs)
                // From the boot, not from zero: everything older is the sweep's.
                var lastLookSec = System.currentTimeMillis() / 1000
                while (scope.isActive) {
                    delay(everyMs)
                    val lookFrom = lastLookSec
                    lastLookSec = System.currentTimeMillis() / 1000
                    try {
                        passGate.withLock { runFastLane(lookFrom) }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: fast lane failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
                    }
                }
            }
        }
        return this
    }

    /**
     * One lane tick: the urls named since [sinceSec], through every [fastLanePasses] entry in
     * order. The caller holds [passGate]. Not bracketed onto the source's row, which is the sweep's.
     */
    suspend fun runFastLane(sinceSec: Long): Int {
        val src = source ?: return 0
        val fresh = src.candidatesSince(sinceSec)
        if (fresh.isEmpty()) return 0
        System.err.println("router: fast lane — ${fresh.size} url(s) named since the last look")
        var learned = 0
        for (pass in fastLanePasses) {
            pass.progress?.begin()
            try {
                // Guarded one at a time, so one pass failing does not cost the passes after it.
                learned += pass.measure(FAST_LANE, fresh, src::canDial, src::onEvent, src.sockets)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: fast lane pass failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            } finally {
                pass.progress?.finish()
            }
        }
        // Not added to learnedTotal: the lane does not move generation.
        return learned
    }

    /** Serialises the sweep and the fast lane. */
    private val passGate = Mutex()

    /** When the next pass is due, in epoch millis, or null while one runs. */
    @Volatile
    private var dueAtMs: Long? = null

    /** Did the last pass have anything to measure? An empty pass retries at [emptyRetryMs]. */
    @Volatile
    private var lastPassHadWork = false

    /**
     * One sweep: derive the candidate set, then run every pass over it in order. The caller
     * holds [passGate]. A pass that throws must not end the loop or skip the passes after it.
     */
    suspend fun runPass(): Int {
        var learned = 0
        // Derived now, so no stream's discovery clock decides what this pass sees.
        val work =
            source?.let { src ->
                src.progress?.begin(Processors.COLLECTING)
                val urls =
                    try {
                        src.candidates()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: alias source failed to derive: ${e.message}")
                        emptyList()
                    } finally {
                        src.progress?.finish()
                    }
                // Empty, not "fewer than two": the per-url passes grade a lone url, and the fold
                // refuses a world of one itself.
                if (urls.isEmpty()) emptyList() else listOf(ALL_STREAMS to Work(urls, src::canDial, src::onEvent, src.sockets))
            } ?: emptyList()
        lastPassHadWork = work.isNotEmpty()
        // One pass at a time over every label, so the fold finishes everywhere before the next pass measures.
        for (pass in passes) {
            pass.progress?.begin()
            try {
                for ((label, w) in work) {
                    // Each label guarded on its own, so one failing does not cost the others.
                    try {
                        val n = pass.measure(label, w.candidates, w.canDial, w.onEvent, w.sockets)
                        learned += n
                        // Per label, not once at the end, so a fan-out starting mid-pass sees the
                        // verdicts already published.
                        if (n > 0) learnedTotal.addAndGet(n.toLong())
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: $label alias pass failed: ${e.javaClass.simpleName}: ${e.message}")
                    }
                }
            } finally {
                // A cancelled or thrown pass still stamps its clock.
                pass.progress?.finish()
            }
        }
        return learned
    }

    companion object {
        /** The sweep's one row, because the set is a union over streams. */
        const val ALL_STREAMS = "all streams"

        const val FAST_LANE = "fast lane"

        /** Matches the default stream refresh. */
        const val DEFAULT_INTERVAL_MS = 6L * 60 * 60 * 1000

        /** So the first pass competes with downloads rather than every stream's opening burst. */
        const val DEFAULT_STARTUP_DELAY_MS = 2L * 60 * 1000

        /** The retry when a cold store has nothing to derive from yet. */
        const val DEFAULT_EMPTY_RETRY_MS = 60L * 1000
    }
}
