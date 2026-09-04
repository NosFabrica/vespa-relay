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

import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.config.SyncStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * What each stream may spend on each of the pool's four jobs: a permit per
 * (stream, job), with no router-wide cap beside it.
 *
 * Admission, not a queue. [tryHold] never waits: a visit that cannot get a
 * permit skips that job, the work stays due for the next visit, and the skip
 * is counted in [deferred]. A visit holds a socket and a worker for its whole
 * life, so waiting on a permit would idle both. A visit-job permit lives as
 * long as the job; a tail's lives as long as the tail and is released from
 * `dropTail`.
 */
internal class PoolLimits(
    /** Per (stream, job). Absent means uncapped for that stream. */
    caps: Map<Pair<String, String>, Int?>,
) {
    /**
     * A granted permit, releasable exactly once. Idempotent because a tail's
     * hold is released from `dropTail`, which races an eviction, a roster
     * drop and a re-open; a double release would be a cap that stops capping.
     */
    internal class Hold(
        /**
         * The gate to hand the permit back to, or null for an uncapped job.
         * Not a shared `Semaphore(Int.MAX_VALUE)`: that starts at its ceiling
         * and the first `release()` throws.
         */
        private val permit: Semaphore?,
    ) {
        private val spent = AtomicBoolean(false)

        fun release() {
            if (spent.compareAndSet(false, true)) permit?.release()
        }
    }

    /** One entry per capped (stream, job), carrying the size and the permits together. */
    private class Gate(
        val cap: Int,
    ) {
        val permits = Semaphore(cap)
    }

    private val gates = caps.filterValues { it != null }.mapValues { (_, n) -> Gate(n!!) }

    private val deferrals = ConcurrentHashMap<Pair<String, String>, AtomicLong>()

    /**
     * A permit for [stream] to do [job], or null when its share is full. An
     * uncapped job is granted a hold over nothing: null must mean refused and
     * only that, or a caller's `?: return` would skip work that has no cap.
     */
    fun tryHold(
        stream: String,
        job: String,
    ): Hold? {
        trySpare(stream, job)?.let { return it }
        deferrals.computeIfAbsent(stream to job) { AtomicLong() }.incrementAndGet()
        return null
    }

    /**
     * [tryHold] for a caller whose refusal is not the end of the work, so it
     * is not counted in [deferred]. The live pool is that caller: a full live
     * gate is its ordinary steady state, and a tail past budget goes to
     * `earnTail` rather than being dropped.
     */
    fun trySpare(
        stream: String,
        job: String,
    ): Hold? {
        val gate = gates[stream to job] ?: return Hold(null)
        return if (gate.permits.tryAcquire()) Hold(gate.permits) else null
    }

    /** How many times [stream] was refused a permit for [job] since boot. */
    fun deferred(
        stream: String,
        job: String,
    ): Long = deferrals[stream to job]?.get() ?: 0L

    /** [stream]'s share of [job], or null where it has none and the dial width bounds it. */
    fun capFor(
        stream: String,
        job: String,
    ): Int? = gates[stream to job]?.cap

    /** Permits handed out and not yet released. */
    fun heldBy(
        stream: String,
        job: String,
    ): Int? = gates[stream to job]?.let { it.cap - it.permits.availablePermits() }

    companion object {
        /**
         * The shares a config asks for, over [JOBS]. The live pool is the one
         * job that must have a number: a tail is taken between visits and held
         * until the roster drops the relay, so nothing else bounds it, and a
         * stream naming no `maxLiveConcurrency` gets
         * [RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY]. Eviction
         * (`VisitPool.earnTail`) happens at that gate, not instead of it.
         */
        fun of(streams: List<SyncStream>): PoolLimits =
            PoolLimits(
                streams
                    .flatMap { stream -> JOBS.map { (job, share) -> (stream.name to job) to share(stream) } }
                    .toMap(),
            )

        /**
         * Every job a stream has a budget for, and where its number comes
         * from. One list, read by [of] to build the gates and by
         * `VisitPool.limitsFor` to publish a row per job, so a budget cannot
         * be enforced and not shown. Ordered as the page draws them.
         */
        val JOBS: List<Pair<String, (SyncStream) -> Int?>> =
            listOf(
                VisitPool.JOB_VISITING to { s: SyncStream -> s.visitConcurrency },
                VisitPool.POOL_LIVE to { s: SyncStream -> s.liveBudget },
                VisitPool.POOL_REFETCHING to { s: SyncStream -> s.refetchConcurrency },
                VisitPool.POOL_NEGENTROPY to { s: SyncStream -> s.negentropyConcurrency },
            )
    }
}
