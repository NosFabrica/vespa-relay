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
 * What each stream may spend on each of the pool's four jobs: a permit per (stream, job),
 * with no router-wide cap beside it. Admission, not a queue: [tryHold] never waits, and a
 * refused job stays due for the next visit.
 */
internal class PoolLimits(
    /** Per (stream, job). Absent means uncapped for that stream. */
    caps: Map<Pair<String, String>, Int?>,
) {
    /**
     * A granted permit, releasable exactly once, because a tail's hold is released from
     * `dropTail`, which races an eviction, a roster drop and a re-open.
     */
    internal class Hold(
        /** The gate to hand the permit back to, or null for an uncapped job. */
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
     * A permit for [stream] to do [job], or null when its share is full. An uncapped job is
     * granted a hold over nothing: null must mean refused and only that.
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
     * [tryHold] for a caller whose refusal is not the end of the work, so it is not counted in
     * [deferred]: the live pool, whose full gate is its ordinary steady state.
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
         * The shares a config asks for, over [JOBS]. The live pool is the one job that must
         * have a number, since nothing else bounds a tail; a stream naming none gets
         * [RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY].
         */
        fun of(streams: List<SyncStream>): PoolLimits =
            PoolLimits(
                streams
                    .flatMap { stream -> JOBS.map { (job, share) -> (stream.name to job) to share(stream) } }
                    .toMap(),
            )

        /**
         * Every job a stream has a budget for, and where its number comes from. Read by [of]
         * and by `VisitPool.limitsFor`, so a budget cannot be enforced and not shown. Ordered
         * as the page draws them.
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
