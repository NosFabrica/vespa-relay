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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * WHAT EACH STREAM MAY SPEND ON EACH OF THE POOL'S FOUR JOBS.
 *
 * ## The problem this is for
 *
 * `visitConcurrency` is a DIAL width: how many relays may be visited at once.
 * It says nothing about what those visits are doing, and the four jobs behind
 * it do not cost remotely the same. A catch-up page is parse and ingest. A
 * negentropy audit builds and compares id sets per window and is the one
 * genuinely CPU-bound thing this router does. A re-fetch is a catch-up over
 * history already held — the most expensive scheduled work in the system. One
 * number for all four can only be set for the worst of them.
 *
 * Nor are the streams peers. A content mirror over ~130 kinds and a
 * thirty-relay index stream ride the same pool, and nothing stopped the first
 * one's audits occupying every worker the second needed — the pool's fairness
 * is per URL, and a stream with ten times the roster gets ten times the
 * workers by construction.
 *
 * So: a permit per (stream, job). PER STREAM AND ONLY PER STREAM — there is no
 * router-wide cap beside it, because there is nothing for one to say that the
 * shares do not: what every stream may take between them is the sum of what
 * each may take, written where the stream that pays it is configured. A second
 * ceiling over the top would be a number an operator has to keep in step with
 * the shares by hand, and the failure it would cause — a stream inside its own
 * share, refused anyway, by a limit named nowhere near it — is exactly the one
 * these are meant to make legible.
 *
 * ## ADMISSION, NOT A QUEUE
 *
 * [tryHold] never waits. A visit that cannot get a permit skips that job and
 * carries on with the rest of the visit, and the work stays due for the next
 * one — every job the pool runs is due-gated and idempotent, so skipping costs
 * a revisit delay and nothing else.
 *
 * Waiting was the obvious alternative and it is wrong here. A visit holds a
 * SOCKET and one of `visitConcurrency`'s slots for its whole life; blocking on
 * a permit would hold both while doing nothing, so a cap of 4 audits would idle
 * as many workers as there were relays due one. Admission spends nothing while
 * the cap is full.
 *
 * The skip is COUNTED, per stream and job ([deferred]), because a cap that
 * silently drops work is indistinguishable from work that was never due. That
 * counter beside `auditsRun` is what tells an operator their cap is the reason
 * a stream's history is not being re-checked.
 *
 * ## What a permit is held ACROSS
 *
 * A visit-job permit lives as long as the job: one leg's walk, one relay's
 * audit. A tail's lives as long as the tail, which is between visits and
 * usually forever — so [Hold.release] is called from `dropTail` and not from a
 * `finally` around a piece of work. Both are the same handle; only the scope
 * differs.
 */
internal class PoolLimits(
    /** Per (stream, job). Absent = uncapped for that stream, which is every job before this existed. */
    caps: Map<Pair<String, String>, Int?>,
) {
    /**
     * A granted permit, releasable exactly once.
     *
     * Idempotent on purpose: a tail's hold is released by `dropTail`, which
     * races an eviction, a roster drop and a re-open, and the failure mode of
     * a double release is a semaphore that hands out permits it does not have
     * — a cap that silently stops capping. The flag costs an atomic and makes
     * the class safe to call from wherever the tail actually ends.
     */
    internal class Hold(
        /**
         * The gate to hand the permit back to, or NULL for an uncapped job —
         * a hold over nothing, which every caller releases like any other.
         *
         * Null rather than a shared unlimited semaphore, and the difference is
         * not stylistic: `Semaphore(Int.MAX_VALUE)` starts AT its ceiling, so
         * the first `release()` against it throws
         * `Error: Maximum permit count exceeded` — out of the `finally` that
         * ends a leg, on the uncapped path, which is every deployment that has
         * configured nothing.
         */
        private val permit: Semaphore?,
    ) {
        private val spent = AtomicBoolean(false)

        fun release() {
            if (spent.compareAndSet(false, true)) permit?.release()
        }
    }

    private val gates = caps.filterValues { it != null }.mapValues { (_, n) -> Semaphore(n!!) }
    private val sizes = caps.filterValues { it != null }.mapValues { (_, n) -> n!! }

    private val deferrals = ConcurrentHashMap<Pair<String, String>, AtomicLong>()

    /**
     * A permit for [stream] to do [job], or null when its share is full.
     *
     * An uncapped job is granted a hold over NOTHING rather than refused or
     * gated: [tryHold] returning null must mean "refused" and only that, or a
     * caller's `?: return` would skip the work every time the job simply has
     * no cap — which is every deployment that has configured none.
     */
    fun tryHold(
        stream: String,
        job: String,
    ): Hold? {
        val gate = gates[stream to job] ?: return Hold(null)
        if (gate.tryAcquire()) return Hold(gate)
        deferrals.computeIfAbsent(stream to job) { AtomicLong() }.incrementAndGet()
        return null
    }

    /** How many times [stream] was refused a permit for [job] since boot. */
    fun deferred(
        stream: String,
        job: String,
    ): Long = deferrals[stream to job]?.get() ?: 0L

    /** [stream]'s share of [job], or null where it has none and is bounded by the dial width. */
    fun capFor(
        stream: String,
        job: String,
    ): Int? = sizes[stream to job]

    /** Permits handed out and not yet released — this stream's share of [job] in use. */
    fun heldBy(
        stream: String,
        job: String,
    ): Int? = sizes[stream to job]?.let { cap -> cap - (gates[stream to job]?.availablePermits() ?: cap) }

    companion object {
        /**
         * The shares a config asks for, over the jobs the pool runs.
         *
         * The live pool's ROUTER-WIDE number is deliberately not a gate here:
         * that one is enforced by the pool's own eviction, which does something
         * no semaphore can — it takes the socket from the tail that has
         * delivered least rather than refusing the newcomer. What is here is
         * the per-stream share of those sockets.
         */
        fun of(streams: List<SyncStream>): PoolLimits =
            PoolLimits(
                streams
                    .flatMap { stream ->
                        listOf(
                            (stream.name to VisitPool.JOB_VISITING) to stream.visitConcurrency,
                            (stream.name to VisitPool.POOL_REFETCHING) to stream.refetchConcurrency,
                            (stream.name to VisitPool.POOL_NEGENTROPY) to stream.negentropyConcurrency,
                            (stream.name to VisitPool.POOL_LIVE) to stream.maxLiveConcurrency,
                        )
                    }.toMap(),
            )
    }
}
