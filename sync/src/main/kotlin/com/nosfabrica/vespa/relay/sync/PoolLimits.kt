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

    /**
     * ONE ENTRY PER CAPPED (stream, job), carrying the size and the permits
     * TOGETHER.
     *
     * They were two maps built from one filter, and every reader of both then
     * needed a fallback for a disagreement construction made impossible.
     */
    private class Gate(
        val cap: Int,
    ) {
        val permits = Semaphore(cap)
    }

    private val gates = caps.filterValues { it != null }.mapValues { (_, n) -> Gate(n!!) }

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
        /**
         * Whether a refusal is WORK TURNED AWAY, which is the only kind
         * [deferred] means to count.
         *
         * False for the live pool's first ask, and that is not bookkeeping
         * taste. A tail past its stream's budget is not dropped — it goes to
         * `earnTail`, which evicts the weakest sitting tail and asks again —
         * so a full live gate is the pool's ordinary steady state and every
         * eviction was recording a deferral against it. `limitsOf` marks a row
         * biting when it is at its cap WITH deferrals climbing, so any stream
         * sitting at its live budget was drawn permanently hot, which is the
         * one row shape the page colours and the one an operator is meant to
         * act on.
         */
        counted: Boolean = true,
    ): Hold? {
        val gate = gates[stream to job] ?: return Hold(null)
        if (gate.permits.tryAcquire()) return Hold(gate.permits)
        if (counted) deferrals.computeIfAbsent(stream to job) { AtomicLong() }.incrementAndGet()
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
    ): Int? = gates[stream to job]?.cap

    /** Permits handed out and not yet released — this stream's share of [job] in use. */
    fun heldBy(
        stream: String,
        job: String,
    ): Int? = gates[stream to job]?.let { it.cap - it.permits.availablePermits() }

    companion object {
        /**
         * The shares a config asks for, over the jobs the pool runs.
         *
         * ## The live pool is the one job that MUST have a number
         *
         * Three of these are uncapped when a config says nothing, and that is
         * safe because something else bounds them: a visit-job permit is taken
         * inside a visit, so `visitConcurrency` — itself the pool's worker
         * count — is a ceiling over the lot of them even where none is set.
         *
         * A tail is not. It is taken BETWEEN visits and released only when the
         * roster drops the relay, so an uncapped live gate is one held socket
         * per relay on the stream's roster, with nothing above it: a
         * thousand-relay stream would sit on a thousand subscriptions and
         * strangle every new connect behind OkHttp's dispatcher. So a stream
         * that names no `maxLiveConcurrency` gets
         * [RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY], which is the number the
         * router-wide `tailBudget` defaulted to before the budgets moved
         * inside the streams, and the number `warnOnSocketBudget` has been
         * assuming for it all along.
         *
         * Eviction is what happens AT that gate rather than instead of it —
         * see `VisitPool.earnTail`, which takes the socket from the tail that
         * has delivered least rather than refusing the newcomer. The gate is
         * what says how many there are to fight over.
         */
        fun of(streams: List<SyncStream>): PoolLimits =
            PoolLimits(
                streams
                    .flatMap { stream -> JOBS.map { (job, share) -> (stream.name to job) to share(stream) } }
                    .toMap(),
            )

        /**
         * EVERY JOB A STREAM HAS A BUDGET FOR, and where its number comes
         * from — the one list, read by [of] to build the gates and by
         * `VisitPool.limitsFor` to publish a row per job.
         *
         * One list because the two used to be two: [of] drops the nulls, so
         * the set of jobs that EXIST is destroyed at construction and the
         * publisher had to restate it. A fifth budget added to one side only
         * gives either a cap enforced and never shown, or a row that always
         * reads uncapped — and both are indistinguishable from a cap that is
         * simply not biting, which is the one reading the panel exists for.
         *
         * Ordered as the page draws them: the dial width, then the three jobs
         * a visit can be doing inside it.
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
