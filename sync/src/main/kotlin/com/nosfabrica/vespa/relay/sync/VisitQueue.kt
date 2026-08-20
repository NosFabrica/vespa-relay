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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * THE POOL'S ROTATION BOOKKEEPING — one home for the invariants five bare
 * collections used to share by convention, and where two audited race bugs
 * lived: the requeue a worker collision swallowed, and the revisit timers
 * that stacked into double-cadence chains.
 *
 * - [offer] dedups against the QUEUE, never against a running visit — a url
 *   can be wanted again while it is being visited.
 * - A url drawn while its visit is still running is PARKED, and the visit's
 *   own worker re-offers it the moment it finishes: the promptness a changed
 *   ask set was promised, kept through the race instead of dropped by it.
 * - A finished visit that consumed no parked requeue arms exactly ONE
 *   revisit timer, however many out-of-band offers (evictions, rebuilds)
 *   landed meanwhile.
 *
 * The queue knows nothing about relays, rosters or tails: [visitLoop]'s three
 * callbacks are the whole contract, which is what makes the invariants a
 * hermetically testable surface rather than probe-only choreography.
 */
internal class VisitQueue(
    private val scope: CoroutineScope,
) {
    private val channel = Channel<NormalizedRelayUrl>(Channel.UNLIMITED)
    private val queued = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val inFlight = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val parked = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /**
     * The pending revisit per url — the CANCELLABLE job (see [disarm] for what
     * a bare membership mark could not express) and when it comes due.
     *
     * The deadline rides beside the job rather than in a second map so the two
     * cannot disagree about which revisit is armed: every path that replaces or
     * cancels a job replaces or removes the pair.
     */
    private class Armed(
        val job: Job,
        val dueAtMs: Long,
    )

    private val armed = ConcurrentHashMap<NormalizedRelayUrl, Armed>()

    /** Guards the two compound reads that decide a park — see [visitLoop]. */
    private val handoff = Any()

    val waiting: Int get() = queued.size

    val visiting: Int get() = inFlight.size

    /**
     * EVERY ARMED REVISIT, in seconds from [nowMs] and keyed by url STRING —
     * the form the status document publishes, so the caller walking thousands
     * of ledger rows does one pass here rather than a lookup per row.
     *
     * A url that is being visited right now, or waiting in the queue, is
     * absent: the timer is armed when a visit FINISHES, and there is nothing to
     * count down to before that. Never negative — a timer that is due but has
     * not run yet reads as `0`, and a countdown running backwards would read as
     * a revisit that is overdue by hours.
     */
    fun revisitsDueInSec(nowMs: Long = System.currentTimeMillis()): Map<String, Long> = armed.entries.associate { (url, a) -> url.url to ((a.dueAtMs - nowMs) / 1000).coerceAtLeast(0) }

    /** Queue [url] now. False when it is already waiting (running is fine). */
    fun offer(url: NormalizedRelayUrl): Boolean {
        if (!queued.add(url)) return false
        channel.trySend(url)
        return true
    }

    /**
     * One worker's forever-loop: draw urls and run [visit] on each, keeping
     * the invariants above. [revisitDelayMs] is read as the visit finishes —
     * the delay depends on what it delivered — and [stillWanted] gates both
     * the visit and every requeue, so a url the roster dropped mid-wait dies
     * quietly. [visit] is expected to contain its own failure handling; a
     * throw that escapes it (cancellation) ends the worker.
     */
    suspend fun visitLoop(
        stillWanted: (NormalizedRelayUrl) -> Boolean,
        revisitDelayMs: (NormalizedRelayUrl) -> Long,
        visit: suspend (NormalizedRelayUrl) -> Unit,
    ) {
        for (url in channel) {
            queued.remove(url)
            // ONE STEP, because these two collections have to agree.
            //
            // `inFlight.add` and `parked.add` were separate, and so were the
            // finishing visit's `inFlight.remove` and `parked.remove`. Between
            // a worker's failed add and its park, the running visit could
            // finish, see nothing parked, and arm a timer — so the prompt
            // requeue this class promises as its second invariant was
            // downgraded to a timer wait, and the park left behind bought one
            // spurious back-to-back visit later. Both blocks are pure
            // collection work and neither suspends, so a plain monitor is the
            // whole fix.
            val parkedInstead =
                synchronized(handoff) {
                    if (inFlight.add(url)) false else parked.add(url).let { true }
                }
            if (parkedInstead) continue
            var requeue = false
            try {
                if (stillWanted(url)) visit(url)
            } finally {
                requeue =
                    synchronized(handoff) {
                        inFlight.remove(url)
                        parked.remove(url)
                    }
            }
            if (requeue) {
                // A requeue arrived mid-visit: back on the queue now, and no
                // timer — the prompt visit will arm its own.
                offer(url)
            } else {
                armRevisit(url, revisitDelayMs, stillWanted)
            }
        }
    }

    /**
     * DROP A PENDING REVISIT so the next completion arms a fresh one.
     *
     * The delay is read once, when the timer is armed, and a url armed while
     * it was TAILED gets the tailed cadence — half an hour against five
     * minutes for an untailed one. Eviction promptly requeues the url, but the
     * visit that follows finds the old timer still standing in `armed` and
     * arms nothing, so the relay that just LOST its live feed then waits out
     * the cadence it earned while it had one. Six times the freshness gap, on
     * exactly the relays least able to afford it.
     *
     * So the pool disarms on eviction. The "exactly one timer" rule is intact
     * — this removes one rather than adding a second — and a url with nothing
     * armed is a no-op.
     */
    fun disarm(url: NormalizedRelayUrl) {
        armed.remove(url)?.job?.cancel()
    }

    private fun armRevisit(
        url: NormalizedRelayUrl,
        revisitDelayMs: (NormalizedRelayUrl) -> Long,
        stillWanted: (NormalizedRelayUrl) -> Boolean,
    ) {
        val delayMs = revisitDelayMs(url)
        // LAZY, and registered before it can run: the body clears its own
        // entry, so a job that started before the map knew about it would
        // either clear a successor's entry or leak its own.
        val job =
            scope.launch(start = CoroutineStart.LAZY) {
                delay(delayMs)
                // Only OUR OWN entry — a [disarm] and re-arm in between put a
                // different one there, and that one still owes a revisit. The
                // map holds (job, deadline) pairs now, so the identity is read
                // off the pair's job and the removal is still atomic on it.
                val current = armed[url]
                if (current != null && current.job === coroutineContext[Job] && armed.remove(url, current)) {
                    if (stillWanted(url)) offer(url)
                }
            }
        // THE LOSER OF THE SLOT HAS TO BE CANCELLED, not merely dropped.
        //
        // `scope.launch` registers the job as a child of the scope's Job the
        // moment it is created — LAZY defers the BODY, not the parenting. A
        // job that is never started and never cancelled therefore stays an
        // incomplete child for the life of the process: one retained Job per
        // lost race, on a scope that lives as long as the router, and a parent
        // that can never complete normally while it is there.
        //
        // The race is narrow but real. `armRevisit` runs after the
        // synchronized block has already dropped the url from `inFlight`, so
        // another worker can draw it, visit it and arm it in the gap — and the
        // gap is as wide as `revisitDelayMs`, which is the caller's lambda and
        // reads the roster.
        val entry = Armed(job, System.currentTimeMillis() + delayMs)
        if (armed.putIfAbsent(url, entry) != null) {
            job.cancel()
            return
        }
        job.start()
    }
}
