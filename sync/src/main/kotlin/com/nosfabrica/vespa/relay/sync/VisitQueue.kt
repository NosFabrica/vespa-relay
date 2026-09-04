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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * The pool's rotation bookkeeping: offers, collisions and revisit timers,
 * generic in its unit of work.
 *
 * - [offer] dedups against the queue, never against a running visit.
 * - A unit drawn while its visit is still running is parked, and the visit's
 *   own worker re-offers it the moment it finishes.
 * - A finished visit that consumed no parked requeue arms exactly one revisit
 *   timer, however many out-of-band offers landed meanwhile.
 *
 * [visitLoop]'s three callbacks are the whole contract; the queue knows
 * nothing about relays, rosters or tails.
 */
internal class VisitQueue<K : Any>(
    private val scope: CoroutineScope,
) {
    private val channel = Channel<K>(Channel.UNLIMITED)
    private val queued = ConcurrentHashMap.newKeySet<K>()
    private val inFlight = ConcurrentHashMap.newKeySet<K>()
    private val parked = ConcurrentHashMap.newKeySet<K>()

    /** The pending revisit per unit, as a cancellable job; see [disarm]. */
    private val armed = ConcurrentHashMap<K, Job>()

    /** Guards the two compound reads that decide a park; see [visitLoop]. */
    private val handoff = Any()

    val waiting: Int get() = queued.size

    val visiting: Int get() = inFlight.size

    /**
     * [waiting] split by [of], in one walk. A snapshot of a set the workers
     * are mutating, so the counts are read at one tick and not one instant.
     */
    fun <G : Any> waitingBy(of: (K) -> G): Map<G, Int> {
        val out = HashMap<G, Int>()
        for (key in queued) out.merge(of(key), 1, Int::plus)
        return out
    }

    /** Queues [key] now. False when it is already waiting (running is fine). */
    fun offer(key: K): Boolean {
        if (!queued.add(key)) return false
        channel.trySend(key)
        return true
    }

    /**
     * One worker's loop: draws units and runs [visit] on each, keeping the
     * invariants above. [revisitDelayMs] is read as the visit finishes, and
     * [stillWanted] gates both the visit and every requeue. A throw escaping
     * [visit] ends the worker.
     */
    suspend fun visitLoop(
        stillWanted: (K) -> Boolean,
        revisitDelayMs: (K) -> Long,
        visit: suspend (K) -> Unit,
    ) {
        for (url in channel) {
            queued.remove(url)
            // One step with the finishing visit's removal below: between a failed add and the
            // park, the running visit could finish, see nothing parked, and arm a timer.
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
                // A requeue arrived mid-visit: back on the queue now, and the prompt visit arms its own timer.
                offer(url)
            } else {
                armRevisit(url, revisitDelayMs, stillWanted)
            }
        }
    }

    /**
     * Drops a pending revisit so the next completion arms a fresh one. The
     * delay is read when the timer is armed, so without this a tail's cadence
     * would outlive the tail. A unit with nothing armed is a no-op.
     */
    fun disarm(key: K) {
        armed.remove(key)?.cancel()
    }

    private fun armRevisit(
        url: K,
        revisitDelayMs: (K) -> Long,
        stillWanted: (K) -> Boolean,
    ) {
        val delayMs = revisitDelayMs(url)
        // Lazy and registered before it can run: the body clears its own entry.
        val job =
            scope.launch(start = CoroutineStart.LAZY) {
                delay(delayMs)
                // Only our own entry: a disarm and re-arm in between put a different job there.
                if (armed.remove(url, coroutineContext[Job])) {
                    if (stillWanted(url)) offer(url)
                }
            }
        // The loser must be cancelled, not dropped: LAZY defers the body, not the parenting,
        // and an unstarted child stays incomplete on the scope for the life of the process.
        if (armed.putIfAbsent(url, job) != null) {
            job.cancel()
            return
        }
        job.start()
    }
}
