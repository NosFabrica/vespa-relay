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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.ReadShape
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip77Negentropy.NegOpenCmd
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * How many plain REQ replays the whole relay runs against the store at once; the rest wait their turn in
 * arrival order, and past [maxWaiting] waiting a new one is refused ([refuses]). Ranked reads are
 * [SearchGate]'s, per connection. The permit covers the store call to EOSE, never the live tail.
 */
class ReplayGate(
    /** Replays in flight across the relay. 0 turns the gate off. */
    val permits: Int,
    /** Replays that may wait for a permit before a new REQ is refused. */
    val maxWaiting: Int = DEFAULT_WAITING,
) {
    private val slots = if (permits > 0) Semaphore(permits) else null
    private val runningNow = AtomicInteger()
    private val waitingNow = AtomicInteger()
    private val waitedTotal = AtomicLong()
    private val refusedTotal = AtomicLong()

    /** Replays holding a permit, and waiting for one, right now. */
    val running: Int get() = runningNow.get()
    val waiting: Int get() = waitingNow.get()

    /** Since boot: replays that had to wait for a permit, and REQs refused because too many were waiting. */
    val waited: Long get() = waitedTotal.get()
    val refused: Long get() = refusedTotal.get()

    /** Whether [filters] replay under this gate: none ranks, since a ranked read waits on its connection's lane instead. */
    fun gates(filters: List<Filter>): Boolean = slots != null && filters.none { ReadShape.of(it).ranked }

    /** True, counted, when a REQ over [filters] would find no permit free and a queue already [maxWaiting] deep. */
    fun refuses(filters: List<Filter>): Boolean {
        if (!gates(filters) || waitingNow.get() < maxWaiting || slots!!.availablePermits > 0) return false
        refusedTotal.incrementAndGet()
        return true
    }

    /** A permit taken at once, or null when [filters] are not gated or none is free: then [through] waits for one. */
    fun tryEnter(filters: List<Filter>): Permit? {
        val slots = slots ?: return null
        if (!gates(filters) || !slots.tryAcquire()) return null
        return Permit(slots)
    }

    /**
     * Runs [block] holding a permit: [held] if [tryEnter] took one, else one waited for here. Released at the
     * first of the wrapped [onEose] firing or [block] leaving; a cancel while waiting leaves nothing held.
     */
    suspend fun <T> through(
        filters: List<Filter>,
        held: Permit?,
        onEose: () -> Unit,
        block: suspend (onEose: () -> Unit) -> T,
    ): T {
        val slots = slots
        if (slots == null || !gates(filters)) return block(onEose)
        val permit = held ?: waitFor(slots)
        try {
            return block {
                permit.release()
                onEose()
            }
        } finally {
            permit.release()
        }
    }

    private suspend fun waitFor(slots: Semaphore): Permit {
        waitingNow.incrementAndGet()
        waitedTotal.incrementAndGet()
        try {
            slots.acquire()
        } finally {
            waitingNow.decrementAndGet()
        }
        return Permit(slots)
    }

    /** One replay's hold on the gate, released once however many times it is asked. */
    inner class Permit internal constructor(
        private val slots: Semaphore,
    ) {
        private val held = AtomicBoolean(true)

        init {
            runningNow.incrementAndGet()
        }

        fun release() {
            if (!held.compareAndSet(true, false)) return
            runningNow.decrementAndGet()
            slots.release()
        }
    }

    /** A per-connection policy that refuses a REQ the gate [refuses], so the client hears `rate-limited:` and when to try again. */
    fun policy(): PassThroughPolicy = Refusing()

    private inner class Refusing : PassThroughPolicy() {
        override fun accept(cmd: ReqCmd): PolicyResult<ReqCmd> = if (refuses(cmd.filters)) PolicyResult.Rejected.rateLimited(BUSY, RETRY_AFTER_MS) else PolicyResult.Accepted(cmd)

        // A reconcile is not a replay; quartz would otherwise judge it as the REQ it reads like.
        override fun accept(cmd: NegOpenCmd): PolicyResult<NegOpenCmd> = PolicyResult.Accepted(cmd)
    }

    companion object {
        /** Unmeasured: a ceiling well above a calm relay's replays in flight and below the store client's 1,024 streams. */
        const val DEFAULT_PERMITS = 128

        /** Unmeasured: a reconnect wave of this many sockets each opening a feed queues whole. */
        const val DEFAULT_WAITING = 8_192

        /** What a refused REQ is told to wait before it asks again. */
        const val RETRY_AFTER_MS = 5_000L

        internal const val BUSY = "this relay is answering as many stored reads as it can; send the REQ again shortly"
    }
}
