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
package com.nosfabrica.vespa.relay.ingest

import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withTimeoutOrNull
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The outcomes of one walk's submissions, so its band is recorded only once the store has
 * answered every one of them.
 *
 * A walk used to record its band as soon as the relay had sent the page, while the events sat
 * in [IngestPipeline]'s queue: a process that stopped then lost them, and the band said the
 * ground was covered, so nothing walked it again. [track] counts a submission before it is
 * made (the outcome can land before [IngestPipeline.submit] returns), and [await] suspends until
 * every tracked one is answered. It answers true only when each was stored or refused for good;
 * a [IEventStore.InsertOutcome.Failed] (the store's fault, or a shutdown) is ground not covered.
 * [awaitWithin] is the bounded wait: a verdict that does not come is ground not covered either.
 */
class IngestAcks {
    private val pending = MutableStateFlow(0)
    private val failed = AtomicInteger()

    /** `created_at` of each timed submission still waiting, as counts, so the newest is one lookup. */
    private val waitingAt = TreeMap<Long, Int>()

    private val newestFailedAt = AtomicLong(Long.MIN_VALUE)

    /** Untimed submissions waiting, and whether one failed: either leaves no bound to report. */
    private val untimedWaiting = AtomicInteger()
    private val untimedFailed = AtomicBoolean()

    /**
     * The callback for one submission. Call it once per submit, before the submit. [createdAt]
     * lets a wait that ends short say where its unconfirmed ground begins ([Verdicts]).
     */
    fun track(createdAt: Long? = null): (IEventStore.InsertOutcome) -> Unit {
        if (createdAt == null) untimedWaiting.incrementAndGet() else synchronized(waitingAt) { waitingAt.merge(createdAt, 1, Int::plus) }
        pending.update { it + 1 }
        return { outcome ->
            if (outcome is IEventStore.InsertOutcome.Failed) {
                failed.incrementAndGet()
                if (createdAt == null) untimedFailed.set(true) else newestFailedAt.accumulateAndGet(createdAt, ::maxOf)
            }
            if (createdAt == null) {
                untimedWaiting.decrementAndGet()
            } else {
                synchronized(waitingAt) { waitingAt.computeIfPresent(createdAt) { _, n -> (n - 1).takeIf { it > 0 } } }
            }
            pending.update { it - 1 }
        }
    }

    /** Waits for every tracked submission; true when none of them failed. */
    suspend fun await(): Boolean {
        pending.first { it == 0 }
        return failed.get() == 0
    }

    /** [await] for at most [timeoutMs], read once at the end, so an answer landing at the cut is counted. */
    suspend fun awaitWithin(timeoutMs: Long): Verdicts {
        withTimeoutOrNull(timeoutMs) { pending.first { it == 0 } }
        // Pending first: an outcome counts its failure before it stops being pending.
        val left = pending.value
        val failures = failed.get()
        return when {
            left > 0 -> Verdicts.Unanswered(left, newestUnconfirmed())
            failures > 0 -> Verdicts.Failed(failures, newestUnconfirmed())
            else -> Verdicts.Stored
        }
    }

    /** The newest `created_at` that failed or is still waiting; null when an untimed one is either, or none is. */
    private fun newestUnconfirmed(): Long? {
        // Waiting first: an untimed outcome records its failure before it stops waiting.
        if (untimedWaiting.get() > 0 || untimedFailed.get()) return null
        val waiting = synchronized(waitingAt) { waitingAt.lastEntry()?.key } ?: Long.MIN_VALUE
        return maxOf(waiting, newestFailedAt.get()).takeIf { it != Long.MIN_VALUE }
    }

    /**
     * How an [awaitWithin] ended. A short one carries [Failed.newestUnconfirmed]: every timed
     * submission newer than it was stored or refused for good.
     */
    sealed interface Verdicts {
        /** Every submission was stored or refused for good. */
        data object Stored : Verdicts

        /** Every submission was answered, and [count] of them failed. */
        class Failed(
            val count: Int,
            val newestUnconfirmed: Long? = null,
        ) : Verdicts

        /** [count] submissions had no answer at the deadline. */
        class Unanswered(
            val count: Int,
            val newestUnconfirmed: Long? = null,
        ) : Verdicts
    }
}
