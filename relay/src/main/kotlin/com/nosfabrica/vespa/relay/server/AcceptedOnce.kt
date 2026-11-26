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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * [inner], answering `Accepted` once among the submissions of one id in flight together: the rest hear
 * [RejectionReason.DUPLICATE], as a resend after the write would. The store dedups regular events outside its
 * writer lock, so two lanes' batches may both accept one id, and `LiveEventStore` fans out every `Accepted`.
 * Remove when quartz's `LiveEventStore` fans an id out once across its concurrent submissions.
 */
internal class AcceptedOnce(
    private val inner: EventIngest,
) : EventIngest {
    private class InFlight {
        var submissions = 0
        var accepted = false
    }

    private val inFlight = ConcurrentHashMap<String, InFlight>()
    private val raced = AtomicLong()

    /** Acceptances answered as duplicates since boot: live deliveries that would have gone out twice. */
    val racedAccepts: Long get() = raced.get()

    override suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) {
        val id = event.id
        inFlight.compute(id) { _, entry -> (entry ?: InFlight()).also { it.submissions++ } }
        val left = AtomicBoolean()
        try {
            inner.submit(event, skipVerify) { outcome -> onComplete(if (left.compareAndSet(false, true)) answer(id, outcome) else outcome) }
        } catch (e: Throwable) {
            // A cancelled send may still have handed the event over; its callback then passes its outcome through.
            if (left.compareAndSet(false, true)) leave(id, accepted = false)
            throw e
        }
    }

    /** One submission of [id] is answered [outcome]; a second acceptance among those in flight is a duplicate. */
    private fun answer(
        id: String,
        outcome: IEventStore.InsertOutcome,
    ): IEventStore.InsertOutcome {
        if (!leave(id, accepted = outcome is IEventStore.InsertOutcome.Accepted)) return outcome
        raced.incrementAndGet()
        return IEventStore.InsertOutcome.Rejected(RejectionReason.DUPLICATE)
    }

    /** True when [accepted] and another submission of [id] in flight with this one was accepted first. */
    private fun leave(
        id: String,
        accepted: Boolean,
    ): Boolean {
        var again = false
        inFlight.computeIfPresent(id) { _, entry ->
            if (accepted) {
                again = entry.accepted
                entry.accepted = true
            }
            if (--entry.submissions == 0) null else entry
        }
        return again
    }

    override fun hasBacklog(): Boolean = inner.hasBacklog()

    override fun close() = inner.close()
}
