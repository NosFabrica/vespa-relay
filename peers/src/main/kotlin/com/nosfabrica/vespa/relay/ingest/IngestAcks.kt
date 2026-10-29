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
import java.util.concurrent.atomic.AtomicInteger

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
 *
 * [awaitWithin] is the wait a walk uses: a verdict that never comes is ground not covered too.
 * One batch pass held for 4 h 40 min on an unanswered store query (NosFabrica/vespa-relay#289)
 * kept nine visits in [await], each holding its slot and showing the walk it had already finished.
 */
class IngestAcks {
    private val pending = MutableStateFlow(0)
    private val failed = AtomicInteger()

    /** Submissions answered as failed so far. */
    val failures: Int get() = failed.get()

    /** Submissions tracked and not yet answered. */
    val unanswered: Int get() = pending.value

    /** The callback for one submission. Call it once per submit, before the submit. */
    fun track(): (IEventStore.InsertOutcome) -> Unit {
        pending.update { it + 1 }
        return { outcome ->
            if (outcome is IEventStore.InsertOutcome.Failed) failed.incrementAndGet()
            pending.update { it - 1 }
        }
    }

    /** Waits for every tracked submission; true when none of them failed. */
    suspend fun await(): Boolean {
        pending.first { it == 0 }
        return failed.get() == 0
    }

    /**
     * [await] for at most [timeoutMs]: false when one failed or when one is still unanswered
     * then, which [unanswered] tells apart. A late answer is still counted, and changes nothing.
     */
    suspend fun awaitWithin(timeoutMs: Long): Boolean = withTimeoutOrNull(timeoutMs) { await() } ?: false
}
