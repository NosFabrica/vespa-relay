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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * **A WEDGED PIPELINE MUST NOT READ AS A BUSY ONE.**
 *
 * The store's query client carries no read or call deadline on purpose, so a
 * request whose response never arrives suspends its caller for the life of the
 * process. Ingest makes three of those per batch — the id probe, the version
 * probe and the write — on eight workers that are the whole of ingest, and in
 * #167 all eight went into one. Nothing here ends that; cutting a pass would
 * discard a batch of good events that nothing re-offers, which is the worse
 * failure. What the router owes instead is an honest account of it, and it did
 * not have one: the queue sat at 8206/8192 and the health line called it
 * backpressure, while a thread dump showed every ingest thread parked in
 * `LinkedBlockingQueue.take` looking idle, because a suspended coroutine has no
 * frame and its pool thread goes back to `take()`.
 *
 * These pin the account: a worker inside a pass is visible from outside the
 * process, and a full queue holding one reads as `wedged` rather than as a
 * mirror going flat out.
 */
class IngestWedgeTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    /** Waits for [ready] or fails the test — never a fixed sleep, which is a guess about a loaded CI box. */
    private suspend fun settle(
        what: String,
        ready: () -> Boolean,
    ) {
        var waitedMs = 0
        while (!ready() && waitedMs < SETTLE_TIMEOUT_MS) {
            delay(5)
            waitedMs += 5
        }
        assertTrue(ready(), "timed out waiting for $what")
    }

    @Test
    fun `every worker held past the threshold is a wedge, whatever the queue depth`() =
        runBlocking {
            val scope = CoroutineScope(Job())
            val pipeline =
                IngestPipeline(
                    WedgedStore(),
                    IngestTuning(concurrency = 1, batch = 1000),
                    audit = null,
                    servingPressure = null,
                    scope = scope,
                    wedgeAfterMs = 50,
                )
            // TEN EVENTS, on a queue that holds thousands. This asserted the
            // opposite once — that an unfull queue is never a wedge — and it was
            // encoding how #167 PRESENTED rather than what a wedge is. Behind a
            // slow upstream every worker can be held with the queue nearly
            // empty, and the depth test rendered that as "keeping up".
            (0 until 10).map { note(it) }.forEach { pipeline.submit(it, skipVerify = true) }
            pipeline.start()
            settle("the worker to enter the wedged pass") { pipeline.inBatch() == 1 }
            settle("the held pass to age past the threshold") { pipeline.wedged() }

            assertTrue(pipeline.queued.get() < pipeline.capacity, "the queue must NOT be full for this to mean anything")
            assertEquals(1, pipeline.workersRunning(), "a wedge is workers held, not workers gone")
            scope.cancel()
            pipeline.close()
        }

    @Test
    fun `a worker still waiting on the channel means ingest is moving`() =
        runBlocking {
            val scope = CoroutineScope(Job())
            val pipeline =
                IngestPipeline(
                    WedgedStore(),
                    IngestTuning(concurrency = 2, batch = 1000),
                    audit = null,
                    servingPressure = null,
                    scope = scope,
                    wedgeAfterMs = 50,
                )
            // ONE event for TWO workers: the first takes it and never returns,
            // the second sits on the channel. That is a pipeline at half
            // capacity, not a stopped one, and the age of the held pass alone
            // must never say otherwise.
            pipeline.submit(note(0), skipVerify = true)
            pipeline.start()
            settle("one worker to enter the wedged pass") { pipeline.inBatch() == 1 }
            settle("the held pass to age well past the threshold") { pipeline.oldestBatchMs() >= 200 }

            assertEquals(false, pipeline.wedged(), "one worker on the channel is ingest still moving")
            scope.cancel()
            pipeline.close()
        }

    /** A store whose write suspends forever — the failure this bounds, with nothing else changed. */
    private class WedgedStore : IEventStore by NostrSemanticsStore(InMemoryEventIndex(), RelayUrlNormalizer.normalize("wss://here.example")) {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = awaitCancellation()
    }

    private companion object {
        const val SETTLE_TIMEOUT_MS = 10_000
    }
}
