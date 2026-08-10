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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusalSink
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Who a store refusal gets blamed on.
 *
 * The pipeline reads outcome `i` as belonging to event `i` of the batch it
 * sent. That is the store's contract, and everywhere else a breach of it would
 * only skew a counter — here it decides which id goes into a filter that
 * suppresses that id forever. So the mismatch is checked rather than trusted,
 * and these pin both halves: attribution happens when the lists line up, and
 * stops when they do not.
 */
class IngestAttributionTest {
    private fun event(n: Int) =
        Event(
            id = "%064x".format(n),
            pubKey = "a1".repeat(32),
            createdAt = 1_700_000_000L + n,
            kind = 1,
            tags = emptyArray(),
            content = "e$n",
            sig = "b2".repeat(32),
        )

    /** Records what the pipeline decided to blame, and for what reason. */
    private class Recorder : RefusalSink {
        override val tracksOrigins = true

        val refusals: MutableList<Pair<String, String>> = Collections.synchronizedList(mutableListOf())

        override fun isSuppressed(event: Event) = false

        override fun onRefused(
            event: Event,
            origin: IngestOrigin,
            reason: String,
        ) {
            refusals.add(event.id to reason)
        }
    }

    /**
     * A store that answers with the right verdicts in the wrong quantity —
     * the contract breach the guard exists for. Interface delegation carries
     * everything except the one method under test.
     */
    private class Misaligning(
        private val inner: IEventStore,
        private val drop: Int,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = events.map { IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED) }.dropLast(drop)
    }

    private class Rejecting(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = events.map { IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED) }
    }

    private fun config() = RouterConfig(connectionTimeoutSec = 5, streams = emptyList(), ingestConcurrency = 1, ingestBatch = 8)

    private fun base(): IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    private suspend fun run(
        store: IEventStore,
        sink: Recorder,
        count: Int,
    ) {
        val scope = CoroutineScope(SupervisorJob())
        val pipeline = IngestPipeline(store, config(), null, null, scope, sink)
        try {
            pipeline.start()
            repeat(count) { pipeline.submit(event(it), skipVerify = true, IngestOrigin.Local) }
            // The pipeline is asynchronous by design; wait for the queue to
            // clear rather than for a fixed time.
            var spins = 0
            while (pipeline.queued.get() > 0 && spins++ < 400) delay(25)
            delay(200)
        } finally {
            pipeline.close()
            scope.cancel()
        }
    }

    @Test
    fun `an aligned batch blames each rejection on the event that earned it`() =
        runBlocking {
            val sink = Recorder()
            run(Rejecting(base()), sink, 8)

            assertEquals(8, sink.refusals.size, "every rejection should be attributed")
            assertEquals(
                (0 until 8).map { event(it).id }.toSet(),
                sink.refusals.map { it.first }.toSet(),
                "and to exactly the ids that were sent",
            )
        }

    @Test
    fun `a store that returns fewer outcomes than events attributes nothing`() {
        // SAFETY. `getOrNull(i)` alone would keep going here and blame the
        // wrong events — outcome i belongs to event i only while the lists are
        // the same length. Every id it named would be a candidate for
        // suppression it never earned, and a suppressed id is never downloaded
        // again to notice the mistake. Silence is the correct output; the
        // pipeline logs the breach once instead.
        runBlocking {
            val sink = Recorder()
            run(Misaligning(base(), drop = 3), sink, 8)

            assertTrue(
                sink.refusals.isEmpty(),
                "a misaligned batch must blame nobody, got ${sink.refusals.map { it.first.take(8) }}",
            )
        }
    }
}
