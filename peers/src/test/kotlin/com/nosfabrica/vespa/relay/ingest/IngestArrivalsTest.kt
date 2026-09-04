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
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusalSink
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * What `submitted` counts, which is the ARRIVAL end of the ingest queue.
 *
 * The drain end (`accepted` + `rejected`) was the only rate the health line
 * carried, and on a full queue it reads `0 ev/s` for a store that has stopped
 * answering and for a fan-out that has gone quiet alike. The arrival count is
 * what tells those apart, so what it must and must not include is pinned here:
 * every event that entered the channel, and nothing the refusal filter turned
 * away before the channel — a suppression has its own counter, and an arrival
 * rate that a suppression storm holds up while the queue sits empty would
 * point the next reader at the store.
 */
class IngestArrivalsTest {
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

    /** Suppresses every event whose sequence number is odd, and blames nothing. */
    private class OddsSuppressed : RefusalSink {
        override val tracksOrigins = false

        override fun isSuppressed(event: Event) = event.createdAt % 2 == 1L

        override fun onRefused(
            event: Event,
            origin: IngestOrigin,
            reason: String,
        ) = Unit
    }

    private fun store(): IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    @Test
    fun `submitted counts what entered the queue and not what the filter turned away`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val pipeline = IngestPipeline(store(), IngestTuning(concurrency = 1, batch = 8), null, null, scope, null, null, OddsSuppressed())
            try {
                pipeline.start()
                repeat(20) { pipeline.submit(event(it), skipVerify = true, IngestOrigin.Local) }
                // Asynchronous by design: wait for the drain rather than a clock.
                var spins = 0
                while (pipeline.queued.get() > 0 && spins++ < 400) delay(25)
                delay(200)

                assertEquals(10L, pipeline.submitted.get(), "the ten even events reached the channel")
                assertEquals(10L, pipeline.suppressed.get(), "the ten odd ones were turned away before it")
                // The two ends of the queue reconcile once it is empty: what
                // arrived is what left, every one with a verdict.
                assertEquals(pipeline.submitted.get(), pipeline.accepted.get() + pipeline.rejected.get())
                assertEquals(0, pipeline.queued.get())
            } finally {
                pipeline.close()
                scope.cancel()
            }
        }
}
