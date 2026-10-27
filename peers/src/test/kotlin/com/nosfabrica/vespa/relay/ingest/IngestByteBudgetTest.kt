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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The relay's publishes may each be a full frame, so its pipeline bounds what it holds in bytes as
 * well as in events: past the budget a publish waits, as it does on a full channel, until a pass
 * that holds the budget ends.
 */
class IngestByteBudgetTest {
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    @Test
    fun `a budget admits while under its limit, and wakes a waiter on release`() =
        runBlocking {
            val budget = ByteBudget(limit = 100)
            budget.acquire(60)
            // Under the limit when it asked, so it goes through and crosses it.
            budget.acquire(60)
            assertEquals(120, budget.heldBytes)
            val waiter = async { budget.acquire(10) }
            delay(50)
            assertTrue(waiter.isActive, "admitted over a full budget")
            budget.release(60)
            withTimeout(2_000) { waiter.await() }
            assertEquals(70, budget.heldBytes)
        }

    @Test
    fun `one event larger than the whole budget still goes through`() =
        runBlocking {
            val budget = ByteBudget(limit = 100)
            withTimeout(2_000) { budget.acquire(10_000) }
            assertEquals(10_000, budget.heldBytes)
        }

    /**
     * A spent byte budget silences a producer's socket exactly as a full channel does: the send
     * waits in `acquire` before it is ever queued. So it counts as no headroom, and the producer
     * counts as parked on its relay — otherwise a walk resumed onto a full budget, or stalled by
     * one, read as the relay going quiet.
     */
    @Test
    fun `a spent byte budget is no headroom, and its waiter is parked on its relay`() =
        runBlocking {
            val relay = RelayUrlNormalizer.normalize("wss://source.example")
            val real = NostrSemanticsStore(InMemoryEventIndex())
            val gate = CompletableDeferred<Unit>()
            val writing = CompletableDeferred<Unit>()
            val store =
                object : IEventStore by real {
                    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
                        writing.complete(Unit)
                        gate.await()
                        return real.batchInsert(events)
                    }
                }
            val scope = CoroutineScope(Job())
            val first = note(1)
            // A queue far from half full by count, and a budget the first event alone spends.
            val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1, queue = 1_024, queueBytes = approxHeapBytes(first)), null, null, scope)
            pipeline.start()
            pipeline.submit(first, skipVerify = true, origin = IngestOrigin(relay))
            withTimeout(5_000) { writing.await() }

            assertTrue(!pipeline.hasHeadroom(), "one event of 1,024 queued, but no byte of the budget left")
            val waiter = scope.async { pipeline.submit(note(2), skipVerify = true, origin = IngestOrigin(relay)) }
            withTimeout(5_000) { while (pipeline.parkedOn(relay) < 1) delay(10) }
            assertEquals(1, pipeline.parkedOn(relay), "waiting on the budget is parked, like waiting on the channel")

            gate.complete(Unit)
            withTimeout(5_000) { waiter.await() }
            assertEquals(0, pipeline.parkedOn(relay), "and released once it gets in")

            pipeline.closeIntake()
            scope.cancel()
            pipeline.close()
        }

    @Test
    fun `a publish waits while a pass holds the budget, and goes once it ends`() =
        runBlocking {
            val relay = RelayUrlNormalizer.normalize("wss://here.example")
            val real = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
            val gate = CompletableDeferred<Unit>()
            val writing = CompletableDeferred<Unit>()
            val store =
                object : IEventStore by real {
                    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
                        writing.complete(Unit)
                        gate.await()
                        return real.batchInsert(events)
                    }
                }
            val scope = CoroutineScope(Job())
            val first = note(1)
            // A budget the first event alone fills.
            val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1, queue = 16, queueBytes = approxHeapBytes(first)), null, null, scope)
            pipeline.start()
            val heard = mutableListOf<IEventStore.InsertOutcome>()
            pipeline.submit(first, skipVerify = false) { synchronized(heard) { heard.add(it) } }
            withTimeout(5_000) { writing.await() }

            // The first event's pass is inside the store write and holds the whole budget.
            assertNull(withTimeoutOrNull(200) { pipeline.submit(note(2), skipVerify = false) { synchronized(heard) { heard.add(it) } } }, "admitted past a spent budget")

            gate.complete(Unit)
            withTimeout(5_000) { pipeline.submit(note(3), skipVerify = false) { synchronized(heard) { heard.add(it) } } }
            withTimeout(5_000) {
                while (synchronized(heard) { heard.size } < 2) delay(10)
            }
            assertTrue(synchronized(heard) { heard.all { it is IEventStore.InsertOutcome.Accepted } }, "$heard")

            pipeline.closeIntake()
            scope.cancel()
            pipeline.close()
        }
}
