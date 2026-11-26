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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A process shutdown writes what ingest already holds, within its bound, and answers every submission once either way. */
class IngestShutdownTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private class SlowStore(
        private val inner: IEventStore,
        private val perBatchMs: Long,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
            delay(perBatchMs)
            return inner.batchInsert(events)
        }
    }

    private class WedgedStore(
        inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = awaitCancellation()
    }

    /** Submits [count] events to a one-worker pipeline over [store], shuts it down, and returns what each heard. */
    private fun shutDownWith(
        store: IEventStore,
        count: Int,
        timeoutMs: Long,
    ): Pair<Boolean, Map<Int, List<IEventStore.InsertOutcome>>> =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 4), null, null, scope)
            val heard = ConcurrentHashMap<Int, MutableList<IEventStore.InsertOutcome>>()
            repeat(count) { i -> pipeline.submit(note(i), skipVerify = true) { heard.computeIfAbsent(i) { mutableListOf() }.add(it) } }
            pipeline.start()
            // The worker is inside its first pass before the shutdown starts.
            while (pipeline.inBatch() == 0) delay(5)
            val drained = shutDownIngest(listOf(pipeline), scope, timeoutMs)
            // A late or second answer would land in this window.
            delay(100)
            drained to heard
        }

    @Test
    fun `what ingest holds at shutdown is written before the writers stop`() {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
        val (drained, heard) = shutDownWith(SlowStore(store, perBatchMs = 100), count = 20, timeoutMs = 10_000)

        assertTrue(drained, "five slow batches fit the bound")
        assertEquals((0 until 20).toSet(), heard.keys)
        heard.forEach { (i, list) -> assertEquals(listOf<IEventStore.InsertOutcome>(IEventStore.InsertOutcome.Accepted), list, "submission $i") }
    }

    @Test
    fun `a store that never answers costs the bound, and every submission still hears once`() {
        val store = WedgedStore(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl))
        val started = System.nanoTime()
        val (drained, heard) = shutDownWith(store, count = 12, timeoutMs = 300)

        assertFalse(drained)
        assertTrue((System.nanoTime() - started) / 1_000_000 < 5_000, "the wait is bounded")
        assertEquals((0 until 12).toSet(), heard.keys, "queued events are answered too")
        heard.forEach { (i, list) ->
            assertEquals(1, list.size, "submission $i: $list")
            assertTrue(list.single() is IEventStore.InsertOutcome.Failed, "submission $i: $list")
        }
    }
}
