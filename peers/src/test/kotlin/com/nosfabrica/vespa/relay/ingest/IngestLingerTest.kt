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
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The linger as the relay's publishes meet it, on the real clock: what a publisher waits for its OK. */
class IngestLingerTest {
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private class Counting(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        val writes = AtomicInteger()

        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
            writes.incrementAndGet()
            return inner.batchInsert(events)
        }
    }

    private fun pipeline(
        store: IEventStore,
        concurrency: Int,
        lingerMs: Long,
        scope: CoroutineScope,
    ) = IngestPipeline(store, IngestTuning(concurrency, 1_000, queue = 1_000 * concurrency, lingerMs = lingerMs), null, null, scope)

    @Test
    fun `a lone publish on an idle relay is not held for the linger`() =
        runBlocking {
            val scope = CoroutineScope(Job())
            val pipeline = pipeline(NostrSemanticsStore(InMemoryEventIndex(), relay = null), 1, 60_000, scope).also { it.start() }
            try {
                val heard = CompletableDeferred<IEventStore.InsertOutcome>()
                val sentAt = System.nanoTime()
                pipeline.submit(note(1), skipVerify = false) { heard.complete(it) }
                assertEquals(IEventStore.InsertOutcome.Accepted, withTimeout(10_000) { heard.await() })
                val tookMs = (System.nanoTime() - sentAt) / 1_000_000
                assertTrue(tookMs < 5_000, "a lone publish took ${tookMs}ms against a 60s linger")
            } finally {
                scope.cancel()
                pipeline.close()
            }
        }

    @Test
    fun `a stream of publishes is answered once each, within the linger of being taken, on every worker count`() {
        for (workers in listOf(1, 2)) {
            runBlocking {
                val scope = CoroutineScope(Job())
                val store = Counting(NostrSemanticsStore(InMemoryEventIndex(), relay = null))
                val lingerMs = 300L
                val pipeline = pipeline(store, workers, lingerMs, scope).also { it.start() }
                try {
                    val events = (0 until 400).map { note(it) }
                    val heard = ConcurrentHashMap<Int, AtomicInteger>()
                    val worstMs = AtomicInteger()
                    events.forEachIndexed { i, e ->
                        val sentAt = System.nanoTime()
                        pipeline.submit(e, skipVerify = false) {
                            heard.computeIfAbsent(i) { AtomicInteger() }.incrementAndGet()
                            worstMs.accumulateAndGet(((System.nanoTime() - sentAt) / 1_000_000).toInt(), ::maxOf)
                        }
                        if (i % 4 == 3) delay(1)
                    }
                    withTimeout(20_000) { while (heard.size < events.size) delay(5) }
                    // A second answer would land in this window.
                    delay(50)
                    assertEquals(events.indices.toSet(), heard.keys)
                    heard.values.forEach { assertEquals(1, it.get()) }
                    // The linger, plus the in-memory writes ahead of it; far under a second unless an OK is held twice.
                    assertTrue(worstMs.get() < lingerMs + 1_500, "$workers worker(s): an OK took ${worstMs.get()}ms against a ${lingerMs}ms linger")
                    // Loose on purpose: the width itself is BatchFillTest's, on a clock the test owns.
                    assertTrue(store.writes.get() < events.size / 2, "$workers worker(s): ${store.writes.get()} writes for ${events.size} publishes")
                } finally {
                    scope.cancel()
                    pipeline.close()
                }
            }
        }
    }
}
