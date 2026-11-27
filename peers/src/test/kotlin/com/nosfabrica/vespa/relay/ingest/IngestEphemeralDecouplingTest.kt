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
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.client.EngineRefusesWrites
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * An ephemeral event (kinds 20000-29999, e.g. NIP-46's 24133) is never stored, so its answer (and
 * the fan-out that follows an Accepted) must not depend on the store write of the storable events
 * that happen to share its batch: neither on that write failing (a feed block) nor on its duration.
 */
class IngestEphemeralDecouplingTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private fun nip46(n: Int): Event = signer.sign(1_700_000_000L + n, 24133, arrayOf(arrayOf("p", "b2".repeat(32))), "ciphertext $n")

    /** The real store over an index whose writes are [onWrite] (the feed-block or slow-disk engine). */
    private fun storeOver(onWrite: suspend (List<EventDoc>) -> Unit): Pair<InMemoryEventIndex, IEventStore> {
        val inner = InMemoryEventIndex()
        val index =
            object : EventIndex by inner {
                override suspend fun put(doc: EventDoc) {
                    onWrite(listOf(doc))
                    inner.put(doc)
                }

                override suspend fun putAll(docs: List<EventDoc>) {
                    onWrite(docs)
                    inner.putAll(docs)
                }
            }
        return inner to NostrSemanticsStore(index, relay = relayUrl)
    }

    /** Submits [offer] before the worker starts, so it drains as ONE batch, as a publish burst does. */
    private suspend fun runBatch(
        store: IEventStore,
        index: InMemoryEventIndex,
        offer: List<Event>,
        scope: CoroutineScope,
    ): Pair<IngestPipeline, List<CompletableDeferred<IEventStore.InsertOutcome>>> {
        val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1000), audit = null, servingPressure = null, scope = scope, knownIds = index::existingIds)
        val heard = offer.map { CompletableDeferred<IEventStore.InsertOutcome>() }
        offer.forEachIndexed { i, e -> pipeline.submit(e, skipVerify = false) { heard[i].complete(it) } }
        pipeline.start()
        return pipeline to heard
    }

    /**
     * FEED BLOCK. The engine refuses every write (507 / INSUFFICIENT_STORAGE). The storable note is
     * rightly answered `error:`, but the ephemeral event needs no write at all; the store itself
     * answers an all-ephemeral batch Accepted without I/O. Sharing a batch with the note must not
     * turn its answer into the note's storage error (and with it, no fan-out).
     */
    @Test
    fun `an ephemeral event is accepted while the store refuses writes, even batched with a storable one`() =
        runBlocking<Unit> {
            val (index, store) = storeOver { throw IllegalStateException("batch insert failed", EngineRefusesWrites("the engine is refusing writes (INSUFFICIENT_STORAGE)")) }
            val scope = CoroutineScope(Job())
            val (pipeline, heard) = runBatch(store, index, listOf(note(1), nip46(2)), scope)
            try {
                val noteOutcome = assertNotNull(withTimeoutOrNull(10_000) { heard[0].await() }, "the note was never answered")
                val ephemeralOutcome = assertNotNull(withTimeoutOrNull(10_000) { heard[1].await() }, "the ephemeral event was never answered")
                // The note's answer is the pipeline's feed-block wording; that part is right.
                assertEquals(IngestPipeline.STORE_REFUSING_WRITES, noteOutcome, "the storable note hears the feed block")
                assertEquals(
                    IEventStore.InsertOutcome.Accepted,
                    ephemeralOutcome,
                    "an ephemeral event needs no store write, so a feed block must not refuse it; it heard $ephemeralOutcome",
                )
            } finally {
                pipeline.closeIntake()
                scope.cancel()
                pipeline.close()
            }
        }

    /** CONTROL (passes today): alone in its batch, the same ephemeral event is Accepted through the same feed block. */
    @Test
    fun `control - an ephemeral event alone in its batch is accepted while the store refuses writes`() =
        runBlocking<Unit> {
            val (index, store) = storeOver { throw IllegalStateException("batch insert failed", EngineRefusesWrites("the engine is refusing writes (INSUFFICIENT_STORAGE)")) }
            val scope = CoroutineScope(Job())
            val (pipeline, heard) = runBatch(store, index, listOf(nip46(2)), scope)
            try {
                assertEquals(IEventStore.InsertOutcome.Accepted, withTimeoutOrNull(10_000) { heard[0].await() })
            } finally {
                pipeline.closeIntake()
                scope.cancel()
                pipeline.close()
            }
        }

    /**
     * SLOW STORE. A write that takes 2 s (a busy engine, a long writer-lock wait) holds the note's
     * answer, rightly; the ephemeral event in the same batch needs no write and must be answered
     * without waiting for it. 500 ms is a quarter of the write and two orders of magnitude above what
     * an ephemeral verdict costs (a signature check and a kind test).
     */
    @Test
    fun `an ephemeral event's answer does not wait on a slow store write it does not need`() =
        runBlocking<Unit> {
            val writeMs = 2_000L
            val (index, store) = storeOver { delay(writeMs) }
            val scope = CoroutineScope(Job())
            val startedAt = System.nanoTime()
            val (pipeline, heard) = runBatch(store, index, listOf(note(1), nip46(2)), scope)
            try {
                val ephemeralOutcome = assertNotNull(withTimeoutOrNull(10_000) { heard[1].await() }, "the ephemeral event was never answered")
                val ephemeralMs = (System.nanoTime() - startedAt) / 1_000_000
                val noteOutcome = assertNotNull(withTimeoutOrNull(10_000) { heard[0].await() }, "the note was never answered")
                val noteMs = (System.nanoTime() - startedAt) / 1_000_000
                assertEquals(IEventStore.InsertOutcome.Accepted, noteOutcome)
                assertEquals(IEventStore.InsertOutcome.Accepted, ephemeralOutcome)
                assertTrue(noteMs >= writeMs, "the slow write did not happen (note answered in ${noteMs}ms)")
                assertTrue(
                    ephemeralMs < 500,
                    "the ephemeral event was answered after ${ephemeralMs}ms, behind a ${writeMs}ms store write it does not need (note answered at ${noteMs}ms)",
                )
            } finally {
                pipeline.closeIntake()
                scope.cancel()
                pipeline.close()
            }
        }
}
