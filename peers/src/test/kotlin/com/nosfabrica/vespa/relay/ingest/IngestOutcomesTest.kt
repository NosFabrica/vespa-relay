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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Every submission that asks for its outcome hears it exactly once, and hears what the relay
 * would tell its publisher: the store's verdict, or the pipeline's own for what it dropped first.
 */
class IngestOutcomesTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun note(n: Int): Event = signer.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private fun profile(
        author: NostrSignerSync,
        at: Long,
    ): Event = author.sign(at, 0, emptyArray(), """{"name":"v$at"}""")

    private fun forge(event: Event) = Event(event.id, event.pubKey, event.createdAt, event.kind, event.tags, event.content, "f".repeat(128))

    /** Each offered event's outcomes, in arrival order of the answers, keyed by position in [offer]. */
    private fun outcomes(
        preload: List<Event>,
        offer: List<Event>,
        store: (InMemoryEventIndex) -> IEventStore = { NostrSemanticsStore(it, relay = relayUrl) },
    ): Map<Int, List<IEventStore.InsertOutcome>> =
        runBlocking {
            val index = InMemoryEventIndex()
            val target = store(index)
            preload.forEach { target.insert(it) }
            val scope = CoroutineScope(Job())
            val pipeline =
                IngestPipeline(
                    target,
                    IngestTuning(concurrency = 1, batch = 1000),
                    audit = null,
                    servingPressure = null,
                    scope = scope,
                    knownIds = index::existingIds,
                    newestVersions = { kind, authors ->
                        index
                            .search(EventQuery(kinds = listOf(kind), authors = authors))
                            .groupBy { it.pubkey }
                            .mapValues { (_, docs) -> docs.maxWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id }).let { AddressVersion(it.createdAt, it.id) } }
                    },
                )
            val heard = ConcurrentHashMap<Int, MutableList<IEventStore.InsertOutcome>>()
            val answers = AtomicInteger()
            // Queued before the workers start, so the whole offer drains as one batch.
            offer.forEachIndexed { i, e ->
                pipeline.submit(e, skipVerify = false) { outcome ->
                    heard.computeIfAbsent(i) { mutableListOf() }.add(outcome)
                    answers.incrementAndGet()
                }
            }
            pipeline.start()
            var waitedMs = 0
            while (answers.get() < offer.size && waitedMs < 10_000) {
                delay(5)
                waitedMs += 5
            }
            // A late second answer would land in this window.
            delay(50)
            scope.cancel()
            pipeline.close()
            heard
        }

    private fun assertRejected(
        prefix: String,
        outcome: IEventStore.InsertOutcome,
    ) {
        assertIs<IEventStore.InsertOutcome.Rejected>(outcome)
        assertTrue(outcome.reason.startsWith(prefix), "expected $prefix, got ${outcome.reason}")
    }

    @Test
    fun `fresh events are each accepted once`() {
        val offer = (0 until 300).map { note(it) }
        val heard = outcomes(emptyList(), offer)
        assertEquals(offer.indices.toSet(), heard.keys)
        heard.values.forEach { assertEquals(listOf<IEventStore.InsertOutcome>(IEventStore.InsertOutcome.Accepted), it) }
    }

    @Test
    fun `what the pipeline drops before the store is answered in the store's words`() {
        val held = (0 until 200).map { note(it) }
        val author = NostrSignerSync()
        val stored = profile(author, 2_000)
        val fresh = note(1_000)
        val offer =
            held + // 0..199 stored already: the id probe drops them
                listOf(
                    fresh, // 200 accepted
                    fresh, // 201 a second copy in the batch
                    forge(note(1_001)), // 202 never verifies
                    profile(author, 1_000), // 203 older than the stored version
                    profile(author, 3_000), // 204 newer: wins
                    profile(author, 2_500), // 205 newer than stored, but beaten in the batch
                )
        val heard = outcomes(held + stored, offer)

        assertEquals(offer.indices.toSet(), heard.keys, "every submission is answered")
        heard.forEach { (i, list) -> assertEquals(1, list.size, "submission $i answered ${list.size} times: $list") }
        (0 until 200).forEach { assertRejected("duplicate:", heard.getValue(it).single()) }
        assertEquals(IEventStore.InsertOutcome.Accepted, heard.getValue(200).single())
        assertRejected("duplicate:", heard.getValue(201).single())
        assertRejected("invalid:", heard.getValue(202).single())
        assertRejected("replaced:", heard.getValue(203).single())
        assertEquals(IEventStore.InsertOutcome.Accepted, heard.getValue(204).single())
        assertRejected("replaced:", heard.getValue(205).single())
    }

    @Test
    fun `a store that cannot write answers every submission as failed`() {
        // The last is a second copy of the first: it must not hear `duplicate` for an event never stored.
        val offer = (0 until 40).map { note(it) }.let { it + it.first() }
        val broken: (InMemoryEventIndex) -> IEventStore = { index ->
            object : IEventStore by NostrSemanticsStore(index, relay = relayUrl) {
                override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = error("engine down")
            }
        }
        val heard = outcomes(emptyList(), offer, broken)
        assertEquals(offer.indices.toSet(), heard.keys)
        heard.values.forEach { assertIs<IEventStore.InsertOutcome.Failed>(it.single()) }
    }
}
