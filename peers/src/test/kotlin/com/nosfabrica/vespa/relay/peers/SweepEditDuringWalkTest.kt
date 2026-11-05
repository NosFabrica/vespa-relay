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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * An edit that lands while a sweep's walk is under way may land after the walk read its url, so the
 * walk's record of it is not trusted: the next edit of that url reads it by `#d` and keeps its tags.
 */
class SweepEditDuringWalkTest {
    private val relay = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val subject = RelayUrlNormalizer.normalize("wss://subject.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")

    /** Refuses the first walk of [self]'s records; the second reads the store, then waits for [release] before answering. */
    private class FailsThenHoldsWalk(
        private val inner: IEventStore,
        private val self: String,
    ) : IEventStore by inner {
        private val walks = AtomicInteger()
        val walkRead = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()

        override suspend fun <T : Event> query(filter: Filter): List<T> {
            if (filter.authors != listOf(self) || filter.tags != null) return inner.query(filter)
            if (walks.incrementAndGet() == 1) error("the store cannot answer")
            val page = inner.query<T>(filter)
            walkRead.complete(Unit)
            release.await()
            return page
        }
    }

    @Test
    fun `an edit landing while the walk is under way is read afresh, so the next edit keeps its tags`(): Unit =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
            // A record of the subject before the sweep, so the walk holds a version of it.
            assertNotNull(RelayVerdictRecord(inner, signer).publishDistinct(subject, 500, "3 compared peer(s)", 2))
            val store = FailsThenHoldsWalk(inner, signer.pubKey)
            val inserting = CompletableDeferred<Unit>()
            val proceed = CompletableDeferred<Unit>()
            val local = StoreWriter.local(inner)
            val holding =
                object : StoreWriter by local {
                    override suspend fun insert(event: Event) {
                        if (event.tags.any { it[0] == "d" && it[1] == subject.url } && !proceed.isCompleted) {
                            inserting.complete(Unit)
                            proceed.await()
                        }
                        local.insert(event)
                    }
                }
            val record = RelayVerdictRecord(store, signer, writer = holding)
            record.beginSweep()
            try {
                withTimeout(10_000) {
                    // The first walk fails, so this edit reads its record by `#d` and is held at its insert.
                    val consistency = async { record.publishConsistency(subject, true, 100, 100, 99, 0.99, 7) }
                    inserting.await()
                    // The next read walks again, reading the subject before the held edit lands.
                    val reading = async { record.load(listOf(other)) }
                    store.walkRead.await()
                    proceed.complete(Unit)
                    assertNotNull(consistency.await(), "the consistency edit did not land")
                    store.release.complete(Unit)
                    reading.await()
                    assertNotNull(record.publishFitness(subject, "prime", "answered", pageable = null, nip77 = null), "the fitness edit did not land")
                }
            } finally {
                record.endSweep()
            }
            val tags =
                inner
                    .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(subject.url))))
                    .maxBy { it.createdAt }
                    .tags
                    .map { it[0] }
            assertTrue(RelayVerdictRecord.SELF_CONSISTENT_TAG in tags, "the fitness edit rewrote the walk's older record, dropping the consistency verdict: $tags")
            assertTrue(RelayVerdictRecord.SAME_AS_TAG in tags && "l" in tags, "the record lost a pass's tags: $tags")
        }
}
