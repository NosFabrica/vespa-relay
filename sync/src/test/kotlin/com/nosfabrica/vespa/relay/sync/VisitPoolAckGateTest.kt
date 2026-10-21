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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A walk records its band only once the store has answered every event it read, and not at all
 * when one of them failed to reach it. Before this, the band was recorded when the relay had
 * sent the page, with the events still in ingest's queue: a stop then lost them, and the band
 * said the ground was covered, so nothing walked it again.
 */
class VisitPoolAckGateTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val notes = Filter(kinds = listOf(1))

    // Trusted, so the fixture's unsigned events reach the store instead of failing verification.
    private val stream = PoolFixture.stream("s", url, notes).copy(trusted = true)

    private val corpus = (1..3).map { PoolFixture.event(it, createdAt = 1_700_000_000L + it) }

    /** An engine whose writes throw, as a store that cannot take them does. */
    private class FailingWrites(
        inner: EventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        override suspend fun put(doc: EventDoc) = error("the engine refused the write")

        override suspend fun putAll(docs: List<EventDoc>) = error("the engine refused the write")
    }

    /** An engine whose writes wait for [open]. */
    private class GatedWrites(
        private val inner: EventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val open = CompletableDeferred<Unit>()

        override suspend fun put(doc: EventDoc) {
            open.await()
            inner.put(doc)
        }

        override suspend fun putAll(docs: List<EventDoc>) {
            open.await()
            inner.putAll(docs)
        }
    }

    @Test
    fun `a walk whose events did not reach the store records no band`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val bands = SyncBands(null)
                val fixture = PoolFixture(scope, listOf(stream), PoolFixture.holding(corpus), bands, index = FailingWrites())
                fixture.pool().start()
                fixture.awaitTail()
                assertNull(bands.band("s", url, notes), "the relay sent every event, but none is stored, so nothing is covered")
                assertEquals(1L, fixture.counts()["bandsWithheld"], "and the withheld band is counted")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a walk records its band only once the store has answered`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val bands = SyncBands(null)
                val engine = GatedWrites()
                val fixture = PoolFixture(scope, listOf(stream), PoolFixture.holding(corpus), bands, index = engine)
                fixture.pool().start()
                // The relay has answered, and the events wait in front of the store.
                withTimeout(10_000) { while (fixture.asked.isEmpty()) delay(10) }
                delay(500)
                assertNull(bands.band("s", url, notes), "read but not stored: not covered yet")
                assertTrue(fixture.tails.isEmpty(), "the visit is still waiting on the store")

                engine.open.complete(Unit)
                fixture.awaitTail()
                assertNotNull(bands.band("s", url, notes), "stored: covered")
                assertEquals(0L, fixture.counts()["bandsWithheld"])
                assertEquals(corpus.map { it.id }.toSet(), engine.storedIds(), "every event the band claims is in the store")
            } finally {
                scope.cancel()
            }
        }

    private suspend fun EventIndex.storedIds(): Set<String> = search(EventQuery()).map { it.id }.toSet()
}
