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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The sweep's one walk belongs to the sweep, not to whichever reader started it: a reader's cancellation
 * leaves it running for the rest, and a failed walk fails its waiters once, as an ordinary exception.
 */
class SweepWalkSurvivesReaderCancelTest {
    private val relay = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val url = RelayUrlNormalizer.normalize("wss://r1.example")

    /** Every whole-corpus walk query of [self]'s records takes [pageMs]. */
    private class SlowWalks(
        private val inner: IEventStore,
        private val self: String,
        private val pageMs: Long,
    ) : IEventStore by inner {
        val walks = AtomicInteger()

        override suspend fun <T : Event> query(filter: Filter): List<T> {
            if (filter.authors == listOf(self) && filter.tags == null) {
                walks.incrementAndGet()
                delay(pageMs)
            }
            return inner.query(filter)
        }
    }

    @Test
    fun `a reader cancelled mid-walk does not take the sweep's one walk down with it`(): Unit =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
            RelayVerdictRecord(inner, signer).publishConsistency(url, true, 10, 10, 10, 1.0, 7)
            val store = SlowWalks(inner, signer.pubKey, pageMs = 1_000)
            val record = RelayVerdictRecord(store, signer)
            record.beginSweep()
            try {
                // The first reader installs the walk; the second arrives while it runs and waits on it.
                val first = async(Dispatchers.Default) { record.load(listOf(url)) }
                delay(300)
                val second = async(Dispatchers.Default) { record.load(listOf(url)) }
                delay(200)
                // As an edit's publish deadline, or the scheduler at shutdown, cancels the first reader.
                first.cancelAndJoin()
                val read = second.await()
                assertTrue(url in read.consistent, "the waiter's read lost the record")
                assertEquals(1, store.walks.get(), "the first reader's cancellation threw the walk away and the waiter walked again from the top")
            } finally {
                record.endSweep()
            }
        }

    @Test
    fun `a walk that outlives its page deadline fails every waiter once with ReadTimeout`(): Unit =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
            RelayVerdictRecord(inner, signer).publishConsistency(url, true, 10, 10, 10, 1.0, 7)
            val store = SlowWalks(inner, signer.pubKey, pageMs = 1_000)
            val record = RelayVerdictRecord(store, signer, readDeadlineMs = 300)
            record.beginSweep()
            try {
                val reads = (1..4).map { async(Dispatchers.Default) { runCatching { record.load(listOf(url)) } } }.awaitAll()
                for (read in reads) {
                    val failure = read.exceptionOrNull()
                    assertTrue(failure is RelayVerdictRecord.ReadTimeout, "a waiter saw $failure, not the read's own timeout")
                }
                assertEquals(1, store.walks.get(), "each waiter walked again after the walk failed")
                // The failure is not kept: a later read walks afresh.
                assertTrue(runCatching { record.load(listOf(url)) }.isFailure)
                assertEquals(2, store.walks.get(), "the read after a failed walk did not walk again")
            } finally {
                record.endSweep()
            }
        }
}
