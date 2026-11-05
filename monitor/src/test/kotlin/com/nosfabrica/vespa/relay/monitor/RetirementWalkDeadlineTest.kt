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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The boot retirement's edits read their records off the retirement's own scan, so walking our records
 * is never charged to one edit's deadline: a corpus slower to walk than an edit is allowed still retires.
 */
class RetirementWalkDeadlineTest {
    private val relay = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())

    /** The first whole-corpus query of [self]'s records (the retirement's own scan) answers at once; every later one takes [slowMs]. */
    private class SlowAfterFirstWalk(
        private val inner: IEventStore,
        private val self: String,
        private val slowMs: Long,
    ) : IEventStore by inner {
        val walkQueries = AtomicInteger()

        override suspend fun <T : Event> query(filter: Filter): List<T> {
            if (filter.authors == listOf(self) && filter.tags == null && walkQueries.incrementAndGet() > 1) delay(slowMs)
            return inner.query(filter)
        }
    }

    @Test
    fun `the boot retirement still retires when walking our records outlasts one edit's deadline`(): Unit =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = relay)
            val stale = (1..4).map { RelayUrlNormalizer.normalize("wss://stale$it.example") }
            for (url in stale) {
                inner.insert(
                    signer.sign(
                        EventTemplate<Event>(
                            nowSeconds(),
                            30166,
                            arrayOf(
                                arrayOf("d", url.url),
                                arrayOf("l", "prime", RelayVerdictRecord.FITNESS_NAMESPACE, "older rules", nowSeconds().toString(), "0"),
                                arrayOf("L", RelayVerdictRecord.FITNESS_NAMESPACE),
                            ),
                            "",
                        ),
                    ),
                )
            }
            // One page of the walk outlives one edit; the read deadline is widened so no single page is cut by it.
            val editDeadlineMs = 1_000L
            val store = SlowAfterFirstWalk(inner, signer.pubKey, slowMs = editDeadlineMs + 1_000)
            val record = RelayVerdictRecord(store, signer, readDeadlineMs = 10 * 60_000L, editDeadlineMs = editDeadlineMs)
            val retired = FitnessRetirement.retireStale(store, record, signer.pubKey)
            assertEquals(4, retired.staleEpochs, "the retirement's walk ran under its first edit's deadline, so no retirement landed")
            assertEquals(1, store.walkQueries.get(), "the boot walked our records more than once")
        }

    /** A store whose queries never answer. */
    private class SilentStore(
        inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun <T : Event> query(filter: Filter): List<T> = awaitCancellation()
    }

    @Test
    fun `a boot walk the store never answers fails within its page deadline, so the passes still start`(): Unit =
        runBlocking {
            val store = SilentStore(NostrSemanticsStore(InMemoryEventIndex(), relay = relay))
            val record = RelayVerdictRecord(store, signer)
            // The engine starts the passes once this returns or throws; a walk that waits forever starts none.
            withTimeout(5_000) {
                assertFailsWith<RelayVerdictRecord.ReadTimeout> { FitnessRetirement.retireStale(store, record, signer.pubKey, pageDeadlineMs = 50) }
            }
        }
}
