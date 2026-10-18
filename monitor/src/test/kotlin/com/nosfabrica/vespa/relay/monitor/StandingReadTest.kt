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
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A pass that cannot read the verdicts it would publish against dials nothing and signs nothing. */
class StandingReadTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()
    private val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
    private val urls = listOf(RelayUrlNormalizer.normalize("wss://nos.example/"), RelayUrlNormalizer.normalize("wss://nos.example/alpha"))

    /** Reads fail, writes land: the failure the passes must not publish through. */
    private class Unreadable(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun <T : Event> query(filter: Filter): List<T> = throw IllegalStateException("the store cannot answer")
    }

    private val dials = AtomicInteger()

    private fun probe() =
        AliasProbe(
            fetch = { _, want, until, _ ->
                dials.incrementAndGet()
                AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want))
            },
            target = 40,
            page = 40,
            fallbackPage = 40,
        )

    private fun assertNothingSigned(
        store: NostrSemanticsStore,
        pass: String,
    ) = runBlocking {
        assertEquals(0, dials.get(), "the $pass pass dialled without the standing verdicts")
        assertTrue(store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND))).isEmpty(), "the $pass pass signed blind")
    }

    @Test
    fun `the fold skips a sweep whose standing read failed`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val fold = AliasFolding(aliases = RelayAliases(), record = RelayVerdictRecord(Unreadable(store), signer), probe = probe())

            assertEquals(0, fold.measure("t", urls, canDial = { true }))
            assertNothingSigned(store, "fold")
        }

    @Test
    fun `the stability gate skips a sweep whose standing read failed`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val pass = ConsistencyPass(consistency = RelayConsistency(), record = RelayVerdictRecord(Unreadable(store), signer), probe = probe())

            assertEquals(0, pass.measure("t", urls, canDial = { true }))
            assertNothingSigned(store, "stability")
        }

    @Test
    fun `fitness skips a batch whose standing read failed`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(Unreadable(store), signer),
                    probe = probe(),
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                    reconcile = { _, _ -> },
                )

            pass.measure("t", urls, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            assertNothingSigned(store, "fitness")
        }
}
