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
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A store too busy to take the monitor's writes delays the signed records, not by a whole sweep:
 * what a batch earned is replayed from memory once the store answers, and nothing is dialled again.
 */
class PendingWritesTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    /** Reads answer; writes hang while [wedged], as they did behind a full ingest queue on staging. */
    private class BusyWrites(
        private val inner: NostrSemanticsStore,
    ) : IEventStore by inner {
        @Volatile
        var wedged = true

        override suspend fun insert(event: Event) {
            if (wedged) CompletableDeferred<Unit>().await()
            inner.insert(event)
        }
    }

    private val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
    private val dials = AtomicInteger()

    /** A url [dark] names answers nothing at all, which the batch guard counts as our dialling failing. */
    private fun fitness(
        store: IEventStore,
        dark: (NormalizedRelayUrl) -> Boolean = { false },
    ) = FitnessPass(
        record = RelayVerdictRecord(store, signer),
        probe =
            AliasProbe(
                fetch = { at, want, until, _ ->
                    dials.incrementAndGet()
                    if (dark(at)) AliasProbe.Page(null) else AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want))
                },
                target = 40,
                page = 40,
                fallbackPage = 40,
            ),
        client = EmptyNostrClient(),
        progress = Processors().of("fitness"),
        // Long enough that a loaded test JVM is not mistaken for a wedged store.
        publishDeadlineMs = 2_000L,
        reconcile = { _, _ -> },
    )

    private suspend fun FitnessPass.sweep(urls: List<NormalizedRelayUrl>) = withTimeout(60_000) { measure(AliasMonitor.ALL_STREAMS, urls, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE) }

    private suspend fun gradeOf(
        store: IEventStore,
        url: NormalizedRelayUrl,
    ): Array<String>? =
        store
            .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))))
            .maxByOrNull { it.createdAt }
            ?.tags
            ?.firstOrNull { it.size > 2 && it[0] == RelayVerdictRecord.LABEL_TAG && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }

    private val servers = (0 until 5).map { url("wss://busy$it.example/") }

    @Test
    fun `verdicts the store would not take land once it answers, without dialling again`() =
        runBlocking {
            val store = BusyWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = null))
            val pass = fitness(store)

            pass.sweep(servers)
            assertTrue(servers.all { gradeOf(store, it) == null })

            val dialled = dials.get()
            store.wedged = false
            assertEquals(servers.size, pass.flushPending())

            assertTrue(servers.all { gradeOf(store, it)?.get(1) == Verdict.PRIME.value })
            assertEquals(dialled, dials.get(), "a retry is a write, never a measurement")
        }

    @Test
    fun `a batch the store stopped keeps its withdrawals and lands them later`() =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val root = url("wss://indexer.coracle.social/")
            val path = url("wss://indexer.coracle.social/alpha-charlie")
            RelayVerdictRecord(inner, signer).publishFitness(path, Verdict.PRIME.value, "answered 20 events", pageable = null, nip77 = null)
            val store = BusyWrites(inner)
            val pass = fitness(store)

            // Enough wedged writes in flight to stop the batch, which used to drop the withdrawal outright.
            pass.sweep(servers + root + path)
            assertEquals(Verdict.PRIME.value, gradeOf(store, path)?.get(1), "the fixture needs the withdrawal not to have landed")

            store.wedged = false
            pass.flushPending()

            assertNull(gradeOf(store, path))
            assertEquals(Verdict.PRIME.value, gradeOf(store, root)?.get(1))
        }

    @Test
    fun `a retried grade keeps the time it was measured, not the time it was written`() =
        runBlocking {
            val store = BusyWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = null))
            val pass = fitness(store)
            pass.sweep(servers.take(1))
            val measured = nowSeconds()

            delay(2_100)
            store.wedged = false
            pass.flushPending()

            val stamp = gradeOf(store, servers.first())!![RelayVerdictRecord.LABEL_MEASURED_AT_INDEX].toLong()
            assertTrue(stamp <= measured, "stamped $stamp, measured by $measured")
        }

    @Test
    fun `a fresh measurement replaces a write still waiting`() =
        runBlocking {
            val store = BusyWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = null))
            val pass = fitness(store)
            pass.sweep(servers)
            store.wedged = false

            pass.sweep(servers)

            assertEquals(0, pass.flushPending())
        }

    @Test
    fun `a batch that refuses to publish keeps the writes still waiting`() =
        runBlocking {
            val store = BusyWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = null))
            val dark = (0 until FitnessPass.GUARD_FLOOR).map { url("wss://dark$it.example/") }.toSet()
            val pass = fitness(store, dark = { it in dark })
            pass.sweep(servers)

            // Enough urls answering nothing that the guard drops the whole batch unwritten.
            pass.sweep(servers + dark)
            store.wedged = false

            assertEquals(servers.size, pass.flushPending(), "a refused batch threw away writes an earlier one earned")
            assertTrue(servers.all { gradeOf(store, it)?.get(1) == Verdict.PRIME.value })
        }

    @Test
    fun `a queued prime is never written over a stability refusal that came after it`() =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val store = BusyWrites(inner)
            val pass = fitness(store)
            val shuffler = servers.first()
            pass.sweep(listOf(shuffler))

            RelayVerdictRecord(inner, signer).publishConsistency(shuffler, consistent = false, first = 40, second = 40, shared = 3, score = 0.075, anchorDays = 7)
            store.wedged = false

            assertEquals(0, pass.flushPending())
            assertNull(gradeOf(store, shuffler), "a stale `prime` put a refused relay back on the roster")
        }

    @Test
    fun `the monitor replays pending writes on its own short clock`() =
        runBlocking {
            val flushed = AtomicInteger()
            val pass =
                object : AliasMonitor.Pass {
                    override suspend fun measure(
                        label: String,
                        candidates: List<NormalizedRelayUrl>,
                        reach: suspend (NormalizedRelayUrl) -> Reach,
                        onEvent: suspend (Event) -> Unit,
                        sockets: Sockets,
                    ): Int = 0

                    override suspend fun flushPending(): Int = flushed.incrementAndGet()
                }
            val scope = CoroutineScope(SupervisorJob())
            try {
                // The sweep is hours away; only the retry clock can call it.
                AliasMonitor(listOf(pass), scope, startupDelayMs = 3_600_000, retryEveryMs = 50).start()
                withTimeout(5_000) { while (flushed.get() < 2) delay(20) }
            } finally {
                scope.cancel()
            }
        }
}
