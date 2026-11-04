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
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A dial permit is held by the dial alone: a pass with one permit still pre-probes and fetches documents beside it. */
class DialPermitTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()
    private val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
    private val urls = listOf(RelayUrlNormalizer.normalize("wss://one.example"), RelayUrlNormalizer.normalize("wss://two.example"))

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    private fun probe(onDial: (NormalizedRelayUrl) -> Unit = {}) =
        AliasProbe(
            fetch = { url, want, until, _ ->
                onDial(url)
                AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want))
            },
            target = 40,
            page = 40,
            fallbackPage = 40,
        )

    /** Lets each caller through only once [parties] distinct callers are waiting, which one permit cannot hold. */
    private class Rendezvous(
        private val parties: Int,
    ) {
        private val arrived = ConcurrentHashMap.newKeySet<String>()
        private val all = CompletableDeferred<Unit>()

        suspend fun meet(who: String) {
            arrived += who
            if (arrived.size >= parties) all.complete(Unit)
            all.await()
        }
    }

    private fun fitness(
        concurrency: Int,
        document: RelayDocument? = null,
        reach: suspend (NormalizedRelayUrl) -> Reach = { Reach.REACHABLE },
        onDial: (NormalizedRelayUrl) -> Unit = {},
    ) = FitnessPass(
        record = RelayVerdictRecord(newStore(), signer),
        probe = probe(onDial),
        client = EmptyNostrClient(),
        progress = Processors().of("fitness"),
        document = document,
        gate = DialGate(concurrency),
        reconcile = { _, _ -> },
        reach = reach,
    )

    @Test
    fun `fitness pre-probes outside the dial permit`(): Unit =
        runBlocking {
            val both = Rendezvous(urls.size)
            // Each pre-probe waits for the other: one permit held across it could never let the second start.
            withTimeout(10_000) {
                fitness(concurrency = 1, reach = { url ->
                    both.meet(url.url)
                    Reach.REACHABLE
                }).measure(RunKind.SWEEP, urls)
            }
        }

    @Test
    fun `the stability gate pre-probes outside the dial permit`() =
        runBlocking {
            val both = Rendezvous(urls.size)
            val pass =
                ConsistencyPass(consistency = RelayConsistency(), record = RelayVerdictRecord(newStore(), signer), probe = probe(), gate = DialGate(1), canDial = { url ->
                    both.meet(url.url)
                    true
                })

            val decided =
                withTimeout(10_000) {
                    pass.measure(RunKind.SWEEP, urls)
                }
            assertEquals(urls.size, decided)
        }

    @Test
    fun `the fold pre-probes outside the dial permit`() =
        runBlocking {
            val both = Rendezvous(2)
            val hosts = listOf("one", "two").flatMap { listOf(RelayUrlNormalizer.normalize("wss://$it.example"), RelayUrlNormalizer.normalize("wss://$it.example/alpha")) }
            val fold =
                AliasFolding(aliases = RelayAliases(), record = RelayVerdictRecord(newStore(), signer), probe = probe(), gate = DialGate(1), canDial = { url ->
                    both.meet(RelayAliases.hostOf(url.url))
                    true
                })

            val learned =
                withTimeout(10_000) {
                    fold.measure(RunKind.SWEEP, hosts)
                }
            assertEquals(2, learned)
        }

    @Test
    fun `a url waiting on its document does not hold the dial permit`() {
        val servers = urls.indices.map { ServerSocket(0) }
        val local = servers.map { RelayUrlNormalizer.normalize("ws://127.0.0.1:${it.localPort}") }
        val dialled = local.associateWith { CountDownLatch(1) }
        // Each document is held open until the other url has been dialled, or until the server gives up.
        val gaveUp = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
        for ((i, server) in servers.withIndex()) {
            val other = local[1 - i]
            thread(isDaemon = true) {
                runCatching {
                    server.accept().use { socket ->
                        val input = socket.getInputStream().bufferedReader()
                        while (input.readLine().orEmpty().isNotEmpty()) Unit
                        if (!dialled.getValue(other).await(HOLD_SECONDS, TimeUnit.SECONDS)) gaveUp += local[i]
                    }
                }
            }
        }
        try {
            val document = RelayDocument(clientFor = { OkHttpClient() }, timeoutSeconds = 2 * HOLD_SECONDS)
            runBlocking {
                withTimeout(30_000) {
                    fitness(concurrency = 1, document = document) { url -> dialled[url]?.countDown() }
                        .measure(RunKind.SWEEP, local)
                }
            }
            assertTrue(dialled.values.all { it.count == 0L }, "both urls were dialled")
            assertEquals(emptySet(), gaveUp, "a url's dial waited behind another url's document")
        } finally {
            servers.forEach { it.close() }
        }
    }

    private companion object {
        const val HOLD_SECONDS = 3L
    }
}
