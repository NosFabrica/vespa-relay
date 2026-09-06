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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.monitor.AliasProbe
import com.nosfabrica.vespa.relay.monitor.FitnessPass
import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.progress.SyncProgress
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.sync.heal.WriteCapability
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Runs the fitness pass over a few live relays, reads the kind-30166 roster back from an
 * in-memory store and lets a small [VisitPool] with two streams work it, printing each stage's
 * decisions and the progress document. Asserts nothing. Selected by `-DvisitPoolProbe=true`.
 */
class VisitPoolLiveProbe {
    /** The kind-30166 source the loader writes for `filter = { "kinds": [30166], "#l": ["prime"] }`. */
    private fun probeSource(monitor: String) =
        RelaySource(
            selects = listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)),
            filter =
                Filter(
                    kinds = listOf(30166),
                    authors = listOf(monitor),
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(Verdict.PRIME.value)),
                ),
            maxAgeSeconds = 3600,
        )

    /** Healthy hosts, an auth wall and a name that does not resolve. */
    private val candidates =
        listOf(
            "wss://nos.lol",
            "wss://schnorr.me",
            "wss://sec01.nostr1.com",
            "wss://ribo.nostria.app",
            "wss://relays.diggoo.com",
            "wss://does-not-resolve.invalid",
        ).map { RelayUrlNormalizer.normalize(it) }

    @Test
    fun verdictsThenVisits() {
        if (System.getProperty("visitPoolProbe") != "true") {
            println("[skip] VisitPoolLiveProbe — set -DvisitPoolProbe=true to dial the public internet")
            return
        }
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val signer = NostrSignerInternal(KeyPair())
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
        val record = RelayVerdictRecord(store, signer)
        val processors = Processors()

        try {
            runBlocking {
                // ---- Plane one: the monitor earns verdicts on live servers.
                val fitness =
                    FitnessPass(
                        record = record,
                        probe = AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { 15_000L },
                        client = client,
                        foldedAway = { emptyMap() },
                        inconsistent = { emptySet() },
                        progress = processors.of("fitness"),
                    )
                val started = System.currentTimeMillis()
                fitness.measure("live probe", candidates, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                println("=".repeat(78))
                println("fitness pass over ${candidates.size} url(s) in ${System.currentTimeMillis() - started}ms — records now say:")
                val roster = RelayDiscovery.discover(store, RelayDiscoveryConfig(listOf(probeSource(signer.pubKey)), 3600, RelayExcludes.NONE))
                println("  prime roster read back: ${roster.map { it.url.url }}")
                println("=".repeat(78))

                // ---- Plane two: the pool runs the roster it just read.
                val ingest =
                    IngestPipeline(
                        store,
                        IngestTuning(concurrency = 1, batch = 200),
                        null,
                        null,
                        scope,
                        null,
                        null,
                    )
                ingest.start()
                val refused = RefusedIds.disabled()

                // Two streams over one roster, so every relay is two units sharing one socket.
                // The second is multi-kind because `rewalksCovered` reads per-kind spans.
                fun probeStream(
                    name: String,
                    filter: Filter,
                    visits: Int,
                    live: Int,
                    audit: Long? = null,
                ) = SyncStream(
                    name = name,
                    dir = SyncDirection.DOWN,
                    filter = filter,
                    urls = emptyList(),
                    trusted = false,
                    visitConcurrency = visits,
                    maxLiveConcurrency = live,
                    negentropySyncThePastSeconds = audit,
                    discovery =
                        RelayDiscoveryConfig(
                            // The same source the roster above read, identity and all.
                            sources = listOf(probeSource(signer.pubKey)),
                            refreshSeconds = 3600,
                            exclude = RelayExcludes.NONE,
                        ),
                )
                val probeStreams =
                    listOf(
                        probeStream("notes", Filter(kinds = listOf(1), limit = 50), visits = 4, live = 3),
                        // Budgets under the roster size, so the caps bite and `deferred` is nonzero.
                        probeStream("mixed", Filter(kinds = listOf(0, 3, 10002), limit = 50), visits = 2, live = 1, audit = 3600),
                    )
                val bands = SyncBands(null)
                val phases = StreamPhases()
                probeStreams.forEach { phases.register(it.name) }
                val pool =
                    VisitPool(
                        reads = ClientRelayReads(client),
                        bands = bands,
                        ingest = ingest,
                        pager =
                            NegentropyPager(
                                StoreWindowIndex(store),
                                ClientWindowSync(client, FilterWidths(), refused = refused),
                                SweepState(null),
                                NegPageTuning(target = 5_000, minTarget = 500, maxTarget = 50_000, slackSeconds = 60),
                            ),
                        healer =
                            Healer(client, store, HealQueue(), WriteCapability(), refused, null),
                        sockets = RelaySockets(client, emptySet()),
                        scope = scope,
                        rosterBuilder =
                            RosterBuilder(
                                store = store,
                                streams = probeStreams,
                                bands = bands,
                            ),
                        streams = probeStreams,
                        progress = processors.of("visits"),
                        phases = phases,
                        workers = VisitPool.workersFor(probeStreams),
                        limits = PoolLimits.of(probeStreams),
                    )
                println("workers for these streams: ${VisitPool.workersFor(probeStreams)} (the SUM of their dial widths)")
                pool.start()
                // Long enough for the first roster rebuild, a full rotation, tails, and one eviction.
                delay(45_000)
                println("=".repeat(78))
                for (p in processors.snapshot()) {
                    println("  ${p.name}: ${p.phase} — " + p.counts.joinToString { "${it.name}=${it.value}" })
                }
                println("=".repeat(78))
                println(
                    SyncProgress.document(
                        streams = phases.snapshot(),
                        processors = processors.snapshot(),
                        live = pool.livePool(),
                        nowSeconds = System.currentTimeMillis() / 1000,
                    ),
                )
                println("=".repeat(78))
            }
        } finally {
            scope.cancel()
        }
    }
}
