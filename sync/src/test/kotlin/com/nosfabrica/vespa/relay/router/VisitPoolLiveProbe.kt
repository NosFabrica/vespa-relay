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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.RelayExcludes
import com.nosfabrica.vespa.relay.router.config.RelaySelect
import com.nosfabrica.vespa.relay.router.config.RelaySource
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.SyncDirection
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.discovery.AliasFolding
import com.nosfabrica.vespa.relay.router.discovery.AliasProbe
import com.nosfabrica.vespa.relay.router.discovery.FitnessPass
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.nosfabrica.vespa.relay.router.discovery.RelaySockets
import com.nosfabrica.vespa.relay.router.discovery.RelayVerdictRecord
import com.nosfabrica.vespa.relay.router.heal.HealQueue
import com.nosfabrica.vespa.relay.router.heal.Healer
import com.nosfabrica.vespa.relay.router.heal.WriteCapability
import com.nosfabrica.vespa.relay.router.progress.Processors
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
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
 * THE NEW PLANES AGAINST REAL RELAYS, end to end: the fitness pass earns
 * verdicts on live servers, the verdicts land on kind-30166 records in an
 * in-memory store, `RelayDiscovery` reads the roster back, and a
 * small [VisitPool] runs it — catch-up pages, tails, the works — printing what
 * each stage decided.
 *
 * This is the probe the pool shipped without, and the one that tells our
 * reading of a relay apart from the relay. The candidate list is chosen for
 * coverage of the verdict vocabulary: healthy strfry hosts, an auth wall
 * (`relays.diggoo.com` answers `auth-required` to an unknown key), and a name
 * that does not resolve.
 *
 * OFF by default, asserts NOTHING — it dials other people's servers, and any
 * of them declining an ephemeral key or being down today is an answer, not a
 * regression.
 *
 * ```
 * ./gradlew :sync:test --tests '*VisitPoolLiveProbe*' -DvisitPoolProbe=true --rerun -i
 * ```
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
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(FitnessPass.Verdict.PRIME.value)),
                ),
            maxAgeSeconds = 3600,
        )

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
                fitness.measure("live probe", candidates, canDial = { true }, onEvent = {}, sockets = AliasFolding.Sockets.NONE)
                println("=".repeat(78))
                println("fitness pass over ${candidates.size} url(s) in ${System.currentTimeMillis() - started}ms — records now say:")
                val roster = RelayDiscovery.discover(store, RelayDiscoveryConfig(listOf(probeSource(signer.pubKey)), 3600, RelayExcludes.NONE))
                println("  prime roster read back: ${roster.map { it.url.url }}")
                println("=".repeat(78))

                // ---- Plane two: the pool runs the roster it just read.
                val ingest =
                    IngestPipeline(
                        store,
                        RouterConfig(connectionTimeoutSec = 20, streams = emptyList(), ingestConcurrency = 1, ingestBatch = 200),
                        null,
                        null,
                        scope,
                        null,
                        null,
                    )
                ingest.start()
                val refused = RefusedIds.disabled()
                val probeStreams =
                    listOf(
                        SyncStream(
                            name = "liveProbe",
                            dir = SyncDirection.DOWN,
                            filter = Filter(kinds = listOf(1), limit = 50),
                            urls = emptyList(),
                            trusted = false,
                            discovery =
                                RelayDiscoveryConfig(
                                    sources =
                                        listOf(
                                            // The same source the roster printed above
                                            // read, identity and all: the two halves of
                                            // the probe have to be about one thing.
                                            probeSource(signer.pubKey),
                                        ),
                                    refreshSeconds = 3600,
                                    exclude = RelayExcludes.NONE,
                                ),
                        ),
                    )
                val bands = SyncBands(null)
                val pool =
                    VisitPool(
                        client = client,
                        bands = bands,
                        ingest = ingest,
                        pager =
                            NegentropyPager(
                                StoreWindowIndex(store),
                                ClientWindowSync(client, refused = refused),
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
                        visitConcurrency = 4,
                        tailBudget = 3,
                    )
                pool.start()
                // Long enough for the roster loop's first rebuild, a full
                // rotation of visits, tails to open, and — with a 3-tail budget
                // over a larger prime set — at least one eviction decision.
                delay(45_000)
                println("=".repeat(78))
                for (p in processors.snapshot()) {
                    println("  ${p.name}: ${p.phase} — " + p.counts.joinToString { "${it.name}=${it.value}" })
                }
                println("=".repeat(78))
            }
        } finally {
            scope.cancel()
        }
    }
}
