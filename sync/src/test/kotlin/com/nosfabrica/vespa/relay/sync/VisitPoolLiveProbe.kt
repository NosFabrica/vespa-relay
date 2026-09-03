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
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(Verdict.PRIME.value)),
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

                // TWO STREAMS OVER ONE ROSTER, which is the whole point of the
                // probe now: the unit of work is a (relay, stream) pair, so
                // every relay here is two units that must run at once, over
                // ONE socket, each with its own tail carrying its own filter.
                // A single-stream probe cannot see any of that.
                //
                // The second is MULTI-KIND on purpose. `rewalksCovered` reads
                // a band's per-kind spans, and kinds do not cover the same
                // time on a real relay — a single-kind probe is exactly the
                // shape that hid the misfiled catch-up in the first place.
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
                            // The same source the roster printed above read,
                            // identity and all: the halves of the probe have
                            // to be about one thing.
                            sources = listOf(probeSource(signer.pubKey)),
                            refreshSeconds = 3600,
                            exclude = RelayExcludes.NONE,
                        ),
                )
                val probeStreams =
                    listOf(
                        probeStream("notes", Filter(kinds = listOf(1), limit = 50), visits = 4, live = 3),
                        // Small budgets deliberately: with more relays than
                        // permits the caps have to BITE, so `deferred` and the
                        // schedule rows carry real numbers instead of zeroes.
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
                // Long enough for the roster loop's first rebuild, a full
                // rotation of visits, tails to open, and — with a 3-tail budget
                // over a larger prime set — at least one eviction decision.
                delay(45_000)
                println("=".repeat(78))
                for (p in processors.snapshot()) {
                    println("  ${p.name}: ${p.phase} — " + p.counts.joinToString { "${it.name}=${it.value}" })
                }
                // THE DOCUMENT THE PAGE READS, in full: the four pools with a
                // `pool` word per held row, each stream's limits with what
                // they turned away, and the schedule rows. This is the half
                // that has only ever been asserted against hand-written JSON.
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
