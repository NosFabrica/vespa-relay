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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.sync.heal.WriteCapability
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.time.Duration
import kotlin.test.Test

/**
 * DO #187'S RELAYS WORK ON EVERY STREAM — the real [VisitPool], both outbox ask
 * shapes, real relays, and a real Vespa the events have to land in.
 *
 * ## The question this exists for, and why a clean dial cannot answer it
 *
 * The issue splits its relays into 28 that failed on BOTH outbox streams and
 * **109 that failed on ONE**, and that second number is the sharpest fact in
 * it. A relay that refuses one stream and serves the other is the same server,
 * on the same socket, under the same pool and the same quartz — nothing about
 * its cursor can differ between the two. The ASK is what differs:
 * `contentViaOutbox` carries 141 kinds, `profileViaOutbox` carries 3. So a
 * fault that follows the STREAM is a fault of the filter, and one that follows
 * the RELAY is a fault of the relay, and the two want opposite fixes.
 *
 * Six shapes of clean dial were run against these relays first and every one
 * advanced its cursor or drained honestly — see `RelayComplianceProbe` and the
 * table in AGENTS.md. What a clean dial is not: 96-way visits, live tails on
 * shared sockets, real band state, a real ingest queue behind `onEvent`, and
 * the pool's own narrowing. This runs those.
 *
 * ## What it prints
 *
 * Every abort line the pool produces, with — since #187 — the PAGE that caused
 * it ([RelayPages]), then a census per (relay, stream). A relay that aborts on
 * `content` and completes on `profile` is the 109's shape reproduced, and the
 * sample on that line says which of the three faults it is: events of a kind
 * that was not asked for, events above the cursor, or events belonging to
 * another subscription entirely.
 *
 * BOUNDED by a `limit` on each filter, for [WidthRescueLiveProbe]'s reason: the
 * width is what is under test and a relay decides on the REQ before an event
 * moves, so the limit cannot mask a refusal and without it this drains
 * strangers' whole corpora into a probe's engine.
 *
 * OFF by default and asserts NOTHING — it dials other people's servers and
 * writes to an engine. A relay that has changed its policy since the issue was
 * filed is a legitimate answer here, not a failure.
 *
 * ```
 * DOCKER_MIN_API_VERSION=1.24 dockerd &
 * VESPA_MEM_LIMIT=6g docker compose up -d vespa
 * until curl -sS http://localhost:19071/state/v1/health | grep -q '"code" : "up"'; do sleep 5; done
 * ABORT_CENSUS_VESPA=http://localhost:8080 ./gradlew :sync:test --tests '*AbortCensusLiveProbe*' --rerun -i
 * #   …relays of your own: -DabortCensusUrls='wss://a.example,wss://b.example'
 * #   …and longer, since a revisit is five minutes: -DabortCensusMinutes=15
 * ```
 */
class AbortCensusLiveProbe {
    /**
     * Relays from #187's "failed on ONE stream" list — the important ones, and
     * the ones whose split between streams is the finding.
     */
    private val urls: List<String> =
        (
            System.getProperty("abortCensusUrls")
                ?: "wss://relay.primal.net,wss://nostr.mom,wss://eden.nostr.land,wss://nostr.land," +
                "wss://relay.nostr.net,wss://nostr.bitcoiner.social,wss://relay.nostrcheck.me," +
                "wss://nostr.azzamo.net,wss://relay.azzamo.net,wss://nostrrelay.win," +
                "wss://relay.wisp.talk,wss://freelay.sovbit.host,wss://nostr.hifish.org," +
                "wss://relay.nostr.info,wss://news.utxo.one,wss://nostr.slothy.win"
        ).split(",").map { it.trim() }.filter { it.isNotEmpty() }

    @Test
    fun doTheseRelaysWorkOnEveryStream() {
        val vespa = System.getenv("ABORT_CENSUS_VESPA")
        if (vespa == null) {
            println("[skip] AbortCensusLiveProbe — set ABORT_CENSUS_VESPA to a live engine to dial the public internet")
            return
        }
        val runMs = (System.getProperty("abortCensusMinutes")?.toLongOrNull() ?: DEFAULT_MINUTES) * 60_000
        val relayUrl = RelayUrlNormalizer.normalize("wss://probe.example")
        VespaEventStore.open(vespa, relay = relayUrl, autoDeploy = true).use { store ->
            runBlocking {
                val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
                val okhttp =
                    OkHttpClient
                        .Builder()
                        .connectTimeout(Duration.ofSeconds(15))
                        .pingInterval(Duration.ofSeconds(120))
                        .build()
                val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
                val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(NostrSignerInternal(KeyPair()).sign(template)) }
                val complaints = ClientRelayComplaints(client)
                // THE INSTRUMENT UNDER TEST as much as the relays are: this is
                // the first time it runs inside the real pool.
                val pages = ClientRelayPages(client)
                val widths = FilterWidths()
                val bands = SyncBands(null)
                val normalized = urls.map { RelayUrlNormalizer.normalize(it) }
                // BOTH STREAMS, which is the whole point — the same relays, the
                // same pool, two ask widths.
                val streams =
                    listOf(
                        SyncStream(
                            name = "contentViaOutbox",
                            dir = SyncDirection.DOWN,
                            filter = Filter(kinds = CONTENT_KINDS, limit = EVENTS_PER_ASK),
                            urls = normalized,
                            trusted = false,
                        ),
                        SyncStream(
                            name = "profileViaOutbox",
                            dir = SyncDirection.DOWN,
                            filter = Filter(kinds = PROFILE_KINDS, limit = EVENTS_PER_ASK),
                            urls = normalized,
                            trusted = false,
                        ),
                    )
                val ingest = IngestPipeline(store, IngestTuning(concurrency = 2, batch = 64), null, null, scope, null, null)
                ingest.start()
                val processors = Processors()
                val pool =
                    VisitPool(
                        reads = ClientRelayReads(client),
                        complaints = complaints,
                        pages = pages,
                        bands = bands,
                        ingest = ingest,
                        pager =
                            NegentropyPager(
                                StoreWindowIndex(store),
                                ClientWindowSync(client, widths, refused = RefusedIds.disabled()),
                                SweepState(null),
                                NegPageTuning(target = 5_000, minTarget = 500, maxTarget = 50_000, slackSeconds = 60),
                                complaints,
                            ),
                        healer = Healer(client, store, HealQueue(), WriteCapability(), RefusedIds.disabled(), null),
                        sockets = RelaySockets(client, emptySet()),
                        scope = scope,
                        rosterBuilder = RosterBuilder(store = store, streams = streams, bands = bands),
                        streams = streams,
                        progress = processors.of("visits"),
                        workers = 4,
                        widths = widths,
                    )
                client.connect()

                // THE ABORT LINES ARE THE DELIVERABLE, and the pool writes them
                // to stderr. Captured rather than watched, so the census below
                // can group them and nothing is lost to a scrolled log.
                val captured = ByteArrayOutputStream()
                val realErr = System.err
                System.setErr(PrintStream(captured, true))
                try {
                    pool.start()
                    val deadline = System.currentTimeMillis() + runMs
                    while (System.currentTimeMillis() < deadline) {
                        delay(POLL_MS)
                    }
                } finally {
                    System.setErr(realErr)
                    runCatching { pages.close() }
                    runCatching { complaints.close() }
                    runCatching { authenticator.destroy() }
                    runCatching { client.disconnect() }
                    ingest.closeIntake()
                    scope.cancel()
                }

                val log = captured.toString()
                val aborts = log.lines().filter { "aborted —" in it }
                println("=".repeat(110))
                println("ABORT CENSUS — ${normalized.size} relay(s) from #187, two streams, ${runMs / 60_000} minute(s)")
                println("=".repeat(110))
                val counts =
                    processors
                        .snapshot()
                        .single { it.name == "visits" }
                        .counts
                        .associate { it.name to it.value }
                for (name in ABORT_COUNTERS) {
                    counts[name]?.takeIf { it > 0 }?.let { println("  %-22s %d".format(name, it)) }
                }
                println("  %-22s %d".format("visitsRun", counts["visitsRun"] ?: 0))
                println()
                if (aborts.isEmpty()) {
                    println("  NO ABORTS. Every relay was served on both streams — which for the 109 is the")
                    println("  outcome to record and NOT the outcome to assume: this ran a few visits over a")
                    println("  handful of relays, not a 1,131-relay backlog at 96-way concurrency.")
                } else {
                    println("  THE LINES, which since #187 carry the page that caused them:")
                    for (line in aborts) println("    ${line.trim()}")
                }
                println("=".repeat(110))
            }
        }
    }

    companion object {
        private const val POLL_MS = 15_000L
        private const val DEFAULT_MINUTES = 8L

        /** Bounded for [WidthRescueLiveProbe]'s reason — a relay decides on the REQ, not on the events. */
        private const val EVENTS_PER_ASK = 200

        private val ABORT_COUNTERS =
            listOf(
                "abortedVisits",
                "abortedAuthRequired",
                "abortedClosed",
                "abortedQuiet",
                "abortedUnreachable",
                "abortedUnpageable",
                "abortedGaveUp",
                "abortedFailed",
            )

        /**
         * `contentViaOutbox`'s kinds, verbatim from `router.conf.example`.
         * Copied rather than shared with [WidthRescueLiveProbe]: that one holds
         * the list the ISSUE quoted for #185 and is pinned to it, and a probe
         * that silently drifted onto another's corpus would be measuring a
         * width nobody deploys.
         */
        private val CONTENT_KINDS = listOf(0, 1, 5, 9, 11, 14, 20, 21, 22, 24, 40, 41, 42, 54, 62, 1010, 1063, 1065, 1068, 1111, 1163, 1301, 1311, 1312, 1313, 1315, 1337, 1617, 1618, 1621, 1622, 1630, 1631, 1632, 1633, 1808, 1985, 2003, 2004, 2473, 3302, 5050, 5100, 5129, 5250, 5302, 5303, 6969, 8333, 9002, 9041, 9321, 9734, 9735, 9736, 9737, 9802, 10002, 10003, 10009, 10040, 10100, 10154, 11871, 12473, 15128, 15129, 30000, 30001, 30002, 30003, 30004, 30005, 30006, 30009, 30015, 30017, 30018, 30019, 30020, 30023, 30030, 30054, 30055, 30063, 30175, 30176, 30177, 30267, 30296, 30297, 30298, 30311, 30312, 30313, 30315, 30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395, 30402, 30617, 30620, 30817, 30818, 31337, 31871, 31872, 31873, 31890, 31922, 31923, 31924, 31925, 31990, 32267, 33401, 33863, 34139, 34235, 34236, 34550, 35128, 35129, 36787, 38000, 38192, 38383, 39000, 39089, 39092, 39701, 40002, 40100, 45001, 45003, 48106)

        /** `profileViaOutbox`'s kinds, verbatim from `router.conf.example`. */
        private val PROFILE_KINDS = listOf(0, 10002, 10040)
    }
}
