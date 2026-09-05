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
import com.nosfabrica.vespa.relay.status.RelayStatusReport
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
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Stands the real [VisitPool] up on `contentViaOutbox`'s filter against two relays that refuse
 * its width and one control, with a live Vespa behind the ingest, and prints each relay's status
 * row, the abort counts and what landed in the store over several revisits. Asserts nothing.
 * Runs only with `WIDTH_RESCUE_VESPA` set to the engine url.
 */
class WidthRescueLiveProbe {
    /** Two relays that refuse `contentViaOutbox`'s width, and nos.lol as the control. */
    private val urls =
        listOf(
            "wss://purplerelay.com",
            "wss://git.cloistr.xyz",
            "wss://nos.lol",
        )

    @Test
    fun doesTheNarrowingActuallyGetUsEvents() {
        val vespa = System.getenv("WIDTH_RESCUE_VESPA")
        if (vespa == null) {
            println("[skip] WidthRescueLiveProbe — set WIDTH_RESCUE_VESPA to a live engine to dial the public internet")
            return
        }
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
                val widths = FilterWidths()
                val bands = SyncBands(null)
                val streams =
                    listOf(
                        SyncStream(
                            name = "contentViaOutbox",
                            dir = SyncDirection.DOWN,
                            // Bounded, unlike the deployment's: a relay judges width on the REQ, before an event moves.
                            filter = Filter(kinds = CONTENT_KINDS, limit = EVENTS_PER_ASK),
                            urls = urls.map { RelayUrlNormalizer.normalize(it) },
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
                        workers = 3,
                        widths = widths,
                    )
                client.connect()
                try {
                    pool.start()
                    // The cap outlives the visit, so the convergence is the sequence of caps across revisits.
                    val deadline = System.currentTimeMillis() + RUN_MS
                    while (System.currentTimeMillis() < deadline) {
                        delay(POLL_MS)
                        val rows = statusRows(bands, pool)
                        println(
                            "  [%4ds] %s".format(
                                (RUN_MS - (deadline - System.currentTimeMillis())) / 1000,
                                rows.joinToString("  ") { r ->
                                    "%s=%s%s".format(
                                        r.first
                                            .removePrefix("wss://")
                                            .removeSuffix("/")
                                            .take(18),
                                        r.second,
                                        r.third?.let { "(<=$it)" } ?: "",
                                    )
                                },
                            ),
                        )
                        if (rows.none { it.second == "refused" }) {
                            println("  …every relay is being served; stopping early")
                            break
                        }
                    }

                    val counts =
                        processors
                            .snapshot()
                            .single { it.name == "visits" }
                            .counts
                            .associate { it.name to it.value }
                    println("=".repeat(100))
                    println("WIDTH-RESCUE — ${counts["visitsRun"]} visit(s) run, ${counts["narrowedRelays"]} relay(s) narrowed")
                    println("=".repeat(100))
                    for (name in listOf(
                        "abortedVisits",
                        "abortedAuthRequired",
                        "abortedClosed",
                        "abortedQuiet",
                        "abortedUnreachable",
                        "abortedUnpageable",
                        "abortedGaveUp",
                        "abortedFailed",
                        "abortedBackpressured",
                        "negentropyRefused",
                        "poolReceived",
                    )) {
                        println("  %-24s %d".format(name, counts[name] ?: 0))
                    }

                    println("-".repeat(100))
                    val rows =
                        RelayStatusReport
                            .build(bands.snapshot(), pool.primeUnits(), System.currentTimeMillis() / 1000)
                            ?.get("rows")
                    println("  rows: $rows")

                    println("-".repeat(100))
                    println("  ingest accepted=${ingest.accepted.get()} rejected=${ingest.rejected.get()}")
                    // Through the store's own read path, so the count proves the events are servable.
                    for (kind in SAMPLE_KINDS) {
                        val held = store.count(Filter(kinds = listOf(kind), limit = 100_000))
                        if (held > 0) println("  kind %-6d %d event(s) stored".format(kind, held))
                    }
                    println("=".repeat(100))
                } finally {
                    runCatching { authenticator.destroy() }
                    runCatching { complaints.close() }
                    runCatching { client.close() }
                    scope.cancel()
                    ingest.closeIntake()
                    ingest.close()
                }
            }
        }
    }

    /** Each unit's (relay, status, learned width) through the report the page draws. */
    private fun statusRows(
        bands: SyncBands,
        pool: VisitPool,
    ): List<Triple<String, String, Int?>> =
        RelayStatusReport
            .build(bands.snapshot(), pool.primeUnits(), System.currentTimeMillis() / 1000)
            ?.get("rows")
            ?.jsonArray
            ?.map {
                Triple(
                    it.jsonObject["relay"]!!.jsonPrimitive.content,
                    it.jsonObject["syncStatus"]!!.jsonPrimitive.content,
                    it.jsonObject["kindCap"]?.jsonPrimitive?.intOrNull,
                )
            }.orEmpty()

    private companion object {
        /** `contentViaOutbox`'s own kinds. */
        val CONTENT_KINDS =
            listOf(
                0,
                1,
                9,
                11,
                14,
                20,
                21,
                22,
                24,
                40,
                41,
                42,
                54,
                1010,
                1063,
                1065,
                1068,
                1111,
                1163,
                1301,
                1311,
                1312,
                1313,
                1315,
                1337,
                1617,
                1618,
                1621,
                1622,
                1630,
                1631,
                1632,
                1633,
                1808,
                1985,
                2003,
                2004,
                2473,
                3302,
                5050,
                5100,
                5129,
                5250,
                5302,
                5303,
                6969,
                8333,
                9002,
                9041,
                9321,
                9734,
                9735,
                9736,
                9737,
                9802,
                10002,
                10003,
                10009,
                10040,
                10100,
                10154,
                11871,
                12473,
                15128,
                15129,
                30000,
                30001,
                30002,
                30003,
                30004,
                30005,
                30006,
                30009,
                30015,
                30017,
                30018,
                30019,
                30020,
                30023,
                30030,
                30054,
                30055,
                30063,
                30175,
                30176,
                30177,
                30267,
                30296,
                30297,
                30298,
                30311,
                30312,
                30313,
                30315,
                30382,
                30383,
                30384,
                30385,
                30392,
                30393,
                30394,
                30395,
                30402,
                30617,
                30620,
                30817,
                30818,
                31337,
                31871,
                31872,
                31873,
                31890,
                31922,
                31923,
                31924,
                31925,
                31990,
                32267,
                33401,
                33863,
                34139,
                34235,
                34236,
                34550,
                35128,
                35129,
                36787,
                38000,
                38192,
                38383,
                39000,
                39089,
                39092,
                39701,
                40002,
                40100,
                45001,
                45003,
                48106,
            )

        val SAMPLE_KINDS = listOf(0, 1, 3, 7, 1111, 10002, 30023, 30078)

        const val EVENTS_PER_ASK = 40

        /** Long enough for several untailed revisits. */
        const val RUN_MS = 1_200_000L
        const val POLL_MS = 30_000L
    }
}
