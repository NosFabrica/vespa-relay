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
 * THE WHOLE FIX, RUNNING: the real [VisitPool], the real relay client, real
 * relays that refuse us, and a real Vespa the events have to land in.
 *
 * Everything else written for #185 is a unit test over a fake, a browser probe
 * over a fixture, or a dial that asserts what a relay SAYS. None of them run
 * the machine. This does: it stands the pool up on a declared-`urls` stream
 * carrying `contentViaOutbox`'s real 141-kind filter, points it at relays that
 * are known to refuse exactly that, and then asks the three questions the issue
 * actually asked —
 *
 *  1. does a relay that refused us now DELIVER, and do its events reach the
 *     store (the convergence claim);
 *  2. does the abort partition name what happened, with the relay's own
 *     sentence (the instrument claim);
 *  3. does a relay that never had a problem still work (the no-regression
 *     claim, which is the one a fix like this quietly breaks).
 *
 * ## What it answered, 2026-09-03 — one run, three relays, a real Vespa
 *
 * ```
 * [ 300s] purplerelay.com=refused(<=17)   git.cloistr.xyz=complete(<=17)  nos.lol=paging
 * [ 330s] purplerelay.com=paging(<=8)     git.cloistr.xyz=complete(<=17)  nos.lol=paging
 *
 * 4 visit(s) run, 2 relay(s) narrowed, 1 abort (abortedClosed)
 * ingest accepted=1178 rejected=51   kinds 0/1/1111/10002/30023 in the engine
 * ```
 *
 * **Both width-capped relays now deliver, and one of them took two visits to
 * get there** — which is the `MAX_NARROWINGS` claim, observed rather than
 * argued. `git.cloistr.xyz` narrowed 139 → 69 → 34 → 17 inside its first visit
 * and was served; `purplerelay.com` spent the same three halvings, was still
 * refused at 17, and aborted — then the revisit five minutes later started from
 * the cap it had learned, narrowed 17 → 8, and paged its history back to 2023.
 * Its row carries `syncStatus: paging` AND the refusal that came before it,
 * which is the "coverage and a refusal are both true" case the report was
 * written for, live.
 *
 * `nos.lol` is the control and is untouched: no cap, no abort, current.
 *
 * **AND IT FOUND A BUG NO OTHER TEST COULD.** On the first run purplerelay
 * stopped narrowing after ONE halving while cloistr managed three, from
 * identical code. The relay's sentence arrives on quartz's CONNECTION listener
 * and the refusal on its SUBSCRIPTION listener, and quartz runs the second
 * first — so reading the sentence straight after a refused walk is a race, and
 * losing it makes `FilterWidths.learn` see nothing and stop. See
 * [RelayComplaints.awaitSince]. Every unit test passed throughout, because a
 * fake answers instantly.
 *
 * OFF by default and asserts NOTHING — it dials other people's servers and
 * writes to an engine. A relay that has changed its policy since the issue was
 * filed is a legitimate answer here, not a failure.
 *
 * ```
 * DOCKER_MIN_API_VERSION=1.24 dockerd &
 * VESPA_MEM_LIMIT=6g docker compose up -d vespa
 * until curl -sS http://localhost:19071/state/v1/health | grep -q '"code" : "up"'; do sleep 5; done
 * WIDTH_RESCUE_VESPA=http://localhost:8080 ./gradlew :sync:test --tests '*WidthRescueLiveProbe*' --rerun -i
 * ```
 */
class WidthRescueLiveProbe {
    /**
     * The relays this runs against.
     *
     * Two that refuse `contentViaOutbox`'s width and one that does not — the
     * control, because a chunking change that broke the ordinary relay would
     * pass every assertion about the two that needed it.
     */
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
                // The router's own wiring, both halves of it.
                val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(NostrSignerInternal(KeyPair()).sign(template)) }
                val complaints = ClientRelayComplaints(client)
                val widths = FilterWidths()
                val bands = SyncBands(null)
                val streams =
                    listOf(
                        SyncStream(
                            name = "contentViaOutbox",
                            dir = SyncDirection.DOWN,
                            // BOUNDED, unlike the deployment's. The width is what
                            // is under test and the limit does not touch it — a
                            // relay decides on the REQ, before an event moves —
                            // and without it this drains three strangers' whole
                            // corpora into a probe's engine.
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
                    // ACROSS REVISITS, which is the claim `MAX_NARROWINGS`
                    // rests on and the one no unit test can make: a visit pays
                    // at most three halvings, and the cap OUTLIVES the visit,
                    // so a relay whose limit is further down is reached over
                    // the next few. The untailed revisit base is five minutes,
                    // so this watches for several of them and prints each
                    // relay's row every time it polls — the convergence is the
                    // sequence of caps, not the last one.
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
                        // Done when nothing is still being refused — the point
                        // this run exists to reach — or when the walls are
                        // plainly not width.
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

                    // WHAT EACH RELAY ENDED UP AT, through the very report the
                    // status page draws — so a row here is the row an operator
                    // would read.
                    println("-".repeat(100))
                    val rows =
                        RelayStatusReport
                            .build(bands.snapshot(), pool.primeUnits(), System.currentTimeMillis() / 1000)
                            ?.get("rows")
                    println("  rows: $rows")

                    // AND THE ONLY ANSWER THAT COUNTS: what is in the store.
                    println("-".repeat(100))
                    println("  ingest accepted=${ingest.accepted.get()} rejected=${ingest.rejected.get()}")
                    // Through the store's own read path, which is also the one
                    // the relay serves clients from — a count taken any other
                    // way would not prove the events are servable.
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
        /** `contentViaOutbox`'s own kinds, as the issue quotes them. */
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

        /** A few of them, read back out of the engine at the end. */
        val SAMPLE_KINDS = listOf(0, 1, 3, 7, 1111, 10002, 30023, 30078)

        const val EVENTS_PER_ASK = 40

        /** Long enough for several untailed revisits (the base is five minutes). */
        const val RUN_MS = 1_200_000L
        const val POLL_MS = 30_000L
    }
}
