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
import com.nosfabrica.vespa.relay.config.RelayIdentity
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
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
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
 * #   …AND THE DEPLOYMENT'S OWN KEY, which is the one relays allowlist:
 * #   -DabortCensusNsec=nsec1…
 * ```
 *
 * ## What it answered, 2026-09-04 — all 137, both streams, 20 minutes
 *
 * ```
 * visitsRun 1065   abortedVisits 421   abortedQuiet 417   abortedUnreachable 4
 * abortedUnpageable 0
 *
 * 58 relay(s) never aborted
 * 77 relay(s) aborted on BOTH streams
 *  2 relay(s) aborted on ONE stream — nostr.bitcoiner.social, relay.nmail.li
 * ```
 *
 * **The fault does not follow the stream, and it is not `unpageable`.** The
 * quiet aborts split 71 / 71 between the 141-kind ask and the 3-kind one —
 * identical counts, so width discriminates nothing — and 77 of the 79 failing
 * relays failed on both. #187's "109 failed on ONE stream" did not reproduce:
 * two did.
 *
 * **And what DID fail is a relay that connects and then never EOSEs**, which is
 * `PagedFetchResult.End.IDLE` and not the cursor at all. Every one of those
 * lines carries no page sample — correctly, because a silent relay sends
 * nothing to sample, and the instrument says nothing rather than "sent 0
 * events". A relay serving the wrong events would have shown its page here.
 *
 * So this environment does not reproduce production's 49%-unpageable, and the
 * likeliest missing ingredient is the identity above: run it with the
 * deployment's own nsec before drawing a conclusion about any relay on the
 * list.
 */
class AbortCensusLiveProbe {
    /**
     * ALL 137 RELAYS #187 NAMES — the 28 that failed on both outbox streams and
     * the 109 that failed on one, verbatim, as a resource rather than a literal
     * so the list is diffable against the issue.
     */
    private val urls: List<String> =
        System
            .getProperty("abortCensusUrls")
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?: javaClass.classLoader
                .getResourceAsStream("issue187-relays.txt")!!
                .bufferedReader()
                .readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") }

    /**
     * REAL RELAY LISTS FROM PRODUCTION, so the run starts from a store that has
     * seen this network rather than an empty one.
     *
     * Kind 10002s naming these relays in an `r` tag, pulled off the deployment's
     * own relay and inserted here. Two things it buys, and neither is the ask
     * shape — both outbox streams bind no authors (`RosterBuilder.asksOf`
     * returns one unbound ask when a source `select` binds nothing, and neither
     * stream's `relaySource` has one), so the REQ is identical either way:
     *
     *  - the ingest path behaves as production's does, refusing what the store
     *    already holds instead of accepting everything as new; and
     *  - the corpus is the one these relays actually serve, so what comes back
     *    is what a real visit would find.
     *
     * NIP-42 AND THE TRUST LENS, both handled the way `RelayListLiveProbe`
     * documents: the deployment answers a stranger's REQ with `auth-required`
     * naming the way out verbatim, so this signs a throwaway 22242 and, if the
     * ranked answer is still empty, re-asks with the NIP-50 `include:spam`
     * token the relay's own notice tells you to use.
     */
    private suspend fun seedRelayLists(
        store: com.vitorpamplona.quartz.nip01Core.store.IEventStore,
        chunks: List<List<String>>,
    ): Int {
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20)).build()
        val signer = NostrSignerInternal(KeyPair())
        var stored = 0
        for ((n, chunk) in chunks.withIndex()) {
            val got = java.util.concurrent.CountDownLatch(1)
            val seen = java.util.Collections.synchronizedList(mutableListOf<com.vitorpamplona.quartz.nip01Core.core.Event>())
            val unranked =
                java.util.concurrent.atomic
                    .AtomicBoolean(false)
            val rs = chunk.joinToString(",") { "\"${it.replace("\"", "")}\"" }
            val socket =
                okhttp.newWebSocket(
                    okhttp3.Request
                        .Builder()
                        .url(SEED_RELAY)
                        .build(),
                    object : okhttp3.WebSocketListener() {
                        fun req(
                            ws: okhttp3.WebSocket,
                            spam: Boolean,
                        ) {
                            val search = if (spam) ",\"search\":\"include:spam\"" else ""
                            ws.send("[\"REQ\",\"seed$n\",{\"kinds\":[10002],\"#r\":[$rs],\"limit\":500$search}]")
                        }

                        override fun onOpen(
                            ws: okhttp3.WebSocket,
                            response: okhttp3.Response,
                        ) = req(ws, false)

                        override fun onMessage(
                            ws: okhttp3.WebSocket,
                            text: String,
                        ) {
                            val frame =
                                runCatching {
                                    kotlinx.serialization.json.Json
                                        .parseToJsonElement(text)
                                        .jsonArray
                                }.getOrNull() ?: return
                            when (frame.firstOrNull()?.jsonPrimitive?.content) {
                                "EVENT" -> {
                                    runCatching {
                                        com.vitorpamplona.quartz.nip01Core.core.Event
                                            .fromJson(frame[2].toString())
                                    }.getOrNull()?.let(seen::add)
                                }

                                "AUTH" -> {
                                    val challenge = frame.getOrNull(1)?.jsonPrimitive?.content ?: return
                                    val auth =
                                        signer.signerSync.sign<com.vitorpamplona.quartz.nip01Core.core.Event>(
                                            System.currentTimeMillis() / 1000,
                                            22242,
                                            arrayOf(arrayOf("relay", SEED_RELAY), arrayOf("challenge", challenge)),
                                            "",
                                        )
                                    ws.send("[\"AUTH\",${auth.toJson()}]")
                                    req(ws, unranked.get())
                                }

                                // An empty first answer is the trust lens, not
                                // an empty corpus — one retry, unranked.
                                "EOSE" -> {
                                    if (seen.isEmpty() && unranked.compareAndSet(false, true)) {
                                        req(ws, true)
                                    } else {
                                        got.countDown()
                                    }
                                }

                                else -> {
                                    Unit
                                }
                            }
                        }

                        override fun onFailure(
                            ws: okhttp3.WebSocket,
                            t: Throwable,
                            response: okhttp3.Response?,
                        ) = got.countDown()
                    },
                )
            got.await(SEED_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
            runCatching { socket.close(1000, null) }
            for (event in seen.toList()) {
                runCatching { store.insert(event) }.onSuccess { stored++ }
            }
            println("  seeded chunk ${n + 1}/${chunks.size}: ${seen.size} relay list(s)")
        }
        return stored
    }

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
                // THE DEPLOYMENT'S OWN IDENTITY IF IT IS OFFERED, and this is
                // not a nicety: `RelayReachLiveProbe` measured sixteen of
                // #185's fifty "unreadable" relays serving us once NIP-42 was
                // answered, and the key relays ALLOWLIST is the deployment's,
                // not a throwaway. A run without it reads a relay's policy as
                // its behaviour.
                val identity =
                    System
                        .getProperty("abortCensusNsec")
                        ?.trim()
                        ?.takeIf { it.isNotEmpty() }
                        ?.let { RelayIdentity.signerFor(it) }
                        ?: NostrSignerInternal(KeyPair())
                println("  identity: ${if (System.getProperty("abortCensusNsec").isNullOrBlank()) "a THROWAWAY key" else "the nsec given"}")
                val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(identity.sign(template)) }
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
                println("SEEDING from $SEED_RELAY — kind 10002s naming these ${normalized.size} relays")
                val seeded = seedRelayLists(store, urls.chunked(SEED_CHUNK))
                println("  $seeded relay list(s) into the engine")
                println()

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
                        workers = WORKERS,
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
                // THE CENSUS PER RELAY, which is the deliverable at this size:
                // a list of 137 relays is unreadable as a log and the question
                // is per (relay, stream) anyway.
                val abortedUnits =
                    aborts
                        .mapNotNull { line ->
                            val parts =
                                line
                                    .substringAfter("router: visit ")
                                    .substringBefore(" aborted —")
                                    .trim()
                                    .split(" ")
                            if (parts.size >= 2) parts[1] to parts[0] else null
                        }.toSet()
                val bad = abortedUnits.map { it.first }.toSet()
                println("  %-4d relay(s) aborted on at least one stream".format(bad.size))
                println("  %-4d relay(s) aborted on BOTH streams".format(bad.count { r -> abortedUnits.count { it.first == r } > 1 }))
                println("  %-4d relay(s) aborted on ONE stream only — #187's shape".format(bad.count { r -> abortedUnits.count { it.first == r } == 1 }))
                println("  %-4d relay(s) never aborted".format(normalized.size - bad.size))
                println()
                if (bad.isNotEmpty()) {
                    println("  PER RELAY, the streams it aborted on:")
                    for (relay in bad.sorted()) {
                        println(
                            "    %-52s %s".format(
                                relay,
                                abortedUnits
                                    .filter { it.first == relay }
                                    .map { it.second }
                                    .sorted()
                                    .joinToString(),
                            ),
                        )
                    }
                    println()
                }
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

        /** The deployment's own relay — where the real relay lists come from. */
        private const val SEED_RELAY = "wss://search-staging.brainstorm.world"

        /** Urls per `#r` ask. Wide enough to be few round trips, narrow enough to be served. */
        private const val SEED_CHUNK = 20

        private const val SEED_WAIT_MS = 30_000L

        /**
         * Workers, and it is 274 units of work — 137 relays on two streams.
         *
         * Nowhere near the deployment's 96 per stream, and deliberately: this
         * shares one box with a Vespa and the point is to reach every relay
         * once, not to reproduce the pool's own contention. What that costs is
         * stated in the census output rather than hidden.
         */
        private const val WORKERS = 16

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
