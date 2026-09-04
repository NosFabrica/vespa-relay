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
 * Runs the real [VisitPool] over the relays issue 187 names, on both outbox
 * ask shapes at once with a live Vespa behind the ingest, then prints every
 * abort line with its page sample and a census of which relays aborted on
 * which stream. Asserts nothing. Selected by `ABORT_CENSUS_VESPA` (the engine
 * url); `-DabortCensusUrls`, `-DabortCensusMinutes` and `-DabortCensusNsec` tune it.
 */
class AbortCensusLiveProbe {
    /** The 137 relays issue 187 names, as a resource so the list is diffable against the issue. */
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
     * Seeds the store with the kind 10002s naming these relays, pulled off the
     * deployment's relay, so ingest refuses what production already holds. The
     * ask shape is unaffected: neither outbox stream binds authors. Answers
     * NIP-42 with a throwaway key and retries once with `include:spam` when the
     * ranked answer is empty.
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

                                // An empty first answer is the trust lens, not an empty corpus.
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
                // The key relays allowlist is the deployment's; without it a relay's policy reads as its behaviour.
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
                val pages = ClientRelayPages(client)
                val widths = FilterWidths()
                val bands = SyncBands(null)
                val normalized = urls.map { RelayUrlNormalizer.normalize(it) }
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

                // The pool writes its abort lines to stderr; captured so the census can group them.
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

        /** The deployment's own relay, where the real relay lists come from. */
        private const val SEED_RELAY = "wss://search-staging.brainstorm.world"

        /** Urls per `#r` ask: few round trips, still served. */
        private const val SEED_CHUNK = 20

        private const val SEED_WAIT_MS = 30_000L

        /** Far below the deployment's width on purpose: the point is to reach every relay once, beside a Vespa. */
        private const val WORKERS = 16

        /** A relay decides width on the REQ, not on the events, so a limit masks nothing. */
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
                "abortedBackpressured",
            )

        /** `contentViaOutbox`'s kinds from `router.conf.example`; [WidthRescueLiveProbe] pins the older list issue 185 quoted. */
        private val CONTENT_KINDS = listOf(0, 1, 5, 9, 11, 14, 20, 21, 22, 24, 40, 41, 42, 54, 62, 1010, 1063, 1065, 1068, 1111, 1163, 1301, 1311, 1312, 1313, 1315, 1337, 1617, 1618, 1621, 1622, 1630, 1631, 1632, 1633, 1808, 1985, 2003, 2004, 2473, 3302, 5050, 5100, 5129, 5250, 5302, 5303, 6969, 8333, 9002, 9041, 9321, 9734, 9735, 9736, 9737, 9802, 10002, 10003, 10009, 10040, 10100, 10154, 11871, 12473, 15128, 15129, 30000, 30001, 30002, 30003, 30004, 30005, 30006, 30009, 30015, 30017, 30018, 30019, 30020, 30023, 30030, 30054, 30055, 30063, 30175, 30176, 30177, 30267, 30296, 30297, 30298, 30311, 30312, 30313, 30315, 30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395, 30402, 30617, 30620, 30817, 30818, 31337, 31871, 31872, 31873, 31890, 31922, 31923, 31924, 31925, 31990, 32267, 33401, 33863, 34139, 34235, 34236, 34550, 35128, 35129, 36787, 38000, 38192, 38383, 39000, 39089, 39092, 39701, 40002, 40100, 45001, 45003, 48106)

        /** `profileViaOutbox`'s kinds, verbatim from `router.conf.example`. */
        private val PROFILE_KINDS = listOf(0, 10002, 10040)
    }
}
