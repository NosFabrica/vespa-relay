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
import com.nosfabrica.vespa.relay.config.MonitorConfig
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.test.Test

/**
 * One sweep of the monitor (fold, stability, fitness, through [AliasMonitor.runPass]) over a corpus of
 * kind-30166 records re-signed into an in-memory store, reporting where each pass's dial permits went.
 * `-DprofileProbe=true -DprofileRecords=<jsonl> -DprofileOut=<dir>`; see the companion for the rest.
 */
class MonitorProfileProbe {
    @Test
    fun profileASweep() {
        if (System.getProperty("profileProbe") != "true") {
            println("[skip] MonitorProfileProbe — set -DprofileProbe=true to dial the public internet")
            return
        }
        // A sandbox's JVM may be pointed at an HTTP proxy that cannot carry a websocket; production dials direct.
        if (System.getProperty("profileDirect") == "true") {
            listOf("https.proxyHost", "https.proxyPort", "http.proxyHost", "http.proxyPort").forEach(System::clearProperty)
        }
        // A line per connect and per CLOSED drowns the report; the passes' own lines are what is read.
        Log.minLevel = LogLevel.ERROR
        val out = File(System.getProperty("profileOut")).apply { mkdirs() }
        val concurrency = System.getProperty("profileConcurrency")?.toInt() ?: MonitorConfig.DEFAULT_DIAL_CONCURRENCY
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val signer = NostrSignerInternal(KeyPair())
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
        val seeded = runBlocking { seed(store, signer) }
        System.err.println("profile: seeded ${seeded.size} record(s) over ${seeded.map { RelayAliases.hostOf(it.url) }.toSet().size} host(s)")

        val peers = PeerClient(scope, signer, connectionTimeoutSec = CONNECT_SEC)
        val sockets = RelaySockets(peers.client, emptySet())
        val processors = Processors()
        val clock = Clock()
        val reach = ReachabilityProbe(null, threads = concurrency)

        fun probe(
            pass: String,
            target: Int,
        ) = AliasProbe.over(peers.client, target, observe = { url, ms, page -> clock.page(pass, url, ms, page) }) { CONNECT_SEC * 1000L }

        val world =
            StreamWorld(
                store,
                sources = null,
                probe = reach,
                monitorAuthors = listOf(signer.pubKey),
                self = signer.pubKey,
                tor = null,
                sockets = sockets,
                onProbeEvent = {},
                progress = processors.of(MonitorEngine.SOURCE_PROCESSOR),
            )
        val folding =
            AliasFolding(
                RelayAliases(),
                RelayVerdictRecord(store, signer),
                probe(MonitorEngine.FOLD_PROCESSOR, RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency,
                progress = processors.of(MonitorEngine.FOLD_PROCESSOR),
                world = { world.known },
            )
        val stability =
            ConsistencyPass(
                RelayConsistency(),
                RelayVerdictRecord(store, signer),
                probe(MonitorEngine.STABILITY_PROCESSOR, RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency,
                progress = processors.of(MonitorEngine.STABILITY_PROCESSOR),
                world = { world.known },
            )
        val fitness =
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe = probe(MonitorEngine.FITNESS_PROCESSOR, FitnessPass.FITNESS_TARGET),
                client = peers.client,
                progress = processors.of(MonitorEngine.FITNESS_PROCESSOR),
                document = RelayDocument(peers::httpFor, concurrency = concurrency),
                concurrency = concurrency,
                tlsCheck = { url -> clock.timed(MonitorEngine.FITNESS_PROCESSOR, url, "tls check") { reach.tls(url) } },
                world = { world.known },
                reconcile = { url, sliver ->
                    clock.timed(MonitorEngine.FITNESS_PROCESSOR, url, "neg-open") {
                        peers.client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = FitnessPass.NIP77_IDLE_MS)
                    }
                },
            )

        fun timedReach(
            pass: String,
            inner: suspend (NormalizedRelayUrl) -> Reach,
        ): suspend (NormalizedRelayUrl) -> Reach = { url -> clock.timed(pass, url, "pre-probe") { inner(url) }.also { clock.reached(pass, url, it) } }

        fun entry(
            name: String,
            run: suspend (String, List<NormalizedRelayUrl>, suspend (NormalizedRelayUrl) -> Reach, Sockets) -> Int,
        ) = object : AliasMonitor.Pass {
            override val progress = processors.of(name)

            override suspend fun measure(
                label: String,
                candidates: List<NormalizedRelayUrl>,
                reach: suspend (NormalizedRelayUrl) -> Reach,
                onEvent: suspend (Event) -> Unit,
                sockets: Sockets,
            ): Int {
                val startedMs = System.currentTimeMillis()
                try {
                    return run(label, candidates, timedReach(name, reach), sockets)
                } finally {
                    clock.passMs[name] = System.currentTimeMillis() - startedMs
                    System.err.println("profile: $name took ${(System.currentTimeMillis() - startedMs) / 1000}s")
                }
            }
        }
        val passes =
            listOf(
                entry(MonitorEngine.FOLD_PROCESSOR) { l, c, r, s -> folding.measure(l, c, { r(it) == Reach.REACHABLE }, {}, s) },
                entry(MonitorEngine.STABILITY_PROCESSOR) { l, c, r, s -> stability.measure(l, c, { r(it) == Reach.REACHABLE }, {}, s) },
                entry(MonitorEngine.FITNESS_PROCESSOR) { l, c, r, s -> fitness.measure(l, c, r, {}, s) },
            )
        val monitor = AliasMonitor(passes, scope, source = world, retryEveryMs = null)

        // Permit occupancy by stage, integrated off the progress rows: the share of a pass's dial budget each step holds.
        val sampler =
            scope.launch {
                var lastNs = System.nanoTime()
                var lastPrintMs = System.currentTimeMillis()
                while (isActive) {
                    delay(SAMPLE_MS)
                    val nowNs = System.nanoTime()
                    val dtMs = (nowNs - lastNs) / 1_000_000
                    lastNs = nowNs
                    for (p in processors.snapshot()) {
                        p.inFlight?.relays?.forEach { clock.occupied(p.name, it.stage, dtMs) }
                        val m = p.measuring
                        if (m != null && System.currentTimeMillis() - lastPrintMs > PRINT_MS) {
                            lastPrintMs = System.currentTimeMillis()
                            System.err.println("profile: ${p.name} ${m.attempted}/${m.toProbe} ${m.unit}, ${p.inFlight?.relays?.size ?: 0} held")
                        }
                    }
                }
            }
        val startedMs = System.currentTimeMillis()
        try {
            peers.connect()
            runBlocking {
                monitor.runPass()
                monitor.flushPending()
            }
            val sweepMs = System.currentTimeMillis() - startedMs
            val grades = runBlocking { gradesIn(store) }
            val report = clock.report(sweepMs, concurrency, world.lastDerivation, grades)
            File(out, "summary.txt").writeText(report)
            File(out, "urls.tsv").writeText(clock.perUrl())
            File(out, "grades.tsv").writeText(grades.entries.sortedBy { it.key }.joinToString("") { "${it.key}\t${it.value}\n" })
            System.err.println(report)
        } finally {
            sampler.cancel()
            runCatching { peers.close() }
            scope.cancel()
        }
    }

    /** The records, sampled by whole host and optionally stripped to their `d` tag, re-signed under [signer]. */
    private suspend fun seed(
        store: NostrSemanticsStore,
        signer: NostrSignerInternal,
    ): List<NormalizedRelayUrl> {
        val cold = System.getProperty("profileCold") == "true"
        val now = System.currentTimeMillis() / 1000
        val records =
            File(System.getProperty("profileRecords")).useLines { lines ->
                lines
                    .filter { it.isNotBlank() }
                    .mapNotNull { line ->
                        val o = Json.parseToJsonElement(line).jsonObject
                        val tags =
                            o
                                .getValue("tags")
                                .jsonArray
                                .map { t -> t.jsonArray.map { it.jsonPrimitive.content }.toTypedArray() }
                                .toTypedArray()
                        val url = tags.firstOrNull { it.size > 1 && it[0] == "d" }?.let { RelayUrlNormalizer.normalizeOrNull(it[1]) } ?: return@mapNotNull null
                        Triple(url, o.getValue("created_at").jsonPrimitive.long, tags)
                    }.toList()
            }
        val hosts = records.map { RelayAliases.hostOf(it.first.url) }.distinct()
        val keep =
            System.getProperty("profileHosts")?.toInt()?.let { n ->
                hosts.shuffled(Random(System.getProperty("profileSeed")?.toLong() ?: 1L)).take(n).toHashSet()
            }
        val taken = records.filter { keep == null || RelayAliases.hostOf(it.first.url) in keep }
        for ((url, createdAt, tags) in taken) {
            val event: Event =
                if (cold) {
                    signer.sign(now, RelayDiscoveryEvent.KIND, arrayOf(arrayOf("d", url.url)), "")
                } else {
                    signer.sign(createdAt, RelayDiscoveryEvent.KIND, tags, "")
                }
            store.insert(event)
        }
        return taken.map { it.first }
    }

    /** Each url's current fitness grade of ours, or `alias`/`ungraded`. */
    private suspend fun gradesIn(store: NostrSemanticsStore): Map<String, String> =
        store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND))).associate { e ->
            val d = e.tags.first { it[0] == "d" }[1]
            val grade = e.tags.firstOrNull { it.size > 2 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }?.get(1)
            d to (grade ?: "ungraded")
        }

    /** Every measurement of the sweep, keyed by pass. */
    private class Clock {
        val passMs = ConcurrentHashMap<String, Long>()

        /** pass/stage -> permit-milliseconds. */
        private val occupancy = ConcurrentHashMap<String, AtomicLong>()

        /** pass/what/class -> [count, total ms]. */
        private val totals = ConcurrentHashMap<String, LongArray>()

        /** pass -> url -> its own ledger. */
        private val urls = ConcurrentHashMap<String, ConcurrentHashMap<NormalizedRelayUrl, Ledger>>()

        /** Silent pages' quartz reasons, per pass. */
        private val reasons = ConcurrentHashMap<String, LongArray>()

        class Ledger {
            var reach: Reach? = null
            var reachMs = 0L
            var pages = 0
            var pageMs = 0L
            var otherMs = 0L
            val classes = StringBuffer()

            /** Each silent page's quartz reason, in order. */
            val reasons = StringBuffer()
        }

        private fun ledger(
            pass: String,
            url: NormalizedRelayUrl,
        ) = urls.getOrPut(pass) { ConcurrentHashMap() }.getOrPut(url) { Ledger() }

        private fun add(
            key: String,
            ms: Long,
        ) {
            val a = totals.getOrPut(key) { LongArray(2) }
            synchronized(a) {
                a[0]++
                a[1] += ms
            }
        }

        fun occupied(
            pass: String,
            stage: String,
            ms: Long,
        ) {
            occupancy.getOrPut("$pass\t$stage") { AtomicLong() }.addAndGet(ms)
        }

        fun page(
            pass: String,
            url: NormalizedRelayUrl,
            ms: Long,
            page: AliasProbe.Page,
        ) {
            val events = page.events
            val kind =
                when {
                    events == null && page.reason?.startsWith("cannot:") == true -> "connect-failed"
                    events == null -> "no-answer"
                    page.authRefused -> "auth"
                    events.isEmpty() && page.refusal != null -> "closed"
                    events.isEmpty() -> "empty-eose"
                    else -> "events"
                }
            add("$pass\tpage\t$kind", ms)
            if (events == null) {
                val why = (page.reason ?: "null").replace(Regex("[0-9]+"), "N").take(REASON_CHARS)
                val a = reasons.getOrPut("$pass\t$kind\t$why") { LongArray(2) }
                synchronized(a) {
                    a[0]++
                    a[1] += ms
                }
            }
            val l = ledger(pass, url)
            synchronized(l) {
                l.pages++
                l.pageMs += ms
                l.classes.append(LETTERS.getValue(kind))
                if (events == null) l.reasons.append(if (l.reasons.isEmpty()) "" else " | ").append(page.reason?.take(REASON_CHARS) ?: "-")
            }
        }

        fun reached(
            pass: String,
            url: NormalizedRelayUrl,
            reach: Reach,
        ) {
            val l = ledger(pass, url)
            synchronized(l) { l.reach = reach }
        }

        suspend fun <T> timed(
            pass: String,
            url: NormalizedRelayUrl,
            what: String,
            block: suspend () -> T,
        ): T {
            val startedNs = System.nanoTime()
            var outcome = "threw"
            try {
                return block().also { outcome = it?.toString() ?: "ok" }
            } finally {
                val ms = (System.nanoTime() - startedNs) / 1_000_000
                add("$pass\t$what\t${if (what == "neg-open") "done" else outcome}", ms)
                val l = ledger(pass, url)
                synchronized(l) { if (what == "pre-probe") l.reachMs += ms else l.otherMs += ms }
            }
        }

        fun report(
            sweepMs: Long,
            concurrency: Int,
            derivation: StreamWorld.Derivation,
            grades: Map<String, String>,
        ): String =
            buildString {
                appendLine("sweep ${sweepMs / 1000}s at $concurrency permit(s); ${derivation.candidates} candidate(s), ${derivation.heldOutDead} held out dead")
                for ((pass, ms) in passMs.entries.sortedBy { order(it.key) }) {
                    val permitMs = ms * concurrency
                    appendLine()
                    appendLine("== $pass: ${ms / 1000}s wall, ${permitMs / 1000} permit-s available, ${urls[pass]?.size ?: 0} url(s) touched")
                    appendLine("  permit occupancy by stage (sampled):")
                    occupancy.entries
                        .filter { it.key.startsWith("$pass\t") }
                        .sortedByDescending { it.value.get() }
                        .forEach { (k, v) -> appendLine("    %-24s %8ds  %5.1f%%".format(k.substringAfter('\t'), v.get() / 1000, 100.0 * v.get() / permitMs.coerceAtLeast(1))) }
                    appendLine("  timed calls:")
                    totals.entries
                        .filter { it.key.startsWith("$pass\t") }
                        .sortedByDescending { it.value[1] }
                        .forEach { (k, v) ->
                            appendLine("    %-34s n=%-7d %8ds  mean %6dms".format(k.substringAfter('\t').replace('\t', ' '), v[0], v[1] / 1000, v[1] / v[0].coerceAtLeast(1)))
                        }
                    appendLine("  urls by what their pages were (c connect-failed, n no-answer, e events, z empty, x closed, a auth):")
                    urls[pass]
                        .orEmpty()
                        .values
                        .groupBy { l ->
                            l.classes
                                .toString()
                                .toSet()
                                .sorted()
                                .joinToString("")
                                .ifEmpty { "-" } + " " + (l.reach ?: "")
                        }.entries
                        .sortedByDescending { e -> e.value.sumOf { it.pageMs + it.reachMs + it.otherMs } }
                        .take(TOP_BUCKETS)
                        .forEach { (k, ls) ->
                            val total = ls.sumOf { it.pageMs + it.reachMs + it.otherMs }
                            appendLine("    %-30s %6d url(s) %8ds  mean %6dms".format(k, ls.size, total / 1000, total / ls.size))
                        }
                    appendLine("  silent pages by quartz reason:")
                    reasons.entries
                        .filter { it.key.startsWith("$pass\t") }
                        .sortedByDescending { it.value[1] }
                        .take(TOP_BUCKETS)
                        .forEach { (k, v) -> appendLine("    %-70s n=%-6d %7ds".format(k.substringAfter('\t').replace('\t', ' '), v[0], v[1] / 1000)) }
                }
                appendLine()
                appendLine(
                    "grades: " +
                        grades.values
                            .groupingBy { it }
                            .eachCount()
                            .entries
                            .sortedByDescending { it.value }
                            .joinToString { "${it.key} ${it.value}" },
                )
            }

        private fun order(pass: String) = listOf(MonitorEngine.FOLD_PROCESSOR, MonitorEngine.STABILITY_PROCESSOR, MonitorEngine.FITNESS_PROCESSOR).indexOf(pass)

        fun perUrl(): String =
            buildString {
                appendLine("pass\turl\treach\treachMs\tpages\tpageMs\totherMs\tclasses\treasons")
                for ((pass, m) in urls) {
                    for ((url, l) in m) appendLine("$pass\t${url.url}\t${l.reach ?: ""}\t${l.reachMs}\t${l.pages}\t${l.pageMs}\t${l.otherMs}\t${l.classes}\t${l.reasons}")
                }
            }
    }

    companion object {
        /** The deployment's `connectionTimeout`, which is also the probes' idle window. */
        private const val CONNECT_SEC = 20L

        private const val SAMPLE_MS = 100L

        private const val PRINT_MS = 60_000L

        private const val REASON_CHARS = 60

        private const val TOP_BUCKETS = 14

        /** One letter per page outcome, as the report's legend spells them. */
        private val LETTERS = mapOf("connect-failed" to 'c', "no-answer" to 'n', "events" to 'e', "empty-eose" to 'z', "closed" to 'x', "auth" to 'a')
    }
}
