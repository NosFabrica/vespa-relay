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

import com.nosfabrica.vespa.relay.peers.PeerClient
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EoseMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.Command
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import okhttp3.Dns
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import java.io.File
import java.io.FilterInputStream
import java.io.FilterOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketAddress
import java.net.SocketException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import javax.net.SocketFactory
import kotlin.test.Test

/**
 * Holds ONE live subscription (`since` a few seconds ago) open on every url of a list at once, as a
 * fleet-wide live tail would, and reports how long the fleet takes to open, EOSE and deliver, then
 * breaks sockets from below and reports whether quartz brings each one back with its REQ replayed.
 * Asserts nothing. `-DfleetProbe=true -DfleetUrls=<file, one url per line>`; the prime list is
 * `node relay/tools/fetch-verdicts.mjs wss://search.brainstorm.world/ v.jsonl` then
 * `jq -r 'select(any(.tags[]; .[0]=="l" and .[1]=="prime")) | .tags[] | select(.[0]=="d") | .[1]' v.jsonl`.
 */
class LiveFleetProbe {
    @Test
    fun canOneClientHoldTheWholeFleetLive() {
        if (System.getProperty("fleetProbe") != "true") {
            println("[skip] LiveFleetProbe — set -DfleetProbe=true to dial the public internet")
            return
        }
        // A sandbox's JVM may be pointed at an HTTP proxy that cannot carry a websocket; production dials direct.
        if (System.getProperty("fleetDirect") == "true") {
            listOf("https.proxyHost", "https.proxyPort", "http.proxyHost", "http.proxyPort").forEach(System::clearProperty)
        }
        Log.minLevel = LogLevel.ERROR

        val listed =
            File(System.getProperty("fleetUrls"))
                .readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") }
        val urls = listed.filterNot { ".onion" in it }.map { RelayUrlNormalizer.normalize(it) }.distinct()
        val maxSockets = System.getProperty("fleetSockets")?.toInt() ?: PeerClient.MAX_CONCURRENT_SOCKETS
        val pingSeconds = System.getProperty("fleetPingSeconds")?.toLong() ?: PeerClient.PING_SECONDS
        val connectMinutes = System.getProperty("fleetConnectMinutes")?.toDouble() ?: 3.0
        val chaos = Chaos.parse(System.getProperty("fleetChaos") ?: "reset:1.0:3,blackhole:0.25:5")
        val sinceSeconds = System.getProperty("fleetSinceSeconds")?.toLong() ?: 5L
        // Unset is PeerClient's own resolver; 0 is the bare system resolver, the arm before it.
        val dnsConcurrency = System.getProperty("fleetDnsConcurrency")?.toInt() ?: PeerClient.MAX_CONCURRENT_LOOKUPS

        val sockets = ChaosSockets()
        val fleet = Fleet()
        val okhttp =
            PeerClient
                .relayHttpBuilder(CONNECT_SEC, if (dnsConcurrency > 0) PeerClient.relayDns(dnsConcurrency) else Dns.SYSTEM)
                .pingInterval(Duration.ofSeconds(pingSeconds))
                .socketFactory(sockets)
                .build()
                .apply { dispatcher.maxRequests = maxSockets }
        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        client.addConnectionListener(fleet.listener)

        println("=".repeat(120))
        println(
            "LIVE-FLEET — ${urls.size} url(s) (${listed.size - urls.size} .onion skipped, no Tor here), " +
                "dispatcher $maxSockets socket(s), ping ${pingSeconds}s, " +
                "dns ${if (dnsConcurrency > 0) "cached, $dnsConcurrency lookups at a time" else "system, unbounded"}, since now-${sinceSeconds}s, chaos $chaos",
        )
        println("=".repeat(120))

        urls.forEach { fleet.track(it) }
        fleet.startMs = System.currentTimeMillis()
        val since = fleet.startMs / 1000 - sinceSeconds
        client.subscribe(SUB, urls.associateWith { listOf(Filter(since = since)) }, null)

        val ticker = Ticker(fleet, client, okhttp, urls.size)
        ticker.runFor((connectMinutes * 60_000).toLong())
        fleet.connectReport(urls.size)

        for (round in chaos) {
            val connected = client.connectedRelaysFlow().value.filter { fleet.tracks.containsKey(it) }
            val servers = connected.map(::endpointOf).distinct().shuffled()
            val chosen = servers.take((servers.size * round.fraction).toInt()).toSet()
            val hit = connected.filter { endpointOf(it) in chosen }
            val atMs = System.currentTimeMillis()
            // Marked first: a reset is noticed within milliseconds, and an unmarked drop counts as organic.
            fleet.mark(hit, atMs)
            val broken = chosen.sumOf { sockets.breakServer(it, round.kind) }
            println("-".repeat(120))
            println("CHAOS ${round.kind} — broke $broken socket(s) on ${chosen.size} of ${servers.size} server(s), ${hit.size} of ${connected.size} connected url(s); watching ${round.minutes} min")
            ticker.runFor((round.minutes * 60_000).toLong())
            fleet.recoveryReport(round, hit, atMs)
        }
        fleet.churnReport()

        runCatching { client.unsubscribe(SUB) }
        runCatching { client.disconnect() }
        scope.cancel()
        okhttp.dispatcher.executorService.shutdownNow()
    }

    /** One way to break a live socket, how much of the fleet it hits, and how long recovery is watched. */
    private class Chaos(
        val kind: String,
        val fraction: Double,
        val minutes: Double,
    ) {
        override fun toString() = "$kind:$fraction:$minutes"

        companion object {
            fun parse(spec: String) =
                spec.split(",").map { it.trim() }.filter { it.isNotEmpty() && it != "none" }.map {
                    val (kind, fraction, minutes) = it.split(":")
                    require(kind == RESET || kind == BLACKHOLE) { "fleetChaos kind must be $RESET or $BLACKHOLE, got $kind" }
                    Chaos(kind, fraction.toDouble(), minutes.toDouble())
                }
        }
    }

    /** What one url did, as wall-clock milliseconds; 0 is never. */
    private class Track {
        @Volatile var dialMs = 0L

        @Volatile var openMs = 0L

        @Volatile var eoseMs = 0L

        @Volatile var firstEventMs = 0L

        @Volatile var closedSaid: String? = null

        @Volatile var lastFailure: String? = null

        @Volatile var reqSentMs = 0L

        @Volatile var lastOpenMs = 0L

        @Volatile var lastEoseMs = 0L

        @Volatile var lastEventMs = 0L

        @Volatile var lastDownMs = 0L

        val events = AtomicLong()
        val opens = AtomicInteger()
        val downs = AtomicInteger()
        val failures = AtomicInteger()

        /** Organic drops: down while open, and how long each took to open again. */
        val naturalDowns = AtomicInteger()
        val naturalBackMs = ConcurrentLinkedQueue<Long>()

        /** When the open socket last dropped on its own; cleared by the reopen that answers it. */
        @Volatile var droppedAtMs = 0L

        /** The first disconnect after a chaos break. */
        @Volatile var noticedMs = 0L

        /** Set while a chaos round owns this url's next reopen, so it is not counted as organic. */
        @Volatile var chaosAtMs = 0L
    }

    private class Fleet {
        val tracks = ConcurrentHashMap<NormalizedRelayUrl, Track>()
        val uniqueIds: MutableSet<String> = ConcurrentHashMap.newKeySet()
        val frames = AtomicLong()

        @Volatile var startMs = 0L

        fun track(url: NormalizedRelayUrl) {
            tracks[url] = Track()
        }

        fun mark(
            hit: List<NormalizedRelayUrl>,
            atMs: Long,
        ) = hit.forEach { url ->
            tracks[url]?.let {
                it.noticedMs = 0L
                it.chaosAtMs = atMs
            }
        }

        val listener =
            object : RelayConnectionListener {
                override fun onConnecting(relay: IRelayClient) {
                    val t = tracks[relay.url] ?: return
                    if (t.dialMs == 0L) t.dialMs = System.currentTimeMillis()
                }

                override fun onConnected(
                    relay: IRelayClient,
                    pingMillis: Int,
                    compressed: Boolean,
                ) {
                    val t = tracks[relay.url] ?: return
                    val now = System.currentTimeMillis()
                    if (t.openMs == 0L) t.openMs = now
                    if (t.droppedAtMs > 0L) {
                        t.naturalBackMs.add(now - t.droppedAtMs)
                        t.droppedAtMs = 0L
                    }
                    t.lastOpenMs = now
                    t.opens.incrementAndGet()
                }

                override fun onSent(
                    relay: IRelayClient,
                    cmdStr: String,
                    cmd: Command,
                    success: Boolean,
                ) {
                    if (cmd is ReqCmd && cmd.subId == SUB && success) tracks[relay.url]?.reqSentMs = System.currentTimeMillis()
                }

                override suspend fun onIncomingMessage(
                    relay: IRelayClient,
                    msgStr: String,
                    msg: Message,
                ) {
                    val t = tracks[relay.url] ?: return
                    val now = System.currentTimeMillis()
                    when (msg) {
                        is EventMessage -> {
                            if (msg.subId != SUB) return
                            frames.incrementAndGet()
                            uniqueIds.add(msg.event.id)
                            t.events.incrementAndGet()
                            if (t.firstEventMs == 0L) t.firstEventMs = now
                            t.lastEventMs = now
                        }

                        is EoseMessage -> {
                            if (msg.subId != SUB) return
                            if (t.eoseMs == 0L) t.eoseMs = now
                            t.lastEoseMs = now
                        }

                        is ClosedMessage -> {
                            if (msg.subId == SUB) t.closedSaid = msg.message
                        }

                        else -> {}
                    }
                }

                override fun onDisconnected(relay: IRelayClient) {
                    val t = tracks[relay.url] ?: return
                    val now = System.currentTimeMillis()
                    // Only a drop after an open is churn; a failed dial also lands here.
                    if (t.lastOpenMs > t.lastDownMs) {
                        t.downs.incrementAndGet()
                        if (t.chaosAtMs == 0L) {
                            t.naturalDowns.incrementAndGet()
                            t.droppedAtMs = now
                        }
                    }
                    if (t.chaosAtMs > 0L && t.noticedMs == 0L) t.noticedMs = now
                    t.lastDownMs = now
                }

                override fun onCannotConnect(
                    relay: IRelayClient,
                    errorMessage: String,
                ) {
                    val t = tracks[relay.url] ?: return
                    t.failures.incrementAndGet()
                    t.lastFailure = errorMessage
                }
            }

        fun connectReport(total: Int) {
            val all = tracks.values
            println("-".repeat(120))
            println("CONNECT — of $total url(s), measured from the subscribe call:")
            pct("dial started", all.map { it.dialMs })
            pct("socket open", all.map { it.openMs })
            pct("REQ answered by EOSE", all.map { it.eoseMs })
            pct("first live event", all.map { it.firstEventMs })
            val closed = all.filter { it.closedSaid != null }
            println(
                "  CLOSED our REQ: ${closed.size} — " +
                    closed
                        .groupingBy { it.closedSaid!!.substringBefore(":").take(30) }
                        .eachCount()
                        .entries
                        .sortedByDescending { it.value }
                        .take(6)
                        .joinToString { "${it.key}=${it.value}" },
            )
            val never = all.filter { it.openMs == 0L }
            println("  never opened: ${never.size} — ${failureClasses(never)}")
            println("  events: ${frames.get()} frame(s), ${uniqueIds.size} unique, from ${all.count { it.events.get() > 0 }} relay(s)")
        }

        fun recoveryReport(
            round: Chaos,
            hit: List<NormalizedRelayUrl>,
            atMs: Long,
        ) {
            val ts = hit.mapNotNull { tracks[it] }
            val reopened = ts.map { if (it.lastOpenMs > atMs) it.lastOpenMs else 0L }
            val reEosed = ts.map { if (it.lastEoseMs > atMs) it.lastEoseMs else 0L }
            val reEvents = ts.map { if (it.lastEventMs > atMs) it.lastEventMs else 0L }
            println("RECOVERY ${round.kind} — ${ts.size} url(s) hit, measured from the break:")
            pct("noticed (disconnected)", ts.map { it.noticedMs }, atMs)
            pct("socket reopened", reopened, atMs)
            pct("REQ replayed and EOSEd", reEosed, atMs)
            pct("live event again", reEvents, atMs)
            val stuck = ts.filterIndexed { i, _ -> reopened[i] == 0L }
            println("  not back: ${stuck.size} — ${failureClasses(stuck)}")
            ts.forEach { it.chaosAtMs = 0L }
        }

        fun churnReport() {
            val all = tracks.values
            val back = all.flatMap { it.naturalBackMs }.sorted()
            println("-".repeat(120))
            println(
                "ORGANIC CHURN over the whole run — ${all.sumOf { it.naturalDowns.get() }} drop(s) on ${all.count { it.naturalDowns.get() > 0 }} relay(s), " +
                    "${back.size} reopened; reopen p50 ${at(back, 0.5)} p90 ${at(back, 0.9)} max ${back.lastOrNull()?.let { "${it}ms" } ?: "-"}",
            )
            val flappers =
                tracks.entries
                    .sortedByDescending { it.value.downs.get() }
                    .take(8)
                    .filter { it.value.downs.get() > 1 }
            flappers.forEach { (url, t) -> println("  flapping ${url.url}: ${t.downs.get()} drop(s), ${t.opens.get()} open(s), last: ${t.lastFailure?.take(90)}") }
            println("TOTAL events: ${frames.get()} frame(s), ${uniqueIds.size} unique")
        }

        /** Percentiles of [values] (0 = never) after [fromMs], over all of them, so "never" weighs in. */
        private fun pct(
            label: String,
            values: List<Long>,
            fromMs: Long = startMs,
        ) {
            val got = values.filter { it > 0L }.map { it - fromMs }.sorted()
            val n = values.size.coerceAtLeast(1)
            val share = { q: Double -> got.getOrNull((n * q).toInt() - 1)?.let { "${it}ms" } ?: "never" }
            println(
                "  %-24s %5d/%-5d  p50 %-9s p90 %-9s p95 %-9s p99 %-9s last %s"
                    .format(label, got.size, values.size, share(0.5), share(0.9), share(0.95), share(0.99), got.lastOrNull()?.let { "${it}ms" } ?: "-"),
            )
        }

        private fun at(
            sorted: List<Long>,
            q: Double,
        ) = sorted.getOrNull(((sorted.size * q).toInt()).coerceAtMost(sorted.size - 1))?.let { "${it}ms" } ?: "-"

        private fun failureClasses(ts: Collection<Track>) =
            ts
                .groupingBy { it.lastFailure?.let(::failureClass) ?: if (it.dialMs == 0L) "never dialled" else "no answer yet" }
                .eachCount()
                .entries
                .sortedByDescending { it.value }
                .take(8)
                .joinToString { "${it.key}=${it.value}" }
    }

    /** One status line every [TICK_MS]: what is open now, against what the dispatcher admits. */
    private class Ticker(
        val fleet: Fleet,
        val client: NostrClient,
        val okhttp: OkHttpClient,
        val total: Int,
    ) {
        fun runFor(ms: Long) {
            val end = System.currentTimeMillis() + ms
            while (System.currentTimeMillis() < end) {
                Thread.sleep(minOf(TICK_MS, end - System.currentTimeMillis()).coerceAtLeast(1))
                val ts = fleet.tracks.values
                val heap = Runtime.getRuntime().let { (it.totalMemory() - it.freeMemory()) / (1 shl 20) }
                println(
                    "  +%4ds connected %4d/%d  opened-ever %4d  eosed-ever %4d  delivering %4d  events %7d (%6d unique)  dispatcher %4d running %4d queued  threads %4d  heap %4dMB"
                        .format(
                            (System.currentTimeMillis() - fleet.startMs) / 1000,
                            client.connectedRelaysFlow().value.size,
                            total,
                            ts.count { it.openMs > 0 },
                            ts.count { it.eoseMs > 0 },
                            ts.count { it.events.get() > 0 },
                            fleet.frames.get(),
                            fleet.uniqueIds.size,
                            okhttp.dispatcher.runningCallsCount(),
                            okhttp.dispatcher.queuedCallsCount(),
                            ManagementFactory.getThreadMXBean().threadCount,
                            heap,
                        ),
                )
            }
        }
    }

    /**
     * Hands OkHttp sockets it can break from below: [RESET] ends the read side as a relay dropping
     * the connection would, [BLACKHOLE] keeps the TCP session up but delivers and sends nothing, as a
     * dead NAT mapping does, which only the websocket ping can notice.
     */
    private class ChaosSockets : SocketFactory() {
        // OkHttp drops the client's EventListener for websockets, so a socket is found by what it dialled.
        private val byEndpoint = ConcurrentHashMap<String, MutableSet<ChaosSocket>>()

        /** Breaks every open socket to [endpoint]; urls sharing that server break with it. */
        fun breakServer(
            endpoint: String,
            kind: String,
        ): Int {
            val open = byEndpoint[endpoint].orEmpty().filterNot { it.isClosed }
            open.forEach { socket ->
                when (kind) {
                    RESET -> runCatching { socket.shutdownInput() }
                    BLACKHOLE -> socket.frozen = true
                }
            }
            return open.size
        }

        override fun createSocket(): Socket = ChaosSocket(this)

        override fun createSocket(
            host: String,
            port: Int,
        ): Socket = createSocket().apply { connect(InetSocketAddress(host, port)) }

        override fun createSocket(
            host: String,
            port: Int,
            localHost: InetAddress,
            localPort: Int,
        ): Socket = throw UnsupportedOperationException()

        override fun createSocket(
            host: InetAddress,
            port: Int,
        ): Socket = createSocket().apply { connect(InetSocketAddress(host, port)) }

        override fun createSocket(
            address: InetAddress,
            port: Int,
            localAddress: InetAddress,
            localPort: Int,
        ): Socket = throw UnsupportedOperationException()

        private class ChaosSocket(
            val owner: ChaosSockets,
        ) : Socket() {
            @Volatile var frozen = false

            @Volatile private var key: String? = null
            private val closedLatch = CountDownLatch(1)

            override fun connect(
                endpoint: SocketAddress,
                timeout: Int,
            ) {
                super.connect(endpoint, timeout)
                val dialled = (endpoint as InetSocketAddress).let { "${it.hostString}:${it.port}" }
                key = dialled
                owner.byEndpoint.computeIfAbsent(dialled) { ConcurrentHashMap.newKeySet() }.add(this)
            }

            override fun close() {
                key?.let { owner.byEndpoint[it]?.remove(this) }
                closedLatch.countDown()
                super.close()
            }

            /** While frozen, everything read is discarded and the reader waits for the close. */
            override fun getInputStream(): InputStream =
                object : FilterInputStream(super.getInputStream()) {
                    override fun read(): Int {
                        val one = ByteArray(1)
                        return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xff
                    }

                    override fun read(
                        b: ByteArray,
                        off: Int,
                        len: Int,
                    ): Int {
                        while (true) {
                            val n =
                                try {
                                    `in`.read(b, off, len)
                                } catch (e: SocketException) {
                                    if (!frozen) throw e
                                    -1
                                }
                            if (!frozen) return n
                            if (n < 0) {
                                closedLatch.await()
                                throw SocketException("Socket closed")
                            }
                        }
                    }
                }

            /** While frozen, writes vanish: the ping goes out to nobody and no pong comes back. */
            override fun getOutputStream(): OutputStream =
                object : FilterOutputStream(super.getOutputStream()) {
                    override fun write(b: Int) {
                        if (!frozen) out.write(b)
                    }

                    override fun write(
                        b: ByteArray,
                        off: Int,
                        len: Int,
                    ) {
                        if (!frozen) out.write(b, off, len)
                    }
                }
        }
    }

    private companion object {
        const val SUB = "fleet-live"
        const val RESET = "reset"
        const val BLACKHOLE = "blackhole"
        const val CONNECT_SEC = 10L

        const val TICK_MS = 10_000L

        /** The host and port OkHttp dials for [url], as [ChaosSockets] keys its sockets. */
        fun endpointOf(url: NormalizedRelayUrl) =
            url.url
                .replaceFirst(Regex("^ws"), "http")
                .toHttpUrl()
                .let { "${it.host}:${it.port}" }

        /** quartz's failure lines end in the exception's class; that, plus the HTTP code if any, is the class. */
        fun failureClass(message: String): String {
            val type =
                Regex("""\(([A-Za-z]+(Exception|Error))\)""")
                    .findAll(message)
                    .lastOrNull()
                    ?.groupValues
                    ?.get(1)
            val code = Regex("""Response: (\d{3})""").find(message)?.groupValues?.get(1)
            // The JVM throws one exception for NXDOMAIN and for a resolver that did not answer in time.
            val dns = if (type == "UnknownHostException") (if ("Temporary failure" in message) "(timed out)" else "(no such name)") else null
            return listOfNotNull(code?.let { "http $it" }, type, dns).joinToString(" ").ifEmpty { message.take(40) }
        }
    }
}
