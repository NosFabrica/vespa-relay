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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.relay.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.ProxySelector
import java.net.SocketAddress
import java.net.URI
import java.time.Duration

/** Is this a hidden service? quartz's own rule, so the router and the normalizer agree. */
internal fun isOnion(url: NormalizedRelayUrl): Boolean = RelayUrlNormalizer.isOnion(url.url)

/** Static `urls` this deployment cannot dial without Tor, as `stream: url` strings for the boot line. */
fun onionUpstreams(streams: List<SyncStream>): List<String> = streams.flatMap { s -> s.urls.filter(::isOnion).map { "${s.name}: ${it.url}" } }

/** Where the SOCKS5 proxy is and how hard to lean on it. Null is the no-Tor deployment. */
data class TorSettings(
    val socksHost: String,
    val socksPort: Int,
    // SYNC_TOR_ALL: every upstream through Tor, not only .onion ones.
    val routeAll: Boolean,
    val connectTimeoutSec: Long,
    val maxSockets: Int,
) {
    val socksAddress: String get() = "$socksHost:$socksPort"

    companion object {
        /** A connect timeout, not an idle window: a circuit plus a rendezvous is seconds of work. */
        const val DEFAULT_CONNECT_TIMEOUT_SEC = 90L

        const val DEFAULT_MAX_SOCKETS = 32

        const val PROBE_TIMEOUT_MS = 3_000

        /** How long a SOCKS probe's answer stands. */
        const val PROBE_TTL_MS = 30_000L

        /** `SYNC_TOR_SOCKS=tor:9050` (a `socks5://` prefix is tolerated). Malformed is an error, not "no Tor". */
        fun fromEnv(env: Map<String, String>): TorSettings? {
            val raw = env["SYNC_TOR_SOCKS"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val hostPort = raw.substringAfter("://")
            // An IPv6 literal keeps its brackets in the env var; InetSocketAddress wants none.
            val host =
                hostPort
                    .substringBeforeLast(':', "")
                    .removeSurrounding("[", "]")
                    .takeIf { it.isNotEmpty() }
            val port = hostPort.substringAfterLast(':', "").toIntOrNull()?.takeIf { it in 1..65_535 }
            require(host != null && port != null) {
                "router: SYNC_TOR_SOCKS='$raw' is not host:port (e.g. tor:9050)"
            }
            return TorSettings(
                socksHost = host,
                socksPort = port,
                routeAll = env["SYNC_TOR_ALL"]?.trim()?.toBooleanStrictOrNull() ?: false,
                connectTimeoutSec =
                    env["SYNC_TOR_CONNECT_TIMEOUT_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(5L)
                        ?: DEFAULT_CONNECT_TIMEOUT_SEC,
                maxSockets =
                    env["SYNC_TOR_MAX_SOCKETS"]?.trim()?.toIntOrNull()?.coerceIn(1, 512)
                        ?: DEFAULT_MAX_SOCKETS,
            )
        }
    }
}

/**
 * How long a probe's fetch may sit silent on this url: the clearnet budget, plus the Tor
 * connect budget on top over the proxy. Summed, because quartz's `idleTimeoutMs` runs from
 * the start of the fetch and covers the circuit as well as the answer.
 */
fun probeIdleMs(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
    clearnetIdleMs: Long,
): Long = if (tor?.routes(url) == true) tor.settings.connectTimeoutSec * 1000L + clearnetIdleMs else clearnetIdleMs

/**
 * The websocket client whose connections go through Tor. OkHttp hands a SOCKS proxy the
 * hostname unresolved, so resolution happens inside Tor; never give this client a `Dns`,
 * which is the one change that would move resolution back out here.
 */
class TorTransport(
    val settings: TorSettings,
    private val direct: OkHttpClient,
) {
    /**
     * The dispatcher is replaced because `newBuilder()` shares the dispatcher object; the proxy
     * is a [ProxySelector] so the SOCKS host resolves per connection, not once at construction.
     */
    private val client: OkHttpClient =
        direct
            .newBuilder()
            .proxySelector(
                object : ProxySelector() {
                    override fun select(uri: URI?): List<Proxy> = listOf(Proxy(Proxy.Type.SOCKS, InetSocketAddress(settings.socksHost, settings.socksPort)))

                    override fun connectFailed(
                        uri: URI?,
                        address: SocketAddress?,
                        failure: java.io.IOException?,
                    ) = Unit
                },
            ).dispatcher(
                Dispatcher().apply {
                    maxRequests = settings.maxSockets
                    // Every onion url is its own host to OkHttp, so the per-host cap rarely bites.
                    maxRequestsPerHost = settings.maxSockets
                },
            ).connectTimeout(Duration.ofSeconds(settings.connectTimeoutSec))
            .build()

    fun routes(url: NormalizedRelayUrl): Boolean = settings.routeAll || isOnion(url)

    /** The client to dial [url] with. */
    fun clientFor(url: NormalizedRelayUrl): OkHttpClient = if (routes(url)) client else direct

    @Volatile private var probedAt = 0L

    @Volatile private var probeSaid = false

    /** One prober at a time; a loser takes the previous answer rather than waiting. */
    private val probing =
        java.util.concurrent.atomic
            .AtomicBoolean(false)

    /**
     * Is our own proxy answering? When it is not, every onion dial fails indistinguishably from
     * the relay being gone. A connect proves the port answers, not that Tor has bootstrapped.
     */
    fun socksAnswers(nowMs: Long = System.currentTimeMillis()): Boolean {
        if (nowMs - probedAt < TorSettings.PROBE_TTL_MS) return probeSaid
        if (!probing.compareAndSet(false, true)) return probeSaid
        try {
            // Re-checked under the flag: a caller can win the CAS after another prober has finished.
            if (nowMs - probedAt < TorSettings.PROBE_TTL_MS) return probeSaid
            probeSaid =
                runCatching {
                    java.net.Socket().use {
                        it.connect(
                            InetSocketAddress(settings.socksHost, settings.socksPort),
                            TorSettings.PROBE_TIMEOUT_MS,
                        )
                    }
                    true
                }.getOrDefault(false)
            // After the answer, never before, or a reader holds the old verdict for a whole TTL.
            probedAt = nowMs
        } finally {
            probing.set(false)
        }
        return probeSaid
    }
}
