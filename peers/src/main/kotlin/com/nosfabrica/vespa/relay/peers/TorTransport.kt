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

/**
 * Static `urls` this deployment cannot dial without Tor, as `stream: url`
 * strings. Reported by [SyncMain] rather than skipped: a relay that silently
 * mirrors nothing looks exactly like a relay that is failing.
 */
fun onionUpstreams(streams: List<SyncStream>): List<String> = streams.flatMap { s -> s.urls.filter(::isOnion).map { "${s.name}: ${it.url}" } }

/**
 * Where the SOCKS5 proxy is and how hard to lean on it. Null (`SYNC_TOR_SOCKS`
 * unset) is the no-Tor deployment, and every path that could dial a `.onion`
 * says so instead of timing out.
 */
data class TorSettings(
    val socksHost: String,
    val socksPort: Int,
    // SYNC_TOR_ALL: every upstream through Tor, not only .onion ones. A
    // different deployment (nothing learns our address), not a better default.
    val routeAll: Boolean,
    val connectTimeoutSec: Long,
    val maxSockets: Int,
) {
    val socksAddress: String get() = "$socksHost:$socksPort"

    companion object {
        /** A circuit plus a rendezvous is seconds of work before the first byte; a connect timeout, not an idle window. */
        const val DEFAULT_CONNECT_TIMEOUT_SEC = 90L

        /** Tor builds a circuit per stream; onion relays are a handful, not a fan-out. */
        const val DEFAULT_MAX_SOCKETS = 32

        /** How long a SOCKS probe may hang before we call our own proxy down. */
        const val PROBE_TIMEOUT_MS = 3_000

        /** How long a probe's answer stands, so a wide cycle costs one connect. */
        const val PROBE_TTL_MS = 30_000L

        /**
         * `SYNC_TOR_SOCKS=tor:9050` (a `socks5://` prefix is tolerated).
         * Malformed is a hard error, not a fallback to "no Tor".
         */
        fun fromEnv(env: Map<String, String>): TorSettings? {
            val raw = env["SYNC_TOR_SOCKS"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val hostPort = raw.substringAfter("://")
            // `[::1]:9050`: the brackets separate an IPv6 literal's colons from
            // the port's; InetSocketAddress wants the address without them.
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
 * How long a probe's fetch may sit silent on this url: the clearnet budget,
 * plus the Tor connect budget on top for anything routed through the proxy.
 * Summed, not maxed: quartz's `idleTimeoutMs` runs from the start of the
 * fetch, so it covers the circuit as well as the answer, and a window that
 * lapses during the connect returns an empty result indistinguishable from
 * an empty relay. This is the monitor's budget, not [NEG_IDLE_MS].
 */
fun probeIdleMs(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
    clearnetIdleMs: Long,
): Long = if (tor?.routes(url) == true) tor.settings.connectTimeoutSec * 1000L + clearnetIdleMs else clearnetIdleMs

/**
 * The second websocket client: the one whose connections go through Tor.
 *
 * Given a SOCKS proxy, OkHttp sends the hostname unresolved to the proxy, so
 * resolution happens inside Tor. That is what makes `.onion` work and what
 * keeps hidden-service names off this box's DNS. Never give this client a
 * `Dns`: that is the one change that would move resolution back out here.
 * Which client a url gets is chosen per url; clearnet keeps the direct one.
 */
class TorTransport(
    val settings: TorSettings,
    private val direct: OkHttpClient,
) {
    /**
     * Built from [direct] so its tuning carries over, but the dispatcher and
     * connect timeout are replaced: `newBuilder()` shares the dispatcher
     * object, which would let onion dials draw down the clearnet budget. The
     * proxy is a [ProxySelector] so the SOCKS host resolves per connection
     * rather than once at construction.
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
                    // Every onion url is its own host to OkHttp, so the per-host
                    // cap only bites on a relay serving several urls.
                    maxRequestsPerHost = settings.maxSockets
                },
            ).connectTimeout(Duration.ofSeconds(settings.connectTimeoutSec))
            .build()

    fun routes(url: NormalizedRelayUrl): Boolean = settings.routeAll || isOnion(url)

    /** The client to dial [url] with. */
    fun clientFor(url: NormalizedRelayUrl): OkHttpClient = if (routes(url)) client else direct

    @Volatile private var probedAt = 0L

    @Volatile private var probeSaid = false

    /**
     * One prober at a time: when the TTL expires every fan-out thread reaches
     * the check together. The loser takes the previous answer rather than
     * waiting; it is at most [TorSettings.PROBE_TTL_MS] stale.
     */
    private val probing =
        java.util.concurrent.atomic
            .AtomicBoolean(false)

    /**
     * Is our own proxy answering? When it is not, every onion dial fails with
     * an exception indistinguishable from the relay being gone, and
     * [Unreachability] would publish that, signed, about someone else's
     * server. A connect proves the port answers, not that Tor has bootstrapped.
     */
    fun socksAnswers(nowMs: Long = System.currentTimeMillis()): Boolean {
        if (nowMs - probedAt < TorSettings.PROBE_TTL_MS) return probeSaid
        if (!probing.compareAndSet(false, true)) return probeSaid
        try {
            // Re-checked under the flag: a caller descheduled between the TTL
            // read and the CAS can win it after another prober has finished.
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
            // After the answer, never before: a reader that saw the new
            // timestamp with the old verdict would hold it for a whole TTL.
            probedAt = nowMs
        } finally {
            probing.set(false)
        }
        return probeSaid
    }
}
