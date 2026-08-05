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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.SyncStream
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
 * Static `urls` this deployment cannot dial without Tor, as
 * `stream: url` strings. Reported by [SyncMain] rather than skipped: a
 * `.onion` in `urls` was typed by hand, and a relay that silently mirrors
 * nothing looks exactly like a relay that is failing.
 */
internal fun onionUpstreams(streams: List<SyncStream>): List<String> = streams.flatMap { s -> s.urls.filter(::isOnion).map { "${s.name}: ${it.url}" } }

/**
 * Where the SOCKS5 proxy is and how hard to lean on it. Null — `SYNC_TOR_SOCKS`
 * unset — is the no-Tor deployment, and every path that could dial a `.onion`
 * says so out loud instead of timing out.
 */
data class TorSettings(
    val socksHost: String,
    val socksPort: Int,
    // SYNC_TOR_ALL: send EVERY upstream through Tor, not only .onion ones. A
    // different deployment (nothing learns our address), not a better default:
    // a 20k-relay dynamic cycle over Tor is a fraction of the throughput, and
    // several large relays refuse exit traffic outright.
    val everything: Boolean,
    val connectTimeoutSec: Long,
    val maxSockets: Int,
) {
    val socksAddress: String get() = "$socksHost:$socksPort"

    companion object {
        /**
         * A circuit plus a rendezvous is seconds of work before the first byte,
         * where the clearnet default (`connectionTimeout = 20`) sizes a TCP
         * handshake. This is a connect timeout — the idle windows that govern
         * transfers ([NEG_IDLE_MS]) already reset per message and need no
         * Tor-specific value.
         */
        const val DEFAULT_CONNECT_TIMEOUT_SEC = 90L

        /**
         * Tor builds a circuit per stream and is not the router's 1024-socket
         * clearnet budget. Onion relays are a handful, not a fan-out.
         */
        const val DEFAULT_MAX_SOCKETS = 32

        /** How long a SOCKS probe may hang before we call our own proxy down. */
        const val PROBE_TIMEOUT_MS = 3_000

        /** How long a probe's answer stands, so a wide cycle costs one connect. */
        const val PROBE_TTL_MS = 30_000L

        /**
         * `SYNC_TOR_SOCKS=tor:9050` (a `socks5://` prefix is tolerated — it is
         * what operators type). Malformed is a hard error, not a fallback to
         * "no Tor": the value exists to turn something on.
         */
        fun fromEnv(env: Map<String, String>): TorSettings? {
            val raw = env["SYNC_TOR_SOCKS"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val hostPort = raw.substringAfter("://")
            val host = hostPort.substringBeforeLast(':', "").takeIf { it.isNotEmpty() }
            val port = hostPort.substringAfterLast(':', "").toIntOrNull()?.takeIf { it in 1..65_535 }
            require(host != null && port != null) {
                "router: SYNC_TOR_SOCKS='$raw' is not host:port (e.g. tor:9050)"
            }
            return TorSettings(
                socksHost = host,
                socksPort = port,
                everything = env["SYNC_TOR_ALL"]?.trim()?.toBooleanStrictOrNull() ?: false,
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
 * The second websocket client: the one whose connections go through Tor.
 *
 * The mechanism is OkHttp's, not ours. Given a SOCKS proxy, `RouteSelector`
 * builds an `InetSocketAddress.createUnresolved(host, port)` instead of
 * calling `Dns.lookup`, and `ConnectPlan` opens a `Socket(proxy)` — so the
 * hostname travels to the proxy and is resolved INSIDE Tor. That is what makes
 * `.onion` work at all (there is nothing for a resolver to answer) and it is
 * also what stops the name of every hidden service we sync with from being
 * broadcast to whatever DNS this box uses. Never give this client a `Dns`:
 * that is the one change that would quietly move resolution back out here.
 *
 * Which client a relay gets is chosen per url, because quartz's
 * `BasicOkHttpWebSocket.Builder` takes `(NormalizedRelayUrl) -> OkHttpClient`.
 * Clearnet keeps the direct client: routing 20,000 discovered relays through
 * Tor to reach the handful on it would trade the fan-out for the exception.
 */
internal class TorTransport(
    val settings: TorSettings,
    private val direct: OkHttpClient,
) {
    /**
     * Built from [direct] so the ping interval and the rest of its tuning
     * carry over — but the dispatcher and the connect timeout are REPLACED,
     * not inherited. `newBuilder()` shares the dispatcher object, and sharing
     * it would have let onion dials draw down the 1024-socket budget the
     * clearnet fan-out is sized against while queueing behind it.
     *
     * The proxy is a [ProxySelector] rather than a fixed `proxy(...)` because
     * `InetSocketAddress(host, port)` resolves ONCE, at construction: a Tor
     * container that restarts on a new address would leave this process
     * dialling the old one until someone restarted the router. Resolved per
     * connection, a restart costs one failed cycle.
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
                    // Every onion url is a different "host" to OkHttp, so the
                    // per-host cap only bites on a relay serving several urls.
                    maxRequestsPerHost = settings.maxSockets
                },
            ).connectTimeout(Duration.ofSeconds(settings.connectTimeoutSec))
            .build()

    /** Does this url go through Tor? */
    fun routes(url: NormalizedRelayUrl): Boolean = settings.everything || isOnion(url)

    /** The client to dial [url] with — the whole point of the per-url builder. */
    fun clientFor(url: NormalizedRelayUrl): OkHttpClient = if (routes(url)) client else direct

    @Volatile private var probedAt = 0L

    @Volatile private var probeSaid = false

    /**
     * Is our own proxy answering? Asked before a cycle dials any hidden
     * service, and it is a question about US.
     *
     * When the Tor container is down or its name does not resolve, every onion
     * dial fails with an exception indistinguishable from the relay being
     * gone — `UnknownHostException` among them, which [Unreachability] accepts
     * as proof and would publish, signed, about someone else's server. One
     * cheap connect to the SOCKS port separates "we have no transport" from
     * "that service is unreachable", and only the second is anyone else's
     * business.
     *
     * A connect proves the port answers, not that Tor has bootstrapped; a
     * still-bootstrapping Tor refuses the stream and the relay is retried next
     * cycle, which is the same shape as any other transient upstream failure.
     */
    fun socksAnswers(nowMs: Long = System.currentTimeMillis()): Boolean {
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
        probedAt = nowMs
        return probeSaid
    }
}
