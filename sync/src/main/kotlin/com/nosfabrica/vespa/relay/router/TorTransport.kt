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
    val routeAll: Boolean,
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
            // `[::1]:9050` — the brackets exist to separate the colons of an
            // IPv6 literal from the port's, and only the port parse needs
            // them. InetSocketAddress wants the address without: left on, the
            // host never resolves and every dial fails on a value that looks
            // exactly right in the log.
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
 * How long the alias fold's fingerprint may sit silent on this url before it
 * gives up — the clearnet budget for a clearnet relay, and the Tor one on top
 * for anything dialled through the proxy.
 *
 * **Quartz's `idleTimeoutMs` is measured from the START of the fetch**, not
 * from the connection: `IdleClock` is constructed with the walk and nothing has
 * bumped it yet, so the window covers the connect as well as the answer. The
 * value [AliasProbe] was given is `connectionTimeout` — the number that sizes a
 * clearnet TCP handshake, 20s by default — and a hidden service is allowed
 * [TorSettings.connectTimeoutSec] (90s) for the circuit and the rendezvous
 * ALONE, precisely because 20s is the wrong size for that. The two disagreeing
 * is not a slow probe, it is a probe that cannot finish: [fetchAll] returns
 * whatever it collected when the window lapses, and what it collected is
 * nothing — which arrives at [RelayAliases] as an EMPTY window, indistinguishable
 * from a relay that holds no events. An empty window folds nothing and clears
 * nothing, so every url on that host stays unmeasured and the fan-out goes on
 * dialling all of them, pass after pass, forever.
 *
 * Summed rather than maxed, because the two budgets buy different things: the
 * Tor one gets us connected, and the clearnet one is the silence every other
 * relay is allowed while ANSWERING. Under `SYNC_TOR_ALL` clearnet urls route
 * through the proxy too and get the same window, for the same reason.
 *
 * **This is the MONITOR plane's silence budget, and it is not [NEG_IDLE_MS].**
 * The sync plane's transfers are allowed 30s of silence apiece; a probe gets
 * `connectionTimeout` (20s) plus a circuit where it needs one. Two planes with
 * two budgets under similar-sounding names is deliberate rather than drift —
 * see [NEG_IDLE_MS] for which is sizing what — and a probe pass puts a hard
 * wall clock on top of this window that no transfer has, because a probe that
 * never returns holds the roster every stream is built from. See
 * [AliasProbe.deadlineMs].
 *
 * What it costs is paid only by hosts that do not answer: a leader that never
 * speaks is asked four times a pass (bare filter then [AliasProbe.FALLBACK_KINDS],
 * each retrying once at the smaller page), so a dead onion group now occupies one
 * of the fold's 16 permits for minutes rather than seconds. That is background
 * work on a 6h clock against the handful of relays Tor reaches — see
 * [TorSettings.DEFAULT_MAX_SOCKETS] — and the alternative is what it replaces:
 * never folding a hidden service at all.
 */
internal fun probeIdleMs(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
    clearnetIdleMs: Long,
): Long = if (tor?.routes(url) == true) tor.settings.connectTimeoutSec * 1000L + clearnetIdleMs else clearnetIdleMs

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

    fun routes(url: NormalizedRelayUrl): Boolean = settings.routeAll || isOnion(url)

    /** The client to dial [url] with — the whole point of the per-url builder. */
    fun clientFor(url: NormalizedRelayUrl): OkHttpClient = if (routes(url)) client else direct

    @Volatile private var probedAt = 0L

    @Volatile private var probeSaid = false

    /**
     * One prober at a time. The gate is asked once per relay in a fan-out that
     * launches a coroutine per discovered relay, so the moment the TTL expires
     * every runnable thread reaches the check together — without this, each of
     * them opens its own connection and the answer to "is our proxy healthy?"
     * is a burst of connections to it. The loser of the race takes the
     * previous answer rather than waiting: it is at most [TorSettings.PROBE_TTL_MS]
     * stale, and the alternative is blocking IO threads on someone else's
     * connect timeout.
     */
    private val probing =
        java.util.concurrent.atomic
            .AtomicBoolean(false)

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
        if (!probing.compareAndSet(false, true)) return probeSaid
        try {
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
            // timestamp with the old verdict would hold a stale answer for a
            // whole TTL, which is the one outcome the probe exists to avoid.
            probedAt = nowMs
        } finally {
            probing.set(false)
        }
        return probeSaid
    }
}
