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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.RelayLogger
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CoroutineScope
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.time.Duration

/**
 * How this process talks to other relays: one websocket client, one socket budget, one Tor
 * transport, one NIP-42 answer, shared by both planes so [RelaySockets] has one pool to count.
 * The owner calls [connect] once everything is registered and [close] after the planes stop.
 */
class PeerClient(
    private val scope: CoroutineScope,
    /**
     * Answers NIP-42 challenges from upstreams that gate reads behind AUTH. Null is the
     * anonymous deployment, where a gated relay looks exactly like an empty one.
     */
    signer: NostrSigner? = null,
    /** `SYNC_TOR_SOCKS`: the proxy `.onion` upstreams are dialled through. Null drops `.onion` urls at discovery. */
    torSettings: TorSettings? = null,
    /** `SYNC_WIRE_LOG`: "" (errors only) / "sent" / "full". */
    wireLogMode: String = "",
    /** How long a dial may take before it is a failure. */
    connectionTimeoutSec: Long = 10,
) : AutoCloseable {
    // The ping surfaces half-open connections as a failed pong, which routes into quartz's reconnect.
    private val okhttp =
        OkHttpClient
            .Builder()
            // An open websocket holds a dispatcher slot for its whole life, so the budget must
            // exceed static upstreams plus every stream's `concurrency`.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(connectionTimeoutSec))
            .build()

    /**
     * Calls running against [MAX_CONCURRENT_SOCKETS] and calls queued behind it; `queued` above
     * zero is the one direct sign the dispatcher is the constraint. Clearnet only.
     */
    fun socketLoad() = SocketLoad(okhttp.dispatcher.runningCallsCount(), okhttp.dispatcher.queuedCallsCount())

    /** See [socketLoad]. */
    class SocketLoad(
        val running: Int,
        val queued: Int,
    )

    /** The Tor client, when there is one, and which urls it takes. */
    val tor = torSettings?.let { TorTransport(it, okhttp) }

    /** The OkHttp client that can reach [url], for dials outside quartz's websocket path. */
    fun httpFor(url: NormalizedRelayUrl): OkHttpClient = tor?.clientFor(url) ?: okhttp

    val client = NostrClient(BasicOkHttpWebSocket.Builder { url -> httpFor(url) }, scope)

    // NIP-42: an unanswered challenge looks exactly like an empty relay.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    /** Wire logging. Errors (NOTICE, CLOSED, failed sends) are always on; `sent`/`full` add more. */
    private val wireLog =
        when (wireLogMode) {
            "full", "sent" -> {
                // The sent/received lines are DEBUG; without lowering the floor the switch prints nothing.
                if (Log.minLevel > LogLevel.DEBUG) {
                    Log.minLevel = LogLevel.DEBUG
                    System.err.println(
                        "router: SYNC_WIRE_LOG=$wireLogMode lowered the quartz log floor to DEBUG — this is verbose",
                    )
                }
                RelayLogger(client, debugSending = true, debugReceiving = wireLogMode == "full")
            }

            else -> {
                RelayLogger(client, debugSending = false, debugReceiving = false)
            }
        }

    fun connect() {
        client.connect()
    }

    /** Say at boot whether the configured Tor proxy is answering. */
    fun announceTor() {
        tor?.let {
            val reach = if (it.socksAnswers()) "answering" else "NOT answering — .onion relays will be skipped until it does"
            System.err.println(
                "router: tor SOCKS ${it.settings.socksAddress} $reach" +
                    (if (it.settings.routeAll) "; SYNC_TOR_ALL is on — EVERY upstream goes through it" else " (.onion upstreams only)"),
            )
        }
    }

    /** Stop dialling. Called after the planes' own scopes are cancelled. */
    override fun close() {
        runCatching { authenticator?.destroy() }
        runCatching { client.close() }
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
    }

    companion object {
        const val MAX_CONCURRENT_SOCKETS = 1024
        const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20
    }
}
