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
 * How this process talks to other relays: one websocket client, one socket
 * budget, one Tor transport, one NIP-42 answer, shared by both planes.
 *
 * One object because the mirror and the monitor dial the same relays: a
 * pooled connection either opens is one the other can use without a second
 * dial, and [RelaySockets] only works because there is one pool to count.
 * A plane holds a [PeerClient] and asks it for a socket; it neither builds
 * nor closes one. The constructor's owner calls [connect] once everything is
 * registered and [close] after the planes have stopped touching it.
 */
class PeerClient(
    private val scope: CoroutineScope,
    /**
     * Answers NIP-42 challenges from upstreams that gate reads behind AUTH.
     * Null is the anonymous deployment, where a gated relay looks exactly
     * like an empty one; `SyncMain` says at boot which of the two this is.
     */
    signer: NostrSigner? = null,
    /** `SYNC_TOR_SOCKS`: the proxy `.onion` upstreams are dialled through. Null drops `.onion` urls at discovery. */
    torSettings: TorSettings? = null,
    /** `SYNC_WIRE_LOG`: "" (errors only) / "sent" / "full". */
    wireLogMode: String = "",
    /** How long a dial may take before it is a failure: `config.connectionTimeoutSec`. */
    connectionTimeoutSec: Long = 10,
) : AutoCloseable {
    // The 120s ping surfaces half-open connections as a failed pong, which
    // routes into quartz's reconnect path.
    private val okhttp =
        OkHttpClient
            .Builder()
            // An open websocket holds a dispatcher slot for its whole life, so
            // the budget must exceed static upstreams plus every stream's
            // `concurrency`, or those knobs stop meaning anything.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(connectionTimeoutSec))
            .build()

    /**
     * Calls running against [MAX_CONCURRENT_SOCKETS], and calls queued behind
     * it. `queued` above zero is the one direct sign that the dispatcher is
     * the constraint. Clearnet only: the Tor dispatcher has its own budget.
     */
    fun socketLoad() = SocketLoad(okhttp.dispatcher.runningCallsCount(), okhttp.dispatcher.queuedCallsCount())

    /** See [socketLoad]. */
    class SocketLoad(
        val running: Int,
        val queued: Int,
    )

    /** The Tor client, when there is one, and which urls it takes. */
    val tor = torSettings?.let { TorTransport(it, okhttp) }

    /**
     * The OkHttp client that can reach [url]. Exposed for the dials outside
     * quartz's websocket path, such as the monitor's NIP-11 fetch.
     */
    fun httpFor(url: NormalizedRelayUrl): OkHttpClient = tor?.clientFor(url) ?: okhttp

    // Per url, so a relay is dialled over the transport that can reach it.
    val client = NostrClient(BasicOkHttpWebSocket.Builder { url -> httpFor(url) }, scope)

    // No passive NIP-66 writer here: the monitor's passes are the only writers
    // of kind 30166, so a record's `created_at` means "we checked this".

    // NIP-42: an unanswered challenge looks exactly like an empty relay.
    // Attaching the authenticator is enough.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    /**
     * What goes down the wire. The error half (NOTICE, CLOSED, failed sends)
     * is always on; `sent`/`full` add outgoing commands / every message.
     */
    private val wireLog =
        when (wireLogMode) {
            "full", "sent" -> {
                // The sent/received lines are DEBUG; without lowering quartz's
                // floor the switch would construct its logger and print nothing.
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

    /**
     * Say at boot whether the configured Tor proxy is answering. The probe
     * asks our own SOCKS port, so a false answer is about this container.
     */
    fun announceTor() {
        tor?.let {
            val reach = if (it.socksAnswers()) "answering" else "NOT answering — .onion relays will be skipped until it does"
            System.err.println(
                "router: tor SOCKS ${it.settings.socksAddress} $reach" +
                    (if (it.settings.routeAll) "; SYNC_TOR_ALL is on — EVERY upstream goes through it" else " (.onion upstreams only)"),
            )
        }
    }

    /**
     * Stop dialling. Called after the planes' own scopes are cancelled, so a
     * worker mid-visit is not racing the close and counting its death as an abort.
     */
    override fun close() {
        runCatching { authenticator?.destroy() }
        runCatching { client.close() }
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
    }

    companion object {
        /** The socket budget for the whole process, and the number the health line reports against. */
        const val MAX_CONCURRENT_SOCKETS = 1024
        const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20
    }
}
