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
package com.nosfabrica.vespa.relay.shared

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
 * HOW THIS PROCESS TALKS TO OTHER RELAYS — one websocket client, one socket
 * budget, one Tor transport, one NIP-42 answer, for both planes.
 *
 * ## Why it is one object rather than two
 *
 * The mirror and the monitor dial the same relays. A pooled connection either
 * of them opens is one the other can measure or transfer over without a second
 * dial, and [RelaySockets] — the refcount that keeps a probe pass from closing
 * a socket a stream is still transferring on — only works because there is one
 * pool for it to count. Split the plumbing and a relay both planes touch costs
 * two connections against a 1,024-socket dispatcher.
 *
 * ## Why it is its own object rather than fields on the engine
 *
 * Because "the engine" was one class doing two jobs, and this was the reason it
 * could not be two. Everything here is genuinely shared; everything that was
 * NOT — the passes, the pool, the ingest queue — is now on the side that owns
 * it. A plane holds a [PeerClient] and asks it for a socket; it does not build
 * one, and it does not close one.
 *
 * The process that constructs this owns its lifecycle: [connect] once
 * everything is registered, [close] after the planes have stopped touching it.
 */
class PeerClient(
    private val scope: CoroutineScope,
    /**
     * Answers NIP-42 challenges from upstreams that gate reads behind AUTH.
     * Null is the anonymous deployment — a gated relay then serves nothing,
     * and an unanswered challenge looks exactly like an ordinary empty relay,
     * which is why `SyncMain` says at boot which of the two this is.
     */
    signer: NostrSigner? = null,
    /**
     * `SYNC_TOR_SOCKS`: the proxy `.onion` upstreams are dialled through. Null
     * is the clearnet-only deployment, where discovery drops `.onion` urls and
     * a configured one is a boot error — never a silent timeout.
     */
    torSettings: TorSettings? = null,
    /** `SYNC_WIRE_LOG`: "" (errors only) / "sent" / "full". */
    wireLogMode: String = "",
    /** How long a dial may take before it is a failure — `config.connectionTimeoutSec`. */
    connectionTimeoutSec: Long = 10,
) : AutoCloseable {
    // One OkHttp client for every upstream. The 120s ping surfaces half-open
    // connections as a failed pong, which routes into quartz's reconnect path.
    private val okhttp =
        OkHttpClient
            .Builder()
            // The dispatcher budget is the real concurrency ceiling for the
            // whole router: an open websocket holds a dispatcher slot for its
            // entire life, so at the stock 64 every stream's `concurrency`
            // silently stopped meaning anything (measured: a 20,340-relay
            // cycle with an ETA of 330 hours). Must exceed static upstreams
            // plus the sum of every stream's `concurrency`.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    // Per HOST; only bites when one host serves several urls.
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(connectionTimeoutSec))
            .build()

    /**
     * The Tor client, when there is one, and which urls it takes. See
     * [TorTransport] for why resolution has to happen inside the proxy.
     */
    val tor = torSettings?.let { TorTransport(it, okhttp) }

    /**
     * The OkHttp client that can reach [url] — the Tor one for a `.onion`, the
     * clearnet one otherwise.
     *
     * Exposed because two things dial outside quartz's websocket path: the
     * monitor's NIP-11 document fetch, which is plain HTTP, and anything else
     * that needs a transport rather than a socket.
     */
    fun httpFor(url: NormalizedRelayUrl): OkHttpClient = tor?.clientFor(url) ?: okhttp

    /** Does [url] go through Tor — asked by the passes that report which transport measured a relay. */
    fun routesThroughTor(url: NormalizedRelayUrl): Boolean = tor?.routes(url) == true

    // Per URL, not one client for the process: quartz's builder takes
    // (NormalizedRelayUrl) -> OkHttpClient precisely so a relay can be dialled
    // over the transport that can reach it.
    val client = NostrClient(BasicOkHttpWebSocket.Builder { url -> httpFor(url) }, scope)

    // NO PASSIVE NIP-66 WRITER. quartz's `RelayMonitor` used to live here,
    // attached to this client as a connection listener, signing a kind-30166
    // for every socket the fan-out opened. Two things followed, both bad. It
    // was a second publisher of facts the monitor passes already state, and
    // because a 30166 is addressable per (author, url) and every writer edits
    // the same record, it rewrote `created_at` on a 5-minute flush for every
    // relay we were actively syncing — so the record's own clock said "we
    // talked recently" instead of "we checked this", and every consumer needed
    // a private freshness convention to work around it.
    //
    // The monitor's passes are the only writers now. `created_at` means what
    // every other NIP-66 consumer takes it to mean, and a stream can bound
    // verdict freshness with a plain NIP-01 `since`.

    // NIP-42: relays that gate reads behind AUTH serve nothing until we answer
    // their challenge — and an unanswered challenge looks exactly like an
    // ordinary empty relay. Attaching the authenticator is enough.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    /**
     * What actually goes down the wire, for when the counters stop making
     * sense. The error half — NOTICE, CLOSED, failed sends — is on always:
     * those are the relay explaining itself. `sent`/`full` add outgoing
     * commands / every message.
     */
    private val wireLog =
        when (wireLogMode) {
            "full", "sent" -> {
                // The sent/received lines are DEBUG and quartz's floor is WARN
                // in every deployment we run — without lowering it the switch
                // would be accepted, construct its logger, and print nothing.
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

    /** How many sockets are open right now, against [socketCeiling]. */
    fun openSockets(): Int = client.connectedRelaysFlow().value.size

    fun connect() {
        client.connect()
    }

    /**
     * Say at boot whether the configured Tor proxy is answering, both ways.
     *
     * A transport that is configured but not answering must not be discovered
     * later, one silent onion relay at a time. The probe asks our OWN SOCKS
     * port, so a false answer here is a statement about this container and
     * nobody else's server.
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
     * Stop dialling. Called AFTER the planes' own scopes are cancelled, so a
     * worker mid-visit stops touching the client before it closes rather than
     * racing it and counting its own death as an abort.
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
        /**
         * The socket budget for the whole process, and the number the health
         * line reports against — see the dispatcher above for what happened at
         * OkHttp's stock 64.
         */
        const val MAX_CONCURRENT_SOCKETS = 1024
        const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20
    }
}
