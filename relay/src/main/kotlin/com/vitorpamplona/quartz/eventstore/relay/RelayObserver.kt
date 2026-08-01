/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.AuthMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EoseMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Everything this relay learns about other relays, learned for free.
 *
 * The router already opens a socket to every relay it mirrors — a static
 * upstream's live tail, a backfill, an up-reconcile, a dynamic cycle's fan-out.
 * Each of those connections answers the questions a NIP-66 monitor exists to
 * ask, and the answers were being thrown away. Attached to the shared client as
 * a [RelayConnectionListener], this sees ALL of them without any code path
 * having to remember to report: a relay dialled by any part of the router is
 * observed by this one.
 *
 * That is the difference from measuring inside one sync loop, which is where
 * this started. A per-loop measurement only knows the relays that loop touched,
 * so the static upstreams — the ones we know most about, because we hold their
 * sockets open for the process lifetime — contributed nothing at all.
 *
 * ## What is measured
 *
 *  - **rtt-open**: `onConnecting` to `onConnected`. A real number, not a
 *    placeholder. Aggregators rank relays by this field, so publishing a
 *    hard-coded zero would be actively unhelpful to everyone reading it.
 *  - **rtt-read**: the first REQ sent to the first EOSE for it. The honest
 *    measure of a relay that accepts a socket and then never answers, which is
 *    the failure mode a connect timeout alone cannot see.
 *  - **reachable**: it opened. Nothing more is claimed by that.
 *  - **the error**, when it did not open, verbatim from the transport.
 *  - **whether it demanded AUTH**, which is why an unauthenticated crawl finds
 *    a relay empty and a NIP-11 read would not have explained it.
 *
 * Observations accumulate in memory and are flushed as signed 30166s on an
 * interval; see [RelayReachability]. Reads are per relay and the map is bounded
 * by the number of distinct relays ever dialled, which is the same set the
 * router already holds urls for.
 */
class RelayObserver : RelayConnectionListener {
    class Observation(
        @Volatile var connectingAtMs: Long? = null,
        @Volatile var rttOpenMs: Long? = null,
        @Volatile var firstReqAtMs: Long? = null,
        @Volatile var rttReadMs: Long? = null,
        @Volatile var reachable: Boolean = false,
        @Volatile var error: String? = null,
        @Volatile var authRequired: Boolean = false,
        @Volatile var notice: String? = null,
    )

    private val seen = ConcurrentHashMap<NormalizedRelayUrl, Observation>()

    private fun of(relay: IRelayClient) = seen.getOrPut(relay.url) { Observation() }

    override fun onConnecting(relay: IRelayClient) {
        val o = of(relay)
        o.connectingAtMs = System.currentTimeMillis()
        // Cleared, not kept: a reconnect is a fresh measurement, and carrying the
        // previous attempt's error forward would report a relay as broken for as
        // long as the process lives after one bad minute.
        o.error = null
    }

    override fun onConnected(
        relay: IRelayClient,
        attempt: Int,
        success: Boolean,
    ) {
        val o = of(relay)
        o.reachable = true
        o.error = null
        o.connectingAtMs?.let { o.rttOpenMs = (System.currentTimeMillis() - it).coerceAtLeast(0) }
    }

    override fun onCannotConnect(
        relay: IRelayClient,
        error: String,
    ) {
        val o = of(relay)
        // NOT `reachable = false`. A relay that answered an hour ago and is down
        // now is a different thing from one that has never answered, and only the
        // flush decides which record to write. Overwriting the success here would
        // lose that distinction on the first blip.
        o.error = error.take(200)
    }

    /**
     * The outgoing REQ starts the read clock. Only the FIRST one per relay: a
     * later subscription on a warm socket measures nothing about the relay.
     */
    fun onRequestSent(relay: NormalizedRelayUrl) {
        val o = seen.getOrPut(relay) { Observation() }
        if (o.firstReqAtMs == null) o.firstReqAtMs = System.currentTimeMillis()
    }

    override fun onIncomingMessage(
        relay: IRelayClient,
        msgStr: String,
        message: Message,
    ) {
        val o = of(relay)
        when (message) {
            is EoseMessage -> {
                if (o.rttReadMs == null) {
                    o.firstReqAtMs?.let { o.rttReadMs = (System.currentTimeMillis() - it).coerceAtLeast(0) }
                }
            }

            // Serving an event is proof of life even from a relay that never sends
            // EOSE — some do not, and treating those as unresponsive would shed
            // relays that are working perfectly well.
            is EventMessage -> {
                o.reachable = true
            }

            is AuthMessage -> {
                o.authRequired = true
            }

            is NoticeMessage -> {
                o.notice = message.message.take(200)
            }

            is ClosedMessage -> {
                if (message.message.startsWith("auth-required")) o.authRequired = true
            }

            else -> {
                Unit
            }
        }
    }

    /**
     * Take everything observed so far and clear it, so a flush never re-reports
     * a measurement it already wrote and a relay untouched since is left alone
     * rather than having its record refreshed for no reason.
     */
    fun drain(): Map<NormalizedRelayUrl, Observation> {
        val out = HashMap<NormalizedRelayUrl, Observation>(seen.size)
        seen.keys.toList().forEach { url -> seen.remove(url)?.let { out[url] = it } }
        return out
    }

    fun size(): Int = seen.size
}
