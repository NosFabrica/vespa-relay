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

import com.nosfabrica.vespa.relay.peers.SocketEnds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals

/** Which cause a failed window is counted under. */
class NegentropyDropsTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val ends = SocketEnds()
    private val drops = NegentropyDrops(ends)

    /** Opens one socket through [ends] and has the relay close it. */
    private fun relayCloses(code: Int) {
        var relaySide: WebSocketListener? = null
        val inner =
            object : WebsocketBuilder {
                override fun build(
                    url: NormalizedRelayUrl,
                    out: WebSocketListener,
                ): WebSocket {
                    relaySide = out
                    return object : WebSocket {
                        override fun needsReconnect() = false

                        override fun connect() = Unit

                        override fun disconnect() = Unit

                        override fun send(msg: String) = true
                    }
                }
            }
        ends.around(inner).build(url, Quiet)
        relaySide!!.onClosed(code, "bye")
    }

    private object Quiet : WebSocketListener {
        override fun onOpen(
            pingMillis: Int,
            compression: Boolean,
        ) = Unit

        override suspend fun onMessage(text: String) = Unit

        override fun onClosed(
            code: Int,
            reason: String,
        ) = Unit

        override fun onFailure(
            t: Throwable,
            code: Int?,
            response: String?,
        ) = Unit
    }

    private fun causes(): Map<String, Long> =
        (drops.document()!!["causes"] as JsonArray).associate {
            val row = it as JsonObject
            row["cause"]!!.jsonPrimitive.content to row["windows"]!!.jsonPrimitive.content.toLong()
        }

    @Test
    fun `a dropped socket is counted under how it ended`() {
        val mark = drops.mark(url)
        relayCloses(1009)
        drops.record(url, mark, 10, "closed: relay disconnected")
        assertEquals(mapOf("relay closed 1009 bye" to 1L), causes())
    }

    @Test
    fun `a refusal in the relay's own words keeps them, whatever the socket did after`() {
        val mark = drops.mark(url)
        relayCloses(1000)
        drops.record(url, mark, 10, "blocked: negentropy disabled")
        assertEquals(mapOf("blocked: negentropy disabled" to 1L), causes())
    }

    @Test
    fun `a drop with no socket end says so`() {
        drops.record(url, drops.mark(url), 10, "closed: relay disconnected")
        assertEquals(mapOf("no socket end: closed: relay disconnected" to 1L), causes())
    }

    @Test
    fun `refusals that differ only in their numbers are one cause`() {
        drops.record(url, drops.mark(url), 10, "rate-limited: 37 requests in 10s")
        drops.record(url, drops.mark(url), 10, "rate-limited: 52 requests in 10s")
        assertEquals(mapOf("rate-limited: # requests in #s" to 2L), causes())
    }
}
