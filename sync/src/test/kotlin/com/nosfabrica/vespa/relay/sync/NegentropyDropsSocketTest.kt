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
import com.nosfabrica.vespa.relay.peers.SocketEnds
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.DataInputStream
import java.net.ServerSocket
import java.security.MessageDigest
import java.util.Base64
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A relay that closes the socket on our NEG-OPEN, through the real client, quartz and the pager. */
class NegentropyDropsSocketTest {
    /** Accepts one websocket, reads one frame, and answers it with a close frame carrying [code]. */
    private fun closingRelay(
        code: Int,
        reason: String,
    ): ServerSocket {
        val server = ServerSocket(0)
        thread(isDaemon = true) {
            server.accept().use { s ->
                val input = DataInputStream(s.getInputStream())
                val headers = StringBuilder()
                while (!headers.endsWith("\r\n\r\n")) headers.append(input.readByte().toInt().toChar())
                val key = Regex("Sec-WebSocket-Key: (.+)\r\n", RegexOption.IGNORE_CASE).find(headers)!!.groupValues[1].trim()
                val accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest((key + WS_GUID).toByteArray()))
                s.getOutputStream().write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\n\r\n".toByteArray())
                // The client's first frame, masked: skip it whole.
                input.readByte()
                val len =
                    when (val l = input.readByte().toInt() and 0x7f) {
                        126 -> input.readUnsignedShort().toLong()
                        127 -> input.readLong()
                        else -> l.toLong()
                    }
                input.skipNBytes(4 + len)
                val body = byteArrayOf((code shr 8).toByte(), code.toByte()) + reason.toByteArray()
                s.getOutputStream().write(byteArrayOf(0x88.toByte(), body.size.toByte()) + body)
                s.getOutputStream().flush()
                // Hold the socket until the client answers the close.
                runCatching { input.readAllBytes() }
            }
        }
        return server
    }

    private object EmptyIndex : NegentropyLocalIndex {
        override suspend fun count(window: Filter): Int = 0

        override suspend fun entriesFor(window: Filter): List<IdAndTime> = emptyList()
    }

    @Test
    fun `a relay that closes on NEG-OPEN is counted under its close code, with what we had sent`() =
        runBlocking {
            val server = closingRelay(1009, "too big")
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
            val peers = PeerClient(scope, socketEnds = SocketEnds())
            try {
                val url = RelayUrlNormalizer.normalize("ws://127.0.0.1:${server.localPort}/")
                val drops = NegentropyDrops(peers.socketEnds)
                val pager =
                    NegentropyPager(
                        EmptyIndex,
                        ClientWindowSync(peers.client, FilterWidths(), idleTimeoutMs = 10_000),
                        SweepState(null),
                        NegPageTuning(),
                        drops = drops,
                    )
                val notes = Filter(kinds = listOf(1))
                val out = withTimeout(20_000) { pager.sweep("notes", url, notes, notes.copy(since = 1_000, until = 2_000)) {} }

                assertFalse(out.negentropyUsable, "the first window failed")
                val doc = drops.document()!!
                val cause = (doc["causes"] as JsonArray).single() as JsonObject
                assertEquals("relay closed 1009 too big", cause["cause"]!!.jsonPrimitive.content)
                assertTrue(cause["largestSentMax"]!!.jsonPrimitive.content.toInt() > 0, "the NEG-OPEN we sent is on the record")
            } finally {
                peers.close()
                scope.cancel()
                server.close()
            }
        }

    private companion object {
        const val WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    }
}
