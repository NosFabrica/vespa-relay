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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.EOFException
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.zip.Deflater
import java.util.zip.Inflater
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/** permessage-deflate on Ktor's websocket, spoken by hand: the compression bit and the offer are what is under test. */
class WebSocketDeflateTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private val offer = "permessage-deflate; client_max_window_bits"

    /** Long enough that its EOSE is past [DEFLATE_MIN_BYTES], short enough for the engine's 256. */
    private val subId = "q".repeat(250)

    private val req = """["REQ","$subId",{"ids":["${"0".repeat(64)}"],"search":"include:spam"}]"""

    private fun <T> serving(
        deflate: Boolean,
        limits: RelayLimits = relayLimitsFromEnv(emptyMap()),
        block: (port: Int) -> T,
    ): T {
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), limits = limits, webSocketDeflate = deflate, wait = false)
        return try {
            block(
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                },
            )
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    private fun connect(port: Int): Socket =
        Socket().apply {
            connect(InetSocketAddress("127.0.0.1", port), 5_000)
            soTimeout = 5_000
        }

    /** The response head to an upgrade carrying [extensions], read byte by byte so no frame is consumed. */
    private fun Socket.upgrade(
        port: Int,
        extensions: String?,
    ): String {
        val offered = extensions?.let { "Sec-WebSocket-Extensions: $it\r\n" } ?: ""
        getOutputStream().apply {
            write(
                (
                    "GET / HTTP/1.1\r\nHost: localhost:$port\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n$offered\r\n"
                ).toByteArray(),
            )
            flush()
        }
        val input = getInputStream()
        val head = StringBuilder()
        while (!head.endsWith("\r\n\r\n")) head.append(input.read().takeIf { it >= 0 }?.toChar() ?: fail("closed during the upgrade: $head"))
        return head.toString()
    }

    private fun String.negotiatedDeflate(): Boolean = lines().any { it.startsWith("Sec-WebSocket-Extensions:", ignoreCase = true) && "permessage-deflate" in it }

    /** One masked text frame, RSV1 set when [compressed]. */
    private fun Socket.sendText(
        payload: ByteArray,
        compressed: Boolean,
    ) {
        val out = getOutputStream()
        out.write(if (compressed) 0xC1 else 0x81)
        when {
            payload.size < 126 -> {
                out.write(0x80 or payload.size)
            }

            payload.size <= 0xFFFF -> {
                out.write(0x80 or 126)
                out.write(payload.size shr 8)
                out.write(payload.size and 0xFF)
            }

            else -> {
                out.write(0x80 or 127)
                for (shift in 56 downTo 0 step 8) out.write((payload.size.toLong() shr shift).toInt() and 0xFF)
            }
        }
        val mask = byteArrayOf(1, 2, 3, 4)
        out.write(mask)
        out.write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it % 4].toInt()).toByte() })
        out.flush()
    }

    private class ServerFrame(
        val opcode: Int,
        val compressed: Boolean,
        val payload: ByteArray,
    )

    /** The next frame, or null once the server has closed the connection. */
    private fun DataInputStream.nextFrame(): ServerFrame? =
        try {
            val first = readUnsignedByte()
            val len =
                when (val short = readUnsignedByte() and 0x7F) {
                    126 -> readUnsignedShort().toLong()
                    127 -> readLong()
                    else -> short.toLong()
                }
            ServerFrame(first and 0x0F, first and 0x40 != 0, ByteArray(len.toInt()).also { readFully(it) })
        } catch (_: EOFException) {
            null
        } catch (_: SocketException) {
            null
        }

    /** A message as RFC 7692 puts it on the wire: raw deflate, sync-flushed, its empty-block tail removed. */
    private fun deflate(text: String): ByteArray {
        val deflater = Deflater(Deflater.DEFAULT_COMPRESSION, true)
        deflater.setInput(text.toByteArray(Charsets.UTF_8))
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        do {
            val n = deflater.deflate(buffer, 0, buffer.size, Deflater.SYNC_FLUSH)
            out.write(buffer, 0, n)
        } while (n == buffer.size)
        deflater.end()
        return out.toByteArray().let { it.copyOf(it.size - 4) }
    }

    private fun Inflater.inflateMessage(payload: ByteArray): String {
        setInput(payload + byteArrayOf(0, 0, -1, -1))
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        while (true) {
            val n = inflate(buffer)
            if (n == 0) break
            out.write(buffer, 0, n)
        }
        return out.toString(Charsets.UTF_8)
    }

    private fun ServerFrame.text(inflater: Inflater = Inflater(true)): String = if (compressed) inflater.inflateMessage(payload) else payload.toString(Charsets.UTF_8)

    /** The EOSE that ends [subId], and whether it arrived compressed. */
    private fun Socket.answerTo(subId: String): Pair<String, Boolean> {
        val input = DataInputStream(getInputStream())
        val inflater = Inflater(true)
        while (true) {
            val frame = input.nextFrame() ?: fail("the connection closed instead of answering")
            assertTrue(frame.opcode != 0x8, "closed instead of answering")
            if (frame.opcode != 0x1) continue
            val text = frame.text(inflater)
            if (subId in text) return text.also { assertContains(it, "EOSE") } to frame.compressed
        }
    }

    @Test
    fun `a client that offers deflate is read compressed and answered compressed`() {
        serving(deflate = true) { port ->
            connect(port).use { socket ->
                assertTrue(socket.upgrade(port, offer).negotiatedDeflate(), "the offer is accepted")
                socket.sendText(deflate(req), compressed = true)
                assertTrue(socket.answerTo(subId).second, "an answer past the threshold goes out compressed")
            }
        }
    }

    @Test
    fun `a client that offers nothing is served plain`() {
        serving(deflate = true) { port ->
            connect(port).use { socket ->
                assertFalse(socket.upgrade(port, extensions = null).negotiatedDeflate())
                socket.sendText(req.toByteArray(), compressed = false)
                assertFalse(socket.answerTo(subId).second, "nothing compressed for a client that did not ask")
            }
        }
    }

    @Test
    fun `with the switch off an offer is declined`() {
        serving(deflate = false) { port ->
            connect(port).use { socket ->
                assertFalse(socket.upgrade(port, offer).negotiatedDeflate())
                socket.sendText(req.toByteArray(), compressed = false)
                assertFalse(socket.answerTo(subId).second)
            }
        }
    }

    @Test
    fun `an offer with parameters the server cannot honour still connects`() {
        serving(deflate = true) { port ->
            for (odd in listOf("permessage-deflate; server_max_window_bits=10", "permessage-deflate; server_max_window_bits", "permessage-deflate; unknown_param")) {
                connect(port).use { socket ->
                    val head = socket.upgrade(port, odd)
                    assertTrue(head.startsWith("HTTP/1.1 101"), "$odd: $head")
                    socket.sendText(req.toByteArray(), compressed = false)
                    socket.answerTo(subId)
                }
            }
        }
    }

    @Test
    fun `a compressed frame that inflates past the cap is refused`() {
        val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1000"))
        // Small on the wire, far past the cap once inflated: only the inflated size can catch it.
        val bomb = """["REQ","bomb",{"ids":["${"0".repeat(64)}"],"search":"include:spam"}""" + " ".repeat(1_000_000) + "]"
        val wire = deflate(bomb)
        assertTrue(wire.size < maxFrameBytes(limits), "the compressed frame is under the frame cap")
        serving(deflate = true, limits = limits) { port ->
            connect(port).use { socket ->
                assertTrue(socket.upgrade(port, offer).negotiatedDeflate())
                socket.sendText(wire, compressed = true)
                val input = DataInputStream(socket.getInputStream())
                while (true) {
                    val frame =
                        try {
                            input.nextFrame()
                        } catch (_: SocketTimeoutException) {
                            fail("the server is still holding a frame it should have refused")
                        } ?: break
                    // The AUTH challenge goes out on connect, before the frame is read.
                    if (frame.opcode == 0x1) frame.text().let { assertTrue(it.startsWith("[\"AUTH\""), "answered a message past the cap: $it") }
                    if (frame.opcode == 0x8) break
                }
            }
            // Refused, not fallen over: the next client is served.
            connect(port).use { next ->
                assertTrue(next.upgrade(port, offer).negotiatedDeflate())
                next.sendText(deflate(req), compressed = true)
                next.answerTo(subId)
            }
        }
    }
}
