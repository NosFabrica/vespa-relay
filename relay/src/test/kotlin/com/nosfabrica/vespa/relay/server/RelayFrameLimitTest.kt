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
import java.io.DataInputStream
import java.io.EOFException
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The websocket's frame cap, spoken by hand: a client library would refuse to declare a frame it
 * does not send, and the declaration is what is under test.
 */
class RelayFrameLimitTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun <T> serving(
        limits: RelayLimits,
        block: (Socket) -> T,
    ): T {
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), limits = limits, wait = false)
        return try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            Socket().use { socket ->
                socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
                socket.soTimeout = 5_000
                upgrade(socket, port)
                block(socket)
            }
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    private fun upgrade(
        socket: Socket,
        port: Int,
    ) {
        socket.getOutputStream().apply {
            write(
                (
                    "GET / HTTP/1.1\r\nHost: localhost:$port\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
                ).toByteArray(),
            )
            flush()
        }
        // Byte by byte, so nothing after the headers is consumed by a buffer.
        val input = socket.getInputStream()
        val head = StringBuilder()
        while (!head.endsWith("\r\n\r\n")) head.append(input.read().takeIf { it >= 0 }?.toChar() ?: fail("closed during the upgrade"))
        assertTrue(head.startsWith("HTTP/1.1 101"), "upgraded: $head")
    }

    /** A masked text frame whose header declares [declared] bytes; only [payload] follows it. */
    private fun OutputStream.textFrame(
        payload: ByteArray,
        declared: Long = payload.size.toLong(),
    ) {
        write(0x81)
        when {
            declared < 126 -> {
                write(0x80 or declared.toInt())
            }

            declared <= 0xFFFF -> {
                write(0x80 or 126)
                write((declared shr 8).toInt() and 0xFF)
                write(declared.toInt() and 0xFF)
            }

            else -> {
                write(0x80 or 127)
                for (shift in 56 downTo 0 step 8) write((declared shr shift).toInt() and 0xFF)
            }
        }
        val mask = byteArrayOf(1, 2, 3, 4)
        write(mask)
        write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it % 4].toInt()).toByte() })
        flush()
    }

    private class ServerFrame(
        val opcode: Int,
        val payload: ByteArray,
    )

    /** The next frame from the server, or null when the server closed the connection. */
    private fun DataInputStream.nextFrame(): ServerFrame? =
        try {
            val opcode = readUnsignedByte() and 0x0F
            val len =
                when (val short = readUnsignedByte() and 0x7F) {
                    126 -> readUnsignedShort().toLong()
                    127 -> readLong()
                    else -> short.toLong()
                }
            ServerFrame(opcode, ByteArray(len.toInt()).also { readFully(it) })
        } catch (_: EOFException) {
            null
        }

    @Test
    fun `a frame declaring more than the cap is refused from its header`() {
        serving(relayLimitsFromEnv(emptyMap())) { socket ->
            // A gigabyte declared, a handful of bytes sent: the server must not wait for the rest.
            socket.getOutputStream().textFrame("[\"REQ\"".toByteArray(), declared = 1L shl 30)
            val input = DataInputStream(socket.getInputStream())
            while (true) {
                // A dropped connection is not a refusal: it is also what a server that tried to buffer the gigabyte does.
                val frame =
                    try {
                        input.nextFrame()
                    } catch (_: SocketTimeoutException) {
                        fail("the server is still reading a frame it should have refused from its header")
                    } ?: fail("the connection dropped without a close frame")
                if (frame.opcode != 0x8) continue
                val code = ((frame.payload[0].toInt() and 0xFF) shl 8) or (frame.payload[1].toInt() and 0xFF)
                assertEquals(1009, code, "closed as too big")
                break
            }
        }
    }

    /** The cap counts bytes and the engine counts characters, so a message the engine admits must fit it. */
    @Test
    fun `a message at the engine's length in three-byte characters is still read`() {
        val subId = "€".repeat(200)
        val req = """["REQ","$subId",{"ids":["${"0".repeat(64)}"],"search":"include:spam"}]"""
        serving(relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to req.length.toString()))) { socket ->
            socket.getOutputStream().textFrame(req.toByteArray(Charsets.UTF_8))
            val input = DataInputStream(socket.getInputStream())
            while (true) {
                val frame = input.nextFrame() ?: fail("the connection closed instead of answering")
                assertTrue(frame.opcode != 0x8, "closed instead of answering")
                if (frame.opcode == 0x1 && subId in frame.payload.toString(Charsets.UTF_8)) break
            }
        }
    }

    @Test
    fun `the cap admits a message of the engine's length in any characters`() {
        val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1000"))
        for (worst in listOf("€".repeat(1000), "😀".repeat(500), "a".repeat(1000))) {
            assertTrue(worst.toByteArray(Charsets.UTF_8).size <= maxFrameBytes(limits), "${worst.take(1)}: ${worst.length} chars")
        }
    }
}
