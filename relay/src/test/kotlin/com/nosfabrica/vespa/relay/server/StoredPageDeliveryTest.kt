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
import com.nosfabrica.vespa.relay.server.config.defaultRelayLimits
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.io.DataInputStream
import java.io.EOFException
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/** A stored page the relay's own limits allow reaches a client that reads it late, on both transports. */
class StoredPageDeliveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val perFilter = 5_000
    private val filters = 3
    private val total = perFilter * filters
    private val base = 1_700_000_000L

    @Test
    fun `a page past the live frame bound ends in EOSE for a client that starts reading late`() {
        assertTrue(total > MAX_OUTGOING_BUFFER, "the page must outgrow the live bound to prove anything")
        assertTrue(filters <= defaultRelayLimits().maxFilters!! && perFilter <= defaultRelayLimits().maxLimit!!)
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
        val signer = NostrSignerSync()
        // Large enough that the socket buffers hold a small share of the page, so most of it waits in the queue.
        val content = "x".repeat(3_000)
        runBlocking { repeat(total) { store.insert(signer.sign<Event>(base + it, 1, emptyArray(), content)) } }
        for (native in listOf(false, true)) {
            assertEquals(total, readPage(store, native), "events before EOSE, native=$native")
        }
    }

    private fun readPage(
        store: NostrSemanticsStore,
        nativeWebSocket: Boolean,
    ): Int {
        val relay = NostrRelayServer(store, relayUrl, requireReadLens = false)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), nativeWebSocket = nativeWebSocket, wait = false)
        return try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            Socket().use { socket ->
                socket.receiveBufferSize = 4_096
                socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
                socket.soTimeout = 20_000
                upgrade(socket, port)
                val windows =
                    (0 until filters).joinToString(",") {
                        """{"kinds":[1],"since":${base + it * perFilter},"until":${base + (it + 1) * perFilter - 1},"limit":$perFilter}"""
                    }
                sendText(socket, """["REQ","page",$windows]""")
                // The store answers at once; the client is busy elsewhere for a moment, as a phone on a slow link is.
                Thread.sleep(3_000)
                countUntilEose(DataInputStream(socket.getInputStream().buffered()))
            }
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    private fun countUntilEose(input: DataInputStream): Int {
        var events = 0
        while (true) {
            val (opcode, payload) =
                try {
                    nextFrame(input)
                } catch (_: EOFException) {
                    fail("closed without a close frame after $events events")
                } catch (_: SocketException) {
                    fail("reset after $events events")
                }
            when (opcode) {
                0x8 -> {
                    fail("closed after $events events: ${payload.drop(2).toByteArray().toString(Charsets.UTF_8)}")
                }

                0x1 -> {
                    val text = payload.toString(Charsets.UTF_8)
                    if (text.startsWith("[\"EOSE\",\"page\"")) return events
                    if (text.startsWith("[\"CLOSED\"")) fail("CLOSED after $events events: $text")
                    if (text.startsWith("[\"EVENT\",\"page\"")) events++
                }
            }
        }
    }

    private fun nextFrame(input: DataInputStream): Pair<Int, ByteArray> {
        val opcode = input.readUnsignedByte() and 0x0F
        val len =
            when (val short = input.readUnsignedByte() and 0x7F) {
                126 -> input.readUnsignedShort().toLong()
                127 -> input.readLong()
                else -> short.toLong()
            }
        return opcode to ByteArray(len.toInt()).also { input.readFully(it) }
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
        val input = socket.getInputStream()
        val head = StringBuilder()
        while (!head.endsWith("\r\n\r\n")) head.append(input.read().takeIf { it >= 0 }?.toChar() ?: fail("closed during the upgrade"))
        assertTrue(head.startsWith("HTTP/1.1 101"), "upgraded: $head")
    }

    /** One masked client text frame. */
    private fun sendText(
        socket: Socket,
        text: String,
    ) {
        val payload = text.toByteArray(Charsets.UTF_8)
        val out = socket.getOutputStream()
        out.write(0x81)
        when {
            payload.size < 126 -> {
                out.write(0x80 or payload.size)
            }

            else -> {
                out.write(0x80 or 126)
                out.write((payload.size shr 8) and 0xFF)
                out.write(payload.size and 0xFF)
            }
        }
        val mask = byteArrayOf(1, 2, 3, 4)
        out.write(mask)
        out.write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it % 4].toInt()).toByte() })
        out.flush()
    }
}
