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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocket08FrameDecoder
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NativeWebSocketTest {
    @Test
    fun `a pre-encoded frame is what a client's decoder reads, at every header size`() {
        // Each length class of RFC 6455's header: 7 bits, 16 bits, 64 bits; and text that is not ASCII.
        val texts = listOf("[\"EOSE\",\"a\"]", "x".repeat(125), "y".repeat(126), "z".repeat(65_535), "w".repeat(70_000), "[\"EVENT\",\"é\",{\"content\":\"日本 🌍\"}]")
        for (text in texts) {
            val client = EmbeddedChannel(WebSocket08FrameDecoder(false, false, 1 shl 20))
            assertTrue(client.writeInbound(encodeTextFrame(UnpooledByteBufAllocator.DEFAULT, text)), "a frame of ${text.length} chars decodes")
            val frame = client.readInbound<TextWebSocketFrame>()
            assertTrue(frame.isFinalFragment)
            assertEquals(text, frame.text())
            frame.release()
            client.finishAndReleaseAll()
        }
    }

    @Test
    fun `live frames appended to one buffer are read as those frames, and the shared body stays whole`() {
        val alloc = UnpooledByteBufAllocator.DEFAULT
        val bodies = listOf("{\"content\":\"日本 🌍\"}", "{\"content\":\"" + "y".repeat(70_000) + "\"}")
        for (body in bodies) {
            val shared = alloc.buffer().also { ByteBufUtil.writeUtf8(it, body + "]") }
            val prefixes = listOf("[\"EVENT\",\"a\",", "[\"EVENT\",\"é-${"s".repeat(130)}\",")
            val out = encodeLiveFrame(alloc, prefixes[0], shared)
            appendLiveFrame(out, prefixes[1], shared)
            assertEquals(body + "]", shared.toString(Charsets.UTF_8), "the shared body is copied, never consumed")
            val client = EmbeddedChannel(WebSocket08FrameDecoder(false, false, 1 shl 20))
            client.writeInbound(out)
            for (prefix in prefixes) {
                val frame = client.readInbound<TextWebSocketFrame>()
                assertEquals(prefix + body + "]", frame.text())
                frame.release()
            }
            client.finishAndReleaseAll()
            shared.release()
        }
    }

    @Test
    fun `a live frame's head and its shared body, written apart, are read as one frame`() {
        val alloc = UnpooledByteBufAllocator.DEFAULT
        val body = "{\"content\":\"" + "z".repeat(70_000) + "\"}]"
        val shared = alloc.buffer().also { ByteBufUtil.writeUtf8(it, body) }
        val client = EmbeddedChannel(WebSocket08FrameDecoder(false, false, 1 shl 20))
        client.writeInbound(encodeLiveFrameHead(alloc, "[\"EVENT\",\"a\",", shared.readableBytes()), shared.retainedDuplicate())
        val frame = client.readInbound<TextWebSocketFrame>()
        assertEquals("[\"EVENT\",\"a\"," + body, frame.text())
        frame.release()
        client.finishAndReleaseAll()
        shared.release()
    }

    /** The status line [request] gets from a relay serving the native websocket. */
    private fun statusLine(
        port: Int,
        request: String,
    ): String =
        Socket().use { socket ->
            socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
            socket.soTimeout = 5_000
            socket.getOutputStream().apply {
                write(request.toByteArray())
                flush()
            }
            socket
                .getInputStream()
                .bufferedReader()
                .readLine()
                .orEmpty()
        }

    /** The status lines a relay on each transport gives [requests], one connection each. */
    private fun answers(
        nativeWebSocket: Boolean,
        requests: List<String>,
    ): List<String> {
        val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), nativeWebSocket = nativeWebSocket, wait = false)
        return try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            requests.map { statusLine(port, it) }
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    @Test
    fun `an upgrade the native handler does not take is answered as the Ktor path answers it`() {
        val keyless = "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n"
        val whole = keyless.replace("Sec-WebSocket-Version", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version")
        val requests = listOf(keyless, whole)
        val native = answers(nativeWebSocket = true, requests)
        assertTrue(native.all { it.startsWith("HTTP/1.1 ") }, "every request answered, none reset: $native")
        assertEquals(answers(nativeWebSocket = false, requests), native)
        assertTrue(native[1].startsWith("HTTP/1.1 101"), "a whole upgrade is taken")
    }
}
