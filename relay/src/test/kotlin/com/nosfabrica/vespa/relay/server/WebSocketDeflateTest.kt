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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocket08FrameDecoder
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtension
import io.netty.handler.codec.http.websocketx.extensions.WebSocketExtensionData
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateClientExtensionHandshaker
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.zip.DataFormatException
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WebSocketDeflateTest {
    private val alloc = UnpooledByteBufAllocator.DEFAULT

    @Test
    fun `an offer is agreed whole or declined whole`() {
        assertNull(negotiateDeflate(emptyList()), "no offer, no compression")
        assertNull(negotiateDeflate(listOf("x-webkit-deflate-frame")), "an extension we do not speak")
        // Chromium's offer, then Firefox's and Safari's.
        for (offer in listOf("permessage-deflate; client_max_window_bits", "permessage-deflate")) {
            val terms = assertNotNull(negotiateDeflate(listOf(offer)), offer)
            assertEquals("permessage-deflate; client_no_context_takeover", terms.response)
            assertEquals(false, terms.serverNoContextTakeover)
        }
        val reset = assertNotNull(negotiateDeflate(listOf("permessage-deflate; server_no_context_takeover; client_no_context_takeover")))
        assertTrue(reset.serverNoContextTakeover)
        assertEquals("permessage-deflate; client_no_context_takeover; server_no_context_takeover", reset.response)
        assertEquals(
            "permessage-deflate; client_no_context_takeover; server_max_window_bits=15",
            negotiateDeflate(listOf("permessage-deflate; server_max_window_bits=15"))?.response,
        )
        // A window smaller than the JDK's cannot be promised; the client's next alternative can.
        assertEquals(
            "permessage-deflate; client_no_context_takeover",
            negotiateDeflate(listOf("permessage-deflate; server_max_window_bits=10, permessage-deflate"))?.response,
        )
        val refused =
            listOf(
                "permessage-deflate; server_max_window_bits=10",
                "permessage-deflate; server_max_window_bits",
                "permessage-deflate; client_max_window_bits=7",
                "permessage-deflate; server_no_context_takeover=1",
                "permessage-deflate; server_no_context_takeover; server_no_context_takeover",
                "permessage-deflate; x-unknown",
            )
        for (offer in refused) assertNull(negotiateDeflate(listOf(offer)), offer)
    }

    @Test
    fun `answers and live frames between them read back in order, through a client that keeps the server's window`() {
        val terms = assertNotNull(negotiateDeflate(listOf("permessage-deflate; client_max_window_bits")))
        assertEquals(notes(60), sendMixed(terms))
    }

    @Test
    fun `with server_no_context_takeover agreed, every frame reads on an empty window`() {
        val terms = assertNotNull(negotiateDeflate(listOf("permessage-deflate; server_no_context_takeover")))
        assertEquals(notes(60), sendMixed(terms))
    }

    @Test
    fun `a window given back while idle leaves the next answer readable`() {
        val terms = assertNotNull(negotiateDeflate(listOf("permessage-deflate")))
        val socket = SocketDeflate(terms, LoopDeflate())
        val client = client(terms)
        val texts = notes(20)
        texts.forEachIndexed { i, text ->
            if (i == 10) repeat(DEFLATE_IDLE_TICKS) { socket.tick() }
            client.writeInbound(socket.encodeText(alloc, text))
        }
        assertEquals(texts, client.texts())
        socket.end()
        client.finishAndReleaseAll()
    }

    @Test
    fun `a compressed answer is smaller than its text, and one too short to gain is sent as it is`() {
        val terms = assertNotNull(negotiateDeflate(listOf("permessage-deflate")))
        val socket = SocketDeflate(terms, LoopDeflate())
        val texts = notes(40)
        val frames = texts.map { socket.encodeText(alloc, it) }
        val wire = frames.sumOf { it.readableBytes() }
        val plain = texts.sumOf { ByteBufUtil.utf8Bytes(it) }
        assertTrue(wire < plain / 2, "$wire bytes on the wire for $plain of text")
        frames.forEach { it.release() }

        val eose = socket.encodeText(alloc, """["EOSE","a"]""")
        assertEquals(0x81, eose.getUnsignedByte(eose.readerIndex()).toInt(), "a short frame keeps RSV1 clear")
        eose.release()
        socket.end()
    }

    @Test
    fun `a client's compressed message is read, and one that inflates past the frame limit is refused`() {
        val terms = assertNotNull(negotiateDeflate(listOf("permessage-deflate")))
        val extension = clientExtension(terms)
        val writer = EmbeddedChannel(extension.newExtensionEncoder())
        val inflater = MessageInflater()
        val texts = listOf("""["REQ","a",{"kinds":[1],"limit":10}]""", notes(1)[0], """["CLOSE","a"]""" + " ".repeat(5_000))
        for (text in texts) {
            writer.writeOutbound(TextWebSocketFrame(text))
            val frame = writer.readOutbound<TextWebSocketFrame>()
            assertEquals(0x4, frame.rsv() and 0x4, "the client compressed it")
            assertEquals(text, inflater.inflateText(frame.content(), 1 shl 20))
            frame.release()
        }
        writer.writeOutbound(TextWebSocketFrame("[" + "0".repeat(200_000) + "]"))
        val bomb = writer.readOutbound<TextWebSocketFrame>()
        assertTrue(bomb.content().readableBytes() < 1_000)
        assertNull(inflater.inflateText(bomb.content(), 100_000), "past the limit once inflated")
        bomb.release()
        val garbage = alloc.buffer().writeBytes(byteArrayOf(0xFF.toByte(), 0x12, 0x34))
        assertFailsWith<DataFormatException> { inflater.inflateText(garbage, 1 shl 20) }
        garbage.release()
        assertEquals(texts[0], inflateOne(writer, inflater, texts[0]), "a refused message leaves the inflater reading")
        writer.finishAndReleaseAll()
    }

    @Test
    fun `a client that offers permessage-deflate reads every stored and live event, and is read in turn`() {
        val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), wait = false)
        val http = OkHttpClient()
        try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            val reader = Socket(http, port)
            assertEquals("permessage-deflate; client_no_context_takeover", reader.agreed)
            // OkHttp compresses what it sends from 1 KiB, so every fifth publish reaches the relay compressed.
            val signer = NostrSignerSync()
            val notes = (0 until 40).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), if (it % 5 == 0) "long note $it ".repeat(200) else "note $it é 日本") }
            val (stored, live) = notes.take(30) to notes.drop(30)
            stored.forEach { reader.send("""["EVENT",${it.toJson()}]""") }
            assertEquals(
                List(stored.size) { true },
                List(stored.size) {
                    reader
                        .next()[2]
                        .jsonPrimitive.content
                        .toBoolean()
                },
                "every publish accepted",
            )

            // An anonymous read waives the ranking lens the relay asks of one (LensRequiredPolicy).
            reader.send("""["REQ","feed",{"kinds":[1],"limit":100,"search":"include:spam"}]""")
            val answer = generateSequence(reader.next()) { previous -> if (previous[0].jsonPrimitive.content == "EVENT") reader.next() else null }.toList()
            assertEquals("EOSE", answer.last()[0].jsonPrimitive.content, "the answer ends at its EOSE: ${answer.last()}")
            val answered = answer.dropLast(1).map { it[2] as JsonObject }
            assertEquals(stored.map { it.id to it.content }.toSet(), answered.map { it.id() to it.content() }.toSet())

            val publisher = Socket(http, port)
            live.forEach { publisher.send("""["EVENT",${it.toJson()}]""") }
            val delivered = List(live.size) { reader.next() }
            assertTrue(delivered.all { it[0].jsonPrimitive.content == "EVENT" && it[1].jsonPrimitive.content == "feed" }, "$delivered")
            assertEquals(live.map { it.id to it.content }.toSet(), delivered.map { (it[2] as JsonObject).let { e -> e.id() to e.content() } }.toSet())
            reader.close()
            publisher.close()
        } finally {
            http.dispatcher.executorService.shutdown()
            server.stop(0, 0)
            relay.close()
        }
    }

    /** An OkHttp websocket on the relay: it offers permessage-deflate as browsers do, and reads each frame as JSON. */
    private class Socket(
        http: OkHttpClient,
        port: Int,
    ) : WebSocketListener() {
        private val opened = CompletableFuture<Response>()
        private val frames = LinkedBlockingQueue<String>()
        private val socket = http.newWebSocket(Request.Builder().url("ws://127.0.0.1:$port/").build(), this)
        val agreed: String? = opened.get(10, TimeUnit.SECONDS).header("Sec-WebSocket-Extensions")

        fun send(text: String) = check(socket.send(text))

        /** The next frame other than the relay's NIP-42 challenge. */
        fun next(): JsonArray =
            generateSequence { Json.parseToJsonElement(assertNotNull(frames.poll(10, TimeUnit.SECONDS), "a frame within 10s")).jsonArray }
                .first { it[0].jsonPrimitive.content != "AUTH" }

        fun close() = socket.close(1000, null)

        override fun onOpen(
            webSocket: WebSocket,
            response: Response,
        ) {
            opened.complete(response)
        }

        override fun onMessage(
            webSocket: WebSocket,
            text: String,
        ) {
            frames.add(text)
        }

        override fun onFailure(
            webSocket: WebSocket,
            t: Throwable,
            response: Response?,
        ) {
            opened.completeExceptionally(t)
            frames.add("""["FAILURE","$t"]""")
        }
    }

    private fun JsonObject.id() = getValue("id").jsonPrimitive.content

    private fun JsonObject.content() = getValue("content").jsonPrimitive.content

    private fun inflateOne(
        writer: EmbeddedChannel,
        inflater: MessageInflater,
        text: String,
    ): String? {
        writer.writeOutbound(TextWebSocketFrame(text))
        val frame = writer.readOutbound<TextWebSocketFrame>()
        return inflater.inflateText(frame.content(), 1 shl 20).also { frame.release() }
    }

    /**
     * Sixty [notes] sent to one client as a socket sends them: answers through its window, and every third a live
     * frame whose body was deflated once for every socket, half of those written as a head and a shared body.
     */
    private fun sendMixed(terms: DeflateTerms): List<String> {
        val loop = LoopDeflate()
        val socket = SocketDeflate(terms, loop)
        val client = client(terms)
        for ((i, text) in notes(60).withIndex()) {
            if (i % 3 != 2) {
                client.writeInbound(socket.encodeText(alloc, text))
                continue
            }
            val prefix = text.substringBefore("{")
            val plain = alloc.buffer().also { ByteBufUtil.writeUtf8(it, text.substring(prefix.length)) }
            val body = alloc.buffer()
            loop.fresh.deflate(plain, body, keepContext = false)
            socket.bypassed()
            if (i % 2 == 0) {
                client.writeInbound(encodeLiveFrame(alloc, prefix, body, deflated = true))
            } else {
                client.writeInbound(encodeLiveFrameHead(alloc, prefix, body.readableBytes(), deflated = true), body.retainedDuplicate())
            }
            plain.release()
            body.release()
        }
        socket.end()
        return client.texts().also { client.finishAndReleaseAll() }
    }

    /** Netty's own client side of [terms]: it keeps the server's window across messages unless the terms say not to. */
    private fun clientExtension(terms: DeflateTerms): WebSocketClientExtension {
        val parameters =
            terms.response
                .split(';')
                .drop(1)
                .associate { it.trim().substringBefore('=') to it.substringAfter('=', "").ifEmpty { null } }
        return assertNotNull(
            PerMessageDeflateClientExtensionHandshaker(6, false, 15, true, terms.serverNoContextTakeover)
                .handshakeExtension(WebSocketExtensionData("permessage-deflate", parameters)),
            "Netty's client accepts ${terms.response}",
        )
    }

    private fun client(terms: DeflateTerms) = EmbeddedChannel(WebSocket08FrameDecoder(false, true, 1 shl 24), clientExtension(terms).newExtensionDecoder())

    private fun EmbeddedChannel.texts(): List<String> =
        generateSequence { readInbound<TextWebSocketFrame>() }
            .map { frame -> frame.text().also { frame.release() } }
            .toList()

    /**
     * EVENT frames that repeat what a feed repeats (an author, the field names, a tag) so frames refer back to
     * earlier ones, with text that is not ASCII; every seventh is past 64 KiB, and every other of those stays so compressed.
     */
    private fun notes(count: Int): List<String> {
        val random = Random(7)
        val authors = List(3) { hex(random, 64) }
        return List(count) { i ->
            val content =
                when (i % 14) {
                    6 -> "long ".repeat(14_000)
                    13 -> hex(random, 140_000)
                    else -> "note $i, é 日本 🌍 ${hex(random, 12)}"
                }
            """["EVENT","sub-$i",{"id":"${hex(random, 64)}","pubkey":"${authors[i % 3]}","created_at":${1_700_000_000 + i},""" +
                """"kind":1,"tags":[["p","${authors[(i + 1) % 3]}"]],"content":"$content","sig":"${hex(random, 128)}"}]"""
        }
    }

    private fun hex(
        random: Random,
        chars: Int,
    ): String = buildString { repeat(chars) { append("0123456789abcdef"[random.nextInt(16)]) } }
}
