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
import com.nosfabrica.vespa.relay.server.config.DEFAULT_MAX_FRAME_BYTES
import com.nosfabrica.vespa.relay.server.config.defaultMaxFrameBytes
import com.nosfabrica.vespa.relay.server.config.maxFrameBytesFromEnv
import com.nosfabrica.vespa.relay.server.config.messageBytes
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.io.DataInputStream
import java.io.EOFException
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The websocket's frame cap, spoken by hand: a client library would refuse to declare a frame it
 * does not send, and the declaration is what is under test.
 */
class RelayFrameLimitTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** Each transport in turn: Ktor's websocket, then the native one, which must enforce the same cap. */
    private fun bothTransports(
        limits: RelayLimits,
        maxFrameBytes: Int = defaultMaxFrameBytes(limits),
        block: (Socket, Boolean) -> Unit,
    ) {
        for (native in listOf(false, true)) serving(limits, maxFrameBytes, native) { socket, _ -> block(socket, native) }
    }

    private fun <T> serving(
        limits: RelayLimits,
        maxFrameBytes: Int,
        nativeWebSocket: Boolean,
        block: (Socket, Int) -> T,
    ): T {
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), limits = limits, maxFrameBytes = maxFrameBytes, nativeWebSocket = nativeWebSocket, wait = false)
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
                block(socket, port)
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
        opcode: Int = 0x1,
        fin: Boolean = true,
    ) {
        write((if (fin) 0x80 else 0) or opcode)
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

    /** [payload] as one text message split into frames of at most [chunk] bytes. */
    private fun OutputStream.fragmented(
        payload: ByteArray,
        chunk: Int,
    ) {
        val parts = (payload.indices step chunk).map { payload.copyOfRange(it, minOf(it + chunk, payload.size)) }
        parts.forEachIndexed { i, part -> textFrame(part, opcode = if (i == 0) 0x1 else 0x0, fin = i == parts.lastIndex) }
    }

    private val signer = NostrSignerSync()

    private fun note(text: String): Event = signer.sign(1_700_000_000L, 1, emptyArray(), text)

    /** A follow list of [follows] keys: a real kind 3 of about 73 bytes a follow. */
    private fun followList(follows: Int): Event = signer.sign(1_700_000_000L, 3, Array(follows) { arrayOf("p", it.toString(16).padStart(64, '0')) }, "")

    private fun eventFrame(event: Event) = """["EVENT",${event.toJson()}]""".toByteArray(Charsets.UTF_8)

    /** Reads until every id in [ids] has its OK, and returns each OK by id; a close or EOF first fails. */
    private fun DataInputStream.oks(ids: Set<String>): Map<String, String> {
        val oks = mutableMapOf<String, String>()
        while (oks.keys != ids) {
            val frame =
                try {
                    nextFrame()
                } catch (_: SocketTimeoutException) {
                    fail("still waiting for an OK on ${ids - oks.keys}; got $oks")
                } catch (_: SocketException) {
                    null
                } ?: fail("the connection dropped before every OK; got $oks")
            assertTrue(frame.opcode != 0x8, "closed before every OK: ${frame.payload.drop(2).toByteArray().toString(Charsets.UTF_8)}; got $oks")
            val text = frame.payload.toString(Charsets.UTF_8)
            ids.firstOrNull { text.startsWith("""["OK","$it",""") }?.let { oks[it] = text }
        }
        return oks
    }

    /** The connection still answers: a REQ gets a frame on its sub id. */
    private fun Socket.stillServes() {
        getOutputStream().textFrame("""["REQ","after",{"ids":["${"0".repeat(64)}"],"search":"include:spam"}]""".toByteArray())
        val input = DataInputStream(getInputStream())
        while (true) {
            val frame = input.nextFrame() ?: fail("the connection closed after the oversized message")
            assertTrue(frame.opcode != 0x8, "closed after the oversized message")
            if (frame.opcode == 0x1 && "\"after\"" in frame.payload.toString(Charsets.UTF_8)) return
        }
    }

    private fun assertRefusedAsTooLarge(ok: String?) {
        assertTrue(ok != null && ok.contains(",false,") && "message too large" in ok, "the oversized EVENT's OK: $ok")
    }

    @Test
    fun `an EVENT over the engine's length is answered by id and the connection keeps serving`() {
        val before = note("before")
        val big = followList(14_000)
        val after = note("after")
        assertTrue(eventFrame(big).size in 1_000_000..2_000_000, "a 1 MB follow list: ${eventFrame(big).size}")
        bothTransports(relayLimitsFromEnv(emptyMap())) { socket, _ ->
            val out = socket.getOutputStream()
            for (e in listOf(before, big, after)) out.textFrame(eventFrame(e))
            val oks = DataInputStream(socket.getInputStream()).oks(setOf(before.id, big.id, after.id))
            assertTrue(",true," in oks.getValue(before.id), oks.getValue(before.id))
            assertRefusedAsTooLarge(oks[big.id])
            assertTrue(",true," in oks.getValue(after.id), oks.getValue(after.id))
            socket.stillServes()
        }
    }

    @Test
    fun `a run of oversized EVENTs past the read budget is answered in full`() {
        // 24 MB queued from one connection pauses its reads on bytes long before 64 commands; each must still resume.
        val bigs = (0 until 24).map { n -> signer.sign<Event>(1_700_000_000L + n, 3, Array(14_000) { arrayOf("p", it.toString(16).padStart(64, '0')) }, "") }
        val after = note("after")
        bothTransports(relayLimitsFromEnv(emptyMap())) { socket, _ ->
            val out = socket.getOutputStream()
            for (e in bigs + after) out.textFrame(eventFrame(e))
            val oks = DataInputStream(socket.getInputStream()).oks((bigs + after).map { it.id }.toSet())
            for (e in bigs) assertRefusedAsTooLarge(oks[e.id])
            assertTrue(",true," in oks.getValue(after.id), oks.getValue(after.id))
        }
    }

    @Test
    fun `a fragmented EVENT over the engine's length is answered by id like a whole one`() {
        val big = followList(14_000)
        val after = note("after")
        bothTransports(relayLimitsFromEnv(emptyMap())) { socket, _ ->
            val out = socket.getOutputStream()
            out.fragmented(eventFrame(big), chunk = 200_000)
            out.textFrame(eventFrame(after))
            val oks = DataInputStream(socket.getInputStream()).oks(setOf(big.id, after.id))
            assertRefusedAsTooLarge(oks[big.id])
            assertTrue(",true," in oks.getValue(after.id), oks.getValue(after.id))
            socket.stillServes()
        }
    }

    /** Every frame up to the close, which must say 1009; a timeout means the server is still reading. */
    private fun DataInputStream.untilTooBig(): List<ServerFrame> {
        val seen = mutableListOf<ServerFrame>()
        while (true) {
            val frame =
                try {
                    nextFrame()
                } catch (_: SocketTimeoutException) {
                    fail("the server is still reading a message it should have refused")
                } catch (_: SocketException) {
                    null
                } ?: fail("dropped without a close frame")
            seen += frame
            if (frame.opcode != 0x8) continue
            val code = ((frame.payload[0].toInt() and 0xFF) shl 8) or (frame.payload[1].toInt() and 0xFF)
            assertEquals(1009, code, "closed as too big")
            return seen
        }
    }

    @Test
    fun `a fragmented message past the frame cap closes with 1009 after a NOTICE`() {
        val limits = relayLimitsFromEnv(emptyMap())
        val cap = messageBytes(limits).toInt() * 2
        bothTransports(limits, maxFrameBytes = cap) { socket, native ->
            // Fragments each under the cap, together over it: the aggregate is bounded, not just each frame.
            val out = socket.getOutputStream()
            out.textFrame("[\"EVENT\",{\"content\":\"".toByteArray(), fin = false)
            repeat(cap / 100_000 + 1) { out.textFrame(ByteArray(100_000) { 'a'.code.toByte() }, opcode = 0x0, fin = false) }
            val frames = DataInputStream(socket.getInputStream()).untilTooBig()
            // Ktor closes from inside its session, with no hook for a NOTICE.
            if (native) assertTrue(frames.any { it.opcode == 0x1 && it.payload.toString(Charsets.UTF_8).startsWith("[\"NOTICE\"") }, "a NOTICE before the close")
        }
    }

    @Test
    fun `MAX_FRAME_BYTES is at least the engine's length in bytes`() {
        val limits = relayLimitsFromEnv(emptyMap())
        assertEquals(DEFAULT_MAX_FRAME_BYTES, maxFrameBytesFromEnv(emptyMap(), limits))
        assertEquals(8_000_000, maxFrameBytesFromEnv(mapOf("MAX_FRAME_BYTES" to "8000000"), limits))
        assertFailsWith<IllegalStateException> { maxFrameBytesFromEnv(mapOf("MAX_FRAME_BYTES" to (messageBytes(limits) - 1).toString()), limits) }
        // An engine length past the default raises the default with it.
        val long = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "8000000"))
        assertEquals(24_000_000, maxFrameBytesFromEnv(emptyMap(), long))
    }

    @Test
    fun `a frame declaring more than the cap is refused from its header`() {
        val limits = relayLimitsFromEnv(emptyMap())
        val cap = defaultMaxFrameBytes(limits)
        for (native in listOf(false, true)) {
            serving(limits, cap, native) { socket, port ->
                // One byte over the cap, a handful sent: a server without the cap would sit waiting for the
                // rest, so a timeout is the failure. A 1009 close or a dropped connection is the refusal.
                socket.getOutputStream().textFrame("[\"REQ\"".toByteArray(), declared = cap + 1L)
                val input = DataInputStream(socket.getInputStream())
                while (true) {
                    val frame =
                        try {
                            input.nextFrame()
                        } catch (_: SocketTimeoutException) {
                            fail("the server is still reading a frame it should have refused from its header")
                        } catch (_: SocketException) {
                            null
                        } ?: break
                    if (frame.opcode != 0x8) continue
                    val code = ((frame.payload[0].toInt() and 0xFF) shl 8) or (frame.payload[1].toInt() and 0xFF)
                    assertEquals(1009, code, "closed as too big")
                    break
                }
                // Refused, not fallen over: the next client is served.
                Socket().use { next ->
                    next.connect(InetSocketAddress("127.0.0.1", port), 5_000)
                    next.soTimeout = 5_000
                    upgrade(next, port)
                }
            }
        }
    }

    /** The cap counts bytes and the engine counts characters, so a message the engine admits must fit it. */
    @Test
    fun `a message at the engine's length in three-byte characters is still read`() {
        val subId = "€".repeat(200)
        val req = """["REQ","$subId",{"ids":["${"0".repeat(64)}"],"#t":["${"€".repeat(250)}"],"search":"include:spam"}]"""
        bothTransports(relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to req.length.toString()))) { socket, _ ->
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
            assertTrue(worst.toByteArray(Charsets.UTF_8).size <= messageBytes(limits), "${worst.take(1)}: ${worst.length} chars")
        }
    }
}
