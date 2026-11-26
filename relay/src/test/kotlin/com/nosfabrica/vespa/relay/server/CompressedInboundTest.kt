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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelOption
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.util.ReferenceCountUtil
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Client messages compressed with permessage-deflate (RFC 7692) on the native transport: every limit applies to
 * the inflated bytes, which are inflated as they arrive and held, scanned or refused like any message's.
 */
class CompressedInboundTest {
    private val streamPast = 786_432L
    private val cap = 4L shl 20
    private val id = "7".repeat(64)
    private val deflate = "permessage-deflate"

    /** The frames of [text] compressed, RSV1 on the first, in frames of at most [chunk] compressed bytes. */
    private fun compressedFrames(
        text: String,
        chunk: Int = Int.MAX_VALUE,
    ): ByteBuf {
        val payload = deflateMessage(text.toByteArray())
        val wire = Unpooled.buffer()
        val parts = (payload.indices step chunk.coerceAtMost(payload.size)).map { payload.copyOfRange(it, minOf(it + chunk, payload.size)) }
        parts.forEachIndexed { i, part ->
            val frame = clientFrame(part, if (i == 0) 0x1 else 0x0, i == parts.lastIndex)
            if (i == 0) frame.setByte(0, frame.getByte(0).toInt() or 0x40)
            wire.writeBytes(frame)
            frame.release()
        }
        return wire
    }

    private class Fed(
        val out: List<Any>,
        val error: Throwable?,
        val peak: Long,
        val inflated: Long,
    )

    private fun feed(
        wire: ByteBuf,
        inflaters: InflaterPool? = InflaterPool(),
    ): Fed {
        val alloc = PeakAllocator()
        val decoder = InboundFrameDecoder(InboundLimits(cap, streamPast), inflaters)
        val channel = EmbeddedChannel()
        channel.config().setOption(ChannelOption.ALLOCATOR, alloc)
        channel.pipeline().addLast(decoder)
        var error: Throwable? = null
        while (wire.isReadable && error == null) {
            try {
                channel.writeInbound(wire.readRetainedSlice(minOf(16_384, wire.readableBytes())))
            } catch (e: Exception) {
                error = e
            }
            alloc.sample()
        }
        wire.release()
        val out = generateSequence { channel.readInbound<Any>() }.toList()
        val fed = Fed(out, error, alloc.peak, decoder.inflatedBytes)
        runCatching { channel.finishAndReleaseAll() }
        return fed
    }

    @Test
    fun `a compressed message the engine may take is handed on as its text`() {
        val text = """["EVENT",{"id":"$id","content":"${"hello ".repeat(20_000)}"}]"""
        for (chunk in listOf(Int.MAX_VALUE, 97)) {
            val fed = feed(compressedFrames(text, chunk))
            val frame = fed.out.single() as TextWebSocketFrame
            assertEquals(text, frame.text(), "in frames of $chunk")
            assertEquals(0, frame.rsv(), "handed on inflated")
            frame.release()
        }
    }

    @Test
    fun `a compressed message past the engine's length is answered from its id, never inflated whole`() {
        // A kilobyte on the wire, two megabytes inflated: held only up to the engine's length, then scanned.
        val text = """["EVENT",{"content":"${"0".repeat(2_000_000)}","id":"$id"}]"""
        val fed = feed(compressedFrames(text, 300))
        assertEquals("""["EVENT",{"id":"$id"}]""", (fed.out.single() as OversizedMessage).head)
        assertTrue(fed.peak <= streamPast + HEAD_BYTES + 65_536, "held ${fed.peak} bytes")
        fed.out.forEach { ReferenceCountUtil.release(it) }
    }

    @Test
    fun `a compressed bomb is refused once it inflates past the cap, and inflating stops there`() {
        val bomb = """["EVENT",{"content":"${"0".repeat(64 shl 20)}"}]"""
        val fed = feed(compressedFrames(bomb, 16_384))
        val error = fed.error as? CorruptedWebSocketFrameException ?: fail("not refused: ${fed.out.map { it::class.simpleName }}")
        assertEquals(1009, error.closeStatus().code())
        assertTrue(fed.inflated in cap..cap + 65_536, "inflated ${fed.inflated} bytes of 64 MiB")
        assertTrue(fed.peak <= streamPast + HEAD_BYTES + 65_536, "held ${fed.peak} bytes")
    }

    @Test
    fun `RSV1 is refused without deflate, on a continuation, and on a control frame, as are RSV2 and RSV3`() {
        fun refusal(
            wire: ByteBuf,
            inflaters: InflaterPool? = InflaterPool(),
        ): String {
            val error = feed(wire, inflaters).error as? CorruptedWebSocketFrameException ?: fail("not refused")
            return "${error.closeStatus().code()}"
        }
        assertEquals("1002", refusal(compressedFrames("""["REQ","x",{}]"""), inflaters = null), "RSV1 without deflate")
        val cont = Unpooled.buffer()
        cont.writeBytes(clientFrame("""["REQ",""".toByteArray(), 0x1, false))
        cont.writeBytes(clientFrame(""""x",{}]""".toByteArray(), 0x0, true).also { it.setByte(0, it.getByte(0).toInt() or 0x40) })
        assertEquals("1002", refusal(cont), "RSV1 on a continuation")
        assertEquals("1002", refusal(clientFrame(ByteArray(0), 0x9).also { it.setByte(0, it.getByte(0).toInt() or 0x40) }), "RSV1 on a ping")
        assertEquals("1002", refusal(clientFrame("x".toByteArray()).also { it.setByte(0, it.getByte(0).toInt() or 0x20) }), "RSV2")
        assertEquals("1002", refusal(clientFrame("x".toByteArray()).also { it.setByte(0, it.getByte(0).toInt() or 0x10) }), "RSV3")
    }

    @Test
    fun `over a socket, a compressed EVENT is answered OK true and an oversized one OK false by id, the connection carrying on`() {
        val signer = NostrSignerSync()
        val small = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "compressed")
        val big = """["EVENT",{"content":"${"z".repeat(3_000_000)}","id":"$id"}]"""
        withRawRelay(native = true, extensions = deflate) { ws ->
            ws.compressed("""["EVENT",${small.toJson()}]""")
            ws.compressed(big, chunk = 1_000)
            val texts = ws.textsUntil { seen -> seen.any { it.startsWith("""["OK","${small.id}",""") } && seen.any { it.startsWith("""["OK","$id",""") } }
            assertTrue(texts.any { it.startsWith("""["OK","${small.id}",true""") }, texts.toString())
            assertEquals("""["OK","$id",false,"invalid: message too large (max 262144)"]""", texts.first { it.startsWith("""["OK","$id",""") })
            ws.stillServes()
        }
    }

    @Test
    fun `over a socket, a compressed bomb closes 1009 after a NOTICE`() {
        withRawRelay(native = true, extensions = deflate, maxFrameBytes = cap.toInt()) { ws ->
            runCatching { ws.compressed("""["EVENT",{"content":"${"0".repeat(64 shl 20)}"}]""", chunk = 16_384) }
            val frames = ws.untilClose()
            assertTrue(frames.any { it.opcode == 0x1 && it.text.startsWith("[\"NOTICE\"") }, "a NOTICE first")
            assertEquals(1009, frames.last().closeCode)
        }
    }

    @Test
    fun `a closing connection gives its compression window back, whichever way it closes`() {
        val signer = NostrSignerSync()
        val held = heldDeflateWindows()
        val ways: List<Pair<String, RawWebSocket.() -> Unit>> =
            listOf(
                "past the cap" to { send("[\"EVENT\"".toByteArray(), declared = cap + 1) },
                "a protocol error" to { send("x".toByteArray(), masked = false) },
                "the held-message deadline" to { send("[\"EVENT\"".toByteArray(), fin = false) },
            )
        for ((way, end) in ways) {
            withRawRelay(native = true, extensions = deflate, maxFrameBytes = cap.toInt(), openMessageDeadlineMs = 1_500, soTimeoutMs = 15_000) { ws ->
                // An OK long enough to be compressed through the socket's window opens one.
                val e = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), way)
                ws.text("""["EVENT",${e.toJson()}]""")
                ws.textsUntil { seen -> seen.any { it.startsWith("""["OK","${e.id}",""") } }
                assertTrue(heldDeflateWindows() > held, "$way: the answer held no window")
                ws.end()
                ws.untilClose(withinMs = 15_000)
                ws.awaitEnd()
            }
            val until = System.nanoTime() + 10_000_000_000L
            while (heldDeflateWindows() > held && System.nanoTime() < until) Thread.sleep(50)
            assertEquals(held, heldDeflateWindows(), "$way: the window was kept")
        }
    }
}
