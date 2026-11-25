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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelOption
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.util.ReferenceCountUtil
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** What one connection's decoder holds and hands on, fed the way a socket feeds it: in reads of a few kilobytes. */
private const val BACKSLASH = "\\"

class InboundFrameDecoderTest {
    /** The engine's default length in bytes: past it a message is only scanned. */
    private val streamPast = 786_432L
    private val limits = InboundLimits(maxMessageBytes = 16L shl 20, streamPastBytes = streamPast)
    private val id = "9".repeat(64)
    private val bigEvent = """["EVENT",{"content":"${"x".repeat(3_000_000)}","id":"$id"}]""".toByteArray()

    private class Fed(
        val out: List<Any>,
        val peak: Long,
    )

    private fun feed(
        wire: ByteBuf,
        read: Int = 16_384,
    ): Fed {
        val alloc = PeakAllocator()
        val channel = EmbeddedChannel()
        channel.config().setOption(ChannelOption.ALLOCATOR, alloc)
        channel.pipeline().addLast(InboundFrameDecoder(limits))
        while (wire.isReadable) {
            channel.writeInbound(wire.readRetainedSlice(minOf(read, wire.readableBytes())))
            alloc.sample()
        }
        wire.release()
        val out = generateSequence { channel.readInbound<Any>() }.toList()
        channel.finishAndReleaseAll()
        return Fed(out, alloc.peak)
    }

    private fun fragments(
        payload: ByteArray,
        chunk: Int,
    ): ByteBuf {
        val wire = Unpooled.buffer()
        val parts = (payload.indices step chunk).map { payload.copyOfRange(it, minOf(it + chunk, payload.size)) }
        parts.forEachIndexed { i, part -> wire.writeBytes(clientFrame(part, if (i == 0) 0x1 else 0x0, i == parts.lastIndex)) }
        return wire
    }

    private fun Fed.only(): Any = out.single().also { ReferenceCountUtil.release(it) }

    @Test
    fun `a message past the engine's length is answered from its id without being held`() {
        val fed = feed(Unpooled.wrappedBuffer(clientFrame(bigEvent)))
        assertEquals("""["EVENT",{"id":"$id"}]""", (fed.only() as OversizedMessage).head)
        // One read and the kept head: Netty's decoder and aggregator held 7.2 MB for this one.
        assertTrue(fed.peak <= HEAD_BYTES + 16_384 + 4_096, "held ${fed.peak} bytes")
    }

    @Test
    fun `a fragmented one is held only up to the engine's length`() {
        val fed = feed(fragments(bigEvent, 200_000))
        assertEquals("""["EVENT",{"id":"$id"}]""", (fed.only() as OversizedMessage).head)
        assertTrue(fed.peak <= streamPast + HEAD_BYTES + 16_384, "held ${fed.peak} bytes")
    }

    @Test
    fun `a message the engine may take is handed on whole in a buffer of its own size`() {
        val text = """["EVENT",{"content":"${"y".repeat(700_000)}"}]"""
        for (wire in listOf(Unpooled.wrappedBuffer(clientFrame(text.toByteArray())), fragments(text.toByteArray(), 50_000))) {
            val fed = feed(wire)
            val frame = fed.out.single() as TextWebSocketFrame
            assertEquals(text, frame.text())
            frame.release()
            assertTrue(fed.peak <= streamPast + 16_384, "held ${fed.peak} bytes")
        }
    }

    @Test
    fun `masks are undone at every alignment, a ping between fragments included`() {
        val text = "0123456789".repeat(70_000).toByteArray()
        val wire = Unpooled.buffer()
        wire.writeBytes(clientFrame(text.copyOfRange(0, 3), 0x1, false))
        wire.writeBytes(clientFrame(ByteArray(0), 0x9))
        wire.writeBytes(clientFrame(text.copyOfRange(3, 300_001), 0x0, false))
        wire.writeBytes(clientFrame(text.copyOfRange(300_001, text.size), 0x0, true))
        for (read in listOf(1, 7, 4_093)) {
            val fed = feed(wire.retainedDuplicate(), read)
            assertEquals(2, fed.out.size, "a pong-worthy ping and the message")
            assertEquals(String(text), (fed.out[1] as TextWebSocketFrame).text(), "reads of $read")
            fed.out.forEach { ReferenceCountUtil.release(it) }
        }
        wire.release()
    }

    @Test
    fun `the scanner reads the id where quartz reads it`() {
        fun head(json: String) = FrameIdScanner(64).apply { json.toByteArray().let { feed(Unpooled.wrappedBuffer(it), 0, it.size, 0, 0) } }.head()
        val hex = "a".repeat(64)
        // The top-level id, past nested ones and past a string holding one.
        assertEquals("""["EVENT",{"id":"$hex"}]""", head("""[ "EVENT" , {"tags":[["id","${"b".repeat(64)}"]],"content":"\"id\":\"${"c".repeat(64)}\"","id" : "$hex"}]"""))
        assertEquals("""["AUTH",{"id":"$hex"}]""", head("""["AUTH",{"id":"$hex"}]"""))
        // Not an id: these are handed to quartz as the message's first bytes.
        for (json in listOf(
            """["EVENT",{"id":"${"A".repeat(64)}"}]""",
            """["EVENT",{"id":"${"a".repeat(63)}"}]""",
            """["EVENT",{"${BACKSLASH}u0069d":"$hex"}]""",
            """["EVENTS",{"id":"$hex"}]""",
            """["REQ","sub",{"ids":["$hex"]}]""",
            """["EVENT",{"id":123}]""",
        )) {
            assertEquals(json.take(64), head(json), json)
        }
    }
}
