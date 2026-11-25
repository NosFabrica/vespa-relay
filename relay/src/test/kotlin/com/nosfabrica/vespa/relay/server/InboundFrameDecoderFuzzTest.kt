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
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.TooLongFrameException
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator
import io.netty.util.ReferenceCountUtil
import java.io.ByteArrayOutputStream
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * [InboundFrameDecoder] against Netty's decoder and aggregator, which it replaces: random valid and broken
 * streams, cut into random reads, must give the same messages, control frames and close codes and reasons.
 * Binary messages are dropped by ours and so left out of Netty's side. A failure names its seed.
 */
class InboundFrameDecoderFuzzTest {
    private val cap = 100_000

    private fun ours(): List<ChannelHandler> = listOf(InboundFrameDecoder(InboundLimits(cap.toLong(), streamPastBytes = null)))

    private fun netty(): List<ChannelHandler> =
        listOf(
            WebSocket13FrameDecoder(
                WebSocketDecoderConfig
                    .newBuilder()
                    .maxFramePayloadLength(cap)
                    .closeOnProtocolViolation(false)
                    .build(),
            ),
            WebSocketFrameAggregator(cap),
        )

    /** Netty's decoder with extensions allowed, its aggregator, then upstream's whole-message inflate of an RSV1 message. */
    private fun nettyDeflate(): List<ChannelHandler> =
        listOf(
            WebSocket13FrameDecoder(
                WebSocketDecoderConfig
                    .newBuilder()
                    .maxFramePayloadLength(cap)
                    .allowExtensions(true)
                    .closeOnProtocolViolation(false)
                    .build(),
            ),
            WebSocketFrameAggregator(cap),
        )

    private fun oursDeflate(): List<ChannelHandler> = listOf(InboundFrameDecoder(InboundLimits(cap.toLong(), streamPastBytes = null), InflaterPool()))

    /** What a pipeline made of [wire] fed in [reads]: one line per message, control frame or error. */
    private fun run(
        handlers: List<ChannelHandler>,
        wire: ByteArray,
        reads: List<Int>,
        inflateRsv1: Boolean = false,
    ): List<String> {
        val channel = EmbeddedChannel(*handlers.toTypedArray())
        val seen = mutableListOf<String>()
        var at = 0
        for (n in reads) {
            if (at >= wire.size) break
            val take = minOf(n, wire.size - at)
            try {
                channel.writeInbound(Unpooled.wrappedBuffer(wire, at, take))
            } catch (e: Exception) {
                // What the read produced before the error came first.
                drain(channel, seen, inflateRsv1)
                seen += error(e)
                break
            }
            at += take
            drain(channel, seen, inflateRsv1)
            if (seen.lastOrNull()?.startsWith("error") == true) break
        }
        // Netty's aggregator throws on a close mid-message: teardown, not a reading.
        runCatching { channel.finishAndReleaseAll() }
        return seen
    }

    private val inflater = MessageInflater()

    private fun drain(
        channel: EmbeddedChannel,
        seen: MutableList<String>,
        inflateRsv1: Boolean = false,
    ) {
        while (true) {
            val m = channel.readInbound<Any>() ?: return
            // A refusal ends the connection: what the same read decoded after it is never handled.
            if (seen.lastOrNull()?.startsWith("error") == true) {
                ReferenceCountUtil.release(m)
                continue
            }
            when (m) {
                is TextWebSocketFrame -> seen += if (inflateRsv1 && m.rsv() and 0x4 != 0) inflater.inflateText(m.content(), cap)?.let { "text $it" } ?: "error 1009" else "text ${m.text()}"
                is PingWebSocketFrame -> seen += "ping ${hex(m.content())}"
                is PongWebSocketFrame -> seen += "pong ${hex(m.content())}"
                is CloseWebSocketFrame -> seen += "close ${hex(m.content())}"
                is BinaryWebSocketFrame -> Unit
                else -> seen += "other ${m::class.simpleName}"
            }
            ReferenceCountUtil.release(m)
        }
    }

    private fun hex(b: ByteBuf) = ByteBufUtil.hexDump(b)

    /** A message past the cap is one refusal whichever handler saw it: Netty's aggregator words it differently. */
    private fun error(e: Throwable): String =
        when {
            e is TooLongFrameException -> "error 1009"
            e is CorruptedWebSocketFrameException && e.closeStatus().code() == 1009 -> "error 1009"
            e is CorruptedWebSocketFrameException -> "error ${e.closeStatus().code()} ${e.message}"
            else -> "error ${e::class.simpleName} ${e.message}"
        }

    private fun frame(
        r: Random,
        out: ByteArrayOutputStream,
        opcode: Int,
        fin: Boolean,
        payload: ByteArray,
    ) {
        out.write((if (fin) 0x80 else 0) or opcode)
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
                for (shift in 56 downTo 0 step 8) out.write(((payload.size.toLong() shr shift) and 0xFF).toInt())
            }
        }
        val mask = r.nextBytes(4)
        out.write(mask)
        out.write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it % 4].toInt()).toByte() })
    }

    private fun text(
        r: Random,
        size: Int,
    ) = String(CharArray(size) { if (r.nextInt(20) == 0) "€😀é\"\\"[r.nextInt(5)] else 'a' + r.nextInt(26) }).toByteArray().copyOf(size)

    private fun size(r: Random) =
        when (r.nextInt(10)) {
            0 -> 0
            in 1..5 -> r.nextInt(126)
            in 6..8 -> r.nextInt(126, 70_000)
            else -> r.nextInt(70_000, cap + 20_000)
        }

    private fun control(
        r: Random,
        out: ByteArrayOutputStream,
    ) = frame(r, out, if (r.nextBoolean()) 0x9 else 0xA, true, r.nextBytes(r.nextInt(126)))

    private fun valid(r: Random): ByteArray {
        val out = ByteArrayOutputStream()
        repeat(r.nextInt(1, 8)) {
            val opcode = if (r.nextInt(5) == 0) 0x2 else 0x1
            if (r.nextInt(4) == 0) {
                control(r, out)
            } else if (r.nextBoolean()) {
                frame(r, out, opcode, true, text(r, size(r)))
            } else {
                val parts = r.nextInt(2, 6)
                for (i in 0 until parts) {
                    frame(r, out, if (i == 0) opcode else 0x0, i == parts - 1, text(r, size(r) / parts))
                    if (r.nextInt(4) == 0 && i < parts - 1) control(r, out)
                }
            }
        }
        if (r.nextInt(3) == 0) {
            val code = listOf(1000, 1001, 1008, 3000, 4999)[r.nextInt(5)]
            frame(r, out, 0x8, true, byteArrayOf((code shr 8).toByte(), code.toByte()) + text(r, r.nextInt(60)))
        }
        return out.toByteArray()
    }

    private fun broken(r: Random): ByteArray {
        val bytes = valid(r)
        if (bytes.isEmpty()) return bytes
        return when (r.nextInt(4)) {
            0 -> {
                bytes.copyOf(r.nextInt(bytes.size))
            }

            1 -> {
                bytes.also {
                    repeat(r.nextInt(1, 4)) { _ ->
                        val i = r.nextInt(it.size.coerceAtMost(64))
                        it[i] = (it[i].toInt() xor (1 shl r.nextInt(8))).toByte()
                    }
                }
            }

            2 -> {
                bytes.also {
                    repeat(r.nextInt(1, 4)) { _ ->
                        val i = r.nextInt(it.size)
                        it[i] = r.nextInt(256).toByte()
                    }
                }
            }

            else -> {
                val at = r.nextInt(bytes.size)
                bytes.copyOf(at) + r.nextBytes(r.nextInt(1, 20)) + bytes.copyOfRange(at, bytes.size)
            }
        }
    }

    private fun reads(
        r: Random,
        total: Int,
    ): List<Int> {
        val reads = mutableListOf<Int>()
        var left = total + 1
        while (left > 0) {
            val n =
                when (r.nextInt(4)) {
                    0 -> 1
                    1 -> r.nextInt(1, 16)
                    2 -> r.nextInt(1, 4_096)
                    else -> r.nextInt(1, 65_536)
                }
            reads += n
            left -= n
        }
        return reads
    }

    @Test
    fun `the decoder reads every stream as Netty's decoder and aggregator do`() {
        for (seed in 1..600) {
            val r = Random(seed)
            val wire = if (seed % 2 == 0) valid(r) else broken(r)
            val reads = reads(r, wire.size)
            assertEquals(run(netty(), wire, reads), run(ours(), wire, reads), "seed $seed (${if (seed % 2 == 0) "valid" else "broken"}, ${wire.size} bytes)")
        }
    }

    @Test
    fun `a length is judged before its mask arrives, as Netty judges it`() {
        val headers =
            listOf(
                // A 16-bit length under 126, a 64-bit one under 65,536, a negative one, one past the cap.
                byteArrayOf(0x81.toByte(), (0x80 or 126).toByte(), 0, 5),
                byteArrayOf(0x81.toByte(), (0x80 or 127).toByte(), 0, 0, 0, 0, 0, 0, 1, 0),
                byteArrayOf(0x81.toByte(), (0x80 or 127).toByte(), 0x80.toByte(), 0, 0, 0, 0, 0, 0, 0),
                byteArrayOf(0x81.toByte(), (0x80 or 127).toByte(), 0, 0, 0, 0, 0, 0x10, 0, 0),
            )
        for (header in headers) {
            val reads = listOf(header.size)
            val netty = run(netty(), header, reads)
            assertEquals(1, netty.size, "Netty refuses ${ByteBufUtil.hexDump(header)} without its mask")
            assertEquals(netty, run(ours(), header, reads), ByteBufUtil.hexDump(header))
        }
    }

    @Test
    fun `a stream cut in two at every byte reads as Netty reads it`() {
        val r = Random(7)
        val out = ByteArrayOutputStream()
        // Fragments of 13, 1 and 40 bytes around a ping, then a whole 300-byte frame: words and tails at every phase.
        frame(r, out, 0x1, false, text(r, 13))
        frame(r, out, 0x9, true, r.nextBytes(5))
        frame(r, out, 0x0, false, text(r, 1))
        frame(r, out, 0x0, true, text(r, 40))
        frame(r, out, 0x1, true, text(r, 300))
        val wire = out.toByteArray()
        for (cut in 0..wire.size) {
            val reads = listOf(cut, wire.size - cut).filter { it > 0 }
            assertEquals(run(netty(), wire, reads), run(ours(), wire, reads), "cut at $cut of ${wire.size}")
        }
    }

    /** A stream of plain and compressed messages, some fragmented with pings between, some inflating past the cap. */
    private fun compressedStream(r: Random): ByteArray {
        val out = ByteArrayOutputStream()
        repeat(r.nextInt(1, 6)) {
            val plain =
                if (r.nextInt(6) == 0) {
                    ByteArray(r.nextInt(cap - 20_000, cap + 60_000)) { '0'.code.toByte() }
                } else {
                    text(r, size(r).coerceAtMost(cap - 1))
                }
            val compressed = r.nextInt(3) > 0
            val payload = if (compressed) deflateMessage(plain) else plain
            if (payload.size > cap) return@repeat
            val parts = if (r.nextBoolean()) 1 else r.nextInt(2, 5)
            val step = (payload.size + parts - 1) / parts.coerceAtLeast(1)
            var at = 0
            var first = true
            do {
                val end = if (step == 0) payload.size else minOf(at + step, payload.size)
                val frameStart = out.size()
                frame(r, out, if (first) 0x1 else 0x0, end == payload.size, payload.copyOfRange(at, end))
                if (first && compressed) {
                    val bytes = out.toByteArray()
                    bytes[frameStart] = (bytes[frameStart].toInt() or 0x40).toByte()
                    out.reset()
                    out.write(bytes)
                }
                if (end < payload.size && r.nextInt(3) == 0) control(r, out)
                first = false
                at = end
            } while (at < payload.size)
        }
        return out.toByteArray()
    }

    @Test
    fun `compressed messages read as Netty's decoder, aggregator and whole-message inflate read them`() {
        for (seed in 1..300) {
            val r = Random(seed)
            val wire = compressedStream(r)
            val reads = reads(r, wire.size)
            val netty = run(nettyDeflate(), wire, reads, inflateRsv1 = true)
            val ours = run(oursDeflate(), wire, reads)
            val at = (0 until maxOf(netty.size, ours.size)).firstOrNull { netty.getOrNull(it) != ours.getOrNull(it) } ?: continue
            fail("seed $seed (${wire.size} bytes), item $at: Netty ${netty.getOrNull(at)?.take(160)} / ours ${ours.getOrNull(at)?.take(160)}")
        }
    }
}
