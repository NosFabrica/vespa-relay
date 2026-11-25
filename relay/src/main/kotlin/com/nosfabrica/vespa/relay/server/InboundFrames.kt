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
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus
import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.nio.ByteOrder
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.util.zip.DataFormatException
import java.util.zip.Inflater

/**
 * What the native transport reads of one message: a text message up to [streamPastBytes] is held and
 * handed on whole; past it, it streams through a [FrameIdScanner] and only its head is kept; past
 * [maxMessageBytes] the connection closes. [streamPastBytes] is null when the engine has no length limit.
 */
internal class InboundLimits(
    val maxMessageBytes: Long,
    val streamPastBytes: Long?,
    val headBytes: Int = HEAD_BYTES,
)

/** Kept of a streamed message: room for a frame's label and sub id, or an event's id read late. */
internal const val HEAD_BYTES = 65_536

/**
 * A text message too long for the engine, read without being held: [head] is what quartz needs to
 * refuse it by id, `["EVENT",{"id":…}]` when the scan found the id, else the message's first bytes.
 */
internal class OversizedMessage(
    val head: String,
)

/**
 * RFC 6455 frames in, whole messages out, replacing Netty's decoder and aggregator: each check and
 * close code is theirs, but a frame's payload is copied as it arrives into a buffer of its exact size,
 * and a message past [InboundLimits.streamPastBytes] is scanned and dropped instead of held.
 * One per connection, on its event loop.
 */
internal class InboundFrameDecoder(
    private val limits: InboundLimits,
    // Set when permessage-deflate was agreed: RSV1 marks a compressed message on its first frame (RFC 7692 6.1).
    private val inflaters: InflaterPool? = null,
) : ChannelInboundHandlerAdapter() {
    private val compressed = inflaters != null

    /** Bytes inflated for the compressed message being read, or the last one. */
    var inflatedBytes = 0L
        private set

    /** Borrowed from [inflaters] while a compressed message is open, given back at its end. */
    private var inflater: Inflater? = null

    private val header = ByteArray(MAX_HEADER)
    private var headerLength = 0

    /** The length of the frame whose header is being read, once read; -1 before. */
    private var frameLength = -1L
    private var state = State.HEADER

    // The frame being read.
    private var fin = false
    private var opcode = 0
    private var mask = 0
    private var payloadRead = 0L
    private var payloadLength = 0L
    private var sink = Sink.SKIP
    private var control: ByteBuf? = null

    /** Where the frame's bytes begin in the held message, and up to where they are unmasked: whole words as they come. */
    private var frameStart = 0
    private var unmaskedTo = 0

    // The data message the frame belongs to.
    private var fragmented = false
    private var messageIsText = false
    private var messageCompressed = false
    private var frameRsv = 0
    private var messageBytes = 0L
    private var message: ByteBuf? = null
    private var scanner: FrameIdScanner? = null

    /** Since when the bytes of an unfinished message have been held, less any time reads were paused, or [IDLE]. */
    var holdingSince = IDLE
        private set

    /** When bytes last arrived, as `System.nanoTime()`. */
    var lastRead = System.nanoTime()
        private set

    /** Takes the time the relay's reads were paused, from [pausedAt] to now, off the held message's clock. */
    fun resumedAfter(pausedAt: Long) {
        if (holdingSince != IDLE) holdingSince += System.nanoTime() - maxOf(pausedAt, holdingSince)
    }

    /** Drops everything read from now on: the connection is closing. */
    fun discard() {
        state = State.DISCARD
        release()
    }

    override fun channelRead(
        ctx: ChannelHandlerContext,
        msg: Any,
    ) {
        if (msg !is ByteBuf) return ctx.fireChannelRead(msg).let { }
        lastRead = System.nanoTime()
        try {
            while (msg.isReadable) {
                when (state) {
                    State.HEADER -> readHeader(ctx, msg)
                    State.PAYLOAD -> readPayload(ctx, msg)
                    State.DISCARD -> msg.skipBytes(msg.readableBytes())
                }
            }
        } finally {
            msg.release()
        }
    }

    private fun readHeader(
        ctx: ChannelHandlerContext,
        buf: ByteBuf,
    ) {
        val before = headerLength
        val needed =
            when {
                headerLength < 2 -> 2
                frameLength < 0 -> 2 + lengthBytes()
                else -> 2 + lengthBytes() + 4
            }
        val take = minOf(needed - headerLength, buf.readableBytes())
        buf.readBytes(header, headerLength, take)
        headerLength += take
        if (before < 2 && headerLength == 2 && !checkFirstBytes(ctx)) return
        if (headerLength < 2) return
        // The length is judged as soon as it is read, before the mask, as in Netty's decoder.
        if (frameLength < 0) {
            if (headerLength < 2 + lengthBytes()) return
            frameLength = readLength(ctx) ?: return
        }
        if (headerLength < 2 + lengthBytes() + 4) return
        val m = 2 + lengthBytes()
        mask = (header[m].toInt() and 0xFF shl 24) or (header[m + 1].toInt() and 0xFF shl 16) or (header[m + 2].toInt() and 0xFF shl 8) or (header[m + 3].toInt() and 0xFF)
        val length = frameLength
        headerLength = 0
        frameLength = -1
        beginFrame(ctx, length)
    }

    /** The frame's payload length, or null after refusing it: not minimal, negative, or a message past the cap. */
    private fun readLength(ctx: ChannelHandlerContext): Long? {
        val length =
            when (val short = header[1].toInt() and 0x7F) {
                126 -> {
                    ((header[2].toInt() and 0xFF) shl 8 or (header[3].toInt() and 0xFF)).toLong().also {
                        if (it < 126) return violation(ctx, "invalid data frame length (not using minimal length encoding)").let { null }
                    }
                }

                127 -> {
                    var l = 0L
                    for (i in 2 until 10) l = (l shl 8) or (header[i].toLong() and 0xFF)
                    if (l < 0) return violation(ctx, "invalid data frame length (negative length)").let { null }
                    if (l < 65_536) return violation(ctx, "invalid data frame length (not using minimal length encoding)").let { null }
                    l
                }

                else -> {
                    short.toLong()
                }
            }
        val total = if (opcode == OPCODE_CONT) messageBytes + length else length
        if (opcode <= 7 && total > limits.maxMessageBytes) {
            return violation(ctx, "Max frame length of ${limits.maxMessageBytes} has been exceeded.", WebSocketCloseStatus.MESSAGE_TOO_BIG).let { null }
        }
        return length
    }

    /** The checks Netty's decoder makes once it has a frame's first two bytes, in its order and words. */
    private fun checkFirstBytes(ctx: ChannelHandlerContext): Boolean {
        val b0 = header[0].toInt()
        val b1 = header[1].toInt()
        fin = b0 and 0x80 != 0
        opcode = b0 and 0x0F
        val rsv = (b0 and 0x70) shr 4
        frameRsv = rsv
        val len7 = b1 and 0x7F
        val problem =
            when {
                rsv != 0 && !compressed -> "RSV != 0 and no extension negotiated, RSV:$rsv"
                rsv != 0 && (rsv != RSV1 || opcode !in 1..2) -> "RSV1 only on a message's first data frame, RSV:$rsv"
                b1 and 0x80 == 0 -> "received a frame that is not masked as expected"
                opcode > 7 && !fin -> "fragmented control frame"
                opcode > 7 && len7 > 125 -> "control frame with payload length > 125 octets"
                opcode > 7 && opcode != OPCODE_CLOSE && opcode != OPCODE_PING && opcode != OPCODE_PONG -> "control frame using reserved opcode $opcode"
                opcode == OPCODE_CLOSE && len7 == 1 -> "received close control frame with payload len 1"
                opcode in 3..7 -> "data frame using reserved opcode $opcode"
                opcode == OPCODE_CONT && !fragmented -> "received continuation data frame outside fragmented message"
                opcode in 1..2 && fragmented -> "received non-continuation data frame while inside fragmented message"
                else -> null
            }
        if (problem != null) violation(ctx, problem)
        return problem == null
    }

    private fun lengthBytes(): Int =
        when (header[1].toInt() and 0x7F) {
            126 -> 2
            127 -> 8
            else -> 0
        }

    private fun beginFrame(
        ctx: ChannelHandlerContext,
        length: Long,
    ) {
        payloadLength = length
        payloadRead = 0
        if (opcode > 7) {
            control = ctx.alloc().buffer(length.toInt())
            sink = Sink.CONTROL
        } else {
            if (opcode != OPCODE_CONT) startMessage(opcode == OPCODE_TEXT, frameRsv == RSV1)
            val total = messageBytes + length
            messageBytes = total
            fragmented = !fin
            sink =
                when {
                    !messageIsText -> Sink.SKIP
                    messageCompressed -> Sink.INFLATE
                    scanner != null -> Sink.SCAN
                    limits.streamPastBytes != null && total > limits.streamPastBytes -> startScanning()
                    else -> hold(ctx, length)
                }
            message?.let {
                frameStart = it.writerIndex()
                unmaskedTo = frameStart
            }
        }
        state = State.PAYLOAD
        if (length == 0L) endFrame(ctx)
    }

    /** A binary message is read and dropped, as the relay has no use for one. */
    private fun startMessage(
        text: Boolean,
        deflated: Boolean,
    ) {
        messageBytes = 0
        messageIsText = text
        messageCompressed = deflated && text
        if (messageCompressed) {
            inflatedBytes = 0
            inflater = inflaters!!.borrow()
        }
        if (text) holdingSince = System.nanoTime()
    }

    private fun hold(
        ctx: ChannelHandlerContext,
        length: Long,
    ): Sink {
        val held = message
        // A whole message's buffer is its exact size; a fragmented one grows, never past streamPastBytes. On the heap,
        // so its string is decoded from the array itself, without the copy a direct buffer needs first.
        if (held == null) {
            message = ctx.alloc().heapBuffer(length.toInt(), heldAtMost())
        } else {
            held.ensureWritable(length.toInt())
        }
        return Sink.HOLD
    }

    /** From here the message is only scanned: what was held goes through the scanner and is let go. */
    private fun startScanning(): Sink {
        val s = FrameIdScanner(limits.headBytes)
        message?.let { held ->
            s.feed(held, held.readerIndex(), held.readableBytes(), 0, 0)
            held.release()
        }
        message = null
        scanner = s
        holdingSince = IDLE
        return Sink.SCAN
    }

    private fun readPayload(
        ctx: ChannelHandlerContext,
        buf: ByteBuf,
    ) {
        val n = minOf(payloadLength - payloadRead, buf.readableBytes().toLong()).toInt()
        val offset = (payloadRead and 3).toInt()
        when (sink) {
            Sink.CONTROL -> {
                control!!.writeBytes(buf, n)
            }

            Sink.HOLD -> {
                holdBytes(buf, n)
            }

            Sink.SCAN -> {
                scanner!!.feed(buf, buf.readerIndex(), n, mask, offset)
                buf.skipBytes(n)
            }

            Sink.INFLATE -> {
                inflateFrom(ctx, buf, n, offset)
                buf.skipBytes(n)
                if (state == State.DISCARD) return
            }

            Sink.SKIP -> {
                buf.skipBytes(n)
            }
        }
        payloadRead += n
        if (payloadRead == payloadLength) endFrame(ctx)
    }

    /** Copied in still masked; every whole 8-byte word from the frame's start is unmasked now, the rest at its end. */
    private fun holdBytes(
        from: ByteBuf,
        n: Int,
    ) {
        val to = message!!
        val at = to.writerIndex()
        if (!to.hasArray()) {
            to.writeBytes(from, n)
            unmask(to, at, n, mask, (at - frameStart) and 3)
            unmaskedTo = at + n
            return
        }
        from.readBytes(to.array(), to.arrayOffset() + at, n)
        to.writerIndex(at + n)
        unmaskedTo = unmaskWords(to.array(), to.arrayOffset(), unmaskedTo, at + n, mask)
    }

    /** The bytes past the frame's last whole word, once it has all arrived. */
    private fun finishUnmask() {
        val to = message ?: return
        val end = to.writerIndex()
        if (unmaskedTo < end) unmask(to, unmaskedTo, end - unmaskedTo, mask, (unmaskedTo - frameStart) and 3)
        unmaskedTo = end
    }

    private fun heldAtMost(): Int = (limits.streamPastBytes ?: limits.maxMessageBytes).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

    /** [n] compressed bytes of [buf], unmasked a slice at a time into the loop's scratch, through the message's inflater. */
    private fun inflateFrom(
        ctx: ChannelHandlerContext,
        buf: ByteBuf,
        n: Int,
        offset: Int,
    ) {
        val input = inflaters!!.input
        var k = 0
        while (k < n && state != State.DISCARD) {
            val c = minOf(input.size, n - k)
            buf.getBytes(buf.readerIndex() + k, input, 0, c)
            unmaskArray(input, 0, c, mask, (offset + k) and 3)
            inflate(ctx, input, 0, c)
            k += c
        }
    }

    /**
     * Inflates [length] bytes of [input] to the end, each slice of output held, scanned or refused as it comes: the
     * message is judged by its inflated size, and inflating stops once that passes the cap.
     */
    private fun inflate(
        ctx: ChannelHandlerContext,
        input: ByteArray,
        from: Int,
        length: Int,
    ) {
        val inflater = inflater ?: return
        val output = inflaters!!.output
        inflater.setInput(input, from, length)
        try {
            while (true) {
                val n = inflater.inflate(output)
                if (n > 0 && !takeInflated(ctx, output, n)) return
                // Output that did not fill the scratch and an inflater wanting input: nothing more to inflate yet.
                if (n < output.size && inflater.needsInput()) return
                if (n == 0) {
                    if (inflater.needsDictionary()) return violation(ctx, "malformed compressed frame", WebSocketCloseStatus.INVALID_PAYLOAD_DATA)
                    if (inflater.needsInput() || inflater.finished()) return
                }
            }
        } catch (_: DataFormatException) {
            violation(ctx, "malformed compressed frame", WebSocketCloseStatus.INVALID_PAYLOAD_DATA)
        }
    }

    /** False once the message inflates past the cap, which refuses it. */
    private fun takeInflated(
        ctx: ChannelHandlerContext,
        bytes: ByteArray,
        n: Int,
    ): Boolean {
        inflatedBytes += n
        if (inflatedBytes > limits.maxMessageBytes) {
            violation(ctx, "message inflates past ${limits.maxMessageBytes} bytes", WebSocketCloseStatus.MESSAGE_TOO_BIG)
            return false
        }
        val past = limits.streamPastBytes
        if (scanner == null && past != null && inflatedBytes > past) startScanning()
        val scanning = scanner
        if (scanning != null) {
            scanning.feed(bytes, 0, n)
        } else {
            val held = message ?: ctx.alloc().heapBuffer(n, heldAtMost()).also { message = it }
            held.writeBytes(bytes, 0, n)
        }
        return true
    }

    private fun giveBackInflater() {
        inflater?.let { inflaters?.giveBack(it) }
        inflater = null
    }

    private fun endFrame(ctx: ChannelHandlerContext) {
        state = State.HEADER
        if (sink == Sink.CONTROL) return endControlFrame(ctx)
        if (sink == Sink.HOLD) finishUnmask()
        if (!fin) return
        if (messageCompressed) {
            // The four bytes a sender leaves off each message's sync flush (RFC 7692 7.2.2).
            inflate(ctx, SYNC_FLUSH_TAIL, 0, SYNC_FLUSH_TAIL.size)
            if (state == State.DISCARD) return
            giveBackInflater()
        }
        val held = message
        val scanned = scanner
        message = null
        scanner = null
        holdingSince = IDLE
        messageBytes = 0
        if (held != null) ctx.fireChannelRead(TextWebSocketFrame(held))
        if (scanned != null) ctx.fireChannelRead(OversizedMessage(scanned.head()))
        // A compressed message that inflated to nothing is still a message, as a plain empty one is.
        if (held == null && scanned == null && messageCompressed) ctx.fireChannelRead(TextWebSocketFrame(Unpooled.EMPTY_BUFFER))
    }

    private fun endControlFrame(ctx: ChannelHandlerContext) {
        val body = control!!
        control = null
        unmask(body, body.readerIndex(), body.readableBytes(), mask, 0)
        when (opcode) {
            OPCODE_PING -> {
                ctx.fireChannelRead(PingWebSocketFrame(body))
            }

            OPCODE_PONG -> {
                ctx.fireChannelRead(PongWebSocketFrame(body))
            }

            else -> {
                closeBodyProblem(body)?.let { (status, reason) ->
                    body.release()
                    return violation(ctx, reason, status)
                }
                // Nothing after a close is read, as in Netty's decoder.
                discard()
                ctx.fireChannelRead(CloseWebSocketFrame(true, 0, body))
            }
        }
    }

    /** Netty's checks on a close frame's body: a valid status code, and a reason in UTF-8. */
    private fun closeBodyProblem(body: ByteBuf): Pair<WebSocketCloseStatus, String>? {
        if (!body.isReadable) return null
        val code = body.getShort(body.readerIndex()).toInt()
        if (!WebSocketCloseStatus.isValidStatusCode(code)) return WebSocketCloseStatus.PROTOCOL_ERROR to "Invalid close frame getStatus code: $code"
        if (body.readableBytes() > 2) {
            // Not the end of input: like Netty's check, a reason cut inside a character passes, a wrong byte does not.
            val reason = body.nioBuffer(body.readerIndex() + 2, body.readableBytes() - 2)
            val result =
                Charsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(reason, CharBuffer.allocate(reason.remaining()), false)
            if (result.isError) return WebSocketCloseStatus.INVALID_PAYLOAD_DATA to "bytes are not UTF-8"
        }
        return null
    }

    private fun violation(
        ctx: ChannelHandlerContext,
        reason: String,
        status: WebSocketCloseStatus = WebSocketCloseStatus.PROTOCOL_ERROR,
    ) {
        discard()
        ctx.fireExceptionCaught(CorruptedWebSocketFrameException(status, reason))
    }

    private fun release() {
        giveBackInflater()
        message?.release()
        message = null
        control?.release()
        control = null
        scanner = null
        holdingSince = IDLE
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) = release()

    override fun channelInactive(ctx: ChannelHandlerContext) {
        release()
        ctx.fireChannelInactive()
    }

    private enum class State { HEADER, PAYLOAD, DISCARD }

    /** Where a frame's payload goes: a control frame's body, the held message, the scanner, the inflater, or nowhere. */
    private enum class Sink { CONTROL, HOLD, SCAN, INFLATE, SKIP }

    companion object {
        const val IDLE = Long.MIN_VALUE
        private const val RSV1 = 0x4

        /** The four bytes each message's sync flush ends with, which its sender leaves off. */
        private val SYNC_FLUSH_TAIL = byteArrayOf(0, 0, -1, -1)
        private const val MAX_HEADER = 14
        private const val OPCODE_CONT = 0x0
        private const val OPCODE_TEXT = 0x1
        private const val OPCODE_CLOSE = 0x8
        private const val OPCODE_PING = 0x9
        private const val OPCODE_PONG = 0xA
    }
}

/** Big-endian longs over a byte array, at any index: the JDK's own word access, whatever Netty's is. */
private val LONGS: VarHandle = MethodHandles.byteArrayViewVarHandle(LongArray::class.java, ByteOrder.BIG_ENDIAN)

/**
 * XORs whole 8-byte words of [array] (a buffer's backing array, from [base]) with [mask], from buffer index [from], a
 * multiple of 8 past the frame's first byte, while a whole word lies before [to]; returns where it stopped.
 */
internal fun unmaskWords(
    array: ByteArray,
    base: Int,
    from: Int,
    to: Int,
    mask: Int,
): Int {
    if (to - from < 8) return from
    if (mask == 0) return from + (to - from) / 8 * 8
    val wide = (mask.toLong() shl 32) or (mask.toLong() and 0xFFFFFFFFL)
    var i = base + from
    val end = base + to
    while (i + 8 <= end) {
        LONGS.set(array, i, LONGS.get(array, i) as Long xor wide)
        i += 8
    }
    return i - base
}

/** XORs [length] bytes of [array] from [from] with [mask], the first of them being byte [phase] of the payload mod 4. */
internal fun unmaskArray(
    array: ByteArray,
    from: Int,
    length: Int,
    mask: Int,
    phase: Int,
) {
    if (mask == 0) return
    val m = Integer.rotateLeft(mask, 8 * phase)
    val words = unmaskWords(array, 0, from, from + length, m)
    for (i in words until from + length) array[i] = (array[i].toInt() xor (m ushr (24 - 8 * ((i - from) and 3)))).toByte()
}

/** XORs [length] bytes of [buf] from [start] with [mask], the first of them being byte [offset] of the frame's payload mod 4. */
internal fun unmask(
    buf: ByteBuf,
    start: Int,
    length: Int,
    mask: Int,
    offset: Int,
) {
    if (mask == 0) return
    val m = Integer.rotateLeft(mask, 8 * offset)
    val wide = (m.toLong() shl 32) or (m.toLong() and 0xFFFFFFFFL)
    var i = start
    val end = start + length
    while (i + 8 <= end) {
        buf.setLong(i, buf.getLong(i) xor wide)
        i += 8
    }
    while (i < end) {
        buf.setByte(i, buf.getByte(i).toInt() xor (m ushr (24 - 8 * ((i - start) and 3))))
        i++
    }
}

/**
 * Reads, a byte at a time, the event id quartz's `eventIdOfFrame` would read off the whole message:
 * the top-level `"id"` of the event in an `["EVENT",{…}]` or `["AUTH",{…}]`, wherever it comes, and
 * keeps the first [headBytes] bytes for the rest of quartz's refusal (a sub id, or a NOTICE).
 * Remove when quartz refuses an oversized frame from a stream (RelaySession takes only a String).
 */
internal class FrameIdScanner(
    headBytes: Int,
) {
    private val head = ByteArray(headBytes)
    private var headLength = 0
    private var state = PRE_ARRAY
    private var label = ""
    private var labelAt = 0
    private var depth = 0
    private var expectKey = false
    private var escaped = false
    private var keyLength = 0
    private var keyIsId = false
    private val id = StringBuilder(64)
    private var found: String? = null

    private val wantsBytes get() = state != DONE || headLength < head.size

    /** Bytes copied out of the buffer and unmasked in bulk, then read from an array. */
    private val scratch = ByteArray(SCRATCH_BYTES)
    private val maskBytes = IntArray(4)

    /** [length] bytes of [buf] from [index], unmasked with [mask] from payload byte [offset] mod 4. */
    fun feed(
        buf: ByteBuf,
        index: Int,
        length: Int,
        mask: Int,
        offset: Int,
    ) {
        for (j in 0..3) maskBytes[j] = (mask ushr (24 - 8 * j)) and 0xFF
        var k = 0
        while (k < length && wantsBytes) {
            val n = minOf(scratch.size, length - k)
            buf.getBytes(index + k, scratch, 0, n)
            if (mask != 0) {
                val at = offset + k
                for (i in 0 until n) scratch[i] = (scratch[i].toInt() xor maskBytes[(at + i) and 3]).toByte()
            }
            consume(n)
            k += n
        }
    }

    /** [length] bytes of [bytes] from [from], already plain. */
    fun feed(
        bytes: ByteArray,
        from: Int,
        length: Int,
    ) {
        var k = 0
        while (k < length && wantsBytes) {
            val n = minOf(scratch.size, length - k)
            System.arraycopy(bytes, from + k, scratch, 0, n)
            consume(n)
            k += n
        }
    }

    private fun consume(n: Int) {
        if (headLength < head.size) {
            val c = minOf(n, head.size - headLength)
            System.arraycopy(scratch, 0, head, headLength, c)
            headLength += c
        }
        var i = 0
        while (i < n && state != DONE) {
            // Inside a string only a quote or a backslash means anything: skip to the next one.
            if (state == STRING && !escaped) {
                while (i < n && scratch[i] != QUOTE && scratch[i] != BACKSLASH) i++
                if (i == n) return
            }
            step(scratch[i].toInt() and 0xFF)
            i++
        }
    }

    /** What quartz is handed: the id found, in the frame's own label, else the message's first bytes. */
    fun head(): String = found?.let { """["$label",{"id":"$it"}]""" } ?: String(head, 0, headLength, Charsets.UTF_8)

    private fun step(b: Int) {
        val space = b == ' '.code || b == '\n'.code || b == '\r'.code || b == '\t'.code
        when (state) {
            PRE_ARRAY -> {
                if (!space) {
                    if (b == '['.code) {
                        state = PRE_LABEL
                    } else {
                        done(null)
                    }
                }
            }

            PRE_LABEL -> {
                if (!space) {
                    if (b == '"'.code) {
                        state = LABEL
                    } else {
                        done(null)
                    }
                }
            }

            LABEL -> {
                stepLabel(b)
            }

            POST_LABEL -> {
                if (!space) {
                    if (b == ','.code) {
                        state = PRE_OBJECT
                    } else {
                        done(null)
                    }
                }
            }

            PRE_OBJECT -> {
                if (!space) {
                    if (b == '{'.code) {
                        state = MEMBERS
                        depth = 1
                        expectKey = true
                    } else {
                        done(null)
                    }
                }
            }

            MEMBERS -> {
                stepMember(b)
            }

            STRING -> {
                when {
                    escaped -> {
                        escaped = false
                    }

                    b == '\\'.code -> {
                        escaped = true
                    }

                    b == '"'.code -> {
                        expectKey = false
                        state = MEMBERS
                    }
                }
            }

            KEY -> {
                stepKey(b)
            }

            ID_COLON -> {
                if (!space) {
                    if (b == ':'.code) {
                        state = ID_QUOTE
                    } else {
                        done(null)
                    }
                }
            }

            ID_QUOTE -> {
                if (!space) {
                    if (b == '"'.code) {
                        state = ID_VALUE
                    } else {
                        done(null)
                    }
                }
            }

            ID_VALUE -> {
                stepId(b)
            }
        }
    }

    /** `"EVENT"` or `"AUTH"`, matched exactly, closing quote included. */
    private fun stepLabel(b: Int) {
        if (labelAt == 0) {
            label =
                when (b) {
                    'E'.code -> "EVENT"
                    'A'.code -> "AUTH"
                    else -> return done(null)
                }
        }
        val expected = if (labelAt < label.length) label[labelAt].code else '"'.code
        if (b != expected) return done(null)
        labelAt++
        if (labelAt > label.length) state = POST_LABEL
    }

    private fun stepMember(b: Int) {
        when (b) {
            '"'.code -> {
                if (depth == 1 && expectKey) {
                    state = KEY
                    keyLength = 0
                    keyIsId = true
                } else {
                    state = STRING
                }
                escaped = false
            }

            '{'.code, '['.code -> {
                depth++
            }

            '}'.code, ']'.code -> {
                depth--
                if (depth == 0) done(null)
            }

            ','.code -> {
                if (depth == 1) expectKey = true
            }

            ':'.code -> {
                if (depth == 1) expectKey = false
            }
        }
    }

    /** A key at the event's top level: only the raw two characters `id` name the id, as in quartz. */
    private fun stepKey(b: Int) {
        when {
            escaped -> {
                escaped = false
                keyLength++
            }

            b == '\\'.code -> {
                escaped = true
                keyIsId = false
                keyLength++
            }

            b == '"'.code -> {
                if (keyIsId && keyLength == 2) {
                    state = ID_COLON
                } else {
                    expectKey = false
                    state = MEMBERS
                }
            }

            else -> {
                keyIsId = keyIsId && ((keyLength == 0 && b == 'i'.code) || (keyLength == 1 && b == 'd'.code))
                keyLength++
            }
        }
    }

    /** The id's raw characters: 64 of lowercase hex, or no id at all. */
    private fun stepId(b: Int) {
        when {
            escaped -> {
                escaped = false
                id.append(b.toChar())
            }

            b == '\\'.code -> {
                escaped = true
                id.append(b.toChar())
            }

            b == '"'.code -> {
                return done(id.toString().takeIf { v -> v.length == 64 && v.all { it in '0'..'9' || it in 'a'..'f' } })
            }

            else -> {
                id.append(b.toChar())
            }
        }
        if (id.length > 64) done(null)
    }

    private fun done(value: String?) {
        found = value
        state = DONE
    }

    private companion object {
        const val PRE_ARRAY = 0
        const val PRE_LABEL = 1
        const val LABEL = 2
        const val POST_LABEL = 3
        const val PRE_OBJECT = 4
        const val MEMBERS = 5
        const val STRING = 6
        const val KEY = 7
        const val ID_COLON = 8
        const val ID_QUOTE = 9
        const val ID_VALUE = 10
        const val DONE = 11
        const val SCRATCH_BYTES = 8_192
        const val QUOTE = '"'.code.toByte()
        const val BACKSLASH = '\\'.code.toByte()
    }
}
