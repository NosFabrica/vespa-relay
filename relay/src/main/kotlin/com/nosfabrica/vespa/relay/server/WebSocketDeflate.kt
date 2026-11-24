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
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufUtil
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.DataFormatException
import java.util.zip.Deflater
import java.util.zip.Inflater

/** The fastest zlib level: answers are compressed on the event loop, between other sockets' frames. */
private const val DEFLATE_LEVEL = Deflater.BEST_SPEED

/** Text shorter than this, in UTF-8 bytes, goes out uncompressed: an EOSE or an OK saves nothing worth a deflate. */
internal const val DEFLATE_MIN_BYTES = 64

/** Sliding windows held at once across the process, each about 256 KiB of zlib state; past it answers go a frame at a time. */
internal const val MAX_DEFLATE_WINDOWS = 512

/** Keepalive ticks a socket's window survives with nothing compressed through it before its memory is given back. */
internal const val DEFLATE_IDLE_TICKS = 5

/** Bytes a stored deflate block's header puts ahead of its data (RFC 1951 §3.2.4). */
internal const val STORED_BLOCK_HEADER = 5

/** The longest live prefix, in chars, whose UTF-8 is sure to fit one stored block. */
internal const val MAX_STORED_PREFIX_CHARS = 0xFFFF / 3

/** The four bytes a sync flush ends with, which a message leaves off and its reader puts back (RFC 7692 §7.2.1). */
private val SYNC_FLUSH_TAIL = byteArrayOf(0, 0, -1, -1)

/**
 * What a client's permessage-deflate offer was agreed as (RFC 7692); [response] is the `Sec-WebSocket-Extensions`
 * answer. It always imposes client_no_context_takeover, so one inflater per event loop reads every socket on it.
 */
internal class DeflateTerms(
    val serverNoContextTakeover: Boolean,
    val response: String,
)

/**
 * The first permessage-deflate offer among [headers] (each a `Sec-WebSocket-Extensions` value) this relay can serve,
 * or null to serve the socket uncompressed. An offer with a parameter it cannot honour is declined, never half-applied.
 */
internal fun negotiateDeflate(headers: List<String>): DeflateTerms? =
    headers
        .asSequence()
        .flatMap { it.split(',') }
        .firstNotNullOfOrNull(::acceptDeflateOffer)

private fun acceptDeflateOffer(offer: String): DeflateTerms? {
    val parts = offer.split(';').map { it.trim() }
    if (!parts[0].equals("permessage-deflate", ignoreCase = true)) return null
    val params = HashMap<String, String?>()
    for (part in parts.drop(1)) {
        val name = part.substringBefore('=').trim().lowercase()
        if (name.isEmpty() || name in params) return null
        params[name] = if ('=' in part) part.substringAfter('=').trim().removeSurrounding("\"") else null
    }
    for ((name, value) in params) {
        val honoured =
            when (name) {
                "server_no_context_takeover", "client_no_context_takeover" -> value == null

                // The JDK's deflater always keeps a 32 KiB window, so a smaller one cannot be promised.
                "server_max_window_bits" -> value == "15"

                "client_max_window_bits" -> value == null || value.toIntOrNull()?.let { it in 8..15 } == true

                else -> false
            }
        if (!honoured) return null
    }
    val serverNoContextTakeover = "server_no_context_takeover" in params
    val response =
        buildString {
            append("permessage-deflate; client_no_context_takeover")
            if (serverNoContextTakeover) append("; server_no_context_takeover")
            if ("server_max_window_bits" in params) append("; server_max_window_bits=15")
        }
    return DeflateTerms(serverNoContextTakeover, response)
}

/** Raw deflate (RFC 1951), sync-flushed once per message; without a kept context each message starts on an empty window. */
internal class MessageDeflater {
    private val deflater = Deflater(DEFLATE_LEVEL, true)
    private var dirty = false

    /** Appends [input]'s readable bytes to [out] as one message's payload; [input] itself is left unread. */
    fun deflate(
        input: ByteBuf,
        out: ByteBuf,
        keepContext: Boolean,
    ) {
        deflater.setInput(input.nioBuffer())
        dirty = true
        while (true) {
            out.ensureWritable(input.readableBytes() / 2 + 64)
            val space = out.writableBytes()
            val written = deflater.deflate(out.nioBuffer(out.writerIndex(), space), Deflater.SYNC_FLUSH)
            out.writerIndex(out.writerIndex() + written)
            // A sync flush that filled the space it was given may have more to say.
            if (written < space) break
        }
        out.writerIndex(out.writerIndex() - SYNC_FLUSH_TAIL.size)
        if (!keepContext) forget()
    }

    /** The next message starts on an empty window, referring to nothing sent before it. */
    fun forget() {
        if (!dirty) return
        deflater.reset()
        dirty = false
    }

    fun end() = deflater.end()
}

/** Inflates client messages, each on an empty window: client_no_context_takeover is part of every agreement. */
internal class MessageInflater {
    private val inflater = Inflater(true)
    private var scratch = ByteArray(INFLATE_SCRATCH_BYTES)

    /** [payload] inflated and read as UTF-8, or null once it inflates past [maxBytes]; a malformed stream throws. */
    fun inflateText(
        payload: ByteBuf,
        maxBytes: Int,
    ): String? {
        val length = payload.readableBytes()
        val input = ByteArray(length + SYNC_FLUSH_TAIL.size)
        payload.getBytes(payload.readerIndex(), input, 0, length)
        SYNC_FLUSH_TAIL.copyInto(input, length)
        try {
            inflater.setInput(input)
            var size = 0
            while (true) {
                if (size == scratch.size) {
                    if (size > maxBytes) return null
                    scratch = scratch.copyOf(minOf(size.toLong() * 2, maxBytes.toLong() + 1).toInt())
                }
                val inflated = inflater.inflate(scratch, size, scratch.size - size)
                size += inflated
                if (size > maxBytes) return null
                if (inflated == 0) {
                    if (inflater.needsDictionary()) throw DataFormatException("a compressed message asked for a preset dictionary")
                    if (inflater.needsInput() || inflater.finished()) break
                }
            }
            return String(scratch, 0, size, Charsets.UTF_8)
        } finally {
            inflater.reset()
            if (scratch.size > INFLATE_KEEP_BYTES) scratch = ByteArray(INFLATE_SCRATCH_BYTES)
        }
    }

    private companion object {
        const val INFLATE_SCRATCH_BYTES = 4_096
        const val INFLATE_KEEP_BYTES = 1 shl 20
    }
}

/** An event loop's share of compression: a deflater for frames that keep no context, and the loop's one inflater. */
internal class LoopDeflate {
    val fresh = MessageDeflater()
    val inflater = MessageInflater()
}

/** The sliding windows held across the process, against [MAX_DEFLATE_WINDOWS]. */
private val heldWindows = AtomicInteger()

private fun tryHoldWindow(): Boolean {
    if (heldWindows.incrementAndGet() <= MAX_DEFLATE_WINDOWS) return true
    heldWindows.decrementAndGet()
    return false
}

/**
 * One socket's outbound compression, touched only on its event loop. Answers share a sliding window while the
 * socket is busy, given back after [DEFLATE_IDLE_TICKS] quiet ticks. A compressed frame that bypassed the window,
 * a live one, resets it: the client's window then holds bytes this one never saw.
 */
internal class SocketDeflate(
    private val terms: DeflateTerms,
    private val loop: LoopDeflate,
) {
    private var window: MessageDeflater? = null
    private var idleTicks = 0

    /** [text] as one frame: compressed once it is [DEFLATE_MIN_BYTES] long, through this socket's window when it holds one. */
    fun encodeText(
        alloc: ByteBufAllocator,
        text: CharSequence,
    ): ByteBuf {
        val length = ByteBufUtil.utf8Bytes(text)
        if (length < DEFLATE_MIN_BYTES) return encodeTextFrame(alloc, text)
        val plain = alloc.buffer(length)
        try {
            ByteBufUtil.writeUtf8(plain, text)
            idleTicks = 0
            val window = window ?: openWindow()
            return encodeDeflatedFrame(alloc, length) { (window ?: loop.fresh).deflate(plain, it, keepContext = window != null) }
        } finally {
            plain.release()
        }
    }

    private fun openWindow(): MessageDeflater? {
        if (terms.serverNoContextTakeover || !tryHoldWindow()) return null
        return MessageDeflater().also { window = it }
    }

    /** A compressed frame went out without the window; the next answer starts it over. */
    fun bypassed() {
        window?.forget()
    }

    fun tick() {
        if (window != null && ++idleTicks >= DEFLATE_IDLE_TICKS) end()
    }

    fun end() {
        val held = window ?: return
        window = null
        held.end()
        heldWindows.decrementAndGet()
    }
}

/** One compressed text frame (RSV1 set) whose payload [deflate] appends; the header is placed once that length is known. */
internal fun encodeDeflatedFrame(
    alloc: ByteBufAllocator,
    plainBytes: Int,
    deflate: (ByteBuf) -> Unit,
): ByteBuf {
    val frame = alloc.ioBuffer(MAX_FRAME_HEADER + plainBytes / 2 + 64)
    try {
        frame.writerIndex(MAX_FRAME_HEADER)
        deflate(frame)
        val length = frame.writerIndex() - MAX_FRAME_HEADER
        val start = MAX_FRAME_HEADER - frameHeaderSize(length)
        setFrameHeader(frame, start, length, compressed = true)
        frame.readerIndex(start)
        return frame
    } catch (e: Throwable) {
        frame.release()
        throw e
    }
}

/** A stored deflate block's header for [length] bytes that follow it uncompressed; not the final block. */
internal fun writeStoredBlockHeader(
    out: ByteBuf,
    length: Int,
) {
    out.writeByte(0)
    out.writeShortLE(length)
    out.writeShortLE(length.inv())
}
