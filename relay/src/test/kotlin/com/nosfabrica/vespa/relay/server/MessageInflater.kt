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
import java.util.zip.DataFormatException
import java.util.zip.Inflater

/** The four bytes a sync flush ends with, which a message leaves off and its reader puts back (RFC 7692 7.2.1). */
private val SYNC_FLUSH_TAIL = byteArrayOf(0, 0, -1, -1)

/**
 * A whole client message inflated at once, as the native transport read one before InboundFrameDecoder inflated
 * them as they arrive: the reference the decoder's inflation is held to in tests.
 */
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
