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

import io.netty.buffer.AbstractByteBufAllocator
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.buffer.UnpooledByteBufAllocator

/** Hands out unpooled buffers and records the most bytes they held at once, read at each allocation and each [sample]. */
internal class PeakAllocator : AbstractByteBufAllocator(false) {
    private val delegate = UnpooledByteBufAllocator(false)
    private val live = mutableListOf<ByteBuf>()
    var peak = 0L
        private set

    override fun newHeapBuffer(
        initialCapacity: Int,
        maxCapacity: Int,
    ): ByteBuf = track(delegate.heapBuffer(initialCapacity, maxCapacity))

    override fun newDirectBuffer(
        initialCapacity: Int,
        maxCapacity: Int,
    ): ByteBuf = track(delegate.heapBuffer(initialCapacity, maxCapacity))

    override fun isDirectBufferPooled() = false

    private fun track(buf: ByteBuf): ByteBuf {
        live += buf
        sample()
        return buf
    }

    /** Live capacity now, a buffer grown since its allocation included. */
    fun sample() {
        live.removeAll { it.refCnt() == 0 }
        peak = maxOf(peak, live.sumOf { it.capacity().toLong() })
    }
}

/** One masked client frame (RFC 6455 5.2) carrying [payload]. */
internal fun clientFrame(
    payload: ByteArray,
    opcode: Int = 0x1,
    fin: Boolean = true,
): ByteBuf {
    val out = Unpooled.buffer(payload.size + 14)
    out.writeByte((if (fin) 0x80 else 0) or opcode)
    when {
        payload.size < 126 -> {
            out.writeByte(0x80 or payload.size)
        }

        payload.size <= 0xFFFF -> {
            out.writeByte(0x80 or 126)
            out.writeShort(payload.size)
        }

        else -> {
            out.writeByte(0x80 or 127)
            out.writeLong(payload.size.toLong())
        }
    }
    val mask = byteArrayOf(0x0B, 0x7C, 0x21, 0x55)
    out.writeBytes(mask)
    for (i in payload.indices) out.writeByte(payload[i].toInt() xor mask[i % 4].toInt())
    return out
}
