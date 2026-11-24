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
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals

/** The native connection's clocks measure the client, never the relay's own backlog, and bytes arriving are a client alive. */
class NativeClockTest {
    private val signer = NostrSignerSync()

    /** One masked frame of [payload] with a zero mask, as bytes, to be cut wherever a test likes. */
    private fun wireFrame(payload: ByteArray): ByteArray =
        ByteArrayOutputStream()
            .apply {
                write(0x81)
                when {
                    payload.size < 126 -> {
                        write(0x80 or payload.size)
                    }

                    payload.size <= 0xFFFF -> {
                        write(0x80 or 126)
                        write(payload.size shr 8)
                        write(payload.size and 0xFF)
                    }

                    else -> {
                        write(0x80 or 127)
                        for (shift in 56 downTo 0 step 8) write(((payload.size.toLong() shr shift) and 0xFF).toInt())
                    }
                }
                write(ByteArray(4))
                write(payload)
            }.toByteArray()

    @Test
    fun `a message the relay stopped reading is not timed while reads are paused`() {
        val events = (0 until 71).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "$it".padEnd(5_000, 'q')) }
        val frames = events.map { wireFrame("""["EVENT",${it.toJson()}]""".toByteArray()) }
        // Seventy whole frames and half of one more: the relay pauses reads past 64 commands with that half held.
        val last = frames.last()
        val first = frames.dropLast(1).fold(ByteArray(0)) { acc, f -> acc + f } + last.copyOf(last.size / 2)
        withRawRelay(native = true, ingest = SlowIngest(100), openMessageDeadlineMs = 2_000, soTimeoutMs = 20_000) { ws ->
            val out = ws.socket.getOutputStream()
            out.write(first)
            out.flush()
            Thread.sleep(500)
            out.write(last.copyOfRange(last.size / 2, last.size))
            out.flush()
            // Draining 70 commands at 100 ms each outlasts the 2 s deadline; the relay's own pause is not the client's delay.
            val texts = ws.textsUntil { seen -> seen.count { it.startsWith("[\"OK\"") } == events.size }
            assertEquals(events.size, texts.count { it.startsWith("[\"OK\"") && ",true," in it })
        }
    }

    @Test
    fun `a frame still arriving keeps its connection alive past the silence timeout`() {
        val id = "5".repeat(64)
        val frame = wireFrame("""["EVENT",{"id":"$id","content":"${"s".repeat(300_000)}"}]""".toByteArray())
        withRawRelay(native = true, keepalive = Keepalive(pingPeriodMs = 1_000, silenceTimeoutMs = 2_000), soTimeoutMs = 10_000) { ws ->
            val out = ws.socket.getOutputStream()
            // Four seconds for one frame, twice the silence timeout, and no pong: only its bytes say the client is there.
            val step = frame.size / 100 + 1
            for (at in frame.indices step step) {
                out.write(frame, at, minOf(step, frame.size - at))
                out.flush()
                Thread.sleep(40)
            }
            val texts = ws.textsUntil { seen -> seen.any { it.startsWith("""["OK","$id",""") } }
            assertEquals(1, texts.count { it.startsWith("""["OK","$id",""") })
        }
    }
}
