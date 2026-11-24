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
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A native connection closed for a message past its cap: what it owed is said first, and the close reaches the client. */
class ClosingConnectionTest {
    private val signer = NostrSignerSync()
    private val cap = 2_000_000

    /** The header of a text frame declaring [declared] bytes, masked with a zero key. */
    private fun header(declared: Long) =
        ByteArrayOutputStream()
            .apply {
                write(0x81)
                write(0x80 or 127)
                for (shift in 56 downTo 0 step 8) write((declared shr shift).toInt() and 0xFF)
                write(ByteArray(4))
            }.toByteArray()

    @Test
    fun `commands read before a message past the cap are answered before the close`() {
        val events = (0 until 20).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "before $it") }
        withRawRelay(native = true, maxFrameBytes = cap) { ws ->
            // One write: twenty EVENTs, then a frame that passes the cap.
            val wire = ByteArrayOutputStream()
            val raw = RawWebSocket(ws.socket, ws.port)
            for (e in events) {
                val frame = ByteArrayOutputStream()
                val payload = """["EVENT",${e.toJson()}]""".toByteArray()
                frame.write(0x81)
                frame.write(0x80 or 126)
                frame.write(payload.size shr 8)
                frame.write(payload.size and 0xFF)
                frame.write(ByteArray(4))
                frame.write(payload)
                wire.write(frame.toByteArray())
            }
            wire.write(header(cap + 1L))
            ws.socket.getOutputStream().apply {
                write(wire.toByteArray())
                flush()
            }
            val frames = raw.untilClose()
            val texts = frames.filter { it.opcode == 0x1 }.map { it.text }
            for (e in events) assertTrue(texts.any { it.startsWith("""["OK","${e.id}",true""") }, "no OK for ${e.id}: ${texts.map { it.take(90) }}")
            assertTrue(texts.any { it.startsWith("[\"NOTICE\"") }, "a NOTICE: ${texts.map { it.take(90) }}")
            assertEquals(1009, frames.last().closeCode)
        }
    }

    @Test
    fun `the NOTICE and the close reach a client still sending`() {
        val events = (0 until 300).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "queued $it") }
        // A small window and no reading: the relay's answers wait in its own send queue, ahead of the NOTICE.
        withRawRelay(native = true, maxFrameBytes = cap, soTimeoutMs = 15_000, receiveBufferBytes = 4_096) { ws ->
            for (e in events) ws.text("""["EVENT",${e.toJson()}]""")
            Thread.sleep(1_000)
            val out = ws.socket.getOutputStream()
            out.write(header(1L shl 40))
            // Keeps uploading the declared terabyte while the relay refuses it, until the socket fails.
            val sender =
                Thread {
                    try {
                        val chunk = ByteArray(65_536)
                        val until = System.nanoTime() + 12_000_000_000L
                        while (System.nanoTime() < until) out.write(chunk)
                    } catch (_: Exception) {
                    }
                }.apply { start() }
            try {
                Thread.sleep(1_500)
                val frames = ws.untilClose()
                assertTrue(frames.any { it.opcode == 0x1 && it.text.startsWith("[\"NOTICE\"") }, "a NOTICE: ${frames.map { it.opcode }}")
                assertEquals(1009, frames.last().closeCode)
            } finally {
                sender.join(20_000)
            }
        }
    }

    @Test
    fun `a client that ignores the close is cut off`() {
        withRawRelay(native = true, maxFrameBytes = cap, soTimeoutMs = 20_000) { ws ->
            val out = ws.socket.getOutputStream()
            out.write(header(cap + 1L))
            out.flush()
            ws.untilClose()
            // The relay shut its side; this client never answers and keeps its own open, writing into it.
            val started = System.nanoTime()
            val cutOff =
                try {
                    while ((System.nanoTime() - started) / 1e9 < 15) {
                        out.write(ByteArray(1_024))
                        out.flush()
                        Thread.sleep(200)
                    }
                    false
                } catch (_: IOException) {
                    true
                }
            assertTrue(cutOff, "the relay still held the connection after 15 s")
        }
    }
}
