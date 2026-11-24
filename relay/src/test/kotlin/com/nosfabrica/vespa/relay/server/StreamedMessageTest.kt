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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Messages past what the engine could accept, larger than any frame the relay once buffered:
 * each is answered the way quartz answers an oversized frame, and the connection carries on.
 */
class StreamedMessageTest {
    private val signer = NostrSignerSync()
    private val tooLarge = "invalid: message too large (max 262144)"

    /** Five megabytes: past the old 4 MiB cap, under today's. */
    private val big = "a".repeat(5_000_000)

    private fun ok(
        texts: List<String>,
        id: String,
    ) = texts.firstOrNull { it.startsWith("""["OK","$id",""") }

    @Test
    fun `an EVENT whose id comes after five megabytes of content is answered by that id`() {
        val id = "1".repeat(64)
        val frame = """["EVENT",{"content":"$big","tags":[["p","${"2".repeat(64)}"]],"id":"$id","kind":1}]"""
        val after = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "after")
        bothTransports { ws, _ ->
            ws.text(frame)
            ws.text("""["EVENT",${after.toJson()}]""")
            val texts = ws.textsUntil { ok(it, id) != null && ok(it, after.id) != null }
            assertEquals("""["OK","$id",false,"$tooLarge"]""", ok(texts, id))
            assertTrue(",true," in ok(texts, after.id)!!, ok(texts, after.id))
            ws.stillServes()
        }
    }

    @Test
    fun `a fragmented EVENT whose id arrives in a late fragment is answered by that id`() {
        val id = "3".repeat(64)
        // The id lies past the point where the relay stops holding the message.
        val frame = """["EVENT",{"content":"${"b".repeat(1_500_000)}","id":"$id"}]""".toByteArray()
        bothTransports { ws, _ ->
            ws.fragmented(frame, chunk = 100_000)
            val texts = ws.textsUntil { ok(it, id) != null }
            assertEquals("""["OK","$id",false,"$tooLarge"]""", ok(texts, id))
            ws.stillServes()
        }
    }

    @Test
    fun `an oversized REQ is closed on its sub id and an unreadable message gets a NOTICE`() {
        bothTransports { ws, _ ->
            ws.text("""["REQ","huge",{"search":"$big"}]""")
            ws.text("""["HELLO","$big"]""")
            val texts = ws.textsUntil { seen -> seen.any { it.startsWith("[\"CLOSED\"") } && seen.any { it.startsWith("[\"NOTICE\"") } }
            assertEquals("""["CLOSED","huge","$tooLarge"]""", texts.first { it.startsWith("[\"CLOSED\"") })
            assertEquals("""["NOTICE","$tooLarge"]""", texts.first { it.startsWith("[\"NOTICE\"") })
            ws.stillServes()
        }
    }

    @Test
    fun `an AUTH too large to read is answered OK false by its event id`() {
        val id = "4".repeat(64)
        bothTransports { ws, _ ->
            ws.text("""["AUTH",{"id":"$id","content":"$big"}]""")
            val texts = ws.textsUntil { ok(it, id) != null }
            assertEquals("""["OK","$id",false,"$tooLarge"]""", ok(texts, id))
        }
    }

    @Test
    fun `a message held open with pings is cut off at the deadline`() {
        withRawRelay(native = true, openMessageDeadlineMs = 2_000, soTimeoutMs = 10_000) { ws ->
            val started = System.nanoTime()
            ws.send("""["EVENT",{"content":"""".toByteArray() + ByteArray(500_000) { 'c'.code.toByte() }, fin = false)
            // A ping every 300 ms: never silent, never finishing the message.
            val pinger =
                Thread {
                    try {
                        while (true) {
                            ws.send(ByteArray(0), opcode = 0x9)
                            Thread.sleep(300)
                        }
                    } catch (_: Exception) {
                    }
                }.apply { start() }
            try {
                val close = ws.untilClose(withinMs = 10_000).last()
                val seconds = (System.nanoTime() - started) / 1e9
                assertEquals(1008, close.closeCode, close.closeReason)
                assertTrue(seconds in 1.5..9.0, "closed after $seconds s")
                ws.awaitEnd()
            } finally {
                pinger.interrupt()
            }
        }
    }
}
