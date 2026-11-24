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

import com.nosfabrica.vespa.relay.server.config.DEFAULT_MAX_FRAME_BYTES
import com.nosfabrica.vespa.relay.server.config.defaultMaxFrameBytes
import com.nosfabrica.vespa.relay.server.config.maxFrameBytesFromEnv
import com.nosfabrica.vespa.relay.server.config.messageBytes
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The websocket's frame cap, spoken by hand: a client library would refuse to declare a frame it
 * does not send, and the declaration is what is under test.
 */
class RelayFrameLimitTest {
    private val signer = NostrSignerSync()

    private fun note(text: String): Event = signer.sign(1_700_000_000L, 1, emptyArray(), text)

    /** A follow list of [follows] keys: a real kind 3 of about 73 bytes a follow. */
    private fun followList(follows: Int): Event = signer.sign(1_700_000_000L, 3, Array(follows) { arrayOf("p", it.toString(16).padStart(64, '0')) }, "")

    private fun eventFrame(event: Event) = """["EVENT",${event.toJson()}]""".toByteArray(Charsets.UTF_8)

    private fun RawWebSocket.oks(ids: Set<String>): Map<String, String> =
        textsUntil { seen -> ids.all { id -> seen.any { it.startsWith("""["OK","$id",""") } } }
            .mapNotNull { text -> ids.firstOrNull { text.startsWith("""["OK","$it",""") }?.let { it to text } }
            .toMap()

    private fun assertRefusedAsTooLarge(ok: String?) {
        assertTrue(ok != null && ok.contains(",false,") && "message too large" in ok, "the oversized EVENT's OK: $ok")
    }

    @Test
    fun `an EVENT over the engine's length is answered by id and the connection keeps serving`() {
        val before = note("before")
        val big = followList(14_000)
        val after = note("after")
        assertTrue(eventFrame(big).size in 1_000_000..2_000_000, "a 1 MB follow list: ${eventFrame(big).size}")
        withRawRelay(native = true) { ws ->
            for (e in listOf(before, big, after)) ws.send(eventFrame(e))
            val oks = ws.oks(setOf(before.id, big.id, after.id))
            assertTrue(",true," in oks.getValue(before.id), oks.getValue(before.id))
            assertRefusedAsTooLarge(oks[big.id])
            assertTrue(",true," in oks.getValue(after.id), oks.getValue(after.id))
            ws.stillServes()
        }
    }

    @Test
    fun `a fragmented EVENT over the engine's length is answered by id like a whole one`() {
        val big = followList(14_000)
        val after = note("after")
        withRawRelay(native = true) { ws ->
            ws.fragmented(eventFrame(big), chunk = 200_000)
            ws.send(eventFrame(after))
            val oks = ws.oks(setOf(big.id, after.id))
            assertRefusedAsTooLarge(oks[big.id])
            assertTrue(",true," in oks.getValue(after.id), oks.getValue(after.id))
            ws.stillServes()
        }
    }

    @Test
    fun `a fragmented message past the frame cap closes with 1009 after a NOTICE`() {
        val limits = relayLimitsFromEnv(emptyMap())
        val cap = messageBytes(limits).toInt() * 2
        withRawRelay(native = true, limits = limits, maxFrameBytes = cap) { ws ->
            // Fragments each under the cap, together over it: the message is bounded, not just each frame.
            ws.send("[\"EVENT\",{\"content\":\"".toByteArray(), fin = false)
            repeat(cap / 100_000 + 1) { ws.send(ByteArray(100_000) { 'a'.code.toByte() }, opcode = 0x0, fin = false) }
            val frames = ws.untilClose()
            assertEquals(1009, frames.last().closeCode, "closed as too big")
            assertTrue(frames.any { it.opcode == 0x1 && it.text.startsWith("[\"NOTICE\"") }, "a NOTICE before the close")
        }
    }

    @Test
    fun `MAX_FRAME_BYTES is at least the engine's length in bytes`() {
        val limits = relayLimitsFromEnv(emptyMap())
        assertEquals(DEFAULT_MAX_FRAME_BYTES, maxFrameBytesFromEnv(emptyMap(), limits))
        assertEquals(8_000_000, maxFrameBytesFromEnv(mapOf("MAX_FRAME_BYTES" to "8000000"), limits))
        assertFailsWith<IllegalStateException> { maxFrameBytesFromEnv(mapOf("MAX_FRAME_BYTES" to (messageBytes(limits) - 1).toString()), limits) }
        // Below the floor the refusal names the setting that sets it, and the number.
        val shipped = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "15728640"))
        val refusal = assertFailsWith<IllegalStateException> { maxFrameBytesFromEnv(mapOf("MAX_FRAME_BYTES" to "4194304"), shipped) }.message!!
        assertTrue("MAX_MESSAGE_LENGTH" in refusal && "47185920" in refusal, refusal)
        // An engine length past the default raises the default with it.
        val long = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "8000000"))
        assertEquals(24_000_000, maxFrameBytesFromEnv(emptyMap(), long))
    }

    @Test
    fun `a frame declaring more than the cap is refused from its header`() {
        val limits = relayLimitsFromEnv(emptyMap())
        val cap = defaultMaxFrameBytes(limits)
        bothTransports(limits, cap) { ws, _ ->
            // One byte over the cap, a handful sent: a server without the cap would sit waiting for the
            // rest, so a timeout is the failure.
            ws.send("[\"REQ\"".toByteArray(), declared = cap + 1L)
            assertEquals(1009, ws.untilClose().last().closeCode, "closed as too big")
            // Refused, not fallen over: the next client is served.
            Socket().use { next ->
                next.connect(InetSocketAddress("127.0.0.1", ws.port), 5_000)
                next.soTimeout = 5_000
                upgrade(next, ws.port)
            }
        }
    }

    /** The cap counts bytes and the engine counts characters, so a message the engine admits must fit it. */
    @Test
    fun `a message at the engine's length in three-byte characters is still read`() {
        val subId = "€".repeat(200)
        val req = """["REQ","$subId",{"ids":["${"0".repeat(64)}"],"#t":["${"€".repeat(250)}"],"search":"include:spam"}]"""
        bothTransports(relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to req.length.toString()))) { ws, _ ->
            ws.text(req)
            ws.textsUntil { seen -> seen.any { subId in it } }
        }
    }

    @Test
    fun `the cap admits a message of the engine's length in any characters`() {
        val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1000"))
        for (worst in listOf("€".repeat(1000), "😀".repeat(500), "a".repeat(1000))) {
            assertTrue(worst.toByteArray(Charsets.UTF_8).size <= messageBytes(limits), "${worst.take(1)}: ${worst.length} chars")
        }
    }
}
