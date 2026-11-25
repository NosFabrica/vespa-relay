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

import kotlin.test.Test
import kotlin.test.assertEquals

/** RFC 6455 violations on the native transport: each closes with its status code, and the socket ends. */
class WebSocketProtocolTest {
    private fun closesWith(
        code: Int,
        what: String,
        send: RawWebSocket.() -> Unit,
    ) = withRawRelay(native = true) { ws ->
        ws.send()
        val close = ws.untilClose(withinMs = 5_000).last()
        assertEquals(code, close.closeCode, "$what: ${close.closeReason}")
        ws.awaitEnd()
    }

    private val text = """["REQ","x",{}]""".toByteArray()

    @Test
    fun `each protocol violation closes with 1002`() {
        closesWith(1002, "unmasked") { send(text, masked = false) }
        closesWith(1002, "an RSV bit") { send(text, rsv = 0b100) }
        closesWith(1002, "reserved data opcode 3") { send(text, opcode = 0x3) }
        closesWith(1002, "a 126-byte ping") { send(ByteArray(126), opcode = 0x9) }
        closesWith(1002, "a non-final ping") { send(ByteArray(0), opcode = 0x9, fin = false) }
        closesWith(1002, "a continuation with no start") { send(text, opcode = 0x0) }
        closesWith(1002, "close code 768") { send(byteArrayOf(0x03, 0x00), opcode = 0x8) }
        closesWith(1002, "a new text inside a fragmented message") {
            send(text, fin = false)
            send(text)
        }
        closesWith(1002, "a 16-bit length under 126") {
            // 0x81, masked + 126, then a length of 5: the minimal encoding is the 7-bit one.
            socket.getOutputStream().apply {
                write(byteArrayOf(0x81.toByte(), (0x80 or 126).toByte(), 0, 5, 1, 2, 3, 4, 1, 2, 3, 4, 5))
                flush()
            }
        }
    }

    @Test
    fun `a close reason that is not UTF-8 closes with 1007`() {
        closesWith(1007, "a malformed reason") { send(byteArrayOf(0x03, 0xE8.toByte(), 0xC3.toByte(), 0x28), opcode = 0x8) }
    }

    @Test
    fun `a client's close is answered with its own code`() {
        withRawRelay(native = true) { ws ->
            ws.send(byteArrayOf(0x03, 0xE8.toByte()), opcode = 0x8)
            assertEquals(1000, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
    }
}
