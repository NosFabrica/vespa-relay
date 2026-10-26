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

import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlin.test.Test
import kotlin.test.assertEquals

class RelayWebSocketTest {
    @Test
    fun `a frame's text is what Ktor's readText reads`() {
        for (text in listOf("", """["REQ","s",{"kinds":[1]}]""", "café 你好 🚀", "x".repeat(100_000))) {
            val frame = Frame.Text(text)
            assertEquals(frame.readText(), frame.utf8Text())
        }
    }

    @Test
    fun `malformed UTF-8 is replaced as readText replaces it`() {
        val cases =
            listOf(
                byteArrayOf(0x5B, 0xC3.toByte(), 0x5D),
                byteArrayOf(0xFF.toByte(), 0xFE.toByte(), 0x41),
                byteArrayOf(0xE4.toByte(), 0xBD.toByte()),
                byteArrayOf(0xED.toByte(), 0xA0.toByte(), 0x80.toByte(), 0x41),
                byteArrayOf(0xF0.toByte(), 0x9F.toByte(), 0x9A.toByte(), 0x41),
            )
        for (bytes in cases) {
            val frame = Frame.Text(true, bytes)
            assertEquals(frame.readText(), frame.utf8Text(), bytes.joinToString { "%02x".format(it) })
        }
    }
}
