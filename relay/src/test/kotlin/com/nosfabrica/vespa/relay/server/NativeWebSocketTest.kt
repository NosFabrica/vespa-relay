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

import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocket08FrameDecoder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NativeWebSocketTest {
    @Test
    fun `a pre-encoded frame is what a client's decoder reads, at every header size`() {
        // Each length class of RFC 6455's header: 7 bits, 16 bits, 64 bits; and text that is not ASCII.
        val texts = listOf("[\"EOSE\",\"a\"]", "x".repeat(125), "y".repeat(126), "z".repeat(65_535), "w".repeat(70_000), "[\"EVENT\",\"é\",{\"content\":\"日本 🌍\"}]")
        for (text in texts) {
            val client = EmbeddedChannel(WebSocket08FrameDecoder(false, false, 1 shl 20))
            assertTrue(client.writeInbound(encodeTextFrame(UnpooledByteBufAllocator.DEFAULT, text)), "a frame of ${text.length} chars decodes")
            val frame = client.readInbound<TextWebSocketFrame>()
            assertTrue(frame.isFinalFragment)
            assertEquals(text, frame.text())
            frame.release()
            client.finishAndReleaseAll()
        }
    }
}
