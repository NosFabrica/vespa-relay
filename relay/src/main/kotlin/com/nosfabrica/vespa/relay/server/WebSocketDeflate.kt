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

import io.ktor.server.websocket.WebSockets
import io.ktor.util.AttributeKey
import io.ktor.websocket.WebSocketDeflateExtension
import io.ktor.websocket.WebSocketExtension
import io.ktor.websocket.WebSocketExtensionFactory
import io.ktor.websocket.WebSocketExtensionHeader

/** Frames shorter than this go out uncompressed: OK, EOSE and CLOSED cost more to deflate than they save. */
internal const val DEFLATE_MIN_BYTES = 256

/**
 * Offers RFC 7692 permessage-deflate to clients that ask for it. An inflated frame is held to
 * [maxFrameBytes], the cap an uncompressed frame meets, so compression cannot carry a larger message in.
 */
internal fun WebSockets.WebSocketOptions.offerDeflate(maxFrameBytes: Long) {
    extensions {
        install(DecliningDeflate) {
            compressIfBiggerThan(DEFLATE_MIN_BYTES)
            maxInflatedFrameSize = maxFrameBytes.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        }
    }
}

/**
 * Ktor's deflate, declining an offer it cannot honour (`server_max_window_bits=10`, an unknown parameter)
 * so the client connects uncompressed, as RFC 7692 asks; Ktor's own throws and answers the upgrade 500.
 * Remove when Ktor's `serverNegotiation` declines instead of throwing.
 */
internal class DecliningDeflate private constructor(
    private val inner: WebSocketDeflateExtension,
) : WebSocketExtension<WebSocketDeflateExtension.Config> by inner {
    override val factory: WebSocketExtensionFactory<WebSocketDeflateExtension.Config, DecliningDeflate> = Companion

    override fun serverNegotiation(requestedProtocols: List<WebSocketExtensionHeader>): List<WebSocketExtensionHeader> =
        try {
            inner.serverNegotiation(requestedProtocols)
        } catch (_: RuntimeException) {
            emptyList()
        }

    companion object : WebSocketExtensionFactory<WebSocketDeflateExtension.Config, DecliningDeflate> {
        override val key: AttributeKey<DecliningDeflate> = AttributeKey("DecliningDeflate")
        override val rsv1: Boolean = WebSocketDeflateExtension.rsv1
        override val rsv2: Boolean = WebSocketDeflateExtension.rsv2
        override val rsv3: Boolean = WebSocketDeflateExtension.rsv3

        override fun install(config: WebSocketDeflateExtension.Config.() -> Unit): DecliningDeflate = DecliningDeflate(WebSocketDeflateExtension.install(config))
    }
}
