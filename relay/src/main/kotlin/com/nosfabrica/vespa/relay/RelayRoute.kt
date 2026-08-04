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
package com.nosfabrica.vespa.relay

import io.ktor.server.routing.Route
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

/**
 * The most frames the outbound queue will hold for one connection before the
 * client is treated as a slow consumer. Cheap for many idle connections (an
 * empty queue costs nothing); a client this far behind is stuck.
 */
private const val MAX_OUTGOING_BUFFER = 8192

/** Mount the relay websocket on `/`; the composition root serves the NIP-11 doc beside it. */
fun Route.nostrRelay(server: NostrRelayServer) {
    webSocket("/") {
        // One writer coroutine drains a BOUNDED ordered queue to the socket.
        // The engine's send callback is non-suspend, so we bridge through the
        // channel with trySend. A bounded buffer caps per-connection memory:
        // when it fills, the client isn't draining fast enough, so we
        // disconnect it rather than dropping frames — silently dropping
        // EVENT/EOSE would corrupt NIP-01 subscription semantics.
        val outCh = Channel<String>(MAX_OUTGOING_BUFFER)
        val writer = launch { for (text in outCh) outgoing.send(Frame.Text(text)) }
        try {
            server.serve(
                send = { text ->
                    val result = outCh.trySend(text)
                    if (result.isFailure && !result.isClosed) {
                        // Slow consumer: stop queueing and evict.
                        outCh.close()
                        launch {
                            runCatching {
                                close(CloseReason(CloseReason.Codes.TRY_AGAIN_LATER, "slow consumer: over $MAX_OUTGOING_BUFFER buffered frames"))
                            }
                        }
                    }
                },
                incoming = { session ->
                    for (frame in incoming) {
                        if (frame is Frame.Text) session.receive(frame.readText())
                    }
                },
            )
        } finally {
            outCh.close()
            writer.cancel()
        }
    }
}
