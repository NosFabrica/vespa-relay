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

import io.ktor.server.routing.Route
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicLong

/** Outbound frames queued for one connection before it is treated as a slow consumer. */
private const val MAX_OUTGOING_BUFFER = 8192

/** Outbound characters queued for one connection, at most two heap bytes each, before the same. */
private const val MAX_OUTGOING_CHARS = 16L * 1024 * 1024

/** How long a slow consumer's close frame gets before the session is torn down anyway. */
private const val CLOSE_GRACE_MS = 5_000L

/** The relay websocket on `/`; the composition root serves the NIP-11 doc beside it. */
fun Route.nostrRelay(server: NostrRelayServer) {
    webSocket("/") {
        // One writer drains a bounded queue; a full queue disconnects the client rather than dropping
        // frames, which would break NIP-01.
        val outQueue = OutboundQueue()
        val writer = launch { outQueue.drain { text -> outgoing.send(Frame.Text(text)) } }
        try {
            server.serve(
                send = { text ->
                    if (!outQueue.offer(text)) {
                        outQueue.close()
                        launch {
                            // The close frame queues behind the congestion that tripped this; cancelling the
                            // session closes the socket regardless.
                            runCatching {
                                withTimeoutOrNull(CLOSE_GRACE_MS) {
                                    close(CloseReason(CloseReason.Codes.TRY_AGAIN_LATER, "slow consumer: outbound buffer full"))
                                }
                            }
                            this@webSocket.cancel()
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
            outQueue.close()
            writer.cancel()
        }
    }
}

/** One connection's outbound frames, bounded by count and by characters; the writer frees both as it sends. */
internal class OutboundQueue(
    maxFrames: Int = MAX_OUTGOING_BUFFER,
    private val maxChars: Long = MAX_OUTGOING_CHARS,
) {
    private val frames = Channel<String>(maxFrames)
    private val queuedChars = AtomicLong()

    @Volatile
    private var closed = false

    /** False only when a bound is reached on an open queue; a closed queue drops [text], its connection is ending. */
    fun offer(text: String): Boolean {
        if (closed) return true
        val chars = text.length.toLong()
        if (queuedChars.addAndGet(chars) > maxChars || !frames.trySend(text).isSuccess) {
            queuedChars.addAndGet(-chars)
            return closed
        }
        return true
    }

    /** Hands each frame to [write] in order until the queue is closed. */
    suspend fun drain(write: suspend (String) -> Unit) {
        for (text in frames) {
            write(text)
            queuedChars.addAndGet(-text.length.toLong())
        }
    }

    fun close() {
        closed = true
        frames.close()
    }
}
