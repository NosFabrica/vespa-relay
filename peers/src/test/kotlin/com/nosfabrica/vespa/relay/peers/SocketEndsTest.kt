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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import kotlinx.coroutines.runBlocking
import java.io.EOFException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** What the recorder says about a socket's end, against a socket that behaves as quartz's own does. */
class SocketEndsTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private var nowMs = 1_000L
    private val ends = SocketEnds { nowMs }

    /** Quartz's socket: `disconnect` reports `onClosed` synchronously, as its contract says. */
    private inner class FakeSocket(
        val out: WebSocketListener,
    ) : WebSocket {
        override fun needsReconnect() = false

        override fun connect() = out.onOpen(5, false)

        override fun disconnect() = out.onClosed(1000, "client disconnect")

        override fun send(msg: String) = true
    }

    private var inner: FakeSocket? = null
    private val builder =
        ends.around(
            object : WebsocketBuilder {
                override fun build(
                    url: NormalizedRelayUrl,
                    out: WebSocketListener,
                ): WebSocket = FakeSocket(out).also { inner = it }
            },
        )

    /** Quartz's relay client: on a failure it disconnects the socket, and it reads the end as it lands. */
    private inner class QuartzSide : WebSocketListener {
        var socket: WebSocket? = null
        var endSeenOnClose: SocketEnds.End? = null

        override fun onOpen(
            pingMillis: Int,
            compression: Boolean,
        ) = Unit

        override suspend fun onMessage(text: String) = Unit

        override fun onClosed(
            code: Int,
            reason: String,
        ) {
            endSeenOnClose = ends.lastEnd(url)
        }

        override fun onFailure(
            t: Throwable,
            code: Int?,
            response: String?,
        ) {
            endSeenOnClose = ends.lastEnd(url)
            socket?.disconnect()
        }
    }

    private fun open(): Pair<WebSocket, QuartzSide> {
        val quartz = QuartzSide()
        val socket = builder.build(url, quartz)
        quartz.socket = socket
        socket.connect()
        return socket to quartz
    }

    @Test
    fun `a close frame from the relay is the relay's with its code and reason, recorded before quartz hears of it`() {
        val (socket, quartz) = open()
        socket.send("[\"NEG-OPEN\",\"s\",{\"kinds\":[1]},\"" + "a".repeat(4_000) + "\"]")
        socket.send("[\"NEG-MSG\",\"s\",\"ab\"]")
        nowMs += 2_000
        inner!!.out.onClosed(1009, "message too big")

        val end = assertNotNull(quartz.endSeenOnClose, "the end must be readable from quartz's own close handler")
        assertEquals(SocketEnds.Side.RELAY, end.side)
        assertEquals(1009, end.code)
        assertEquals("message too big", end.reason)
        assertEquals(2, end.sent.frames)
        assertEquals("NEG-OPEN", end.sent.largest?.verb)
        assertTrue(end.sent.largest!!.bytes > 4_000)
        assertEquals("NEG-MSG", end.sent.last?.verb)
        assertEquals(2_000, end.atMs - end.openedAtMs!!)
        assertTrue(end.cause.startsWith("relay closed 1009"), end.cause)
    }

    @Test
    fun `our own close is ours and names who asked, not the close the socket reports for it`() {
        val (socket, _) = open()
        socket.disconnect()

        val end = assertNotNull(ends.lastEnd(url))
        assertEquals(SocketEnds.Side.US, end.side)
        assertNull(end.code, "the 1000 the socket reports for our own close is not a relay's code")
        assertTrue(end.by!!.startsWith("SocketEndsTest."), "the caller should be this test, was ${end.by}")
    }

    @Test
    fun `a transport failure is the transport's, and the disconnect quartz answers it with does not overwrite it`() {
        val (_, quartz) = open()
        inner!!.out.onFailure(EOFException("stream ended"), null, null)

        assertEquals(SocketEnds.Side.TRANSPORT, quartz.endSeenOnClose?.side)
        val end = assertNotNull(ends.lastEnd(url))
        assertEquals(SocketEnds.Side.TRANSPORT, end.side)
        assertTrue(end.reason!!.startsWith("EOFException"), end.reason)
    }

    @Test
    fun `a dial that never opened says so`() {
        val quartz = QuartzSide()
        builder.build(url, quartz)
        inner!!.out.onFailure(java.net.SocketTimeoutException("connect timed out"), null, null)

        val end = assertNotNull(ends.lastEnd(url))
        assertNull(end.openedAtMs)
        assertTrue(end.toString().contains("before it opened"), end.toString())
    }

    @Test
    fun `since a mark reads only what happened after it`() =
        runBlocking {
            val (first, _) = open()
            first.disconnect()
            nowMs += 10
            val mark = ends.mark(url)

            val (second, _) = open()
            second.send("[\"NEG-OPEN\",\"s\",{},\"61\"]")
            inner!!.out.onMessage("[\"NEG-MSG\",\"s\",\"6100\"]")
            inner!!.out.onMessage("[\"EVENT\",\"s\",{}]")
            nowMs += 30
            val live = ends.since(mark)
            assertNull(live.end, "the earlier socket's end is not this ask's")
            assertEquals(1, live.sent?.frames, "the live socket's frames are still readable")
            assertEquals(1, live.negFramesIn)
            assertEquals(30, live.elapsedMs)

            inner!!.out.onClosed(1001, "going away")
            assertEquals(1001, ends.since(mark).end?.code)
        }

    @Test
    fun `sizes are UTF-8 bytes and verbs come off the frame`() {
        assertEquals(3, SocketEnds.utf8Length("abc"))
        assertEquals(2, SocketEnds.utf8Length("é"))
        assertEquals(4, SocketEnds.utf8Length("😀"))
        assertEquals("REQ", SocketEnds.verbOf("[\"REQ\",\"s\",{}]"))
        assertEquals("?", SocketEnds.verbOf("not a frame"))
    }
}
