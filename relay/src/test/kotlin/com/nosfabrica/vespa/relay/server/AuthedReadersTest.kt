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

import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Presence, and the three ways it was going to be wrong.
 *
 * The properties asserted here are the ones the mirror's behaviour rests on —
 * an identity survives one of its two sockets closing, a disconnect really does
 * end presence, and a truncated response says so — rather than the shape of the
 * map behind them.
 */
class AuthedReadersTest {
    private fun key(prefix: String) = prefix.repeat(64 / prefix.length)

    private val alice = key("a")
    private val bob = key("b")
    private val carol = key("c")

    @Test
    fun `a reader on two sockets survives one of them closing`() {
        // The bug this exists for: presence keyed by identity, decremented on
        // disconnect, and a client that opens a second tab and closes the first
        // vanishes from the mirror while still sitting there reading.
        val readers = AuthedReaders("t")
        readers.signedIn(1, alice)
        readers.signedIn(2, alice)

        readers.onDisconnect(1)

        assertEquals(listOf(alice), readers.snapshot().pubkeys)
        assertEquals(1, readers.connectionCount())
    }

    @Test
    fun `a disconnect takes every identity on that socket`() {
        val readers = AuthedReaders("t")
        readers.signedIn(1, alice)
        readers.signedIn(1, bob)
        readers.signedIn(2, carol)

        readers.onDisconnect(1)

        assertEquals(listOf(carol), readers.snapshot().pubkeys)
    }

    @Test
    fun `the same login recorded twice is one reader`() {
        // Quartz accepts an AUTH frame for its whole ten-minute window and a
        // client may replay it. The policy's own `told` set dedups the notice,
        // but nothing downstream should depend on that.
        val readers = AuthedReaders("t")
        readers.signedIn(1, alice)
        readers.signedIn(1, alice)

        assertEquals(listOf(alice), readers.snapshot().pubkeys)
    }

    @Test
    fun `order is the oldest connection, so a burst of arrivals cannot displace who is already here`() {
        // The bound has to cut somewhere and this is what decides who it cuts.
        // Cutting the newest means the subscriptions the mirror is already
        // holding survive a rush; any other order makes an established reader's
        // mirroring a matter of luck.
        val readers = AuthedReaders("t")
        readers.signedIn(9, carol)
        readers.signedIn(3, alice)
        readers.signedIn(5, bob)

        assertEquals(listOf(alice, bob, carol), readers.snapshot().pubkeys)
    }

    @Test
    fun `an identity sorts at its oldest connection, not its newest`() {
        val readers = AuthedReaders("t")
        readers.signedIn(1, alice)
        readers.signedIn(2, bob)
        // Alice opens a third socket. She has been here longest either way.
        readers.signedIn(3, alice)

        assertEquals(listOf(alice, bob), readers.snapshot().pubkeys)
    }

    @Test
    fun `what did not fit is counted, never silently dropped`() {
        // The one failure of this feature that produces no error anywhere: the
        // omitted readers ARE signed in and nothing is mirroring for them.
        val readers = AuthedReaders("t")
        val overflow = AuthedReaders.MAX_READERS + 7
        repeat(overflow) { i -> readers.signedIn(i.toLong(), "%064x".format(i)) }

        val snapshot = readers.snapshot()

        assertEquals(AuthedReaders.MAX_READERS, snapshot.pubkeys.size)
        assertEquals(7, snapshot.omitted)
        assertTrue(snapshot.toJson().contains(""""omitted":7"""))
    }

    @Test
    fun `omitted is published at zero too`() {
        // A reader of the document has to tell "nobody was left out" from "this
        // build does not say", and only the member's presence does that.
        val readers = AuthedReaders("t")
        readers.signedIn(1, alice)

        assertEquals(
            """{"pubkeys":["$alice"],"count":1,"omitted":0}""",
            readers.snapshot().toJson(),
        )
    }

    @Test
    fun `an empty relay serves an empty list rather than nothing`() {
        assertEquals("""{"pubkeys":[],"count":0,"omitted":0}""", AuthedReaders("t").snapshot().toJson())
    }

    @Test
    fun `only the exact bearer token authorizes`() {
        val readers = AuthedReaders("s3cret")

        assertTrue(readers.authorizes("Bearer s3cret"))
        assertFalse(readers.authorizes("Bearer s3cre"), "a prefix is not the token")
        assertFalse(readers.authorizes("Bearer s3cretx"))
        assertFalse(readers.authorizes("bearer s3cret"), "the scheme is case-sensitive as NIP-98 and OAuth spell it")
        assertFalse(readers.authorizes("s3cret"), "a bare token is not a credential header")
        assertFalse(readers.authorizes(""))
        assertFalse(readers.authorizes(null), "a request with no header at all")
    }

    @Test
    fun `it is a connection listener, so the engine's own disconnect ends presence`() {
        // Stated as a type check because the wiring is the whole mechanism:
        // nothing else in this process learns that a socket closed, and a
        // registry that was not a listener could only expire presence on a
        // timeout — i.e. mirror for people who left.
        val readers: RelayServerListener = AuthedReaders("t")
        readers.onConnect(1)
        readers.onDisconnect(1)
    }
}
