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
package com.nosfabrica.vespa.relay.monitor

import java.io.EOFException
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import javax.net.ssl.SSLHandshakeException
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A negative NIP-66 record is signed and public, so only a failure proving the connection never opened may produce one. */
class UnreachabilityTest {
    private fun proves(e: Exception) = Unreachability.proves(e)

    @Test
    fun `a connection that never opened is unreachable`() {
        assertTrue(proves(UnknownHostException("relay.example: Name or service not known")))
        assertTrue(proves(UnknownHostException("relay.example: No address associated with hostname")))
        assertTrue(proves(ConnectException("connection refused")))
        assertTrue(proves(SSLHandshakeException("cert expired")))
    }

    @Test
    fun `our own resolver or route failing is not the relay's`() {
        // EAI_AGAIN is the resolver not answering, which is every host at once.
        assertFalse(proves(UnknownHostException("relay.example: Temporary failure in name resolution")))
        assertFalse(proves(ConnectException("Network is unreachable")))
        assertTrue(proves(UnknownHostException("relay.example: Name or service not known")))
        // A failed lookup the JVM answers from its cache carries the name and no reason.
        assertFalse(proves(UnknownHostException("relay.example")))
        assertTrue(Unreachability.ourSide(UnknownHostException("relay.example: Temporary failure in name resolution")))
    }

    @Test
    fun `a relay that hung up mid-transfer is not unreachable`() {
        assertFalse(proves(EOFException("stream closed")))
    }

    @Test
    fun `our own bug is never the relay's fault`() {
        assertFalse(proves(ConcurrentModificationException()))
        assertFalse(proves(NullPointerException()))
        assertFalse(proves(ClassCastException("HashMap\$Node cannot be cast")))
    }

    @Test
    fun `an unrecognised failure stays quiet`() {
        assertFalse(proves(SocketTimeoutException("read timed out")))
        assertFalse(proves(RuntimeException("something new")))
    }
}
