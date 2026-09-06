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

import kotlin.test.Test
import kotlin.test.assertEquals

/** The cases are the strings the JDK, OkHttp and quartz produce; unrecognised text goes to UNKNOWN. */
class SilenceTest {
    @Test
    fun `the five causes Unreachability already proves are told apart`() {
        assertEquals(Silence.NAME, Silence.of("cannot: java.net.UnknownHostException: gone.example"))
        assertEquals(Silence.NAME, Silence.of("cannot: Unable to resolve host \"gone.example\""))
        assertEquals(Silence.NAME, Silence.of("cannot: gone.example: Name or service not known"))
        assertEquals(Silence.REFUSED, Silence.of("cannot: java.net.ConnectException: Connection refused"))
        assertEquals(Silence.NO_ROUTE, Silence.of("cannot: java.net.NoRouteToHostException: No route to host"))
        assertEquals(Silence.NO_ROUTE, Silence.of("cannot: connect: Network is unreachable"))
        assertEquals(Silence.TLS, Silence.of("cannot: javax.net.ssl.SSLHandshakeException: PKIX path building failed"))
        assertEquals(Silence.TLS, Silence.of("cannot: Trust anchor for certification path not found"))
        assertEquals(Silence.TIMEOUT, Silence.of("cannot: java.net.SocketTimeoutException: connect timed out"))
        assertEquals(Silence.TIMEOUT, Silence.of("cannot: timeout"))
    }

    @Test
    fun `the strings a live pass actually produced`() {
        // Quartz's `WebSocket Failure: <message> (<ExceptionName>)`.
        assertEquals(
            Silence.REFUSED,
            Silence.of("cannot:WebSocket Failure: Failed to connect to localhost/127.0.0.1:1 (ConnectException)"),
            "the message says nothing; the exception name in the suffix is the whole evidence",
        )
        assertEquals(
            Silence.UPGRADE,
            Silence.of("cannot:WebSocket Failure: Unexpected response code for CONNECT: 502 (IOException)"),
            "reachable, and what answered is not a websocket",
        )
        assertEquals(
            Silence.UPGRADE,
            Silence.of("cannot:WebSocket Failure: Expected HTTP 101 response but was '404 Not Found'"),
        )
    }

    @Test
    fun `the specific pattern wins over the general one`() {
        // First hit wins, so the order of the table is part of its meaning.
        assertEquals(Silence.RATE_LIMITED, Silence.of("cannot: 429 too many requests, retry after timeout"))
        // A connect timeout surfaces as ConnectException on some platforms, so the timeout outranks the class name.
        assertEquals(Silence.TIMEOUT, Silence.of("cannot: java.net.ConnectException: connect timed out"))
        assertEquals(Silence.TIMEOUT, Silence.of("cannot: failed to connect after 10000ms: connect timed out"))
        assertEquals(Silence.REFUSED, Silence.of("CANNOT: CONNECTION REFUSED"))
    }

    @Test
    fun `text this table does not recognise is counted, never forced`() {
        assertEquals(Silence.UNKNOWN, Silence.of("cannot: something nobody has seen yet"))
        assertEquals(Silence.UNKNOWN, Silence.of(""))
        assertEquals(Silence.UNKNOWN, Silence.of(null))
    }
}
