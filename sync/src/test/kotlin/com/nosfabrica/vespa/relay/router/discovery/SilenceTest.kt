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
package com.nosfabrica.vespa.relay.router.discovery

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The one judgement this router makes about text somebody else formats.
 *
 * `never answered a REQ` is the largest bucket a probe pass produces, and the
 * only evidence that splits it is the socket layer's own message, reaching us
 * through quartz's terminal reason for the url. Matching on that text is a
 * guess about another project's formatting, so the guesses are all in one table
 * and the cases below are the strings the JDK and OkHttp actually produce.
 *
 * The last test is the one that matters most: nothing is forced into a bucket.
 */
class SilenceTest {
    @Test
    fun `the five causes Unreachability already proves are told apart`() {
        // The same exceptions `Unreachability.proves` accepts as proving a relay
        // unreachable, in the wording they reach this layer with.
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
    fun `the specific pattern wins over the general one`() {
        // `connect timed out` is a timeout and not a refusal, and a rate limit
        // that mentions a timeout is a rate limit — first hit wins, so the order
        // of the table is part of its meaning.
        assertEquals(Silence.RATE_LIMITED, Silence.of("cannot: 429 too many requests, retry after timeout"))
        assertEquals(Silence.TIMEOUT, Silence.of("cannot: failed to connect after 10000ms: connect timed out"))
        // Case is not a contract either: the same message arrives capitalised
        // from one platform and not from another.
        assertEquals(Silence.REFUSED, Silence.of("CANNOT: CONNECTION REFUSED"))
    }

    @Test
    fun `text this table does not recognise is counted, never forced`() {
        // THE PROPERTY THE WHOLE CLASSIFIER RESTS ON. A misclassification here
        // is a claim about somebody else's server made from a string we did not
        // read properly, and the bucket that refuses to guess is what makes the
        // rest of the table safe to extend later.
        assertEquals(Silence.UNKNOWN, Silence.of("cannot: something nobody has seen yet"))
        assertEquals(Silence.UNKNOWN, Silence.of(""))
        // …and a walk that ended with the transport saying nothing at all is the
        // same "we do not know", rather than a finding of its own.
        assertEquals(Silence.UNKNOWN, Silence.of(null))
    }
}
