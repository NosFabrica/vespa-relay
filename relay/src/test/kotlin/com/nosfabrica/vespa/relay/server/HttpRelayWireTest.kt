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

import io.ktor.http.HttpStatusCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The HTTP reads' small decisions: which body is filters, which prefix is which status, who takes gzip. */
class HttpReadsWireTest {
    @Test
    fun `a body is one filter or a non-empty array of them`() {
        assertEquals(1, parseFilters("""{"kinds":[1]}""".encodeToByteArray())?.size)
        assertEquals(2, parseFilters("""[{"kinds":[1]},{"ids":[]}]""".encodeToByteArray())?.size)
        for (bad in listOf("", "[]", "[1]", """[{"kinds":[1]},"x"]""", "null", "\"REQ\"", "{", """{"a":1} trailing""")) {
            assertNull(parseFilters(bad.encodeToByteArray()), "'$bad' is not filters")
        }
    }

    @Test
    fun `a refusal's machine-readable prefix picks the status`() {
        assertEquals(HttpStatusCode.Unauthorized, statusFor("auth-required: sign in"))
        assertEquals(HttpStatusCode.Forbidden, statusFor("restricted: not for you"))
        assertEquals(HttpStatusCode.Forbidden, statusFor("blocked: banned"))
        assertEquals(HttpStatusCode.TooManyRequests, statusFor("rate-limited: slow down"))
        assertEquals(HttpStatusCode.InternalServerError, statusFor("error: store failed"))
        assertEquals(HttpStatusCode.BadRequest, statusFor("invalid: too many filters"))
        assertEquals(HttpStatusCode.BadRequest, statusFor("no prefix at all"))
    }

    @Test
    fun `gzip is taken when named or wildcarded, and not at q=0`() {
        assertTrue(acceptsGzip("gzip"))
        assertTrue(acceptsGzip("br, gzip;q=0.5, deflate"))
        assertTrue(acceptsGzip("*"))
        assertTrue(acceptsGzip("GZIP"))
        assertFalse(acceptsGzip(null))
        assertFalse(acceptsGzip("deflate, br"))
        assertFalse(acceptsGzip("gzip;q=0"))
        assertFalse(acceptsGzip("gzip; q=0.0"))
    }

    @Test
    fun `a CLOSED frame escapes its reason`() {
        assertEquals("""["CLOSED","http","error: a \"quoted\" reason"]""", closedFrame("error: a \"quoted\" reason"))
    }
}
