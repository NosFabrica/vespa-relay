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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The host side's small decisions: who takes gzip, which address a proxy vouches for, how a refusal made
 * here is written. Which body is which command and which frame is which status are quartz's
 * (HttpRelayHandlerTest there).
 */
class HttpRelayWireTest {
    @Test
    fun `gzip is taken when named or wildcarded, and not at q=0`() {
        assertTrue(acceptsGzip("gzip"))
        assertTrue(acceptsGzip("br, gzip;q=0.5, deflate"))
        assertTrue(acceptsGzip("*"))
        assertTrue(acceptsGzip("GZIP"))
        assertFalse(acceptsGzip(null))
        assertFalse(acceptsGzip("deflate, br"))
        assertFalse(acceptsGzip("gzip;q=0"))
        assertFalse(acceptsGzip("gzip; Q=0.0"))
        assertFalse(acceptsGzip("gzip;q=0, *"), "a named coding decides over the wildcard")
        assertFalse(acceptsGzip("*;q=0"))
    }

    @Test
    fun `an address block matches its own addresses and never resolves a name`() {
        val block = Cidr.parse("172.16.0.0/12")!!
        assertTrue(block.contains("172.20.1.2"))
        assertFalse(block.contains("172.32.0.1"))
        assertTrue(Cidr.parse("10.0.0.1")!!.contains("10.0.0.1"))
        assertFalse(Cidr.parse("10.0.0.1")!!.contains("10.0.0.2"))
        assertTrue(Cidr.parse("::1/128")!!.contains("0:0:0:0:0:0:0:1"))
        assertFalse(Cidr.parse("::1/128")!!.contains("127.0.0.1"))
        assertTrue(Cidr.parse("0.0.0.0/0")!!.contains("8.8.8.8"))
        for (bad in listOf("example.com", "999.1.1.1", "10.0.0.0/33", "10.0.0/8", "", "10.0.0.0/x")) assertNull(Cidr.parse(bad), "'$bad'")
        assertFalse(block.contains("localhost"), "a name is not an address")
    }

    @Test
    fun `a CLOSED frame escapes its reason`() {
        assertEquals("""["CLOSED","error: a \"quoted\" reason"]""", closedFrame("error: a \"quoted\" reason"))
    }
}
