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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The host side's small decision: who takes gzip. Which body is
 * which command and which frame is which status are quartz's (HttpRelayHandlerTest there).
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
}
