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
package com.nosfabrica.vespa.relay.util

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A scheme's default port names the same endpoint as no port, and nothing else does. Quartz's
 * normalizer folds it since 2.9.0; every relay url here goes through it, so this pins that.
 */
class DefaultPortTest {
    private fun folded(raw: String) = RelayUrlNormalizer.normalize(raw).url

    @Test
    fun `each scheme's own default port folds away`() {
        assertEquals("wss://relay.example/", folded("wss://relay.example:443"))
        assertEquals("ws://relay.example/alpha", folded("ws://relay.example:80/alpha"))
        assertEquals("wss://relay.example/", folded("wss://relay.example"))
    }

    @Test
    fun `any other port is a different endpoint`() {
        assertEquals("wss://relay.example:80/", folded("wss://relay.example:80"))
        assertEquals("ws://relay.example:443/", folded("ws://relay.example:443"))
        assertEquals("wss://relay.example:8443/", folded("wss://relay.example:8443"))
    }

    @Test
    fun `an IPv6 literal keeps its address and loses only a port`() {
        assertEquals("ws://[2602:f36a:1::1]/alpha", folded("ws://[2602:f36a:1::1]:80/alpha"))
        assertEquals("ws://[2602:f36a:1::80]/", folded("ws://[2602:f36a:1::80]"))
    }

    @Test
    fun `an onion's spelled-out port is the same address`() {
        assertEquals(folded("ws://abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion"), folded("ws://abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion:80"))
    }
}
