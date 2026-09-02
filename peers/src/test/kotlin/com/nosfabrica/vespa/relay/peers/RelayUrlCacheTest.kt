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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The memo over [RelayDiscovery.normalize]'s rules. A cache is only worth having
 * if it cannot change an answer, so every test here is about the answer and not
 * about the hit.
 */
class RelayUrlCacheTest {
    /**
     * THE ONE WAY A MEMO HERE COULD LIE, and the reason `allowOnion` is not part
     * of the key: a deployment WITH Tor and one without must get different
     * answers for the same string, in either order, from one cache.
     */
    @Test
    fun `the onion gate survives being cached under the other answer`() {
        val onion = "wss://vespaxyz2h4pnvxvxjyklnvvbfvvvvvvvvvvvvvvvvvvvvvvvvvvid.onion"

        // Clearnet first, then Tor: the cached refusal must not stick.
        val a = RelayUrlCache()
        assertNull(a.normalize(onion, allowOnion = false))
        assertNotNull(a.normalize(onion, allowOnion = true), "a cached clearnet refusal must not outlive Tor")

        // ...and the other order, which is the one that would leak an onion to
        // a resolver that cannot see it.
        val b = RelayUrlCache()
        assertNotNull(b.normalize(onion, allowOnion = true))
        assertNull(b.normalize(onion, allowOnion = false), "a cached Tor answer must not survive into a clearnet read")

        // One entry either way: the deployment's transport is not the string.
        assertEquals(1, a.size())
        assertEquals(1, b.size())
    }

    /** Every reject rule, cached and re-asked, still rejects. */
    @Test
    fun `a refusal is an answer and is cached like one`() {
        val cache = RelayUrlCache()
        val refused =
            listOf(
                "",
                "   ",
                "wss://has space.example",
                "nostr.example",
                "https://kbin.social/",
                // The normalizer REPAIRS as well as canonicalises: this passes a
                // startsWith("wss://") test and comes out an https:// web page.
                "wss://https//nostr.watch/relay/nostr.21crypto.ch",
                "ws://localhost:7777",
                "ws://127.0.0.1:7777",
            )
        for (raw in refused) {
            assertNull(cache.normalize(raw, allowOnion = false), "first look at '$raw'")
            assertNull(cache.normalize(raw, allowOnion = false), "cached look at '$raw'")
            assertNull(cache.normalize(raw, allowOnion = true), "with Tor, '$raw'")
        }
    }

    /** A repeat is the same object's value, not a re-decision. */
    @Test
    fun `the same spelling answers the same thing every time`() {
        val cache = RelayUrlCache()
        val first = cache.normalize("wss://relay.example:443", allowOnion = false)
        assertEquals("wss://relay.example/", first?.url, "the redundant :443 is stripped")
        repeat(5) { assertEquals(first, cache.normalize("wss://relay.example:443", allowOnion = false)) }
        assertEquals(1, cache.size(), "one spelling is one entry however often it arrives")
    }

    /**
     * The keys come from strangers — anyone's kind 10002 can name urls nobody
     * has seen — so the map is dropped whole at the cap rather than growing
     * with whatever a relay list invents. A miss costs the parse it was
     * avoiding; it never costs correctness.
     */
    @Test
    fun `an invented flood cannot grow the map without bound`() {
        val cache = RelayUrlCache(maxEntries = 64)
        repeat(500) { cache.normalize("wss://minted$it.example", allowOnion = false) }
        assertTrue(cache.size() <= 64, "held ${cache.size()} of 500 invented spellings")
        // And it still answers, which is the whole contract.
        assertEquals("wss://real.example/", cache.normalize("wss://real.example", allowOnion = false)?.url)
    }
}
