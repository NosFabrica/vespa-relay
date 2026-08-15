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
package com.nosfabrica.vespa.relay.router.presence

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The two silences a presence stream has to tell apart, and the one number that
 * cannot be recovered later.
 */
class AuthedFeedTest {
    private val alice = "a".repeat(64)
    private val bob = "b".repeat(64)

    @Test
    fun `before the first poll, nobody is here and we have not been told`() {
        // Different states with the same emptiness: one is a quiet night, the
        // other is a url or a token an operator has to fix, and the poller's
        // wording depends on which.
        val feed = AuthedFeed()

        assertEquals(emptyList(), feed.readers())
        assertFalse(feed.everFed())
    }

    @Test
    fun `an empty answer is still an answer`() {
        val feed = AuthedFeed()

        feed.adopt(emptyList(), omitted = 0)

        assertEquals(emptyList(), feed.readers())
        assertTrue(feed.everFed(), "a relay nobody is signed in to has spoken")
    }

    @Test
    fun `the set is stable, so an unchanged readership produces no churn`() {
        // The far end orders by connection age, which is the right cut for its
        // own bound and the wrong key here: a set that reordered every poll
        // would make every diff look like people arriving and leaving.
        val feed = AuthedFeed()

        feed.adopt(listOf(bob, alice, bob), omitted = 0)

        assertEquals(listOf(alice, bob), feed.readers())
    }

    @Test
    fun `a lost feed means nobody, not the last people we saw`() {
        // The sockets those readers were signed in ON live in the relay we can
        // no longer reach. Holding their subscriptions open is precisely the
        // "mirroring for nobody" this stream shape exists to avoid.
        val feed = AuthedFeed()
        feed.adopt(listOf(alice), omitted = 3)

        feed.clear()

        assertEquals(emptyList(), feed.readers())
        assertEquals(0, feed.omitted)
        assertTrue(feed.everFed(), "whether the feed has EVER worked is what tells a typo from an outage")
    }

    @Test
    fun `truncation is carried through, because it changes what the list means`() {
        val feed = AuthedFeed()

        feed.adopt(listOf(alice), omitted = 12)

        assertEquals(12, feed.omitted)
    }
}
