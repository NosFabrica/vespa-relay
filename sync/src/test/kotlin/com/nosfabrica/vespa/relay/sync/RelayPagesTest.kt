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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * THE PAGE AN ABORT COULD NOT NAME — #187's instrument.
 *
 * `VisitPool.refusedOutright` aborts on `downloaded == 0`, so every
 * `abortedUnpageable` is a walk in which no event matched the ask. The pool's
 * own `onEvent` therefore sees nothing, and the events that DID arrive are
 * discarded by the very match that produced the abort. These pin that the
 * sample says which part of the ask went unhonoured, and that an idle sampler
 * costs nothing and claims nothing.
 */
class RelayPagesTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val anchor = 1_788_000_000L

    private fun sample(of: NormalizedRelayUrl = url) = RelayPages.Sample(of)

    /** The real slot logic, over a client that will never deliver a message. */
    private fun pages() = ClientRelayPages(EmptyNostrClient())

    @Test
    fun `a page of the wrong kind is named as the wrong kind`() {
        // The ask carries kinds; the relay answers with something else. Under
        // quartz that page matches nothing, `downloaded` stays 0, and the walk
        // ends UNPAGEABLE — a word about paging for a fault that has nothing to
        // do with the cursor. The sentence is what tells them apart.
        val s = sample()
        repeat(3) { s.add("sub1", kind = 7, createdAt = anchor - it) }
        val said = assertNotNull(s.render(Filter(kinds = listOf(1), until = anchor)))

        assertTrue("3 off-kind" in said, said)
        assertTrue("k7@" in said, said)
        assertTrue("above the `until`" !in said, said)
    }

    @Test
    fun `a page above the cursor is named as above the cursor`() {
        val s = sample()
        repeat(2) { s.add("sub1", kind = 1, createdAt = anchor + 500 + it) }
        val said = assertNotNull(s.render(Filter(kinds = listOf(1), until = anchor)))

        assertTrue("2 above the `until`" in said, said)
        assertTrue("off-kind" !in said, said)
    }

    @Test
    fun `a page that matches the ask perfectly says exactly that`() {
        // THE READING THAT MATTERS MOST, and the one a naive line would make
        // impossible: a page that matched everything asked, which quartz still
        // counted as nothing downloaded, is not a relay misbehaving — it is our
        // side of the walk, and an operator must be able to tell.
        val s = sample()
        s.add("sub1", kind = 1, createdAt = anchor - 10)
        val said = assertNotNull(s.render(Filter(kinds = listOf(1), since = anchor - 100, until = anchor)))

        assertTrue("MATCHING the ask" in said, said)
    }

    @Test
    fun `the subscriptions are reported, because a page carrying somebody else's is its own finding`() {
        // Several streams share one socket. The walk's own subscription id is
        // not knowable from a connection listener — `fetchAllPages` mints its
        // own — so the ids are printed rather than filtered on: one id is this
        // walk, several means the socket's other traffic landed inside the ask,
        // which is a different fault with a different fix.
        val s = sample()
        s.add("walk", kind = 1, createdAt = anchor - 1)
        s.add("tail", kind = 1, createdAt = anchor - 2)
        val said = assertNotNull(s.render(Filter(kinds = listOf(1), until = anchor)))

        assertTrue("2 subscription(s)" in said, said)
        assertTrue("walk" in said && "tail" in said, said)
    }

    @Test
    fun `nothing arriving is not a finding and says nothing at all`() {
        // A socket that carried no event is not evidence about the relay. "sent
        // 0 events" would read as one, so the line is simply absent — the same
        // rule the monitor's own passes follow for a measurement they did not
        // take.
        assertNull(sample().render(Filter(kinds = listOf(1))))
    }

    @Test
    fun `the sample is bounded, and says how many it did not print`() {
        val s = sample()
        repeat(50) { s.add("sub1", kind = 7, createdAt = anchor - it) }
        val said = assertNotNull(s.render(Filter(kinds = listOf(1), until = anchor)))

        assertTrue("carried 50 event(s)" in said, "the COUNT is all of them: $said")
        assertEquals(
            RelayPages.MAX_ROWS,
            Regex("k7@").findAll(said).count(),
            "…and only ${RelayPages.MAX_ROWS} are printed: $said",
        )
    }

    // ------------------------------------------------------------------------
    // The slot.
    // ------------------------------------------------------------------------

    @Test
    fun `one walk holds a relay's slot at a time, and the loser reports nothing`() {
        // Several streams visit one relay at once over one socket. A second
        // sampler would be collecting the FIRST walk's events, and a line that
        // attributed them to the second ask would be worse than no line.
        val pages = pages()
        val first = assertNotNull(pages.arm(url))
        assertNull(pages.arm(url), "the relay's slot is taken")
        assertNotNull(pages.arm(other), "…and it is per relay, not global")

        pages.free(first)
        assertNotNull(pages.arm(url), "freed, so the next walk of this relay samples")
    }

    @Test
    fun `freeing a slot a later walk already took does not steal it`() {
        // A walk that returns slowly must not drop a slot somebody else is
        // using — the identity check in `free`. Without it the sampler would
        // silently stop working for whichever relay is busiest, which is the
        // one it exists for.
        val pages = pages()
        val stale = assertNotNull(pages.arm(url))
        pages.free(stale)
        val current = assertNotNull(pages.arm(url))

        pages.free(stale)

        assertNull(pages.arm(url), "the current walk still holds it")
        pages.free(current)
        assertNotNull(pages.arm(url))
    }
}
