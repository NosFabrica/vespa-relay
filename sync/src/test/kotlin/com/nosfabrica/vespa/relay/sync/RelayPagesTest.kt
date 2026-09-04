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
 * The sample an abort renders: it says which part of the ask the page failed,
 * and an idle sampler claims nothing.
 */
class RelayPagesTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val anchor = 1_788_000_000L

    private fun sample(
        asked: Filter,
        of: NormalizedRelayUrl = url,
    ) = RelayPages.Sample(of, asked)

    private val kind1Below = Filter(kinds = listOf(1), until = anchor)

    /** The real slot logic, over a client that will never deliver a message. */
    private fun pages() = ClientRelayPages(EmptyNostrClient())

    @Test
    fun `a page of the wrong kind is named as the wrong kind`() {
        // The page matches nothing, so the walk ends UNPAGEABLE; the sentence tells an off-kind page from a cursor fault.
        val s = sample(kind1Below)
        repeat(3) { s.add("sub1", kind = 7, createdAt = anchor - it) }
        val said = assertNotNull(s.render(downloaded = 0))

        assertTrue("3 off-kind" in said, said)
        assertTrue("k7@" in said, said)
        assertTrue("above the `until`" !in said, said)
    }

    @Test
    fun `a page above the cursor is named as above the cursor`() {
        val s = sample(kind1Below)
        repeat(2) { s.add("sub1", kind = 1, createdAt = anchor + 500 + it) }
        val said = assertNotNull(s.render(downloaded = 0))

        assertTrue("2 above the `until`" in said, said)
        assertTrue("off-kind" !in said, said)
    }

    @Test
    fun `a page that matches the ask perfectly says exactly that`() {
        // A page that matched everything, which quartz still counted as nothing, is our side of the walk.
        val s = sample(Filter(kinds = listOf(1), since = anchor - 100, until = anchor))
        s.add("sub1", kind = 1, createdAt = anchor - 10)
        val said = assertNotNull(s.render(downloaded = 0))

        assertTrue("MATCHING the ask" in said, said)
        assertTrue("OUR side of the walk" in said, said)
    }

    @Test
    fun `the same page under a walk that DID download says the ordinary thing`() {
        // The sharp reading is only true at zero; the live probe renders on a walk that downloaded.
        val s = sample(kind1Below)
        s.add("sub1", kind = 1, createdAt = anchor - 10)
        val said = assertNotNull(s.render(downloaded = 158))

        assertTrue("matching the ask" in said, said)
        assertTrue("OUR side of the walk" !in said, said)
    }

    @Test
    fun `the subscriptions are reported, because a page carrying somebody else's is its own finding`() {
        // The walk's own subscription id is not knowable from a connection listener, so ids are printed, not filtered on.
        val s = sample(kind1Below)
        s.add("walk", kind = 1, createdAt = anchor - 1)
        s.add("tail", kind = 1, createdAt = anchor - 2)
        val said = assertNotNull(s.render(downloaded = 0))

        assertTrue("2 subscription(s)" in said, said)
        assertTrue("walk" in said && "tail" in said, said)
    }

    @Test
    fun `nothing arriving is not a finding and says nothing at all`() {
        // A socket that carried nothing is not evidence about the relay.
        assertNull(sample(Filter(kinds = listOf(1))).render(downloaded = 0))
    }

    @Test
    fun `the counts are over every event, not over the handful kept for display`() {
        // The rows are a sample; the counts are not.
        val s = sample(kind1Below)
        repeat(5) { s.add("sub1", kind = 1, createdAt = anchor - it) }
        repeat(45) { s.add("sub1", kind = 7, createdAt = anchor - 100 - it) }
        val said = assertNotNull(s.render(downloaded = 0))

        assertTrue("45 off-kind" in said, said)
        assertTrue("MATCHING the ask" !in said, "the first five matched; the page did not: $said")
    }

    @Test
    fun `the sample is bounded, and says how many it did not print`() {
        val s = sample(kind1Below)
        repeat(50) { s.add("sub1", kind = 7, createdAt = anchor - it) }
        val said = assertNotNull(s.render(downloaded = 0))

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
        // Several streams visit one relay over one socket; a second sampler would collect the first walk's events.
        val pages = pages()
        val first = assertNotNull(pages.arm(url, kind1Below))
        assertNull(pages.arm(url, kind1Below), "the relay's slot is taken")
        assertNotNull(pages.arm(other, kind1Below), "…and it is per relay, not global")

        pages.free(first)
        assertNotNull(pages.arm(url, kind1Below), "freed, so the next walk of this relay samples")
    }

    @Test
    fun `freeing a slot a later walk already took does not steal it`() {
        // The identity check in `free`: a slow walk must not drop a slot somebody else holds.
        val pages = pages()
        val stale = assertNotNull(pages.arm(url, kind1Below))
        pages.free(stale)
        val current = assertNotNull(pages.arm(url, kind1Below))

        pages.free(stale)

        assertNull(pages.arm(url, kind1Below), "the current walk still holds it")
        pages.free(current)
        assertNotNull(pages.arm(url, kind1Below))
    }
}
