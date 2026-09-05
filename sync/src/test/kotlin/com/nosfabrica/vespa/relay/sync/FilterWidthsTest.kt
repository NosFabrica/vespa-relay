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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Width learning from refusals: every path narrows strictly or declines to narrow, so it terminates. */
class FilterWidthsTest {
    private val url = RelayUrlNormalizer.normalize("wss://purplerelay.com")
    private val other = RelayUrlNormalizer.normalize("wss://mostro-p2p.tech")

    @Test
    fun `a relay that states its limit is taken at its word`() {
        assertEquals(100, FilterWidths.capFrom("invalid: too many kinds (max 100)", kindsAsked = 139))
    }

    @Test
    fun `a relay that quotes our own width back is halved, not believed`() {
        // The number quoted is our own width; adopted as a cap it would re-send the same REQ.
        assertEquals(69, FilterWidths.capFrom("ERROR: bad req: filter validation failed: too many kinds in filter: 139", kindsAsked = 139))
        assertEquals(69, FilterWidths.capFrom("error: too many kinds in filter", kindsAsked = 139))
    }

    @Test
    fun `a refusal that is not about width teaches nothing`() {
        for (said in listOf(
            "auth-required: we only serve authenticated users",
            "restricted: not on this relay's allowlist",
            "rate-limited: slow down",
            "error: too many filters",
            "blocked: you are not welcome here",
        )) {
            assertNull(FilterWidths.capFrom(said, kindsAsked = 139), said)
        }
    }

    @Test
    fun `halving bottoms out at one kind and stops`() {
        val widths = FilterWidths()
        val said = "error: too many kinds in filter"
        val steps = mutableListOf<Int>()
        var asked = 139
        while (widths.learn(url, said, asked)) {
            asked = widths.capFor(url)!!
            steps += asked
        }
        assertEquals(listOf(69, 34, 17, 8, 4, 2, 1), steps)
        assertEquals(FilterWidths.MIN_CAP, widths.capFor(url))
        assertFalse(widths.learn(url, said, kindsAsked = 1), "one kind refused is not a width this router can narrow")
    }

    @Test
    fun `a cap already held is not news, and a wider one never replaces it`() {
        // `learn` returning true re-walks the leg, so a repeat answering true would loop.
        val widths = FilterWidths()
        assertTrue(widths.learn(url, "invalid: too many kinds (max 100)", kindsAsked = 139))
        assertFalse(widths.learn(url, "invalid: too many kinds (max 100)", kindsAsked = 139), "the same cap again is not news")
        assertFalse(widths.learn(url, "invalid: too many kinds (max 120)", kindsAsked = 139), "a wider cap never widens what we learned")
        assertTrue(widths.learn(url, "invalid: too many kinds (max 20)", kindsAsked = 100), "…and a narrower one does")
        assertEquals(20, widths.capFor(url))
    }

    @Test
    fun `nothing said is nothing learned`() {
        // A relay that NOTICEs and never EOSEs leaves no sentence dated to the walk.
        val widths = FilterWidths()
        assertFalse(widths.learn(url, said = null, kindsAsked = 139))
        assertNull(widths.capFor(url))
    }

    @Test
    fun `the cap is the relay's, and it splits kinds and nothing else`() {
        val widths = FilterWidths()
        val leg = Filter(kinds = (1..250).toList(), authors = listOf("a".repeat(64)), since = 1_700_000_000)
        assertEquals(listOf(leg), widths.chunk(url, leg))

        assertTrue(widths.learn(url, "invalid: too many kinds (max 100)", kindsAsked = 250))
        val chunks = widths.chunk(url, leg)
        assertEquals(listOf(100, 100, 50), chunks.map { it.kinds!!.size }, "kinds in chunks of what the relay takes")
        assertEquals((1..250).toList(), chunks.flatMap { it.kinds!! }, "every kind asked for, in order, exactly once")
        assertTrue(chunks.all { it.authors == leg.authors && it.since == leg.since }, "the rest of the ask is untouched")

        assertEquals(listOf(leg), widths.chunk(other, leg))
        assertEquals(1, widths.narrowed)
    }

    @Test
    fun `a sweep window is split by kinds and by nothing else`() {
        // A chunk that moved `since` or `until` would compare different ground.
        val widths = FilterWidths()
        val window = Filter(kinds = (1..250).toList(), since = 1_600_000_000, until = 1_700_000_000)
        assertEquals(listOf(window), widths.chunk(url, window), "untouched until the relay complains")

        assertTrue(widths.learn(url, "invalid: too many kinds (max 100)", kindsAsked = 250))
        val chunks = widths.chunk(url, window)
        assertEquals(listOf(100, 100, 50), chunks.map { it.kinds!!.size })
        assertTrue(
            chunks.all { it.since == window.since && it.until == window.until },
            "the window is untouched — only the kinds are split",
        )
    }

    @Test
    fun `an ask that already fits, or names no kinds, is one REQ`() {
        val widths = FilterWidths()
        assertTrue(widths.learn(url, "invalid: too many kinds (max 100)", kindsAsked = 139))
        val fits = Filter(kinds = listOf(0, 3, 10002))
        assertEquals(listOf(fits), widths.chunk(url, fits))
        val unkinded = Filter(authors = listOf("a".repeat(64)))
        assertEquals(listOf(unkinded), widths.chunk(url, unkinded))
    }
}
