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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * What the rewrite is FOR is how the grammar downstream reads the result, so
 * every case asserts through quartz's `SearchQuery.parse` rather than against
 * the rewritten string. The mechanism (quoting) is free to change; "a dash-led
 * literal is required text and never an exclusion" is not.
 */
class SearchLiteralsTest {
    /** How the store's parser reads a string once the rewrite has been over it. */
    private fun parse(typed: String) = SearchQuery.parse(quoteDashedLiterals(typed))

    @Test
    fun `a dash-decorated literal is required text, not an exclusion`() {
        val typed = "--------------06:30--------------"
        val q = parse(typed)
        assertEquals(listOf(typed), q.phrases, "the literal survives verbatim as a requirement")
        assertEquals(emptyList(), q.notTerms, "nothing was excluded — this is the bug in one line")
        assertTrue(q.hasText, "a query that asks for text must not fall through to plain recall")
    }

    @Test
    fun `one dash is still the exclusion operator`() {
        val q = parse("bitcoin -spam")
        assertEquals("bitcoin", q.terms)
        assertEquals(listOf("spam"), q.notTerms, "NIP-50's minus keeps working")
        assertEquals(emptyList(), q.phrases)
    }

    @Test
    fun `a lone dash stays an ordinary term`() {
        assertEquals("-", parse("-").terms, "quartz treats it as text and so must this")
    }

    @Test
    fun `a phrase exclusion is left alone`() {
        val q = parse("""-"nostr apps"""")
        assertEquals(listOf("nostr apps"), q.notPhrases)
        assertEquals(emptyList(), q.phrases, "the negative span must not be flipped positive")
    }

    @Test
    fun `quoted spans pass through untouched, dashes and all`() {
        val q = parse(""""--------------06:30--------------" bitcoin""")
        assertEquals(listOf("--------------06:30--------------"), q.phrases, "already a phrase; nothing to do")
        assertEquals("bitcoin", q.terms)
    }

    @Test
    fun `an unclosed span still swallows the rest, as the grammar says`() {
        // The rewrite must not "helpfully" close it: an unclosed span runs to the
        // end of the string, and a dash-led token INSIDE it is already literal.
        val q = parse(""""06:30 --------------""")
        assertEquals(listOf("06:30 --------------"), q.phrases)
        assertEquals("", q.terms)
    }

    @Test
    fun `extensions survive beside a rewritten literal`() {
        val q = parse("--------------06:30-------------- include:spam sort:rank")
        assertEquals(listOf("--------------06:30--------------"), q.phrases)
        assertTrue(q.includeSpam, "include:spam still reaches the store")
        assertEquals("rank", q.extensions["sort"])
        assertEquals("", q.terms, "an extension never becomes a term")
    }

    @Test
    fun `a stray dash run beside real words costs nothing`() {
        // Quoting THIS would make the whole query unsatisfiable — a phrase with
        // no token an index can hold is provably-no-match — so a fat-fingered
        // trailing dash run must keep dropping out the way it does today.
        val q = parse("bitcoin --")
        assertEquals("bitcoin", q.terms)
        assertEquals(emptyList(), q.phrases, "nothing unsatisfiable was added")
        assertEquals(emptyList(), q.notTerms)
    }

    @Test
    fun `a query of nothing but dashes matches nothing rather than everything`() {
        // The whole query is punctuation. Left as syntax it parses to no terms
        // at all, and the read falls through to plain recall — the relay answers
        // a text search with the newest events in the store. As a phrase the
        // index cannot hold, it is provably-no-match instead.
        val q = parse("----")
        assertEquals(listOf("----"), q.phrases)
        assertTrue(q.hasText, "the query still ASKS for text; it just cannot be satisfied")
    }

    @Test
    fun `a search with nothing to rewrite comes back as the same list`() {
        val plain = listOf(Filter(search = "bitcoin -spam"), Filter(kinds = listOf(1)))
        assertSame(plain, plain.quotingDashedLiterals(), "the common path must not allocate")
        val noSearch = listOf(Filter(kinds = listOf(1)), Filter(authors = listOf("ab".repeat(32))))
        assertSame(noSearch, noSearch.quotingDashedLiterals())
    }

    @Test
    fun `only the filters that carry a literal are rewritten`() {
        val untouched = Filter(kinds = listOf(1), search = "06:30")
        val rewritten = Filter(kinds = listOf(1), search = "--------------06:30--------------")
        val out = listOf(untouched, rewritten).quotingDashedLiterals()
        assertSame(untouched, out[0], "a filter with nothing to change keeps its identity")
        assertEquals(listOf("--------------06:30--------------"), SearchQuery.parse(out[1].search).phrases)
        assertEquals(listOf(1), out[1].kinds, "the rest of the filter is carried")
    }
}
