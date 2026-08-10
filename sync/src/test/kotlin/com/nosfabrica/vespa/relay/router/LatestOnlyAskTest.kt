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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * `latestOnly` is the only thing that saves bandwidth on the fetch path, where
 * a REQ streams bodies without ever naming an id first. Its correctness is
 * entirely about what it refuses to touch.
 */
class LatestOnlyAskTest {
    private val authors = (1..5).map { "%064x".format(it) }

    @Test
    fun `a replaceable ask becomes one limit-1 filter per address`() {
        val batches = LatestOnlyAsk.decompose(Filter(kinds = listOf(0), authors = authors), maxFiltersPerReq = 100)
        val filters = batches.flatten()
        assertEquals(5, filters.size)
        assertTrue(filters.all { it.limit == 1 }, "a bare limit on a multi-author filter is newest-first GLOBALLY")
        assertTrue(filters.all { it.authors?.size == 1 })
        assertEquals(authors.toSet(), filters.mapNotNull { it.authors?.single() }.toSet())
    }

    @Test
    fun `several replaceable kinds each get their own per-author ask`() {
        val filters = LatestOnlyAsk.decompose(Filter(kinds = listOf(0, 10002), authors = authors), 100).flatten()
        assertEquals(10, filters.size)
        assertTrue(filters.all { it.kinds?.size == 1 })
    }

    @Test
    fun `regular kinds pass through untouched rather than erroring`() {
        // A flag that threw on kind 1 would make a mixed-kind stream
        // unconfigurable, which is worse than a no-op.
        val f = Filter(kinds = listOf(1), authors = authors)
        assertFalse(LatestOnlyAsk.applies(f))
        assertEquals(listOf(listOf(f)), LatestOnlyAsk.decompose(f, 100))
    }

    @Test
    fun `a mixed ask decomposes the replaceable kinds and carries the rest as one filter`() {
        val filters = LatestOnlyAsk.decompose(Filter(kinds = listOf(0, 1), authors = authors), 100).flatten()
        assertEquals(6, filters.size, "five per-author kind-0 asks plus one untouched kind-1 ask")
        assertEquals(1, filters.count { it.kinds == listOf(1) })
        assertEquals(null, filters.single { it.kinds == listOf(1) }.limit, "the passthrough keeps its own shape")
    }

    @Test
    fun `addressable kinds pass through, because one limit-1 per author would drop every other d`() {
        // The honest limit of this technique: asking per `d` needs the `d`
        // values, which are only knowable from a corpus we may not have.
        val f = Filter(kinds = listOf(30_382), authors = authors)
        assertFalse(LatestOnlyAsk.applies(f))
        assertEquals(listOf(listOf(f)), LatestOnlyAsk.decompose(f, 100))
    }

    @Test
    fun `a filter with no authors passes through, because there is nothing to decompose on`() {
        val f = Filter(kinds = listOf(0))
        assertFalse(LatestOnlyAsk.applies(f))
        assertEquals(listOf(listOf(f)), LatestOnlyAsk.decompose(f, 100))
    }

    @Test
    fun `batches respect the relay's max_filters`() {
        // NIP-11 advertises it and relays enforce it, so a stream with thousands
        // of authors has to become many REQs rather than one refusal.
        val many = (1..250).map { "%064x".format(it) }
        val batches = LatestOnlyAsk.decompose(Filter(kinds = listOf(0), authors = many), maxFiltersPerReq = 20)
        assertEquals(13, batches.size)
        assertTrue(batches.all { it.size <= 20 })
        assertEquals(250, batches.sumOf { it.size })
    }

    @Test
    fun `the time bounds are preserved, so the ask still composes with a band leg`() {
        val filters =
            LatestOnlyAsk
                .decompose(Filter(kinds = listOf(0), authors = authors, since = 100, until = 200), 100)
                .flatten()
        assertTrue(filters.all { it.since == 100L && it.until == 200L })
    }
}
