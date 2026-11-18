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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SweeperTest {
    private val author = TestKey()
    private val now = 1_800_000_000L

    /** 50 matching notes a day apart, from the start of 2024. */
    private val notes = (0 until 50).map { event(author, 1, "nsec1 talk $it", 1_704_067_200L + it * 86_400L) }

    /** Answers no COUNT for a window wider than [maxCountableSpan] seconds, like a relay under load. */
    private class Flaky(
        private val inner: Corpus,
        private val maxCountableSpan: Long,
    ) : Corpus {
        var refused = 0

        override suspend fun fetch(filters: List<Filter>) = inner.fetch(filters)

        override suspend fun count(filter: Filter): Int? {
            val span = (filter.until ?: Long.MAX_VALUE) - (filter.since ?: 0)
            return if (span > maxCountableSpan) null.also { refused++ } else inner.count(filter)
        }
    }

    private fun sweep(
        corpus: Corpus,
        pageCap: Int,
    ): Pair<List<Event>, Sweeper> {
        val got = mutableListOf<Event>()
        val sweeper = Sweeper(corpus, pageCap, now) {}
        runBlocking { sweeper.sweep(Filter(search = "nsec1")) { got += it } }
        return got to sweeper
    }

    @Test
    fun `every match is read past the page cap, each once`() {
        val (got, sweeper) = sweep(InMemoryCorpus(notes), pageCap = 4)
        assertEquals(notes.map { it.id }.toSet(), got.map { it.id }.toSet())
        assertEquals(notes.size, got.size)
        assertTrue(sweeper.gaps.isEmpty())
    }

    @Test
    fun `a window the relay will not count is split until it will, and nothing is lost`() {
        val flaky = Flaky(InMemoryCorpus(notes), maxCountableSpan = 30L * 86_400)
        val (got, sweeper) = sweep(flaky, pageCap = 4)

        assertTrue(flaky.refused > 0, "the fixture must refuse some COUNTs")
        assertEquals(notes.map { it.id }.toSet(), got.map { it.id }.toSet())
        assertTrue(sweeper.gaps.isEmpty())
    }

    @Test
    fun `a relay that never counts is still read, and a full unsized page is a gap`() {
        val crowded = (0 until 6).map { event(author, 1, "nsec1 burst $it", 1_704_067_200L + it) }
        val (got, sweeper) = sweep(Flaky(InMemoryCorpus(crowded), maxCountableSpan = -1), pageCap = 4)

        assertEquals(4, got.size)
        assertEquals(1, sweeper.gaps.size)
    }
}
