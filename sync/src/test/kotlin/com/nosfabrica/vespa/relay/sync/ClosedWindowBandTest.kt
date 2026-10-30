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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A walk read to the end of a closed window covered the whole window, events or none: a
 * past-only stream settles every relay that answers it, and owes none of them another ask.
 */
class ClosedWindowBandTest {
    private val url = RelayUrlNormalizer.normalize("wss://closed.example")
    private val day = 86_400L

    private fun now() = System.currentTimeMillis() / 1000

    private val until = now() - 30 * day

    private val mutesAndReports = Filter(kinds = listOf(1984, 10000), until = until)

    private fun owed(
        filter: Filter,
        answer: suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult,
    ): Pair<SyncBands, List<Filter>> =
        runBlocking {
            val stream = PoolFixture.stream("past", url, filter)
            val key = SyncBands.coverageKeys(stream).single()
            val bands = SyncBands(null)
            PoolFixture.visitOnce(listOf(stream), bands, answer)
            bands to bands.legs(key, url, filter)
        }

    @Test
    fun `a relay holding none of the kinds is settled by its empty answer`() {
        val (bands, legs) = owed(mutesAndReports) { _, _ -> PagedFetchResult(0, PagedFetchResult.End.DRAINED) }

        val band = assertNotNull(bands.band("past", url, mutesAndReports), "an EOSE on an empty page is an answer, and it is recorded")
        assertTrue(band.complete, "nothing below the floor either")
        assertEquals(emptyList(), legs, "and the relay owes this stream nothing more")
    }

    @Test
    fun `a relay whose newest event is older than until owes no newer leg`() {
        val corpus =
            listOf(
                PoolFixture.event(1, until - 5 * day, kind = 1984),
                PoolFixture.event(2, until - 9 * day, kind = 10000),
                PoolFixture.event(3, until - 40 * day, kind = 1984),
            )
        val (_, legs) = owed(mutesAndReports, PoolFixture.holding(corpus))

        assertEquals(emptyList(), legs, "the window (newest, until] was read with the rest of the walk")
    }

    @Test
    fun `a kind the relay holds none of is settled beside one it holds`() {
        val corpus = listOf(PoolFixture.event(1, until - 5 * day, kind = 1984))
        val (bands, legs) = owed(mutesAndReports, PoolFixture.holding(corpus))

        val band = assertNotNull(bands.band("past", url, mutesAndReports))
        assertEquals(setOf(1984, 10000), band.spans.keys, "each kind the walk asked for carries what it proved")
        assertEquals(emptyList(), legs)
    }

    @Test
    fun `an open window still resumes from the newest event it saw`() {
        val notes = Filter(kinds = listOf(1))
        val newest = now() - 3 * day
        val (_, legs) = owed(notes, PoolFixture.holding(listOf(PoolFixture.event(1, newest), PoolFixture.event(2, newest - day))))

        assertEquals(listOf(newest), legs.map { it.since }, "late arrivals below the present are why the forward edge is the newest event")
    }

    @Test
    fun `an ask bounded in the past holds no live tail`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, listOf(PoolFixture.stream("past", url, mutesAndReports)))
                val pool = fixture.pool()
                pool.start()
                fixture.awaitVisit(pool)
                assertTrue(fixture.tails.isEmpty(), "nothing live can match it, so it takes no live permit")
            } finally {
                scope.cancel()
            }
        }
}
