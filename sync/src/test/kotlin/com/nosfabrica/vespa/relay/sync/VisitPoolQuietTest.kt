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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** Silence ends a visit; a relay answering every ask, even with nothing, is not silent. */
class VisitPoolQuietTest {
    private val url = RelayUrlNormalizer.normalize("wss://quiet.example")

    @Test
    fun `a unit of many empty but answered asks finishes and reaches its tail`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val stream = PoolFixture.stream("authors", url, Filter(kinds = listOf(1)))
            val asks = (0 until 40).map { RosterBuilder.Ask(stream, Filter(kinds = listOf(1), authors = listOf("%064x".format(it)))) }
            val roster =
                RosterBuilder.Roster(
                    asks = mapOf(url to mapOf(stream.name to RosterBuilder.UnitAsks(asks, asks.map { it.filter.toJson() }.toSet()))),
                    sharedAuthors = emptyMap(),
                )
            // Each ask is answered, empty, well inside the idle window; together they outlast the quiet limit.
            val fixture =
                PoolFixture(scope, listOf(stream), answer = { _, _ ->
                    delay(15)
                    PagedFetchResult(0, PagedFetchResult.End.DRAINED)
                })
            try {
                fixture.pool(quietGiveUpMs = 200, roster = { roster }).start()
                fixture.awaitTail()
                assertEquals(40, fixture.asked.size, "every ask was asked")
                assertEquals(0L, fixture.counts()["abortedGaveUp"], "and none of the answers read as silence")
            } finally {
                scope.cancel()
            }
        }
}
