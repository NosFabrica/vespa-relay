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
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/** A tail event is matched against the unit's asks by author first; the answer must be the full scan's. */
class TailMatchTest {
    private val stream = PoolFixture.stream("t", RelayUrlNormalizer.normalize("wss://t.example"), Filter(kinds = listOf(1)))

    @Test
    fun `the author index finds exactly the asks a full scan matches`() {
        val random = Random(7)
        val authors = List(12) { "%064x".format(it + 1) }
        val kinds = listOf(0, 1, 7, 30382)
        val asks =
            List(60) {
                val bound = random.nextInt(4)
                RosterBuilder.Ask(
                    stream,
                    Filter(
                        kinds = kinds.shuffled(random).take(1 + random.nextInt(kinds.size)),
                        // A quarter bind no author, the rest one to three.
                        authors = if (bound == 0) null else authors.shuffled(random).take(bound),
                    ),
                )
            }
        val unit = RosterBuilder.UnitAsks(asks, asks.map { it.filter.toJson() }.toSet())
        val stranger = "f".repeat(64)

        repeat(500) { n ->
            val event = PoolFixture.event(n, 1_700_000_000L + n, kinds.random(random), (authors + stranger).random(random))
            val scanned = asks.filter { it.filter.match(event) }
            val indexed = unit.candidates(event.pubKey).filter { it.filter.match(event) }
            assertEquals(scanned.toSet(), indexed.toSet(), "event by ${event.pubKey.take(8)} kind ${event.kind}")
        }
    }

    @Test
    fun `a unit of bound asks alone offers nothing to a stranger`() {
        val asks = List(3) { RosterBuilder.Ask(stream, Filter(kinds = listOf(1), authors = listOf("%064x".format(it)))) }
        val unit = RosterBuilder.UnitAsks(asks, asks.map { it.filter.toJson() }.toSet())
        assertEquals(emptyList(), unit.candidates("f".repeat(64)))
        assertEquals(listOf(asks[1]), unit.candidates("%064x".format(1)))
    }

    private fun RosterBuilder.UnitAsks.candidates(pubKey: String): List<RosterBuilder.Ask> = ArrayList<RosterBuilder.Ask>().also { out -> forEachCandidate(pubKey) { out += it } }
}
