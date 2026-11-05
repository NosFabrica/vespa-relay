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
package com.nosfabrica.vespa.relay.fold

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/** A pass's deltas applied together read exactly as the same deltas applied one at a time, in order. */
class RelayAliasesMergeTest {
    private val pool = (0 until 40).map { RelayUrlNormalizer.normalize("wss://host${it % 8}.example/p$it") }

    private fun delta(random: Random): RelayAliases.Learned {
        val folded = (0 until random.nextInt(4)).associate { pool.random(random) to pool.random(random) }.filter { (a, b) -> a != b }
        val distinct = (0 until random.nextInt(4)).mapTo(HashSet()) { pool.random(random) }
        return RelayAliases.Learned(folded = folded, distinct = distinct)
    }

    @Test
    fun `merged deltas give the view that applying each in turn gives`() {
        for (seed in 0 until 200) {
            val random = Random(seed)
            val start = RelayAliases() + delta(random)
            val deltas = (0 until random.nextInt(1, 12)).map { delta(random) }
            var oneByOne = start
            for (d in deltas) oneByOne += d
            val merged = start + deltas

            assertEquals(oneByOne.size(), merged.size(), "seed $seed")
            for (url in pool) {
                assertEquals(oneByOne.canonicalOf(url), merged.canonicalOf(url), "seed $seed: canonicalOf($url)")
                assertEquals(oneByOne.measured(url), merged.measured(url), "seed $seed: measured($url)")
            }
            assertEquals(oneByOne.unresolved(pool), merged.unresolved(pool), "seed $seed: unresolved")
        }
    }
}
