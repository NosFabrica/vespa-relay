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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.fold.Fingerprint
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The fold's comparison decides exactly what a pairwise comparison of every window decides, at any host size. */
class FoldComparisonTest {
    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    private fun hexId(random: Random): String = (1..4).joinToString("") { "%016x".format(random.nextLong()) }

    @Test
    fun `folds, clearances and their evidence match the pairwise comparison on varied groups`() {
        val random = Random(30166)
        val rules =
            listOf(
                Rules(),
                Rules(minSample = 10, minOverlap = 0.3),
                Rules(minSample = 5, groupMetadataMinSample = 2, minOverlap = 0.8),
                Rules(minSample = 1, minOverlap = 0.0),
            )
        var decided = 0
        var ontoHeads = 0
        var twins = 0
        repeat(400) { round ->
            val r = rules[round % rules.size]
            val host = "h$round.example"
            val group = groupOn(host, random)
            val pool = clusterIds(random)
            val prints = printsFor(group, pool, random)
            val known = existingFolds(group, random)
            val view = r.aliases().over(group, known, emptySet())
            val leader = view.toProbe(group).first()
            val kinds = if (random.nextInt(4) == 0) RelayAliases.GROUP_METADATA_KINDS else null

            val learned = view.learn(group, leader, prints.mapValues { Fingerprint.of(it.value) }, kinds)
            val reference = PairwiseFold(r, known).learn(group, leader, prints, kinds)

            assertEquals(reference.folded.toList(), learned.folded.toList(), "folds, round $round")
            assertEquals(reference.distinct.toList(), learned.distinct.toList(), "clearances, round $round")
            assertEquals(reference.twins, learned.twins, "twins, round $round")
            assertEquals(reference.shared, learned.shared, "evidence, round $round")
            decided += learned.folded.size + learned.distinct.size
            ontoHeads += learned.folded.count { (alias, canonical) -> canonical != leader && alias !in learned.twins }
            twins += learned.twins.size
        }
        assertTrue(decided > 1_000, "the generator decided too little to compare: $decided")
        assertTrue(ontoHeads > 50 && twins > 50, "the generator missed a branch: $ontoHeads onto a head, $twins twins")
    }

    @Test
    fun `a host of a thousand distinct paths decides quickly`() {
        val random = Random(1000)
        val group = (0 until 1_000).map { url("wss://filter.example/p$it") }
        val prints = group.associateWith { Fingerprint.of((0 until RelayAliases.DEFAULT_PROBE_TARGET).map { hexId(random) }) }
        val view = RelayAliases()
        val startedNs = System.nanoTime()

        val learned = view.learn(group, view.toProbe(group).first(), prints)

        val tookMs = (System.nanoTime() - startedNs) / 1_000_000
        assertEquals(group.toSet(), learned.distinct, "every path serves its own window")
        assertTrue(learned.folded.isEmpty())
        // Far above the index's cost and far below a pairwise comparison's, so load cannot flip it.
        assertTrue(tookMs < 10_000, "a thousand distinct paths took ${tookMs}ms")
    }

    /** A group of paths on [host], some with a plain twin, one at the root half the time. */
    private fun groupOn(
        host: String,
        random: Random,
    ): List<NormalizedRelayUrl> {
        val paths = (0 until 2 + random.nextInt(14)).map { "p$it" }
        val urls = LinkedHashSet<NormalizedRelayUrl>()
        if (random.nextBoolean()) urls += url("wss://$host")
        for (path in paths) {
            urls += url("wss://$host/$path")
            if (random.nextInt(3) == 0) urls += url("ws://$host/$path")
        }
        if (random.nextInt(5) == 0) urls += url("ws://$host/lonely")
        return urls.toList()
    }

    /** A few relays' stores, each a run of ids, hex and not. */
    private fun clusterIds(random: Random): List<List<String>> =
        (0 until 1 + random.nextInt(4)).map { c ->
            (0 until 60).map { i -> if (random.nextInt(5) == 0) "odd-$c-$i" else hexId(random) }
        }

    /** Each url serves a slice of one relay's store, sometimes thin, sometimes mixed with another's; some are silent. */
    private fun printsFor(
        group: List<NormalizedRelayUrl>,
        pool: List<List<String>>,
        random: Random,
    ): Map<NormalizedRelayUrl, Set<String>> {
        val prints = LinkedHashMap<NormalizedRelayUrl, Set<String>>()
        for (u in group) {
            if (random.nextInt(8) == 0) continue
            val own = pool[random.nextInt(pool.size)]
            val other = pool[random.nextInt(pool.size)]
            val size = listOf(0, 3, 8, 15, 25, 40, 60)[random.nextInt(7)]
            val mixed = random.nextInt(size + 1)
            prints[u] = (own.shuffled(random).take(size - mixed) + other.shuffled(random).take(mixed)).toSet()
        }
        return prints
    }

    /** Folds the store already holds: a url now and then onto another of the group. */
    private fun existingFolds(
        group: List<NormalizedRelayUrl>,
        random: Random,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        if (group.size < 3 || random.nextInt(3) != 0) return emptyMap()
        val canonical = group[random.nextInt(group.size)]
        val alias = group.filter { it != canonical && it.url.startsWith("wss://") }.randomOrNull(random) ?: return emptyMap()
        return mapOf(alias to canonical)
    }

    private data class Rules(
        val minSample: Int = RelayAliases.DEFAULT_MIN_SAMPLE,
        val groupMetadataMinSample: Int = RelayAliases.DEFAULT_GROUP_METADATA_MIN_SAMPLE,
        val minOverlap: Double = RelayAliases.DEFAULT_MIN_OVERLAP,
    ) {
        fun aliases() = RelayAliases(minSample = minSample, groupMetadataMinSample = groupMetadataMinSample, minOverlap = minOverlap)
    }

    /** The comparison before the index: every unmatched window against every head, then every distinct url against every other. */
    private class PairwiseFold(
        val rules: Rules,
        /** One hop each, onto a url that is not itself folded. */
        val folded: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    ) {
        val canonicals = folded.values.toSet()

        fun learn(
            group: List<NormalizedRelayUrl>,
            leader: NormalizedRelayUrl,
            prints: Map<NormalizedRelayUrl, Set<String>>,
            kinds: List<Int>?,
        ): RelayAliases.Learned {
            val leaderPrint = prints[leader] ?: return RelayAliases.Learned()
            val window = if (kinds == RelayAliases.GROUP_METADATA_KINDS) minOf(rules.minSample, rules.groupMetadataMinSample) else rules.minSample
            val floor = if (kinds == RelayAliases.GROUP_METADATA_KINDS) window else 0
            val folds = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
            val cleared = LinkedHashSet<NormalizedRelayUrl>()
            var compared = 0
            val twins = schemeTwins(group, leader, prints)
            val unmatched = ArrayList<NormalizedRelayUrl>()
            for (u in group) {
                if (u == leader || u in twins || u in folded) continue
                val print = prints[u] ?: continue
                if (sameRelay(leaderPrint, print, window, floor)) {
                    compared++
                    folds[u] = leader
                } else if (print.size >= rules.minSample && leaderPrint.size >= rules.minSample) {
                    compared++
                    unmatched += u
                }
            }
            val heads = ArrayList<NormalizedRelayUrl>()
            for (u in unmatched) {
                val print = prints.getValue(u)
                val head = heads.firstOrNull { sameRelay(prints.getValue(it), print, window, floor) }
                if (head == null) {
                    heads += u
                    cleared += u
                } else {
                    folds[u] = head
                }
            }
            for ((plain, secure) in twins) {
                var canonical = secure
                repeat(8) { canonical = folds[canonical] ?: folded[canonical] ?: return@repeat }
                if (canonical == plain) continue
                folds[plain] = canonical
            }
            if (compared > 0 && leaderPrint.size >= rules.minSample && leader !in canonicals && leader !in folds.values) cleared += leader
            // What the fold pass computed for the evidence on each record, before learn returned it.
            val shared = HashMap<NormalizedRelayUrl, Int>()
            for ((alias, canonical) in folds) shared[alias] = prints[canonical].orEmpty().count { it in prints[alias].orEmpty() }
            for (u in cleared) {
                val print = prints[u].orEmpty()
                shared[u] = ((cleared + leader) - u).maxOfOrNull { other -> prints[other].orEmpty().count { it in print } } ?: 0
            }
            return RelayAliases.Learned(folds, cleared, twins.keys, shared)
        }

        private fun sameRelay(
            a: Set<String>,
            b: Set<String>,
            window: Int,
            floor: Int,
        ): Boolean {
            val smaller = minOf(a.size, b.size)
            if (smaller < window) return false
            val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
            if (shared < floor) return false
            return shared.toDouble() / smaller >= rules.minOverlap
        }

        private fun schemeTwins(
            group: List<NormalizedRelayUrl>,
            leader: NormalizedRelayUrl,
            prints: Map<NormalizedRelayUrl, Set<String>>,
        ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
            val secure = HashMap<String, NormalizedRelayUrl>()
            for (u in group) if (u.url.startsWith("wss://")) secure.putIfAbsent(endpoint(u), u)
            val twins = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
            for (u in group) {
                if (u == leader || u.url.startsWith("wss://") || u in folded) continue
                val twin = secure[endpoint(u)] ?: continue
                val plainPrint = prints[u] ?: continue
                val twinPrint = prints[twin] ?: continue
                if (plainPrint.isNotEmpty() && plainPrint.count { it in twinPrint }.toDouble() / plainPrint.size < rules.minOverlap) continue
                twins[u] = twin
            }
            return twins
        }

        private fun endpoint(u: NormalizedRelayUrl): String = RelayAliases.hostOf(u.url) + "/" + RelayAliases.pathOf(u.url)
    }
}
