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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

/**
 * Prices a relay-list read two ways against a real store: the paged scan
 * [RelayDiscovery.discover] runs and the per-tag `distinctTagValues` corpus visit.
 * Read-only; selected by `BENCH_VESPA_URL`, and `BENCH_LIST_VISIT=0` skips the visit arm.
 */
class RelayListReadCostBench {
    private val url = System.getenv("BENCH_VESPA_URL")
    private val kind = System.getenv("BENCH_LIST_KIND")?.toIntOrNull() ?: 10040
    private val tags =
        System
            .getenv("BENCH_LIST_TAGS")
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() } ?: DEFAULT_TAGS
    private val valueIndex = System.getenv("BENCH_LIST_INDEX")?.toIntOrNull() ?: 2

    @Test
    fun `price the projection against the index`() =
        runBlocking {
            val url = url ?: return@runBlocking println("LIST-READ-BENCH skipped — set BENCH_VESPA_URL")
            VespaEventStore.open(url, autoDeploy = false, deferTrustProjection = false).use { store ->
                val filter = Filter(kinds = listOf(kind))
                val matches = store.count(filter)
                println("LIST-READ-BENCH kind=$kind tags=${tags.size} matches=$matches")

                // One corpus visit per tag name. Opt-out, because on a production store this arm is the bug.
                val viaVisit = LinkedHashMap<String, Set<String>>()
                val visitStart = System.nanoTime()
                if (System.getenv("BENCH_LIST_VISIT") != "0") {
                    for (tag in tags) {
                        val at = System.nanoTime()
                        val values = store.store.distinctTagValues(filter, tagName = tag, valueIndex = valueIndex)
                        viaVisit[tag] = values
                        println("  visit  $tag  ${secs(System.nanoTime() - at)}  ${values.size} value(s)")
                    }
                }
                val visitTotal = System.nanoTime() - visitStart

                // The same predicate as `distinctTagValues`, so the two sides compare as sets, not only clocks.
                val viaIndex = HashMap<String, MutableSet<String>>()
                val at = System.nanoTime()
                var events = 0
                RelayDiscovery.scan(store, filter, SCAN_PAGE) { event ->
                    events++
                    for (tag in event.tags) {
                        if (tag.size <= valueIndex) continue
                        if (tag[0] !in tags) continue
                        tag[valueIndex].takeIf(String::isNotEmpty)?.let { viaIndex.getOrPut(tag[0]) { HashSet() }.add(it) }
                    }
                }
                val indexTotal = System.nanoTime() - at

                println("  TOTAL  index ${secs(indexTotal)} over 1 walk of $events event(s)")
                // This rate says when the store needs to project `tags` off a search.
                println("  RATE   index ${"%.3f".format(indexTotal / 1_000_000.0 / events.coerceAtLeast(1))} ms/event")
                if (viaVisit.isNotEmpty()) {
                    println("  TOTAL  visit ${secs(visitTotal)} over ${tags.size} corpus walk(s)")
                    println("  ratio  ${"%.1f".format(visitTotal.toDouble() / indexTotal.coerceAtLeast(1))}x")
                }

                for (tag in viaVisit.keys) {
                    val a = viaVisit[tag].orEmpty()
                    val b = viaIndex[tag].orEmpty()
                    if (a != b) println("  DISAGREEMENT on $tag: visit-only=${(a - b).size} index-only=${(b - a).size}")
                }
            }
        }

    private fun secs(nanos: Long) = "%.4fs".format(nanos / 1_000_000_000.0)

    private companion object {
        /** The page size [RelayDiscovery.discover] walks at, so the number printed is the one production pays. */
        const val SCAN_PAGE = 10_000

        /** One NIP-85 delegation tag and one Tapestry Trusted List one; both put the url at 2. */
        val DEFAULT_TAGS = listOf("30382:rank", "30392")
    }
}
