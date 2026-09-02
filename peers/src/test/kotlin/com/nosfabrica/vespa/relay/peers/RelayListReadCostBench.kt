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
 * WHAT A RELAY-LIST READ COSTS, against a real store — the walk
 * [RelayDiscovery.discover] now takes for every source, and the one it used to
 * take, side by side.
 *
 *  - the PAGED SCAN, which is what runs: `/search/` selects the match set off
 *    the `kind` attribute (`fast-search` in the schema) and ONE walk answers
 *    every tag name the source asks about.
 *  - the store's TAGS PROJECTION (`distinctTagValues`), which is a
 *    `document/v1` visit carrying a selection expression. The predicate is
 *    evaluated per document with no index behind it, so it walks the WHOLE
 *    CORPUS however few documents it wants — and it does that ONCE PER TAG
 *    NAME, because the projection answers about one tag.
 *
 * On `vespa-eventstore-staging` (2026-09-02, #182) one SERIAL visit over
 * `(event.kind==10040)` cost 75.0s against 0.0058s for the same predicate
 * through `/search/`, and the monitor's 10040 source names 38 tags — 40 corpus
 * walks per derivation pass counting the 10002 and 10009 sources beside it, all
 * on the document API, which is the lane the ingest dedup probe queues in. That
 * is the mechanism behind the wedges in #167: `/search/` stayed fast
 * throughout, so a health-check query issued by hand answered instantly while
 * the mirror was dead.
 *
 * TWO REASONS TO RUN IT NOW THAT NOTHING CHOOSES BETWEEN THEM. The projection
 * arm is the evidence for the choice, and a claim nothing can re-check is a
 * claim that rots — the gap only widens as the corpus grows, so this should
 * read MORE lopsided every time. And the scan arm is the read that ships: at
 * 10^11 documents the corpus walk is hours and settled, but paging is a
 * function of the MATCH SET, so a relay-list kind growing into the millions is
 * the next thing to watch. The ms/event this prints is what says whether the
 * store needs to project `tags` off a SEARCH before that day.
 *
 * `BENCH_LIST_VISIT=0` skips the projection arm. Do that against a busy
 * production store: that arm IS the bug, and it is a real corpus walk on the
 * document API.
 *
 * ASSERTS NOTHING, like every bench here. Comparing the two answers is half the
 * point: they must name the same set, or the faster path is faster for the
 * wrong reason.
 *
 * READ-ONLY, and it deploys nothing (`autoDeploy = false`), so it is safe to
 * point at a live deployment. Skipped unless `BENCH_VESPA_URL` names one:
 *
 *     BENCH_VESPA_URL=http://localhost:8080 ./gradlew :peers:test \
 *       --tests '*RelayListReadCostBench*' --rerun -i
 *
 * The kind and the tags default to the read that produced #182 — one NIP-85
 * declaration tag and one Tapestry one. `--rerun` is load-bearing: the task is
 * up-to-date-checked, so a second identical run is SKIPPED and prints nothing,
 * which reads as a silent pass.
 *
 *     BENCH_LIST_KIND=10002 BENCH_LIST_TAGS=r BENCH_LIST_INDEX=1 …
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
                // The size of the answer, off the index — one grouping count.
                val matches = store.count(filter)
                println("LIST-READ-BENCH kind=$kind tags=${tags.size} matches=$matches")

                // THE PROJECTION: one corpus visit per tag name. Opt-out,
                // because on a production store this arm IS the bug.
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

                // THE INDEX: one paged walk answering every tag at once. The
                // predicate is `distinctTagValues`' own, so the two sides are
                // comparable as sets and not only as clocks.
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
                // The number to watch as a relay-list kind grows: paging is a
                // function of the match set, so this rate says when the scan
                // itself needs the store to project `tags` off a SEARCH.
                println("  RATE   index ${"%.3f".format(indexTotal / 1_000_000.0 / events.coerceAtLeast(1))} ms/event")
                if (viaVisit.isNotEmpty()) {
                    println("  TOTAL  visit ${secs(visitTotal)} over ${tags.size} corpus walk(s)")
                    println("  ratio  ${"%.1f".format(visitTotal.toDouble() / indexTotal.coerceAtLeast(1))}x")
                }

                // Same question, so the same answer — a path that is faster
                // because it found less has not been measured, it has been
                // mismeasured.
                for (tag in viaVisit.keys) {
                    val a = viaVisit[tag].orEmpty()
                    val b = viaIndex[tag].orEmpty()
                    if (a != b) println("  DISAGREEMENT on $tag: visit-only=${(a - b).size} index-only=${(b - a).size}")
                }
            }
        }

    private fun secs(nanos: Long) = "%.4fs".format(nanos / 1_000_000_000.0)

    private companion object {
        /**
         * Deliberately the same page size [RelayDiscovery.discover] walks at, so
         * the number printed here is the one production would pay. Named rather
         * than passed inline because a bench that quietly reads at a different
         * page size is measuring a walk nothing performs.
         */
        const val SCAN_PAGE = 10_000

        /** One NIP-85 delegation tag and one Tapestry Trusted List one — both put the url at 2. */
        val DEFAULT_TAGS = listOf("30382:rank", "30392")
    }
}
