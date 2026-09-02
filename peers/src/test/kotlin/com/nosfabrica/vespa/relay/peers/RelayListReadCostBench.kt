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
 * THE TWO WAYS TO READ A RELAY LIST OUT OF THE STORE, priced against a real one
 * — the measurement [RelayDiscovery.visitBeatsTheIndex] decides from, run where
 * the decision will actually be taken.
 *
 * A relay-list source names a tag and a kind. That can be answered two ways:
 *
 *  - the store's TAGS PROJECTION (`distinctTagValues`), which is a
 *    `document/v1` visit carrying a selection expression. The predicate is
 *    evaluated per document and never touches the search index, so it walks the
 *    WHOLE CORPUS however few documents it is looking for — and it does that
 *    ONCE PER TAG NAME, because the projection answers about one tag.
 *  - the PAGED SCAN over the same filter, which the `kind` attribute
 *    (`fast-search` in the schema) selects, and which reads every tag name a
 *    source asks about in ONE walk.
 *
 * Both numbers matter and they multiply. On `vespa-eventstore-staging`
 * (2026-09-02, #182) one SERIAL visit over `(event.kind==10040)` cost 75.0s
 * against 0.0058s for the same predicate through `/search/`, and the monitor's
 * 10040 source names 38 tags — 40 corpus walks per derivation pass counting the
 * 10002 and 10009 sources beside it, all on the document API, which is the lane
 * the ingest dedup probe queues in. That is the mechanism behind the wedges in
 * #167: `/search/` stayed fast throughout, so a health-check query issued by
 * hand answered instantly while the mirror was dead.
 *
 * THE PER-VISIT CLOCK IS THE THING TO MEASURE HERE rather than to take from
 * that ticket. The router's visit splits into `VespaEventIndex.visitSlices`
 * concurrent streamed slices (2 x host cores, 4..32), so it beats the serial
 * 75.0s by whatever the content node's cores allow, and the access log cannot
 * tell one sliced visit from N separate ones — every slice carries the same
 * selection string, built from one `nowSecs()`. This runs the router's own
 * call, so the clock it prints is the router's.
 *
 * ASSERTS NOTHING, like every bench here. It prints what the two paths cost and
 * what they each found, and — because the constant behind the decision is an
 * exchange rate that only a real store can check — which way
 * [RelayDiscovery.visitBeatsTheIndex] calls it on the corpus in front of it.
 * Comparing the two answers is the other half: they must name the same set, or
 * the cheaper path is cheaper for the wrong reason.
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
                // The two numbers the decision is made from, and both are
                // grouping counts off the index — the cheap read on either path.
                val matches = store.count(filter)
                val corpus = store.count(Filter())
                println("LIST-READ-BENCH kind=$kind tags=${tags.size} matches=$matches corpus=$corpus")

                // THE PROJECTION: one corpus visit per tag name.
                val viaVisit = LinkedHashMap<String, Set<String>>()
                val visitStart = System.nanoTime()
                for (tag in tags) {
                    val at = System.nanoTime()
                    val values = store.store.distinctTagValues(filter, tagName = tag, valueIndex = valueIndex)
                    viaVisit[tag] = values
                    println("  visit  $tag  ${secs(System.nanoTime() - at)}  ${values.size} value(s)")
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

                println("  TOTAL  visit ${secs(visitTotal)} over ${tags.size} walk(s)")
                println("  TOTAL  index ${secs(indexTotal)} over 1 walk of $events event(s)")
                println("  ratio  ${"%.1f".format(visitTotal.toDouble() / indexTotal.coerceAtLeast(1))}x")
                println(
                    "  decision: RelayDiscovery takes the ${if (RelayDiscovery.visitBeatsTheIndex(matches) { corpus }) "VISIT" else "INDEX"}",
                )

                // Same question, so the same answer — a path that is faster
                // because it found less has not been measured, it has been
                // mismeasured.
                for (tag in tags) {
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
