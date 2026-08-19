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
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * WHICH queries each cadence asks — the claim the two-tier document rests on.
 *
 * The split in [StatsTier] is an argument about cost: the counters run about
 * once a minute, so nothing whose cost scales with the corpus may be in them.
 * Against a real Vespa that claim is only checkable with a 90M-document corpus
 * and a stopwatch, and it fails silently — a pipeline that drifts into the fast
 * tier does not break a chart, it just runs fifteen times more often than it can
 * afford, until an engine falls over. Against [StatsQueries] it is an assertion
 * over a recorded list, which is why that seam exists.
 *
 * The engine's responses here are the ones [StatsYqlTest] captured from Vespa
 * 8.733, and the fake is deliberately loose about which one it hands back: these
 * tests are about the questions, not the answers. The one place an answer matters
 * is `newestEvent`, which is the single number that crosses the tier boundary.
 */
class StatsRollupTest {
    /**
     * Every question asked of the engine, in order.
     *
     * [spans] is the only answer any test varies, because it is the only one a
     * test's conclusion depends on: `spanBy(kind)` over a window is how the
     * counters tier finds the newest event, and an EMPTY answer there is a
     * mirror that has published nothing lately — the case the carry-forward
     * exists for.
     */
    private class FakeQueries(
        private val spans: String = SPAN_BY_KIND,
        private val failing: Set<String> = emptySet(),
    ) : StatsQueries {
        data class Ask(
            val pipeline: String,
            val where: String,
            val source: String,
        )

        val asked = mutableListOf<Ask>()

        override suspend fun group(
            pipeline: String,
            where: String,
            source: String,
        ): JsonObject {
            asked += Ask(pipeline, where, source)
            if (pipeline in failing) error("vespa 400 — refused `$pipeline`")
            val body =
                when {
                    pipeline == StatsYql.TOTAL -> TOTAL

                    pipeline == StatsYql.distinct("pubkey") -> DISTINCT

                    pipeline == StatsYql.countsBy("kind") -> COUNTS_BY_KIND

                    pipeline == StatsYql.spanBy("kind") -> spans

                    pipeline == StatsYql.nested("kind", StatsYql.DAY) -> COUNTS_BY_KIND

                    // Every bucketed series comes back in one shape and these
                    // tests do not read the buckets — see the class KDoc.
                    else -> COUNTS_BY_DAY
                }
            return Json.parseToJsonElement(body).jsonObject
        }
    }

    private fun rollup(queries: FakeQueries) = StatsRollup(queries, relayUrl = "wss://relay.example", nowSeconds = { NOW })

    /**
     * The members that are the document's envelope rather than one of its
     * sections.
     *
     * `title` and `counted` joined it when one markup file started serving all
     * three services: the heading, the tab and the line about what the numbers
     * cover cannot be in the page any more, because the page does not know
     * which service is serving it. They describe the document, so they are
     * envelope — a section is a thing a panel reads.
     */
    private fun sectionsOf(doc: JsonObject) = doc.keys - setOf("schema", "relay", "title", "generatedAt", "scope", "counted", "countedAs", "timezone", "tiers")

    // ---- the tiering itself -------------------------------------------------

    /**
     * The counters may only ask questions whose cost is bounded by something
     * other than the corpus.
     *
     * Stated as an invariant over the pipelines rather than as a list of the
     * queries we happen to make today, because the failure this guards against
     * is a NEW query landing in the fast tier. Four shapes are the expensive
     * ones, and all four are recognisable in the YQL:
     *
     *  - `group(pubkey)` with no `kind` filter materialises the store's whole
     *    distinct-pubkey set
     *  - a grouping nested inside `each(...)` materialises one such set per
     *    bucket — the shape that OOMKilled this engine twice
     *  - `tag_index` emits every tag pair on every matched document
     *  - a grouping bounded ONLY by a populous `kind` — see [SELECTIVE_KINDS]
     *
     * …and a bucketed grouping is only affordable over a window.
     */
    @Test
    fun `the counters tier asks nothing whose cost scales with the corpus`() {
        runBlocking {
            val queries = FakeQueries()
            rollup(queries).compute(StatsTier.COUNTERS)

            assertTrue(queries.asked.isNotEmpty(), "the counters tier does query the engine")
            for ((pipeline, where, _) in queries.asked) {
                assertFalse(pipeline.contains(StatsYql.TAG), "tag_index is a per-tag emission, not a counter: `$pipeline`")
                assertFalse(pipeline.contains("each(all(group("), "a set per bucket is the shape that OOMs: `$pipeline`")
                // A count() needs no bound at all: it does not materialise what
                // it counts. Everything that GROUPS does.
                if (!pipeline.startsWith("all(group(")) continue
                val windowed = where.contains("created_at >=")
                val kinds = KIND_FILTER.findAll(where).map { it.groupValues[1].toInt() }.toSet()
                assertTrue(windowed || kinds.isNotEmpty(), "a grouping over the whole store cannot run every minute: `$pipeline` where `$where`")
                if (!windowed) {
                    assertTrue(
                        kinds.all { it in SELECTIVE_KINDS },
                        "a `kind` filter bounds the group set, not the walk — $kinds is not a selective kind: `$pipeline` where `$where`",
                    )
                }
            }
        }
    }

    /** …and the expensive shapes are all still asked, on the slow cadence. */
    @Test
    fun `the charts tier is where the corpus-wide groupings live`() {
        runBlocking {
            val queries = FakeQueries()
            rollup(queries).compute(StatsTier.CHARTS)
            val asked = queries.asked

            assertTrue(
                asked.any { it.pipeline == StatsYql.distinct("pubkey") && it.where == "true" },
                "the store's distinct authors are counted here, once every slow pass",
            )
            assertTrue(asked.any { it.pipeline == StatsYql.countsBy("kind") && it.where == "true" }, "the kind histogram walks the whole store")
            assertTrue(asked.any { it.pipeline.contains("each(all(group(pubkey)") }, "distinct authors per bucket is a charts cost")
            assertTrue(asked.any { it.pipeline.contains(StatsYql.TAG) }, "the relay distribution groups tag_index")
            assertTrue(
                asked.any { it.pipeline == StatsYql.distinct("pubkey") && it.where.contains("kind = 9735") },
                "the zap wallets walk every receipt in the store to return a handful of services",
            )
        }
    }

    /**
     * The two tiers partition the document: no member is computed twice, and
     * none is left with nobody to compute it.
     *
     * A member owned by neither tier would simply never appear — a panel that
     * reads "not in this document" forever, with nothing failing anywhere to say
     * why. A member owned by both would be written by two cadences, which is the
     * contradiction the split exists to avoid.
     */
    @Test
    fun `every section belongs to exactly one tier`() {
        runBlocking {
            val counters = rollup(FakeQueries()).compute(StatsTier.COUNTERS)
            val charts = rollup(FakeQueries()).compute(StatsTier.CHARTS)

            assertEquals(emptySet(), StatsTier.COUNTERS.sections intersect StatsTier.CHARTS.sections)
            // What each tier PUBLISHED is what it declares it owns — `sync`
            // excepted, which is absent with no router files to read. The
            // declaration is what StatsSnapshot removes stale members by, so a
            // tier that quietly publishes outside it would leave sections
            // nobody ever clears.
            assertEquals(StatsTier.COUNTERS.sections - "sync", sectionsOf(counters))
            assertEquals(StatsTier.CHARTS.sections, sectionsOf(charts))
            for (member in sectionsOf(counters) + sectionsOf(charts)) {
                assertTrue(
                    member in StatsTier.COUNTERS.sections || member in StatsTier.CHARTS.sections,
                    "$member is published by a tier that does not declare it",
                )
            }
        }
    }

    /**
     * Each pass says which cadence it is, when it ran, and what it produced.
     *
     * The `sections` list is the published set rather than the owned one: a list
     * naming `sync` on a relay with no router reads as a section that failed
     * silently, which is the opposite of what an absent one means.
     */
    @Test
    fun `a pass states its own cadence`() {
        runBlocking {
            val doc = rollup(FakeQueries()).compute(StatsTier.COUNTERS, previous = null, everySeconds = 60)
            val tier = assertNotNull(doc["tiers"]).jsonObject["counters"]!!.jsonObject

            assertEquals(60, tier["everySeconds"]!!.jsonPrimitive.content.toInt())
            assertNotNull(tier["generatedAt"], "a pass is dated on its own, not only through the document")
            assertNotNull(tier["tookMs"])
            assertEquals(sectionsOf(doc).toList().sorted(), tier["sections"]!!.jsonArray.map { it.jsonPrimitive.content }.sorted())
            assertNull(doc["tiers"]!!.jsonObject["charts"], "a pass claims nothing about the other half of the document")
            // Dropped at schema 2: a document computed in two passes has no one
            // duration, and `tiers.<name>.tookMs` is where each pass states its own.
            assertNull(doc["tookMs"])
        }
    }

    /**
     * The counters carry the totals; the expensive counters are elsewhere.
     *
     * `pubkeys` and `kinds` used to sit in `corpus`. Both had to leave — a
     * section carries one `generatedAt` for all of its members, so a number
     * refreshed on the slow cadence inside a section stamped seconds ago is a
     * section that lies about half of itself.
     */
    @Test
    fun `the corpus section is the cheap half of what it used to be`() {
        runBlocking {
            val counters = rollup(FakeQueries()).compute(StatsTier.COUNTERS)
            val corpus = counters["corpus"]!!.jsonObject["data"]!!.jsonObject

            assertEquals(setOf("events", "futureDated", "newestEvent", "asOf"), corpus.keys)
            val charts = rollup(FakeQueries()).compute(StatsTier.CHARTS)
            assertEquals(
                setOf("pubkeys"),
                charts["authors"]!!.jsonObject["data"]!!.jsonObject.keys,
                "the store's distinct authors moved to a section that states its own age",
            )
            assertNotNull(charts["kinds"]!!.jsonObject["data"]!!.jsonObject["total"], "how many kinds is the histogram's own number now")
        }
    }

    // ---- the one number that crosses the boundary ----------------------------

    /**
     * Freshness is asked for over a WINDOW, which is what makes it affordable
     * every minute.
     *
     * The same `spanBy(kind)` pipeline the kinds histogram uses, bounded at both
     * ends: the match set is a couple of days of events instead of the store, and
     * the answer is identical whenever the mirror is publishing at all.
     */
    @Test
    fun `the newest event is asked for over days, not over the store`() {
        runBlocking {
            val queries = FakeQueries()
            rollup(queries).compute(StatsTier.COUNTERS)
            val ask = assertNotNull(queries.asked.firstOrNull { it.pipeline == StatsYql.spanBy("kind") })

            assertEquals(StatsYql.window(NOW - 2 * 86_400L, NOW), ask.where, "two days back, and bounded at the present")
        }
    }

    /**
     * A quiet window does not retract a freshness this relay has already
     * reported.
     *
     * `newestEvent` is an absolute timestamp, so carrying it forward is not a
     * stale number — it is exactly as true as when it was taken. Both places the
     * previous document may hold it are read, and the answer is the MAXIMUM of
     * those and the fresh window, so the tile only ever moves forward.
     */
    @Test
    fun `the newest event survives a quiet window and never goes backwards`() {
        runBlocking {
            val quiet = rollup(FakeQueries(spans = EMPTY_GROUPS))
            assertNull(
                quiet.compute(StatsTier.COUNTERS)["corpus"]!!.jsonObject["data"]!!.jsonObject["newestEvent"],
                "nothing measured and nothing known is an absent number, not a zero",
            )

            // The counters tier's own last answer.
            val carried = quiet.compute(StatsTier.COUNTERS, previousWith(corpusNewest = 1_900_000_000L))
            assertEquals(1_900_000_000L, carried.newestEvent())

            // The charts tier's per-kind spans, which are the authoritative
            // whole-corpus maximum and the only thing present before the first
            // counters pass has published anything.
            val fromKinds = quiet.compute(StatsTier.COUNTERS, previousWith(kindsLastSeen = 1_800_000_000L))
            assertEquals(1_800_000_000L, fromKinds.newestEvent())

            // A fresh window beats an older carry…
            assertEquals(1_754_581_422L, rollup(FakeQueries()).compute(StatsTier.COUNTERS, previousWith(corpusNewest = 1_700_000_000L)).newestEvent())
            // …and loses to a newer one rather than winding the tile back.
            assertEquals(1_900_000_000L, rollup(FakeQueries()).compute(StatsTier.COUNTERS, previousWith(corpusNewest = 1_900_000_000L)).newestEvent())
        }
    }

    // ---- what a failure costs ------------------------------------------------

    /**
     * A refused query costs its own number, is named, and is TIMED.
     *
     * The timing is the part that is new and the reason it is published at all:
     * which queries can afford the fast cadence is a measurement, not a
     * deduction, and a corpus twice this size moves the boundary. A failure is
     * timed too — a query that took a minute to be refused is a different
     * problem from one refused instantly.
     */
    @Test
    fun `every query is timed, including the ones that fail`() {
        runBlocking {
            val queries = FakeQueries(failing = setOf(StatsYql.TOTAL))
            val corpus = rollup(queries).compute(StatsTier.COUNTERS)["corpus"]!!.jsonObject

            assertEquals("partial", corpus["status"]!!.jsonPrimitive.content)
            assertTrue(corpus["errors"]!!.jsonObject.keys.containsAll(setOf("events", "futureDated")), "both counts use that pipeline")
            val timings = assertNotNull(corpus["queryMs"]).jsonObject
            assertEquals(setOf("events", "futureDated", "newestEvent"), timings.keys, "every attempt is timed, under the key its error would carry")
            assertNull(corpus["data"]!!.jsonObject["events"], "a refused count is absent rather than zero")
        }
    }

    /** A relay with no router has no sync section — and does not claim one. */
    @Test
    fun `a serve-only relay publishes no sync section`() {
        runBlocking {
            val doc = rollup(FakeQueries()).compute(StatsTier.COUNTERS, previous = null, everySeconds = 60)

            assertNull(doc["sync"])
            assertFalse(
                doc["tiers"]!!
                    .jsonObject["counters"]!!
                    .jsonObject["sections"]!!
                    .jsonArray
                    .any { it.jsonPrimitive.content == "sync" },
                "a listed section that is not in the document reads as one that failed",
            )
        }
    }

    // ---- helpers ------------------------------------------------------------

    private fun JsonObject.newestEvent(): Long? =
        this["corpus"]
            ?.jsonObject
            ?.get("data")
            ?.jsonObject
            ?.get("newestEvent")
            ?.jsonPrimitive
            ?.content
            ?.toLong()

    /** A previously served document carrying a freshness in one of the two places it can live. */
    private fun previousWith(
        corpusNewest: Long? = null,
        kindsLastSeen: Long? = null,
    ) = buildJsonObject {
        corpusNewest?.let {
            put("corpus", buildJsonObject { put("data", buildJsonObject { put("newestEvent", it) }) })
        }
        kindsLastSeen?.let {
            put(
                "kinds",
                buildJsonObject {
                    put(
                        "data",
                        buildJsonObject {
                            put("total", 1)
                            put(
                                "all",
                                Json.parseToJsonElement("""[{"kind":1,"events":9,"firstSeen":1,"lastSeen":$it}]"""),
                            )
                        },
                    )
                },
            )
        }
    }

    private companion object {
        /** A fixed clock, so a window is an exact string a test can assert. */
        const val NOW = 1_800_000_000L

        /** Every `kind = N` a WHERE clause pins. */
        val KIND_FILTER = Regex("""kind = (\d+)""")

        /**
         * The kinds a counters query may lean on as its only bound.
         *
         * A CLAIM ABOUT POPULATIONS, which is what makes it worth pinning here.
         * A `kind` filter bounds the group set — `group(pubkey)` over kind 30382
         * returns the few services that publish scores — but it does not bound
         * the walk, and the engine still touches every matching document. That is
         * fine for the NIP-85 kinds, which run to thousands of events on a real
         * mirror, and is not fine for kind 9735: a mirror holds millions of zap
         * receipts, which is why `zaps` sits with the charts despite being three
         * counts. A new fast-tier query filtered on kind 1 would look exactly as
         * bounded as these and cost a full pass.
         */
        val SELECTIVE_KINDS = setOf(10040, 30382)

        // Vespa 8.733 — the same captures StatsYqlTest asserts the readers against.
        const val TOTAL =
            """{"id":"toplevel","fields":{"totalCount":602},"children":[{"id":"group:root:0","fields":{"count()":602}}]}"""
        const val DISTINCT =
            """{"id":"toplevel","children":[{"id":"group:root:0","children":[{"id":"grouplist:pubkey","fields":{"count()":417}}]}]}"""
        const val COUNTS_BY_KIND =
            """{"id":"toplevel","children":[{"id":"group:root:0","children":[{"id":"grouplist:kind","children":[
              {"id":"group:long:1","value":"1","fields":{"count()":83}},
              {"id":"group:long:0","value":"0","fields":{"count()":79}},
              {"id":"group:long:3","value":"3","fields":{"count()":65}}]}]}]}"""
        const val SPAN_BY_KIND =
            """{"id":"toplevel","children":[{"id":"group:root:0","children":[{"id":"grouplist:kind","children":[
              {"id":"group:long:0","value":"0","fields":{"min(created_at)":1751137726,"max(created_at)":1754581422}},
              {"id":"group:long:1","value":"1","fields":{"min(created_at)":1751277649,"max(created_at)":1754533972}}]}]}]}"""
        const val COUNTS_BY_DAY =
            """{"id":"toplevel","children":[{"id":"group:root:0","children":[{"id":"grouplist:time.date(created_at)","children":[
              {"id":"group:string:2025-1-5","value":"2025-1-5","fields":{"count()":1}}]}]}]}"""

        /** A window nothing was published in: the grouping matched, and returned no groups. */
        const val EMPTY_GROUPS = """{"id":"toplevel","fields":{"totalCount":0}}"""
    }
}
