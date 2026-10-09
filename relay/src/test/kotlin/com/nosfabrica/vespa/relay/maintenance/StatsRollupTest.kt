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
 * Which queries each cadence of [StatsTier] asks. Against [StatsQueries] that
 * is an assertion over a recorded list, and the fake is loose about the answers.
 */
class StatsRollupTest {
    /**
     * Every question asked of the engine, in order. [spans] is the one answer a
     * test varies: an empty one is a mirror that has published nothing lately.
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

                    // Nothing here reads buckets, so every bucketed series shares one shape.
                    else -> COUNTS_BY_DAY
                }
            return Json.parseToJsonElement(body).jsonObject
        }
    }

    private fun rollup(queries: FakeQueries) = StatsRollup(queries, relayUrl = "wss://relay.example", nowSeconds = { NOW })

    /** The members that describe the document rather than being a section a panel reads. */
    private fun sectionsOf(doc: JsonObject) = doc.keys - setOf("schema", "relay", "title", "generatedAt", "scope", "counted", "countedAs", "timezone", "tiers")

    // ---- the tiering itself -------------------------------------------------

    /**
     * An invariant over the pipelines rather than a list of today's queries, so a new
     * query landing in the fast tier fails here.
     */
    @Test
    fun `the counters tier asks nothing whose cost scales with the corpus`() {
        runBlocking {
            val queries = FakeQueries()
            rollup(queries).compute(StatsTier.COUNTERS)

            for ((pipeline, where, source) in queries.asked) {
                assertFalse(pipeline.contains(StatsYql.TAG), "tag_index is a per-tag emission, not a counter: `$pipeline`")
                assertFalse(pipeline.contains("each(all(group("), "a set per bucket is the shape that OOMs: `$pipeline`")
                assertFalse(source == StatsYql.REPUTATION, "the reputation store holds a document per scored pubkey: `$pipeline`")
                // A count() materialises nothing but still walks every document it matches.
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
            assertTrue(asked.any { it.pipeline == StatsYql.TOTAL && it.where == "true" }, "the event total walks the whole store")
            assertTrue(
                asked.any { it.pipeline == StatsYql.distinct("pubkey") && it.where.contains("kind = 30382") },
                "the score providers walk every stored score",
            )
            assertTrue(asked.any { it.pipeline.contains("each(all(group(pubkey)") }, "distinct authors per bucket is a charts cost")
            assertTrue(asked.any { it.pipeline.contains(StatsYql.TAG) }, "the relay distribution groups tag_index")
            assertTrue(
                asked.any { it.pipeline == StatsYql.distinct("pubkey") && it.where.contains("kind = 9735") },
                "the zap wallets walk every receipt in the store to return a handful of services",
            )
        }
    }

    /** A member owned by neither tier never appears; one owned by both is written by two cadences. */
    @Test
    fun `every section belongs to exactly one tier`() {
        runBlocking {
            val counters = rollup(FakeQueries()).compute(StatsTier.COUNTERS)
            val charts = rollup(FakeQueries()).compute(StatsTier.CHARTS)

            assertEquals(emptySet(), StatsTier.COUNTERS.sections intersect StatsTier.CHARTS.sections)
            // `sync` is absent with no router files to read. StatsSnapshot clears stale members
            // by the declaration, so a section published outside it is one nobody clears.
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

    /** The `sections` list is the published set, not the owned one; an absent `sync` named reads as a failure. */
    @Test
    fun `a pass states its own cadence`() {
        runBlocking {
            val doc = rollup(FakeQueries()).compute(StatsTier.CHARTS, previous = null, everySeconds = 900)
            val tier = assertNotNull(doc["tiers"]).jsonObject["charts"]!!.jsonObject

            assertEquals(900, tier["everySeconds"]!!.jsonPrimitive.content.toInt())
            assertNotNull(tier["generatedAt"], "a pass is dated on its own, not only through the document")
            assertNotNull(tier["tookMs"])
            assertEquals(sectionsOf(doc).toList().sorted(), tier["sections"]!!.jsonArray.map { it.jsonPrimitive.content }.sorted())
            assertNull(doc["tiers"]!!.jsonObject["counters"], "a pass claims nothing about the other half of the document")
            // A document computed in two passes has no one duration.
            assertNull(doc["tookMs"])
        }
    }

    /** The page's tiles read these members, so they keep their places whichever pass computes them. */
    @Test
    fun `the corpus and trust totals keep the members the page reads`() {
        runBlocking {
            val charts = rollup(FakeQueries()).compute(StatsTier.CHARTS)
            val corpus = charts["corpus"]!!.jsonObject["data"]!!.jsonObject

            assertEquals(setOf("events", "futureDated", "newestEvent", "asOf"), corpus.keys)
            assertEquals(
                setOf("scoredPubkeys", "observers", "providers", "scores"),
                charts["trust"]!!.jsonObject["data"]!!.jsonObject.keys,
            )
            assertEquals(
                setOf("pubkeys"),
                charts["authors"]!!.jsonObject["data"]!!.jsonObject.keys,
                "the store's distinct authors moved to a section that states its own age",
            )
            assertNotNull(charts["kinds"]!!.jsonObject["data"]!!.jsonObject["total"], "how many kinds is the histogram's own number now")
        }
    }

    // ---- the newest event -----------------------------------------------------

    /** Read off the histogram's own spans, so the most optimistically dated spam is not the newest. */
    @Test
    fun `the newest event is bounded at the present`() {
        runBlocking {
            val queries = FakeQueries()
            val doc = rollup(queries).compute(StatsTier.CHARTS)
            val spans = queries.asked.filter { it.pipeline == StatsYql.spanBy("kind") }

            assertEquals(listOf(StatsYql.upTo(NOW)), spans.map { it.where }, "one span query, and it stops at now")
            assertEquals(1_754_581_422L, doc.newestEvent())
        }
    }

    /**
     * `newestEvent` is an absolute timestamp, so the maximum of the previous
     * document's two copies and the fresh spans is as true as when it was taken.
     */
    @Test
    fun `the newest event survives a quiet window and never goes backwards`() {
        runBlocking {
            val quiet = rollup(FakeQueries(spans = EMPTY_GROUPS))
            assertNull(
                quiet.compute(StatsTier.CHARTS)["corpus"]!!.jsonObject["data"]!!.jsonObject["newestEvent"],
                "nothing measured and nothing known is an absent number, not a zero",
            )

            val carried = quiet.compute(StatsTier.CHARTS, previousWith(corpusNewest = 1_900_000_000L))
            assertEquals(1_900_000_000L, carried.newestEvent())

            // A document missing `corpus` still carries the newest in its per-kind spans.
            val fromKinds = quiet.compute(StatsTier.CHARTS, previousWith(kindsLastSeen = 1_800_000_000L))
            assertEquals(1_800_000_000L, fromKinds.newestEvent())

            // Fresh spans beat an older carry and lose to a newer one.
            assertEquals(1_754_581_422L, rollup(FakeQueries()).compute(StatsTier.CHARTS, previousWith(corpusNewest = 1_700_000_000L)).newestEvent())
            assertEquals(1_900_000_000L, rollup(FakeQueries()).compute(StatsTier.CHARTS, previousWith(corpusNewest = 1_900_000_000L)).newestEvent())
        }
    }

    // ---- what a failure costs ------------------------------------------------

    /** Which queries can afford the fast cadence is a measurement; a slow refusal is its own problem. */
    @Test
    fun `every query is timed, including the ones that fail`() {
        runBlocking {
            val queries = FakeQueries(failing = setOf(StatsYql.TOTAL))
            val corpus = rollup(queries).compute(StatsTier.CHARTS)["corpus"]!!.jsonObject

            assertEquals("failed", corpus["status"]!!.jsonPrimitive.content, "both of the section's own queries were refused")
            assertTrue(corpus["errors"]!!.jsonObject.keys.containsAll(setOf("events", "futureDated")), "both counts use that pipeline")
            val timings = assertNotNull(corpus["queryMs"]).jsonObject
            assertEquals(setOf("events", "futureDated"), timings.keys, "every attempt is timed, under the key its error would carry")
            assertNull(corpus["data"]!!.jsonObject["events"], "a refused count is absent rather than zero")
        }
    }

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
        const val NOW = 1_800_000_000L

        val KIND_FILTER = Regex("""kind = (\d+)""")

        /**
         * The kinds a counters query may lean on as its only bound. A kind filter bounds the
         * group set, not the walk, so only sparse kinds qualify.
         */
        val SELECTIVE_KINDS = setOf(10040)

        // Vespa 8.733, the captures StatsYqlTest asserts the readers against.
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
