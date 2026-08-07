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

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The router's two state files, folded into the coverage the stats page charts.
 *
 * Every fixture below is in the shape the router ACTUALLY writes — captured
 * from `SyncBands` and `SweepState` rather than written from the docs, because
 * the two files disagree about their own key separator and that is precisely
 * the kind of thing a hand-written fixture gets wrong in the same direction as
 * the parser.
 */
class SyncCoverageReportTest {
    private val now = 1_800_000_000L

    /**
     * A band key: relay, a SPACE, then the whole filter — and the filter's own
     * quotes escaped, because the key is a JSON string containing JSON.
     */
    private fun bands(vararg entries: Pair<String, String>) = "{" + entries.joinToString(",") { "\"${it.first.replace("\"", "\\\"")}\": ${it.second}" } + "}"

    private fun band(
        min: Long,
        max: Long,
        complete: Boolean,
        spans: String? = null,
    ) = """{"min":$min,"max":$max,"complete":$complete,"fullAt":$now${spans?.let { ",\"spans\":$it" } ?: ""}}"""

    private fun streams(doc: JsonObject?): JsonArray = assertNotNull(doc)["streams"]!!.jsonArray

    private fun rowsOf(stream: JsonObject): List<JsonObject> = stream["rows"]!!.jsonArray.map { it.jsonObject }

    @Test
    fun `no files at all is no section, not an empty one`() {
        // A serve-only relay does not mirror. "No card" and "a card saying
        // zero" are different claims and only one of them is true here.
        assertNull(SyncCoverageReport.build(null, null, now))
        assertNull(SyncCoverageReport.build("", "   ", now))
        assertNull(SyncCoverageReport.build("{}", """{"peers":{},"sweeps":{}}""", now))
    }

    @Test
    fun `a corrupt file costs the card, never the rollup`() {
        // Both files are written temp-then-move, but a truncated disk or a
        // rollback to an older writer is not something to throw over.
        assertNull(SyncCoverageReport.build("{ not json", "also not json", now))
        // ...and one bad file does not take the other with it.
        val doc = SyncCoverageReport.build(bands("wss://nos.lol/ {\"kinds\":[1]}" to band(100, 200, true)), "{{{", now)
        assertEquals(1, streams(doc).size)
    }

    /**
     * The two files key the same pair differently — a SPACE in the band file,
     * a PIPE in the sweep file — so a cursor only lands on its relay if both
     * are split on their own separator.
     */
    @Test
    fun `bands and sweeps join despite their different key separators`() {
        val doc =
            SyncCoverageReport.build(
                bands("wss://nos.lol/ {\"kinds\":[0,10002]}" to band(1_000, 2_000, false)),
                """
                {"peers":{"wss://nos.lol/":{"target":12500,"cap":12500}},
                 "sweeps":{"wss://nos.lol/|{\"kinds\":[0,10002]}":{"downTo":500,"upTo":1500,"at":$now}}}
                """.trimIndent(),
                now,
            )
        val rows = rowsOf(streams(doc).single().jsonObject)
        val row = rows.single()
        // Canonicalised on the way out — the router writes `wss://nos.lol/` and
        // the relay distribution table two cards up says `wss://nos.lol`. One
        // relay must be one string across the whole document.
        assertEquals("wss://nos.lol", row["relay"]!!.jsonPrimitive.contentOrNull)
        assertEquals(1_000L, row["min"]!!.jsonPrimitive.longOrNull)
        assertEquals(500L, row["sweep"]!!.jsonObject["downTo"]!!.jsonPrimitive.longOrNull)
        // The learned window is keyed by relay alone — a property of the peer.
        assertEquals(12_500L, row["target"]!!.jsonPrimitive.longOrNull)
    }

    /**
     * A sweep's key drops `since`/`until`/`limit`; a band's key keeps them. A
     * stream whose filter carries a time bound must still find its own cursor,
     * or a relay mid-walk renders as idle on the one card built to show it.
     */
    @Test
    fun `a time-bounded filter still matches its cursor`() {
        val doc =
            SyncCoverageReport.build(
                bands("wss://nos.lol/ {\"kinds\":[1],\"since\":900}" to band(1_000, 2_000, false)),
                """{"peers":{},"sweeps":{"wss://nos.lol/|{\"kinds\":[1]}":{"downTo":950,"upTo":1200,"at":$now}}}""",
                now,
            )
        // ONE stream, not two: the band and the cursor are the same coverage
        // question and a shape that kept `since` would have split them.
        val stream = streams(doc).single().jsonObject
        assertEquals(1, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertNotNull(rowsOf(stream).single()["sweep"], "the cursor must land on its own relay")
    }

    @Test
    fun `filters differing in more than time stay separate streams`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    "wss://nos.lol/ {\"kinds\":[1]}" to band(1_000, 2_000, true),
                    "wss://nos.lol/ {\"kinds\":[30382]}" to band(1_500, 2_000, true),
                ),
                null,
                now,
            )
        assertEquals(2, streams(doc).size, "a different ask is not covered because this one was")
    }

    @Test
    fun `one filter groups every relay that has walked it`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    "wss://a.example/ {\"kinds\":[0,10002]}" to band(5_000, 9_000, false),
                    "wss://b.example/ {\"kinds\":[0,10002]}" to band(1_000, 9_000, true),
                    "wss://c.example/ {\"kinds\":[0,10002]}" to band(3_000, 9_000, true),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(3, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(2, stream["reconciled"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(1, stream["paged"]!!.jsonPrimitive.longOrNull?.toInt())
        // Deepest first: the staircase is the shape of the answer, and the
        // relay carrying the group belongs at the top of it.
        assertEquals(
            listOf("wss://b.example", "wss://c.example", "wss://a.example"),
            rowsOf(stream).map { it["relay"]!!.jsonPrimitive.contentOrNull },
        )
    }

    /**
     * The frame is taken from the data, and it has to include a cursor that
     * reaches below every band — a sweep walking newest-first into new ground
     * is the one thing that extends the picture leftward.
     */
    @Test
    fun `the frame spans the oldest thing anywhere, band or cursor`() {
        val doc =
            SyncCoverageReport.build(
                bands("wss://a.example/ {\"kinds\":[1]}" to band(5_000, 9_000, false)),
                """{"peers":{},"sweeps":{"wss://a.example/|{\"kinds\":[1]}":{"downTo":800,"upTo":5000,"at":$now}}}""",
                now,
            )
        assertEquals(800L, assertNotNull(doc)["from"]!!.jsonPrimitive.longOrNull)
        assertEquals(now, doc["to"]!!.jsonPrimitive.longOrNull)
    }

    /**
     * A relay mid-FIRST-sweep has a cursor and no band at all: `SyncBands`
     * records nothing until a leg finishes. That relay is exactly what this
     * card exists to show, so it must get a row.
     */
    @Test
    fun `a sweep with no band yet still gets a row`() {
        val doc =
            SyncCoverageReport.build(
                "{}",
                """{"peers":{},"sweeps":{"wss://new.example/|{\"kinds\":[1]}":{"downTo":700,"upTo":900,"at":$now}}}""",
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(1, stream["sweeping"]!!.jsonPrimitive.longOrNull?.toInt())
        val row = rowsOf(stream).single()
        assertNull(row["min"], "nothing durable has been recorded for this relay yet")
        assertEquals(700L, row["sweep"]!!.jsonObject["downTo"]!!.jsonPrimitive.longOrNull)
    }

    /**
     * The known open bug, made visible instead of charted as coverage: one band
     * holds a span per kind, and `min`/`max` are the OUTER edges — so kind 0
     * vouches for kind 30382 and a row drawn from them alone over-claims.
     */
    @Test
    fun `per-kind spans narrow the claim when they disagree`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    "wss://a.example/ {\"kinds\":[0,30382]}" to
                        band(1_000, 9_000, false, spans = """{"0":{"min":1000,"max":9000},"30382":{"min":6000,"max":8000}}"""),
                ),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertEquals(1_000L, row["min"]!!.jsonPrimitive.longOrNull, "the band still claims what it claims")
        // ...and the intersection is the part no kind is missing from.
        assertEquals(6_000L, row["everyKindMin"]!!.jsonPrimitive.longOrNull)
        assertEquals(8_000L, row["everyKindMax"]!!.jsonPrimitive.longOrNull)
    }

    @Test
    fun `agreeing spans are not written twice`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    "wss://a.example/ {\"kinds\":[1]}" to
                        band(1_000, 9_000, true, spans = """{"1":{"min":1000,"max":9000}}"""),
                ),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        // The overwhelming case. Emitting it would double the array to say
        // nothing, on the one section that carries a row per relay.
        assertNull(row["everyKindMin"])
        assertTrue(row["complete"]!!.jsonPrimitive.booleanOrNull == true)
    }

    /**
     * Disjoint per-kind spans are a real state — two kinds walked over
     * non-overlapping windows — and must not come back inverted, which would
     * draw a bar with a negative width.
     */
    @Test
    fun `an empty intersection falls back rather than inverting`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    "wss://a.example/ {\"kinds\":[0,1]}" to
                        band(1_000, 9_000, false, spans = """{"0":{"min":1000,"max":2000},"1":{"min":8000,"max":9000}}"""),
                ),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertNull(row["everyKindMin"], "no window is covered for every kind; do not invent an inverted one")
    }

    /** A pre-spans file still charts — that is the whole reason min/max are still written. */
    @Test
    fun `a band file written before per-kind spans still reads`() {
        val doc = SyncCoverageReport.build(bands("wss://a.example/ {\"kinds\":[1]}" to """{"min":10,"max":20,"complete":true}"""), null, now)
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertEquals(10L, row["min"]!!.jsonPrimitive.longOrNull)
        assertNull(row["everyKindMin"])
    }

    @Test
    fun `a key with no separator is skipped rather than charted as a relay`() {
        val doc = SyncCoverageReport.build("""{"nonsense-with-no-space":{"min":10,"max":20,"complete":true}}""", null, now)
        assertNull(doc, "an unsplittable key names no relay and no filter")
    }
}
