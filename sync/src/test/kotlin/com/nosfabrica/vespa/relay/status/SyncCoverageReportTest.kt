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
package com.nosfabrica.vespa.relay.status

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
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
 * Fixtures are built by [nested] in the shape the router writes, stream → filter → relay.
 */
class SyncCoverageReportTest {
    private val now = 1_800_000_000L

    /** The stream most fixtures nest under. */
    private val mirror = "notes"

    /** One (stream, filter, relay) leg of a file, and whatever it carries there. */
    private data class Leg(
        val stream: String,
        val filter: String,
        val relay: String,
        val body: String,
    )

    private fun leg(
        relay: String,
        filter: String,
        body: String,
        stream: String = mirror,
    ) = Leg(stream, filter, relay, body)

    /** A JSON string key: the filter levels are JSON nested inside JSON. */
    private fun q(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    private fun nested(legs: List<Leg>): String =
        legs.groupBy { it.stream }.entries.joinToString(",", "{", "}") { (stream, ofStream) ->
            q(stream) + ":" +
                ofStream.groupBy { it.filter }.entries.joinToString(",", "{", "}") { (filter, ofFilter) ->
                    q(filter) + ":" + ofFilter.joinToString(",", "{", "}") { q(it.relay) + ":" + it.body }
                }
        }

    private fun bands(vararg legs: Leg) = nested(legs.toList())

    /** The sweep file: `peers`, keyed by the relay alone, beside the same three levels. */
    private fun sweeps(
        vararg legs: Leg,
        peers: String = "{}",
    ) = """{"peers":$peers,"sweeps":${nested(legs.toList())}}"""

    private fun band(
        min: Long,
        max: Long,
        complete: Boolean,
        spans: String? = null,
    ) = """{"min":$min,"max":$max,"complete":$complete,"fullAt":$now${spans?.let { ",\"spans\":$it" } ?: ""}}"""

    private fun mark(
        downTo: Long,
        upTo: Long,
        at: Long = now,
    ) = """{"downTo":$downTo,"upTo":$upTo,"at":$at}"""

    private fun streams(doc: JsonObject?): JsonArray = assertNotNull(doc)["streams"]!!.jsonArray

    private fun rowsOf(stream: JsonObject): List<JsonObject> = stream["rows"]!!.jsonArray.map { it.jsonObject }

    private fun named(
        doc: JsonObject?,
        name: String,
    ): JsonObject = streams(doc).map { it.jsonObject }.single { it["name"]?.jsonPrimitive?.contentOrNull == name }

    @Test
    fun `no files at all is no section, not an empty one`() {
        // A serve-only relay does not mirror: no card and a card saying zero are different claims.
        assertNull(SyncCoverageReport.build(null as String?, null, now))
        assertNull(SyncCoverageReport.build("", "   ", now))
        assertNull(SyncCoverageReport.build("{}", """{"peers":{},"sweeps":{}}""", now))
    }

    @Test
    fun `a corrupt file costs the card, never the rollup`() {
        // A truncated disk or a rollback to an older writer is not something to throw over.
        assertNull(SyncCoverageReport.build("{ not json", "also not json", now))
        // One bad file does not take the other with it.
        val doc = SyncCoverageReport.build(bands(leg("wss://nos.lol/", """{"kinds":[1]}""", band(100, 200, true))), "{{{", now)
        assertEquals(1, streams(doc).size)
    }

    /** The two files nest the same three names, so the join has no key to take apart. */
    @Test
    fun `bands and sweeps join on the stream, the filter and the relay`() {
        val filter = """{"kinds":[0,10002]}"""
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://nos.lol/", filter, band(1_000, 2_000, false))),
                sweeps(
                    leg("wss://nos.lol/", filter, mark(500, 1_500)),
                    peers = """{"wss://nos.lol/":{"target":12500,"cap":12500}}""",
                ),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(mirror, stream["name"]!!.jsonPrimitive.contentOrNull, "the router's own name for the group")
        val row = rowsOf(stream).single()
        // Canonicalised on the way out, so one relay is one string across the document.
        assertEquals("wss://nos.lol", row["relay"]!!.jsonPrimitive.contentOrNull)
        assertEquals(1_000L, row["min"]!!.jsonPrimitive.longOrNull)
        assertEquals(500L, row["sweep"]!!.jsonObject["downTo"]!!.jsonPrimitive.longOrNull)
        // The learned window is keyed by relay alone, a property of the peer.
        assertEquals(12_500L, row["target"]!!.jsonPrimitive.longOrNull)
    }

    /** A sweep's filter drops the time bounds and a band's keeps them; they are one leg either way. */
    @Test
    fun `a time-bounded band and its cursor are one row`() {
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://nos.lol/", """{"kinds":[1],"since":900}""", band(1_000, 2_000, false))),
                sweeps(leg("wss://nos.lol/", """{"kinds":[1]}""", mark(950, 1_200))),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(1, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertNotNull(rowsOf(stream).single()["sweep"], "the cursor must land on its own relay")
        assertNull(stream["narrowedBy"], "a time bound is not something the legs disagree about")
    }

    @Test
    fun `two streams are two groups, whatever they ask`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://nos.lol/", """{"kinds":[1]}""", band(1_000, 2_000, true)),
                    leg("wss://nos.lol/", """{"kinds":[1]}""", band(1_500, 2_000, true), stream = "assertions"),
                ),
                null,
                now,
            )
        assertEquals(2, streams(doc).size)
        assertEquals(listOf("notes", "assertions"), streams(doc).map { it.jsonObject["name"]!!.jsonPrimitive.contentOrNull })
    }

    @Test
    fun `one stream groups every relay it has walked`() {
        val filter = """{"kinds":[0,10002]}"""
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", filter, band(5_000, 9_000, false)),
                    leg("wss://b.example/", filter, band(1_000, 9_000, true)),
                    leg("wss://c.example/", filter, band(3_000, 9_000, true)),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(3, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(2, stream["reconciled"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(1, stream["paged"]!!.jsonPrimitive.longOrNull?.toInt())
        // Deepest first: the relay carrying the group belongs at the top of the staircase.
        assertEquals(
            listOf("wss://b.example", "wss://c.example", "wss://a.example"),
            rowsOf(stream).map { it["relay"]!!.jsonPrimitive.contentOrNull },
        )
    }

    /** A sweep walking newest-first into new ground is the one thing that extends the picture leftward. */
    @Test
    fun `the frame spans the oldest thing anywhere, band or cursor`() {
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://a.example/", """{"kinds":[1]}""", band(5_000, 9_000, false))),
                sweeps(leg("wss://a.example/", """{"kinds":[1]}""", mark(800, 5_000))),
                now,
            )
        assertEquals(800L, assertNotNull(doc)["from"]!!.jsonPrimitive.longOrNull)
        assertEquals(now, doc["to"]!!.jsonPrimitive.longOrNull)
    }

    /** A band is recorded only when a leg finishes, so a relay mid-sweep has a cursor and no band. */
    @Test
    fun `a sweep with no band yet still gets a row`() {
        val doc =
            SyncCoverageReport.build(
                "{}",
                sweeps(leg("wss://new.example/", """{"kinds":[1]}""", mark(700, 900))),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(mirror, stream["name"]!!.jsonPrimitive.contentOrNull)
        assertEquals(1, stream["sweeping"]!!.jsonPrimitive.longOrNull?.toInt())
        // Published from the cursor, the only thing that has said what this stream asks for.
        assertEquals(listOf(1), stream["filter"]!!.jsonObject["kinds"]!!.jsonArray.map { it.jsonPrimitive.int })
        val row = rowsOf(stream).single()
        assertNull(row["min"], "nothing durable has been recorded for this relay yet")
        assertEquals(700L, row["sweep"]!!.jsonObject["downTo"]!!.jsonPrimitive.longOrNull)
    }

    /** `sweeping` counts the cursors this document places; a cursor missing an edge has no span to draw. */
    @Test
    fun `a cursor with no readable span is not counted as sweeping`() {
        val filter = """{"kinds":[1]}"""
        val doc =
            SyncCoverageReport.build(
                "{}",
                sweeps(
                    leg("wss://good.example/", filter, mark(700, 900)),
                    leg("wss://half.example/", filter, """{"upTo":900,"at":$now}"""),
                ),
                now,
            )
        val stream = streams(doc).single().jsonObject
        // Both relays stay on the card; a row that can say nothing about one shows it was reached.
        assertEquals(2, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(1, stream["sweeping"]!!.jsonPrimitive.longOrNull?.toInt())
        val byRelay = rowsOf(stream).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals(700L, byRelay["wss://good.example"]!!["sweep"]!!.jsonObject["downTo"]!!.jsonPrimitive.longOrNull)
        assertNull(byRelay["wss://half.example"]!!["sweep"], "an unreadable cursor has no span to place")
    }

    /** A band holds a span per kind, so a row drawn from the outer `min`/`max` alone over-claims. */
    @Test
    fun `per-kind spans narrow the claim when they disagree`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg(
                        "wss://a.example/",
                        """{"kinds":[0,30382]}""",
                        band(1_000, 9_000, false, spans = """{"0":{"min":1000,"max":9000},"30382":{"min":6000,"max":8000}}"""),
                    ),
                ),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertEquals(1_000L, row["min"]!!.jsonPrimitive.longOrNull, "the band still claims what it claims")
        // The intersection is the part no kind is missing from.
        assertEquals(6_000L, row["everyKindMin"]!!.jsonPrimitive.longOrNull)
        assertEquals(8_000L, row["everyKindMax"]!!.jsonPrimitive.longOrNull)
    }

    @Test
    fun `agreeing spans are not written twice`() {
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://a.example/", """{"kinds":[1]}""", band(1_000, 9_000, true, spans = """{"1":{"min":1000,"max":9000}}"""))),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        // Emitting the agreeing case would double the array on the one section with a row per relay.
        assertNull(row["everyKindMin"])
        assertTrue(row["complete"]!!.jsonPrimitive.booleanOrNull == true)
    }

    /** Disjoint per-kind spans are a real state and must not come back as a bar of negative width. */
    @Test
    fun `an empty intersection falls back rather than inverting`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg(
                        "wss://a.example/",
                        """{"kinds":[0,1]}""",
                        band(1_000, 9_000, false, spans = """{"0":{"min":1000,"max":2000},"1":{"min":8000,"max":9000}}"""),
                    ),
                ),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertNull(row["everyKindMin"], "no window is covered for every kind; do not invent an inverted one")
    }

    /** A pre-spans file still charts, which is why min/max are still written. */
    @Test
    fun `a band file written before per-kind spans still reads`() {
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://a.example/", """{"kinds":[1]}""", """{"min":10,"max":20,"complete":true}""")),
                null,
                now,
            )
        val row = rowsOf(streams(doc).single().jsonObject).single()
        assertEquals(10L, row["min"]!!.jsonPrimitive.longOrNull)
        assertNull(row["everyKindMin"])
    }

    /** quartz records edges up to a day ahead, so future-dated bands alone could put `from` past `to`. */
    @Test
    fun `a future-dated band cannot invert the frame`() {
        val doc =
            SyncCoverageReport.build(
                bands(leg("wss://a.example/", """{"kinds":[1]}""", band(now + 3_600, now + 7_200, false))),
                null,
                now,
            )
        val from = assertNotNull(doc)["from"]!!.jsonPrimitive.longOrNull!!
        val to = doc["to"]!!.jsonPrimitive.longOrNull!!
        assertTrue(from <= to, "the frame must never invert: from=$from to=$to")
    }

    /** Relays sharing one ask store it once; the nesting keeps a many-author filter affordable. */
    @Test
    fun `many relays sharing one large filter group as one stream`() {
        val authors = (0 until 300).joinToString(",") { "\"${"%064x".format(it)}\"" }
        val filter = """{"kinds":[0,10002],"authors":[$authors]}"""
        val legs = (0 until 400).map { leg("wss://relay-$it.example/", filter, band(1_000L + it, 9_000, it % 3 == 0)) }
        val doc = SyncCoverageReport.build(bands(*legs.toTypedArray()), null, now)
        val stream = streams(doc).single().jsonObject
        assertEquals(400, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(134, stream["reconciled"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(listOf(0, 10002), stream["filter"]!!.jsonObject["kinds"]!!.jsonArray.map { it.jsonPrimitive.int })
        // Every leg agrees on the authors and they are still not published: 300 hex keys per stats poll.
        assertNull(stream["filter"]!!.jsonObject["authors"])
        assertEquals(listOf("authors"), stream["narrowedBy"]!!.jsonArray.map { it.jsonPrimitive.contentOrNull })
        // One leg per relay, so `legs` would say nothing `relays` does not.
        assertNull(stream["legs"])
    }

    /** A `relaySource` binding `authors` gives every relay its own filter; they are legs of one stream. */
    @Test
    fun `relays a narrow gave their own authors are one stream, not one each`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", """{"kinds":[30023],"authors":["aa"]}""", band(5_000, 9_000, true)),
                    leg("wss://b.example/", """{"kinds":[30023],"authors":["bb"]}""", band(1_000, 9_000, true)),
                    leg("wss://c.example/", """{"kinds":[30023],"authors":["cc","dd"]}""", band(3_000, 9_000, false)),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(3, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(2, stream["reconciled"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(3, rowsOf(stream).size)
        // Naming the authors as varying tells this from the same kinds asked of every relay whole.
        assertEquals(listOf(30023), stream["filter"]!!.jsonObject["kinds"]!!.jsonArray.map { it.jsonPrimitive.int })
        assertEquals(listOf("authors"), stream["narrowedBy"]!!.jsonArray.map { it.jsonPrimitive.contentOrNull })
    }

    /** A bound ask and an unbound one for the same kind differ; the group must not publish the narrower. */
    @Test
    fun `a member only some legs carry is reported as varying`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", """{"kinds":[30023],"authors":["aa"]}""", band(5_000, 9_000, true)),
                    leg("wss://b.example/", """{"kinds":[30023]}""", band(1_000, 9_000, true)),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(listOf(30023), stream["filter"]!!.jsonObject["kinds"]!!.jsonArray.map { it.jsonPrimitive.int })
        assertEquals(listOf("authors"), stream["narrowedBy"]!!.jsonArray.map { it.jsonPrimitive.contentOrNull })
    }

    /** One relay's ask can be split into several legs, so it arrives several times in one group. */
    @Test
    fun `several legs against one relay merge into one row`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["aa"]}""", band(1_000, 4_000, true)),
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["bb"]}""", band(3_000, 9_000, false)),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(1, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(2, stream["legs"]!!.jsonPrimitive.longOrNull?.toInt())
        // The outer edges union, and one paged leg costs the relay its `reconciled` tone.
        val row = rowsOf(stream).single()
        assertEquals(1_000L, row["min"]!!.jsonPrimitive.longOrNull)
        assertEquals(9_000L, row["max"]!!.jsonPrimitive.longOrNull)
        assertEquals(false, row["complete"]!!.jsonPrimitive.booleanOrNull)
        assertEquals(0, stream["reconciled"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(1, stream["paged"]!!.jsonPrimitive.longOrNull?.toInt())
        // The legs overlap at 3,000–4,000, the only span neither is missing from.
        assertEquals(3_000L, row["everyKindMin"]!!.jsonPrimitive.longOrNull)
        assertEquals(4_000L, row["everyKindMax"]!!.jsonPrimitive.longOrNull)
    }

    /** `legs` is compared against the band-bearing relays; a relay on its first sweep has no band. */
    @Test
    fun `a relay sweeping its first leg cannot hide a merge`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["aa"]}""", band(1_000, 4_000, true)),
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["bb"]}""", band(3_000, 9_000, true)),
                ),
                sweeps(leg("wss://b.example/", """{"kinds":[30382],"authors":["cc"]}""", mark(900, 4_000))),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(2, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertEquals(2, stream["legs"]!!.jsonPrimitive.longOrNull?.toInt(), "two bands on one relay is a merge worth stating")
    }

    /** Windowed reconciliation writes a band per window, whose `since`/`until` are that window's. */
    @Test
    fun `a window's own time bounds are not published as the stream's`() {
        val doc =
            SyncCoverageReport.build(
                bands(
                    leg("wss://a.example/", """{"kinds":[1],"since":1000,"until":2000}""", band(1_000, 2_000, true)),
                    leg("wss://b.example/", """{"kinds":[1],"since":2000,"until":3000}""", band(2_000, 3_000, true)),
                ),
                null,
                now,
            )
        val stream = streams(doc).single().jsonObject
        val filter = stream["filter"]!!.jsonObject
        assertEquals(listOf(1), filter["kinds"]!!.jsonArray.map { it.jsonPrimitive.int })
        assertNull(filter["since"])
        assertNull(filter["until"])
        assertNull(stream["narrowedBy"], "the bounds are dropped, not reported as a difference between the legs")
    }

    /** A sweep on each leg is one relay moving, not two. */
    @Test
    fun `several cursors against one relay merge into one slice`() {
        val doc =
            SyncCoverageReport.build(
                null,
                sweeps(
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["aa"]}""", mark(2_000, 6_000, at = now - 90)),
                    leg("wss://a.example/", """{"kinds":[30382],"authors":["bb"]}""", mark(900, 4_000)),
                ),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertEquals(1, stream["sweeping"]!!.jsonPrimitive.longOrNull?.toInt())
        val sweep = rowsOf(stream).single()["sweep"]!!.jsonObject
        assertEquals(900L, sweep["downTo"]!!.jsonPrimitive.longOrNull)
        assertEquals(6_000L, sweep["upTo"]!!.jsonPrimitive.longOrNull)
        // The most recent advance; the older cursor's `at` would understate a relay that just moved.
        assertEquals(now, sweep["at"]!!.jsonPrimitive.longOrNull)
    }

    // ---- pre-stream files, written before the format nested -----------------

    /** A band's flat key: relay, a space, then the whole filter. */
    private fun flat(vararg entries: Pair<String, String>) = entries.joinToString(",", "{", "}") { q(it.first) + ":" + it.second }

    private fun flatSweeps(vararg entries: Pair<String, String>) = """{"peers":{},"sweeps":${flat(*entries)}}"""

    /** A pre-stream file holds pairs no stream has claimed; dropped, a walked relay charts as un-walked. */
    @Test
    fun `pre-stream keys still group, unnamed, by their filter's shape`() {
        val doc =
            SyncCoverageReport.build(
                flat(
                    """wss://a.example/ {"kinds":[30023],"authors":["aa"]}""" to band(5_000, 9_000, true),
                    """wss://b.example/ {"kinds":[30023],"authors":["bb"]}""" to band(1_000, 9_000, true),
                ),
                // A pipe in this one, and the time bounds already stripped.
                flatSweeps("""wss://c.example/|{"kinds":[30023],"authors":["cc"]}""" to mark(900, 4_000)),
                now,
            )
        val stream = streams(doc).single().jsonObject
        assertNull(stream["name"], "a flat key names no stream, and the page must not invent one")
        assertEquals(3, stream["relays"]!!.jsonPrimitive.longOrNull?.toInt(), "the two files' shapes still join")
        assertEquals(listOf("authors"), stream["narrowedBy"]!!.jsonArray.map { it.jsonPrimitive.contentOrNull })
    }

    @Test
    fun `a pre-stream ask that differs in more than time stays its own group`() {
        val doc =
            SyncCoverageReport.build(
                flat(
                    """wss://a.example/ {"kinds":[1]}""" to band(1_000, 2_000, true),
                    """wss://a.example/ {"kinds":[30382]}""" to band(1_500, 2_000, true),
                ),
                null,
                now,
            )
        assertEquals(2, streams(doc).size, "a different ask is not covered because this one was")
    }

    /** Mid-migration a file nests claimed streams beside flat leftovers, told apart by a band's `min`. */
    @Test
    fun `a nested stream and a pre-stream leftover coexist in one file`() {
        val claimed = bands(leg("wss://a.example/", """{"kinds":[1]}""", band(1_000, 9_000, true)))
        val leftover = q("""wss://b.example/ {"kinds":[7]}""") + ":" + band(2_000, 8_000, false)
        val doc = SyncCoverageReport.build(claimed.dropLast(1) + "," + leftover + "}", null, now)
        assertEquals(2, streams(doc).size)
        assertEquals(1, named(doc, mirror)["relays"]!!.jsonPrimitive.longOrNull?.toInt())
        assertNull(streams(doc)[1].jsonObject["name"], "the leftover is charted too, and honestly unnamed")
    }

    @Test
    fun `a pre-stream key with no separator is skipped rather than charted as a relay`() {
        val doc = SyncCoverageReport.build("""{"nonsense-with-no-space":{"min":10,"max":20,"complete":true}}""", null, now)
        assertNull(doc, "an unsplittable flat key names no relay and no filter")
    }
}
