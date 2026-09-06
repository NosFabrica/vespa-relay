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

import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The four answers, against the band file that produces each. The fixture is the file as
 * written to disk, the contract between two classes.
 */
class RelayStatusReportTest {
    /**
     * A unit owing [askKeys], the filter json strings the roster hands over. Spelled
     * out at every call site because the denominator is what this report gets wrong.
     */
    private fun unit(
        relay: String,
        stream: String,
        vararg askKeys: String,
        visiting: Boolean = false,
        live: Boolean = false,
        abort: String? = null,
        said: String? = null,
        abortAtSec: Long = 0,
        speaksNegentropy: Boolean? = null,
        kindCap: Int? = null,
        watched: Boolean = true,
    ) = RelayStatusReport.PrimeUnit(
        relay = relay,
        stream = stream,
        askKeys = askKeys.toSet(),
        visiting = visiting,
        live = live,
        speaksNegentropy = speaksNegentropy,
        watched = watched,
        kindCap = kindCap,
        abortReason = abort,
        abortSaid = said,
        abortAtSec = abortAtSec,
    )

    /** The one-kind ask for fixtures that do not care. */
    private val plain = """{"kinds":[1]}"""

    private fun bands(json: String) = Json.parseToJsonElement(json).jsonObject

    private fun rowsOf(o: JsonObject) = o["rows"]!!.jsonArray.map { it.jsonObject }

    private fun statusesOf(o: JsonObject) =
        o["statuses"]!!
            .jsonArray
            .associate { it.jsonObject["syncStatus"]!!.jsonPrimitive.content to it.jsonObject["pairs"]!!.jsonPrimitive.int }

    @Test
    fun `a settled band is complete, an unsettled one is still paging`() {
        // quartz's `complete` means a paged leg drained or a reconcile finished.
        val doc =
            bands(
                """
                {"content": {
                   "{\"kinds\":[1]}": {
                     "wss://done.example/":   {"min": 1600000000, "max": 1700000000, "complete": true, "verifiedAt": 1699999000},
                     "wss://paging.example/": {"min": 1690000000, "max": 1700000000, "complete": false}}}}
                """.trimIndent(),
            )
        val out =
            RelayStatusReport.build(
                doc,
                listOf(unit("wss://done.example/", "content", plain), unit("wss://paging.example/", "content", plain)),
                nowSeconds = 1_700_000_000,
            )!!
        val rows = rowsOf(out).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals("complete", rows.getValue("wss://done.example/")["syncStatus"]!!.jsonPrimitive.content)
        assertEquals("paging", rows.getValue("wss://paging.example/")["syncStatus"]!!.jsonPrimitive.content)
        // The last completed reconcile, as an age: the closest thing to "last synced".
        assertEquals(1_000L, rows.getValue("wss://done.example/")["verifiedAgoSec"]!!.jsonPrimitive.long)
        assertNull(rows.getValue("wss://paging.example/")["verifiedAgoSec"], "no reconcile has ever finished for it")
        // How far back each has reached, which is what partial means.
        assertEquals(1_690_000_000L, rows.getValue("wss://paging.example/")["coveredFrom"]!!.jsonPrimitive.long)
        assertEquals(1_700_000_000L, rows.getValue("wss://paging.example/")["coveredTo"]!!.jsonPrimitive.long)
    }

    @Test
    fun `no band is two different findings, and the abort is what tells them apart`() {
        // A relay never reached and one refused on every visit look the same in the band file.
        val out =
            RelayStatusReport.build(
                bands("{}"),
                listOf(
                    unit("wss://fresh.example/", "content", plain),
                    unit(
                        "wss://walled.example/",
                        "content",
                        plain,
                        abort = "the relay would not accept our NIP-42 identity",
                        said = "auth-required: you are not authorized to perform reqs",
                        abortAtSec = 1_699_999_100,
                    ),
                ),
                nowSeconds = 1_700_000_000,
            )!!
        val rows = rowsOf(out).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals("notStarted", rows.getValue("wss://fresh.example/")["syncStatus"]!!.jsonPrimitive.content)
        val walled = rows.getValue("wss://walled.example/")
        assertEquals("refused", walled["syncStatus"]!!.jsonPrimitive.content)
        // The router's reading of which wall, and the relay's own sentence.
        assertTrue("NIP-42" in walled["refusedFor"]!!.jsonPrimitive.content)
        assertTrue("not authorized" in walled["relaySaid"]!!.jsonPrimitive.content)
        assertEquals(900L, walled["refusedAgoSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `coverage AND a refusal is both, and the band decides the status`() {
        // Reporting only one of the two loses a relay that has stopped being maintained.
        val doc =
            bands(
                """
                {"content": {"{\"kinds\":[1]}": {"wss://was.example/": {"min": 1600000000, "max": 1700000000, "complete": true}}}}
                """.trimIndent(),
            )
        val row =
            rowsOf(
                RelayStatusReport.build(
                    doc,
                    listOf(unit("wss://was.example/", "content", plain, abort = "the relay closed the subscription", said = "rate-limited: slow down", abortAtSec = 1_699_999_000)),
                    nowSeconds = 1_700_000_000,
                )!!,
            ).single()
        assertEquals("complete", row["syncStatus"]!!.jsonPrimitive.content)
        assertTrue("rate-limited" in row["relaySaid"]!!.jsonPrimitive.content, "and the refusal rides beside it")
    }

    @Test
    fun `one unit's asks are folded to its outer edges, and any unsettled band unsettles it`() {
        // A unit holds one band per bound author; the row is about the relay, so the edges are the outer ones.
        val doc =
            bands(
                """
                {"content": {
                   "{\"kinds\":[30382],\"authors\":[\"a\"]}": {"wss://p.example/": {"min": 1600000000, "max": 1690000000, "complete": true}},
                   "{\"kinds\":[30382],\"authors\":[\"b\"]}": {"wss://p.example/": {"min": 1650000000, "max": 1700000000, "complete": false}}}}
                """.trimIndent(),
            )
        val row = rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://p.example/", "content", """{"kinds":[30382],"authors":["a"]}""", """{"kinds":[30382],"authors":["b"]}""")), 1_700_000_000)!!).single()
        assertEquals("paging", row["syncStatus"]!!.jsonPrimitive.content)
        assertEquals(1_600_000_000L, row["coveredFrom"]!!.jsonPrimitive.long, "the deepest of them")
        assertEquals(1_700_000_000L, row["coveredTo"]!!.jsonPrimitive.long, "and the newest")
        assertEquals(2, row["bands"]!!.jsonPrimitive.int)
        assertEquals(1, row["settled"]!!.jsonPrimitive.int, "one of the two is settled, which is not the unit")
        assertEquals(2, row["asks"]!!.jsonPrimitive.int)
    }

    @Test
    fun `one settled band out of forty owed asks is not a synced relay`() {
        // The denominator is what the unit owes, not the count of bands the pair holds.
        val owed = (1..40).map { """{"kinds":[30382],"authors":["a$it"]}""" }
        val doc =
            bands(
                """
                {"content": {"${owed.first().replace("\"", "\\\"")}": {"wss://p.example/": {"min": 1600000000, "max": 1700000000, "complete": true}}}}
                """.trimIndent(),
            )
        val row = rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://p.example/", "content", *owed.toTypedArray())), 1_700_000_000)!!).single()
        assertEquals("paging", row["syncStatus"]!!.jsonPrimitive.content)
        assertEquals(40, row["asks"]!!.jsonPrimitive.int)
        assertEquals(1, row["settled"]!!.jsonPrimitive.int)

        // The same unit with every ask settled is complete.
        val all =
            bands(
                "{\"content\": {" +
                    owed.joinToString(",") { "${'"'}${it.replace("\"", "\\\"")}${'"'}: {\"wss://p.example/\": {\"min\": 1600000000, \"max\": 1700000000, \"complete\": true}}" } +
                    "}}",
            )
        val done = rowsOf(RelayStatusReport.build(all, listOf(unit("wss://p.example/", "content", *owed.toTypedArray())), 1_700_000_000)!!).single()
        assertEquals("complete", done["syncStatus"]!!.jsonPrimitive.content)
        assertEquals(40, done["settled"]!!.jsonPrimitive.int)
    }

    @Test
    fun `a band for an ask the roster no longer makes is not this unit's`() {
        // Counted, a left-behind band would move the row's edges and inflate its denominator.
        val doc =
            bands(
                """
                {"content": {
                   "{\"kinds\":[30382],\"authors\":[\"live\"]}":  {"wss://p.example/": {"min": 1690000000, "max": 1700000000, "complete": true}},
                   "{\"kinds\":[30382],\"authors\":[\"dropped\"]}": {"wss://p.example/": {"min": 1500000000, "max": 1700000000, "complete": false}}}}
                """.trimIndent(),
            )
        val row =
            rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://p.example/", "content", """{"kinds":[30382],"authors":["live"]}""")), 1_700_000_000)!!).single()
        assertEquals("complete", row["syncStatus"]!!.jsonPrimitive.content, "the dropped ask's unsettled band is not this unit's")
        assertEquals(1_690_000_000L, row["coveredFrom"]!!.jsonPrimitive.long, "and it does not deepen the row either")
        assertEquals(1, row["bands"]!!.jsonPrimitive.int)
    }

    @Test
    fun `the unit is the pair, so one relay is many rows with their own answers`() {
        // One relay can be finished for a narrow stream and never started for a wide one.
        val doc =
            bands(
                """
                {"indexers": {"{\"kinds\":[10002]}": {"wss://both.example/": {"min": 1600000000, "max": 1700000000, "complete": true}}}}
                """.trimIndent(),
            )
        val out = RelayStatusReport.build(doc, listOf(unit("wss://both.example/", "indexers", """{"kinds":[10002]}"""), unit("wss://both.example/", "content", plain)), 1_700_000_000)!!
        val byStream = rowsOf(out).associate { it["stream"]!!.jsonPrimitive.content to it["syncStatus"]!!.jsonPrimitive.content }
        assertEquals(mapOf("indexers" to "complete", "content" to "notStarted"), byStream)
        assertEquals(2, out["pairs"]!!.jsonPrimitive.int)
    }

    @Test
    fun `the statuses partition the pairs, and they close even when the rows are cut`() {
        // The counts are taken off every unit, never off the published rows.
        val units =
            (1..RelayStatusReport.MAX_ROWS + 50).map { unit("wss://r$it.example/", "content", plain) } +
                unit("wss://bad.example/", "content", plain, abort = "the relay closed the subscription")
        val out = RelayStatusReport.build(bands("{}"), units, 1_700_000_000)!!
        val statuses = statusesOf(out)
        assertEquals(units.size, out["pairs"]!!.jsonPrimitive.int)
        assertEquals(units.size, statuses.values.sum(), "the statuses must add up to the pairs they split")
        assertEquals(1, statuses["refused"])
        assertEquals(RelayStatusReport.MAX_ROWS, rowsOf(out).size)
        assertEquals(units.size - RelayStatusReport.MAX_ROWS, out["omitted"]!!.jsonPrimitive.int)
        // Worst first is what makes the cut safe.
        assertEquals("wss://bad.example/", rowsOf(out).first()["relay"]!!.jsonPrimitive.content)
    }

    @Test
    fun `complete says nothing about current, and the sort no longer pretends it does`() {
        // Complete with a dead tail and nothing newer than last week is worse than paging and live.
        val cold = 1_700_000_000 - 9 * 86_400
        val doc =
            bands(
                """
                {"content": {
                   "{\"kinds\":[1]}": {
                     "wss://cold.example/": {"min": 1600000000, "max": $cold, "complete": true},
                     "wss://busy.example/": {"min": 1600000000, "max": 1699999900, "complete": false}}}}
                """.trimIndent(),
            )
        val out =
            RelayStatusReport.build(
                doc,
                listOf(
                    unit("wss://cold.example/", "content", plain),
                    unit("wss://busy.example/", "content", plain, live = true),
                ),
                nowSeconds = 1_700_000_000,
            )!!
        val rows = rowsOf(out)
        val byRelay = rows.associateBy { it["relay"]!!.jsonPrimitive.content }

        // Both readings on both rows, and neither derived from the other.
        assertEquals("complete", byRelay.getValue("wss://cold.example/")["syncStatus"]!!.jsonPrimitive.content)
        assertEquals("older", byRelay.getValue("wss://cold.example/")["behind"]!!.jsonPrimitive.content)
        assertEquals("paging", byRelay.getValue("wss://busy.example/")["syncStatus"]!!.jsonPrimitive.content)
        assertEquals("current", byRelay.getValue("wss://busy.example/")["behind"]!!.jsonPrimitive.content)

        // The cold complete pair is the fault and comes first.
        assertTrue(
            byRelay
                .getValue("wss://cold.example/")["fault"]!!
                .jsonPrimitive.content
                .toBoolean(),
        )
        assertFalse(byRelay.getValue("wss://busy.example/").containsKey("fault"))
        assertEquals("wss://cold.example/", rows.first()["relay"]!!.jsonPrimitive.content)
    }

    @Test
    fun `a tailed pair is never a staleness fault, however quiet the relay is`() {
        // The tail carries the present between visits, so old content on a tailed pair is a quiet relay.
        val cold = 1_700_000_000 - 60 * 86_400
        val doc =
            bands("""{"content": {"{\"kinds\":[1]}": {"wss://quiet.example/": {"min": 1600000000, "max": $cold, "complete": true}}}}""")
        val row =
            rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://quiet.example/", "content", plain, live = true)), 1_700_000_000)!!).single()
        assertEquals("older", row["behind"]!!.jsonPrimitive.content, "the age is still reported…")
        assertFalse(row.containsKey("fault"), "…but something is listening, so it is not ours to fix")
    }

    @Test
    fun `the freshness buckets partition the pairs too, and the two axes are counted apart`() {
        val doc =
            bands(
                """
                {"content": {"{\"kinds\":[1]}": {
                   "wss://a.example/": {"min": 1600000000, "max": 1699999900, "complete": true},
                   "wss://b.example/": {"min": 1600000000, "max": 1699990000, "complete": true}}}}
                """.trimIndent(),
            )
        val out =
            RelayStatusReport.build(
                doc,
                listOf(
                    unit("wss://a.example/", "content", plain),
                    unit("wss://b.example/", "content", plain),
                    unit("wss://none.example/", "content", plain),
                ),
                1_700_000_000,
            )!!
        val fresh =
            out["freshness"]!!
                .jsonArray
                .associate { it.jsonObject["behind"]!!.jsonPrimitive.content to it.jsonObject["pairs"]!!.jsonPrimitive.int }
        assertEquals(RelayStatusReport.FRESHNESS_ORDER.toSet(), fresh.keys, "every bucket, in the document's own order")
        assertEquals(3, fresh.values.sum(), "the buckets add up to the pairs they split")
        assertEquals(1, fresh["current"], "100 seconds old")
        assertEquals(1, fresh["today"], "ten thousand seconds old — inside the day, past the hour")
        assertEquals(1, fresh["nothing"], "and one with no coverage at all has no age to state")
        // The other partition still closes over the same pairs.
        assertEquals(3, statusesOf(out).values.sum())
    }

    @Test
    fun `the terms a relay serves us on ride the row, and unmeasured is not false`() {
        // A `paging` row against a relay that cannot reconcile never settles by itself.
        val rows =
            rowsOf(
                RelayStatusReport.build(
                    bands("{}"),
                    listOf(
                        unit("wss://neg.example/", "content", plain, speaksNegentropy = true),
                        unit("wss://noneg.example/", "content", plain, speaksNegentropy = false, kindCap = 8),
                        unit("wss://plain.example/", "content", plain),
                    ),
                    1_700_000_000,
                )!!,
            ).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals(
            true,
            rows
                .getValue("wss://neg.example/")["negentropy"]!!
                .jsonPrimitive.content
                .toBoolean(),
        )
        assertEquals(
            false,
            rows
                .getValue("wss://noneg.example/")["negentropy"]!!
                .jsonPrimitive.content
                .toBoolean(),
        )
        assertEquals(8, rows.getValue("wss://noneg.example/")["kindCap"]!!.jsonPrimitive.int)
        // Unmeasured is a third reading, so the member is absent rather than false.
        assertFalse(rows.getValue("wss://plain.example/").containsKey("negentropy"))
        assertFalse(rows.getValue("wss://plain.example/").containsKey("kindCap"))
    }

    @Test
    fun `a relay the mirror syncs and our monitor does not grade is counted, not merely sorted`() {
        // The failure this names: `monitor { sources }` and the streams describe different sets,
        // so the mirror walks relays no verdict of ours covers and nothing says so.
        val doc =
            RelayStatusReport.build(
                bands("{}"),
                listOf(
                    unit("wss://a.example", "content", plain),
                    unit("wss://b.example", "content", plain, watched = false),
                    unit("wss://c.example", "content", plain, watched = false),
                ),
                1_700_000_000,
            )!!

        assertEquals(3, doc["pairs"]!!.jsonPrimitive.int)
        assertEquals(2, doc["unwatched"]!!.jsonPrimitive.int, "both unwatched pairs count, not the relays behind them")
        assertEquals(
            setOf("wss://b.example", "wss://c.example"),
            rowsOf(doc).filter { it["unwatched"] != null }.map { it["relay"]!!.jsonPrimitive.content }.toSet(),
        )
        assertNull(
            rowsOf(doc).single { it["relay"]!!.jsonPrimitive.content == "wss://a.example" }["unwatched"],
            "absent on a watched pair, so the member reads as a flag and never as a false",
        )
    }

    @Test
    fun `the unwatched count is whole even when the rows are cut`() {
        val many = (1..RelayStatusReport.MAX_ROWS + 40).map { unit("wss://r$it.example", "content", plain, watched = false) }
        val doc = RelayStatusReport.build(bands("{}"), many, 1_700_000_000)!!

        assertEquals(many.size, doc["unwatched"]!!.jsonPrimitive.int, "counted over every pair, like the statuses beside it")
        assertTrue(rowsOf(doc).size < many.size, "the fixture has to be cut for the assertion above to mean anything")
    }

    @Test
    fun `an unwatched pair sorts above a watched one, so drift survives the cut`() {
        val doc =
            RelayStatusReport.build(
                bands("{}"),
                // Named so the url tiebreak would put the watched one first if nothing else ranked them.
                listOf(
                    unit("wss://a-watched.example", "content", plain),
                    unit("wss://z-unwatched.example", "content", plain, watched = false),
                ),
                1_700_000_000,
            )!!

        assertEquals(
            "wss://z-unwatched.example",
            rowsOf(doc).first()["relay"]!!.jsonPrimitive.content,
        )
    }

    @Test
    fun `the four status words are the wire's, and the glossary defines every one`() {
        // The page maps these literals to labels in shared/sync.js; renamed here alone, a row shows a raw name.
        assertEquals(listOf("refused", "notStarted", "paging", "complete"), RelayStatusReport.STATUS_ORDER)
        val defined = StatusVocabulary.TERMS["syncStatus"]!!.jsonPrimitive.content
        for (word in RelayStatusReport.STATUS_ORDER) {
            assertTrue("`$word`" in defined, "the glossary's `syncStatus` entry does not name $word, so a reader meets it undefined")
        }
    }

    @Test
    fun `the roster's keys and the band file's keys are the same strings, against the real SyncBands`() {
        // The join is verbatim on (stream, url, filter json); this drives the real `SyncBands` and Filter.
        val bands = SyncBands(null)
        val url = RelayUrlNormalizer.normalize("wss://real.example")
        val filter = Filter(kinds = listOf(1, 30023))
        bands.record(
            "content",
            url,
            filter,
            observedMin = 1_600_000_000,
            observedMax = 1_700_000_000,
            paged = true,
            observedByKind =
                mapOf(
                    1 to SyncCoverage.Span(1_600_000_000, 1_700_000_000, complete = true),
                    30023 to SyncCoverage.Span(1_600_000_000, 1_700_000_000, complete = true),
                ),
            drained = true,
        )

        // `url.url` and `filter.toJson()` are what `VisitPool.primeUnits` hands over.
        val row =
            rowsOf(
                RelayStatusReport.build(
                    bands.snapshot(),
                    listOf(RelayStatusReport.PrimeUnit(url.url, "content", setOf(filter.toJson()), visiting = false, live = false)),
                    1_700_000_000,
                )!!,
            ).single()
        assertEquals("complete", row["syncStatus"]!!.jsonPrimitive.content, "a drained band the real SyncBands wrote must reach its own roster row")
        assertEquals(1, row["settled"]!!.jsonPrimitive.int)
        assertEquals(url.url, row["relay"]!!.jsonPrimitive.content, "and the row names the relay by the url both sides key on")
    }

    @Test
    fun `a router with no prime relays publishes no section at all`() {
        // Absent, not empty: an empty table reads as one that has lost its relays.
        assertNull(RelayStatusReport.build(bands("{}"), emptyList(), 1_700_000_000))
    }

    @Test
    fun `a band entry this build cannot read costs a row its claim, never the document`() {
        // Runs inside the status tick and must never cost the rollup; no `complete` reads as not settled.
        val doc =
            bands(
                """
                {"content": {"{\"kinds\":[1]}": {"wss://odd.example/": {"min": 1600000000, "max": 1700000000},
                                                 "wss://junk.example/": 7}}}
                """.trimIndent(),
            )
        val rows =
            rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://odd.example/", "content", plain), unit("wss://junk.example/", "content", plain)), 1_700_000_000)!!)
                .associate { it["relay"]!!.jsonPrimitive.content to it["syncStatus"]!!.jsonPrimitive.content }
        assertEquals("paging", rows["wss://odd.example/"], "no `complete` is read as not settled")
        assertEquals("notStarted", rows["wss://junk.example/"], "an entry that is not an object is skipped, not thrown on")
    }

    @Test
    fun `the live marks ride beside the status, not as values of it`() {
        // A pair can be paging and tailed and visited at once; absent rather than false when not held.
        val row =
            rowsOf(RelayStatusReport.build(bands("{}"), listOf(unit("wss://busy.example/", "content", plain, visiting = true, live = true)), 1_700_000_000)!!)
                .single()
        assertTrue(row["visiting"]!!.jsonPrimitive.content.toBoolean())
        assertTrue(row["tailed"]!!.jsonPrimitive.content.toBoolean())
        val quiet =
            rowsOf(RelayStatusReport.build(bands("{}"), listOf(unit("wss://quiet.example/", "content", plain)), 1_700_000_000)!!).single()
        assertFalse(quiet.containsKey("visiting"))
        assertFalse(quiet.containsKey("tailed"))
    }
}
