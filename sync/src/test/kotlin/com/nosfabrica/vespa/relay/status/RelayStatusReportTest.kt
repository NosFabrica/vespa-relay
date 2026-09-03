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
 * The four answers, against the band file that produces each.
 *
 * The fixture is written as the file an operator would find on disk — the same
 * convention [SyncCoverageReportTest] uses — because the shape is the contract
 * between two classes and a hand-built map would pin the wrong one.
 */
class RelayStatusReportTest {
    private fun unit(
        relay: String,
        stream: String,
        asks: Int = 1,
        visiting: Boolean = false,
        live: Boolean = false,
        abort: String? = null,
        said: String? = null,
        abortAtSec: Long = 0,
    ) = RelayStatusReport.Unit(relay, stream, asks, visiting, live, abort, said, abortAtSec)

    private fun bands(json: String) = Json.parseToJsonElement(json).jsonObject

    private fun rowsOf(o: JsonObject) = o["rows"]!!.jsonArray.map { it.jsonObject }

    private fun statusesOf(o: JsonObject) =
        o["statuses"]!!
            .jsonArray
            .associate { it.jsonObject["syncStatus"]!!.jsonPrimitive.content to it.jsonObject["pairs"]!!.jsonPrimitive.int }

    @Test
    fun `a settled band is complete, an unsettled one is still paging`() {
        // The one distinction the whole table turns on, and it is quartz's:
        // `complete` on a span means a paged leg DRAINED or a reconcile
        // finished — the past below it is settled — and anything else is a
        // walk still working backwards.
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
                listOf(unit("wss://done.example", "content"), unit("wss://paging.example", "content")),
                nowSeconds = 1_700_000_000,
            )!!
        val rows = rowsOf(out).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals("complete", rows.getValue("wss://done.example")["syncStatus"]!!.jsonPrimitive.content)
        assertEquals("paging", rows.getValue("wss://paging.example")["syncStatus"]!!.jsonPrimitive.content)
        // The last completed reconcile, as an AGE — the closest thing this
        // router has to "last synced", and the only number here a reader can
        // judge without doing the subtraction themselves.
        assertEquals(1_000L, rows.getValue("wss://done.example")["verifiedAgoSec"]!!.jsonPrimitive.long)
        assertNull(rows.getValue("wss://paging.example")["verifiedAgoSec"], "no reconcile has ever finished for it")
        // How far BACK each has reached, which is what "partial" means.
        assertEquals(1_690_000_000L, rows.getValue("wss://paging.example")["coveredFrom"]!!.jsonPrimitive.long)
        assertEquals(1_700_000_000L, rows.getValue("wss://paging.example")["coveredTo"]!!.jsonPrimitive.long)
    }

    @Test
    fun `no band is two different findings, and the abort is what tells them apart`() {
        // THE ROW THAT DID NOT EXIST. A relay the pool has never reached and
        // one it is refused by on every visit have the same absence in the band
        // file — which is why the coverage card can draw neither, and why 92.5%
        // of visits could abort with the page showing nothing wrong.
        val out =
            RelayStatusReport.build(
                bands("{}"),
                listOf(
                    unit("wss://fresh.example", "content"),
                    unit(
                        "wss://walled.example",
                        "content",
                        abort = "the relay would not accept our NIP-42 identity",
                        said = "auth-required: you are not authorized to perform reqs",
                        abortAtSec = 1_699_999_100,
                    ),
                ),
                nowSeconds = 1_700_000_000,
            )!!
        val rows = rowsOf(out).associateBy { it["relay"]!!.jsonPrimitive.content }
        assertEquals("notStarted", rows.getValue("wss://fresh.example")["syncStatus"]!!.jsonPrimitive.content)
        val walled = rows.getValue("wss://walled.example")
        assertEquals("refused", walled["syncStatus"]!!.jsonPrimitive.content)
        // Both halves: the router's reading of WHICH wall, and the relay's own
        // sentence, which is the only thing that says what to do about it.
        assertTrue("NIP-42" in walled["refusedFor"]!!.jsonPrimitive.content)
        assertTrue("not authorized" in walled["relaySaid"]!!.jsonPrimitive.content)
        assertEquals(900L, walled["refusedAgoSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `coverage AND a refusal is both, and the band decides the status`() {
        // A relay that was synced and has since started refusing is not
        // `refused` — it has real coverage — and it is not merely `complete`
        // either. Reporting only one of the two would lose exactly the reading
        // that matters: a relay that has stopped being maintained.
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
                    listOf(unit("wss://was.example", "content", abort = "the relay closed the subscription", said = "rate-limited: slow down", abortAtSec = 1_699_999_000)),
                    nowSeconds = 1_700_000_000,
                )!!,
            ).single()
        assertEquals("complete", row["syncStatus"]!!.jsonPrimitive.content)
        assertTrue("rate-limited" in row["relaySaid"]!!.jsonPrimitive.content, "and the refusal rides beside it")
    }

    @Test
    fun `one unit's asks are folded to its outer edges, and any unsettled band unsettles it`() {
        // A scanning stream owes a relay one ask PER BOUND AUTHOR, so a unit
        // holds many bands. The row is about the RELAY, so the edges are the
        // outer ones — and `complete` may only be claimed when every one of
        // them is, or the table would call a unit finished while one provider's
        // history was still being walked.
        val doc =
            bands(
                """
                {"content": {
                   "{\"kinds\":[30382],\"authors\":[\"a\"]}": {"wss://p.example/": {"min": 1600000000, "max": 1690000000, "complete": true}},
                   "{\"kinds\":[30382],\"authors\":[\"b\"]}": {"wss://p.example/": {"min": 1650000000, "max": 1700000000, "complete": false}}}}
                """.trimIndent(),
            )
        val row = rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://p.example", "content", asks = 2)), 1_700_000_000)!!).single()
        assertEquals("paging", row["syncStatus"]!!.jsonPrimitive.content)
        assertEquals(1_600_000_000L, row["coveredFrom"]!!.jsonPrimitive.long, "the deepest of them")
        assertEquals(1_700_000_000L, row["coveredTo"]!!.jsonPrimitive.long, "and the newest")
        assertEquals(2, row["bands"]!!.jsonPrimitive.int)
        assertEquals(2, row["asks"]!!.jsonPrimitive.int)
    }

    @Test
    fun `the unit is the pair, so one relay is many rows with their own answers`() {
        // The reason a row is not a relay: one relay can be finished for a
        // narrow stream and never started for a wide one, and a per-relay row
        // would have to invent a verdict over the two.
        val doc =
            bands(
                """
                {"indexers": {"{\"kinds\":[10002]}": {"wss://both.example/": {"min": 1600000000, "max": 1700000000, "complete": true}}}}
                """.trimIndent(),
            )
        val out = RelayStatusReport.build(doc, listOf(unit("wss://both.example", "indexers"), unit("wss://both.example", "content")), 1_700_000_000)!!
        val byStream = rowsOf(out).associate { it["stream"]!!.jsonPrimitive.content to it["syncStatus"]!!.jsonPrimitive.content }
        assertEquals(mapOf("indexers" to "complete", "content" to "notStarted"), byStream)
        assertEquals(2, out["pairs"]!!.jsonPrimitive.int)
    }

    @Test
    fun `the statuses partition the pairs, and they close even when the rows are cut`() {
        // The counts are the key to the table, so they are taken off every unit
        // and never off the published rows — a truncated list read as the whole
        // answer is the failure every list in this document discloses against.
        val units =
            (1..RelayStatusReport.MAX_ROWS + 50).map { unit("wss://r$it.example", "content") } +
                unit("wss://bad.example", "content", abort = "the relay closed the subscription")
        val out = RelayStatusReport.build(bands("{}"), units, 1_700_000_000)!!
        val statuses = statusesOf(out)
        assertEquals(units.size, out["pairs"]!!.jsonPrimitive.int)
        assertEquals(units.size, statuses.values.sum(), "the statuses must add up to the pairs they split")
        assertEquals(1, statuses["refused"])
        assertEquals(RelayStatusReport.MAX_ROWS, rowsOf(out).size)
        assertEquals(units.size - RelayStatusReport.MAX_ROWS, out["omitted"]!!.jsonPrimitive.int)
        // WORST FIRST, which is what makes the cut safe: the one row naming a
        // fault is above it, not on page nine.
        assertEquals("wss://bad.example", rowsOf(out).first()["relay"]!!.jsonPrimitive.content)
    }

    @Test
    fun `the four status words are the wire's, and the glossary defines every one`() {
        // These strings ARE the contract: the document publishes them, the page
        // maps them to labels and tones (`SYNC_STATUSES` in shared/sync.js,
        // pinned to the same literal list from the other side), and a reader
        // looks them up in the document's own glossary. Renaming one here
        // without the other two would draw a row labelled with a raw member
        // name and leave the word undefined — the same silent break the four
        // pool words are pinned against, one table over.
        assertEquals(listOf("refused", "notStarted", "paging", "complete"), RelayStatusReport.STATUS_ORDER)
        val defined = StatusVocabulary.TERMS["syncStatus"]!!.jsonPrimitive.content
        for (word in RelayStatusReport.STATUS_ORDER) {
            assertTrue("`$word`" in defined, "the glossary's `syncStatus` entry does not name $word, so a reader meets it undefined")
        }
    }

    @Test
    fun `a router with no prime relays publishes no section at all`() {
        // Absent, not empty. A visit-less deployment has no roster, and an
        // empty table would read as one that has lost its relays — the same
        // call the store section makes.
        assertNull(RelayStatusReport.build(bands("{}"), emptyList(), 1_700_000_000))
    }

    @Test
    fun `a band entry this build cannot read costs a row its claim, never the document`() {
        // Best-effort exactly as SyncCoverageReport is: this runs inside the
        // status tick, and the one hard rule is that it must never cost the
        // mirror its rollup. A band with no `complete` is read as NOT settled,
        // which is the claim that costs a re-walk rather than the one that
        // skips history.
        val doc =
            bands(
                """
                {"content": {"{\"kinds\":[1]}": {"wss://odd.example/": {"min": 1600000000, "max": 1700000000},
                                                 "wss://junk.example/": 7}}}
                """.trimIndent(),
            )
        val rows =
            rowsOf(RelayStatusReport.build(doc, listOf(unit("wss://odd.example", "content"), unit("wss://junk.example", "content")), 1_700_000_000)!!)
                .associate { it["relay"]!!.jsonPrimitive.content to it["syncStatus"]!!.jsonPrimitive.content }
        assertEquals("paging", rows["wss://odd.example"], "no `complete` is read as not settled")
        assertEquals("notStarted", rows["wss://junk.example"], "an entry that is not an object is skipped, not thrown on")
    }

    @Test
    fun `the live marks ride beside the status, not as values of it`() {
        // A pair can be paging AND tailed AND have a worker on it, so they
        // cannot be statuses — and both are absent rather than false when they
        // do not hold, so a row stays as short as its truth.
        val row =
            rowsOf(RelayStatusReport.build(bands("{}"), listOf(unit("wss://busy.example", "content", visiting = true, live = true)), 1_700_000_000)!!)
                .single()
        assertTrue(row["visiting"]!!.jsonPrimitive.content.toBoolean())
        assertTrue(row["tailed"]!!.jsonPrimitive.content.toBoolean())
        val quiet =
            rowsOf(RelayStatusReport.build(bands("{}"), listOf(unit("wss://quiet.example", "content")), 1_700_000_000)!!).single()
        assertFalse(quiet.containsKey("visiting"))
        assertFalse(quiet.containsKey("tailed"))
    }
}
