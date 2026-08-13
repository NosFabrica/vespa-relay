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
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The relay's read of the router's progress file.
 *
 * Two properties, and both are about not trusting the writer: the partition is
 * RE-DERIVED here rather than forwarded, so `accountedFor` is a statement about
 * the document being served; and every member is rebuilt individually, so a
 * hand-edited or half-migrated file can cost this object and nothing else.
 */
class SyncProgressReportTest {
    private val live =
        """
        {
          "writtenAt": 1770000000,
          "streams": [
            {
              "name": "content",
              "phase": "fetching",
              "phaseForSec": 412,
              "cycle": {
                "startedAt": 1769999000,
                "outcome": "running",
                "urls": {"discovered": 16752, "foldedOntoAnother": 11429, "excluded": 0, "taken": 5323},
                "hosts": 850,
                "taken": {"delivered": 2200, "nothingNew": 900, "unreachable": 800,
                          "transferFailed": 100, "noRoute": 1000, "hostStruckOut": 200,
                          "knownDead": 100, "torUnavailable": 0, "pending": 23},
                "foldedOnto": {"relays": [{"relay": "wss://nostr.oxtr.dev/", "urls": 55,
                                           "examples": ["wss://nostr.oxtr.dev/alpha", "wss://nostr.oxtr.dev/beta", "wss://nostr.oxtr.dev/x"]}],
                               "omitted": 480},
                "balanced": true,
                "received": 481203
              }
            }
          ]
        }
        """.trimIndent()

    private fun firstCycle(doc: JsonObject) = (doc["streams"] as JsonArray)[0].jsonObject["cycle"]!!.jsonObject

    @Test
    fun `the disposition accounts for every discovered url`() {
        // The number this whole file exists to produce: 16,752 discovered against
        // 5,323 band-bearing used to leave ~11,400 with no published disposition
        // at all.
        val doc = SyncProgressReport.build(live, nowSeconds = 1_770_000_060)!!
        val cycle = firstCycle(doc)
        val urls = cycle["urls"]!!.jsonObject
        val taken = cycle["taken"]!!.jsonObject

        assertEquals(16_752L, urls["discovered"]!!.jsonPrimitive.long)
        assertEquals(
            urls["discovered"]!!.jsonPrimitive.long,
            urls["foldedOntoAnother"]!!.jsonPrimitive.long + urls["excluded"]!!.jsonPrimitive.long + urls["taken"]!!.jsonPrimitive.long,
        )
        assertEquals(5_323L, taken.values.sumOf { it.jsonPrimitive.long })
        assertTrue(cycle["accountedFor"]!!.jsonPrimitive.booleanOrNull!!)
    }

    @Test
    fun `staleness is measured against THIS rollup's clock, not the file's`() {
        // A router that stopped writing an hour ago has to say so however recent
        // its own last timestamp looked.
        val doc = SyncProgressReport.build(live, nowSeconds = 1_770_003_600)!!

        assertEquals(3_600L, doc["staleForSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a file stamped in the future is skew, not a negative age`() {
        val doc = SyncProgressReport.build(live, nowSeconds = 1_769_999_999)!!

        assertEquals(0L, doc["staleForSec"]!!.jsonPrimitive.long, "a negative age reads as a bug in the relay")
    }

    @Test
    fun `a partition that does not hold is published as not holding`() {
        // Forwarding the writer's own `balanced` would make this side blind to a
        // file that says one thing and carries another.
        val broken = live.replace("\"delivered\": 2200", "\"delivered\": 2500")
        val cycle = firstCycle(SyncProgressReport.build(broken, nowSeconds = 1_770_000_000)!!)

        assertFalse(cycle["accountedFor"]!!.jsonPrimitive.booleanOrNull!!, "the outcomes no longer sum to `taken`")
        assertTrue(cycle["balanced"]!!.jsonPrimitive.booleanOrNull!!, "and the router still thinks they do — which localises the fault")
    }

    @Test
    fun `an outcome the file omits counts as zero rather than shrinking the sum`() {
        // The member list is fixed on this side. Taking it from whatever the
        // writer emitted would let a future router widen the total silently.
        val thin =
            """
            {"writtenAt": 1, "streams": [{"name": "s", "cycle": {
              "urls": {"discovered": 4, "foldedOntoAnother": 0, "taken": 4},
              "taken": {"delivered": 4}}}]}
            """.trimIndent()
        val taken = firstCycle(SyncProgressReport.build(thin, nowSeconds = 1)!!)["taken"]!!.jsonObject

        assertEquals(10, taken.size, "every outcome is named, present in the file or not")
        assertEquals(0L, taken["noRoute"]!!.jsonPrimitive.long)
        assertEquals(4L, taken.values.sumOf { it.jsonPrimitive.long })
    }

    @Test
    fun `the fold summary names survivors, capped again on this side, truncation disclosed`() {
        // The router already bounds its list; this bounds it a second time
        // rather than trusting that it did, because the cap is the only thing
        // between a hand-edited file and an unbounded array in a served
        // document.
        val fold = firstCycle(SyncProgressReport.build(live, nowSeconds = 1_770_000_000)!!)["foldedOnto"]!!.jsonObject
        val row = (fold["relays"] as JsonArray)[0].jsonObject

        assertEquals("wss://nostr.oxtr.dev/", row["relay"]!!.jsonPrimitive.content)
        assertEquals(55L, row["urls"]!!.jsonPrimitive.long)
        assertEquals(2, (row["examples"] as JsonArray).size, "examples are capped on this side too")
        assertEquals(480L, fold["omitted"]!!.jsonPrimitive.long, "and what was left out is carried through")
    }

    @Test
    fun `the two not-dialled-for-being-dead states are counted apart`() {
        // One is out until a signed record ages past its TTL; the other is back
        // on the next cycle. As one number they answered "will it try again"
        // both ways at once.
        val taken = firstCycle(SyncProgressReport.build(live, nowSeconds = 1_770_000_000)!!)["taken"]!!.jsonObject

        assertEquals(200L, taken["hostStruckOut"]!!.jsonPrimitive.long)
        assertEquals(100L, taken["knownDead"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a stream with no name says nothing and is dropped`() {
        val doc = SyncProgressReport.build("""{"writtenAt": 1, "streams": [{"phase": "idle"}]}""", nowSeconds = 1)!!

        assertEquals(0, (doc["streams"] as JsonArray).size)
    }

    @Test
    fun `an object where a name should be costs this object, not the section`() {
        // `jsonPrimitive` is an ASSERTION and throws on an object. One of these
        // in a file this process did not write would take the whole sync section
        // — the coverage half with it.
        val doc = SyncProgressReport.build("""{"writtenAt": 1, "streams": [{"name": {}}]}""", nowSeconds = 1)!!

        assertEquals(0, (doc["streams"] as JsonArray).size)
    }

    @Test
    fun `the url partition holds when the fold synthesised a survivor`() {
        // `foldOnto` MERGES onto a canonical, and a canonical discovery did not
        // itself hand over is added to the result — so the counts were once
        // inferred from `candidates - relays`, which made `taken` over-count and
        // left a healthy finished cycle with `pending` stuck above zero.
        val synthesised =
            """
            {"writtenAt": 1, "streams": [{"name": "s", "cycle": {"outcome": "completed",
              "urls": {"discovered": 10, "foldedOntoAnother": 4, "excluded": 1, "taken": 5},
              "taken": {"delivered": 5}}}]}
            """.trimIndent()
        val cycle = firstCycle(SyncProgressReport.build(synthesised, nowSeconds = 1)!!)

        assertEquals(0L, cycle["taken"]!!.jsonObject["pending"]!!.jsonPrimitive.long, "a finished cycle has nothing outstanding")
        assertTrue(cycle["accountedFor"]!!.jsonPrimitive.booleanOrNull!!)
    }

    @Test
    fun `a corrupt or absent file is absent, never an exception`() {
        assertNull(SyncProgressReport.build(null, nowSeconds = 1))
        assertNull(SyncProgressReport.build("", nowSeconds = 1))
        assertNull(SyncProgressReport.build("{not json", nowSeconds = 1))
        assertNull(SyncProgressReport.build("[]", nowSeconds = 1))
        assertNull(SyncProgressReport.build("{}", nowSeconds = 1), "neither a heartbeat nor a stream is not a router being quiet")
    }

    @Test
    fun `a heartbeat with no streams still publishes, because that is the finding`() {
        // A router that is up and running nothing is a real state, and the one
        // the heartbeat exists to distinguish from a router that is gone.
        val doc = SyncProgressReport.build("""{"writtenAt": 1770000000, "streams": []}""", nowSeconds = 1_770_000_010)!!

        assertEquals(10L, doc["staleForSec"]!!.jsonPrimitive.long)
        assertEquals(0, (doc["streams"] as JsonArray).size)
    }

    @Test
    fun `a row this side cannot read is counted, not silently dropped`() {
        // The contract this object states about itself. A truncated list that
        // does not disclose the truncation reads as the whole answer — and for
        // `inFlight` the whole answer is the thing an operator is chasing, so a
        // url that vanished because its row was malformed is the worst possible
        // silence.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "streams": [{"name": "content",
                 "inFlight": {"relays": [{"relay": "wss://good.example/", "heldForSec": 5, "events": 1, "quietForSec": 0},
                                         {"relay": {}, "heldForSec": 9},
                                         {"heldForSec": 9}],
                              "omitted": 7}}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!

        val f =
            (out["streams"] as JsonArray)[0]
                .jsonObject["inFlight"]!!
                .jsonObject
        assertEquals(1, (f["relays"] as JsonArray).size, "only the readable row is published")
        assertEquals(
            9,
            f["omitted"]!!.jsonPrimitive.int,
            "the writer's 7 plus the two rows this side could not read",
        )
    }

    @Test
    fun `every pass still running is republished, each with its own partition`() {
        // The state a single `cycle` could not describe: a walk ends when its
        // last url is handed out and its slowest legs run on past it, so the
        // previous pass is normally still finishing while the new one walks.
        // Published as one cycle, the old pass's counters stopped being served
        // the moment the new one opened.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "streams": [{"name": "content",
                 "cycle": {"number": 12, "owner": "dynamic", "outcome": "running",
                   "urls": {"discovered": 10, "taken": 10}, "taken": {"delivered": 2}},
                 "passes": [
                   {"number": 11, "owner": "dynamic", "outcome": "completed", "endedAt": 880,
                    "urls": {"discovered": 10, "taken": 10}, "taken": {"delivered": 6, "pending": 4}, "received": 400},
                   {"number": 12, "owner": "dynamic", "outcome": "running",
                    "urls": {"discovered": 10, "taken": 10}, "taken": {"delivered": 2, "pending": 8}, "received": 40}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val passes = (out["streams"] as JsonArray)[0].jsonObject["passes"] as JsonArray

        assertEquals(2, passes.size)
        assertEquals(11L, passes[0].jsonObject["number"]!!.jsonPrimitive.long)
        // Each pass's partition is closed on ITS OWN numbers, so a straggler's
        // urls are never counted against the walk that did not hand them out —
        // and the four still in flight belong to the pass that HANDED THEM OUT,
        // not to the one walking now.
        assertEquals(
            4L,
            passes[0]
                .jsonObject["taken"]!!
                .jsonObject["pending"]!!
                .jsonPrimitive.long,
        )
        assertEquals(
            8L,
            passes[1]
                .jsonObject["taken"]!!
                .jsonObject["pending"]!!
                .jsonPrimitive.long,
        )
        assertTrue(
            passes[0]
                .jsonObject["accountedFor"]!!
                .jsonPrimitive.content
                .toBoolean(),
        )
    }

    @Test
    fun `a router that publishes one pass publishes no passes array`() {
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "streams": [{"name": "content",
                 "cycle": {"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}},
                 "passes": [{"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!

        assertNull((out["streams"] as JsonArray)[0].jsonObject["passes"], "one pass is what `cycle` already says")
    }

    @Test
    fun `the processors are republished, and only the counters this side names`() {
        // Rebuilt member by member like everything else here: the file is
        // another process's, and a hand-edited one must not be able to put a new
        // member — one no glossary defines — into a document served under this
        // relay's name.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "streams": [],
                 "processors": [
                   {"name": "aliasFold", "phase": "idle", "phaseForSec": 400, "passes": 3, "nextInSec": 20800,
                    "somethingInvented": 7,
                    "streams": [{"name": "content", "subjects": 40, "outstanding": 12, "measured": 20, "decided": 4,
                      "undecided": {"reasons": [{"reason": "out of probe budget", "hosts": 2,
                                                 "examples": ["a.example", "b.example", "c.example", "d.example"]}],
                                    "omitted": 1}}]},
                   {"name": "ingest", "phase": "running", "queued": 12, "capacity": 20000}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val rows = out["processors"] as JsonArray
        val fold = rows[0].jsonObject

        assertEquals(20_800L, fold["nextInSec"]!!.jsonPrimitive.long)
        assertNull(fold["somethingInvented"], "a member this side does not name is not passed through")
        val work = (fold["streams"] as JsonArray)[0].jsonObject
        assertEquals(12L, work["outstanding"]!!.jsonPrimitive.long)
        val reason = (work["undecided"]!!.jsonObject["reasons"] as JsonArray)[0].jsonObject
        assertEquals(3, (reason["examples"] as JsonArray).size, "the examples are capped again on this side")
        assertEquals(1L, work["undecided"]!!.jsonObject["omitted"]!!.jsonPrimitive.long, "and the cap discloses itself")
        assertEquals(12L, rows[1].jsonObject["queued"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a processor with no name says nothing, rather than an anonymous row`() {
        val out =
            SyncProgressReport.build(
                """{"writtenAt": 900, "streams": [], "processors": [{"phase": "idle", "queued": 3}]}""",
                nowSeconds = 1_000,
            )!!

        assertNull(out["processors"], "a row that names no processor cannot be looked up or acted on")
    }

    @Test
    fun `every processor and counter this object publishes can be drawn by the card`() {
        // The same pin as the disposition one below, for the same failure: the
        // publisher is Kotlin and the reader is inline JS in a resource, so a
        // name added on one side and not the other does not fail — the number
        // simply stops being drawn, on a card that looks complete.
        val card = card()

        val undrawn = SyncProgressReport.COUNTERS.filterNot { card.contains("has(\"$it\")") }
        assertEquals(emptyList(), undrawn, "published as a gauge, drawn by no line: $undrawn")

        // The names come from `SyncEngine`'s own constants, which live in the
        // other module and cannot be read from here. Restated rather than
        // imported, and the pin is still worth having: it is the page half that
        // silently stops describing a processor.
        val processors = listOf("aliasFold", "stability", "reachability", "ingest", "heal", "upstreamPush")
        val unnamed = processors.filterNot { card.contains("[\"$it\", ") }
        assertEquals(emptyList(), unnamed, "the router registers these and the card names none of them: $unnamed")
    }

    private fun card(): String =
        SyncProgressReportTest::class.java
            .getResourceAsStream("/stats.html")!!
            .readBytes()
            .decodeToString()

    @Test
    fun `every outcome this object publishes can be drawn by the card`() {
        // The drift that produced the bug: `busy` was added to the partition,
        // summed into `accountedFor`, and never added to the card's
        // `DISPOSITION`. The stack is drawn from members that sum to the total
        // BY CONSTRUCTION, so a missing one does not fail — it under-fills, and
        // the count simply disappears from a card that still says the numbers
        // add up. Nothing else pins the two lists together: one is Kotlin, the
        // other is inline JS in a resource.
        val card =
            SyncProgressReportTest::class.java
                .getResourceAsStream("/stats.html")!!
                .readBytes()
                .decodeToString()
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "streams": [{"name": "content",
                 "cycle": {"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}}}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val published =
            ((out["streams"] as JsonArray)[0].jsonObject["cycle"]!!.jsonObject["taken"] as JsonObject).keys

        val undrawn = published.filterNot { card.contains("[\"$it\",") }
        assertEquals(emptyList(), undrawn, "published in the partition, drawn by no bar segment: $undrawn")
    }
}
