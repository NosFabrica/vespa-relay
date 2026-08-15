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
import kotlinx.serialization.json.longOrNull
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
                 "inFlight": {"relays": [{"relay": "wss://good.example/", "heldForSec": 5, "events": 1, "quietForSec": 0,
                                          "doing": "paging", "pagingUntil": 1689857148},
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
        // Carried across, and never defaulted: the stream's own `reached` or a
        // zero would date a leg from a walk it is not on.
        assertEquals(
            1_689_857_148L,
            (f["relays"] as JsonArray)[0]
                .jsonObject["pagingUntil"]!!
                .jsonPrimitive.long,
        )
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

    /** A document with everything the gauge series is sampled from. */
    private fun sampled(
        rate: Int,
        queued: Int,
    ) = """
        {"writtenAt": 900,
         "health": {"bottleneck": "ingest", "eventsPerSec": $rate, "heapUsedMb": 900, "heapMaxMb": 2048,
                    "sockets": 41, "socketCeiling": 1024},
         "processors": [{"name": "ingest", "phase": "running", "queued": $queued, "capacity": 4096}],
         "streams": [{"name": "c",
          "cycle": {"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}}}]}
        """.trimIndent()

    private fun series(doc: JsonObject) = doc["series"]!!.jsonObject

    private fun at(doc: JsonObject) = (series(doc)["at"] as JsonArray).map { it.jsonPrimitive.long }

    @Test
    fun `the gauge series appends to what the previous document carried`() {
        // The whole design: nothing new holds this. `StatsSnapshot` already
        // merges each tier into the document it is serving and already persists
        // that document, so a series that lives in the document is carried
        // across rollups by the merge and across restarts by the file — no ring
        // buffer, no scheduler, no second lifetime to reason about.
        val first = SyncProgressReport.build(sampled(rate = 10, queued = 100), nowSeconds = 1_000)!!
        val second = SyncProgressReport.build(sampled(rate = 20, queued = 200), nowSeconds = 1_060, previous = first)!!

        assertEquals(listOf(1_000L, 1_060L), at(second))
        assertEquals(listOf(10L, 20L), (series(second)["eventsPerSec"] as JsonArray).map { it.jsonPrimitive.long })
        // `queued` is not on `health` — it is ingest's, read off the processor
        // row, because the queue depth is the one gauge the constraint verdict
        // is actually made from.
        assertEquals(listOf(100L, 200L), (series(second)["queued"] as JsonArray).map { it.jsonPrimitive.long })
        // Derived, not copied: a percentage is what compares across samples.
        assertEquals(listOf(43L, 43L), (series(second)["heapPct"] as JsonArray).map { it.jsonPrimitive.long })
    }

    @Test
    fun `a document republished on the same clock does not sample twice`() {
        // The failure this guards: a sample per REQUEST rather than per rollup.
        // `build` runs once per counters tick, but anything that rebuilt the
        // document without a new reading would lay down a run of samples all
        // claiming the same instant — and a rate drawn against them reads as a
        // spike that never happened.
        val first = SyncProgressReport.build(sampled(rate = 10, queued = 100), nowSeconds = 1_000)!!
        val again = SyncProgressReport.build(sampled(rate = 99, queued = 999), nowSeconds = 1_000, previous = first)!!

        assertEquals(listOf(1_000L), at(again))
        assertEquals(listOf(10L), (series(again)["eventsPerSec"] as JsonArray).map { it.jsonPrimitive.long })
    }

    @Test
    fun `a router that says nothing leaves a null in the series, not a zero`() {
        // "The router said nothing" and "the router said none" are different
        // facts and a chart cannot tell them apart once one is written as the
        // other. A carried-forward value would be worse still: it draws a flat
        // line through an outage.
        val first = SyncProgressReport.build(sampled(rate = 10, queued = 100), nowSeconds = 1_000)!!
        val quiet =
            SyncProgressReport.build(
                """{"writtenAt": 900, "streams": [{"name": "c"}]}""",
                nowSeconds = 1_060,
                previous = first,
            )!!

        assertEquals(listOf(1_000L, 1_060L), at(quiet))
        assertEquals(listOf(10L, null), (series(quiet)["eventsPerSec"] as JsonArray).map { it.jsonPrimitive.longOrNull })
    }

    @Test
    fun `nothing to sample and nothing carried publishes no series at all`() {
        // An hour of flat zeroes reads as a dead mirror. A router too old to
        // publish health, or one whose first health tick has not fired, has to
        // be absent rather than flat.
        val out =
            SyncProgressReport.build(
                """{"writtenAt": 900, "streams": [{"name": "c"}]}""",
                nowSeconds = 1_000,
            )!!

        assertNull(out["series"])
    }

    @Test
    fun `the ring is bounded by count, whatever the operator set the cadence to`() {
        // Bounded by COUNT rather than by age on purpose: the bound has to hold
        // whatever `STATS_COUNTERS_INTERVAL_SECONDS` is, and this costs the
        // document the same either way.
        var doc = SyncProgressReport.build(sampled(rate = 1, queued = 1), nowSeconds = 1_000)!!
        for (i in 1..SyncProgressReport.MAX_SAMPLES + 20) {
            doc = SyncProgressReport.build(sampled(rate = i, queued = i), nowSeconds = 1_000L + i * 60, previous = doc)!!
        }

        assertEquals(SyncProgressReport.MAX_SAMPLES, at(doc).size)
        for (member in SyncProgressReport.SERIES) {
            assertEquals(SyncProgressReport.MAX_SAMPLES, (series(doc)[member] as JsonArray).size, "$member must ride the same ring")
        }
        // The OLDEST is what falls off, so the newest sample is always the last.
        assertEquals(1_000L + (SyncProgressReport.MAX_SAMPLES + 20) * 60, at(doc).last())
    }

    @Test
    fun `a health object this side recognises nothing in is absent, not empty`() {
        // Every member of `health` is allowlisted — a word `bottleneckOf` cannot
        // emit is dropped, and so is a gauge this side does not name. When that
        // takes all of them, the rebuilt object was still published as `{}`,
        // which is a claim that the router reported its constraint. The card
        // believed it and drew a chip with nothing in it, beside the live one.
        //
        // The case is not hypothetical: it is what a router OLDER than these
        // gauges produces, which is every router during a rolling deploy.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "health": {"bottleneck": "somethingElse", "gauge": 4},
                 "streams": [{"name": "content",
                 "cycle": {"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}}}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!

        assertNull(out["health"], "an empty health object is a claim; absence is the honest answer")
    }

    @Test
    fun `a health object keeps whichever members survive, one word or one gauge`() {
        // …and the converse, so the fix above cannot be "drop health whenever
        // anything is missing". Each member stands on its own: the card guards
        // the verdict on `bottleneck` and each gauge on its own pair, so a
        // document carrying only half of them is still worth serving.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "health": {"bottleneck": "downloads", "heapUsedMb": 900},
                 "streams": [{"name": "content",
                 "cycle": {"outcome": "running", "urls": {"discovered": 1, "taken": 1}, "taken": {"delivered": 1}}}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!

        val health = out["health"]!!.jsonObject
        assertEquals("downloads", health["bottleneck"]!!.jsonPrimitive.content)
        assertEquals(setOf("bottleneck", "heapUsedMb"), health.keys, "only what survived, and all of it")
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
                   {"name": "aliasFold", "phase": "idle", "phaseForSec": 400, "passesRun": 3, "nextInSec": 20800,
                    "somethingInvented": 7,
                    "streams": [{"name": "content", "candidates": 40, "unmeasured": 12, "dialled": 20, "decided": 4,
                      "undecided": {"reasons": [{"reason": "cooling down from an earlier failed pass", "hosts": 2,
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
        assertEquals(12L, work["unmeasured"]!!.jsonPrimitive.long)
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

        // The gauge's NAME anywhere in the page, not `has("x")` specifically:
        // not every gauge belongs on the counts line. `lostToStore` is drawn on
        // its own, loud and only when non-zero, because a loss counter reading
        // zero belongs in the JSON and one above zero belongs in front of an
        // operator. The pin is about "published and never drawn", not about
        // which line does the drawing.
        val undrawn = SyncProgressReport.COUNTERS.filterNot { card.contains(it) }
        assertEquals(emptyList(), undrawn, "published as a gauge, drawn by no line: $undrawn")

        // The names come from `SyncEngine`'s own constants, which live in the
        // other module and cannot be read from here. Restated rather than
        // imported, and the pin is still worth having: it is the page half that
        // silently stops describing a processor.
        val processors = listOf("aliasFold", "consistency", "reachability", "ingest", "heal", "upstreamPush")
        val unnamed = processors.filterNot { card.contains("[\"$it\", ") }
        assertEquals(emptyList(), unnamed, "the router registers these and the card names none of them: $unnamed")
    }

    /**
     * EVERYTHING THE CARD IS MADE OF, not just the page.
     *
     * This read `/stats.html` alone, and then the card's judgements — which
     * legs to draw, what each bar is a proportion of, the ten outcomes
     * themselves — moved into `/web/shared/sync.js` so `tools/webtest` could
     * assert them. The pin went red, correctly: it had been asking whether a
     * name appeared in one file rather than whether the card draws it.
     *
     * So it reads the page AND the module the page imports. A grep over a
     * moving target is exactly what this pin is for — its whole job is to fail
     * when a name is published on the Kotlin side and drawn on neither.
     */
    private fun card(): String =
        listOf("/stats.html", "/web/shared/sync.js")
            .joinToString("\n") {
                SyncProgressReportTest::class.java
                    .getResourceAsStream(it)!!
                    .readBytes()
                    .decodeToString()
            }

    @Test
    fun `every member of a probe pass's partition can be drawn by the card`() {
        // The same drift as the outcome partition below, on the newer one. A
        // probe pass's row divides its candidate set — folded, consistent,
        // inconsistent, and what is left — and the funnel on the card is drawn
        // from members that sum to the total BY CONSTRUCTION. A member the page
        // never reads does not fail: it lands in the funnel's `unattributed`
        // slice, which is honest and is not the same as being drawn.
        val card = card()
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "processors": [{"name": "consistency", "phase": "idle",
                 "streams": [{"name": "all streams", "candidates": 40, "foldedAway": 8, "consistent": 9,
                   "inconsistent": 1, "unmeasured": 22, "dialled": 22, "decided": 2,
                   "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 22, "hosts": 7,
                                              "examples": ["dead.example"]}], "omitted": 0}}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val row = ((out["processors"] as JsonArray)[0].jsonObject["streams"] as JsonArray)[0].jsonObject
        val published = row.keys + (row["undecided"]!!.jsonObject["reasons"] as JsonArray)[0].jsonObject.keys
        val undrawn = published.filterNot { card.contains(it) }
        assertEquals(emptyList(), undrawn, "published in the pass's partition, drawn nowhere on the card: $undrawn")
    }

    @Test
    fun `every reason the gate can reach survives this side, and its hosts are ranked as sent`() {
        // THE CAP THAT WAS ONE SHORT. This side bounds what the router already
        // bounded, and the two numbers have to be read together: the router
        // publishes up to `Processors.MAX_UNDECIDED_REASONS` (8) and the
        // stability gate can reach seven of them, while this side cut at six.
        // Cutting BELOW the router is not bounding, it is dropping — and the
        // dropped reason's urls then land in the card's `not accounted for`
        // slice, which reports an arithmetic fault against a document that was
        // complete when it arrived.
        val reasons =
            listOf(
                "declined by our own transport",
                "never answered a REQ",
                "answered one of the two asks, not both",
                "refused our auth",
                "answered, but served no filter we know",
                "too few events to judge on",
                "the probe failed mid-walk",
            )
        val rows =
            reasons.joinToString(",") {
                """{"reason": "$it", "urls": 10, "hosts": 2,
                 "top": [{"host": "a.example", "urls": 6}, {"host": "b.example", "urls": 4}]}"""
            }
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "processors": [{"name": "consistency", "phase": "idle",
                 "streams": [{"name": "all streams", "candidates": 70, "foldedAway": 0, "consistent": 0,
                   "inconsistent": 0, "unmeasured": 70, "dialled": 70, "decided": 0,
                   "undecided": {"reasons": [$rows], "omitted": 0}}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val row = ((out["processors"] as JsonArray)[0].jsonObject["streams"] as JsonArray)[0].jsonObject
        val undecided = row["undecided"]!!.jsonObject
        val kept = (undecided["reasons"] as JsonArray).map { it.jsonObject["reason"]!!.jsonPrimitive.content }
        assertEquals(reasons, kept, "every reason the gate can reach must survive the trip")
        assertEquals(0, undecided["omitted"]!!.jsonPrimitive.int, "and nothing is reported as dropped")

        // The order is the ROUTER's ranking, taken over the whole set it
        // measured. Re-sorting a capped list here would order the head by a
        // criterion never applied to the tail, which reads as a top-N and is not.
        val top = ((undecided["reasons"] as JsonArray)[0].jsonObject["top"] as JsonArray)
        assertEquals(listOf("a.example", "b.example"), top.map { it.jsonObject["host"]!!.jsonPrimitive.content })
        assertEquals(6, top[0].jsonObject["urls"]!!.jsonPrimitive.int)
    }

    @Test
    fun `a host row with no name is dropped rather than published as an anonymous count`() {
        // It is the NAME that makes the fourth level worth drawing: a slice
        // labelled with a number and nothing else is a slice nobody can chase.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "processors": [{"name": "consistency", "phase": "idle",
                 "streams": [{"name": "all streams", "candidates": 10, "unmeasured": 10,
                   "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 10, "hosts": 1,
                     "top": [{"urls": 5}, {"host": "real.example", "urls": 4}]}], "omitted": 0}}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val row = ((out["processors"] as JsonArray)[0].jsonObject["streams"] as JsonArray)[0].jsonObject
        val top = ((row["undecided"]!!.jsonObject["reasons"] as JsonArray)[0].jsonObject["top"] as JsonArray)
        assertEquals(listOf("real.example"), top.map { it.jsonObject["host"]!!.jsonPrimitive.content })
    }

    @Test
    fun `a probe pass's numbers are checked for adding up, and a mismatch is published`() {
        // The same check `cycle` gets, on the other partition this object
        // publishes — and recomputed here rather than forwarded, so it is a
        // statement about the document being served.
        fun row(streams: String) =
            (
                (
                    SyncProgressReport
                        .build("""{"writtenAt": 900, "processors": [{"name": "consistency", "streams": [$streams]}]}""", nowSeconds = 1_000)!!
                        ["processors"] as JsonArray
                )[0].jsonObject["streams"] as JsonArray
            )[0].jsonObject

        val whole =
            row(
                """{"name": "all streams", "candidates": 40, "foldedAway": 8, "consistent": 9, "inconsistent": 1,
                    "unmeasured": 22, "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 22, "hosts": 7}], "omitted": 0}}""",
            )
        assertTrue(whole["accountedFor"]!!.jsonPrimitive.booleanOrNull!!, "8 + 9 + 1 + 22 = 40, and the rows cover all 22")

        // The candidate set does not divide.
        val short =
            row(
                """{"name": "all streams", "candidates": 40, "foldedAway": 8, "consistent": 9, "inconsistent": 1,
                    "unmeasured": 20, "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 20, "hosts": 7}], "omitted": 0}}""",
            )
        assertFalse(short["accountedFor"]!!.jsonPrimitive.booleanOrNull!!, "38 of 40 urls have a disposition")

        // …and the rows do not cover what has no verdict, which is the identity
        // the second level of the tree rests on.
        val gap =
            row(
                """{"name": "all streams", "candidates": 40, "foldedAway": 8, "consistent": 9, "inconsistent": 1,
                    "unmeasured": 22, "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 9, "hosts": 7}], "omitted": 0}}""",
            )
        assertFalse(gap["accountedFor"]!!.jsonPrimitive.booleanOrNull!!, "9 of the 22 undecided urls are under a reason")

        // A pass that publishes no partition makes no claim about it either.
        assertFalse("accountedFor" in row("""{"name": "all streams", "candidates": 40, "unmeasured": 22}"""))
    }

    @Test
    fun `a pass that measures no verdicts publishes none, rather than zero of them`() {
        // The alias fold has no stability verdicts to report, and neither has a
        // router written before the partition existed. A zero would be a
        // measurement neither of them took — the card draws `0 refused as
        // inconsistent` from it, which is a claim about every relay in the
        // fan-out — so the members are carried only where the router wrote them.
        val out =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "processors": [{"name": "aliasFold", "phase": "idle",
                 "streams": [{"name": "all streams", "candidates": 40, "unmeasured": 12, "dialled": 20, "decided": 4}]}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!
        val row = ((out["processors"] as JsonArray)[0].jsonObject["streams"] as JsonArray)[0].jsonObject
        assertEquals(
            emptyList(),
            listOf("foldedAway", "consistent", "inconsistent").filter { it in row },
            "a verdict nobody measured must not be published as zero: $row",
        )
        // …and the members it DID write are still there, so this is an absence
        // rather than a row that failed to read.
        assertEquals(40, row["candidates"]!!.jsonPrimitive.int)
    }

    @Test
    fun `every outcome this object publishes can be drawn by the card`() {
        // The drift that produced the bug: `busy` was added to the partition,
        // summed into `accountedFor`, and never added to the card's
        // `DISPOSITION`. The stack is drawn from members that sum to the total
        // BY CONSTRUCTION, so a missing one does not fail — it under-fills, and
        // the count simply disappears from a card that still says the numbers
        // add up. Nothing else pins the two lists together: one is Kotlin, the
        // other is a JS table in a resource — see [card] for why that is read
        // as more than one file.
        val card = card()
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

        // The NAME anywhere in the card, not the `["x", …]` table shape it used
        // to be stored in — the same rule the COUNTERS pin above already uses,
        // and for the reason stated there: this is about "published and never
        // drawn", not about which line does the drawing. The card summarises
        // these outcomes now rather than stacking all ten into one bar, and
        // pinning the old shape would have forced a partition table back into
        // the page to satisfy a grep.
        val undrawn = published.filterNot { card.contains(it) }
        assertEquals(emptyList(), undrawn, "published in the partition, drawn nowhere on the card: $undrawn")
    }
}
