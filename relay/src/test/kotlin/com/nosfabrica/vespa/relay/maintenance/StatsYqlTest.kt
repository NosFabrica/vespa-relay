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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import java.time.DayOfWeek
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The readers, against responses a real engine actually produced.
 *
 * Every fixture below is verbatim output from Vespa 8.733 answering the exact
 * pipeline named above it, captured from a node running this repo's bundled
 * application package, trimmed to three groups. That provenance is the point:
 * a grouping response is nested, version-shaped, and easy to hand-write into
 * the shape you assumed rather than the one you get — which is how both of the
 * traps these tests pin got written in the first place.
 */
class StatsYqlTest {
    private fun root(json: String) = Json.parseToJsonElement(json).jsonObject

    // ---- the two traps ------------------------------------------------------

    /**
     * `time.date` does not zero-pad, and unpadded dates misorder as text. The
     * bug this prevents is not a crash: every count stays correct and only the
     * x-axis is scrambled, which reads as noisy data rather than a defect.
     */
    @Test
    fun `day groups are rekeyed to sortable iso dates`() {
        val groups = StatsYql.topGroups(root(COUNTS_BY_DAY))
        assertEquals(listOf("2025-1-5", "2025-10-9"), groups.map { StatsYql.valueOf(it) }, "what Vespa actually returns")
        assertEquals(listOf("2025-01-05", "2025-10-09"), groups.mapNotNull { StatsYql.valueOf(it)?.let(StatsYql::isoDay) })
    }

    /**
     * The property that matters: sorting the normalized values gives calendar
     * order, and sorting the raw ones does not.
     *
     * The counterexample set is chosen, not incidental. `2025-1-5` beside
     * `2025-10-9` sorts CORRECTLY as text — `'-'` is below `'0'` — so a test
     * written around the pair the fixture happens to contain proves nothing.
     * The values that break are the ones whose digit count differs in the same
     * position: November before February, the 15th before the 5th.
     */
    @Test
    fun `unpadded day values are the ones that misorder`() {
        val calendar = listOf("2025-2-1", "2025-11-1", "2026-1-5", "2026-1-15")
        assertTrue(calendar.sorted() != calendar, "these raw values do NOT sort into calendar order")
        assertEquals(
            listOf("2025-02-01", "2025-11-01", "2026-01-05", "2026-01-15"),
            calendar.mapNotNull(StatsYql::isoDay).sorted(),
        )
    }

    /** Anything that is not `Y-M-D` is dropped rather than charted under a label the axis cannot order. */
    @Test
    fun `a day value that is not a date is refused`() {
        for (bad in listOf("", "2025", "2025-10", "2025-10-09-01", "2025-13-01", "2025-10-32", "x-y-z", "2025--9")) {
            assertNull(StatsYql.isoDay(bad), "must refuse $bad")
        }
    }

    /**
     * The counts pipeline and the distinct-authors pipeline answer in the SAME
     * shape and mean different things — Vespa collapses the nested list's
     * aggregate onto the outer group. Nothing in the response distinguishes
     * them, so this pins the numbers from the two fixtures against each other:
     * kind 0 has 79 events from 35 authors, and reading either through the
     * other's reader must not quietly produce the wrong one.
     */
    @Test
    fun `documents and distinct authors are the same shape and different numbers`() {
        val events: Map<String, Long> =
            StatsYql
                .topGroups(root(COUNTS_BY_KIND))
                .mapNotNull { g ->
                    StatsYql.valueOf(g)?.let { v -> StatsYql.aggOf(g, "count()")?.let { v to it } }
                }.toMap()
        val authors: Map<String, Long> =
            StatsYql
                .topGroups(root(DISTINCT_AUTHORS_BY_KIND))
                .mapNotNull { g ->
                    StatsYql.valueOf(g)?.let { v -> StatsYql.distinctCountOf(g)?.let { v to it } }
                }.toMap()

        assertEquals(mapOf("0" to 79L, "1" to 83L, "3" to 65L), events)
        assertEquals(mapOf("0" to 35L, "1" to 36L, "3" to 33L), authors)
        assertTrue(events.keys == authors.keys && authors.all { (kind, a) -> a < events.getValue(kind) }, "authors cannot exceed their own events")
    }

    /**
     * The nested-list fallback in [StatsYql.distinctCountOf], for an engine
     * that stops collapsing. Hand-built rather than captured, because no
     * version we run answers this way — it exists so an upgrade that changes
     * the shape degrades to the right number instead of to no column at all.
     */
    @Test
    fun `distinct count also reads an uncollapsed nested list`() {
        val nested =
            """
            {"children":[{"id":"group:root:0","children":[{"id":"grouplist:kind","children":[
              {"id":"group:long:1","value":"1","children":[{"id":"grouplist:pubkey","fields":{"count()":36}}]}]}]}]}
            """.trimIndent()
        val groups = StatsYql.topGroups(root(nested))
        assertEquals(1, groups.size)
        assertEquals(36L, StatsYql.distinctCountOf(groups.single()))
    }

    // ---- the ordinary readers -----------------------------------------------

    @Test
    fun `a flat count is read through the group wrapper`() {
        assertEquals(602L, StatsYql.singleCount(root(TOTAL)))
        // Zero matches is a count of zero, not a missing one — the distinction
        // a page needs to tell "we hold none of these" from "we did not ask".
        assertEquals(0L, StatsYql.singleCount(root(EMPTY_MATCH)))
    }

    @Test
    fun `min and max created_at come back per group`() {
        val spans =
            StatsYql.topGroups(root(SPAN_BY_KIND)).associate {
                StatsYql.valueOf(it) to (StatsYql.aggOf(it, "min(created_at)") to StatsYql.aggOf(it, "max(created_at)"))
            }
        assertEquals(1751137726L to 1754581422L, spans["0"])
        assertTrue(spans.values.all { (first, last) -> first!! <= last!! })
    }

    /**
     * The group list is found by descending to the shallowest one rather than
     * by counting levels — Vespa wraps every grouping result in a `group:root:0`
     * whose depth is not part of any contract we control.
     */
    @Test
    fun `the reader does not depend on how deeply vespa wraps the result`() {
        assertEquals(3, StatsYql.topGroups(root(COUNTS_BY_KIND)).size)

        // The same list one wrapper deeper, and one wrapper shallower.
        val leaf = """{"id":"group:long:1","value":"1","fields":{"count()":7}}"""
        val list = """{"id":"grouplist:kind","children":[$leaf]}"""
        for (depth in 0..3) {
            var node = list
            repeat(depth) { node = """{"id":"group:root:0","children":[$node]}""" }
            val groups = StatsYql.topGroups(root("""{"id":"toplevel","children":[$node]}"""))
            assertEquals(listOf("1"), groups.map { StatsYql.valueOf(it) }, "wrapped $depth deep")
        }
        // A response with no grouping at all is empty, not an exception.
        assertEquals(emptyList(), StatsYql.topGroups(root("""{"id":"toplevel","fields":{"totalCount":0}}""")))
    }

    /**
     * The two-level pipeline that turned eight queries into one.
     *
     * Its inner list does NOT collapse onto the outer group the way
     * [StatsYql.distinctAuthorsBy]'s does — the `each()` means there are many
     * inner values to keep rather than one aggregate to fold up — so the same
     * response carries an outer group with no `count()` of its own and a nested
     * list that has one per day. Reading it with the flat reader would find
     * nothing; reading a flat response with [StatsYql.childGroups] finds nothing
     * either. This pins both directions.
     */
    @Test
    fun `a nested pipeline is read one level down`() {
        val outer = StatsYql.topGroups(root(KIND_BY_DAY))
        assertEquals(listOf("1", "7"), outer.map { StatsYql.valueOf(it) })
        // The outer group of a nested pipeline carries no aggregate of its own.
        assertNull(StatsYql.aggOf(outer.first(), "count()"))

        val perKind =
            outer.associate { g ->
                StatsYql.valueOf(g) to
                    StatsYql
                        .childGroups(g)
                        .mapNotNull { d ->
                            StatsYql.valueOf(d)?.let(StatsYql::isoDay)?.let { day -> day to StatsYql.aggOf(d, "count()") }
                        }.toMap()
            }
        assertEquals(mapOf("2026-07-10" to 17L, "2026-07-11" to 16L), perKind["1"])
        assertEquals(mapOf("2026-07-10" to 13L, "2026-07-11" to 8L), perKind["7"])

        // A FLAT response has nothing one level down — the two readers cannot be
        // swapped and quietly return the other pipeline's numbers.
        assertEquals(emptyList(), StatsYql.childGroups(StatsYql.topGroups(root(COUNTS_BY_KIND)).first()))
    }

    @Test
    fun `the nested pipeline keeps an each on the inner level`() {
        // The `each()` is what distinguishes this from distinctAuthorsBy, whose
        // inner list has none and therefore collapses. Losing it here would turn
        // every kind's whole series into a single distinct-day count.
        assertEquals("all(group(kind) each(all(group(time.date(created_at)) each(output(count())))))", StatsYql.nested("kind", StatsYql.DAY))
        assertTrue(!StatsYql.distinctAuthorsBy("kind").contains("each(output(count()))"), "the collapsing shape has no inner each()")
    }

    // ---- the other bucket decoders ------------------------------------------

    /**
     * Week buckets land on Mondays.
     *
     * The shift is the whole content of [StatsYql.WEEK]: epoch second 0 is a
     * THURSDAY, so the un-shifted `created_at / 604800` buckets
     * Thursday-to-Wednesday — which produces a chart no reader would question
     * and every reader would misread, because a weekly axis is assumed to start
     * on Monday. These bucket indices came back from a real node over the
     * seeded corpus.
     */
    @Test
    fun `week buckets start on monday`() {
        assertEquals("2026-04-06", StatsYql.isoWeekStart("2936"))
        assertEquals("2026-04-13", StatsYql.isoWeekStart("2937"))
        assertEquals("2026-04-20", StatsYql.isoWeekStart("2938"))
        // Consecutive buckets are exactly seven days apart, and all Mondays.
        for (bucket in 2900..2960) {
            val day = assertNotNull(StatsYql.isoWeekStart(bucket.toString()))
            assertEquals(DayOfWeek.MONDAY, LocalDate.parse(day).dayOfWeek, "bucket $bucket is $day")
        }
        assertEquals(0, StatsYql.WEEK.count { it == '%' }, "the pipeline is a literal expression, not a format string")
    }

    /**
     * Month buckets decode `year * 12 + month`, and December is the case that
     * breaks a naive split: `year * 12 + 12` must stay in that year rather than
     * rolling into January of the next.
     */
    @Test
    fun `month buckets decode year and month, december included`() {
        assertEquals("2026-04", StatsYql.isoMonth("24316"), "the value a real node returned for April 2026")
        assertEquals("2026-12", StatsYql.isoMonth((2026 * 12 + 12).toString()))
        assertEquals("2027-01", StatsYql.isoMonth((2027 * 12 + 1).toString()))
        // Consecutive indices are consecutive months, across the year boundary.
        val run: List<String> = (2026 * 12 + 10..2027 * 12 + 2).mapNotNull { StatsYql.isoMonth(it.toString()) }
        assertEquals(listOf("2026-10", "2026-11", "2026-12", "2027-01", "2027-02"), run)
        // Sortable as text, which is the entire reason for the zero padding.
        assertEquals(run.sorted(), run)
        for (bad in listOf("", "0", "-5", "x", "1")) assertNull(StatsYql.isoMonth(bad), "must refuse $bad")
    }

    /**
     * `tag_index` pairs are `<letter>:<value>`, CASED — so NIP-57's `P` (sender)
     * and `p` (recipient) are different tags and must not collapse.
     */
    @Test
    fun `tag pairs are split by letter, case sensitively`() {
        assertEquals("wss://nos.lol", StatsYql.tagValue("r:wss://nos.lol", 'r'))
        assertNull(StatsYql.tagValue("r:wss://nos.lol", 'e'), "a relay list must not be read as an event reference")
        assertEquals("abc", StatsYql.tagValue("P:abc", 'P'))
        assertNull(StatsYql.tagValue("P:abc", 'p'), "P (zap sender) and p (recipient) are different tags")
        // A url contains colons of its own; only the first one delimits.
        assertEquals("wss://a.example:444/path", StatsYql.tagValue("r:wss://a.example:444/path", 'r'))
        for (bad in listOf("", "r", "r:", "rr:x", ":x")) assertNull(StatsYql.tagValue(bad, 'r'), "must refuse '$bad'")
    }

    /**
     * The spellings a hand-written relay list carries collapse onto one relay.
     *
     * This is what makes the distribution a count of RELAYS. Sorted by count,
     * a relay split across a trailing slash and a capitalised host sits below
     * relays it actually outnumbers, and neither row is its real total.
     */
    @Test
    fun `relay urls that name one relay canonicalise to one string`() {
        val canonical = StatsYql.canonicalRelay("wss://nos.lol")
        for (spelling in listOf("wss://nos.lol", "wss://nos.lol/", "wss://NOS.LOL", "  wss://nos.lol  ")) {
            assertEquals(canonical, StatsYql.canonicalRelay(spelling), "'$spelling' is the same relay")
        }
        // Idempotent, or a second pass over a rolled-up value would drift.
        assertEquals(canonical, StatsYql.canonicalRelay(canonical))
        // Displayed without the normalizer's trailing slash.
        assertEquals("wss://nos.lol", canonical)
    }

    /**
     * Different relays stay different — the failure mode a normalizer invites
     * is over-merging, which silently ADDS counts to the wrong row.
     *
     * `wss://nos.lol:443` is in here rather than in the merged set above, and
     * that is Quartz's call, not ours: its normalizer preserves an explicit
     * port even when it is the scheme's default, and `RelayOnionAuthTest` pins
     * exactly that ("the normalizer keeps the two strings apart") because a
     * relay is identified by the url a client dialled and AUTH'd against.
     * Stripping the port here would be a THIRD identity rule, disagreeing with
     * the router on the one page that reports what the router sees, to merge a
     * spelling that is vanishingly rare in real relay lists.
     */
    @Test
    fun `a path, a port and a scheme are not normalised away`() {
        val distinct =
            listOf(
                "wss://nos.lol",
                "wss://relay.nos.lol",
                "wss://nos.lol/nostr",
                "wss://nos.lol:444",
                "wss://nos.lol:443",
                "ws://nos.lol",
            ).map { StatsYql.canonicalRelay(it) }
        assertEquals(distinct.size, distinct.toSet().size, "these are six different endpoints: $distinct")
    }

    /**
     * A url the normalizer cannot parse keeps its place in the census.
     *
     * The panel says how many relays our lists name; dropping the unparseable
     * ones would make `total` a count of the well-formed ones under a heading
     * that claims otherwise.
     */
    @Test
    fun `an unparseable relay url survives as itself`() {
        assertEquals("not a url at all", StatsYql.canonicalRelay("  not a url at all  "))
        assertEquals("", StatsYql.canonicalRelay(""))
    }

    // ---- the queries --------------------------------------------------------

    @Test
    fun `every aggregation is unranked, unlimited and unsorted`() {
        val q = StatsYql.query(StatsYql.countsBy("kind"), StatsYql.window(100, 200))
        assertEquals("select * from event where created_at >= 100 and created_at <= 200 limit 0 | all(group(kind) each(output(count())))", q)
        // `order by` would reintroduce the match phase that UNRANKED exists to
        // avoid, and a capped match set undercounts silently.
        assertTrue(!q.contains("order by"))
        assertEquals("-1", StatsYql.params["grouping.defaultMaxGroups"])
        assertEquals("-1", StatsYql.params["grouping.defaultMaxHits"])
    }

    /**
     * The trust section reads a second document type, and the source has to
     * reach the FROM clause rather than being assumed.
     */
    @Test
    fun `an aggregation can name the document type it runs over`() {
        assertEquals("select * from event where true limit 0 | ${StatsYql.TOTAL}", StatsYql.query(StatsYql.TOTAL))
        assertEquals(
            "select * from reputation where true limit 0 | ${StatsYql.TOTAL}",
            StatsYql.query(StatsYql.TOTAL, source = StatsYql.REPUTATION),
        )
    }

    /**
     * Freshness is bounded to the present and the future is counted separately —
     * the two halves of the same fact.
     *
     * `created_at` is author-signed, so an unbounded `max(created_at)` reports
     * the corpus's most optimistically-dated spam and a relay whose mirror died
     * an hour ago still shows a freshness of "in 74 years". The bound is what
     * makes the number an answer to "is this relay keeping up".
     */
    @Test
    fun `freshness excludes the future, and the future is its own question`() {
        assertEquals("created_at <= 500", StatsYql.upTo(500))
        assertEquals("created_at > 500", StatsYql.after(500))
        assertEquals("kind = 1 and created_at <= 500", StatsYql.kindUpTo(1, 500))
        // The two partition the corpus at the same instant — no event is in
        // both, none in neither, so a total can be reassembled from the pair.
        assertTrue(StatsYql.upTo(500).contains("<= 500") && StatsYql.after(500).contains("> 500"))
    }

    @Test
    fun `the window is closed at both ends`() {
        // The upper bound is what keeps one event dated 2100 from opening a
        // bucket 74 years out and flattening every real bar in the chart.
        assertEquals("created_at >= 10 and created_at <= 20", StatsYql.window(10, 20))
        assertEquals("kind = 1 and created_at >= 10 and created_at <= 20", StatsYql.windowOfKind(1, 10, 20))
    }

    private companion object {
        // Vespa 8.733, `all(output(count()))`.
        const val TOTAL =
            """{"id":"toplevel","relevance":1.0,"fields":{"totalCount":602},"coverage":{"coverage":100,"documents":602,"full":true,"nodes":1,"results":1,"resultsFull":1},"children":[{"id":"group:root:0","relevance":1.0,"continuation":{"this":""},"fields":{"count()":602}}]}"""

        // Vespa 8.733, `all(output(count()))` over a filter matching nothing.
        const val EMPTY_MATCH =
            """{"id":"toplevel","relevance":1.0,"fields":{"totalCount":0},"coverage":{"coverage":100,"documents":602,"full":true,"nodes":1,"results":1,"resultsFull":1},"children":[{"id":"group:root:0","relevance":1.0,"continuation":{"this":""},"fields":{"count()":0}}]}"""

        // Vespa 8.733, `all(group(kind) each(output(count())))`.
        const val COUNTS_BY_KIND =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 602}, "coverage": {"coverage": 100, "documents": 602, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:kind", "relevance": 1.0, "label": "kind", "children": [{"id": "group:long:0", "relevance": 0.0, "value": "0", "fields": {"count()": 79}}, {"id": "group:long:1", "relevance": 0.0, "value": "1", "fields": {"count()": 83}}, {"id": "group:long:3", "relevance": 0.0, "value": "3", "fields": {"count()": 65}}]}]}]}"""

        // Vespa 8.733, `all(group(kind) each(all(group(pubkey) output(count()))))` — note the collapse.
        const val DISTINCT_AUTHORS_BY_KIND =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 602}, "coverage": {"coverage": 100, "documents": 602, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:kind", "relevance": 1.0, "label": "kind", "children": [{"id": "group:long:0", "relevance": 0.0, "value": "0", "fields": {"count()": 35}}, {"id": "group:long:1", "relevance": 0.0, "value": "1", "fields": {"count()": 36}}, {"id": "group:long:3", "relevance": 0.0, "value": "3", "fields": {"count()": 33}}]}]}]}"""

        // Vespa 8.733, `all(group(kind) each(output(min(created_at), max(created_at))))`.
        const val SPAN_BY_KIND =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 602}, "coverage": {"coverage": 100, "documents": 602, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:kind", "relevance": 1.0, "label": "kind", "children": [{"id": "group:long:0", "relevance": 0.0, "value": "0", "fields": {"min(created_at)": 1751137726, "max(created_at)": 1754581422}}, {"id": "group:long:1", "relevance": 0.0, "value": "1", "fields": {"min(created_at)": 1751277649, "max(created_at)": 1754533972}}, {"id": "group:long:3", "relevance": 0.0, "value": "3", "fields": {"min(created_at)": 1751163032, "max(created_at)": 1754565906}}]}]}]}"""

        // Vespa 8.733, `all(group(kind) each(all(group(time.date(created_at)) each(output(count())))))`
        // over two kinds, each inner list trimmed to two days.
        const val KIND_BY_DAY =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 721}, "coverage": {"coverage": 100, "documents": 1500, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:kind", "relevance": 1.0, "label": "kind", "children": [{"id": "group:long:1", "relevance": 0.0, "value": "1", "children": [{"id": "grouplist:time.date(created_at)", "relevance": 1.0, "label": "time.date(created_at)", "children": [{"id": "group:string:2026-7-10", "relevance": 0.0, "value": "2026-7-10", "fields": {"count()": 17}}, {"id": "group:string:2026-7-11", "relevance": 0.0, "value": "2026-7-11", "fields": {"count()": 16}}]}]}, {"id": "group:long:7", "relevance": 0.0, "value": "7", "children": [{"id": "grouplist:time.date(created_at)", "relevance": 1.0, "label": "time.date(created_at)", "children": [{"id": "group:string:2026-7-10", "relevance": 0.0, "value": "2026-7-10", "fields": {"count()": 13}}, {"id": "group:string:2026-7-11", "relevance": 0.0, "value": "2026-7-11", "fields": {"count()": 8}}]}]}]}]}]}"""

        // Vespa 8.733, `all(group(time.date(created_at)) each(output(count())))` over two
        // documents nine months apart — the unpadded values, exactly as returned.
        const val COUNTS_BY_DAY =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 2}, "coverage": {"coverage": 100, "documents": 602, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:time.date(created_at)", "relevance": 1.0, "label": "time.date(created_at)", "children": [{"id": "group:string:2025-1-5", "relevance": 0.0, "value": "2025-1-5", "fields": {"count()": 1}}, {"id": "group:string:2025-10-9", "relevance": 0.0, "value": "2025-10-9", "fields": {"count()": 1}}]}]}]}"""
    }
}
