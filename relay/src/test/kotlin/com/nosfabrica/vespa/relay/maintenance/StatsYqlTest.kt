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

import com.nosfabrica.vespa.relay.util.canonicalRelay
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import java.time.DayOfWeek
import java.time.Instant
import java.time.LocalDate
import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The readers, against grouping responses captured verbatim from Vespa 8.733
 * running this repo's application package, trimmed to three groups.
 */
class StatsYqlTest {
    private fun root(json: String) = Json.parseToJsonElement(json).jsonObject

    // ---- the two traps ------------------------------------------------------

    /** `time.date` does not zero-pad, and unpadded dates misorder as text. */
    @Test
    fun `day groups are rekeyed to sortable iso dates`() {
        val groups = StatsYql.topGroups(root(COUNTS_BY_DAY))
        assertEquals(listOf("2025-1-5", "2025-10-9"), groups.map { StatsYql.valueOf(it) }, "what Vespa actually returns")
        assertEquals(listOf("2025-01-05", "2025-10-09"), groups.mapNotNull { StatsYql.valueOf(it)?.let(StatsYql::isoDay) })
    }

    /**
     * The fixture's own pair, `2025-1-5` beside `2025-10-9`, happens to sort
     * correctly as text; the values that break differ in digit count at one position.
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

    @Test
    fun `a day value that is not a date is refused`() {
        for (bad in listOf("", "2025", "2025-10", "2025-10-09-01", "2025-13-01", "2025-10-32", "x-y-z", "2025--9")) {
            assertNull(StatsYql.isoDay(bad), "must refuse $bad")
        }
    }

    /**
     * Vespa collapses the nested pubkey list's aggregate onto the outer group,
     * so both pipelines answer in one shape and only the numbers tell them apart.
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

    /** Hand-built, not captured: no version we run answers uncollapsed, but an upgrade that does must still give a number. */
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
        // Zero matches is a count of zero, not a missing one.
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

    /** Vespa wraps every grouping result in a `group:root:0` whose depth is not a contract. */
    @Test
    fun `the reader does not depend on how deeply vespa wraps the result`() {
        assertEquals(3, StatsYql.topGroups(root(COUNTS_BY_KIND)).size)

        val leaf = """{"id":"group:long:1","value":"1","fields":{"count()":7}}"""
        val list = """{"id":"grouplist:kind","children":[$leaf]}"""
        for (depth in 0..3) {
            var node = list
            repeat(depth) { node = """{"id":"group:root:0","children":[$node]}""" }
            val groups = StatsYql.topGroups(root("""{"id":"toplevel","children":[$node]}"""))
            assertEquals(listOf("1"), groups.map { StatsYql.valueOf(it) }, "wrapped $depth deep")
        }
        assertEquals(emptyList(), StatsYql.topGroups(root("""{"id":"toplevel","fields":{"totalCount":0}}""")))
    }

    /**
     * With an `each()` on the inner level the list does not collapse: the outer
     * group carries no `count()` of its own and the days sit one level down.
     */
    @Test
    fun `a nested pipeline is read one level down`() {
        val outer = StatsYql.topGroups(root(KIND_BY_DAY))
        assertEquals(listOf("1", "7"), outer.map { StatsYql.valueOf(it) })
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

        assertEquals(emptyList(), StatsYql.childGroups(StatsYql.topGroups(root(COUNTS_BY_KIND)).first()))
    }

    @Test
    fun `the nested pipeline keeps an each on the inner level`() {
        // Without the inner `each()` the list collapses, as distinctAuthorsBy's does.
        assertEquals("all(group(kind) each(all(group(time.date(created_at)) each(output(count())))))", StatsYql.nested("kind", StatsYql.DAY))
        assertTrue(!StatsYql.distinctAuthorsBy("kind").contains("each(output(count()))"), "the collapsing shape has no inner each()")
    }

    // ---- the other bucket decoders ------------------------------------------

    /**
     * Epoch second 0 is a Thursday, so the shift in [StatsYql.WEEK] is what
     * puts buckets on Mondays. The indices came back from a real node.
     */
    @Test
    fun `week buckets start on monday`() {
        assertEquals("2026-04-06", StatsYql.isoWeekStart("2936"))
        assertEquals("2026-04-13", StatsYql.isoWeekStart("2937"))
        assertEquals("2026-04-20", StatsYql.isoWeekStart("2938"))
        for (bucket in 2900..2960) {
            val day = assertNotNull(StatsYql.isoWeekStart(bucket.toString()))
            assertEquals(DayOfWeek.MONDAY, LocalDate.parse(day).dayOfWeek, "bucket $bucket is $day")
        }
        assertEquals(0, StatsYql.WEEK.count { it == '%' }, "the pipeline is a literal expression, not a format string")
    }

    /** `year * 12 + month`, where December must stay in its year rather than rolling into January. */
    @Test
    fun `month buckets decode year and month, december included`() {
        assertEquals("2026-04", StatsYql.isoMonth("24316"), "the value a real node returned for April 2026")
        assertEquals("2026-12", StatsYql.isoMonth((2026 * 12 + 12).toString()))
        assertEquals("2027-01", StatsYql.isoMonth((2027 * 12 + 1).toString()))
        val run: List<String> = (2026 * 12 + 10..2027 * 12 + 2).mapNotNull { StatsYql.isoMonth(it.toString()) }
        assertEquals(listOf("2026-10", "2026-11", "2026-12", "2027-01", "2027-02"), run)
        // Sortable as text is the reason for the padding.
        assertEquals(run.sorted(), run)
        for (bad in listOf("", "0", "-5", "x", "1")) assertNull(StatsYql.isoMonth(bad), "must refuse $bad")
    }

    /**
     * Every label must be one [StatsYql.isoMonth] would produce: the rollup
     * fills gaps by matching these strings against decoded buckets, and a month
     * spelled two ways draws twice.
     */
    @Test
    fun `the month axis runs from the anchor to now, in the format the buckets decode to`() {
        val jan2023 = YearMonth.of(2023, 1)
        val labels = months(jan2023, at("2023-04-17T05:00:00Z"))
        assertEquals(listOf("2023-01", "2023-02", "2023-03", "2023-04"), labels, "the current month is included, whole")
        assertEquals(StatsYql.isoMonth((2023 * 12 + 1).toString()), labels.first())
        assertEquals(StatsYql.isoMonth((2023 * 12 + 4).toString()), labels.last())
        assertEquals(labels.sorted(), labels, "the page sorts on these")

        // Anchored, so the span grows rather than sliding.
        val later = months(jan2023, at("2026-08-10T00:00:00Z"))
        assertEquals("2023-01", later.first())
        assertEquals("2026-08", later.last())
        assertEquals(44, later.size, "Jan 2023 through Aug 2026 inclusive")

        // One second before the anchor month is December, and a window opening there returns a bucket for it.
        val start = StatsYql.startOfMonth(jan2023)
        assertEquals(at("2023-01-01T00:00:00Z"), start)
        assertEquals("2022-12", months(YearMonth.of(2022, 12), start - 1).last())

        // A clock behind the anchor yields no axis, not a backwards one.
        assertEquals(emptyList(), StatsYql.monthSlicesFrom(jan2023, at("2022-12-31T23:59:59Z")))
        assertEquals(listOf("2023-01"), months(jan2023, start))
    }

    /**
     * A gap between windows drops what was signed in it; an overlap puts one
     * month in two slices, whose distinct authors cannot be recombined.
     */
    @Test
    fun `year slices tile the month span with no gap and no overlap`() {
        val now = at("2026-08-10T14:23:00Z")
        val slices = StatsYql.monthSlicesFrom(YearMonth.of(2023, 1), now)
        assertEquals(listOf(2023, 2024, 2025, 2026), slices.map { it.year })
        assertEquals(44, slices.sumOf { it.months.size })

        slices.zipWithNext { a, b -> assertEquals(a.until + 1, b.since, "${a.year} must hand straight over to ${b.year}") }
        assertEquals(at("2023-01-01T00:00:00Z"), slices.first().since)
        // The last slice stops at now, which keeps future-dated events out of the newest bar.
        assertEquals(now, slices.last().until)
        assertEquals(at("2026-01-01T00:00:00Z"), slices.last().since)

        for (slice in slices) {
            assertEquals(StatsYql.startOfMonth(YearMonth.parse(slice.months.first())), slice.since)
            assertTrue(slice.months.all { it.startsWith("${slice.year}-") }, "slice ${slice.year} holds ${slice.months}")
            val lastMonthEnds = StatsYql.endOfMonth(YearMonth.parse(slice.months.last()))
            assertTrue(slice.until == lastMonthEnds || slice.until == now, "slice ${slice.year} ends at ${slice.until}")
        }
        val all = slices.flatMap { it.months }
        assertEquals(all.size, all.toSet().size)
        assertEquals(all.sorted(), all)

        assertEquals(at("2024-02-29T23:59:59Z"), StatsYql.endOfMonth(YearMonth.of(2024, 2)))
        assertEquals(StatsYql.startOfMonth(YearMonth.of(2024, 3)), StatsYql.endOfMonth(YearMonth.of(2024, 2)) + 1)
        assertEquals(StatsYql.startOfMonth(YearMonth.of(2025, 1)), StatsYql.endOfMonth(YearMonth.of(2024, 12)) + 1)

        // An anchor partway through a year opens its slice at the anchor, not in January.
        val mid = StatsYql.monthSlicesFrom(YearMonth.of(2023, 4), now).first()
        assertEquals(listOf("2023-04", "2023-05", "2023-06", "2023-07", "2023-08", "2023-09", "2023-10", "2023-11", "2023-12"), mid.months)
        assertEquals(at("2023-04-01T00:00:00Z"), mid.since)
    }

    /** The axis the chart draws: every slice's months, in order. */
    private fun months(
        start: YearMonth,
        nowSeconds: Long,
    ): List<String> = StatsYql.monthSlicesFrom(start, nowSeconds).flatMap { it.months }

    private fun at(iso: String): Long = Instant.parse(iso).epochSecond

    /** `tag_index` pairs are cased: NIP-57's `P` (sender) and `p` (recipient) are different tags. */
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

    /** What makes the distribution a count of relays rather than of spellings. */
    @Test
    fun `relay urls that name one relay canonicalise to one string`() {
        val canonical = canonicalRelay("wss://nos.lol")
        for (spelling in listOf("wss://nos.lol", "wss://nos.lol/", "wss://NOS.LOL", "  wss://nos.lol  ")) {
            assertEquals(canonical, canonicalRelay(spelling), "'$spelling' is the same relay")
        }
        // Idempotent, or a second pass over a rolled-up value would drift.
        assertEquals(canonical, canonicalRelay(canonical))
        // Displayed without the normalizer's trailing slash.
        assertEquals("wss://nos.lol", canonical)
    }

    /**
     * `wss://nos.lol:443` stays distinct: quartz's normalizer keeps an explicit
     * port, and a relay is identified by the url a client AUTH'd against.
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
            ).map { canonicalRelay(it) }
        assertEquals(distinct.size, distinct.toSet().size, "these are six different endpoints: $distinct")
    }

    /** Dropping unparseable urls would make `total` a count of the well-formed ones. */
    @Test
    fun `an unparseable relay url survives as itself`() {
        assertEquals("not a url at all", canonicalRelay("  not a url at all  "))
        assertEquals("", canonicalRelay(""))
    }

    // ---- the queries --------------------------------------------------------

    @Test
    fun `every aggregation is unranked, unlimited and unsorted`() {
        val q = StatsYql.query(StatsYql.countsBy("kind"), StatsYql.window(100, 200))
        assertEquals("select * from event where created_at >= 100 and created_at <= 200 limit 0 | all(group(kind) each(output(count())))", q)
        // `order by` would reintroduce the match phase that unranked avoids, and a capped match set undercounts silently.
        assertTrue(!q.contains("order by"))
        assertEquals("-1", StatsYql.params["grouping.defaultMaxGroups"])
        assertEquals("-1", StatsYql.params["grouping.defaultMaxHits"])
    }

    @Test
    fun `an aggregation can name the document type it runs over`() {
        assertEquals("select * from event where true limit 0 | ${StatsYql.TOTAL}", StatsYql.query(StatsYql.TOTAL))
        assertEquals(
            "select * from reputation where true limit 0 | ${StatsYql.TOTAL}",
            StatsYql.query(StatsYql.TOTAL, source = StatsYql.REPUTATION),
        )
    }

    /**
     * `created_at` is author-signed, so an unbounded `max(created_at)` reports
     * the most optimistically dated spam rather than whether the mirror keeps up.
     */
    @Test
    fun `freshness excludes the future, and the future is its own question`() {
        assertEquals("created_at <= 500", StatsYql.upTo(500))
        assertEquals("created_at > 500", StatsYql.after(500))
        assertEquals("kind = 1 and created_at <= 500", StatsYql.kindUpTo(1, 500))
        // The two partition the corpus at one instant, so a total can be reassembled from the pair.
        assertTrue(StatsYql.upTo(500).contains("<= 500") && StatsYql.after(500).contains("> 500"))
    }

    @Test
    fun `the window is closed at both ends`() {
        // The upper bound keeps one event dated 2100 from opening a bucket 74 years out.
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

        // Vespa 8.733, `all(group(kind) each(all(group(pubkey) output(count()))))`, collapsed onto the kind.
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
        // documents nine months apart; the unpadded values, exactly as returned.
        const val COUNTS_BY_DAY =
            """{"id": "toplevel", "relevance": 1.0, "fields": {"totalCount": 2}, "coverage": {"coverage": 100, "documents": 602, "full": true, "nodes": 1, "results": 1, "resultsFull": 1}, "children": [{"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": [{"id": "grouplist:time.date(created_at)", "relevance": 1.0, "label": "time.date(created_at)", "children": [{"id": "group:string:2025-1-5", "relevance": 0.0, "value": "2025-1-5", "fields": {"count()": 1}}, {"id": "group:string:2025-10-9", "relevance": 0.0, "value": "2025-10-9", "fields": {"count()": 1}}]}]}]}"""
    }
}
