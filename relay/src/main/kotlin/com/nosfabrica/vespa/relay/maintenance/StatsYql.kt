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
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.time.Instant
import java.time.YearMonth
import java.time.ZoneOffset

/**
 * The dashboard's aggregation queries: the YQL this relay asks Vespa for its
 * own corpus statistics, and the readers for what comes back.
 *
 * Pure string building and pure tree walking, so both halves are testable
 * without an engine — which matters more here than usual, because a grouping
 * pipeline is accepted or rejected WHOLE by Vespa and the failure arrives as an
 * HTTP 400 with a parser message, not as a wrong number.
 *
 * ## Why this is not built on the store's EventYql
 *
 * `EventYql` already carries the four aggregations the store itself needs —
 * `buildCount`, `buildDistinctCount`, `buildKindHistogram`, `buildDistinctAuthors`
 * — and this file reuses their proven shapes verbatim where they fit. What it
 * cannot reuse is the builder: `EventYql.grouping()` is private, its pipelines
 * are a fixed set, and it lives in vespa-eventstore, so every new pipeline here
 * would be a store release plus a JitPack pin bump before the page could change.
 * The dashboard is the fastest-moving thing in this repo and the store is the
 * slowest; coupling them at that seam would price every chart at a dependency
 * upgrade. These queries are grouping-only, read-only, and touch no filter the
 * store maps, so the duplication is the WHERE clause and nothing else.
 *
 * The two settings that are NOT optional are carried over deliberately — see
 * [params]. Getting either wrong yields a plausible number rather than an error.
 */
internal object StatsYql {
    /**
     * The rank profile every aggregation uses. Load-bearing, not a default:
     * the recency profiles run a match phase that CAPS the match set on a large
     * corpus, so a ranked grouping silently under-counts (EventYql.buildCount
     * measured that at 10x+). Unranked has no match phase.
     */
    const val UNRANKED = "unranked"

    /**
     * Vespa's "no ceiling" sentinel for the two grouping limits that travel as
     * query parameters. Without them a `max()`-less pipeline returns
     * `grouping.defaultMaxGroups` groups — TEN — and reports no error at all,
     * which for a kind histogram is a plausible-looking top-ten.
     *
     * The third ceiling, `grouping.globalMaxGroups`, cannot be sent per-request
     * (Vespa 400s any query carrying it). It lives at -1 in the application
     * package's `search/query-profiles/default.xml`, which the relay deploys on
     * every boot — so these pipelines work here for the same reason the store's
     * own do, and a deployment that replaces that profile breaks both together.
     */
    const val UNLIMITED_GROUPS = "-1"

    /** The query parameters every aggregation below carries. See [UNLIMITED_GROUPS]. */
    val params: Map<String, String> =
        mapOf(
            "grouping.defaultMaxGroups" to UNLIMITED_GROUPS,
            "grouping.defaultMaxHits" to UNLIMITED_GROUPS,
        )

    // ---- pipelines ----------------------------------------------------------
    //
    // The first four are EventYql's, unchanged — they are the shapes this
    // deployment has already run against a 42.8M-doc corpus. The rest extend
    // them along `created_at`, which is a fast-search attribute like `kind` and
    // `pubkey`, so the engine groups it the same way.

    /** Documents in the match set. Exact: grouping over an unranked match set is not a `totalCount` estimate. */
    const val TOTAL = "all(output(count()))"

    /**
     * DISTINCT values of [field] — `count()` on the group LIST, so the engine
     * never materializes the values. `group(pubkey)` this way is how the store
     * counts authors; the same shape over `kind` is how many kinds we hold.
     */
    fun distinct(field: String) = "all(group($field) output(count()))"

    /** One leaf group per [field] value, each carrying its document count. */
    fun countsBy(field: String) = "all(group($field) each(output(count())))"

    /**
     * Oldest and newest `created_at` per [field] value — the `first_seen` /
     * `last_seen` columns, in one pass over the same groups as [countsBy].
     * Separate from it on purpose: see the note on isolation in [StatsRollup].
     */
    fun spanBy(field: String) = "all(group($field) each(output(min(created_at), max(created_at))))"

    /**
     * DISTINCT authors per [field] value: a nested group list whose `count()`
     * is the number of pubkeys, not the number of documents. The inner list
     * carries no `each()`, so — as in [distinct] — the pubkeys themselves never
     * cross the wire; only the size of each set does.
     *
     * **This answers in exactly the same shape as [countsBy] and means
     * something else.** Vespa collapses the inner list's aggregate onto the
     * outer group, so both pipelines come back as one leaf per value carrying
     * one `count()` — verified on Vespa 8.733: over the same corpus,
     * `countsBy("kind")` reported 79 for kind 0 and this reported 35, from
     * responses that are byte-for-byte the same structure. There is no reader
     * that can tell them apart and no error either way; the only thing keeping
     * events out of a column labelled users is calling the right function here.
     */
    fun distinctAuthorsBy(field: String) = "all(group($field) each(all(group(pubkey) output(count()))))"

    /**
     * The UTC calendar day of `created_at`.
     *
     * `time.date` respects Vespa's `timezone` query parameter and defaults to
     * UTC. The parameter is deliberately not set: a relay's day boundary is not
     * the operator's timezone, and a dashboard that silently re-buckets when
     * someone moves the container is worse than one that is always UTC. The
     * page says UTC for the same reason.
     *
     * The group VALUE is not ISO-8601 — see [isoDay], which every reader of
     * this pipeline must go through.
     */
    const val DAY = "time.date(created_at)"

    /**
     * Vespa's `time.date` value as a sortable ISO-8601 date.
     *
     * `time.date` does NOT zero-pad. Verified against Vespa 8.733 on a real
     * node: two documents nine months apart group as `"2025-1-5"` and
     * `"2025-10-9"`, not `"2025-01-05"` and `"2025-10-09"`.
     *
     * Unpadded values sort as text wherever the digit COUNT differs in the same
     * position, so November (`-11-`) lands before February (`-2-`) and the 15th
     * before the 5th. Not everywhere — `"2025-1-5"` and `"2025-10-9"` happen to
     * come out in calendar order, because `'-'` sorts below `'0'` — which is
     * what makes this worth a function rather than a comment: the wrongness is
     * intermittent across a window, so a month of data can look fine and the
     * next one interleaves. Either way nothing errors and every bar keeps its
     * correct height; only the axis is wrong, which reads as noisy data.
     *
     * Returns null for anything that is not `Y-M-D`, so a future engine that
     * starts padding (or stops emitting dates at all) drops the point rather
     * than charting a label the axis cannot order.
     */
    fun isoDay(value: String): String? {
        val parts = value.split('-')
        if (parts.size != 3) return null
        val year = parts[0].toIntOrNull() ?: return null
        val month = parts[1].toIntOrNull()?.takeIf { it in 1..12 } ?: return null
        val day = parts[2].toIntOrNull()?.takeIf { it in 1..31 } ?: return null
        return "%04d-%02d-%02d".format(year, month, day)
    }

    /** The UTC hour-of-day of `created_at`, 0–23 — the shape of a day, folded over the window. */
    const val HOUR = "time.hourofday(created_at)"

    /** Seconds in a 7-day bucket. */
    private const val WEEK_SECONDS = 604_800L

    /** Epoch second 0 back to the preceding Monday, 1969-12-29 — see [WEEK]. */
    private const val WEEK_SHIFT = 259_200L

    /**
     * Monday-aligned 7-day buckets, as one integer.
     *
     * Grouping expressions take ARITHMETIC — verified on 8.733, where this
     * renders as `div(add(created_at, 259200), 604800)` — which is worth more
     * than it looks: a bucket that is a plain integer needs no `time.*` function
     * and therefore inherits none of [isoDay]'s padding problem, and it sorts
     * correctly as a number before anyone formats it.
     *
     * The shift is what puts the boundary on a Monday. Epoch second 0 is a
     * THURSDAY, so `created_at / 604800` alone buckets Thursday-to-Wednesday —
     * which is not wrong, but every reader of a weekly chart will assume weeks
     * start on Monday and none of them will check. [WEEK_SHIFT] is the distance
     * back to the Monday before the epoch (1969-12-29), so bucket 0 starts
     * there and every bucket after it starts on a Monday. Shifting FORWARD to
     * the first Monday after the epoch would be the same idea with negative
     * bucket indices for 1970-1974, and negative integer division is not a
     * thing to bet a chart on.
     */
    const val WEEK = "(created_at + $WEEK_SHIFT) / $WEEK_SECONDS"

    /** The Monday a [WEEK] bucket starts on, ISO-8601; null if the value is not a bucket index. */
    fun isoWeekStart(value: String): String? {
        val bucket = value.toLongOrNull() ?: return null
        val startsAt = bucket * WEEK_SECONDS - WEEK_SHIFT
        if (startsAt < 0) return null
        return Instant
            .ofEpochSecond(startsAt)
            .atOffset(ZoneOffset.UTC)
            .toLocalDate()
            .toString()
    }

    /**
     * Calendar months, as one sortable integer: `year * 12 + month`.
     *
     * Months are the one bucket that cannot be an even division of seconds, and
     * the obvious spelling — nesting `time.year` inside `time.monthofyear` —
     * works but answers in a two-level tree that every reader here would then
     * have to descend. Folding both into one arithmetic expression keeps the
     * response the same flat one-leaf-per-bucket shape as everything else: 2026
     * April comes back as `24316`, and [isoMonth] is the only thing that has to
     * know that.
     */
    const val MONTH = "time.year(created_at) * 12 + time.monthofyear(created_at)"

    /** A [MONTH] bucket as `YYYY-MM`; null if the value is not a month index. */
    fun isoMonth(value: String): String? {
        val index = value.toLongOrNull()?.takeIf { it > 0 } ?: return null
        // `- 1` before the split because month is 1-based: December 2026 is
        // year*12 + 12, which must decode to that year and not to January next.
        val year = (index - 1) / 12
        val month = (index - 1) % 12 + 1
        if (year < 1970 || year > 9999) return null
        return "%04d-%02d".format(year, month)
    }

    /**
     * One calendar year of a monthly series: the months in it, and the window
     * that asks for exactly those months.
     *
     * [months] are the labels [isoMonth] would produce, so a filled series and
     * the engine's own buckets spell a month identically — the only thing that
     * keeps a zero-filled chart from drawing one month twice.
     */
    data class MonthSlice(
        val year: Int,
        val months: List<String>,
        val since: Long,
        val until: Long,
    )

    /**
     * The monthly series from [start] to now, CUT INTO ONE SLICE PER CALENDAR
     * YEAR — one query's worth each, rather than one query for the whole span.
     *
     * ## Why the cut is at a year and not at an arbitrary width
     *
     * Because a [MONTH] bucket must fall ENTIRELY inside one slice. Events could
     * survive a bucket split across two windows — they sum — but the other half
     * of this series cannot: [distinctAuthorsBy] answers distinct pubkeys, and
     * someone who posted in both halves of a split month is one author in the
     * month and two in the sum of its pieces. Years divide months exactly, so
     * every slice's counts are final and merging them is a union of disjoint
     * keys rather than an addition that would quietly overcount authors.
     *
     * The windows are derived from the months themselves — first instant of the
     * first month, last instant of the last — so they tile the span with no gap
     * and no overlap by construction rather than by arithmetic that has to be
     * kept in step with the label enumeration.
     *
     * ## What the slicing buys
     *
     * A bound on ANY ONE query. `distinctAuthorsBy(MONTH)` materialises a pubkey
     * set per bucket, which is the shape whose whole-corpus form OOMKilled this
     * engine twice (see `StatsRollup.kindsSection`). Asked once over an anchored
     * window, that cost grows every month, forever, with nothing in the request
     * path to cap it. Asked a year at a time it is CONSTANT — twelve months of
     * one year, whatever the anchor is — and only the NUMBER of queries grows,
     * by one a year. Total work over the corpus is the same either way; the peak
     * is what moves, and the peak is what kills a container.
     *
     * It also localises failure: a year that the engine refuses costs that
     * year's bars and names itself in the section's errors, instead of taking
     * the whole series with it.
     *
     * The last slice ends at [nowSeconds], not at the year's end — the upper
     * bound every window here carries, for the reason in [window].
     *
     * Empty when [start] is in the future rather than counting backwards. The
     * rollup's clock is injected, so a clock behind the anchor is a state a test
     * can reach, and this is what keeps the sequence finite.
     */
    fun monthSlicesFrom(
        start: YearMonth,
        nowSeconds: Long,
    ): List<MonthSlice> {
        val current = YearMonth.from(Instant.ofEpochSecond(nowSeconds).atOffset(ZoneOffset.UTC))
        if (current < start) return emptyList()
        return generateSequence(start) { it.plusMonths(1) }
            .takeWhile { it <= current }
            .groupBy { it.year }
            .map { (year, months) ->
                MonthSlice(
                    year = year,
                    months = months.map { it.toString() },
                    since = startOfMonth(months.first()),
                    // Clipped, so the current year's slice stops at the present
                    // rather than reaching into the rest of the year and
                    // collecting whatever future-dated spam is signed for it.
                    until = minOf(endOfMonth(months.last()), nowSeconds),
                )
            }
    }

    /**
     * The first instant of [month] in UTC — a monthly window's lower bound.
     *
     * An exact calendar boundary, and it has to be: [MONTH] buckets on
     * `time.monthofyear`, so a window that opens partway through a month still
     * returns that whole month's bucket carrying only the part of it that fell
     * inside — a leading bar that is short for a reason nothing on the page can
     * state. UTC for the same reason [DAY] is.
     */
    fun startOfMonth(month: YearMonth): Long = month.atDay(1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()

    /**
     * The last instant of [month] in UTC — a monthly window's upper bound.
     *
     * The second BEFORE the next month starts, because [window] is inclusive at
     * both ends: spelling this as the next month's first instant would put one
     * second of January in December's slice as well as its own, and that second
     * carries whatever events it carries.
     */
    fun endOfMonth(month: YearMonth): Long = startOfMonth(month.plusMonths(1)) - 1

    /**
     * The derived `<letter>:<value>` tag pairs — the only tag shape a filter or
     * a grouping can address.
     *
     * Grouping this emits EVERY pair on every matched document, so it is only
     * affordable behind a `kind` filter whose events carry few tags and whose
     * values repeat: NIP-65 relay lists (`r:` on kind 10002) are the case it
     * exists for, where the distinct values are relay urls and there are
     * thousands, not millions. Do not reach for it on kind 1.
     *
     * Single-letter names only, and CASED — so NIP-57's `P` (sender) and `p`
     * (recipient) are distinct pairs. Multi-character names (`bolt11`,
     * `description`, `emoji`) are absent by construction, and so is every tag
     * element past the second: NIP-65's read/write marker is an `r` tag's THIRD
     * element and cannot be recovered from here at all.
     */
    const val TAG = "tag_index"

    /** The value of a `<letter>:<value>` pair when its letter is [letter], else null. */
    fun tagValue(
        pair: String,
        letter: Char,
    ): String? = if (pair.length > 2 && pair[0] == letter && pair[1] == ':') pair.substring(2) else null

    // ---- the query ----------------------------------------------------------

    /**
     * One complete aggregation: [pipeline] over the events [where] selects.
     *
     * `limit 0` because no aggregation here wants hits, and NO `order by` —
     * attribute sorting trips the same match phase [UNRANKED] exists to avoid.
     */
    fun query(
        pipeline: String,
        where: String = "true",
        source: String = EVENTS,
    ) = "select * from $source where $where limit 0 | $pipeline"

    /** The stored events — every aggregation here except the trust one. */
    const val EVENTS = "event"

    /**
     * The trust projection's parent documents, one per pubkey the web of trust
     * knows anything about.
     *
     * A SECOND document type, and the only reason to reach for it: whether a
     * reader's ranked search can rank at all depends on this table being
     * populated, and nothing in the `event` corpus reveals that. `TrustReconcile`
     * warns about exactly the failure it detects — "a corpus mirrored before its
     * provider lists arrived stays silently unprojected, and every ranked search
     * comes back empty" — which until now had no number attached to it.
     *
     * Only counted, never decomposed: the scores themselves are mapped TENSORS
     * keyed by observer (`influence_scores`, `follower_counts`), and grouping
     * cannot take a tensor apart. "How trusted is the average author" is not a
     * question this endpoint can ask; "does anyone have trust state at all" is.
     */
    const val REPUTATION = "reputation"

    /**
     * A bare aggregate with NO grouping level — `all(output(max(created_at)))` —
     * is not a thing Vespa can answer.
     *
     * It fails with HTTP 500 and `Cannot invoke SingleResultNode.max(…) because
     * "this.max" is null`, which reads like a bug in the engine rather than a
     * malformed request and sends you looking in the wrong place. `count()` at
     * that level works fine, so the shape looks proven right up until you swap
     * the aggregator. Anything else needs a `group(...) each(...)` around it —
     * which is why the corpus-wide newest event here is derived from the
     * per-kind spans rather than asked for directly.
     */
    const val NO_BARE_AGGREGATES = "group(...) each(output(...)) — see NO_BARE_AGGREGATES"

    /**
     * `created_at` within [since]..[until], inclusive.
     *
     * The upper bound is not decoration. `created_at` is whatever the author
     * signed, so one spam event dated 2100 — the corpus has them; the reference
     * dashboard this page was modelled on reports a `last_seen` of 4130944797,
     * which is the year 2100 — would open a day bucket 74 years out and squash
     * every real bar in the chart against the axis. Bounding the window at
     * `now` costs nothing and is what "the last 30 days" already means.
     */
    fun window(
        since: Long,
        until: Long,
    ) = "created_at >= $since and created_at <= $until"

    /** [window], narrowed to one kind — the per-kind series. */
    fun windowOfKind(
        kind: Int,
        since: Long,
        until: Long,
    ) = "kind = $kind and ${window(since, until)}"

    /**
     * Everything signed no later than [until] — the whole corpus minus the
     * future.
     *
     * What makes "the newest event we hold" mean anything. `created_at` is
     * author-signed, so an unbounded `max(created_at)` reports whatever the most
     * optimistically-dated spam in the corpus claims, and a relay whose mirror
     * died an hour ago would still show a freshness of "in 74 years". Bounded,
     * the number answers the question an operator is actually asking.
     */
    fun upTo(until: Long) = "created_at <= $until"

    /** Events signed for a time that has not happened — clock skew and spam, counted rather than hidden. */
    fun after(instant: Long) = "created_at > $instant"

    /** One kind, bounded to the past — the honest per-kind freshness. */
    fun kindUpTo(
        kind: Int,
        until: Long,
    ) = "kind = $kind and ${upTo(until)}"

    // ---- readers ------------------------------------------------------------

    /**
     * Every `group:` node under the SHALLOWEST group list in [root].
     *
     * Depth-first from the top rather than "the children of root.children[0]":
     * Vespa nests the grouping result under a `group:root:N` wrapper whose depth
     * has changed between versions, and a reader that counts levels breaks on an
     * engine upgrade with an empty chart rather than an error. The shallowest
     * list is unambiguous for every pipeline here, each of which groups once at
     * the top.
     */
    fun topGroups(root: JsonElement): List<JsonObject> = shallowestGroups(listOf(root))

    /**
     * Breadth-first to the first `grouplist:` at or below [from], and its
     * `group:` children.
     *
     * Shared by [topGroups] and [childGroups] so both levels of a [nested]
     * pipeline are read the same way — and so neither counts wrapper depth,
     * which is not part of any contract we control.
     */
    private fun shallowestGroups(from: List<JsonElement>): List<JsonObject> {
        var frontier = from
        // Bounded: the deepest pipeline here nests twice, and the wrapper adds
        // one. Ten is "we are lost", not a tuning parameter.
        repeat(10) {
            val lists = frontier.filterIsInstance<JsonObject>().filter { it.idOf().startsWith("grouplist:") }
            if (lists.isNotEmpty()) {
                return lists.flatMap { list ->
                    list.childList().filterIsInstance<JsonObject>().filter { it.idOf().startsWith("group:") }
                }
            }
            frontier = frontier.flatMap { node -> if (node is JsonObject) node.childList() else emptyList() }
            if (frontier.isEmpty()) return emptyList()
        }
        return emptyList()
    }

    /**
     * A two-level pipeline: [outer] values, each carrying its own [inner]
     * breakdown.
     *
     * One round trip where the obvious shape is N. The per-kind daily series was
     * a query per kind — eight sequential aggregations for eight sparklines —
     * and this asks the same thing once: verified on Vespa 8.733 returning
     * kind → day → count in a single 310ms response.
     *
     * The inner list does NOT collapse onto the outer group the way
     * [distinctAuthorsBy]'s does, because it has an `each()`: there are many
     * inner values to keep, not one aggregate to fold up. Read it with
     * [childGroups].
     */
    fun nested(
        outer: String,
        inner: String,
    ) = "all(group($outer) each(all(group($inner) each(output(count())))))"

    /** The grouped value as text — an int for `group(kind)`, a date for `group(time.date(…))`, one reader for both. */
    fun valueOf(group: JsonObject): String? = (group["value"] as? JsonPrimitive)?.content

    /**
     * The groups of the first list nested UNDER [group] — the inner level of a
     * [nested] pipeline.
     *
     * Same descent as [topGroups], started one level down, so an extra wrapper
     * on either level costs nothing here either.
     */
    fun childGroups(group: JsonObject): List<JsonObject> = shallowestGroups(group.childList())

    /** An aggregate this group carries directly, e.g. `count()` or `max(created_at)`. */
    fun aggOf(
        group: JsonObject,
        name: String,
    ): Long? =
        group["fields"]
            ?.jsonObject
            ?.get(name)
            ?.jsonPrimitive
            ?.longOrNull

    /**
     * The `count()` a [distinctAuthorsBy] group carries — the size of its inner
     * pubkey set.
     *
     * Vespa 8.733 collapses that onto the outer group's own `fields`, so this
     * is [aggOf] on the group itself; the nested-list walk is the fallback for
     * an engine that stops collapsing. Written this way round rather than
     * nested-first because the collapsed shape is what this deployment actually
     * returns, and an unconditional descent would find nothing and report no
     * authors at all.
     *
     * The fallback cannot read the wrong number: in the collapsed shape the
     * group's own `count()` IS the distinct count, and in the nested shape the
     * outer group carries no `count()` for [aggOf] to find.
     */
    fun distinctCountOf(group: JsonObject): Long? {
        aggOf(group, "count()")?.let { return it }
        var frontier = group.childList()
        repeat(10) {
            frontier
                .filterIsInstance<JsonObject>()
                .firstOrNull { it.idOf().startsWith("grouplist:") }
                ?.let { return aggOf(it, "count()") }
            frontier = frontier.flatMap { node -> if (node is JsonObject) node.childList() else emptyList() }
            if (frontier.isEmpty()) return null
        }
        return null
    }

    /** The single `count()` anywhere in [root] — [TOTAL] and [distinct], which group at most once and output once. */
    fun singleCount(root: JsonElement): Long? =
        when (root) {
            is JsonObject -> {
                aggOf(root, "count()")
                    ?: root.childList().firstNotNullOfOrNull { singleCount(it) }
            }

            is JsonArray -> {
                root.firstNotNullOfOrNull { singleCount(it) }
            }

            else -> {
                null
            }
        }

    private fun JsonObject.idOf(): String = (this["id"] as? JsonPrimitive)?.content ?: ""

    private fun JsonObject.childList(): List<JsonElement> = (this["children"] as? JsonArray) ?: emptyList()
}
