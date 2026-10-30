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
 * The dashboard's aggregation pipelines, the [Scope] each runs over, and the readers for what comes
 * back, all testable without an engine. The store sends them ([StatsVespa]): unranked, group
 * ceilings off, a degraded answer refused.
 */
internal object StatsYql {
    // ---- pipelines ----------------------------------------------------------

    /** Documents in the match set. Exact, not a `totalCount` estimate. */
    const val TOTAL = "all(output(count()))"

    /**
     * Reputation documents by key length. The store keeps its own markers there under keys that are
     * not 64 hex, so only [scoredPubkeys]'s group of 64 is the scored population.
     */
    const val KEY_LENGTHS = "all(group(strlen(pubkey)) each(output(count())))"

    /** The scored pubkeys in a [KEY_LENGTHS] answer: 0 when no key is 64 long. */
    fun scoredPubkeys(root: JsonElement): Long = topGroups(root).firstOrNull { valueOf(it) == "64" }?.let { aggOf(it, "count()") } ?: 0L

    /** Distinct values of [field]: `count()` on the group list, so the values are never materialised. */
    fun distinct(field: String) = "all(group($field) output(count()))"

    /** One leaf group per [field] value, each carrying its document count. */
    fun countsBy(field: String) = "all(group($field) each(output(count())))"

    /** Oldest and newest `created_at` per [field] value. */
    fun spanBy(field: String) = "all(group($field) each(output(min(created_at), max(created_at))))"

    /**
     * Distinct authors per [field] value. Answers in exactly the shape of [countsBy], because Vespa
     * collapses the inner list's count onto the outer group; nothing downstream can tell them apart.
     */
    fun distinctAuthorsBy(field: String) = "all(group($field) each(all(group(pubkey) output(count()))))"

    /** The UTC calendar day of `created_at`. The value is not ISO-8601; every reader goes through [isoDay]. */
    const val DAY = "time.date(created_at)"

    /**
     * Vespa's `time.date` value as a sortable ISO-8601 date; `time.date` does not zero-pad. Null for
     * anything that is not `Y-M-D`, so a changed format drops the point rather than mis-ordering an axis.
     */
    fun isoDay(value: String): String? {
        val parts = value.split('-')
        if (parts.size != 3) return null
        val year = parts[0].toIntOrNull() ?: return null
        val month = parts[1].toIntOrNull()?.takeIf { it in 1..12 } ?: return null
        val day = parts[2].toIntOrNull()?.takeIf { it in 1..31 } ?: return null
        return "%04d-%02d-%02d".format(year, month, day)
    }

    /** The UTC hour-of-day of `created_at`, 0–23. */
    const val HOUR = "time.hourofday(created_at)"

    private const val WEEK_SECONDS = 604_800L

    /** Epoch second 0 back to the preceding Monday. */
    private const val WEEK_SHIFT = 259_200L

    /**
     * Monday-aligned 7-day buckets, as one integer. Epoch second 0 is a Thursday, so the shift puts
     * the boundary on a Monday; shifting backward keeps every bucket index non-negative.
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

    /** Calendar months as one sortable integer, `year * 12 + month`; [isoMonth] alone knows the encoding. */
    const val MONTH = "time.year(created_at) * 12 + time.monthofyear(created_at)"

    /** A [MONTH] bucket as `YYYY-MM`; null if the value is not a month index. */
    fun isoMonth(value: String): String? {
        val index = value.toLongOrNull()?.takeIf { it > 0 } ?: return null
        // Month is 1-based: December is year*12 + 12, not January next year.
        val year = (index - 1) / 12
        val month = (index - 1) % 12 + 1
        if (year < 1970 || year > 9999) return null
        return "%04d-%02d".format(year, month)
    }

    /** One calendar year of a monthly series: its months as [isoMonth] labels them, and their window. */
    data class MonthSlice(
        val year: Int,
        val months: List<String>,
        val since: Long,
        val until: Long,
    )

    /**
     * The monthly series from [start] to now, one slice per calendar year. A [MONTH] bucket must
     * fall entirely inside one slice, because distinct authors do not sum across a split month.
     * Empty when [start] is in the future.
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
                    // Clipped to now, so the current slice collects no future-dated spam.
                    until = minOf(endOfMonth(months.last()), nowSeconds),
                )
            }
    }

    /** The first instant of [month] in UTC; a window opening mid-month returns that month's bucket short. */
    fun startOfMonth(month: YearMonth): Long = month.atDay(1).atStartOfDay(ZoneOffset.UTC).toEpochSecond()

    /** The last instant of [month] in UTC; [window] is inclusive at both ends. */
    fun endOfMonth(month: YearMonth): Long = startOfMonth(month.plusMonths(1)) - 1

    /**
     * The derived `<letter>:<value>` tag pairs, the only tag shape a grouping can address. Grouping
     * it emits every pair on every matched document, so it is only affordable behind a selective `kind`.
     */
    const val TAG = "tag_index"

    /** The value of a `<letter>:<value>` pair when its letter is [letter], else null. */
    fun tagValue(
        pair: String,
        letter: Char,
    ): String? = if (pair.length > 2 && pair[0] == letter && pair[1] == ':') pair.substring(2) else null

    // ---- the scope ---------------------------------------------------------

    /** Which events an aggregation reads: [kinds] (empty is every kind) signed within [since]..[until], inclusive. */
    data class Scope(
        val kinds: List<Int> = emptyList(),
        val since: Long? = null,
        val until: Long? = null,
    ) {
        override fun toString(): String =
            listOfNotNull(
                kinds.takeIf { it.isNotEmpty() }?.let { "kind in (${it.joinToString(", ")})" },
                since?.let { "created_at >= $it" },
                until?.let { "created_at <= $it" },
            ).joinToString(" and ").ifEmpty { "every event" }
    }

    /** The whole corpus. */
    val ALL = Scope()

    /**
     * A bare aggregate with no grouping level, `all(output(max(created_at)))`, fails with HTTP 500;
     * only `count()` works at that level. Wrap anything else in a `group(...) each(...)`.
     */
    const val NO_BARE_AGGREGATES = "group(...) each(output(...)) — see NO_BARE_AGGREGATES"

    /** One kind, every signing time. */
    fun ofKind(kind: Int) = Scope(kinds = listOf(kind))

    /**
     * `created_at` within [since]..[until], inclusive. The upper bound matters: `created_at` is
     * author-signed, and one far-future spam event opens a bucket that squashes every real bar.
     */
    fun window(
        since: Long,
        until: Long,
    ) = Scope(since = since, until = until)

    /** Everything signed no later than [until], so "the newest event" is not the worst-dated spam. */
    fun upTo(until: Long) = Scope(until = until)

    /** Events signed for a time that has not happened: clock skew and spam. */
    fun after(instant: Long) = Scope(since = instant + 1)

    // ---- readers ------------------------------------------------------------

    /**
     * Every `group:` node under the shallowest group list in [root]. A search rather than a fixed
     * path: the `group:root:N` wrapper's depth has changed between Vespa versions.
     */
    fun topGroups(root: JsonElement): List<JsonObject> = shallowestGroups(listOf(root))

    /** Breadth-first to the first `grouplist:` at or below [from], and its `group:` children. */
    private fun shallowestGroups(from: List<JsonElement>): List<JsonObject> {
        var frontier = from
        // The deepest pipeline here nests twice; ten levels is lost.
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
     * A two-level pipeline: [outer] values, each carrying its own [inner] breakdown. The inner list
     * keeps its `each()`, so it does not collapse like [distinctAuthorsBy]'s; read it with [childGroups].
     */
    fun nested(
        outer: String,
        inner: String,
    ) = "all(group($outer) each(all(group($inner) each(output(count())))))"

    /** The grouped value as text, whatever the field's type. */
    fun valueOf(group: JsonObject): String? = (group["value"] as? JsonPrimitive)?.content

    /** The groups of the first list nested under [group], the inner level of a [nested] pipeline. */
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
     * The `count()` a [distinctAuthorsBy] group carries. Vespa collapses it onto the outer group's
     * own `fields`; the nested-list walk is the fallback for an engine that stops collapsing.
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

    /** The single `count()` anywhere in [root], for [TOTAL] and [distinct]. */
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
