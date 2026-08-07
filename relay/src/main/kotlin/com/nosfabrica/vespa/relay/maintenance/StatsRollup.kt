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

import com.nosfabrica.vespa.relay.server.StatsSnapshot
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.time.Instant

/**
 * The corpus statistics document this relay publishes at `GET /stats.json`,
 * and the background job that recomputes it.
 *
 * ## What this answers, and what it does not
 *
 * Every number here describes THIS RELAY'S STORE — what the router has actually
 * mirrored — not the Nostr network. The two are easy to confuse because the
 * charts look identical, and the confusion runs one way: a reader who compares
 * our event count against a network-wide dashboard's and finds it smaller will
 * read it as a broken relay rather than as a mirror's coverage. The document
 * says so in a `scope` field rather than leaving it to the page, because the
 * JSON is the artifact people will actually reuse.
 *
 * ## Anonymous, always
 *
 * These queries go to the raw engine, not through the trust projection, and
 * that is the only correct choice: the store gates an AUTHENTICATED reader to
 * authors that reader has scored, so the same pipeline run under an operator's
 * own lens would answer a smaller, different question under an identical label.
 * `kind_stats.html` makes the same call for the same reason.
 *
 * ## Sections fail independently
 *
 * Every section is computed by its own queries and carries its own status, so
 * one rejected pipeline costs one panel rather than the document. This is not
 * defensive habit — it is the only way this ships honestly. A grouping pipeline
 * is accepted or rejected WHOLE by Vespa, the four shapes borrowed from
 * `EventYql` are proven against this deployment and the rest are not, and a
 * single try/catch around the lot would turn one bad column into a blank page
 * with nothing to fix it from. A failed section carries the engine's own
 * message and the YQL that produced it, which is enough to correct the pipeline
 * without a debugger.
 */
internal class StatsRollup(
    private val vespa: StatsVespa,
    private val relayUrl: String,
    /** Wall clock in epoch seconds; injected so the window bounds are assertable. */
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
    private val windowDays: Int = DEFAULT_WINDOW_DAYS,
    // In weeks and months, not days: these are what the page prints, and a
    // window stated as "the last 744 days" on a monthly chart is a number no
    // reader converts. The seconds are derived where the query is built.
    private val weekWindowWeeks: Int = DEFAULT_WEEK_WINDOW_WEEKS,
    private val monthWindowMonths: Int = DEFAULT_MONTH_WINDOW_MONTHS,
    private val hourWindowDays: Int = DEFAULT_HOUR_WINDOW_DAYS,
    private val topKinds: Int = DEFAULT_TOP_KINDS,
    private val topRelays: Int = DEFAULT_TOP_RELAYS,
    private val kindSeries: Int = DEFAULT_KIND_SERIES,
) {
    /** Compute the whole document. Never throws: a section that fails says so in the document. */
    suspend fun compute(): JsonObject {
        val startedMs = System.currentTimeMillis()
        val corpus = corpusSection()
        val kinds = kindsSection()
        val activity = activitySection()
        // The per-kind series follow the histogram the kinds section just
        // computed, so the panel tracks the corpus rather than a hardcoded list
        // that would go stale in the direction of hiding whatever grew.
        val kindActivity = kindActivitySection(topKindNumbers(kinds))
        val relays = relaysSection()
        val zaps = zapsSection()
        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            put("relay", relayUrl)
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            put(
                "scope",
                "This relay's own store — the events it has mirrored and serves. NOT the Nostr network: " +
                    "a total below a network-wide dashboard's is this mirror's coverage, not a fault.",
            )
            put("countedAs", "anonymous")
            put("timezone", "UTC")
            put("corpus", corpus)
            put("kinds", kinds)
            put("activity", activity)
            put("kindActivity", kindActivity)
            put("relayDistribution", relays)
            put("zaps", zaps)
            // Declared, not computed. Names what it needs rather than being
            // absent — a panel missing from the document is indistinguishable
            // from one the reader's page is too old to know about, and "we have
            // not built this" is a different fact from "this relay holds none
            // of that".
            put(
                "newUsers",
                pending(
                    "Needs first-seen (min created_at) per pubkey across the whole corpus — one group per author, " +
                        "so a nightly rollup rather than a query this endpoint can run.",
                ),
            )
        }
    }

    /**
     * The kinds to draw a per-kind series for: the largest few from the
     * histogram [kindsSection] just computed.
     *
     * Reads the section's own output rather than re-querying — the histogram is
     * already sorted and already paid for. An empty list when that section
     * failed is the right answer: no series is better than a series over kinds
     * we could not count.
     */
    private fun topKindNumbers(kinds: JsonObject): List<Int> =
        (kinds["data"] as? JsonObject)
            ?.get("top")
            ?.let { it as? JsonArray }
            ?.mapNotNull { entry -> (entry as? JsonObject)?.get("kind")?.jsonPrimitive?.intOrNull }
            ?.take(kindSeries)
            .orEmpty()

    // ---- sections -----------------------------------------------------------

    /** Corpus totals: three independent distinct/count queries over everything. */
    private suspend fun corpusSection(): JsonObject =
        section { errors ->
            val events = attempt(errors, "events") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL)) }
            val pubkeys = attempt(errors, "pubkeys") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"))) }
            val kinds = attempt(errors, "kinds") { StatsYql.singleCount(vespa.group(StatsYql.distinct("kind"))) }
            buildJsonObject {
                events?.let { put("events", it) }
                pubkeys?.let { put("pubkeys", it) }
                kinds?.let { put("kinds", it) }
            }
        }

    /**
     * The per-kind table: documents, distinct authors, and the span of
     * `created_at`, as three queries rather than one combined pipeline.
     *
     * Split on purpose. The author count is by far the most expensive of the
     * three — it builds a pubkey set per kind — and it is also the one shape
     * here with no proven precedent in the store. Combined, a rejection of that
     * clause would cost the whole table; split, the counts still render and the
     * authors column reads "—".
     */
    private suspend fun kindsSection(): JsonObject =
        section { errors ->
            val counts =
                attempt(errors, "events") { longsByGroup(StatsYql.countsBy("kind")) }
                    ?: return@section buildJsonObject { }
            val authors = attempt(errors, "pubkeys") { distinctByGroup(StatsYql.distinctAuthorsBy("kind")) }
            val spans = attempt(errors, "span") { spansByGroup(StatsYql.spanBy("kind")) }
            buildJsonObject {
                put("total", counts.size)
                put("shown", minOf(counts.size, topKinds))
                putJsonArray("top") {
                    counts.entries
                        .sortedWith(compareByDescending<Map.Entry<String, Long>> { it.value }.thenBy { it.key.toIntOrNull() ?: 0 })
                        .take(topKinds)
                        .forEach { (kind, events) ->
                            add(
                                buildJsonObject {
                                    put("kind", kind.toIntOrNull() ?: -1)
                                    put("events", events)
                                    authors?.get(kind)?.let { put("pubkeys", it) }
                                    spans?.get(kind)?.let { (first, last) ->
                                        put("firstSeen", first)
                                        put("lastSeen", last)
                                    }
                                },
                            )
                        }
                }
            }
        }

    /**
     * The time series: events and distinct authors per UTC day, week and month,
     * plus the hour-of-day shape.
     *
     * Three granularities rather than one, because a coarser bucket is NOT the
     * finer one re-added. Events sum, but distinct authors do not: someone who
     * posts every day is one author in the week and seven in the sum of its
     * days. So a weekly "publishing pubkeys" has to be asked of the engine at
     * weekly granularity, and the page's Daily/Weekly/Monthly toggle switches
     * between three answers rather than re-aggregating one.
     */
    private suspend fun activitySection(): JsonObject =
        section { errors ->
            val now = nowSeconds()
            val hourWindow = StatsYql.window(now - hourWindowDays * DAY_SECONDS, now)
            val days = series(errors, "days", StatsYql.DAY, StatsYql::isoDay, now, windowDays)
            val weeks = series(errors, "weeks", StatsYql.WEEK, StatsYql::isoWeekStart, now, weekWindowWeeks * 7)
            // 31 days a month, so the window reaches at least this many whole
            // calendar months back. Over-reaching costs a leading partial
            // month; under-reaching would silently drop one.
            val months = series(errors, "months", StatsYql.MONTH, StatsYql::isoMonth, now, monthWindowMonths * 31)
            val hours = attempt(errors, "hours") { longsByGroup(StatsYql.countsBy(StatsYql.HOUR), hourWindow) }
            buildJsonObject {
                put("windowDays", windowDays)
                put("windowWeeks", weekWindowWeeks)
                put("windowMonths", monthWindowMonths)
                put("hourWindowDays", hourWindowDays)
                days?.let { put("days", it) }
                weeks?.let { put("weeks", it) }
                months?.let { put("months", it) }
                hours?.let { byHour ->
                    putJsonArray("hours") {
                        // Every hour, including the empty ones: a chart that
                        // silently drops 03:00 because nobody posted redraws
                        // 23 bars as if the day were shorter.
                        (0..23).forEach { hour ->
                            add(
                                buildJsonObject {
                                    put("hour", hour)
                                    put("events", byHour[hour.toString()] ?: 0L)
                                },
                            )
                        }
                    }
                }
            }
        }

    /**
     * The relay urls this store's NIP-65 lists name, and how many lists name
     * each — the "where do our users read and write" panel.
     *
     * One grouping over `tag_index` filtered to kind 10002, which is affordable
     * exactly because of what a relay list is: few tags per event, and the
     * values repeat across users, so the distinct set is relay urls (thousands)
     * rather than event ids (millions). The same pipeline on kind 1 would try to
     * return every `e` and `p` tag in the corpus.
     *
     * `lists` and not `users`: kind 10002 is replaceable, so the store holds one
     * per author and the two are equal today — but that equality is the store's
     * supersession behaving, not something this query establishes, and a column
     * headed "users" would be asserting it.
     */
    private suspend fun relaysSection(): JsonObject =
        section(
            note =
                "How many stored NIP-65 lists name each relay. NOT split by read/write: that marker is an `r` tag's " +
                    "THIRD element and tag_index holds only `<letter>:<value>`, so it is not queryable — it needs a " +
                    "walk over kind 10002.",
        ) { errors ->
            val pairs =
                attempt(errors, "relays") { longsByGroup(StatsYql.countsBy(StatsYql.TAG), "kind = 10002") }
                    ?: return@section buildJsonObject { }
            // A 10002 may carry tags other than `r`; keep the relay urls and
            // drop the rest rather than charting whatever else was on the event.
            val relays = pairs.mapNotNull { (pair, count) -> StatsYql.tagValue(pair, 'r')?.let { it to count } }.toMap()
            buildJsonObject {
                put("total", relays.size)
                put("shown", minOf(relays.size, topRelays))
                putJsonArray("top") {
                    relays.entries
                        .sortedWith(compareByDescending<Map.Entry<String, Long>> { it.value }.thenBy { it.key })
                        .take(topRelays)
                        .forEach { (url, lists) ->
                            add(
                                buildJsonObject {
                                    put("relay", url)
                                    put("lists", lists)
                                },
                            )
                        }
                }
            }
        }

    /**
     * Zap receipts: how many, from how many wallets, and their shape over time.
     *
     * Deliberately NOT sats. The amount lives in the `bolt11` tag and in the
     * kind-9734 request nested in `description`, both multi-character tag names
     * that `tag_index` cannot address, and `content` is summary-only — so no
     * grouping query can reach a number of satoshis, and a total here would have
     * to be invented. Senders and recipients are the same story for a different
     * reason: they ARE addressable (`P:` and `p:`, cased and single-letter), but
     * grouping `tag_index` over kind 9735 would emit every `e:` tag with them,
     * which is one distinct value per zap.
     *
     * `wallets` is the receipt's own author — under NIP-57 that is the LNURL
     * service that settled the invoice, not the person who zapped. Worth having
     * on its own terms; it is not a user count and is not labelled as one.
     */
    private suspend fun zapsSection(): JsonObject =
        section(
            note =
                "Receipt counts only. Sats are unreachable by any grouping — the amount is in the `bolt11` and " +
                    "`description` tags, whose multi-character names are absent from tag_index — and senders/recipients " +
                    "would drag every `e:` tag along with them. Both need a walk over kind 9735.",
        ) { errors ->
            val now = nowSeconds()
            val window = StatsYql.window(now - windowDays * DAY_SECONDS, now)
            val receipts = attempt(errors, "receipts") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, "kind = 9735")) }
            val wallets = attempt(errors, "wallets") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = 9735")) }
            val days =
                attempt(errors, "days") {
                    bucketed(StatsYql.countsBy(StatsYql.DAY), "kind = 9735 and $window", StatsYql::isoDay, distinct = false)
                }
            buildJsonObject {
                receipts?.let { put("receipts", it) }
                wallets?.let { put("wallets", it) }
                put("windowDays", windowDays)
                days?.let { byDay ->
                    putJsonArray("days") {
                        byDay.entries.sortedBy { it.key }.forEach { (day, count) ->
                            add(
                                buildJsonObject {
                                    put("day", day)
                                    put("receipts", count)
                                },
                            )
                        }
                    }
                }
            }
        }

    /**
     * A daily series per kind, for the largest few — the mirror filling, broken
     * out by what it is filling with.
     *
     * The reference dashboard declares this panel and ships it empty. Ours is
     * one query per kind, which is why it is capped: the cost is linear in the
     * number of series and nobody reads twenty sparklines.
     *
     * Takes the kinds from [kindsSection]'s own histogram rather than a list
     * here, so the panel follows the corpus instead of an opinion about it that
     * would go stale in the direction of hiding whatever grew.
     */
    private suspend fun kindActivitySection(topByEvents: List<Int>): JsonObject =
        section { errors ->
            val now = nowSeconds()
            val since = now - windowDays * DAY_SECONDS
            // Queried first, assembled second: the JSON builders are not
            // coroutine bodies, so a suspending call cannot run inside one.
            val perKind =
                topByEvents.map { kind ->
                    kind to
                        attempt(errors, "kind $kind") {
                            bucketed(
                                StatsYql.countsBy(StatsYql.DAY),
                                StatsYql.windowOfKind(kind, since, now),
                                StatsYql::isoDay,
                                distinct = false,
                            )
                        }
                }
            buildJsonObject {
                put("windowDays", windowDays)
                putJsonArray("kinds") {
                    perKind.forEach { (kind, byDay) ->
                        if (byDay == null) return@forEach
                        add(
                            buildJsonObject {
                                put("kind", kind)
                                putJsonArray("days") {
                                    byDay.entries.sortedBy { it.key }.forEach { (day, count) ->
                                        add(
                                            buildJsonObject {
                                                put("day", day)
                                                put("events", count)
                                            },
                                        )
                                    }
                                }
                            },
                        )
                    }
                }
            }
        }

    /**
     * One bucketed series — events and distinct authors per bucket — as the
     * array the page charts.
     *
     * [decode] turns the engine's bucket value into the label the page sorts and
     * prints. Every bucket pipeline here needs one and none of them agree: a day
     * arrives unpadded, a week as an epoch-relative integer, a month as
     * `year * 12 + month`. Routing all three through this parameter is what
     * keeps [StatsYql.isoDay]'s trap from having to be re-remembered per series.
     */
    private suspend fun series(
        errors: MutableMap<String, String>,
        name: String,
        bucket: String,
        decode: (String) -> String?,
        now: Long,
        spanDays: Int,
    ): JsonArray? {
        val where = StatsYql.window(now - spanDays * DAY_SECONDS, now)
        val events = attempt(errors, "$name.events") { bucketed(StatsYql.countsBy(bucket), where, decode, distinct = false) } ?: return null
        val authors = attempt(errors, "$name.pubkeys") { bucketed(StatsYql.distinctAuthorsBy(bucket), where, decode, distinct = true) }
        return buildJsonArray {
            events.entries.sortedBy { it.key }.forEach { (label, count) ->
                add(
                    buildJsonObject {
                        put("period", label)
                        put("events", count)
                        authors?.get(label)?.let { put("pubkeys", it) }
                    },
                )
            }
        }
    }

    // ---- section plumbing ---------------------------------------------------

    /**
     * Run [body], collecting per-query failures, and wrap the result in the
     * status envelope every section shares:
     *
     *   ok       everything this section asks for came back
     *   partial  some of it did; `data` holds that, `errors` names the rest
     *   failed   none of it did
     *   pending  not computed by this build at all — see [pending]
     *
     * [note] is a caveat on a section that SUCCEEDED — what it deliberately does
     * not contain, and why. Distinct from `errors`, which is what broke, and
     * from `pending`, which is a section with no data at all. Zaps and relay
     * distribution both need one: each answers a real question while leaving out
     * the field a reader of the reference dashboard would come looking for
     * (satoshis; the read/write split), and an unannotated number is how someone
     * concludes we hold no zap amounts rather than that we cannot query them.
     */
    private suspend fun section(
        note: String? = null,
        body: suspend (MutableMap<String, String>) -> JsonObject,
    ): JsonObject {
        val errors = LinkedHashMap<String, String>()
        val startedMs = System.currentTimeMillis()
        val data = body(errors)
        val status =
            when {
                errors.isEmpty() -> "ok"
                data.isEmpty() -> "failed"
                else -> "partial"
            }
        return buildJsonObject {
            put("status", status)
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            note?.let { put("note", it) }
            put("data", data)
            if (errors.isNotEmpty()) {
                putJsonObject("errors") { errors.forEach { (k, v) -> put(k, v) } }
            }
        }
    }

    /** A section this build does not compute, and the reason — see the note in [compute]. */
    private fun pending(note: String): JsonObject =
        buildJsonObject {
            put("status", "pending")
            put("note", note)
        }

    /**
     * Run one query, recording a failure under [key] instead of propagating it.
     *
     * `CancellationException` is rethrown: shutdown cancels the maintenance
     * scope, and a cancelled rollup that recorded itself as a Vespa error would
     * persist a document blaming the engine for the operator stopping the relay.
     */
    private suspend fun <T> attempt(
        errors: MutableMap<String, String>,
        key: String,
        query: suspend () -> T?,
    ): T? =
        try {
            query()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            errors[key] = e.message ?: e.toString()
            null
        }

    // ---- readers ------------------------------------------------------------

    private suspend fun longsByGroup(
        pipeline: String,
        where: String = "true",
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val value = StatsYql.valueOf(g) ?: return@mapNotNull null
                val count = StatsYql.aggOf(g, "count()") ?: return@mapNotNull null
                value to count
            }.toMap()
    }

    private suspend fun distinctByGroup(
        pipeline: String,
        where: String = "true",
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val value = StatsYql.valueOf(g) ?: return@mapNotNull null
                val count = StatsYql.distinctCountOf(g) ?: return@mapNotNull null
                value to count
            }.toMap()
    }

    /**
     * A bucketed pipeline, rekeyed by [decode] — the one path every time series
     * goes through.
     *
     * The decoder is not optional formatting. No bucket pipeline here returns a
     * value that can be used as a chart label or a sort key as it stands: a day
     * arrives unpadded ([StatsYql.isoDay]), a week as an epoch-relative bucket
     * index, a month as `year * 12 + month`. A bucket the decoder rejects is
     * DROPPED rather than passed through, so an engine that changes a format
     * loses points visibly instead of scrambling an axis.
     */
    private suspend fun bucketed(
        pipeline: String,
        where: String,
        decode: (String) -> String?,
        distinct: Boolean,
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val label = StatsYql.valueOf(g)?.let(decode) ?: return@mapNotNull null
                val count = (if (distinct) StatsYql.distinctCountOf(g) else StatsYql.aggOf(g, "count()")) ?: return@mapNotNull null
                label to count
            }.toMap()
    }

    private suspend fun spansByGroup(
        pipeline: String,
        where: String = "true",
    ): Map<String, Pair<Long, Long>> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val value = StatsYql.valueOf(g) ?: return@mapNotNull null
                val first = StatsYql.aggOf(g, "min(created_at)") ?: return@mapNotNull null
                val last = StatsYql.aggOf(g, "max(created_at)") ?: return@mapNotNull null
                value to (first to last)
            }.toMap()
    }

    companion object {
        /**
         * Bumped when a field CHANGES MEANING or leaves, not when one is added:
         * the page reads what it knows and ignores the rest, so additions are
         * already safe. A reader that sees a schema above the one it was
         * written for should say so rather than chart fields it is guessing at.
         *
         * 2 — `retention` removed. It only ever carried a `pending` placeholder,
         * so nothing charted from it and the temptation is to call this an
         * addition-shaped change and leave the number alone. The rule above says
         * a field that LEAVES bumps, and it says that because a reader coding
         * against a key has no way to distinguish "the relay dropped this" from
         * "this rollup happened to fail" without one. Applying it to the cheap
         * case is what makes it trustworthy in the expensive one.
         */
        const val SCHEMA_VERSION = 2

        const val DEFAULT_WINDOW_DAYS = 30

        /** The spans the reference dashboard's own Weekly/Monthly toggle covers. */
        const val DEFAULT_WEEK_WINDOW_WEEKS = 26
        const val DEFAULT_MONTH_WINDOW_MONTHS = 24

        const val DEFAULT_HOUR_WINDOW_DAYS = 7
        const val DEFAULT_TOP_KINDS = 50
        const val DEFAULT_TOP_RELAYS = 50

        /**
         * How many kinds get their own daily series. One query each, so this is
         * the panel's whole cost — and past a handful nobody reads the
         * sparklines anyway.
         */
        const val DEFAULT_KIND_SERIES = 8
        private const val DAY_SECONDS = 86_400L
    }
}

/**
 * Recompute the stats document every [everySeconds] into [snapshot], persisting
 * each result so a restart serves the last one instead of an empty page.
 *
 * Runs BEHIND the server like every other job in this package — the first
 * rollup on a large corpus is minutes of grouping, and no client should wait on
 * it. Until it finishes, `/stats.json` serves whatever the state file held.
 */
internal fun launchStatsRollup(
    scope: CoroutineScope,
    rollup: StatsRollup,
    snapshot: StatsSnapshot,
    everySeconds: Long,
) {
    scope.launch {
        while (true) {
            val startedMs = System.currentTimeMillis()
            runCatching { rollup.compute() }
                .onSuccess { doc ->
                    snapshot.publish(doc)
                    val secs = (System.currentTimeMillis() - startedMs) / 1000
                    // The failure count, not the failures: a section's own
                    // error text is in the document, where the operator can
                    // read it beside the query that produced it.
                    val failed = doc.entries.count { (_, v) -> (v as? JsonObject)?.statusOf() in setOf("failed", "partial") }
                    println(
                        "stats: rolled up in ${secs}s" +
                            (if (failed > 0) " — $failed section(s) incomplete, see /stats.json" else ""),
                    )
                }.onFailure { e ->
                    if (e is CancellationException) throw e
                    // compute() catches per query, so reaching here means the
                    // assembly itself broke — worth a loud line, and worth
                    // keeping the previous document rather than blanking it.
                    System.err.println("stats: rollup failed (${e.message}) — serving the previous document")
                }
            delay(everySeconds * 1000)
        }
    }
}

private fun JsonObject.statusOf(): String? = (this["status"] as? kotlinx.serialization.json.JsonPrimitive)?.content
