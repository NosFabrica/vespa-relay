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
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.vitorpamplona.quartz.kinds.KindNames
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
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.io.File
import java.time.Instant
import java.time.YearMonth

/**
 * Which cadence a section is computed on: the two halves of `/stats.json`.
 *
 * The split is by cost. Cheap is bounded by something other than the corpus:
 * a `count()` over a match set, a grouping behind a genuinely selective
 * `kind` filter, a grouping over a window of days, a file read. Expensive
 * scales with the corpus or its distinct pubkeys: `group(pubkey)` over the
 * store, `distinctAuthorsBy(bucket)`, a `tag_index` grouping, a full-corpus
 * histogram, and anything behind a populous kind's filter.
 *
 * The tier is the section, not the query, because a section carries one
 * `generatedAt` for everything in its `data`. The tiers share the engine and
 * are not serialised; the per-query `queryMs` each section publishes is how a
 * query that has grown too slow for [COUNTERS] gets caught.
 */
internal enum class StatsTier(
    /** What this tier is called in the document, under `tiers`. */
    val member: String,
    /**
     * The top-level members this tier owns. Ownership is total: a member missing
     * from a pass is removed from the served document, see `StatsSnapshot.publish`.
     */
    val sections: Set<String>,
) {
    /**
     * Totals, freshness, trust health and the router's manifest. A small set on
     * purpose: everything here runs fifteen times per charts pass. `sync` is here
     * because it is three file reads; `zaps` is not, because a `kind` filter over
     * millions of receipts bounds the group set and not the walk.
     */
    COUNTERS("counters", setOf("corpus", "trust", "sync")),

    /** Everything whose cost scales with the corpus or a populous kind. Nothing here is watched by the minute. */
    CHARTS("charts", setOf("kinds", "authors", "activity", "kindActivity", "zaps", "relayDistribution")),
}

/**
 * The corpus statistics document this relay publishes at `GET /stats.json`,
 * and the background job that recomputes it.
 *
 * [compute] takes a [StatsTier] and returns only that tier's members;
 * `StatsSnapshot` merges them into the served document, so two sections side
 * by side may carry different `generatedAt`s. Every section is computed by its
 * own queries and carries its own status, so one rejected pipeline costs one
 * panel, with the engine's message and the YQL beside it.
 *
 * Every number describes this relay's store, not the network, and the document
 * says so in `scope`. The queries go to the raw engine, anonymously: under an
 * authenticated reader's lens the same pipeline answers a smaller question.
 */
internal class StatsRollup(
    private val vespa: StatsQueries,
    private val relayUrl: String,
    /** Wall clock in epoch seconds; injected so the window bounds are assertable. */
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
    private val windowDays: Int = DEFAULT_WINDOW_DAYS,
    // In weeks, because that is what the page prints.
    private val weekWindowWeeks: Int = DEFAULT_WEEK_WINDOW_WEEKS,
    /** The first month the monthly series covers: an anchor, not a length. See [DEFAULT_MONTH_SERIES_START]. */
    private val monthSeriesStart: YearMonth = DEFAULT_MONTH_SERIES_START,
    private val hourWindowDays: Int = DEFAULT_HOUR_WINDOW_DAYS,
    /** How far back the counters tier looks for the newest event. See [recentNewest]. */
    private val newestWindowDays: Int = DEFAULT_NEWEST_WINDOW_DAYS,
    private val topRelays: Int = DEFAULT_TOP_RELAYS,
    private val kindSeries: Int = DEFAULT_KIND_SERIES,
    /**
     * The router's manifest on the shared volume; null in a serve-only
     * deployment. The one router file this side still reads: it is config, not
     * state, and must be answerable with the router stopped. See [MirrorReport].
     */
    private val syncManifestFile: File? = null,
) {
    /**
     * Compute one [tier]'s members. Never throws: a section that fails says so
     * in the document. [previous] is the served document, read only for
     * [carriedNewest], so a restart cannot disagree about what the last pass
     * was. [everySeconds] is published so a reader knows how often each half
     * moves.
     */
    suspend fun compute(
        tier: StatsTier,
        previous: JsonObject? = null,
        everySeconds: Long = 0,
    ): JsonObject {
        val startedMs = System.currentTimeMillis()
        val sections = LinkedHashMap<String, JsonObject>()
        when (tier) {
            StatsTier.COUNTERS -> {
                sections["corpus"] = corpusSection(previous)
                sections["trust"] = trustSection()
                // Absent, not empty, without a router: "0 relays" reads as a
                // broken mirror rather than no mirror.
                syncSection()?.let { sections["sync"] = it }
            }

            StatsTier.CHARTS -> {
                // Kinds first: the per-kind series follow its histogram.
                val kinds = kindsSection()
                sections["kinds"] = kinds
                sections["authors"] = authorsSection()
                sections["activity"] = activitySection()
                sections["kindActivity"] = kindActivitySection(topKindNumbers(kinds))
                sections["zaps"] = zapsSection()
                sections["relayDistribution"] = relaysSection()
            }
        }
        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            // One markup file serves relay, mirror and monitor; the heading
            // comes from the document.
            put("title", "Relay stats")
            put("relay", relayUrl)
            // When either tier last touched the document, so a poller can
            // tell two fetches apart; the honest timestamps are per section.
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put(
                "scope",
                "This relay's own store — the events it has mirrored and serves. NOT the Nostr network: " +
                    "a total below a network-wide dashboard's is this mirror's coverage, not a fault.",
            )
            put("countedAs", "anonymous")
            put("counted", "Counted anonymously against the whole store.")
            put("timezone", "UTC")
            putJsonObject("tiers") {
                putJsonObject(tier.member) {
                    put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                    put("tookMs", System.currentTimeMillis() - startedMs)
                    // Omitted rather than zero: "every 0 seconds" is not a cadence.
                    if (everySeconds > 0) put("everySeconds", everySeconds)
                    // What this pass produced, not what the tier owns: `sync` is
                    // absent on a serve-only relay.
                    putJsonArray("sections") { sections.keys.forEach { add(it) } }
                }
            }
            sections.forEach { (member, value) -> put(member, value) }
        }
    }

    /**
     * The kind set this relay mirrors, read off the shared volume: the one
     * section that queries nothing. An unreadable file is a failed section
     * beside working ones, the same contract every queried section has.
     */
    private suspend fun syncSection(): JsonObject? {
        if (syncManifestFile == null) return null
        var data: JsonObject? = null
        val section =
            section { attempts ->
                val mirrors = attempt(attempts, "mirrors") { MirrorReport.build(readOrNull(syncManifestFile)) }
                data = mirrors?.let { buildJsonObject { put("mirrors", it) } }
                data ?: buildJsonObject { }
            }
        // Nothing read and nothing failed means no router has ever written here.
        return if (data == null && section["errors"] == null) null else section
    }

    /** Missing is the normal case; unreadable is not, and is allowed to throw into [attempt]. */
    private fun readOrNull(file: File?): String? = file?.takeIf { it.isFile }?.readText()

    /**
     * The newest `created_at` in the store, as the maximum over the per-kind
     * spans of a [kindsSection]; Vespa answers no bare `max()`, see
     * [StatsYql.NO_BARE_AGGREGATES]. Bounded to the present like the spans.
     */
    private fun newestOf(kinds: JsonObject?): Long? =
        (kinds?.get("data") as? JsonObject)
            ?.get("all")
            ?.let { it as? JsonArray }
            ?.mapNotNull { entry -> (entry as? JsonObject)?.get("lastSeen")?.jsonPrimitive?.longOrNull }
            ?.maxOrNull()

    /**
     * The newest event, asked over a [newestWindowDays] window rather than the
     * store, so the counters tier can afford it every minute. Null when nothing
     * was published in the window; [carriedNewest] answers for that case.
     */
    private suspend fun recentNewest(now: Long): Long? =
        spansByGroup(StatsYql.spanBy("kind"), StatsYql.window(now - newestWindowDays * DAY_SECONDS, now))
            .values
            .maxOfOrNull { (_, last) -> last }

    /**
     * The newest event the last document knew about. A `created_at` carried
     * forward is exactly as true as when it was taken, which a count would not
     * be. Read from both `corpus.newestEvent` and the charts tier's per-kind
     * spans, since either may be absent; the caller takes the maximum.
     */
    private fun carriedNewest(previous: JsonObject?): Long? {
        val corpus =
            ((previous?.get("corpus") as? JsonObject)?.get("data") as? JsonObject)
                ?.get("newestEvent")
                ?.jsonPrimitive
                ?.longOrNull
        return listOfNotNull(corpus, newestOf(previous?.get("kinds") as? JsonObject)).maxOrNull()
    }

    /**
     * The kinds to draw a per-kind series for: the largest few from the
     * histogram just computed. Empty when that section failed.
     */
    private fun topKindNumbers(kinds: JsonObject): List<Int> =
        (kinds["data"] as? JsonObject)
            ?.get("all")
            ?.let { it as? JsonArray }
            ?.mapNotNull { entry -> (entry as? JsonObject)?.get("kind")?.jsonPrimitive?.intOrNull }
            ?.take(kindSeries)
            .orEmpty()

    // ---- sections -----------------------------------------------------------

    /**
     * The headline counters, every one cheap because this half runs about once
     * a minute. `pubkeys` is a full pubkey set over the store and lives in
     * [authorsSection]; `kinds` is the histogram's own `kinds.total`, and a copy
     * on another timer would disagree with it.
     */
    private suspend fun corpusSection(previous: JsonObject?): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val events = attempt(attempts, "events") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL)) }
            // Clock skew and spam. Worth counting: it is why every freshness
            // number here is bounded.
            val future = attempt(attempts, "futureDated") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, StatsYql.after(now))) }
            val newest = attempt(attempts, "newestEvent") { recentNewest(now) }
            buildJsonObject {
                events?.let { put("events", it) }
                future?.let { put("futureDated", it) }
                // The maximum of measured and carried, so this only moves
                // forward: a quiet window or a failed query must not retract it.
                listOfNotNull(newest, carriedNewest(previous)).maxOrNull()?.let { put("newestEvent", it) }
                put("asOf", now)
            }
        }

    /**
     * How many distinct pubkeys the store holds events from. A section of its
     * own because `group(pubkey)` over the corpus materialises every pubkey, and
     * a section carries one `generatedAt` for all its members.
     */
    private suspend fun authorsSection(): JsonObject =
        section(
            note =
                "Every pubkey this relay holds an event from — the store's distinct authors, which is why it is " +
                    "computed on the slow cadence rather than beside the corpus totals. NOT the same population as " +
                    "`trust.scoredPubkeys`; see that section's note.",
        ) { attempts ->
            val pubkeys = attempt(attempts, "pubkeys") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"))) }
            buildJsonObject { pubkeys?.let { put("pubkeys", it) } }
        }

    /**
     * The trust view's own health, read from a second document type. A
     * `scoredPubkeys` of zero is the silent failure `TrustReconcile` warns
     * about, stated as a number. The four counts are a chain (observers name
     * providers, providers publish scores, scores project onto pubkeys), so
     * reading them together localises a break.
     */
    private suspend fun trustSection(): JsonObject =
        section(
            note =
                "`scoredPubkeys` and `authors.pubkeys` are INDEPENDENT populations, not a ratio: the web of " +
                    "trust scores people whose events this relay may not hold, and holds events from people nobody " +
                    "has scored. Compare their magnitudes, not their quotient.",
        ) { attempts ->
            val scored = attempt(attempts, "scoredPubkeys") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, source = StatsYql.REPUTATION)) }
            val observers = attempt(attempts, "observers") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = $KIND_OBSERVER")) }
            val providers = attempt(attempts, "providers") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = $KIND_SCORE")) }
            val scores = attempt(attempts, "scores") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, "kind = $KIND_SCORE")) }
            buildJsonObject {
                scored?.let { put("scoredPubkeys", it) }
                observers?.let { put("observers", it) }
                providers?.let { put("providers", it) }
                scores?.let { put("scores", it) }
            }
        }

    /**
     * What a kind is called, from Quartz's protocol-wide registry rather than
     * the web UI's renderer table, since this table enumerates the store.
     * Emitted into the document so a reader elsewhere can label an axis.
     */
    private fun kindName(kind: Int): String? = KindNames.nameFor(kind)

    /**
     * The per-kind table: documents and the span of `created_at`, every kind,
     * sorted by volume.
     *
     * No author count: a distinct count over a high-cardinality field is the
     * group set, and per kind that partitions the whole corpus into pubkey sets.
     * If the column comes back it needs a different source, not a different
     * query. Every kind rather than a top-N, because the grouping enumerates
     * what the store holds and a truncation would hide the long tail this table
     * exists to reveal.
     */
    private suspend fun kindsSection(): JsonObject =
        section { attempts ->
            val counts =
                attempt(attempts, "events") { longsByGroup(StatsYql.countsBy("kind")) }
                    ?: return@section buildJsonObject { }
            // Bounded to now, or the most optimistically-dated spam in a kind
            // is its "newest"; the future-dated count lives in `corpus`.
            val spans = attempt(attempts, "span") { spansByGroup(StatsYql.spanBy("kind"), StatsYql.upTo(nowSeconds())) }
            buildJsonObject {
                put("total", counts.size)
                putJsonArray("all") {
                    counts.entries
                        .sortedWith(compareByDescending<Map.Entry<String, Long>> { it.value }.thenBy { it.key.toIntOrNull() ?: 0 })
                        .forEach { (kind, events) ->
                            add(
                                buildJsonObject {
                                    val n = kind.toIntOrNull() ?: -1
                                    put("kind", n)
                                    kindName(n)?.let { put("name", it) }
                                    put("events", events)
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
     * plus the hour-of-day shape. Three granularities because distinct authors
     * do not sum from a finer bucket. The monthly series is anchored to a date,
     * filled to its whole span, and asked a year at a time.
     */
    private suspend fun activitySection(): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val hourWindow = StatsYql.window(now - hourWindowDays * DAY_SECONDS, now)
            // Trailing windows: one query each and nothing to fill.
            val days = series(attempts, "days", StatsYql.DAY, StatsYql::isoDay, listOf(Slice(now - windowDays * DAY_SECONDS, now)))
            val weeks =
                series(attempts, "weeks", StatsYql.WEEK, StatsYql::isoWeekStart, listOf(Slice(now - weekWindowWeeks * 7 * DAY_SECONDS, now)))
            // One slice per calendar year, on exact month boundaries.
            val monthSlices = StatsYql.monthSlicesFrom(monthSeriesStart, now)
            val months =
                series(
                    attempts,
                    "months",
                    StatsYql.MONTH,
                    StatsYql::isoMonth,
                    monthSlices.map { Slice(it.since, it.until, key = it.year.toString(), fill = it.months) },
                )
            val hours = attempt(attempts, "hours") { longsByGroup(StatsYql.countsBy(StatsYql.HOUR), hourWindow) }
            buildJsonObject {
                put("windowDays", windowDays)
                put("windowWeeks", weekWindowWeeks)
                // The months the window covers, which exceeds the bar count
                // when a year's query failed. Derived from the slices so the
                // two cannot disagree.
                put("windowMonths", monthSlices.sumOf { it.months.size })
                // The anchor itself, so a reader need not count back from
                // `generatedAt` to find where the series starts.
                put("monthsSince", monthSeriesStart.toString())
                put("hourWindowDays", hourWindowDays)
                days?.let { put("days", it) }
                weeks?.let { put("weeks", it) }
                months?.let { put("months", it) }
                hours?.let { byHour ->
                    putJsonArray("hours") {
                        // Every hour, including the empty ones.
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
     * each. A `tag_index` grouping is affordable here only because a relay list
     * has few tags whose values repeat. `lists`, not `users`: kind 10002 is
     * replaceable, but that equality is the store's, not this query's.
     */
    private suspend fun relaysSection(): JsonObject =
        section(
            note =
                "How many stored NIP-65 lists name each relay. NOT split by read/write: that marker is an `r` tag's " +
                    "THIRD element and tag_index holds only `<letter>:<value>`, so it is not queryable — it needs a " +
                    "walk over kind 10002.",
        ) { attempts ->
            val pairs =
                attempt(attempts, "relays") { longsByGroup(StatsYql.countsBy(StatsYql.TAG), "kind = 10002") }
                    ?: return@section buildJsonObject { }
            // Keep the `r` values and sum per canonical url: the grouping
            // returns one row per distinct string, and a trailing slash makes
            // two strings for one relay. [canonicalRelay] is the normalizer
            // the router dials with.
            val relays =
                pairs
                    .mapNotNull { (pair, count) -> StatsYql.tagValue(pair, 'r')?.let { canonicalRelay(it) to count } }
                    .groupingBy { it.first }
                    .fold(0L) { sum, (_, count) -> sum + count }
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
     * On the slow cadence because kind 9735 is populous and `group(pubkey)` over
     * it walks every receipt. No sats: the amount is in `bolt11` and
     * `description`, which `tag_index` cannot address. `wallets` is the
     * receipt's author, the LNURL service, and is not a user count.
     */
    private suspend fun zapsSection(): JsonObject =
        section(
            note =
                "Receipt counts only. Sats are unreachable by any grouping — the amount is in the `bolt11` and " +
                    "`description` tags, whose multi-character names are absent from tag_index — and senders/recipients " +
                    "would drag every `e:` tag along with them. Both need a walk over kind 9735.",
        ) { attempts ->
            val now = nowSeconds()
            val window = StatsYql.window(now - windowDays * DAY_SECONDS, now)
            val receipts = attempt(attempts, "receipts") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, "kind = 9735")) }
            val wallets = attempt(attempts, "wallets") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = 9735")) }
            val days =
                attempt(attempts, "days") {
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
     * A daily series for each of the largest few kinds, taken from
     * [kindsSection]'s histogram so the panel follows the corpus. Capped
     * because nobody reads twenty sparklines.
     */
    private suspend fun kindActivitySection(topByEvents: List<Int>): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val since = now - windowDays * DAY_SECONDS
            if (topByEvents.isEmpty()) return@section buildJsonObject { put("windowDays", windowDays) }
            // One nested query for every series, not one per kind.
            val perKind =
                attempt(attempts, "series") {
                    nestedBuckets(
                        StatsYql.nested("kind", StatsYql.DAY),
                        "kind in (${topByEvents.joinToString(", ")}) and ${StatsYql.window(since, now)}",
                        StatsYql::isoDay,
                    )
                }.orEmpty()
            buildJsonObject {
                put("windowDays", windowDays)
                putJsonArray("kinds") {
                    // In the histogram's order, so the panel reads like the table.
                    topByEvents.forEach { kind ->
                        val byDay = perKind[kind.toString()] ?: return@forEach
                        add(
                            buildJsonObject {
                                put("kind", kind)
                                kindName(kind)?.let { put("name", it) }
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
     * One bucketed series, events and distinct authors per bucket, as the array
     * the page charts. [decode] turns the engine's bucket value into the label
     * the page sorts; no bucket pipeline returns one usable as is.
     *
     * One query pair per slice, run sequentially so the slicing's peak bound
     * holds, and merged as a union of disjoint keys. [Slice.fill] names the
     * labels emitted at zero when the engine returned no bucket; a grouping only
     * returns buckets that matched. A slice whose events query fails
     * contributes nothing, not even its fill: absent bars are a gap, zero bars a
     * claim. The pubkeys column is all or nothing across slices, because a
     * point without `pubkeys` charts as a real zero.
     */
    private suspend fun series(
        attempts: Attempts,
        name: String,
        bucket: String,
        decode: (String) -> String?,
        slices: List<Slice>,
    ): JsonArray? {
        val events = LinkedHashMap<String, Long>()
        val authors = LinkedHashMap<String, Long>()
        var answered = 0
        var authorsWhole = true
        for (slice in slices) {
            // "months.2024.events" when there are years to tell apart, plain
            // "days.events" when there are not.
            val key = slice.key?.let { "$name.$it" } ?: name
            val where = StatsYql.window(slice.since, slice.until)
            val counts = attempt(attempts, "$key.events") { bucketed(StatsYql.countsBy(bucket), where, decode, distinct = false) }
            if (counts == null) {
                // Both columns lose this slice.
                authorsWhole = false
                continue
            }
            answered++
            // The union: a bucket outside the fill is a disagreement between
            // window and enumeration, and dropping it would hide that.
            val labels = counts.keys + slice.fill
            labels.forEach { events[it] = counts[it] ?: 0L }
            // Empty is not zero: [bucketed] drops a group it cannot read, so
            // an unreadable shape arrives as an empty map, and zero-filling
            // it would make the page state "No publishing pubkeys".
            val byLabel =
                attempt(attempts, "$key.pubkeys") { bucketed(StatsYql.distinctAuthorsBy(bucket), where, decode, distinct = true) }
                    ?.takeIf { it.isNotEmpty() }
            if (byLabel == null) {
                authorsWhole = false
            } else {
                // Zero rather than absent once the column is readable.
                labels.forEach { authors[it] = byLabel[it] ?: 0L }
            }
        }
        if (answered == 0) return null
        return buildJsonArray {
            events.keys.sorted().forEach { label ->
                add(
                    buildJsonObject {
                        put("period", label)
                        put("events", events[label] ?: 0L)
                        if (authorsWhole) authors[label]?.let { put("pubkeys", it) }
                    },
                )
            }
        }
    }

    /**
     * One query's worth of a series: its window, the labels it is responsible
     * for, and [key] to name it in an error. Null [key] keeps a one-query
     * series' errors as `days.events`.
     */
    private data class Slice(
        val since: Long,
        val until: Long,
        val key: String? = null,
        val fill: List<String> = emptyList(),
    )

    // ---- section plumbing ---------------------------------------------------

    /**
     * Run [body], collecting per-query failures, and wrap the result in the
     * shared envelope: `ok`, `partial` (`data` holds what came back, `errors`
     * names the rest) or `failed`. [note] is a caveat on a section that
     * succeeded, what it deliberately leaves out, distinct from `errors`.
     */
    private suspend fun section(
        note: String? = null,
        body: suspend (Attempts) -> JsonObject,
    ): JsonObject {
        val attempts = Attempts()
        val startedMs = System.currentTimeMillis()
        val data = body(attempts)
        return buildJsonObject {
            put("status", attempts.status())
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            note?.let { put("note", it) }
            put("data", data)
            // What each query cost, keyed as `errors` is. Published, not
            // logged: it is what an operator re-tiers a query from. Failures
            // are timed too.
            if (attempts.queryMs.isNotEmpty()) {
                putJsonObject("queryMs") { attempts.queryMs.forEach { (k, v) -> put(k, v) } }
            }
            if (attempts.errors.isNotEmpty()) {
                putJsonObject("errors") { attempts.errors.forEach { (k, v) -> put(k, v) } }
            }
        }
    }

    /**
     * What a section's queries did. The status counts successes rather than
     * inspecting `data`, which is never empty: every section writes its own
     * metadata before a query returns.
     */
    private class Attempts {
        val errors = LinkedHashMap<String, String>()

        /** What each attempt took, keyed as its errors would be. See [section]. */
        val queryMs = LinkedHashMap<String, Long>()
        var succeeded = 0
            private set

        fun ok() {
            succeeded++
        }

        fun status(): String =
            when {
                errors.isEmpty() -> "ok"
                succeeded == 0 -> "failed"
                else -> "partial"
            }
    }

    /**
     * Run one query, recording a failure under [key] instead of propagating it.
     * `CancellationException` is rethrown: a cancelled rollup must not persist
     * a document blaming the engine for shutdown.
     */
    private suspend fun <T> attempt(
        attempts: Attempts,
        key: String,
        query: suspend () -> T?,
    ): T? {
        val startedMs = System.currentTimeMillis()
        return try {
            query().also { attempts.ok() }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            attempts.errors[key] = e.message ?: e.toString()
            null
        } finally {
            // In a finally so a refusal is timed as well as a success.
            attempts.queryMs[key] = System.currentTimeMillis() - startedMs
        }
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

    /** A two-level pipeline: outer value -> inner label -> count. The inner labels go through [decode] like the flat ones. */
    private suspend fun nestedBuckets(
        pipeline: String,
        where: String,
        decode: (String) -> String?,
    ): Map<String, Map<String, Long>> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { outer ->
                val key = StatsYql.valueOf(outer) ?: return@mapNotNull null
                val inner =
                    StatsYql
                        .childGroups(outer)
                        .mapNotNull { g ->
                            val label = StatsYql.valueOf(g)?.let(decode) ?: return@mapNotNull null
                            val count = StatsYql.aggOf(g, "count()") ?: return@mapNotNull null
                            label to count
                        }.toMap()
                key to inner
            }.toMap()
    }

    /**
     * A bucketed pipeline, rekeyed by [decode]; a bucket the decoder rejects is
     * dropped rather than passed through, so a changed format loses points
     * visibly instead of scrambling an axis.
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
         * Bumped when a released field changes meaning or leaves, not when one is
         * added. 2: `pubkeys` moved from `corpus` to `authors.pubkeys`, `corpus.kinds`
         * dropped as a duplicate of `kinds.total`, and `tookMs` moved from the top
         * level to `tiers.<name>.tookMs`.
         */
        const val SCHEMA_VERSION = 2

        const val DEFAULT_WINDOW_DAYS = 30

        /** The span the reference dashboard's own Weekly toggle covers. */
        const val DEFAULT_WEEK_WINDOW_WEEKS = 26

        /**
         * The monthly chart's first bucket: a date, not a rolling count, so the
         * corpus's early years never walk off the left edge. The window grows a
         * month every month, and [StatsYql.monthSlicesFrom] keeps any one query
         * at twelve months of pubkey sets however far back this sits.
         */
        val DEFAULT_MONTH_SERIES_START: YearMonth = YearMonth.of(2023, 1)

        const val DEFAULT_HOUR_WINDOW_DAYS = 7

        /**
         * How far back [recentNewest] looks, in days. Two so an overnight pause
         * cannot empty the window; not more, because [carriedNewest] answers
         * beyond it with a timestamp rather than a guess.
         */
        const val DEFAULT_NEWEST_WINDOW_DAYS = 2
        const val DEFAULT_TOP_RELAYS = 50

        /** How many kinds get their own daily series. */
        const val DEFAULT_KIND_SERIES = 8
        private const val DAY_SECONDS = 86_400L

        /** NIP-85: the list naming which service scores which dimension for an observer. */
        private const val KIND_OBSERVER = 10040

        /** NIP-85: one published trust score. */
        private const val KIND_SCORE = 30382
    }
}

/**
 * Recompute one [tier] of the stats document every [everySeconds] into
 * [snapshot]. One call per tier, each on its own coroutine; a tier never
 * launched leaves its sections out of the document. Runs behind the server:
 * the first charts pass on a large corpus is minutes of grouping.
 *
 * [everySeconds] is the gap between passes, not a period, so a slow pass
 * delays the next rather than overlapping it.
 */
internal fun launchStatsRollup(
    scope: CoroutineScope,
    rollup: StatsRollup,
    snapshot: StatsSnapshot,
    tier: StatsTier,
    everySeconds: Long,
) {
    scope.launch {
        while (true) {
            val startedMs = System.currentTimeMillis()
            // The served document, read at the start of every pass so a tier
            // picks up what the other has published. See StatsRollup.carriedNewest.
            val previous = snapshot.served()?.doc
            runCatching { rollup.compute(tier, previous, everySeconds) }
                .onSuccess { members ->
                    snapshot.publish(members, owns = tier.sections, tier = tier.member)
                    val secs = (System.currentTimeMillis() - startedMs) / 1000
                    // The count only; each section's error text is in the document.
                    val failed = members.entries.count { (_, v) -> (v as? JsonObject)?.statusOf() in setOf("failed", "partial") }
                    println(
                        "stats: ${tier.member} rolled up in ${secs}s" +
                            (if (failed > 0) " — $failed section(s) incomplete, see /stats.json" else ""),
                    )
                }.onFailure { e ->
                    if (e is CancellationException) throw e
                    // compute() catches per query, so this is the assembly itself
                    // breaking. The previous document stays served.
                    System.err.println("stats: ${tier.member} rollup failed (${e.message}) — serving the previous document")
                    // Marked in the document, and by tier: one cadence can fail for
                    // hours while the other keeps publishing, and an unattributed
                    // notice reads as a page-wide outage.
                    snapshot.markStale("the last ${tier.member} rollup failed: ${e.message ?: e.javaClass.simpleName}", tier = tier.member)
                }
            delay(everySeconds * 1000)
        }
    }
}

private fun JsonObject.statusOf(): String? = (this["status"] as? kotlinx.serialization.json.JsonPrimitive)?.content
