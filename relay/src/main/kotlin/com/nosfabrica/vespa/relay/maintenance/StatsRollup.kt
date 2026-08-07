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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
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
    private val hourWindowDays: Int = DEFAULT_HOUR_WINDOW_DAYS,
    private val topKinds: Int = DEFAULT_TOP_KINDS,
) {
    /** Compute the whole document. Never throws: a section that fails says so in the document. */
    suspend fun compute(): JsonObject {
        val startedMs = System.currentTimeMillis()
        val corpus = corpusSection()
        val kinds = kindsSection()
        val activity = activitySection()
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
            // Declared, not computed. Each names what it needs rather than
            // being absent — a panel missing from the document is
            // indistinguishable from one the reader's page is too old to know
            // about, and "we have not built this" is a different fact from
            // "this relay holds none of that".
            put(
                "newUsers",
                pending(
                    "Needs first-seen (min created_at) per pubkey across the whole corpus — one group per author, " +
                        "so a nightly rollup rather than a query this endpoint can run.",
                ),
            )
            put(
                "retention",
                pending("Cohorts are built on the same per-pubkey first-seen as newUsers; it lands with that rollup."),
            )
            put(
                "zaps",
                pending(
                    "Counts and distinct senders/recipients are groupable, but sats are not: the amount lives in " +
                        "the `bolt11` and `description` tags, whose names are multi-character and therefore absent " +
                        "from tag_index by construction, and `content` is summary-only. Needs a walk over kind 9735.",
                ),
            )
            put(
                "relayDistribution",
                pending(
                    "NIP-65 read/write markers are the THIRD element of an `r` tag, and tag_index stores only " +
                        "`<letter>:<value>` — so the marker is not queryable. Needs a walk over kind 10002, which is " +
                        "cheap (one event per user).",
                ),
            )
        }
    }

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

    /** The time series: events and distinct authors per UTC day, plus the hour-of-day shape. */
    private suspend fun activitySection(): JsonObject =
        section { errors ->
            val now = nowSeconds()
            val dayWindow = StatsYql.window(now - windowDays * DAY_SECONDS, now)
            val hourWindow = StatsYql.window(now - hourWindowDays * DAY_SECONDS, now)
            val events = attempt(errors, "days.events") { byIsoDay(StatsYql.countsBy(StatsYql.DAY), dayWindow, distinct = false) }
            val authors = attempt(errors, "days.pubkeys") { byIsoDay(StatsYql.distinctAuthorsBy(StatsYql.DAY), dayWindow, distinct = true) }
            val hours = attempt(errors, "hours") { longsByGroup(StatsYql.countsBy(StatsYql.HOUR), hourWindow) }
            buildJsonObject {
                put("windowDays", windowDays)
                put("hourWindowDays", hourWindowDays)
                events?.let { byDay ->
                    putJsonArray("days") {
                        byDay.entries.sortedBy { it.key }.forEach { (day, count) ->
                            add(
                                buildJsonObject {
                                    put("day", day)
                                    put("events", count)
                                    authors?.get(day)?.let { put("pubkeys", it) }
                                },
                            )
                        }
                    }
                }
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

    // ---- section plumbing ---------------------------------------------------

    /**
     * Run [body], collecting per-query failures, and wrap the result in the
     * status envelope every section shares:
     *
     *   ok       everything this section asks for came back
     *   partial  some of it did; `data` holds that, `errors` names the rest
     *   failed   none of it did
     *   pending  not computed by this build at all — see [pending]
     */
    private suspend fun section(body: suspend (MutableMap<String, String>) -> JsonObject): JsonObject {
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
     * The same as [longsByGroup]/[distinctByGroup] for a [StatsYql.DAY]
     * pipeline, rekeyed to ISO dates — see [StatsYql.isoDay] for why the raw
     * group value cannot be used as a chart label or a sort key.
     */
    private suspend fun byIsoDay(
        pipeline: String,
        where: String,
        distinct: Boolean,
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val day = StatsYql.valueOf(g)?.let(StatsYql::isoDay) ?: return@mapNotNull null
                val count = (if (distinct) StatsYql.distinctCountOf(g) else StatsYql.aggOf(g, "count()")) ?: return@mapNotNull null
                day to count
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
         */
        const val SCHEMA_VERSION = 1

        const val DEFAULT_WINDOW_DAYS = 30
        const val DEFAULT_HOUR_WINDOW_DAYS = 7
        const val DEFAULT_TOP_KINDS = 50
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
