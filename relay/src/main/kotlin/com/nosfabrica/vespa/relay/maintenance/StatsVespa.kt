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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * The one place the dashboard talks to Vespa: POST a [StatsYql] pipeline to
 * `/search/`, refuse a degraded answer, hand back the parsed root.
 *
 * POST rather than GET for the same reason the store POSTs — a grouping
 * pipeline plus its parameters outgrows a sane URL — and on the JDK's own
 * client rather than Ktor's so the rollup adds no dependency to a module whose
 * Ktor artifacts are all server-side.
 */
internal class StatsVespa(
    vespaUrl: String,
    private val http: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build(),
) {
    private val searchUrl = URI.create(vespaUrl.trimEnd('/') + "/search/")

    /**
     * Run [pipeline] over the [source] documents [where] selects and return
     * Vespa's `root`.
     *
     * Throws on anything that is not a complete answer — an HTTP error, a
     * degraded response, a body without a root. A thrown aggregation shows up
     * on the page as a named failure; a swallowed one shows up as a chart that
     * is simply wrong, and no reader could tell which.
     */
    suspend fun group(
        pipeline: String,
        where: String = "true",
        source: String = StatsYql.EVENTS,
    ): JsonObject {
        val yql = StatsYql.query(pipeline, where, source)
        val body =
            buildJsonObject {
                put("yql", yql)
                put("hits", "0")
                put("ranking", StatsYql.UNRANKED)
                StatsYql.params.forEach { (k, v) -> put(k, v) }
            }.toString()

        val response =
            withContext(Dispatchers.IO) {
                // Blocking send on an IO thread rather than sendAsync: this runs
                // on a background timer, and one aggregation is a single round
                // trip. Nothing is waiting on the thread.
                http.send(
                    HttpRequest
                        .newBuilder(searchUrl)
                        .header("Content-Type", "application/json")
                        // No client-side read timeout on purpose. The bundled
                        // query profile puts Vespa's own deadline at its maximum
                        // with soft timeout OFF, precisely so a slow aggregation
                        // finishes instead of returning a quiet half-answer; a
                        // timeout here would reintroduce the truncation one
                        // layer up, as a retry that keeps failing on a corpus
                        // that has simply grown.
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
            }
        if (response.statusCode() >= 400) failed("vespa ${response.statusCode()}", yql, response.body())
        val root =
            Json.parseToJsonElement(response.body()).jsonObject["root"]?.jsonObject
                ?: failed("vespa answered with no root", yql, response.body())
        root.requireUndegraded(yql)
        return root
    }

    /**
     * Refuse a partial answer, asking Vespa's own `isDegraded()` question —
     * `coverage` at 100 with no `degraded` block — rather than its `full` flag.
     *
     * This mirrors `SearchCoverage.undegraded` in the store, and the reasoning
     * there is worth not re-deriving: `full` and `coverage` are computed from
     * DIFFERENT denominators and disagree at both boundaries. `full` is
     * `docs == active`, an exact equality; `coverage` rounds `docs/targetActive`.
     * A node a hair short of its target is `full: false` at 100% — harmless, and
     * refusing it refuses every query while the node settles. A node holding
     * documents not yet active anywhere is `full: true` at any percentage at all
     * — genuinely short, and keying on `full` waves it through. One question
     * answers both spellings; `full` answers neither.
     *
     * A statistic is exactly the kind of number nobody can sanity-check by
     * looking at it, so the residual matters here more than on a feed: at 100%
     * at most 0.5% of the target went unsearched, and that is the accuracy this
     * page can claim.
     */
    private fun JsonObject.requireUndegraded(yql: String) {
        val coverage = this["coverage"]?.jsonObject ?: return
        val pct = coverage["coverage"]?.let { (it as? JsonPrimitive)?.intOrNull } ?: 100
        val degraded = coverage["degraded"]?.jsonObject
        if (pct < 100 || degraded != null) {
            failed("vespa searched only $pct% of the corpus — refusing a partial statistic", yql, "degraded: ${degraded ?: "unspecified"}")
        }
    }

    /**
     * Log the whole failure, throw a message safe to publish.
     *
     * The distinction is the point. `/stats.json` is PUBLIC and unauthenticated,
     * and `StatsRollup` copies an exception's message straight into the document
     * a failed section carries — so anything in that message is served to the
     * internet. Vespa's response body is the part that must not go: it names
     * content nodes, container hostnames and internal ports, none of which is
     * anyone else's business, and all of which used to ride out on the first
     * failed aggregation.
     *
     * The YQL stays in the published message, deliberately. It is our own
     * pipeline, it is in this repo already, and it is the whole diagnostic value
     * of the field — an operator reading "which query broke" without knowing
     * which query is reading nothing. Detail without disclosure: the pipeline
     * and the status code are public, the engine's own words are for the log.
     */
    private fun failed(
        summary: String,
        yql: String,
        detail: String,
    ): Nothing {
        System.err.println("stats: $summary for `$yql` — ${detail.take(500)}")
        error("$summary for `$yql` (engine detail in the relay log)")
    }
}
