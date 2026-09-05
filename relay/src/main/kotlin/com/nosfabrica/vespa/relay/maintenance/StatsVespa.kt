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

/** Somewhere to send a [StatsYql] pipeline; the seam [StatsRollup] depends on instead of the engine. */
internal interface StatsQueries {
    /** Run [pipeline] over the [source] documents [where] selects and return Vespa's `root`. */
    suspend fun group(
        pipeline: String,
        where: String = "true",
        source: String = StatsYql.EVENTS,
    ): JsonObject
}

/**
 * The one place the dashboard talks to Vespa: POST a [StatsYql] pipeline to `/search/`, refuse a
 * degraded answer, hand back the parsed root.
 */
internal class StatsVespa(
    vespaUrl: String,
    private val http: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build(),
) : StatsQueries {
    private val searchUrl = URI.create(vespaUrl.trimEnd('/') + "/search/")

    /**
     * Throws on anything that is not a complete answer: a thrown aggregation shows on the page as
     * a named failure, a swallowed one as a wrong chart.
     */
    override suspend fun group(
        pipeline: String,
        where: String,
        source: String,
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
                http.send(
                    HttpRequest
                        .newBuilder(searchUrl)
                        .header("Content-Type", "application/json")
                        // No read timeout: Vespa's own deadline is at its maximum so a slow
                        // aggregation finishes rather than answering by halves.
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
     * Refuse a partial answer by Vespa's own `isDegraded()` test, `coverage` at 100 with no
     * `degraded` block, not by the `full` flag, which uses a different denominator.
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
     * Log the whole failure, throw a message safe to publish in `/stats.json`: Vespa's body names
     * hosts and ports and stays in the log.
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
