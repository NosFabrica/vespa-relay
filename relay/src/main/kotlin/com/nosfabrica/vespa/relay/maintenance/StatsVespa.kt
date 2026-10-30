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

import com.nosfabrica.vespa.eventstore.EngineReads
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonObject
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/** Somewhere to send a [StatsYql] pipeline; the seam [StatsRollup] depends on instead of the store. */
internal interface StatsQueries {
    /** Run [pipeline] over the [source] documents [scope] selects and return Vespa's `root`. */
    suspend fun group(
        pipeline: String,
        scope: StatsYql.Scope = StatsYql.ALL,
        source: EngineReads.Source = EngineReads.Source.EVENTS,
    ): JsonObject
}

/** A failed statistic whose message is safe to publish: our own words and pipeline, never the engine's. */
internal class StatsQueryFailed(
    message: String,
) : IllegalStateException(message)

/**
 * What `/stats.json` may say about [e]: a [StatsQueryFailed]'s own message, otherwise only the
 * exception's class, since any other message may quote a host, a path or a response body.
 */
internal fun publicReason(e: Throwable): String = (e as? StatsQueryFailed)?.message ?: "${e::class.simpleName ?: "error"} (detail in the relay log)"

/**
 * The one place the dashboard reaches the engine: the store's metered grouping read, which rotates
 * the query endpoints and refuses a degraded answer. Every failure is logged whole and rethrown as
 * a [StatsQueryFailed], since the store's message can quote Vespa's response body.
 */
internal class StatsVespa(
    private val read: suspend (pipeline: String, query: EventQuery, source: EngineReads.Source) -> JsonObject,
    private val requestTimeout: Duration = REQUEST_TIMEOUT,
) : StatsQueries {
    constructor(engine: EngineReads) : this(engine::group)

    override suspend fun group(
        pipeline: String,
        scope: StatsYql.Scope,
        source: EngineReads.Source,
    ): JsonObject {
        val query = EventQuery(kinds = scope.kinds, since = scope.since, until = scope.until)
        val root =
            try {
                withTimeoutOrNull(requestTimeout) { read(pipeline, query, source) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                failed("the store's grouping read failed (${e::class.simpleName})", pipeline, scope, e.toString())
            }
        return root ?: failed("vespa did not answer within $requestTimeout", pipeline, scope, "timed out")
    }

    private fun failed(
        summary: String,
        pipeline: String,
        scope: StatsYql.Scope,
        detail: String,
    ): Nothing {
        System.err.println("stats: $summary for `$pipeline` over $scope — ${detail.take(500)}")
        throw StatsQueryFailed("$summary for `$pipeline` over $scope (engine detail in the relay log)")
    }

    companion object {
        /**
         * Far past any aggregation that finishes, since Vespa's own deadline is what keeps a slow one
         * whole: this catches an engine that stopped answering, which would otherwise hold the tier.
         */
        val REQUEST_TIMEOUT: Duration = 20.minutes
    }
}
