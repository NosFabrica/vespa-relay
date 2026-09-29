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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.relay.graph.CypherAccess
import com.vitorpamplona.neo4j.eventstore.GraphProjection
import com.vitorpamplona.neo4j.eventstore.cypher.CypherRequest
import com.vitorpamplona.neo4j.eventstore.cypher.CypherService
import com.vitorpamplona.quartz.nip98HttpAuth.Nip98AuthVerifier
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.respondText
import io.ktor.server.response.respondTextWriter
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * The graph projection's HTTP surface (neo4j-eventstore docs/spec.md §8): read-only Cypher gated by
 * [access], and two public documents — the live schema and the projection's health.
 * The NIP-98 token is verified WITH the body, so a signed query cannot be replayed with another.
 */
internal fun Route.graphRoutes(
    graph: GraphProjection,
    access: CypherAccess,
    admins: Set<String>,
    publicUrl: String,
    verifier: Nip98AuthVerifier = Nip98AuthVerifier(),
) {
    // Public and unauthenticated, so served from a short cache: each build is a dozen count queries.
    val schema = SchemaCache { graph.schema().toString() }
    get("/graph/schema") { call.respondText(schema.get(), ContentType.Application.Json) }
    get("/graph/health.json") { call.respondText(graphHealth(graph).toString(), ContentType.Application.Json) }
    if (access == CypherAccess.OFF) return

    post("/graph/cypher") {
        // Nothing unauthenticated is read: without a token there is no reason to take a body.
        if (access != CypherAccess.PUBLIC && call.request.headers["Authorization"].isNullOrBlank()) {
            return@post call.respondText(error("NIP-98 authorization required"), ContentType.Application.Json, HttpStatusCode.Unauthorized)
        }
        // Bounded, and in bounded time: the body is read before the token can be checked against it.
        val bytes =
            try {
                withTimeout(HttpRelay.DEFAULT_BODY_TIMEOUT_MS) { call.receiveBounded(MAX_CYPHER_BODY_BYTES) }
            } catch (e: TimeoutCancellationException) {
                return@post call.respondText(error("the body did not arrive in time"), ContentType.Application.Json, HttpStatusCode.RequestTimeout)
            } ?: return@post call.respondText(
                error("the body is over $MAX_CYPHER_BODY_BYTES bytes"),
                ContentType.Application.Json,
                HttpStatusCode.PayloadTooLarge,
            )
        val body = bytes.decodeToString()
        val caller: String? =
            if (access == CypherAccess.PUBLIC) {
                null
            } else {
                when (val r = verifier.verify(call.request.headers["Authorization"], "POST", publicUrl.trimEnd('/') + "/graph/cypher", bytes)) {
                    is Nip98AuthVerifier.Result.Verified -> {
                        if (access == CypherAccess.ADMIN && r.pubkey !in admins) {
                            return@post call.respondText(error("not an administrator"), ContentType.Application.Json, HttpStatusCode.Forbidden)
                        } else {
                            r.pubkey
                        }
                    }

                    else -> {
                        return@post call.respondText(error("NIP-98 authorization required"), ContentType.Application.Json, HttpStatusCode.Unauthorized)
                    }
                }
            }
        val request =
            runCatching { CypherRequest.fromJson(body) }.getOrElse {
                return@post call.respondText(error(it.message ?: "bad request"), ContentType.Application.Json, HttpStatusCode.BadRequest)
            }
        // The status is chosen before streaming: a refused query is a 400, never a truncated 200.
        graph.cypher.precheck(request, caller)?.let {
            return@post call.respondText(error("refused: " + it.reason), ContentType.Application.Json, HttpStatusCode.BadRequest)
        }
        call.respondTextWriter(ContentType.Application.Json) {
            val outcome = graph.cypher.execute(request, caller) { chunk -> write(chunk) }
            if (outcome is CypherService.Outcome.Rejected) write(error("refused: " + outcome.reason))
        }
    }
}

private fun error(message: String) = JsonObject(mapOf("error" to JsonPrimitive(message))).toString()

/** A Cypher request is a query and its parameters: a megabyte is far past any real one. */
internal const val MAX_CYPHER_BODY_BYTES = 1L shl 20

/** [build]'s value, rebuilt at most every [ttlMillis]. */
private class SchemaCache(
    private val ttlMillis: Long = 30_000,
    private val build: suspend () -> String,
) {
    @Volatile private var value: String? = null

    @Volatile private var builtAt = 0L

    suspend fun get(): String {
        val now = System.currentTimeMillis()
        value?.let { if (now - builtAt < ttlMillis) return it }
        return build().also {
            value = it
            builtAt = now
        }
    }
}

/** Counts only: what the feed dropped, how far behind it is, what the reconciler last found. */
internal fun graphHealth(graph: GraphProjection): JsonObject {
    val feed = graph.feedStats()
    val last = graph.reconcileLoop.last
    return JsonObject(
        mapOf(
            "queue" to
                JsonObject(
                    mapOf(
                        "queued" to JsonPrimitive(feed.queued),
                        "pendingItems" to JsonPrimitive(feed.pendingItems),
                        "dropped" to JsonPrimitive(feed.dropped),
                        "failures" to JsonPrimitive(feed.failures),
                    ),
                ),
            "lagMillis" to JsonPrimitive(feed.lagMillis),
            "appliedEvents" to JsonPrimitive(feed.appliedEvents),
            "removedIds" to JsonPrimitive(feed.removedIds),
            "dirtyHours" to JsonPrimitive(graph.dirty.pendingHours()),
            "dirtyRemovals" to JsonPrimitive(graph.dirty.pendingRemovals()),
            // Removal ids were dropped for want of room: only the full sweep will find them.
            "removalsOverflowed" to JsonPrimitive(feed.removalsOverflowed),
            "reconcile" to
                JsonObject(
                    mapOf(
                        "lastTickAt" to JsonPrimitive(graph.reconcileLoop.lastTickAt),
                        "sweepCursor" to JsonPrimitive(graph.reconcileLoop.sweepCursor),
                        "missing" to JsonPrimitive(last.missing),
                        "extra" to JsonPrimitive(last.extra),
                        "failed" to JsonPrimitive(last.failed),
                        "windows" to JsonPrimitive(last.windows),
                    ),
                ),
        ),
    )
}
