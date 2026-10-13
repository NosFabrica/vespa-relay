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

import com.nosfabrica.vespa.eventstore.ScoredSearch
import com.vitorpamplona.quartz.utils.Hex
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/** One scored profile search: kind-0 hits with their own engine score under `observer`, and its lens. */
typealias ScoredProfileSearch = suspend (q: String, observer: String, limit: Int) -> ScoredSearch

/** The deployment's ORE search backend settings, from the ORE env vars; see docs/configuration.md. */
class OpenRankingConfig(
    /** Searches running at once; one past it is refused, not queued. */
    val concurrency: Int = DEFAULT_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_CONCURRENCY = 8
    }
}

/** How long after its 10040 a pov whose cards are not projected yet answers 202 rather than 422. */
internal const val POV_GRACE_SECONDS = 6 * 3_600L

/** The ORE search backend: the search it answers from and how it is bounded. */
class OpenRanking(
    val search: ScoredProfileSearch,
    val config: OpenRankingConfig,
    val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /** How long a search waits for a free slot before it is refused. */
    val slotWaitMillis: Long = SLOT_WAIT_MILLIS,
) {
    internal val slots = Semaphore(config.concurrency)
}

/** Queue a burst briefly, like a websocket's searches, but well inside the server's 5 s proxy timeout. */
internal const val SLOT_WAIT_MILLIS = 2_000L

/**
 * ORE-05's `POST /search/pubkeys`, ranked by the store's own search relevance: a private backend
 * Brainstorm's server proxies verbatim, not a public provider; see README.
 */
internal fun Route.openRanking(ore: OpenRanking) {
    post(SEARCH_PATH) {
        val ask =
            when (val parsed = parseSearch(call.receiveText())) {
                is SearchAsk.Valid -> parsed
                is SearchAsk.Invalid -> return@post call.respondError(parsed.status, parsed.reason)
            }
        withTimeoutOrNull(ore.slotWaitMillis) { ore.slots.acquire() }
            ?: return@post call.respondError(HttpStatusCode.ServiceUnavailable, "busy: too many searches running; retry later")
        try {
            val found =
                try {
                    ore.search(ask.query, ask.observer, ask.limit)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    return@post call.respondError(HttpStatusCode.ServiceUnavailable, "search is unavailable; retry later")
                }
            call.answer(found, ask.pov, ask.limit, listFresh = { createdAt -> ore.nowSecs() - createdAt < POV_GRACE_SECONDS })
        } finally {
            ore.slots.release()
        }
    }
}

/** The lens decides: only a READY lens is the asked-for view, and no other view is substituted (ORE-01). */
private suspend fun ApplicationCall.answer(
    found: ScoredSearch,
    pov: String?,
    limit: Int,
    listFresh: (createdAt: Long) -> Boolean,
) {
    val lens = found.lens
    if (lens.status == ScoredSearch.LensStatus.READY) return respondText(found.toResults(limit), ContentType.Application.Json)
    if (pov == null) return respondError(HttpStatusCode.ServiceUnavailable, "the provider's House Observer has no ranked trust on this relay yet; retry later")
    val cannot = "algorithm '$RELEVANCE_POV' cannot be served for pov $pov"
    val fallback = "fall back to the endpoint's default algorithm '$RELEVANCE' for a global view"
    if (lens.status == ScoredSearch.LensStatus.NO_LIST) {
        return respondError(HttpStatusCode.UnprocessableEntity, "$cannot: the pov has no trust provider list (kind 10040) naming a rank service. Either $fallback, or publish one")
    }
    if (lens.listCreatedAt?.let(listFresh) == true) {
        return respondComputing("the pov's trust provider list is recent and its scores are still being mirrored; retry the identical request after $RETRY_AFTER_SECONDS seconds")
    }
    respondError(HttpStatusCode.UnprocessableEntity, "$cannot: its trust provider list names a rank service with no scores on this relay. Either $fallback, or have the service publish its scores")
}

/** ORE-05's 202: `Retry-After` and a `computing` body, decided here from the pov's lens. */
private suspend fun ApplicationCall.respondComputing(reason: String) {
    response.headers.append(X_REASON, reason.latin1())
    response.headers.append(HttpHeaders.RetryAfter, RETRY_AFTER_SECONDS.toString())
    val body =
        buildJsonObject {
            put("status", "computing")
            put("retry_after", RETRY_AFTER_SECONDS)
        }
    respondText(body.toString(), ContentType.Application.Json, HttpStatusCode.Accepted)
}

private sealed interface SearchAsk {
    data class Valid(
        val query: String,
        /** Null for the global algorithm, whatever the body carried. */
        val pov: String?,
        val limit: Int,
        /** Whose lens ranks it: the pov, or for a global search the proxy's House Observer. */
        val observer: String,
    ) : SearchAsk

    data class Invalid(
        val status: HttpStatusCode,
        val reason: String,
    ) : SearchAsk
}

private fun parseSearch(body: String): SearchAsk {
    val obj = runCatching { Json.parseToJsonElement(body) }.getOrNull() as? JsonObject ?: return malformed()
    val raw = obj.string("query") ?: return unprocessable("query field is missing or empty")
    val query = raw.replace(CONTROL_CHARS, "").trim()
    if (query.isEmpty()) return unprocessable("query field is missing or empty")
    if (query.codePointCount(0, query.length) > MAX_QUERY_LENGTH) return unprocessable("query exceeds the maximum length of $MAX_QUERY_LENGTH characters")
    val limit =
        when (val l = obj["limit"]) {
            null, JsonNull -> DEFAULT_LIMIT
            else -> (l as? JsonPrimitive)?.takeUnless { it.isString }?.intOrNull ?: return unprocessable("limit must be a positive integer")
        }
    if (limit <= 0) return unprocessable("limit must be a positive integer")
    if (limit > MAX_LIMIT) return unprocessable("limit exceeds the provider's maximum of $MAX_LIMIT")
    val algorithm =
        when (val a = obj["algorithm"]) {
            null, JsonNull -> RELEVANCE
            else -> (a as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return unprocessable("algorithm must be a string")
        }
    return when (algorithm) {
        // ORE-01: a pov sent to a global algorithm is ignored, whatever it is. The proxy names its
        // House Observer: this relay keeps none of its own.
        RELEVANCE -> {
            val house = obj.string("observer") ?: return unprocessable("Algorithm '$RELEVANCE' requires the proxy's 'observer' pubkey")
            val key = house.lowercase().takeIf(Hex::isHex64) ?: return unprocessable("observer is not a valid 64-character lowercase hex pubkey")
            SearchAsk.Valid(query, null, limit, key)
        }

        RELEVANCE_POV -> {
            val pov = obj.string("pov") ?: return unprocessable("Algorithm '$RELEVANCE_POV' requires a 'pov' pubkey")
            val key = pov.lowercase().takeIf(Hex::isHex64) ?: return unprocessable("pov is not a valid 64-character lowercase hex pubkey")
            SearchAsk.Valid(query, key, limit, key)
        }

        else -> {
            unprocessable("Algorithm '$algorithm' is not supported by $SEARCH_PATH")
        }
    }
}

private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.takeIf { it.isString }?.content

private fun malformed() = SearchAsk.Invalid(HttpStatusCode.BadRequest, "request body is malformed or not valid JSON")

private fun unprocessable(reason: String) = SearchAsk.Invalid(HttpStatusCode.UnprocessableEntity, reason)

/** ORE-00's error shape: the reason in the body and in `X-Reason`, whose copy is coerced to latin-1. */
private suspend fun ApplicationCall.respondError(
    status: HttpStatusCode,
    reason: String,
) {
    response.headers.append(X_REASON, reason.latin1())
    respondText(buildJsonObject { put("error", reason) }.toString(), ContentType.Application.Json, status)
}

private fun String.latin1(): String = map { if (it.code < 256) it else '?' }.joinToString("")

private fun ScoredSearch.toResults(limit: Int): String =
    buildJsonObject {
        putJsonArray("results") {
            hits.take(limit).forEach {
                addJsonObject {
                    put("pubkey", it.pubkey)
                    put("rank", it.score)
                }
            }
        }
        put("ttl", RESULTS_TTL_SECONDS)
    }.toString()

private const val SEARCH_PATH = "/search/pubkeys"
private const val X_REASON = "X-Reason"
private const val DEFAULT_LIMIT = 100
private const val MAX_LIMIT = 400
private const val MAX_QUERY_LENGTH = 512
private val CONTROL_CHARS = Regex("[\\x00-\\x1f\\x7f]")
private const val RELEVANCE = "relevance"
private const val RELEVANCE_POV = "relevance-pov"
private const val RESULTS_TTL_SECONDS = 300
private const val RETRY_AFTER_SECONDS = 60
