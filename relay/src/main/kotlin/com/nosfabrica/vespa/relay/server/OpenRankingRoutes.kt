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
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.vitorpamplona.quartz.utils.Hex
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
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
    /** Searches running at once; one past it waits up to [SLOT_WAIT_MILLIS] for a slot, then is refused. */
    val concurrency: Int = DEFAULT_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_CONCURRENCY = 8
    }
}

/** The ORE search backend: the search it answers from and how it is bounded. */
class OpenRanking(
    val search: ScoredProfileSearch,
    val config: OpenRankingConfig,
    /** How long a search waits for a free slot before it is refused. */
    val slotWaitMillis: Long = SLOT_WAIT_MILLIS,
    /** How long the body may take to arrive before the request is a 408. */
    val bodyWaitMillis: Long = BODY_WAIT_MILLIS,
    /** Where a failed search is reported; the caller only sees a 503. */
    val log: (String) -> Unit = { System.err.println(it) },
    /** The relay's read latency, which the mirror throttles ingest on; an ORE search is a read like a REQ. */
    val pressure: ServingPressure? = null,
) {
    internal val slots = Semaphore(config.concurrency)
}

/** Queue a burst briefly, like a websocket's searches, but well inside the server's 5 s proxy timeout. */
internal const val SLOT_WAIT_MILLIS = 2_000L

/** A valid body is a few hundred bytes; one still arriving past this is not coming. */
internal const val BODY_WAIT_MILLIS = 2_000L

/**
 * ORE-05's `POST /search/pubkeys`, ranked by the store's own search relevance: the backend
 * Brainstorm's server proxies verbatim, public but not a provider (no capability document); see README.
 */
internal fun Route.openRanking(ore: OpenRanking) {
    post(SEARCH_PATH) {
        val body =
            try {
                withTimeout(ore.bodyWaitMillis) { call.receiveBounded(MAX_BODY_BYTES.toLong()) }
                    ?: return@post call.respondError(HttpStatusCode.PayloadTooLarge, "request body exceeds $MAX_BODY_BYTES bytes")
            } catch (_: TimeoutCancellationException) {
                call.response.header(HttpHeaders.Connection, "close")
                return@post call.respondError(HttpStatusCode.RequestTimeout, "request body did not arrive within ${ore.bodyWaitMillis / 1000.0}s")
            }
        val ask =
            when (val parsed = parseSearch(body.decodeToString())) {
                is SearchAsk.Valid -> parsed
                is SearchAsk.Invalid -> return@post call.respondError(parsed.status, parsed.reason)
            }
        // The flag, not the timeout's result: the timeout can land after acquire() returned and before the
        // block does, and then withTimeoutOrNull answers null while the permit is held — leaked for good.
        var acquired = false
        withTimeoutOrNull(ore.slotWaitMillis) {
            ore.slots.acquire()
            acquired = true
        }
        if (!acquired) return@post call.respondError(HttpStatusCode.ServiceUnavailable, "busy: too many searches running; retry later")
        try {
            val read = ore.pressure?.begin()
            val found =
                try {
                    ore.search(ask.query, ask.observer, ask.limit).also { read?.finish() }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // The message up to its first ':' ("vespa search 400"): past it the engine quotes its
                    // response body, which can echo the YQL and so what was searched — user data, which the
                    // store keeps only where an operator opts in (CostLedger's slow-query ring).
                    ore.log("ore: search failed (${e::class.simpleName}: ${e.message?.substringBefore(':')?.take(80)}) — answered 503")
                    return@post call.respondError(HttpStatusCode.ServiceUnavailable, "search is unavailable; retry later")
                } finally {
                    // A no-op once finished; a failed or cancelled search is no sample, only no longer in flight.
                    read?.abandon()
                }
            call.answer(found, ask)
        } finally {
            ore.slots.release()
        }
    }
}

/**
 * The observer's 10040 decides, and no other view is substituted (ORE-01): one that names a rank
 * service is the observer's ranking, whatever it holds — a profile with no projected score is gated
 * out, as in a NIP-50 read — and without one there is no ranking to serve.
 */
private suspend fun ApplicationCall.answer(
    found: ScoredSearch,
    ask: SearchAsk.Valid,
) {
    val service = found.rankService
    if (service != null) return respondText(found.toResults(ask, service), ContentType.Application.Json)
    val pov = ask.pov
    if (pov == null) {
        return respondError(HttpStatusCode.ServiceUnavailable, "the provider's House Observer has no trust provider list (kind 10040) naming a rank service on this relay yet; retry later")
    }
    respondError(
        HttpStatusCode.UnprocessableEntity,
        "algorithm '$RELEVANCE_POV' cannot be served for pov $pov: the pov has no trust provider list (kind 10040) naming a rank service. " +
            "Either fall back to the endpoint's default algorithm '$RELEVANCE' for a global view, or publish one",
    )
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
            val key = house.lowercase().takeIf(Hex::isHexKey) ?: return unprocessable("observer is not a valid 64-character lowercase hex pubkey")
            SearchAsk.Valid(query, null, limit, key)
        }

        RELEVANCE_POV -> {
            val pov = obj.string("pov") ?: return unprocessable("Algorithm '$RELEVANCE_POV' requires a 'pov' pubkey")
            val key = pov.lowercase().takeIf(Hex::isHexKey) ?: return unprocessable("pov is not a valid 64-character lowercase hex pubkey")
            SearchAsk.Valid(query, key, limit, key)
        }

        else -> {
            unprocessable("Algorithm '${algorithm.shown()}' is not supported by $SEARCH_PATH")
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

/** A header value carries no control character (Ktor refuses the response) and nothing past latin-1. */
private fun String.latin1(): String = map { if (it.code < 256 && !it.isISOControl()) it else '?' }.joinToString("")

/** A caller's own string, echoed into a reason: control characters replaced, cut to [MAX_SHOWN] characters. */
private fun String.shown(): String {
    val clean = map { if (it.isISOControl()) '?' else it }.joinToString("")
    if (clean.codePointCount(0, clean.length) <= MAX_SHOWN) return clean
    return clean.substring(0, clean.offsetByCodePoints(0, MAX_SHOWN)) + "..."
}

/**
 * ORE-05's `results` and `ttl`, then whose view ranked them: the `algorithm` served, the `observer`
 * whose trust weighted the match, and the `rank_service` its 10040 names (docs/decisions/relay-server.md).
 */
private fun ScoredSearch.toResults(
    ask: SearchAsk.Valid,
    service: String,
): String =
    buildJsonObject {
        putJsonArray("results") {
            hits.take(ask.limit).forEach {
                addJsonObject {
                    put("pubkey", it.pubkey)
                    put("rank", it.score)
                }
            }
        }
        put("ttl", RESULTS_TTL_SECONDS)
        put("algorithm", if (ask.pov == null) RELEVANCE else RELEVANCE_POV)
        put("observer", ask.observer)
        put("rank_service", service)
    }.toString()

private const val SEARCH_PATH = "/search/pubkeys"
private const val X_REASON = "X-Reason"
private const val DEFAULT_LIMIT = 100
private const val MAX_LIMIT = 400
private const val MAX_QUERY_LENGTH = 512

/** Far past the longest valid body: 512 escaped astral characters are 6 KiB, the other fields under 1 KiB. */
private const val MAX_BODY_BYTES = 16 * 1024
private const val MAX_SHOWN = 64
private val CONTROL_CHARS = Regex("[\\x00-\\x1f\\x7f]")
private const val RELEVANCE = "relevance"
private const val RELEVANCE_POV = "relevance-pov"
private const val RESULTS_TTL_SECONDS = 300
