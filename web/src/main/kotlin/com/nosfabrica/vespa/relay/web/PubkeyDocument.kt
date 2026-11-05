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
package com.nosfabrica.vespa.relay.web

import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.serialization.json.JsonObject

/** A service's answer about one pubkey, or null when it is over its rate and has nothing fresh to give. */
fun interface PubkeyDocuments {
    /** [query] is the request's query string, each name to its values in order; the service reads what it understands. */
    fun answer(
        pubkey: String,
        query: Map<String, List<String>>,
    ): JsonObject?
}

/**
 * `GET {path}` with a `{pubkey}` segment: [source]'s document about that pubkey. 400 for anything but
 * 64 hex, 503 while [source] is null (the service is still starting), 429 with `Retry-After` when it is
 * over its rate. A served answer may be cached for [maxAgeSeconds]. Readable from any origin through the
 * site's own CORS ([installPageDefaults]); setting the header here too sent it twice, which a browser refuses.
 */
fun Route.pubkeyDocument(
    path: String,
    maxAgeSeconds: Long,
    source: () -> PubkeyDocuments?,
) {
    require("{pubkey}" in path) { "$path has no {pubkey} segment" }
    get(path) {
        val pubkey = pubkeyOf(call.parameters["pubkey"])
        if (pubkey == null) {
            call.respondText("""{"error":"a pubkey is 64 hex characters"}""", ContentType.Application.Json, HttpStatusCode.BadRequest)
            return@get
        }
        val documents = source()
        if (documents == null) {
            call.respondText("""{"error":"this service is still starting"}""", ContentType.Application.Json, HttpStatusCode.ServiceUnavailable)
            return@get
        }
        val query = call.request.queryParameters.let { params -> params.names().associateWith { params.getAll(it).orEmpty() } }
        val doc = documents.answer(pubkey, query)
        if (doc == null) {
            call.response.header(HttpHeaders.RetryAfter, "1")
            call.respondText("""{"error":"too many requests; ask again in a second"}""", ContentType.Application.Json, HttpStatusCode.TooManyRequests)
            return@get
        }
        call.response.header(HttpHeaders.CacheControl, "public, max-age=$maxAgeSeconds")
        call.respondText(doc.toString(), ContentType.Application.Json)
    }
}

private val PUBKEY = Regex("^[0-9a-f]{64}$")

/** A pubkey as a route takes it: 64 hex in either case, returned lowercase, or null. */
fun pubkeyOf(raw: String?): String? = raw?.lowercase()?.takeIf { PUBKEY.matches(it) }
