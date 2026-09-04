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
import io.ktor.http.defaultForFilePath
import io.ktor.http.withCharset
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondRedirect
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import java.util.concurrent.ConcurrentHashMap

/**
 * `GET /web/…`: a page's native ES modules, straight off the classpath. Its
 * own prefix because the relay's root is already overloaded, and its own
 * function so a test can mount it alone. One route serves :web's shared assets
 * and each service's own beside them; a jar boundary is not a url.
 */
fun Route.webModules() {
    get("/web/{path...}") {
        val rel =
            call.parameters
                .getAll("path")
                .orEmpty()
                .joinToString("/")
        val asset = WebAssets.get(rel)
        if (asset == null) {
            call.respond(HttpStatusCode.NotFound)
        } else {
            call.respondAsset(asset)
        }
    }
}

/**
 * `GET /favicon.ico`, for everything that has not read the page's markup: a
 * bare `/stats.json` tab, a 404, a crawler. The same bytes as `/web/favicon.ico`.
 */
fun Route.favicon(icon: () -> String? = { null }) {
    get("/favicon.ico") {
        val override = icon()
        if (override != null) {
            // Temporary: a NIP-86 rpc can change the icon mid-run, and a 301 may be cached forever.
            call.respondRedirect(override, permanent = false)
        } else {
            WebAssets.get("favicon.ico")?.let { call.respondAsset(it) } ?: call.respond(HttpStatusCode.NotFound)
        }
    }
}

/** The same exchange [respondPage] runs, for a /web module, which additionally may be reused for a minute. */
internal suspend fun ApplicationCall.respondAsset(asset: WebAssets.Asset) {
    response.header(HttpHeaders.ETag, asset.etag)
    response.header(HttpHeaders.CacheControl, "max-age=60")
    if (matchesEtag(asset.etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondBytes(asset.bytes, asset.contentType)
    }
}

/**
 * A page's ES modules, read off the classpath once each and held. Filled on
 * first use rather than enumerated at boot; the safety is [WEB_PATH], which
 * admits only plain segment names under `web/`.
 */
object WebAssets {
    class Asset(
        val bytes: ByteArray,
        val contentType: ContentType,
        val etag: String,
    )

    private val cache = ConcurrentHashMap<String, Asset>()
    private val WEB_PATH = Regex("^[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)*(?:/[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)*)*$")

    fun get(rel: String): Asset? {
        if (rel.isEmpty() || rel.contains("..") || !WEB_PATH.matches(rel)) return null
        cache[rel]?.let { return it }
        val bytes = javaClass.getResourceAsStream("/web/$rel")?.use { it.readBytes() } ?: return null
        // The modules carry non-ASCII, so text states its charset; anything else keeps the derived type.
        val base = ContentType.defaultForFilePath(rel)
        val type = if (base.contentType == "text" || base.contentSubtype in TEXTUAL) base.withCharset(Charsets.UTF_8) else base
        return cache.computeIfAbsent(rel) { Asset(bytes, type, etagOf(bytes)) }
    }

    private val TEXTUAL = setOf("javascript", "json", "xml", "svg+xml")
}
