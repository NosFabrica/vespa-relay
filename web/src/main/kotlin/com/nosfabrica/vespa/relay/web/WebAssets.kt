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
 * `GET /web/…` — a page's native ES modules, straight off the classpath, no
 * build step.
 *
 * A distinct /web prefix rather than a root fallback: on the relay the root is
 * already three-way overloaded (WS upgrade, NIP-11 negotiation, the landing
 * page), and a wildcard there would have to lose to all of them by routing
 * subtlety instead of by construction.
 *
 * Served from [WebAssets] rather than Ktor's `staticResources` for the
 * validator. A short max-age is the freshness bound the page wants — the html is
 * revalidated every time, so a stale module can outlive a new page by at most
 * a minute — but on its own it means every load past that minute re-downloads
 * all 23 modules in full, and a deep link IS a full load. The classpath carries
 * no validators to revalidate against, so this mints one from the content: a
 * returning reader gets 23 empty 304s instead of ~40KB. The read and the hash
 * happen once per module for the life of the process; before this, every request
 * re-opened the classpath entry and re-gzipped it.
 *
 * Its own function so a test can mount it alone — the module directory is the
 * whole page, and a broken route here is a blank site. It is also why the
 * classpath, rather than a per-module resource root, is the lookup: :web ships
 * the assets every service's page shares, each service ships its own beside
 * them, and one route serves both because a jar boundary is not a url.
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
 * `GET /favicon.ico` — the tab icon at the path a browser guesses.
 *
 * The pages all carry `<link rel="icon">` hints, so this route is for
 * everything that is NOT one of them: `/stats.json` opened in a tab, a 404, a
 * bookmark to the websocket url, and the crawlers and feed readers that only
 * ever ask the well-known path. Before this the relay served no icon at all and
 * a tab showed the browser's blank sheet.
 *
 * The same bytes [webModules] serves at `/web/favicon.ico`, from the same cache
 * — two urls for one resource on purpose. The markup points at `/web/…` because
 * that is where the page's assets live and where the validator and the
 * `max-age` already work; this path exists because a browser asking for it has
 * not read our markup.
 *
 * Its own function so a test can mount it alone, the same reason [webModules]
 * is one.
 */
fun Route.favicon(icon: () -> String? = { null }) {
    get("/favicon.ico") {
        val override = icon()
        if (override != null) {
            // Temporary, not permanent: `RELAY_ICON` is an operator setting and
            // a NIP-86 rpc can change it mid-run, and a 301 is the one redirect
            // a browser is entitled to cache forever — an icon changed once
            // would keep resolving to the old host on every client that saw it.
            call.respondRedirect(override, permanent = false)
        } else {
            // Absent only if someone deleted the resource — a 404 is then the
            // honest answer, and it is the answer this route replaced.
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
 * A page's ES modules, read off the classpath once each and held.
 *
 * Lazily, not enumerated at boot: the resources live inside the jar in a
 * deployment and inside a build directory in a test, and walking either is
 * more machinery than a map that fills itself on first use. The safety comes
 * from the path check rather than from the enumeration — [WEB_PATH] admits only
 * plain segment names, and `..` is rejected outright, so a request can only
 * ever name a file under `web/`.
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
        // The modules are UTF-8 source and carry non-ASCII (the "…" in every
        // clipped label), so the charset has to be stated. Anything that is not
        // text keeps the type Ktor derives from its extension, unannotated.
        val base = ContentType.defaultForFilePath(rel)
        val type = if (base.contentType == "text" || base.contentSubtype in TEXTUAL) base.withCharset(Charsets.UTF_8) else base
        return cache.computeIfAbsent(rel) { Asset(bytes, type, etagOf(bytes)) }
    }

    private val TEXTUAL = setOf("javascript", "json", "xml", "svg+xml")
}
