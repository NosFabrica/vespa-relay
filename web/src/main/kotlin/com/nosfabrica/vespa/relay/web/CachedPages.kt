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
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondText
import java.security.MessageDigest

/**
 * A strong ETag over [bytes] — the first 16 hex of its SHA-256.
 *
 * Content-derived rather than a timestamp on purpose: a jar entry's mtime is
 * the build's, not the file's, so two deploys of an unchanged module would
 * still miss. A hash makes "unchanged" mean unchanged.
 */
internal fun etagOf(bytes: ByteArray): String =
    MessageDigest
        .getInstance("SHA-256")
        .digest(bytes)
        .take(8)
        .joinToString("") { "%02x".format(it) }
        .let { "\"$it\"" }

/** An HTML page, with the validator that saves re-sending it. */
class CachedPage(
    val html: String,
) {
    val etag: String = etagOf(html.toByteArray(Charsets.UTF_8))
}

/**
 * A page whose icon links follow the service's relay document.
 *
 * Two states, and the common one costs nothing: with no override the html is
 * the classpath's own bytes and the [CachedPage] is built once, exactly as
 * before. With one, the markup is re-rendered — and re-rendered AGAIN whenever a
 * NIP-86 `changerelayicon` moves the doc, because a cached page is otherwise
 * frozen at boot and would keep serving a link to an icon the relay no longer
 * claims.
 *
 * Rebuilt on change rather than rendered per request: the substitution and the
 * etag hash together walk 25KB of markup, a page load asks for that markup all
 * the time, and an admin rpc arrives approximately never.
 */
class IconedPage(
    private val template: String,
    icon: String?,
) {
    @Volatile
    private var current: String? = icon

    @Volatile
    var page: CachedPage = CachedPage(pageWithIcon(template, icon))
        private set

    /** Re-theme, unless this is the icon already drawn — most rpcs change something else. */
    fun icon(icon: String?) {
        if (icon == current) return
        current = icon
        page = CachedPage(pageWithIcon(template, icon))
    }
}

/**
 * The icon an operator set, or null when the icon in the doc is the one this
 * service serves anyway.
 *
 * The whole point is the null. With `RELAY_ICON` unset the NIP-11 doc now
 * publishes [selfIconUrl] — this relay's own `/favicon.ico` — so "the doc has an
 * icon" stopped meaning "the operator overrode the icon". Treating it as an
 * override would rewrite every page's `<link rel="icon">` to a url identical to
 * the built-in one it replaced (harmless, but a needless absolute url and a
 * second name for one file), and, worse, point `/favicon.ico` at itself: a
 * browser following that redirect arrives at the route that issued it and loops
 * until it gives up with no icon at all.
 */
fun iconOverride(
    icon: String?,
    selfIconUrl: String?,
): String? = icon?.takeIf { it.isNotBlank() && it != selfIconUrl }

/**
 * An HTML page, revalidated every time but re-sent only when it changed.
 *
 * `no-cache` is NOT "do not store": it means the browser must ask before
 * reusing, which is exactly the property these pages need — a deploy is picked
 * up on the next load, and the modules under /web can never outlive their page
 * by more than their own max-age. What it adds is the 304: reloading a page
 * that has not changed since the last deploy costs a header exchange instead of
 * 25KB of markup and inline CSS re-gzipped from scratch.
 */
suspend fun ApplicationCall.respondPage(page: CachedPage) {
    response.header(HttpHeaders.ETag, page.etag)
    response.header(HttpHeaders.CacheControl, "no-cache")
    if (matchesEtag(page.etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondText(page.html, ContentType.Text.Html)
    }
}

/**
 * A document recomputed behind the server, revalidated every time and re-sent
 * only when the writer actually produced something new.
 *
 * The 304 is the point rather than a nicety: a page polls its document on a
 * timer and the rollup behind it is far slower than the poll, so most fetches
 * are for bytes the reader already has. `no-cache` (revalidate, don't reuse
 * blind) rather than a `max-age` guess, because the interval is an operator
 * setting and a cache lifetime picked here would be wrong for anyone who
 * changed it.
 */
suspend fun ApplicationCall.respondDocument(
    bytes: ByteArray,
    etag: String,
    contentType: ContentType = ContentType.Application.Json,
) {
    response.header(HttpHeaders.ETag, etag)
    response.header(HttpHeaders.CacheControl, "no-cache")
    if (matchesEtag(etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondBytes(bytes, contentType)
    }
}

/**
 * Does the request already hold this exact version?
 *
 * `If-None-Match` is a comma-separated list, and a proxy may hand back a weak
 * form (`W/"…"`) of a tag we minted strong. Both are the same content by
 * construction here — one immutable resource, one hash — so the weak prefix is
 * stripped rather than treated as a mismatch that would re-send the body.
 */
internal fun ApplicationCall.matchesEtag(etag: String): Boolean =
    request.headers[HttpHeaders.IfNoneMatch]
        ?.split(',')
        ?.any { it.trim().removePrefix("W/") == etag } == true
