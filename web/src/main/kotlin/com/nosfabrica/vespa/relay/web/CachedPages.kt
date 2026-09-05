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

/** A strong ETag over [bytes], the first 16 hex of its SHA-256; a jar entry's mtime is the build's. */
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
 * A page whose icon links follow the service's relay document, rebuilt on change rather than
 * rendered per request.
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

    /** Re-themes, unless this is the icon already drawn. */
    fun icon(icon: String?) {
        if (icon == current) return
        current = icon
        page = CachedPage(pageWithIcon(template, icon))
    }
}

/**
 * The icon an operator set, or null when the doc's icon is [selfIconUrl], the one this service
 * serves anyway; treating that as an override would point `/favicon.ico` at itself.
 */
fun iconOverride(
    icon: String?,
    selfIconUrl: String?,
): String? = icon?.takeIf { it.isNotBlank() && it != selfIconUrl }

/**
 * An HTML page, revalidated every time and re-sent only when it changed. `no-cache` means ask
 * before reusing, not do not store.
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
 * A document recomputed behind the server, polled far more often than it changes. `no-cache`
 * rather than a `max-age`, since the interval is an operator setting.
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
 * Does the request already hold this exact version? `If-None-Match` is a list, and a proxy may
 * weaken a tag minted strong; the weak form is the same content here.
 */
internal fun ApplicationCall.matchesEtag(etag: String): Boolean =
    request.headers[HttpHeaders.IfNoneMatch]
        ?.split(',')
        ?.any { it.trim().removePrefix("W/") == etag } == true
