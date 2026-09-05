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
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

/**
 * `GET /pulse.json`, the live document the pulse page charts.
 *
 * BUILT PER REQUEST, unlike `/stats.json`. That document costs Vespa queries,
 * so it is rolled up on a clock and served from memory; this one is a read of
 * in-process counters and gauges, so rolling it up would only make it older.
 * The [document] lambda is therefore called on the request thread and must
 * stay cheap — `PulseDocument.of` is.
 *
 * `no-store`, and no ETag. Every field moves, `generatedAt` above all, so a
 * validator would never match and computing one would be pure cost. Nothing
 * here should be cached by anything anyway: the page's whole method is to
 * difference two consecutive polls.
 *
 * Its own function so a test can mount it without a store.
 */
fun Route.pulseDocument(
    document: () -> JsonObject?,
    path: String = "/pulse.json",
) {
    get(path) {
        val doc = document()
        call.response.header(HttpHeaders.CacheControl, "no-store")
        if (doc == null) {
            // 503, not an empty document: this process has no metered store, and
            // a 200 of zeros would read as a store doing nothing.
            call.respondText(
                """{"error":"this process serves no metered store"}""",
                ContentType.Application.Json,
                HttpStatusCode.ServiceUnavailable,
            )
        } else {
            call.respondText(JSON.encodeToString(JsonObject.serializer(), doc), ContentType.Application.Json)
        }
    }
}

/** Not pretty-printed: the document is machine-read first, and polled. */
private val JSON = Json

/**
 * The pulse page on its own port, with the document it charts and the assets
 * it needs.
 *
 * ITS OWN PORT, AND OFF BY DEFAULT WHERE IT IS WIRED. The document carries
 * what the relay's public `/stats.json` deliberately never does — with
 * `clientDerived` on, which observer lenses and which search terms are driving
 * the load, and a slow-read log that quotes the query. That is operator data,
 * not public data. Binding it apart is what lets an operator publish the relay
 * port and keep this one on the private side of the network; nothing in this
 * function makes that decision, and nothing here authenticates, so the port
 * itself is the boundary.
 *
 * [wait] is false by default: this is never what keeps a process alive.
 */
fun servePulseSite(
    port: Int,
    page: String,
    document: () -> JsonObject?,
    // Null keeps the page's markup byte-identical to the classpath's. See [pageWithIcon].
    icon: String? = null,
    wait: Boolean = false,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    val cached = CachedPage(pageWithIcon(page, icon))
    return embeddedServer(Netty, port = port) {
        installPageDefaults()
        routing {
            get("/") { call.respondPage(cached) }
            get("/pulse.html") { call.respondPage(cached) }
            pulseDocument(document)
            webModules()
            favicon { icon }
        }
    }.start(wait = wait)
}
