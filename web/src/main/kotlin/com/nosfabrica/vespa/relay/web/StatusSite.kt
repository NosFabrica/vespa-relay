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
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.compression.Compression
import io.ktor.server.plugins.compression.deflate
import io.ktor.server.plugins.compression.gzip
import io.ktor.server.plugins.compression.minimumSize
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.routing

/**
 * The policy every public page is served under: no inline, eval'd or foreign script, and no
 * framing. Images, media and sockets reach any origin because the events and the relays they name
 * do; inline style attributes stay, since the renderers emit them per event.
 */
const val PAGE_CONTENT_SECURITY_POLICY =
    "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; style-src-elem 'self'; " +
        "img-src 'self' data: blob: https:; media-src 'self' data: blob: https:; " +
        "connect-src 'self' https: http: wss: ws:; manifest-src 'self'; object-src 'none'; " +
        "frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

/** No framing, no sniffing, no referrer, and [policy] as the CSP, on every response. */
internal fun Application.installSecurityHeaders(policy: String) {
    install(
        createApplicationPlugin("SecurityHeaders") {
            onCall { call ->
                call.response.header("X-Frame-Options", "DENY")
                call.response.header("X-Content-Type-Options", "nosniff")
                call.response.header("Referrer-Policy", "no-referrer")
                call.response.header("Content-Security-Policy", policy)
            }
        },
    )
}

/** Compression, CORS and the security headers, on the terms every public page this repo serves wants them. */
fun Application.installPageDefaults() {
    installSecurityHeaders(PAGE_CONTENT_SECURITY_POLICY)

    // Text above a threshold only; websocket frames are untouched.
    install(Compression) {
        gzip { priority = 1.0 }
        deflate { priority = 0.9 }
        minimumSize(1024)
    }

    install(CORS) {
        // The endpoints are public by design; the admin rpc's security is the NIP-98 token, not the Origin.
        anyHost()
        allowMethod(HttpMethod.Post)
        allowHeader(HttpHeaders.Authorization)
        allowHeader(HttpHeaders.ContentType)
        // NIP-FE's back-off and auth-scheme hints; neither is CORS-safelisted, so a page cannot read them otherwise.
        exposeHeader(HttpHeaders.RetryAfter)
        exposeHeader(HttpHeaders.WWWAuthenticate)
    }
}

/**
 * `GET /stats.json`, the document a status page charts. Its own function so a test can mount it
 * without the writer.
 */
fun Route.statsDocument(
    snapshot: StatsSnapshot?,
    path: String = "/stats.json",
) {
    snapshot ?: return
    get(path) {
        val doc = snapshot.served()
        if (doc == null) {
            // 503, not an empty document: a 200 of zeros looks like a service holding nothing.
            call.respondText(
                """{"error":"no statistics computed yet"}""",
                ContentType.Application.Json,
                HttpStatusCode.ServiceUnavailable,
            )
        } else {
            call.respondDocument(doc.bytes, doc.etag)
        }
    }
}

/**
 * A background service's own status site: one page, the document it charts, and the assets both
 * need. Each service binds its own port, so liveness is the connection. [wait] is false by default.
 */
fun serveStatusSite(
    port: Int,
    page: String,
    snapshot: StatsSnapshot?,
    // Null keeps the page's markup byte-identical to the classpath's.
    icon: String? = null,
    routes: Route.() -> Unit = {},
    wait: Boolean = false,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    val cached = CachedPage(pageWithIcon(page, icon))
    return embeddedServer(Netty, port = port) {
        installPageDefaults()
        routing {
            get("/") { call.respondPage(cached) }
            statsDocument(snapshot)
            webModules()
            favicon { icon }
            routes()
        }
    }.start(wait = wait)
}
