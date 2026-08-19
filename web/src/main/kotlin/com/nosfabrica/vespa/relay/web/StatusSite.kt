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
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.routing

/**
 * Compression and CORS, on the terms every page this repo serves wants them.
 *
 * One function rather than a block in each service, because these two were
 * arrived at with evidence and a service that quietly omitted one would be
 * slower or broken in a way nobody would think to look for.
 */
fun Application.installPageDefaults() {
    // The pages are ~117KB of text — html, ES modules, css — and none of it
    // was compressed. Measured over a Cloudflare tunnel, a cold load was
    // 1,513ms for 13 requests; text of this shape gives back roughly 4x to
    // gzip, and the saving lands entirely on the link, which is where the
    // time goes for anyone not on localhost.
    //
    // Text only, and above a threshold: the websocket path is untouched
    // (its frames are already small and latency-sensitive), and compressing
    // a 200-byte NIP-11 document costs more than it saves.
    install(Compression) {
        gzip { priority = 1.0 }
        deflate { priority = 0.9 }
        minimumSize(1024)
    }

    install(CORS) {
        // NIP-11 is consumed by browser clients and NIP-86 by browser admin
        // tools; both need CORS, and a stats document a dashboard charts needs
        // it for the same reason. anyHost is correct here — the endpoints are
        // public by design, and the admin RPC's security is the NIP-98 token,
        // not the Origin.
        anyHost()
        allowMethod(HttpMethod.Post)
        allowHeader(HttpHeaders.Authorization)
        allowHeader(HttpHeaders.ContentType)
    }
}

/**
 * `GET /stats.json` — the document a status page charts.
 *
 * Its own function so a test can mount it without standing up the service that
 * writes it.
 */
fun Route.statsDocument(
    snapshot: StatsSnapshot?,
    path: String = "/stats.json",
) {
    snapshot ?: return
    get(path) {
        val doc = snapshot.served()
        if (doc == null) {
            // 503, not an empty document: "no statistics yet" is a state a
            // poller should retry, and a 200 carrying zeros is
            // indistinguishable from a service that genuinely holds nothing.
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
 * A background service's own status site: one page, the document it charts, and
 * the assets both need.
 *
 * ## Why a service serves its own page rather than writing one for the relay
 *
 * The mirror used to publish its state as JSON files on a volume the serving
 * relay read back, re-parsed and re-narrated. That cost ~2,500 lines on the
 * relay's side whose only job was to re-derive what the writer already knew,
 * and it could not answer the question an operator actually asks. A file cannot
 * say whether the process that writes it is alive — the mirror had to stamp a
 * `writtenAt` heartbeat and the reader had to turn it into a `staleForSec`,
 * because "a mirror that has been down for a day used to publish exactly the
 * same card as one mid-cycle". An HTTP request answers that by whether it
 * answers.
 *
 * So each service binds its own port. The state stays in memory where it is
 * produced, the page reads it directly, and liveness is the connection.
 *
 * [wait] is false by default, the opposite of the relay's own server: this is
 * never the thing keeping a process alive — the engine's threads are — and a
 * status site that blocked its caller would have to be started last, which is
 * exactly when it is least useful.
 */
fun serveStatusSite(
    port: Int,
    page: String,
    snapshot: StatsSnapshot?,
    // The icon an operator set, when there is one. Null keeps the page's own
    // markup byte-identical to the classpath's — see [pageWithIcon].
    icon: String? = null,
    // Anything this particular service adds — a health line, a probe endpoint.
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
