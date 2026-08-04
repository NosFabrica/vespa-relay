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

import com.nosfabrica.vespa.relay.config.defaultRelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import io.ktor.http.CacheControl
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.http.content.staticResources
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets

/**
 * The NIP-11 relay identity served on `GET /` (Accept: application/nostr+json).
 * [selfPubkey] is the relay's OWN key; [contactPubkey] the admin contact key;
 * [contact] a human contact string. [version] overrides the build version.
 */
data class Nip11Info(
    val name: String = "vespa-relay",
    val description: String? = null,
    val icon: String? = null,
    val banner: String? = null,
    val contactPubkey: String? = null,
    val selfPubkey: String? = null,
    val contact: String? = null,
    val version: String? = null,
    val postingPolicy: String? = null,
    val privacyPolicy: String? = null,
    val termsOfService: String? = null,
)

/**
 * Stand up the complete relay on [port] over Ktor's Netty engine:
 *
 *   WS   /  -> the NIP-50 relay ([nostrRelay])
 *   GET  /  -> the NIP-11 doc on Accept: application/nostr+json, else [landingPage]
 *   GET  /web/… -> the landing page's ES modules, straight off the classpath
 *   GET  /npub1…, /nprofile1…, /note1…, /nevent1…, /naddr1… -> [landingPage],
 *        which decodes the identifier and renders the entity itself
 *   GET  /kind_stats.html -> [statsPage] (per-kind COUNTs — an operator diagnostic)
 *   GET  /observer_stats.html -> [observerStatsPage]
 *   POST /  -> the NIP-86 management RPC, when [admin] is configured
 *
 * The NIP-11 doc is held mutably so NIP-86 change-name/description/icon RPCs
 * update what `GET /` serves at runtime. The store is NOT owned here — the
 * caller opens and closes it. With [wait] = true (the default) this blocks
 * until the server stops.
 */
fun serveRelay(
    relay: NostrRelayServer,
    port: Int,
    nip11: Nip11Info,
    limits: RelayLimits = defaultRelayLimits(),
    supportedNips: List<Int> = BASE_SUPPORTED_NIPS,
    admin: Nip86Admin? = null,
    landingPage: String? = null,
    statsPage: String? = null,
    observerStatsPage: String? = null,
    wait: Boolean = true,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    // NIP-86 advertises itself in supported_nips only when an admin is wired.
    val effectiveNips = if (admin != null) supportedNips + 86 else supportedNips
    val info = MutableRelayInfo(buildRelayInfo(nip11, limits, effectiveNips))

    return embeddedServer(Netty, port = port) {
        install(CORS) {
            // NIP-11 is consumed by browser clients and NIP-86 by browser
            // admin tools; both need CORS. anyHost is correct here — the
            // endpoints are public by design, and the admin RPC's security
            // is the NIP-98 token, not the Origin.
            anyHost()
            allowMethod(HttpMethod.Post)
            allowHeader(HttpHeaders.Authorization)
            allowHeader(HttpHeaders.ContentType)
        }
        install(WebSockets) {
            // Without a ping, a phone that walks off NAT leaves a half-open
            // socket whose session — subscriptions, fanout work, outbound
            // buffer — survives until the OS gives up, which can be never.
            // The ping makes dead peers detectable; the timeout reaps them.
            pingPeriodMillis = 30_000
            timeoutMillis = 60_000
        }
        routing {
            nostrRelay(relay)
            get("/") {
                val accept = call.request.headers["Accept"] ?: ""
                if (accept.contains("application/nostr+json")) {
                    call.respondText(info.nip11Json(), NOSTR_JSON)
                } else {
                    landingPage?.let { call.respondText(it, ContentType.Text.Html) }
                        ?: call.respondText("${nip11.name} - a NIP-50 search relay; connect a WebSocket here.")
                }
            }
            // The landing page's behavior lives in native ES modules under
            // resources/web/ — no build step, so they are served as-is. A
            // distinct /web prefix rather than a root fallback: the root is
            // already three-way overloaded (WS upgrade, NIP-11 negotiation,
            // the landing page), and a wildcard there would have to lose to
            // all of them by routing subtlety instead of by construction.
            // A short max-age: without any cache header the modules are
            // refetched on every full page load (each /npub1… deep link is
            // one), and jar-served resources carry no validators to
            // revalidate against. 60s keeps in-session navigation free while
            // bounding version skew after a deploy — index.html itself is
            // served uncached, so a stale module can outlive a new page by
            // at most a minute.
            staticResources("/web", "web") {
                cacheControl { listOf(CacheControl.MaxAge(maxAgeSeconds = 60)) }
            }
            // Any NIP-19 identifier is a page. The server validates only the
            // SHAPE and serves the landing page; decoding — checksum, TLV,
            // what the identifier names — belongs to the page, which already
            // speaks bech32. Deliberately not a catch-all: /favicon.ico and
            // typos should stay 404s, not empty search pages. Ktor prefers
            // literal routes, so /kind_stats.html and /web/… are unaffected.
            landingPage?.let { page ->
                get("/{nip19}") {
                    if (NIP19_PATH.matches(call.parameters["nip19"] ?: "")) {
                        call.respondText(page, ContentType.Text.Html)
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }
            }
            statsPage?.let { page ->
                get("/kind_stats.html") { call.respondText(page, ContentType.Text.Html) }
            }
            observerStatsPage?.let { page ->
                get("/observer_stats.html") { call.respondText(page, ContentType.Text.Html) }
            }
            admin?.let { nip86Admin(it, info) }
        }
    }.start(wait = wait)
}

/** The NIP-11 content type, parsed once — every client fetches this on connect. */
private val NOSTR_JSON = ContentType.parse("application/nostr+json")

/**
 * A path segment that is plausibly a NIP-19 identifier: the five entity
 * prefixes over the bech32 charset (which excludes 1, b, i and o). Shape
 * only — no length cap, because identifiers with relay hints legitimately
 * exceed classic bech32's 90 characters, and no checksum, because the page
 * verifies it and renders "invalid" with more context than a bare 404.
 * Case-insensitive: bech32 permits all-uppercase (QR codes emit it), and the
 * page lowercases before decoding.
 */
private val NIP19_PATH = Regex("^(npub|nprofile|note|nevent|naddr)1[02-9ac-hj-np-z]+$", RegexOption.IGNORE_CASE)

/** A mutable holder for the live NIP-11 document, updated by NIP-86 admin RPCs. */
internal class MutableRelayInfo(
    initial: Nip11RelayInformation,
) : com.vitorpamplona.quartz.nip86RelayManagement.server.Nip86Server.InfoHolder {
    @Volatile private var current: Nip11RelayInformation = initial

    // The serialized form rides along with the doc: NIP-11 is fetched by
    // every connecting client but changes only on a rare admin RPC, so
    // serializing per request would be pure waste.
    @Volatile private var currentJson: String = initial.toJson()

    fun nip11Json(): String = currentJson

    override fun get(): Nip11RelayInformation = current

    override fun set(value: Nip11RelayInformation) {
        current = value
        currentJson = value.toJson()
    }
}
