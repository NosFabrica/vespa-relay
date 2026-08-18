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
import com.nosfabrica.vespa.relay.web.IconedPage
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.favicon
import com.nosfabrica.vespa.relay.web.iconOverride
import com.nosfabrica.vespa.relay.web.installPageDefaults
import com.nosfabrica.vespa.relay.web.respondPage
import com.nosfabrica.vespa.relay.web.statsDocument
import com.nosfabrica.vespa.relay.web.webModules
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.request.host
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
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
 *   GET  /favicon.ico -> the tab icon, for anything that never read our markup
 *   GET  /npub1…, /nprofile1…, /note1…, /nevent1…, /naddr1… -> [landingPage],
 *        which decodes the identifier and renders the entity itself
 *   GET  /observer_stats.html -> [observerStatsPage]
 *   GET  /stats.html -> [statsPage], the view over [statsJson]
 *   GET  /kind_stats.html -> 301 to /stats.html, whose Kinds table replaced it
 *   GET  /stats.json -> [statsJson], this relay's corpus statistics
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
    observerStatsPage: String? = null,
    statsPage: String? = null,
    // The corpus statistics document, recomputed behind the server by
    // StatsRollup. Null — or present but never yet published — makes
    // GET /stats.json a 503 rather than a page of zeros.
    statsJson: StatsSnapshot? = null,
    // When set, GET /pressure serves the relay's mean client-read latency, so
    // the sync process — its own container since the split — can keep yielding
    // ingest to slow reads the way it did when both shared a JVM.
    pressure: ServingPressure? = null,
    // The `http://…onion/` this relay also answers at, advertised on every
    // clearnet response. Asked per response rather than captured once: the
    // hidden service can come up after this server did.
    onionLocation: () -> String? = { null },
    // Called whenever a NIP-86 admin RPC rewrites the served document
    // (`changerelayname`, `changerelaydescription`, `changerelayicon`), so
    // anything DERIVED from it can follow — today the relay's own kind 0, which
    // would otherwise keep saying what the environment said at boot while the
    // doc says something else. Not called for the initial document: that one is
    // this call's own argument.
    onInfoChanged: (Nip11RelayInformation) -> Unit = {},
    // The url of the icon THIS relay serves at /favicon.ico — `selfIconUrl` of
    // RELAY_URL, computed by the caller because only it knows that address.
    // Passed in to be compared against, not to be published: [nip11] already
    // carries it as the `icon` when the operator set none, and the doc's icon
    // being ours is precisely what must NOT make /favicon.ico redirect.
    selfIconUrl: String? = null,
    wait: Boolean = true,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    // NIP-86 advertises itself in supported_nips only when an admin is wired.
    val effectiveNips = if (admin != null) supportedNips + 86 else supportedNips

    // Hashed once at boot rather than per request: these strings are read off
    // the classpath at startup, and re-themed only when the icon changes under
    // a NIP-86 rpc — see IconedPage.
    val landing = landingPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val observerStats = observerStatsPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val stats = statsPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val pages = listOfNotNull(landing, observerStats, stats)

    // Built after the pages so the change hook can reach them: an admin rpc that
    // rewrites the icon has to repaint the markup that links it, or the tab
    // keeps the old icon until someone restarts the relay — the same staleness
    // `onInfoChanged` exists to keep out of the relay's kind 0.
    val info =
        MutableRelayInfo(buildRelayInfo(nip11, limits, effectiveNips)) { doc ->
            pages.forEach { it.icon(iconOverride(doc.icon, selfIconUrl)) }
            onInfoChanged(doc)
        }

    return embeddedServer(Netty, port = port) {
        installPageDefaults()

        // Onion-Location: the standard way a clearnet service says "I am also
        // this hidden service". Tor Browser turns it into the ".onion
        // available" button; Amethyst records it from ANY response — the
        // NIP-11 fetch, the websocket handshake, a media request — and, when
        // the user has Tor on, dials the .onion instead, so the connection
        // never crosses an exit node. That is why this is a response hook and
        // not a route: the clients that most need it may only ever open the
        // websocket, and a header on `GET /` alone would never reach them.
        //
        // The header only makes sense pointing INTO the network from outside
        // it, so a request that already arrived on the .onion does not get one
        // — the alternative is caching a host as its own alternative.
        install(
            createApplicationPlugin("OnionLocation") {
                onCall { call ->
                    if (call.request.host().endsWith(".onion", ignoreCase = true)) return@onCall
                    onionLocation()?.let { call.response.header(ONION_LOCATION, it) }
                }
            },
        )

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
                    landing?.let { call.respondPage(it.page) }
                        ?: call.respondText("${nip11.name} - a NIP-50 search relay; connect a WebSocket here.")
                }
            }
            webModules()
            // The doc's icon, unless the doc's icon is the one we serve — see
            // iconOverride. Asked per request rather than captured: a NIP-86
            // rpc can change it while the server runs.
            favicon { iconOverride(info.get().icon, selfIconUrl) }
            // Any NIP-19 identifier is a page. The server validates only the
            // SHAPE and serves the landing page; decoding — checksum, TLV,
            // what the identifier names — belongs to the page, which already
            // speaks bech32. Deliberately not a catch-all: a typo stays a 404
            // rather than becoming an empty search page. Ktor prefers literal
            // routes, so /stats.html, /favicon.ico and /web/… are unaffected.
            landing?.let { page ->
                get("/{nip19}") {
                    if (NIP19_PATH.matches(call.parameters["nip19"] ?: "")) {
                        call.respondPage(page.page)
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }
            }
            // What the sync process polls to throttle its ingest. Public on
            // purpose, like the stats pages: a mean read latency names no
            // client and no query, and gating it would cost the mirror its
            // feed the moment an auth config drifts. `samples` is capped at
            // the MIN_SAMPLES gate it exists to answer — uncapped it is a
            // lifetime query counter, and polling it twice would hand anyone
            // the relay's queries-per-second, which the throttle never needed.
            pressure?.let { p ->
                get("/pressure") {
                    val samples = p.sampleCount().coerceAtMost(ServingPressure.MIN_SAMPLES.toLong())
                    call.respondText(
                        """{"meanMs":${p.meanMs()},"samples":$samples}""",
                        ContentType.Application.Json,
                    )
                }
            }
            observerStats?.let { page ->
                get("/observer_stats.html") { call.respondPage(page.page) }
            }
            corpusStats(stats, statsJson)
            admin?.let { nip86Admin(it, info) }
        }
    }.start(wait = wait)
}

/**
 * The header a clearnet service uses to name its hidden service. Spelled as
 * the Tor Project spells it; HTTP header names are case-insensitive, and
 * Amethyst's `response.header("Onion-Location")` is too.
 */
private const val ONION_LOCATION = "Onion-Location"

/** The NIP-11 content type, parsed once — every client fetches this on connect. */
private val NOSTR_JSON = ContentType.parse("application/nostr+json")

/**
 * `GET /stats.html` and `GET /stats.json` — the corpus statistics page and the
 * document it charts.
 *
 * One name, two representations: the `.json` is the artifact and the `.html` is
 * a view over it, and the shared stem is what says so — a reader who finds one
 * can guess the other. No `<subject>_` prefix because there is no subject to
 * name: the sibling `observer_stats.html` earns its prefix by being about
 * observers specifically, while this page is about the relay, which on a relay
 * is everything.
 *
 * The document is PUBLIC, like `/pressure` and the two other stats pages. Every
 * number in it describes STORED EVENTS, which is what a relay already hands to
 * anyone who asks for them over a REQ, and publishing it is most of the point:
 * a reader charting our coverage against a network-wide dashboard should not
 * have to scrape this page's markup for numbers the relay already computed.
 *
 * The line worth holding is the one `/pressure` holds. That route caps its
 * `samples` field precisely because an uncapped one would have handed anyone the
 * relay's queries-per-second, which its throttle never needed — so: nothing
 * about CLIENTS goes in this document. Everything currently in it is a fact
 * about the corpus, and a statistics endpoint is exactly where a field like
 * "searches per hour" gets added later without anyone noticing.
 *
 * Its own function so a test can mount it without standing up a relay — the
 * same reason [webModules] is one.
 */
internal fun Route.corpusStats(
    page: IconedPage?,
    snapshot: StatsSnapshot?,
) {
    page?.let {
        get("/stats.html") { call.respondPage(it.page) }
        // `/kind_stats.html` was the per-kind COUNT page this one's Kinds table
        // replaced. A 301 rather than a 404 because the old url is what is
        // bookmarked, linked from operator runbooks, and printed in this repo's
        // own history — and because the answer genuinely moved rather than
        // going away: the new table covers EVERY kind, where that page could
        // only count the ones it already knew to name.
        get("/kind_stats.html") { call.respondRedirect("/stats.html", permanent = true) }
    }
    statsDocument(snapshot)
}

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
    // Fired on every admin rewrite, so what the relay says about itself
    // elsewhere — its own kind 0 — tracks the document rather than the boot
    // environment. Runs on the RPC's thread; the one listener launches its work
    // into a background scope rather than publishing inline.
    private val onChange: (Nip11RelayInformation) -> Unit = {},
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
        onChange(value)
    }
}
