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
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.defaultForFilePath
import io.ktor.http.withCharset
import io.ktor.server.application.ApplicationCall
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
import io.ktor.server.request.host
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

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
    wait: Boolean = true,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    // NIP-86 advertises itself in supported_nips only when an admin is wired.
    val effectiveNips = if (admin != null) supportedNips + 86 else supportedNips
    val info = MutableRelayInfo(buildRelayInfo(nip11, limits, effectiveNips), onInfoChanged)
    // Hashed once at boot rather than per request: these strings are read off
    // the classpath at startup and never change while the process lives.
    val landing = landingPage?.let(::CachedPage)
    val observerStats = observerStatsPage?.let(::CachedPage)
    val stats = statsPage?.let(::CachedPage)

    return embeddedServer(Netty, port = port) {
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
                    landing?.let { call.respondPage(it) }
                        ?: call.respondText("${nip11.name} - a NIP-50 search relay; connect a WebSocket here.")
                }
            }
            webModules()
            favicon()
            // Any NIP-19 identifier is a page. The server validates only the
            // SHAPE and serves the landing page; decoding — checksum, TLV,
            // what the identifier names — belongs to the page, which already
            // speaks bech32. Deliberately not a catch-all: a typo stays a 404
            // rather than becoming an empty search page. Ktor prefers literal
            // routes, so /stats.html, /favicon.ico and /web/… are unaffected.
            landing?.let { page ->
                get("/{nip19}") {
                    if (NIP19_PATH.matches(call.parameters["nip19"] ?: "")) {
                        call.respondPage(page)
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
                get("/observer_stats.html") { call.respondPage(page) }
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
 * `GET /web/…` — the landing page's native ES modules, straight off the
 * classpath, no build step.
 *
 * A distinct /web prefix rather than a root fallback: the root is already
 * three-way overloaded (WS upgrade, NIP-11 negotiation, the landing page), and
 * a wildcard there would have to lose to all of them by routing subtlety
 * instead of by construction.
 *
 * Served from [WebAssets] rather than Ktor's `staticResources` for the
 * validator. A short max-age is the freshness bound the page wants — index.html
 * is revalidated every time, so a stale module can outlive a new page by at most
 * a minute — but on its own it means every load past that minute re-downloads
 * all 23 modules in full, and a deep link IS a full load. The classpath carries
 * no validators to revalidate against, so this mints one from the content: a
 * returning reader gets 23 empty 304s instead of ~40KB. The read and the hash
 * happen once per module for the life of the process; before this, every request
 * re-opened the classpath entry and re-gzipped it.
 *
 * Its own function so a test can mount it alone — the module directory is the
 * whole page, and a broken route here is a blank site.
 */
internal fun Route.webModules() {
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
internal fun Route.favicon() {
    get("/favicon.ico") {
        // Absent only if someone deleted the resource — a 404 is then the
        // honest answer, and it is the answer this route replaced.
        WebAssets.get("favicon.ico")?.let { call.respondAsset(it) } ?: call.respond(HttpStatusCode.NotFound)
    }
}

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
    page: CachedPage?,
    snapshot: StatsSnapshot?,
) {
    page?.let {
        get("/stats.html") { call.respondPage(it) }
        // `/kind_stats.html` was the per-kind COUNT page this one's Kinds table
        // replaced. A 301 rather than a 404 because the old url is what is
        // bookmarked, linked from operator runbooks, and printed in this repo's
        // own history — and because the answer genuinely moved rather than
        // going away: the new table covers EVERY kind, where that page could
        // only count the ones it already knew to name.
        get("/kind_stats.html") { call.respondRedirect("/stats.html", permanent = true) }
    }
    snapshot?.let {
        get("/stats.json") {
            val doc = it.served()
            if (doc == null) {
                // 503, not an empty document: "no statistics yet" is a state a
                // poller should retry, and a 200 carrying zeros is
                // indistinguishable from a relay that genuinely holds nothing.
                call.respondText(
                    """{"error":"no statistics computed yet"}""",
                    ContentType.Application.Json,
                    HttpStatusCode.ServiceUnavailable,
                )
            } else {
                call.respondSnapshot(doc)
            }
        }
    }
}

/**
 * A strong ETag over [bytes] — the first 16 hex of its SHA-256.
 *
 * Content-derived rather than a timestamp on purpose: a jar entry's mtime is
 * the build's, not the file's, so two deploys of an unchanged module would
 * still miss. A hash makes "unchanged" mean unchanged.
 */
private fun etagOf(bytes: ByteArray): String =
    MessageDigest
        .getInstance("SHA-256")
        .digest(bytes)
        .take(8)
        .joinToString("") { "%02x".format(it) }
        .let { "\"$it\"" }

/** One of the three HTML pages, with the validator that saves re-sending it. */
internal class CachedPage(
    val html: String,
) {
    val etag: String = etagOf(html.toByteArray(Charsets.UTF_8))
}

/**
 * The landing/stats pages, revalidated every time but re-sent only when they
 * changed.
 *
 * `no-cache` is NOT "do not store": it means the browser must ask before
 * reusing, which is exactly the property these pages need — a deploy is picked
 * up on the next load, and the modules under /web can never outlive their page
 * by more than their own max-age. What it adds is the 304: reloading a page
 * that has not changed since the last deploy costs a header exchange instead of
 * 25KB of markup and inline CSS re-gzipped from scratch.
 */
private suspend fun ApplicationCall.respondPage(page: CachedPage) {
    response.header(HttpHeaders.ETag, page.etag)
    response.header(HttpHeaders.CacheControl, "no-cache")
    if (matchesEtag(page.etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondText(page.html, ContentType.Text.Html)
    }
}

/**
 * The statistics document, revalidated every time and re-sent only when the
 * rollup actually produced something new.
 *
 * The 304 is the point rather than a nicety: the page polls this on a timer and
 * the rollup is far slower than the poll, so most fetches are for bytes the
 * reader already has. `no-cache` (revalidate, don't reuse blind) rather than a
 * `max-age` guess, because the rollup interval is an operator setting and a
 * cache lifetime picked here would be wrong for anyone who changed it.
 */
private suspend fun ApplicationCall.respondSnapshot(doc: StatsSnapshot.Served) {
    response.header(HttpHeaders.ETag, doc.etag)
    response.header(HttpHeaders.CacheControl, "no-cache")
    if (matchesEtag(doc.etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondBytes(doc.bytes, ContentType.Application.Json)
    }
}

/** The same exchange for a /web module, which additionally may be reused for a minute. */
private suspend fun ApplicationCall.respondAsset(asset: WebAssets.Asset) {
    response.header(HttpHeaders.ETag, asset.etag)
    response.header(HttpHeaders.CacheControl, "max-age=60")
    if (matchesEtag(asset.etag)) {
        respond(HttpStatusCode.NotModified)
    } else {
        respondBytes(asset.bytes, asset.contentType)
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
private fun ApplicationCall.matchesEtag(etag: String): Boolean =
    request.headers[HttpHeaders.IfNoneMatch]
        ?.split(',')
        ?.any { it.trim().removePrefix("W/") == etag } == true

/**
 * The page's ES modules, read off the classpath once each and held.
 *
 * Lazily, not enumerated at boot: the resources live inside the jar in a
 * deployment and inside a build directory in a test, and walking either is
 * more machinery than a map that fills itself on first use. The safety comes
 * from the path check rather than from the enumeration — [WEB_PATH] admits only
 * plain segment names, and `..` is rejected outright, so a request can only
 * ever name a file under `web/`.
 */
internal object WebAssets {
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
