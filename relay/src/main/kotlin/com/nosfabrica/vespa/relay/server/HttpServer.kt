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

import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.server.config.defaultRelayLimits
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
 * [selfPubkey] is the relay's own key; [contactPubkey] the admin contact key.
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
 * The whole relay on [port]: the websocket and the NIP-11 doc on `/`, the landing page and its
 * modules, the stats pages, `/pressure`, and the NIP-86 RPC on `POST /` when [admin] is set.
 * With [wait] (the default) this blocks until the server stops.
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
    trustPage: String? = null,
    trustJson: (() -> kotlinx.serialization.json.JsonObject)? = null,
    trustExplain: (suspend (String) -> kotlinx.serialization.json.JsonObject)? = null,
    // Null, or never yet published, makes GET /stats.json a 503.
    statsJson: StatsSnapshot? = null,
    // When set, GET /pressure serves the mean read latency the sync process throttles on.
    pressure: ServingPressure? = null,
    // Asked per response: the hidden service can come up after this server did.
    onionLocation: () -> String? = { null },
    // Fires when a NIP-86 RPC rewrites the served document, not for the initial one.
    onInfoChanged: (Nip11RelayInformation) -> Unit = {},
    // The icon this relay serves itself, compared against the doc's icon to tell "no override".
    selfIconUrl: String? = null,
    wait: Boolean = true,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    val effectiveNips = if (admin != null) supportedNips + 86 else supportedNips

    // Re-themed when a NIP-86 rpc changes the icon.
    val landing = landingPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val observerStats = observerStatsPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val stats = statsPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val trust = trustPage?.let { IconedPage(it, iconOverride(nip11.icon, selfIconUrl)) }
    val pages = listOfNotNull(landing, observerStats, stats)

    // After the pages, so the change hook can repaint the markup that links the icon.
    val info =
        MutableRelayInfo(buildRelayInfo(nip11, limits, effectiveNips)) { doc ->
            pages.forEach { it.icon(iconOverride(doc.icon, selfIconUrl)) }
            onInfoChanged(doc)
        }

    return embeddedServer(Netty, port = port) {
        installPageDefaults()

        // A response hook, not a route: clients record Onion-Location off any response,
        // including the websocket handshake. A request that arrived on the .onion gets none.
        install(
            createApplicationPlugin("OnionLocation") {
                onCall { call ->
                    if (call.request.host().endsWith(".onion", ignoreCase = true)) return@onCall
                    onionLocation()?.let { call.response.header(ONION_LOCATION, it) }
                }
            },
        )

        install(WebSockets) {
            // The ping makes a peer that walked off NAT detectable; the timeout reaps its session.
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
            // Asked per request: a NIP-86 rpc can change the icon while the server runs.
            favicon { iconOverride(info.get().icon, selfIconUrl) }
            // Shape only; the page decodes the identifier. Not a catch-all, so a typo stays a 404.
            landing?.let { page ->
                get("/{nip19}") {
                    if (NIP19_PATH.matches(call.parameters["nip19"] ?: "")) {
                        call.respondPage(page.page)
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }
            }
            // Public, like the stats pages. `samples` is capped at the gate it answers,
            // so polling it cannot yield the relay's queries per second.
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
            trustHealth(trust, trustJson, trustExplain)
            admin?.let { nip86Admin(it, info) }
        }
    }.start(wait = wait)
}

/** Spelled as the Tor Project spells it; readers compare case-insensitively. */
private const val ONION_LOCATION = "Onion-Location"

private val NOSTR_JSON = ContentType.parse("application/nostr+json")

/**
 * `GET /stats.html` and `GET /stats.json`: the corpus statistics page and the document it charts.
 * Public: every field is a fact about stored events, and nothing about clients belongs in it.
 */
internal fun Route.corpusStats(
    page: IconedPage?,
    snapshot: StatsSnapshot?,
) {
    page?.let {
        get("/stats.html") { call.respondPage(it.page) }
        // The old per-kind count page is bookmarked and linked; its table moved here.
        get("/kind_stats.html") { call.respondRedirect("/stats.html", permanent = true) }
    }
    statsDocument(snapshot)
}

/**
 * `GET /trust.html`, `/trust.json` and `/trust/explain/{pubkey}`: whether ranked
 * search is working on this deployment, and if not which part of the trust
 * projection is incomplete.
 *
 * PUBLIC, unlike `/pulse.html`. The pulse is gated because it quotes what
 * people searched for; every field here is a count, a phase or a query SHAPE.
 * Keeping these numbers behind that gate is what let an incomplete projection
 * stay invisible for days while every other signal read clean.
 *
 * `explain` answers for one pubkey. A pubkey is public and this relay already
 * serves what it holds about one over NIP-01, so naming it here reveals
 * nothing the protocol does not — the answer is about the PROJECTION, not
 * about the person.
 */
internal fun Route.trustHealth(
    page: IconedPage?,
    document: (() -> kotlinx.serialization.json.JsonObject)?,
    explain: (suspend (String) -> kotlinx.serialization.json.JsonObject)?,
) {
    page?.let { get("/trust.html") { call.respondPage(it.page) } }
    document?.let { doc ->
        get("/trust.json") { call.respondText(doc().toString(), ContentType.Application.Json) }
    }
    explain?.let { ask ->
        get("/trust/explain/{pubkey}") {
            val key = call.parameters["pubkey"].orEmpty()
            call.respondText(ask(key).toString(), ContentType.Application.Json)
        }
    }
}

/**
 * A path segment shaped like a NIP-19 identifier: no length cap (relay hints exceed bech32's 90),
 * no checksum (the page verifies it), and case-insensitive because bech32 permits all-uppercase.
 */
private val NIP19_PATH = Regex("^(npub|nprofile|note|nevent|naddr)1[02-9ac-hj-np-z]+$", RegexOption.IGNORE_CASE)

/** The live NIP-11 document, rewritten by NIP-86 admin RPCs. */
internal class MutableRelayInfo(
    initial: Nip11RelayInformation,
    // Runs on the RPC's thread; a listener that publishes launches its work elsewhere.
    private val onChange: (Nip11RelayInformation) -> Unit = {},
) : com.vitorpamplona.quartz.nip86RelayManagement.server.Nip86Server.InfoHolder {
    @Volatile private var current: Nip11RelayInformation = initial

    // Serialized on change, not per request: every connecting client fetches this.
    @Volatile private var currentJson: String = initial.toJson()

    fun nip11Json(): String = currentJson

    override fun get(): Nip11RelayInformation = current

    override fun set(value: Nip11RelayInformation) {
        current = value
        currentJson = value.toJson()
        onChange(value)
    }
}
