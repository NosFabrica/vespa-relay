/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener

/**
 * Run a standalone trust-ranking Nostr relay against a Vespa. It opens the
 * store, serves the NIP-50 relay + NIP-11 doc, and blocks. When a router config
 * is set it also mirrors events from upstream relays into that store (see the
 * router env vars below); large-scale crawling/trust-sync remains a separate
 * concern, for which SoT (github.com/vitorpamplona/sot) is the reference app.
 *
 * Configuration is entirely from the environment:
 *
 *   VESPA_URL           the Vespa query endpoint          (default http://localhost:8080)
 *   RELAY_PORT          the port to listen on             (default 7777)
 *   RELAY_URL           this relay's own ws url — its NIP-42 identity and NIP-62
 *                       vanish scope                      (REQUIRED)
 *   DEFAULT_OBSERVER    64-hex pubkey whose web of trust ranks anonymous searches;
 *                       unset ⇒ anonymous searches are untrusted
 *   AUTO_DEPLOY         deploy the bundled schema on first run (default true)
 *
 *   NIP-11 identity:
 *   RELAY_NAME / RELAY_DESCRIPTION / RELAY_ICON / RELAY_BANNER /
 *   RELAY_CONTACT_PUBKEY / RELAY_SELF_PUBKEY / RELAY_CONTACT (human contact) /
 *   RELAY_VERSION (override the build version) / RELAY_POSTING_POLICY /
 *   RELAY_PRIVACY_POLICY / RELAY_TERMS_OF_SERVICE
 *
 *   Protection limits (each optional; sane defaults otherwise, see RelayConfig):
 *   MAX_MESSAGE_LENGTH / MAX_SUBSCRIPTIONS / MAX_FILTERS / MAX_LIMIT /
 *   DEFAULT_LIMIT / MAX_SUBID_LENGTH / MAX_EVENT_TAGS / MAX_CONTENT_LENGTH /
 *   MIN_POW_DIFFICULTY / CREATED_AT_LOWER_LIMIT / CREATED_AT_UPPER_LIMIT
 *
 *   NIP-77 negentropy tuning (optional; strfry-parity defaults):
 *   NEG_FRAME_SIZE_LIMIT / NEG_MAX_SYNC_EVENTS / NEG_MAX_SESSIONS_PER_CONNECTION
 *
 *   NIP-86 relay management (optional):
 *   RELAY_ADMIN_PUBKEYS   comma/space-separated 64-hex admin keys; empty ⇒ off
 *   RELAY_HTTP_URL        the http(s) url NIP-98 auth must be tagged with
 *                         (default: RELAY_URL with ws→http, wss→https)
 *
 *   Router / upstream mirror (optional; see RouterConfig / MirrorRouter):
 *   ROUTER_CONFIG            strfry-style `streams { }` config (HOCON), inline
 *   ROUTER_CONFIG_FILE       path to a file holding that config
 *   ROUTER_BACKFILL_SECONDS  default history window streams negentropy-backfill
 *                            before their live tail (default 0 ⇒ live-only)
 */
fun main() {
    val env = System.getenv()
    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val port = env["RELAY_PORT"]?.toIntOrNull() ?: 7777
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — this relay's own ws url (NIP-42 identity / NIP-62 vanish scope).")
    val relayUrl =
        RelayUrlNormalizer.normalizeOrNull(relayUrlRaw)
            ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")
    val autoDeploy = env["AUTO_DEPLOY"]?.toBooleanStrictOrNull() ?: true

    val limits = relayLimitsFromEnv(env)
    val negentropy = negentropySettingsFromEnv(env)
    val rejectFutureSeconds = rejectFutureSecondsFromEnv(env)

    // NIP-86 is enabled only when at least one valid admin key is configured;
    // its ban lists persist to RELAY_STATE_FILE when set.
    val adminPubkeys = adminPubkeysFromEnv(env)
    val banStore = if (adminPubkeys.isNotEmpty()) openBanStore(env["RELAY_STATE_FILE"]) else null

    val listener =
        if (env["LOG_CONNECTIONS"]?.toBooleanStrictOrNull() == true) {
            ConnectionCountListener()
        } else {
            RelayServerListener.None
        }

    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = autoDeploy)
    val relay =
        NostrRelayServer(
            store = store,
            defaultObserver = env["DEFAULT_OBSERVER"],
            relayUrl = relayUrl,
            listener = listener,
            limits = limits,
            negentropySettings = negentropy,
            banStore = banStore,
            pubkeyAllow = allowPubkeysFromEnv(env),
            pubkeyDeny = denyPubkeysFromEnv(env),
            kindAllow = allowKindsFromEnv(env),
            kindDeny = denyKindsFromEnv(env),
            rejectFutureSeconds = rejectFutureSeconds,
        )

    // Prune NIP-40 expired events on a schedule (the store schedules nothing itself).
    val sweeper = ExpirationSweeper(store, expirationSweepSecondsFromEnv(env)).start()

    // The router (strfry-style mirror): when ROUTER_CONFIG / ROUTER_CONFIG_FILE is
    // set, keep live subscriptions open against the configured upstream relays and
    // mirror their events of the configured kinds into this same store. Fills what
    // the search serves, from the network. Unset ⇒ serve-only.
    val router = RouterConfigLoader.fromEnv(env)?.let { MirrorRouter(store, it).start() }

    val admin =
        banStore?.let {
            Nip86Admin(
                banStore = it,
                adminPubkeys = adminPubkeys,
                relayHttpUrl = env["RELAY_HTTP_URL"] ?: relayUrlRaw.httpFromWs(),
                // Banning a source also drops what it already published.
                purge = { filter -> store.delete(filter) },
            )
        }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Stop mirroring into the store before the relay and store close.
            router?.close()
            sweeper.close()
            relay.close()
            store.close()
        },
    )

    println(
        "vespa-relay listening on :$port  (vespa $vespaUrl, relay $relayUrl)" +
            (if (admin != null) "  [NIP-86 admin: ${adminPubkeys.size} key(s)]" else "") +
            (if (router != null) "  [router: mirroring ${router.upstreamCount()} upstream(s)]" else ""),
    )
    serveRelay(
        relay = relay,
        port = port,
        nip11 =
            Nip11Info(
                name = env["RELAY_NAME"] ?: "vespa-relay",
                description = env["RELAY_DESCRIPTION"],
                icon = env["RELAY_ICON"],
                banner = env["RELAY_BANNER"],
                contactPubkey = env["RELAY_CONTACT_PUBKEY"],
                selfPubkey = env["RELAY_SELF_PUBKEY"],
                contact = env["RELAY_CONTACT"],
                version = env["RELAY_VERSION"],
                postingPolicy = env["RELAY_POSTING_POLICY"],
                privacyPolicy = env["RELAY_PRIVACY_POLICY"],
                termsOfService = env["RELAY_TERMS_OF_SERVICE"],
            ),
        limits = limits,
        admin = admin,
        // The bundled web UI (a NIP-50 client) — served on a plain browser GET.
        landingPage = webUi(),
    )
}

/** Map a ws/wss url to its http/https origin for NIP-98's `u` tag. */
private fun String.httpFromWs(): String =
    when {
        startsWith("wss://") -> "https://" + substring(6)
        startsWith("ws://") -> "http://" + substring(5)
        else -> this
    }

/** The bundled search UI (`resources/index.html`), or null if it isn't on the classpath. */
private fun webUi(): String? = object {}.javaClass.getResource("/index.html")?.readText()
