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
import kotlinx.coroutines.runBlocking

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
 *                            (how far back a stream reaches is its filter's own
 *                            `since`/`until` — absent means the whole history)
 *   ROUTER_DYNAMIC_*         defaults for `relaySource = [...]` streams, whose relay
 *                            list is read from the store's own 10002s/10040s
 *                            rather than configured (see RelaySource)
 *
 *   Trust view (see TrustProjection.reconcile):
 *   TRUST_RECONCILE_ON_START  at startup, re-derive any service whose scores are
 *                             not projected under its current observer. Needed
 *                             because the view is maintained by write triggers
 *                             that a duplicate never reaches (default true)
 *
 *   Parse audit / quartz logging (optional; see ParseAudit):
 *   QUARTZ_LOG_LEVEL         quartz's log floor: DEBUG/INFO/WARN/ERROR. Quartz
 *                            defaults to DEBUG, so malformed upstream profiles
 *                            log a line per event during a backfill
 *   PARSE_AUDIT_FILE         collect what quartz cannot parse into a grouped JSON
 *                            report here, with raw sample events; unset ⇒ off
 *   PARSE_AUDIT_SAMPLES      raw events kept per distinct failure (default 5)
 *   PARSE_AUDIT_INTERVAL_SECONDS  report rewrite interval (default 60)
 *
 *   Router resume state (optional; see SyncCursors):
 *   ROUTER_SYNC_STATE_FILE   where the per-(relay, filter) synced `created_at`
 *                            band is kept, so a relay without NIP-77 is not
 *                            re-read from scratch on every restart; unset ⇒
 *                            in memory only
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

    // The trust view is derived on WRITE, and dedup drops an event the store
    // already holds before the projection sees it — so a corpus mirrored before
    // its 10040s arrived stays unprojected, and every ranked search comes back
    // empty with nothing logged anywhere. Settling it here costs a few queries
    // when the answer is "nothing to do", which is the normal case.
    if (env["TRUST_RECONCILE_ON_START"]?.toBooleanStrictOrNull() != false) {
        runBlocking {
            runCatching { store.reconcileTrust() }
                .onSuccess { r ->
                    if (r.isClean()) {
                        println("trust: ${r.services} service(s) checked, projection consistent")
                    } else {
                        println("trust: re-derived ${r.rebuilt.size} of ${r.services} service(s) whose scores were unprojected")
                    }
                }.onFailure {
                    System.err.println("trust: reconcile failed (${it.message}); ranking stays incomplete until it runs clean")
                }
        }
    }
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
    // Opt-in diagnostic: collect the events quartz cannot fully parse, with their
    // raw JSON, so the gaps can be fixed upstream. Also the knob that quiets
    // quartz's own logging, whose floor defaults to DEBUG.
    val parseAudit = ParseAudit.installFromEnv(env)

    // Where a paged relay's already-walked history is remembered, so a restart
    // resumes instead of re-reading the corpus. Unset ⇒ in memory, which is the
    // same as not having it.
    val cursors = SyncCursors.fromEnv(env)

    val router = RouterConfigLoader.fromEnv(env)?.let { MirrorRouter(store, it, audit = parseAudit, cursors = cursors).start() }

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
            // After the router, so the final report includes the last batch.
            parseAudit?.close()
            sweeper.close()
            relay.close()
            store.close()
        },
    )

    println(
        "vespa-relay listening on :$port  (vespa $vespaUrl, relay $relayUrl)" +
            (if (admin != null) "  [NIP-86 admin: ${adminPubkeys.size} key(s)]" else "") +
            (
                if (router != null) {
                    "  [router: mirroring ${router.upstreamCount()} upstream(s)" +
                        (if (router.dynamicStreamCount() > 0) " + ${router.dynamicStreamCount()} dynamic stream(s)" else "") +
                        "]"
                } else {
                    ""
                }
            ),
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
