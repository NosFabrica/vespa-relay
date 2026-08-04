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
package com.nosfabrica.vespa.relay

import com.nosfabrica.vespa.eventstore.store.VespaEventStore
import com.nosfabrica.vespa.relay.config.PubKeys
import com.nosfabrica.vespa.relay.config.RelayIdentity
import com.nosfabrica.vespa.relay.config.adminPubkeysFromEnv
import com.nosfabrica.vespa.relay.config.allowKindsFromEnv
import com.nosfabrica.vespa.relay.config.allowPubkeysFromEnv
import com.nosfabrica.vespa.relay.config.denyKindsFromEnv
import com.nosfabrica.vespa.relay.config.denyPubkeysFromEnv
import com.nosfabrica.vespa.relay.config.expirationSweepSecondsFromEnv
import com.nosfabrica.vespa.relay.config.negentropySettingsFromEnv
import com.nosfabrica.vespa.relay.config.rejectFutureSecondsFromEnv
import com.nosfabrica.vespa.relay.config.relayLimitsFromEnv
import com.nosfabrica.vespa.relay.maintenance.ExpirationSweeper
import com.nosfabrica.vespa.relay.maintenance.ParseAudit
import com.nosfabrica.vespa.relay.maintenance.deployBundledSchema
import com.nosfabrica.vespa.relay.maintenance.launchFtsReindex
import com.nosfabrica.vespa.relay.maintenance.launchOrphanScoreSweep
import com.nosfabrica.vespa.relay.maintenance.reconcileTrustWithRetry
import com.nosfabrica.vespa.relay.maintenance.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.router.SyncBands
import com.nosfabrica.vespa.relay.router.SyncEngine
import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.server.ConnectionCountListener
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.nosfabrica.vespa.relay.server.Nip86Admin
import com.nosfabrica.vespa.relay.server.NostrRelayServer
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.server.openBanStore
import com.nosfabrica.vespa.relay.server.serveRelay
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

/**
 * Run a standalone trust-ranking Nostr relay against a Vespa: open the store,
 * serve the NIP-50 relay + NIP-11 doc, optionally mirror upstream relays into
 * the same store, and block.
 *
 * Configuration is entirely from the environment. `README.md` documents every
 * variable; the essentials:
 *
 *   VESPA_URL     the Vespa query endpoint (default http://localhost:8080)
 *   RELAY_PORT    the port to listen on (default 7777)
 *   RELAY_URL     this relay's own ws url (REQUIRED)
 *   RELAY_NSEC    the relay's identity key (NIP-11 self, NIP-42, NIP-66)
 *   ROUTER_CONFIG / ROUTER_CONFIG_FILE   the mirror's streams (HOCON)
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

    // The relay's own keypair. Read first so a malformed key stops the
    // process here with a clear message, rather than surfacing hours later as
    // upstreams that mysteriously serve nothing. Unset ⇒ anonymous.
    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("relay identity: ${identity.pubKey.take(12)}… (NIP-11 self, NIP-42 auth, NIP-66 monitor)")
    }

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

    // Deploy the schema this build expects, every boot — see [deployBundledSchema].
    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    if (autoDeploy) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        deployBundledSchema(vespaUrl, configUrl)
        System.err.println("schema: deployed and serving")
    }

    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = false, configUrl = configUrl)

    // Background maintenance. Everything here runs BEHIND the server and is
    // awaited nowhere: blocking the port on any of it turns every restart
    // into an outage (the trust reconcile alone was measured at 12+ minutes).
    val maintenanceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    if (env["REINDEX_FTS_ON_START"]?.toBooleanStrictOrNull() == true) {
        launchFtsReindex(maintenanceScope, store, env["FTS_CURSOR_FILE"] ?: "/var/lib/vespa-relay/fts-cursor.txt")
    }
    env["SWEEP_ORPHAN_SCORES_ON_START"]?.trim()?.takeIf { it.isNotEmpty() }?.let { setting ->
        // Anything other than `true` is a dry run — a sweep that deletes on a
        // typo is not a sweep anyone should have to think twice about.
        launchOrphanScoreSweep(maintenanceScope, store, dryRun = setting.toBooleanStrictOrNull() != true)
    }
    if (env["TRUST_RECONCILE_ON_START"]?.toBooleanStrictOrNull() != false) {
        maintenanceScope.launch {
            println("trust: reconciling in the background — ranked search may return less until this finishes")
            val startedMs = System.currentTimeMillis()
            reconcileTrustWithRetry(store)
            println("trust: background reconcile finished in ${(System.currentTimeMillis() - startedMs) / 1000}s")
        }
    }

    // One instance, shared: the relay server measures client reads into it,
    // the router reads it back to decide whether to yield.
    val servingPressure = ServingPressure(thresholdMs = env["SERVING_PRESSURE_THRESHOLD_MS"]?.trim()?.toLongOrNull()?.coerceAtLeast(100) ?: 2_000)
    val relay =
        NostrRelayServer(
            store = store,
            servingPressure = servingPressure,
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

    // Opt-in diagnostic; also the knob that quiets quartz's own logging.
    val parseAudit = ParseAudit.installFromEnv(env)

    // Where a paged relay's already-walked history is remembered, so a
    // restart resumes instead of re-reading the corpus.
    val bands = SyncBands.fromEnv(env)

    // The router: when ROUTER_CONFIG / ROUTER_CONFIG_FILE is set, mirror the
    // configured upstreams into this same store. Unset ⇒ serve-only.
    val router =
        RouterConfigLoader.fromEnv(env)?.let {
            SyncEngine(
                store,
                it,
                audit = parseAudit,
                bands = bands,
                signer = identity,
                wireLogMode = env["ROUTER_WIRE_LOG"]?.trim()?.lowercase() ?: "",
                servingPressure = servingPressure,
            ).start()
        }

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
            // Cancelled first and NOT waited for: an unfinished reconcile
            // costs a less complete ranking until the next start, which is
            // exactly what it costs anyway.
            maintenanceScope.cancel()
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
                    "  [router: mirroring ${router.upstreamCount()} relay(s)" +
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
                contactPubkey = PubKeys.decodeOrNull(env["RELAY_CONTACT_PUBKEY"], "RELAY_CONTACT_PUBKEY"),
                // Derived, never declared: a typed-in pubkey is an assertion
                // no reader can check, while this one is provable against
                // every 22242 and 30166 the relay signs.
                selfPubkey = identity?.pubKey,
                contact = env["RELAY_CONTACT"],
                version = env["RELAY_VERSION"],
                postingPolicy = env["RELAY_POSTING_POLICY"],
                privacyPolicy = env["RELAY_PRIVACY_POLICY"],
                termsOfService = env["RELAY_TERMS_OF_SERVICE"],
            ),
        limits = limits,
        admin = admin,
        // The bundled web UI (a NIP-50 client) — served on a plain browser GET.
        landingPage = resourceText("/index.html"),
        statsPage = resourceText("/kind_stats.html"),
        observerStatsPage = resourceText("/observer_stats.html"),
    )
}

/** Map a ws/wss url to its http/https origin for NIP-98's `u` tag. */
private fun String.httpFromWs(): String =
    when {
        startsWith("wss://") -> "https://" + substring(6)
        startsWith("ws://") -> "http://" + substring(5)
        else -> this
    }

/** A bundled classpath resource, or null if it isn't there. */
private fun resourceText(path: String): String? = object {}.javaClass.getResource(path)?.readText()
