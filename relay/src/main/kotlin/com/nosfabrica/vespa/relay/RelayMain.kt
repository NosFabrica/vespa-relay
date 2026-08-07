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

import com.nosfabrica.vespa.eventstore.VespaEventStore
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
import com.nosfabrica.vespa.relay.config.relayAddressesFromEnv
import com.nosfabrica.vespa.relay.config.relayLimitsFromEnv
import com.nosfabrica.vespa.relay.maintenance.ExpirationSweeper
import com.nosfabrica.vespa.relay.maintenance.StatsRollup
import com.nosfabrica.vespa.relay.maintenance.StatsVespa
import com.nosfabrica.vespa.relay.maintenance.applyQuartzLogLevel
import com.nosfabrica.vespa.relay.maintenance.deployBundledSchema
import com.nosfabrica.vespa.relay.maintenance.launchFtsReindex
import com.nosfabrica.vespa.relay.maintenance.launchOrphanScoreSweep
import com.nosfabrica.vespa.relay.maintenance.launchStatsRollup
import com.nosfabrica.vespa.relay.maintenance.reconcileTrustWithRetry
import com.nosfabrica.vespa.relay.maintenance.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.server.ConnectionCountListener
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.nosfabrica.vespa.relay.server.Nip86Admin
import com.nosfabrica.vespa.relay.server.NostrRelayServer
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.server.StatsSnapshot
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
 * serve the NIP-50 relay + NIP-11 doc, and block. Mirroring upstream relays
 * into the same store is `:sync`'s job, as its own process — see SyncMain.
 *
 * Configuration is entirely from the environment. `docs/configuration.md`
 * documents every variable; the essentials:
 *
 *   VESPA_URL     the Vespa query endpoint (default http://localhost:8080)
 *   RELAY_PORT    the port to listen on (default 7777)
 *   RELAY_URL     this relay's own ws url (REQUIRED)
 *   RELAY_NSEC    the relay's identity key (NIP-11 self, NIP-42, NIP-66)
 *   RELAY_ONION_* the .onion this relay also answers at, for NIP-42
 */
fun main() {
    val env = System.getenv()

    // The router moved to its own process; a stream config aimed here would
    // once have started the mirror and now starts nothing. Refusing to boot is
    // the migration notice — a configured component must never be silently
    // inert, and "the mirror stopped mirroring" is the worst spelling of it.
    listOf("SYNC_CONFIG", "SYNC_CONFIG_FILE", "ROUTER_CONFIG", "ROUTER_CONFIG_FILE")
        .firstOrNull { !env[it].isNullOrBlank() }
        ?.let {
            error(
                "$it is set, but the relay no longer runs the sync engine — it moved to its own process " +
                    "(the vespa-sync binary / the `sync` service in docker-compose.yml, enabled with " +
                    "`docker compose --profile sync up`). Move the SYNC_* settings there, or unset $it " +
                    "to serve without mirroring.",
            )
        }

    // The rest of the family that moved with the mirror. Not fatal — none of
    // these starts a subsystem, so nothing is half-running — but a setting
    // read by nobody must not pass in silence: PARSE_AUDIT_FILE left here
    // would look exactly like an audit that found nothing.
    env.keys
        .filter { key ->
            (
                key.startsWith("SYNC_") || key.startsWith("ROUTER_") ||
                    key.startsWith("PARSE_AUDIT_") || key == "SERVING_PRESSURE_THRESHOLD_MS"
            ) &&
                !env[key].isNullOrBlank()
        }.sorted()
        .takeIf { it.isNotEmpty() }
        ?.let {
            System.err.println(
                "relay: ${it.joinToString()} — read by the sync process, not the relay; set them on that service or they do nothing",
            )
        }
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

    // A hidden service in front of this same port answers at a second address,
    // and a Tor client signs the one it dialled. Read here so a malformed
    // RELAY_ONION_URL stops the boot rather than costing Tor clients their
    // ranking lens quietly. The eager alternates() call is for the log: an
    // address Tor has already published is named at boot, and one that
    // appears later is named by the connection that first finds it.
    val addresses = relayAddressesFromEnv(env).also { it.alternates() }

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
    // The corpus statistics behind GET /stats.json and /relay_stats.html.
    // Seeded from the state file first, so a restart serves the last document
    // instead of a blank page for however long the first rollup takes.
    val statsSnapshot = StatsSnapshot(env["STATS_FILE"] ?: "/var/lib/vespa-relay/stats.json").also { it.loadFromFile() }
    val statsIntervalSeconds = env["STATS_INTERVAL_SECONDS"]?.toLongOrNull() ?: 900L
    if (statsIntervalSeconds > 0) {
        launchStatsRollup(
            scope = maintenanceScope,
            // The NORMALIZED url, the same string NIP-42 and NIP-62 key off —
            // so a document fetched from two of this relay's addresses names
            // one relay rather than reading as two.
            rollup = StatsRollup(StatsVespa(vespaUrl), relayUrl = relayUrl.url),
            snapshot = statsSnapshot,
            everySeconds = statsIntervalSeconds,
        )
    } else {
        // A zero/negative interval is "don't compute", which is a legitimate
        // choice on a busy box — the grouping competes with client reads. Say
        // so, because the alternative is an operator watching a page that never
        // fills and looking for the bug in the rollup.
        println("stats: STATS_INTERVAL_SECONDS=$statsIntervalSeconds — no rollup; /stats.json serves the state file, or 503 if there is none")
    }
    if (env["TRUST_RECONCILE_ON_START"]?.toBooleanStrictOrNull() != false) {
        maintenanceScope.launch {
            println("trust: reconciling in the background — ranked search may return less until this finishes")
            val startedMs = System.currentTimeMillis()
            reconcileTrustWithRetry(store)
            println("trust: background reconcile finished in ${(System.currentTimeMillis() - startedMs) / 1000}s")
        }
    }

    // The relay server measures client reads into it; the sync process polls
    // the mean over GET /pressure to decide whether its ingest should yield.
    // No threshold here on purpose: this side only records and serves — the
    // threshold belongs to the process that yields on it, and reading
    // SERVING_PRESSURE_THRESHOLD_MS into an instance whose backoffMs() nobody
    // calls would be a setting that is accepted and does nothing.
    val servingPressure = ServingPressure()
    val relay =
        NostrRelayServer(
            store = store,
            servingPressure = servingPressure,
            relayUrl = relayUrl,
            alsoServedAt = addresses::alternates,
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

    // The knob that quiets quartz's own logging. The parse audit itself lives
    // in the sync process — ingest is what feeds it, and nothing here does.
    applyQuartzLogLevel(env)

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
            sweeper.close()
            relay.close()
            store.close()
        },
    )

    println(
        "vespa-relay listening on :$port  (vespa $vespaUrl, relay $relayUrl)" +
            (if (admin != null) "  [NIP-86 admin: ${adminPubkeys.size} key(s)]" else ""),
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
        pressure = servingPressure,
        // "This relay is also a hidden service" — read by Tor Browser and by
        // clients that move the connection inside the network when Tor is on.
        onionLocation = addresses::onionLocation,
        // The bundled web UI (a NIP-50 client) — served on a plain browser GET.
        landingPage = resourceText("/index.html"),
        observerStatsPage = resourceText("/observer_stats.html"),
        relayStatsPage = resourceText("/relay_stats.html"),
        statsJson = statsSnapshot,
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
