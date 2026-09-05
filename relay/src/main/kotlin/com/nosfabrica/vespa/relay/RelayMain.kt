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
import com.nosfabrica.vespa.relay.config.requireReadLensFromEnv
import com.nosfabrica.vespa.relay.config.searchConcurrencyPerConnectionFromEnv
import com.nosfabrica.vespa.relay.config.searchExpansionFromEnv
import com.nosfabrica.vespa.relay.maintenance.ExpirationSweeper
import com.nosfabrica.vespa.relay.maintenance.RelayProfile
import com.nosfabrica.vespa.relay.maintenance.STORE_WRITERS
import com.nosfabrica.vespa.relay.maintenance.StatsRollup
import com.nosfabrica.vespa.relay.maintenance.StatsTier
import com.nosfabrica.vespa.relay.maintenance.StatsVespa
import com.nosfabrica.vespa.relay.maintenance.applyQuartzLogLevel
import com.nosfabrica.vespa.relay.maintenance.deployBundledSchema
import com.nosfabrica.vespa.relay.maintenance.launchFtsReindex
import com.nosfabrica.vespa.relay.maintenance.launchOrphanScoreSweep
import com.nosfabrica.vespa.relay.maintenance.launchRelayProfile
import com.nosfabrica.vespa.relay.maintenance.launchStatsRollup
import com.nosfabrica.vespa.relay.maintenance.reconcileTrustWithRetry
import com.nosfabrica.vespa.relay.maintenance.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.pulse.PulseDocument
import com.nosfabrica.vespa.relay.pulse.pulseSlowReadMs
import com.nosfabrica.vespa.relay.server.ConnectionCountListener
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.nosfabrica.vespa.relay.server.Nip86Admin
import com.nosfabrica.vespa.relay.server.NostrRelayServer
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.server.TrustNotice
import com.nosfabrica.vespa.relay.server.openBanStore
import com.nosfabrica.vespa.relay.server.selfIconUrl
import com.nosfabrica.vespa.relay.server.serveRelay
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.servePulseSite
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.io.File

/**
 * The relay process: open the store, serve the NIP-50 relay and the NIP-11
 * doc, and block. Mirroring upstreams into the same store is the sync
 * process's job (SyncMain).
 *
 * Configured from the environment; `docs/configuration.md` lists every
 * variable. `RELAY_URL` is required.
 */
fun main() {
    val env = System.getenv()

    // A sync config aimed at this process is a configured component that would run nothing.
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

    // The rest of the sync family starts no subsystem here, so a warning is enough.
    env.keys
        .filter { key ->
            (
                key.startsWith("SYNC_") || key.startsWith("ROUTER_") ||
                    key.startsWith("PARSE_AUDIT_") || key == "SERVING_PRESSURE_THRESHOLD_MS"
            ) &&
                // The stats rollup reads these off the shared volume; the name is the same on both services.
                key !in SYNC_FILES_THE_RELAY_READS &&
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

    // Read first so a malformed key stops the boot here. Unset is an anonymous relay.
    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("relay identity: ${identity.pubKey.take(12)}… (NIP-11 self, NIP-42 auth, NIP-66 monitor)")
    }

    // The eager alternates() names an already published .onion in the boot log.
    val addresses = relayAddressesFromEnv(env).also { it.alternates() }

    val limits = relayLimitsFromEnv(env)
    val negentropy = negentropySettingsFromEnv(env)
    val rejectFutureSeconds = rejectFutureSecondsFromEnv(env)
    // Announced only when off: that is the line worth finding when a client gets more than it expected.
    val requireReadLens = requireReadLensFromEnv(env)
    if (!requireReadLens) {
        System.err.println("relay: REQUIRE_READ_LENS=false — anonymous reads are answered unranked, over the whole corpus")
    }

    // Announced only when off or capped at nothing: fewer events than promised looks like an empty corpus.
    val searchExpansion = searchExpansionFromEnv(env)
    if (!searchExpansion.enabled) {
        System.err.println("relay: SEARCH_EXPAND_REFERENCES=false — a search answers with its own hits only")
    } else if (searchExpansion.maxPerEvent == 0 || searchExpansion.maxPerRequest == 0) {
        System.err.println("relay: search reference expansion is on but capped at 0 — it will add nothing")
    }

    // From RELAY_HTTP_URL when set: a relay behind a proxy on another name would otherwise
    // advertise an icon at the websocket's host. See [selfIconUrl].
    val ownIconUrl = selfIconUrl(env["RELAY_HTTP_URL"] ?: relayUrl.url)

    // One description of this relay: served as NIP-11 and published as its kind 0.
    val nip11 =
        Nip11Info(
            name = env["RELAY_NAME"] ?: "vespa-relay",
            description = env["RELAY_DESCRIPTION"],
            // Unset publishes the icon this relay already serves on its own tab.
            icon = env["RELAY_ICON"] ?: ownIconUrl,
            banner = env["RELAY_BANNER"],
            contactPubkey = PubKeys.decodeOrNull(env["RELAY_CONTACT_PUBKEY"], "RELAY_CONTACT_PUBKEY"),
            // Derived from the key, never declared: provable against every event the relay signs.
            selfPubkey = identity?.pubKey,
            contact = env["RELAY_CONTACT"],
            version = env["RELAY_VERSION"],
            postingPolicy = env["RELAY_POSTING_POLICY"],
            privacyPolicy = env["RELAY_PRIVACY_POLICY"],
            termsOfService = env["RELAY_TERMS_OF_SERVICE"],
        )

    // NIP-86 exists only with at least one valid admin key.
    val adminPubkeys = adminPubkeysFromEnv(env)
    val banStore = if (adminPubkeys.isNotEmpty()) openBanStore(env["RELAY_STATE_FILE"]) else null

    val listener =
        if (env["LOG_CONNECTIONS"]?.toBooleanStrictOrNull() == true) {
            ConnectionCountListener()
        } else {
            RelayServerListener.None
        }

    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    if (autoDeploy) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        deployBundledSchema(vespaUrl, configUrl)
        System.err.println("schema: deployed and serving")
    }

    // Read before the store is opened: the slow-read ring is a constructor
    // setting, and it is the one place the store retains a query string.
    val pulsePort = env["PULSE_PORT"]?.trim()?.toIntOrNull() ?: 0
    val pulseClientDetail = env["PULSE_CLIENT_DETAIL"]?.trim()?.toBooleanStrictOrNull() ?: false
    val slowReadMs = pulseSlowReadMs(env, "PULSE_SLOW_READ_MS", pulseClientDetail, "PULSE_CLIENT_DETAIL")

    // STORE_WRITERS is a property of this deployment: the sync process writes the same index.
    val store =
        VespaEventStore.open(
            vespaUrl,
            relay = relayUrl,
            autoDeploy = false,
            configUrl = configUrl,
            writers = STORE_WRITERS,
            searchExpansion = searchExpansion,
            slowQueryThresholdMillis = slowReadMs,
        )

    // Everything on this scope runs behind the server and is awaited nowhere;
    // blocking the port on any of it turns a restart into an outage.
    val maintenanceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    if (env["REINDEX_FTS_ON_START"]?.toBooleanStrictOrNull() == true) {
        launchFtsReindex(maintenanceScope, store, env["FTS_CURSOR_FILE"] ?: "/var/lib/vespa-relay/fts-cursor.txt")
    }
    env["SWEEP_ORPHAN_SCORES_ON_START"]?.trim()?.takeIf { it.isNotEmpty() }?.let { setting ->
        // Anything other than `true` is a dry run.
        launchOrphanScoreSweep(maintenanceScope, store, dryRun = setting.toBooleanStrictOrNull() != true)
    }
    // Seeded from the state file so a restart serves the last document until the first rollup.
    val statsSnapshot = StatsSnapshot(env["STATS_FILE"] ?: "/var/lib/vespa-relay/stats.json").also { it.loadFromFile() }
    // Two intervals: the tiers' queries are not within an order of magnitude of each other. See StatsTier.
    val statsIntervalSeconds = statsInterval(env, "STATS_INTERVAL_SECONDS", 900L)
    val statsCountersIntervalSeconds = statsInterval(env, "STATS_COUNTERS_INTERVAL_SECONDS", 60L)
    if (statsIntervalSeconds > 0 || statsCountersIntervalSeconds > 0) {
        val rollup =
            StatsRollup(
                StatsVespa(vespaUrl),
                // The normalized url, the same string NIP-42 and NIP-62 key off.
                relayUrl = relayUrl.url,
                // Read-only, off the volume the sync service writes to. Absent on a serve-only deployment.
                syncManifestFile = syncFile(env, "SYNC_MANIFEST_FILE", "/var/lib/vespa-relay/sync-manifest.json"),
            )
        // One rollup, two timers: the tiers write disjoint halves of the document.
        // Counters first, so a fresh relay has totals within seconds of boot.
        if (statsCountersIntervalSeconds > 0) {
            launchStatsRollup(maintenanceScope, rollup, statsSnapshot, StatsTier.COUNTERS, statsCountersIntervalSeconds)
        } else {
            println("stats: STATS_COUNTERS_INTERVAL_SECONDS=$statsCountersIntervalSeconds — no counters; /stats.json will carry only the chart sections")
        }
        if (statsIntervalSeconds > 0) {
            launchStatsRollup(maintenanceScope, rollup, statsSnapshot, StatsTier.CHARTS, statsIntervalSeconds)
        } else {
            println("stats: STATS_INTERVAL_SECONDS=$statsIntervalSeconds — no charts; /stats.json will carry only the counter sections")
        }
    } else {
        println("stats: both stats intervals are off — no rollup; /stats.json serves the state file, or 503 if there is none")
    }
    // The relay's own kind 0 and kind 10002, only with a key to sign them.
    // One instance, held past the boot publish: a NIP-86 rename republishes through it.
    val profile = identity?.let { RelayProfile(store, it, relayUrl) }
    if (profile != null) {
        launchRelayProfile(maintenanceScope, profile, nip11)
    }
    if (env["TRUST_RECONCILE_ON_START"]?.toBooleanStrictOrNull() != false) {
        maintenanceScope.launch {
            println("trust: reconciling in the background — ranked search may return less until this finishes")
            val startedMs = System.currentTimeMillis()
            reconcileTrustWithRetry(store)
            println("trust: background reconcile finished in ${(System.currentTimeMillis() - startedMs) / 1000}s")
        }
    }
    // The trust descent turns itself on once the max_rank walk has finished; the
    // walk runs even with the operator's switch off, and the line says which state applies.
    maintenanceScope.launch {
        val written =
            runCatching { store.awaitTrustDescent() }.getOrElse { e ->
                System.err.println("trust descent: OFF — the max_rank walk did not finish (${e.message?.take(200)}); ranked search stays on the full walk")
                return@launch
            }
        val walked = if (written > 0) " — max_rank written onto $written reputation documents" else ""
        println(
            if (store.trustDescent) {
                "trust descent: on$walked"
            } else {
                "trust descent: off by ${VespaEventStore.TRUST_DESCENT_ENV}$walked; ranked search takes the full walk"
            },
        )
    }

    // Recorded here, served over GET /pressure; the threshold belongs to the sync process that yields on it.
    val servingPressure = ServingPressure()
    // On maintenanceScope: a login must not wait on the store, and a check still running at shutdown dies with it.
    val trustNotice = TrustNotice(store, maintenanceScope)
    val relay =
        NostrRelayServer(
            store = store,
            servingPressure = servingPressure,
            relayUrl = relayUrl,
            onAuthenticated = trustNotice::check,
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
            requireReadLens = requireReadLens,
            searchConcurrencyPerConnection = searchConcurrencyPerConnectionFromEnv(env),
        )

    val sweeper = ExpirationSweeper(store, expirationSweepSecondsFromEnv(env)).start()

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

    // WHERE THIS PROCESS'S STORE RESOURCES GO, on its own port and off unless
    // an operator asks for it. Its own port because the document is not
    // public: `/stats.json` states every field is a fact about stored events
    // and nothing about clients belongs in it, and with PULSE_CLIENT_DETAIL on
    // this one carries which observer lenses and which search terms are
    // driving the load, plus a slow-read log that quotes the query. The port
    // is the boundary — nothing here authenticates — so bind it on the private
    // side of the network and do not publish it.
    val pulseSite =
        if (pulsePort <= 0) {
            null
        } else {
            val page = resourceText("/pulse.html") ?: error("pulse.html is missing from the :web jar — no pulse page can be served.")
            servePulseSite(
                port = pulsePort,
                page = page,
                document =
                    PulseDocument.reader(
                        store,
                        title = "Eventstore pulse — relay",
                        scope = "The serving relay's own store: what reads cost, what the engine did, and what the write path is waiting on.",
                        clientDerived = pulseClientDetail,
                    ),
                icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
            )
        }
    if (pulseSite != null) {
        println("vespa-relay pulse page on http://localhost:$pulsePort/" + (if (pulseClientDetail) "  [client detail ON — do not publish this port]" else ""))
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Cancelled and not waited for: an unfinished reconcile costs what it costs anyway.
            maintenanceScope.cancel()
            // Before the store: the page reads counters the store is about to stop keeping.
            pulseSite?.stop(0, 0)
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
        nip11 = nip11,
        limits = limits,
        admin = admin,
        pressure = servingPressure,
        // The doc is the profile's source, so a NIP-86 rewrite republishes the kind 0
        // and clears the fields the doc no longer carries.
        onInfoChanged = { doc ->
            profile?.let {
                launchRelayProfile(
                    maintenanceScope,
                    it,
                    nip11.copy(name = doc.name ?: nip11.name, description = doc.description, icon = doc.icon, banner = doc.banner),
                )
            }
        },
        onionLocation = addresses::onionLocation,
        landingPage = resourceText("/index.html"),
        observerStatsPage = resourceText("/observer_stats.html"),
        statsPage = resourceText("/stats.html"),
        statsJson = statsSnapshot,
        // What "no override" looks like; the doc cannot tell our url from an operator's.
        selfIconUrl = ownIconUrl,
    )
}

/** The http/https origin of a ws/wss url, for NIP-98's `u` tag. */
private fun String.httpFromWs(): String =
    when {
        startsWith("wss://") -> "https://" + substring(6)
        startsWith("ws://") -> "http://" + substring(5)
        else -> this
    }

/** A bundled classpath resource, or null if it isn't there. */
private fun resourceText(path: String): String? = object {}.javaClass.getResource(path)?.readText()

/**
 * One of the stats rollup's two intervals, in seconds. A value that does not
 * parse stops the boot: `=off` falling back to the default would run the
 * rollup on the schedule the operator was trying to change.
 */
private fun statsInterval(
    env: Map<String, String>,
    key: String,
    default: Long,
): Long =
    env[key]?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull() ?: error("$key='$it' is not a number of seconds. Use 0 to disable it.")
    } ?: default

/**
 * The sync process's files this relay reads, exempt from the unused-settings
 * warning. The other sync files are served by the sync process itself.
 */
private val SYNC_FILES_THE_RELAY_READS = setOf("SYNC_MANIFEST_FILE")

/**
 * Where one of the sync process's files lives: the env, or the path
 * `docker-compose.yml` mounts it at. An explicit empty value turns the card off.
 */
private fun syncFile(
    env: Map<String, String>,
    key: String,
    default: String,
): File? =
    when (val raw = env[key]) {
        null -> File(default)
        else -> raw.trim().takeIf { it.isNotEmpty() }?.let(::File)
    }
