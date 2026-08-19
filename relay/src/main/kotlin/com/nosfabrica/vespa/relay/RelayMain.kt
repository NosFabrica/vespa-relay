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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.io.File

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
                // ...except the ones the relay now READS. They are still the
                // router's files and it is still the only writer, but the stats
                // rollup reads them off the shared volume, so the warning's
                // premise — "read by nobody here" — is false for exactly these.
                // Named identically on both services on purpose: one setting
                // per file, so moving the path cannot leave the card pointed at
                // where the router used to write.
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

    // The icon this relay serves on its own tab, as an absolute url a stranger
    // can fetch — the value `RELAY_ICON` defaults to, and the one the server
    // compares against to tell "no override" from an operator's own icon.
    //
    // From `RELAY_HTTP_URL` when it is set, because that is already this
    // deployment's answer to "what http origin am I reachable at" (NIP-98 binds
    // its tokens to it), and a relay behind a proxy on another name would
    // otherwise advertise an icon at the websocket's host. Null when neither
    // address is one a stranger can reach.
    val ownIconUrl = selfIconUrl(env["RELAY_HTTP_URL"] ?: relayUrl.url)

    // How this relay presents itself. Built here rather than at the serveRelay
    // call it used to sit in, because the relay's own kind 0 is these same
    // fields — one description of this relay, served as NIP-11 and published
    // as an event, and no second place for it to drift.
    val nip11 =
        Nip11Info(
            name = env["RELAY_NAME"] ?: "vespa-relay",
            description = env["RELAY_DESCRIPTION"],
            // One icon, both places. Unset publishes the one this relay already
            // serves on its own tab, so a stock deployment stops advertising
            // nothing to the clients that draw a relay beside its picture;
            // set, it is also what the pages link and what /favicon.ico
            // redirects to. Null for an address a stranger cannot reach — see
            // selfIconUrl, which refuses to sign `http://localhost:7777/…` into
            // a public kind 0 on every development boot.
            icon = env["RELAY_ICON"] ?: ownIconUrl,
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
        )

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

    // STORE_WRITERS, not the store's default, because the answer is a property
    // of this deployment and not of the library: the sync process writes the
    // same index, and the deletions this relay must honour are largely ones it
    // mirrored.
    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = false, configUrl = configUrl, writers = STORE_WRITERS)

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
    // The corpus statistics behind GET /stats.json and /stats.html.
    // Seeded from the state file first, so a restart serves the last document
    // instead of a blank page for however long the first rollup takes.
    val statsSnapshot = StatsSnapshot(env["STATS_FILE"] ?: "/var/lib/vespa-relay/stats.json").also { it.loadFromFile() }
    // TWO INTERVALS, because the queries behind this document are not within an
    // order of magnitude of each other in cost — see `StatsTier`. The
    // corpus-wide groupings keep the setting they always had; the cheap counters
    // (totals, freshness, trust health, the router's heartbeat) get their own,
    // fifteen times faster, because nothing about them justified being that
    // stale.
    val statsIntervalSeconds = statsInterval(env, "STATS_INTERVAL_SECONDS", 900L)
    val statsCountersIntervalSeconds = statsInterval(env, "STATS_COUNTERS_INTERVAL_SECONDS", 60L)
    if (statsIntervalSeconds > 0 || statsCountersIntervalSeconds > 0) {
        val rollup =
            StatsRollup(
                StatsVespa(vespaUrl),
                // The NORMALIZED url, the same string NIP-42 and NIP-62 key off —
                // so a document fetched from two of this relay's addresses names
                // one relay rather than reading as two.
                relayUrl = relayUrl.url,
                // Read-only, off the volume the sync service writes it to.
                // Absent is the normal case — a serve-only deployment has no
                // router — and the section is then simply not in the document.
                // The manifest ALONE: what the mirror has walked and what it is
                // doing are served by the sync process itself now.
                syncManifestFile = syncFile(env, "SYNC_MANIFEST_FILE", "/var/lib/vespa-relay/sync-manifest.json"),
            )
        // ONE ROLLUP, two timers: the tiers write disjoint halves of the
        // document and share nothing else, so a second instance would only be a
        // second copy of the same config.
        //
        // The counters go first so a fresh relay has totals, trust health and
        // router progress within a second or two of boot, rather than a blank
        // page for however long the first histogram takes.
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
        // A zero/negative interval is "don't compute", which is a legitimate
        // choice on a busy box — the grouping competes with client reads. Say
        // so, because the alternative is an operator watching a page that never
        // fills and looking for the bug in the rollup.
        println("stats: both stats intervals are off — no rollup; /stats.json serves the state file, or 503 if there is none")
    }
    // Say who this relay is, in the two kinds every client already reads: a
    // kind 0 carrying the NIP-11 name and description, and a kind 10002 naming
    // this relay as its own inbox and outbox. Only with a key to sign them —
    // an anonymous relay has nothing to be discovered AS.
    //
    // One instance, held past the boot publish: a NIP-86 rename republishes
    // through the same object below, and it is the object that serializes them.
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

    // The relay server measures client reads into it; the sync process polls
    // the mean over GET /pressure to decide whether its ingest should yield.
    // No threshold here on purpose: this side only records and serves — the
    // threshold belongs to the process that yields on it, and reading
    // SERVING_PRESSURE_THRESHOLD_MS into an instance whose backoffMs() nobody
    // calls would be a setting that is accepted and does nothing.
    val servingPressure = ServingPressure()
    // What a reader is told the moment they sign in: whether this relay holds
    // the two things their ranking depends on, and nothing at all when it does.
    // On `maintenanceScope` like every other read that happens behind the
    // server — a login must not wait on the store, and a check still running
    // when the process is asked to stop dies with it.
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
        nip11 = nip11,
        limits = limits,
        admin = admin,
        pressure = servingPressure,
        // A NIP-86 admin RPC can rename this relay, re-describe it or change
        // its icon while it runs. The doc is the profile's source, so the
        // profile follows it here rather than freezing what the environment
        // said at boot — the fields the doc no longer carries are cleared from
        // the kind 0 for the same reason an unset RELAY_DESCRIPTION is.
        onInfoChanged = { doc ->
            profile?.let {
                launchRelayProfile(
                    maintenanceScope,
                    it,
                    nip11.copy(name = doc.name ?: nip11.name, description = doc.description, icon = doc.icon, banner = doc.banner),
                )
            }
        },
        // "This relay is also a hidden service" — read by Tor Browser and by
        // clients that move the connection inside the network when Tor is on.
        onionLocation = addresses::onionLocation,
        // The bundled web UI (a NIP-50 client) — served on a plain browser GET.
        landingPage = resourceText("/index.html"),
        observerStatsPage = resourceText("/observer_stats.html"),
        statsPage = resourceText("/stats.html"),
        statsJson = statsSnapshot,
        // What "no override" looks like. The server compares the doc's icon
        // against this to decide whether /favicon.ico redirects — it cannot
        // read that off the doc, where our own url and an operator's are the
        // same field.
        selfIconUrl = ownIconUrl,
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

/**
 * One of the stats rollup's two intervals, in seconds.
 *
 * A value that will not parse STOPS the boot rather than falling back to the
 * default. `=0s` or `=off` are the obvious ways an operator writes "turn this
 * off", and `?: default` accepted both by silently running the rollup on the
 * schedule they were trying to change — a setting that was read, was wrong, and
 * did the opposite of what it said, which is exactly the silent inertness this
 * codebase refuses elsewhere. The message names [key] because there are two of
 * these now and they are off by a factor of fifteen.
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
 * The router file the stats rollup reads. A shared name, not a relay-side copy —
 * see the exemption in the unused-settings warning above.
 *
 * ONE, where it was four. `SYNC_STATE_FILE`, `SYNC_SWEEP_STATE_FILE` and
 * `SYNC_PROGRESS_FILE` are the sync process's own now: it serves what it has
 * walked and what it is doing on its own status site, so setting them here is
 * exactly the mistake the warning above exists to catch.
 */
private val SYNC_FILES_THE_RELAY_READS = setOf("SYNC_MANIFEST_FILE")

/**
 * Where one of the router's files lives, from the env or the path
 * `docker-compose.yml` mounts it at.
 *
 * The default is not a guess: it is the same literal the compose file gives
 * both services, so the stock deployment charts its sync with nothing set. An
 * explicit empty value turns the card off, which is the escape hatch for a
 * deployment that mounts the volume but does not want the relay reading it.
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
