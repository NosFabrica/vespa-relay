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
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.relay.identity.RelayIdentity
import com.nosfabrica.vespa.relay.identity.adminPubkeysFromEnv
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.probedIngestPipeline
import com.nosfabrica.vespa.relay.maintenance.ExpirationSweeper
import com.nosfabrica.vespa.relay.maintenance.RelayProfile
import com.nosfabrica.vespa.relay.maintenance.RelayProfilePublisher
import com.nosfabrica.vespa.relay.maintenance.StatsRollup
import com.nosfabrica.vespa.relay.maintenance.StatsTier
import com.nosfabrica.vespa.relay.maintenance.StatsVespa
import com.nosfabrica.vespa.relay.maintenance.launchFtsReindex
import com.nosfabrica.vespa.relay.maintenance.launchOrphanScoreSweep
import com.nosfabrica.vespa.relay.maintenance.launchStatsRollup
import com.nosfabrica.vespa.relay.maintenance.orphanSweepDryRun
import com.nosfabrica.vespa.relay.maintenance.parseReindexKinds
import com.nosfabrica.vespa.relay.maintenance.reconcileTrustWithRetry
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.pulse.PulseDocument
import com.nosfabrica.vespa.relay.pulse.StoreMetricsLog
import com.nosfabrica.vespa.relay.pulse.TrustDocument
import com.nosfabrica.vespa.relay.pulse.pulseAdmins
import com.nosfabrica.vespa.relay.pulse.pulsePublic
import com.nosfabrica.vespa.relay.pulse.pulsePublicUrl
import com.nosfabrica.vespa.relay.pulse.pulseSlowReadMs
import com.nosfabrica.vespa.relay.server.ConnectionCountListener
import com.nosfabrica.vespa.relay.server.HttpRelay
import com.nosfabrica.vespa.relay.server.MirrorDeleteHandler
import com.nosfabrica.vespa.relay.server.MirrorLane
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.nosfabrica.vespa.relay.server.Nip86Admin
import com.nosfabrica.vespa.relay.server.NostrRelayServer
import com.nosfabrica.vespa.relay.server.OpenRanking
import com.nosfabrica.vespa.relay.server.OpenRankingConfig
import com.nosfabrica.vespa.relay.server.TrustNotice
import com.nosfabrica.vespa.relay.server.config.MaintenanceSwitches
import com.nosfabrica.vespa.relay.server.config.RelayAddresses
import com.nosfabrica.vespa.relay.server.config.RelayEndpoint
import com.nosfabrica.vespa.relay.server.config.RelayFiles
import com.nosfabrica.vespa.relay.server.config.RelayIngest
import com.nosfabrica.vespa.relay.server.config.StatsIntervals
import com.nosfabrica.vespa.relay.server.config.allowKindsFromEnv
import com.nosfabrica.vespa.relay.server.config.allowPubkeysFromEnv
import com.nosfabrica.vespa.relay.server.config.denyKindsFromEnv
import com.nosfabrica.vespa.relay.server.config.denyPubkeysFromEnv
import com.nosfabrica.vespa.relay.server.config.expirationSweepSecondsFromEnv
import com.nosfabrica.vespa.relay.server.config.httpFromWs
import com.nosfabrica.vespa.relay.server.config.httpRelayFromEnv
import com.nosfabrica.vespa.relay.server.config.maintenanceSwitchesFromEnv
import com.nosfabrica.vespa.relay.server.config.mirrorIngestFromEnv
import com.nosfabrica.vespa.relay.server.config.negentropySettingsFromEnv
import com.nosfabrica.vespa.relay.server.config.nip11FromEnv
import com.nosfabrica.vespa.relay.server.config.openRankingFromEnv
import com.nosfabrica.vespa.relay.server.config.refuseSyncConfig
import com.nosfabrica.vespa.relay.server.config.rejectFutureSecondsFromEnv
import com.nosfabrica.vespa.relay.server.config.relayAddressesFromEnv
import com.nosfabrica.vespa.relay.server.config.relayEndpointFromEnv
import com.nosfabrica.vespa.relay.server.config.relayFilesFromEnv
import com.nosfabrica.vespa.relay.server.config.relayIngestFromEnv
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.nosfabrica.vespa.relay.server.config.requireReadLensFromEnv
import com.nosfabrica.vespa.relay.server.config.searchConcurrencyPerConnectionFromEnv
import com.nosfabrica.vespa.relay.server.config.searchExpansionFromEnv
import com.nosfabrica.vespa.relay.server.config.setUnder
import com.nosfabrica.vespa.relay.server.config.statsIntervalsFromEnv
import com.nosfabrica.vespa.relay.server.config.syncSettingsIgnored
import com.nosfabrica.vespa.relay.server.openBanStore
import com.nosfabrica.vespa.relay.server.serveRelay
import com.nosfabrica.vespa.relay.store.STORE_WRITERS
import com.nosfabrica.vespa.relay.store.VespaEndpoints
import com.nosfabrica.vespa.relay.store.deployBundledSchema
import com.nosfabrica.vespa.relay.store.feedBatch
import com.nosfabrica.vespa.relay.store.providerRefreshSeconds
import com.nosfabrica.vespa.relay.store.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.store.vespaEndpoints
import com.nosfabrica.vespa.relay.util.applyQuartzLogLevel
import com.nosfabrica.vespa.relay.util.exitOnBootFailure
import com.nosfabrica.vespa.relay.util.strictFlag
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.nosfabrica.vespa.relay.web.Nip98AdminGate
import com.nosfabrica.vespa.relay.web.PulseGuard
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.servePulseSite
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicReference

/**
 * The relay process: open the store, serve the NIP-50 relay and the NIP-11 doc, and block.
 * Configured from the environment (`docs/configuration.md` lists every variable); `RELAY_URL`
 * is required. Mirroring into the same store is the sync process's job.
 */
fun main() {
    exitOnBootFailure()
    val env = System.getenv()
    val boot = readSettings(env)
    deploySchema(boot)
    val store = openStore(boot)
    // The pulse page states its totals over this window, which opens with the store, not the process.
    val storeOpenedAt = System.currentTimeMillis()
    // Runs behind the server and is awaited nowhere; blocking the port on it makes a restart an outage.
    val maintenance = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    startMaintenance(boot, store, maintenance)
    val statsSnapshot = startStats(boot, maintenance)
    val profile = startProfile(boot, store, maintenance)
    val servingPressure = ServingPressure(thresholdMs = boot.pressureThresholdMs)
    // Its own scope: shutdown stops the writers after the connections, never before.
    val ingestScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    val ingest = startIngest(boot, store, ingestScope)
    val mirror = startMirrorLane(boot, store, ingestScope, servingPressure)
    val relay = buildRelay(boot, store, servingPressure, TrustNotice(store, maintenance), ingest, mirror)
    // The retraction audit's deletes, from a sync that forwards here; see MirrorDelete.
    boot.identity?.let { relay.eventCommandHandler = MirrorDeleteHandler(it.pubKey, purge = { filter -> store.delete(filter) }) }
    val sweeper = ExpirationSweeper(store, boot.expirationSweepSeconds).start()
    applyQuartzLogLevel(env)
    val pulseSite = startPulseSite(boot, store, storeOpenedAt)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Cancelled and not waited for: an unfinished reconcile costs what it costs anyway.
            maintenance.cancel()
            // Before the store: the page reads counters the store is about to stop keeping.
            pulseSite?.stop(0, 0)
            sweeper.close()
            relay.close()
            ingest?.closeIntake()
            mirror?.closeIntake()
            ingestScope.cancel()
            ingest?.close()
            mirror?.close()
            store.close()
        },
    )
    serve(boot, relay, store, servingPressure, statsSnapshot, profile)
}

/** Everything the process reads from its environment, parsed before anything touches the network. */
private class Boot(
    val vespaUrl: String,
    val vespa: VespaEndpoints,
    val configUrl: String,
    val autoDeploy: Boolean,
    val port: Int,
    val endpoint: RelayEndpoint,
    val identity: NostrSignerInternal?,
    val addresses: RelayAddresses,
    val limits: RelayLimits,
    val negentropy: NegentropySettings,
    val rejectFutureSeconds: Int,
    val providerRefreshSeconds: Long,
    val reindexKinds: List<Int>?,
    val requireReadLens: Boolean,
    val openRanking: OpenRankingConfig?,
    val httpRelay: HttpRelay?,
    val searchExpansion: SearchExpansionLimits,
    val nip11: Nip11Info,
    val adminPubkeys: Set<String>,
    val banStore: BanStore?,
    val files: RelayFiles,
    val stats: StatsIntervals,
    val switches: MaintenanceSwitches,
    val orphanSweepDryRun: Boolean?,
    val pubkeyAllow: Set<String>,
    val pubkeyDeny: Set<String>,
    val kindAllow: Set<Int>,
    val kindDeny: Set<Int>,
    val searchConcurrency: Int,
    val ingest: RelayIngest,
    val mirrorIngest: IngestTuning,
    /** Where the mirror lane starts yielding to client reads; the sync process reads the same setting. */
    val pressureThresholdMs: Long,
    val expirationSweepSeconds: Long,
    val pulse: Pulse,
)

/** The pulse site's settings, resolved with the rest so a port with no administrator stops the boot early. */
private class Pulse(
    val port: Int,
    val clientDetail: Boolean,
    val public: Boolean,
    val slowReadMs: Long?,
    val guard: PulseGuard?,
    val icon: String?,
)

/** Reads and checks every setting, saying what is set but inert; a bad value stops the boot here. */
private fun readSettings(env: Map<String, String>): Boot {
    refuseSyncConfig(env)
    syncSettingsIgnored(env).takeIf { it.isNotEmpty() }?.let {
        System.err.println("relay: ${it.joinToString()} — read by the sync process, not the relay; set them on that service or they do nothing")
    }
    val vespaUrl = env["VESPA_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: "http://localhost:8080"
    val vespa = vespaEndpoints(env)
    vespa.describe()?.let { System.err.println("relay: the store sends its $it") }
    val endpoint = relayEndpointFromEnv(env)
    // Read first so a malformed key stops the boot here. Unset is an anonymous relay.
    val identity = RelayIdentity.fromEnv { env[it] }
    identity?.let { System.err.println("relay identity: ${it.pubKey.take(12)}… (NIP-11 self, NIP-42 auth, NIP-66 monitor)") }
    // The eager alternates() names an already published .onion in the boot log.
    val addresses = relayAddressesFromEnv(env).also { it.alternates() }

    val requireReadLens = requireReadLensFromEnv(env)
    if (!requireReadLens) System.err.println("relay: REQUIRE_READ_LENS=false — anonymous reads are answered unranked, over the whole corpus")
    val openRanking = openRankingFromEnv(env)
    if (openRanking == null) {
        (env.setUnder("ORE_") - "ORE_ENABLED").takeIf { it.isNotEmpty() }?.let { System.err.println("relay: ORE_ENABLED is off — ${it.joinToString()} do nothing") }
    }
    // Every address the relay answers at is an origin a NIP-98 token may name.
    val httpRelay = httpRelayFromEnv(env) { listOf(endpoint.http) + addresses.alternates().map { it.url.httpFromWs() } }
    if (httpRelay == null) {
        env.setUnder("HTTP_RELAY_").takeIf { it.isNotEmpty() }?.let { System.err.println("relay: HTTP_RELAY=false — ${it.joinToString()} do nothing") }
    }
    val searchExpansion = searchExpansionFromEnv(env)
    if (!searchExpansion.enabled) {
        System.err.println("relay: SEARCH_EXPAND_REFERENCES=false — a search answers with its own hits only")
    } else if (searchExpansion.maxPerEvent == 0 || searchExpansion.maxPerRequest == 0) {
        System.err.println("relay: search reference expansion is on but capped at 0 — it will add nothing")
    }

    val reindexKinds = parseReindexKinds(env["REINDEX_FTS_KINDS"])
    val switches = maintenanceSwitchesFromEnv(env)
    if (!switches.reindexFts && reindexKinds != null) {
        System.err.println("relay: REINDEX_FTS_KINDS is set but REINDEX_FTS_ON_START is not true — no reindex runs")
    }
    val files = relayFilesFromEnv(env)
    val adminPubkeys = adminPubkeysFromEnv(env)
    // The store reads it; checked here so a typo stops the boot instead of quietly turning batching off.
    feedBatch(env)?.let { n ->
        System.err.println("relay: the store writes " + (if (n > 1) "$n documents per request through the batch handler" else "one document per request (VESPA_FEED_BATCH=$n)"))
    }
    return Boot(
        vespaUrl = vespaUrl,
        vespa = vespa,
        configUrl = env["VESPA_CONFIG_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: vespaConfigUrlFor(vespaUrl),
        autoDeploy = env.strictFlag("AUTO_DEPLOY") ?: true,
        port = env.strictInt("RELAY_PORT", 1..65_535) ?: 7777,
        endpoint = endpoint,
        identity = identity,
        addresses = addresses,
        limits = relayLimitsFromEnv(env),
        negentropy = negentropySettingsFromEnv(env),
        rejectFutureSeconds = rejectFutureSecondsFromEnv(env),
        providerRefreshSeconds = providerRefreshSeconds(env),
        reindexKinds = reindexKinds,
        requireReadLens = requireReadLens,
        openRanking = openRanking,
        httpRelay = httpRelay,
        searchExpansion = searchExpansion,
        nip11 = nip11FromEnv(env, identity?.pubKey, endpoint.selfIconUrl),
        adminPubkeys = adminPubkeys,
        // Opened here: a ban file that cannot be read stops the boot before the schema deploy.
        banStore = if (adminPubkeys.isNotEmpty()) openBanStore(files.banState) else null,
        files = files,
        stats = statsIntervalsFromEnv(env),
        switches = switches,
        orphanSweepDryRun = orphanSweepDryRun(env["SWEEP_ORPHAN_SCORES_ON_START"]),
        pubkeyAllow = allowPubkeysFromEnv(env),
        pubkeyDeny = denyPubkeysFromEnv(env),
        kindAllow = allowKindsFromEnv(env),
        kindDeny = denyKindsFromEnv(env),
        searchConcurrency = searchConcurrencyPerConnectionFromEnv(env),
        ingest = relayIngestFromEnv(env),
        mirrorIngest = mirrorIngestFromEnv(env),
        pressureThresholdMs = env.strictLong("SERVING_PRESSURE_THRESHOLD_MS", 100L..Long.MAX_VALUE) ?: ServingPressure.DEFAULT_THRESHOLD_MS,
        expirationSweepSeconds = expirationSweepSecondsFromEnv(env),
        pulse = pulseFromEnv(env, adminPubkeys),
    )
}

/** `PULSE_*`: a port with no administrator named refuses the boot, and a public pulse must be the operational half only. */
private fun pulseFromEnv(
    env: Map<String, String>,
    adminPubkeys: Set<String>,
): Pulse {
    val port = env.strictInt("PULSE_PORT", 0..65_535) ?: 0
    val clientDetail = env.strictFlag("PULSE_CLIENT_DETAIL") ?: false
    val public = pulsePublic(env, clientDetail)
    return Pulse(
        port = port,
        clientDetail = clientDetail,
        public = public,
        slowReadMs = pulseSlowReadMs(env, "PULSE_SLOW_READ_MS", clientDetail, "PULSE_CLIENT_DETAIL"),
        guard = if (port <= 0) null else PulseGuard(Nip98AdminGate(pulseAdmins(adminPubkeys, "PULSE_PORT", public = public), pulsePublicUrl(env, "PULSE_PUBLIC_URL", port))),
        icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
    )
}

private fun deploySchema(boot: Boot) {
    if (!boot.autoDeploy) {
        System.err.println("schema: AUTO_DEPLOY=false — not deploying; Vespa serves the application package it already has")
        return
    }
    System.err.println("schema: deploying the bundled application package to ${boot.configUrl}")
    deployBundledSchema(boot.vespaUrl, boot.configUrl)
    System.err.println("schema: deployed and serving")
}

private fun openStore(boot: Boot): VespaEventStore =
    VespaEventStore.open(
        boot.vespaUrl,
        relay = boot.endpoint.ws,
        autoDeploy = false,
        feedEndpoints = boot.vespa.feed,
        queryEndpoints = boot.vespa.query,
        configUrl = boot.configUrl,
        writers = STORE_WRITERS,
        providerRefreshSeconds = boot.providerRefreshSeconds,
        searchExpansion = boot.searchExpansion,
        slowQueryThresholdMillis = boot.pulse.slowReadMs,
    )

/** The background repairs and the metrics line, each only when switched on. */
private fun startMaintenance(
    boot: Boot,
    store: VespaEventStore,
    scope: CoroutineScope,
) {
    if (boot.switches.reindexFts) launchFtsReindex(scope, store, boot.files.ftsCursor.path, boot.reindexKinds)
    boot.orphanSweepDryRun?.let { dryRun -> launchOrphanScoreSweep(scope, store, dryRun) }
    // On by default: the page is gated, these counters are not, and a diagnostic nobody enables is missing.
    StoreMetricsLog.startLogging("relay", store, boot.switches.storeMetricsLogSeconds)
    if (boot.switches.trustReconcile) {
        scope.launch {
            println("trust: reconciling in the background — ranked search may return less until this finishes")
            val startedMs = System.currentTimeMillis()
            reconcileTrustWithRetry(store)
            println("trust: background reconcile finished in ${(System.currentTimeMillis() - startedMs) / 1000}s")
        }
    }
}

/** The `/stats.json` rollups, counters first so a fresh relay has totals within seconds of boot. */
private fun startStats(
    boot: Boot,
    scope: CoroutineScope,
): StatsSnapshot {
    // Seeded from the state file so a restart serves the last document until the first rollup.
    val snapshot = StatsSnapshot(boot.files.stats.path).also { it.loadFromFile() }
    val (charts, counters) = boot.stats.chartsSeconds to boot.stats.countersSeconds
    if (charts == 0L && counters == 0L) {
        println("stats: both stats intervals are off — no rollup; /stats.json serves the state file, or 503 if there is none")
        return snapshot
    }
    val rollup = StatsRollup(StatsVespa(boot.vespa.queryUrl(boot.vespaUrl)), relayUrl = boot.endpoint.ws.url, syncManifestFile = boot.files.syncManifest)
    if (counters > 0) {
        launchStatsRollup(scope, rollup, snapshot, StatsTier.COUNTERS, counters)
    } else {
        println("stats: STATS_COUNTERS_INTERVAL_SECONDS=0 — no counters; /stats.json will carry only the chart sections")
    }
    if (charts > 0) {
        launchStatsRollup(scope, rollup, snapshot, StatsTier.CHARTS, charts)
    } else {
        println("stats: STATS_INTERVAL_SECONDS=0 — no charts; /stats.json will carry only the counter sections")
    }
    return snapshot
}

/** The relay's own kind 0 and 10002 when it has an identity: published at boot and on every NIP-86 rewrite. */
private class Profile(
    private val base: Nip11Info,
    publisher: (source: () -> Nip11Info) -> RelayProfilePublisher?,
) {
    // Read by the publisher when it runs, so the newest rewrite is what gets signed.
    private val current = AtomicReference(base)
    private val publisher = publisher(current::get)

    fun publish() {
        publisher?.request()
    }

    fun rewritten(doc: Nip11RelayInformation) {
        current.set(base.copy(name = doc.name ?: base.name, description = doc.description, icon = doc.icon, banner = doc.banner))
        publish()
    }
}

private fun startProfile(
    boot: Boot,
    store: VespaEventStore,
    scope: CoroutineScope,
): Profile =
    Profile(boot.nip11) { source ->
        boot.identity?.let { RelayProfilePublisher(scope, RelayProfile(store, it, boot.endpoint.ws), source) }
    }.also { it.publish() }

private fun buildRelay(
    boot: Boot,
    store: VespaEventStore,
    pressure: ServingPressure,
    trustNotice: TrustNotice,
    ingest: IngestPipeline?,
    mirror: IngestPipeline?,
): NostrRelayServer =
    NostrRelayServer(
        store = store,
        servingPressure = pressure,
        relayUrl = boot.endpoint.ws,
        onAuthenticated = trustNotice::check,
        alsoServedAt = boot.addresses::alternates,
        listener = if (boot.switches.logConnections) ConnectionCountListener() else RelayServerListener.None,
        limits = boot.limits,
        negentropySettings = boot.negentropy,
        banStore = boot.banStore,
        pubkeyAllow = boot.pubkeyAllow,
        pubkeyDeny = boot.pubkeyDeny,
        kindAllow = boot.kindAllow,
        kindDeny = boot.kindDeny,
        rejectFutureSeconds = boot.rejectFutureSeconds,
        requireReadLens = boot.requireReadLens,
        searchConcurrencyPerConnection = boot.searchConcurrency,
        ingestBatch = boot.ingest.batch,
        ingest = ingest,
        mirrorLane = mirror?.let { pipeline -> boot.identity?.let { MirrorLane(it.pubKey, pipeline) } },
    )

/**
 * The sync process's write lane ([MirrorLane]): its own pipeline, yielding to the clients' reads.
 * Only with an identity, since the lane is chosen by a connection signing in as it; without one a
 * sync forwarding here takes the publish lane, which is correct and only shares its queue.
 */
private fun startMirrorLane(
    boot: Boot,
    store: VespaEventStore,
    scope: CoroutineScope,
    pressure: ServingPressure,
): IngestPipeline? {
    if (boot.identity == null) {
        println("relay: no RELAY_NSEC, so no mirror lane — a sync forwarding here (SYNC_INGEST=relay) shares the publishes' queue")
        return null
    }
    val tuning = boot.mirrorIngest
    println("relay: the mirror lane writes what the sync forwards, ${tuning.concurrency} worker(s) of up to ${tuning.batch} event(s)")
    return probedIngestPipeline(store, tuning, scope, servingPressure = pressure).also(IngestPipeline::start)
}

/** The publish path: the mirror's pipeline, or quartz's single-writer queue when `RELAY_INGEST=queue`. */
private fun startIngest(
    boot: Boot,
    store: VespaEventStore,
    scope: CoroutineScope,
): IngestPipeline? {
    val tuning = boot.ingest.tuning
    println(
        "relay: publishes write through " +
            (tuning?.let { "the ingest pipeline, ${it.concurrency} worker(s) of up to ${it.batch} event(s)" } ?: "quartz's ingest queue, batches of up to ${boot.ingest.batch}"),
    )
    return tuning?.let { probedIngestPipeline(store, it, scope).also(IngestPipeline::start) }
}

/** Administrators only, on its own port: the pulse names the lenses and terms driving the load. */
private fun startPulseSite(
    boot: Boot,
    store: VespaEventStore,
    storeOpenedAt: Long,
) = boot.pulse.guard?.let { guard ->
    val pulse = boot.pulse
    val page = resourceText("/pulse.html") ?: error("pulse.html is missing from the :web jar — no pulse page can be served.")
    servePulseSite(
        port = pulse.port,
        page = page,
        guard = guard,
        public = pulse.public,
        document =
            PulseDocument.reader(
                store,
                startedAtMillis = storeOpenedAt,
                title = "Eventstore pulse — relay",
                scope = "The serving relay's own store: what reads cost, what the engine did, and what the write path is waiting on.",
                clientDerived = pulse.clientDetail,
            ),
        icon = pulse.icon,
    ).also {
        println(
            "vespa-relay pulse page on http://localhost:${pulse.port}/  [${boot.adminPubkeys.size} admin key(s)" +
                (if (pulse.clientDetail) ", client detail ON" else "") + "]",
        )
    }
}

/** Serves the relay on its port and blocks. */
private fun serve(
    boot: Boot,
    relay: NostrRelayServer,
    store: VespaEventStore,
    pressure: ServingPressure,
    statsSnapshot: StatsSnapshot,
    profile: Profile,
) {
    val admin =
        boot.banStore?.let {
            Nip86Admin(
                banStore = it,
                adminPubkeys = boot.adminPubkeys,
                relayHttpUrl = boot.endpoint.http,
                // Banning a source also drops what it already published.
                purge = { filter -> store.delete(filter) },
            )
        }
    println(
        "vespa-relay listening on :${boot.port}  (vespa ${boot.vespaUrl}, relay ${boot.endpoint.ws})" +
            (if (admin != null) "  [NIP-86 admin: ${boot.adminPubkeys.size} key(s)]" else ""),
    )
    serveRelay(
        relay = relay,
        port = boot.port,
        nip11 = boot.nip11,
        limits = boot.limits,
        admin = admin,
        pressure = pressure,
        // The doc is the kind 0's source: a NIP-86 rewrite republishes it, clearing what the doc dropped.
        onInfoChanged = profile::rewritten,
        onionLocation = boot.addresses::onionLocation,
        landingPage = resourceText("/index.html"),
        observerStatsPage = resourceText("/observer_stats.html"),
        statsPage = resourceText("/stats.html"),
        // Public on purpose (docs/decisions/relay-server.md); the pulse's gate is for what people searched for.
        trustPage = resourceText("/trust.html"),
        trustJson = TrustDocument.reader(store, "This relay's trust projection"),
        trustExplain = { key -> TrustDocument.explain(store, key) },
        openRanking = boot.openRanking?.let { config -> OpenRanking({ q, observer, limit -> store.store.scoredProfileSearch(q, observer, limit) }, config, pressure = pressure) },
        statsJson = statsSnapshot,
        selfIconUrl = boot.endpoint.selfIconUrl,
        httpRelay = boot.httpRelay,
    )
}

/** A bundled classpath resource, or null if it isn't there. */
private fun resourceText(path: String): String? = object {}.javaClass.getResource(path)?.readText()
