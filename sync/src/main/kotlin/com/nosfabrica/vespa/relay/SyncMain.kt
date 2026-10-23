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
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.identity.RelayIdentity
import com.nosfabrica.vespa.relay.identity.adminPubkeysFromEnv
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.RelayWriter
import com.nosfabrica.vespa.relay.ingest.knownIdsProbe
import com.nosfabrica.vespa.relay.ingest.newestVersionsProbe
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.onionUpstreams
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.pulse.PulseDocument
import com.nosfabrica.vespa.relay.pulse.StoreMetricsLog
import com.nosfabrica.vespa.relay.pulse.pulseAdmins
import com.nosfabrica.vespa.relay.pulse.pulsePublic
import com.nosfabrica.vespa.relay.pulse.pulsePublicUrl
import com.nosfabrica.vespa.relay.pulse.pulseSlowReadMs
import com.nosfabrica.vespa.relay.status.StatusRollup
import com.nosfabrica.vespa.relay.status.SyncProgress
import com.nosfabrica.vespa.relay.status.SyncStatus
import com.nosfabrica.vespa.relay.store.STORE_WRITERS
import com.nosfabrica.vespa.relay.store.awaitServedSchema
import com.nosfabrica.vespa.relay.store.providerRefreshSeconds
import com.nosfabrica.vespa.relay.store.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.store.vespaEndpoints
import com.nosfabrica.vespa.relay.sync.KeyVerdict
import com.nosfabrica.vespa.relay.sync.PressurePoller
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.sync.SyncEngine
import com.nosfabrica.vespa.relay.sync.SyncManifest
import com.nosfabrica.vespa.relay.sync.ingestDialFromEnv
import com.nosfabrica.vespa.relay.sync.keyVerdict
import com.nosfabrica.vespa.relay.sync.relaySelf
import com.nosfabrica.vespa.relay.sync.shutdownStep
import com.nosfabrica.vespa.relay.sync.watchRelayKey
import com.nosfabrica.vespa.relay.util.BootClock
import com.nosfabrica.vespa.relay.util.BootTask
import com.nosfabrica.vespa.relay.util.applyQuartzJsonEngine
import com.nosfabrica.vespa.relay.util.cacheDnsBriefly
import com.nosfabrica.vespa.relay.util.exitOnBootFailure
import com.nosfabrica.vespa.relay.util.strictChoice
import com.nosfabrica.vespa.relay.util.strictFlag
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.nosfabrica.vespa.relay.web.Nip98AdminGate
import com.nosfabrica.vespa.relay.web.PulseGuard
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.servePulseSite
import com.nosfabrica.vespa.relay.web.serveStatusSite
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlin.system.exitProcess

/** One past the relay's 7777, so the pair is guessable from either end. */
private const val DEFAULT_STATUS_PORT = 7778

private const val DEFAULT_MONITOR_STATUS_PORT = 7779

/** Matched to the mirror's own progress tick. */
private const val DEFAULT_STATUS_INTERVAL_SECONDS = 30L

/** `SYNC_STATUS_INTERVAL_SECONDS`. */
private fun statusInterval(env: Map<String, String>): Long = env.strictLong("SYNC_STATUS_INTERVAL_SECONDS", 1L..Long.MAX_VALUE) ?: DEFAULT_STATUS_INTERVAL_SECONDS

/**
 * A page's markup, off the classpath; `/stats.html` serves all three services. Missing means
 * a broken build, so it is an error rather than a blank page.
 */
private fun statusPage(resource: String = "/stats.html"): String =
    SyncStatus::class.java.getResourceAsStream(resource)?.use { it.readBytes().decodeToString() }
        ?: error("$resource is missing from the :web jar — no page can be served.")

/**
 * How long the boot waits for the relay to state its key (compose starts both at once), and how
 * often it asks meanwhile. Often, because the ask is one local HTTP request and everything that
 * writes waits on the answer: at 5 s apart, the sync heard its relay up to 5 s after it was ready.
 */
private const val SELF_KEY_WAIT_SECONDS = 60L
private const val SELF_KEY_POLL_MILLIS = 250L

/** How often [watchRelayKey] asks again once the boot has given up waiting. */
private const val SELF_KEY_WATCH_SECONDS = 30L

/**
 * Stops the boot when the relay at [dial] states a key other than [ours], or none ([keyVerdict]).
 * A relay that never answers is not refused: the writer fails its batches until it does, and those
 * withhold their bands, so nothing is lost by starting. It is watched instead ([watchRelayKey]),
 * and the process stops the moment it answers with the wrong key, as the boot would have.
 */
private fun refuseAnotherRelayKey(
    dial: NormalizedRelayUrl,
    ours: String,
) {
    val deadline = System.nanoTime() + SELF_KEY_WAIT_SECONDS * 1_000_000_000
    while (true) {
        when (val verdict = keyVerdict(relaySelf(dial), dial, ours)) {
            KeyVerdict.Ours -> {
                return
            }

            is KeyVerdict.Refused -> {
                error(verdict.reason)
            }

            KeyVerdict.Pending -> {
                if (System.nanoTime() >= deadline) break
                Thread.sleep(SELF_KEY_POLL_MILLIS)
            }
        }
    }
    System.err.println(
        "sync: the relay at ${dial.url} did not answer its NIP-11 document after ${SELF_KEY_WAIT_SECONDS}s; " +
            "starting anyway — writes fail, and their bands wait, until it answers; its key is checked every ${SELF_KEY_WATCH_SECONDS}s until then",
    )
    watchRelayKey(
        dial,
        ours,
        pollMillis = SELF_KEY_WATCH_SECONDS * 1_000,
        onVerified = { System.err.println("sync: the relay at ${dial.url} now answers, and states this process's key") },
        onRefused = { reason ->
            // What the boot would have refused, reached late: stop, so a supervisor restarts into that refusal.
            System.err.println("sync: STOPPING — $reason")
            exitProcess(1)
        },
    )
}

/**
 * Run the sync engine as its own process against the Vespa the serving relay uses, so the
 * mirror can be restarted or lost to an OOM without the relay dropping a client. Configured
 * entirely from the environment; `docs/configuration.md` documents every variable.
 */
fun main() {
    cacheDnsBriefly()
    BootClock.main("sync")
    exitOnBootFailure()
    val env = System.getenv()
    // Before anything reads or writes a Nostr frame.
    applyQuartzJsonEngine(env)

    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — the served relay's own ws url; mirrored events are stored as its.")
    val relayUrl =
        RelayUrlNormalizer.normalizeOrNull(relayUrlRaw)
            ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")

    val config =
        RouterConfigLoader.fromEnv(env)
            ?: error("SYNC_CONFIG or SYNC_CONFIG_FILE is required — this process only mirrors; without streams it has no job.")

    // Checked before anything slow: a `.onion` upstream with no transport looks like a
    // stream that mirrors nothing.
    val torSettings = TorSettings.fromEnv(env)
    // Before anything is dialled: a monitor that measures nothing must say so, not read as one that is failing.
    RouterConfigLoader.refuseUndeclaredMonitor(config)

    val onion = onionUpstreams(config.streams)
    if (torSettings == null && onion.isNotEmpty()) {
        error(
            "SYNC_TOR_SOCKS is unset, but ${onion.size} configured upstream(s) are hidden services " +
                "(${onion.joinToString()}) — point it at a Tor SOCKS proxy (e.g. tor:9050) or remove them.",
        )
    }

    // Parsed before the schema deploy and its retries, so a bad value refuses the boot at once.
    val providerRefresh = providerRefreshSeconds(env)
    val vespa = vespaEndpoints(env)
    vespa.describe()?.let { System.err.println("sync: the store sends its $it") }

    // Every write goes to the relay, signed in as its key: without one this process cannot write at all.
    val identity =
        RelayIdentity.fromEnv { env[it] }
            ?: error(
                "RELAY_NSEC is required — the relay's own key. Every write this process makes goes to the relay, which takes a " +
                    "socket's writes as the mirror's only when it signs in as that key; it also answers upstream NIP-42 challenges.",
            )
    System.err.println("sync identity: ${identity.pubKey.take(12)}… (NIP-42 auth, NIP-66 monitor, the relay's writer)")

    // Before the store opens, read-only: the relay is the index's only writer.
    val dial = ingestDialFromEnv(env, relayUrl)
    val statusPort = env.strictInt("SYNC_STATUS_PORT", 0..65_535) ?: DEFAULT_STATUS_PORT
    // The monitor's page has its own port: a different question on a different clock.
    val monitorPort = env.strictInt("MONITOR_STATUS_PORT", 0..65_535) ?: DEFAULT_MONITOR_STATUS_PORT
    val statusEverySeconds = statusInterval(env)
    val icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() }
    BootClock.mark("sync: settings read")

    // Beside the boot, and awaited before the writer, the first thing that dials the relay as its
    // key, which is what the check is for. Compose starts the relay with this process, so the
    // answer is the relay's whole boot away, and nothing up to the writer needs it: this process
    // waited it out before opening its store, then bound its pages after both. A refusal still
    // stops the boot the moment it is heard (BootTask).
    val keyChecked =
        BootTask("relay key check") {
            refuseAnotherRelayKey(dial, identity.pubKey)
            BootClock.mark("sync: the relay's key checked")
        }
    // The pages bind beside the boot too, so a boot that stalls anywhere past here is on them. Until
    // the engine exists to publish, they serve the previous run's documents from their state files,
    // or "not computed yet" without one.
    val statusSnapshot = if (statusPort > 0) StatsSnapshot(env["SYNC_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() }).also { it.loadFromFile() } else null
    val monitorSnapshot = if (monitorPort > 0) StatsSnapshot(env["MONITOR_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() }).also { it.loadFromFile() } else null
    val pagesBound =
        BootTask("status pages") {
            val servers =
                Pair(
                    statusSnapshot?.let { serveStatusSite(port = statusPort, page = statusPage(), snapshot = it, icon = icon) },
                    monitorSnapshot?.let { serveStatusSite(port = monitorPort, page = statusPage(), snapshot = it, icon = icon) },
                )
            BootClock.mark("sync: status pages bound")
            servers
        }
    System.err.println("sync: writes go to the relay at ${dial.url}; the store is opened read-only")

    // The relay owns the schema: this process waits for it rather than deploying one (awaitServedSchema).
    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    // Read only to say it is the relay's: a shared .env sets it for both services.
    if (env["AUTO_DEPLOY"] != null) System.err.println("schema: AUTO_DEPLOY is the relay's alone; this process never deploys")
    awaitServedSchema(vespaUrl)
    BootClock.mark("sync: Vespa serves the schema")

    // Read before the store opens: the slow-read ring is a constructor setting.
    val pulseClientDetail = env.strictFlag("SYNC_PULSE_CLIENT_DETAIL") ?: false
    // Off by default, unlike the two status pages: with SYNC_PULSE_CLIENT_DETAIL on this
    // document quotes slow queries and is not public.
    val pulsePort = env.strictInt("SYNC_PULSE_PORT", 0..65_535) ?: 0
    // Refuses the pair: a public pulse must be the operational half only.
    val pulseIsPublic = pulsePublic(env, pulseClientDetail, "SYNC_PULSE_PUBLIC", "SYNC_PULSE_CLIENT_DETAIL")
    // Resolved here so a port set with no administrator named refuses before boot has spent
    // a minute standing everything else up.
    val pulseGuard =
        if (pulsePort <= 0) {
            null
        } else {
            PulseGuard(
                Nip98AdminGate(
                    pulseAdmins(adminPubkeysFromEnv(env), "SYNC_PULSE_PORT", public = pulseIsPublic),
                    pulsePublicUrl(env, "SYNC_PULSE_PUBLIC_URL", pulsePort),
                ),
            )
        }
    val store =
        VespaEventStore.open(
            vespaUrl,
            relay = relayUrl,
            autoDeploy = false,
            configUrl = configUrl,
            feedEndpoints = vespa.feed,
            queryEndpoints = vespa.query,
            // Read-only, so this names the deployment's topology for the store, and writes nothing under it.
            writers = STORE_WRITERS,
            providerRefreshSeconds = providerRefresh,
            slowQueryThresholdMillis = pulseSlowReadMs(env, "SYNC_PULSE_SLOW_READ_MS", pulseClientDetail, "SYNC_PULSE_CLIENT_DETAIL"),
            readOnly = true,
        )
    BootClock.mark("sync: store open (read-only)")
    // When the counters start; the page states every total as cumulative over this window.
    val storeOpenedAt = System.currentTimeMillis()

    val parseAudit = ParseAudit.installFromEnv(env)

    // From the parsed streams too: a stream may set its own `refetchThePastSeconds`.
    val bands = SyncBands.fromEnv(env, config.streams)

    // Written once at boot: a `sync.conf` edit is a restart.
    val manifest = SyncManifest.fromEnv(env)
    // Only the unset case is announced; a failed write has already said so.
    if (manifest.publishes) {
        manifest.write(config.streams)
    } else {
        System.err.println(
            "router: SYNC_MANIFEST_FILE unset — the relay cannot say which kinds this mirror holds, " +
                "so a client comparing our count against an upstream's total will read a complete mirror as partial",
        )
    }

    SyncProgress.refuseRemovedEnv(env)
    val progress = SyncProgress()

    val storeCalls = StoreCalls.fromEnv(env)

    val sweepState = SweepState.fromEnv(env)
    // Nothing walks below a stream's own `since`, nor below the plausible floor without one.
    val refusedFloor = config.streams.mapNotNull { it.filter.since }.fold(SyncCoverage.PLAUSIBLE_FLOOR, ::minOf)
    val refusedIds = RefusedIds.fromEnv(env, refusedFloor)

    // Opt-in: a sync running without a relay has no readers to yield to.
    val pressureUrl = env["SYNC_PRESSURE_URL"]?.trim()?.takeIf { it.isNotEmpty() }
    val servingPressure =
        pressureUrl?.let {
            ServingPressure(
                thresholdMs = env.strictLong("SERVING_PRESSURE_THRESHOLD_MS", 100L..Long.MAX_VALUE) ?: ServingPressure.DEFAULT_THRESHOLD_MS,
            )
        }
    val poller = servingPressure?.let { PressurePoller(pressureUrl, it).start() }
    if (poller == null) {
        System.err.println("router: SYNC_PRESSURE_URL unset — repairs will not yield to relay reads")
    }

    keyChecked.await()
    // Its own scope: the writer outlives the engine's, which is cancelled first at shutdown.
    val writer = RelayWriter(dial, relayUrl, identity, CoroutineScope(Dispatchers.IO + SupervisorJob()))
    val engine =
        SyncEngine(
            store,
            config,
            audit = parseAudit,
            bands = bands,
            sweepState = sweepState,
            refusedIds = refusedIds,
            signer = identity,
            wireLogMode = env.strictChoice("SYNC_WIRE_LOG", setOf("sent", "full")) ?: "",
            servingPressure = servingPressure,
            torSettings = torSettings,
            progress = progress,
            storeCalls = storeCalls,
            knownIds = store.knownIdsProbe(),
            newestVersions = store.newestVersionsProbe(),
            writer = writer,
        )
    // Not started yet: the pages get their first documents, and are bound, first, so a boot that
    // stalls in `start()` is exactly the state they exist to show.
    if (statusPort <= 0) {
        System.err.println("router: SYNC_STATUS_PORT=$statusPort — no status page; what this mirror is doing will be visible only in this log")
    }
    if (monitorPort <= 0) {
        System.err.println("router: MONITOR_STATUS_PORT=$monitorPort — no monitor page; what this router has decided about each relay url will be visible only in the signed records")
    }
    val statusRollup =
        statusSnapshot?.let { snapshot ->
            // `engine::primeUnits`, not a copy: the pool rebuilds the roster on the discovery clock.
            val status = SyncStatus(bands, sweepState, progress, snapshot, statusEverySeconds, engine::primeUnits)
            status.publish()
            StatusRollup("sync", statusEverySeconds, status::publish).start()
        }
    val monitorRollup =
        monitorSnapshot?.let { snapshot ->
            val monitorStatus = engine.monitorStatus(statusEverySeconds, relayUrl.url)

            fun publish() = snapshot.publish(monitorStatus.document())
            publish()
            StatusRollup("monitor", statusEverySeconds, ::publish).start()
        }
    val (statusServer, monitorServer) = pagesBound.await()
    val statusSite = statusRollup?.let { rollup -> statusServer?.let { rollup to it } }
    val monitorSite = monitorRollup?.let { rollup -> monitorServer?.let { rollup to it } }
    if (statusSite != null) {
        println("vespa-sync status page on http://localhost:$statusPort/ (refreshed every ${statusEverySeconds}s)")
    }
    if (monitorSite != null) {
        println("vespa-sync monitor page on http://localhost:$monitorPort/")
    }

    BootClock.mark("sync: status pages published, starting the engine")
    engine.start()
    BootClock.mark("sync: engine started (streams, visits, monitor launched)")

    // Both processes log this, so load can be attributed to a role without scaling one to zero.
    StoreMetricsLog.startLogging("sync", store, env.strictInt("STORE_METRICS_LOG_SECONDS") ?: 300)

    // Administrators only, against the same RELAY_ADMIN_PUBKEYS the relay's NIP-86 RPC
    // uses; a port set with no administrator named stops the boot.
    val pulseSite =
        if (pulseGuard == null) {
            null
        } else {
            servePulseSite(
                port = pulsePort,
                page = statusPage("/pulse.html"),
                guard = pulseGuard,
                public = pulseIsPublic,
                document =
                    PulseDocument.reader(
                        store,
                        startedAtMillis = storeOpenedAt,
                        title = "Eventstore pulse — mirror",
                        scope = "The mirror's own store: what ingest costs, what it admitted, and what the write path is waiting behind.",
                        clientDerived = pulseClientDetail,
                    ),
                icon = icon,
            )
        }
    if (pulseSite != null) {
        println(
            "vespa-sync pulse page on http://localhost:$pulsePort/  [${adminPubkeysFromEnv(env).size} admin key(s)" +
                (if (pulseClientDetail) ", client detail ON" else "") + "]",
        )
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Each step guarded: one that throws must not keep the state files from their last write.
            // Pages before the engine: they read state the engine is about to stop updating.
            shutdownStep("stop the pulse page") { pulseSite?.stop(0, 0) }
            listOfNotNull(statusSite, monitorSite).forEach { (rollup, server) ->
                shutdownStep("stop a status rollup") { rollup.close() }
                shutdownStep("stop a status page") { server.stop(1_000, 2_000) }
            }
            // Engine before the store it writes to; the audit after the engine, so its final
            // report includes the last batch.
            shutdownStep("close the engine") { engine.close() }
            shutdownStep("close the parse audit") { parseAudit?.close() }
            shutdownStep("close the refused ids") { refusedIds.close() }
            shutdownStep("close the pressure poller") { poller?.close() }
            shutdownStep("close the bands") { bands.close() }
            shutdownStep("close the sweep state") { sweepState.close() }
            shutdownStep("close the store") { store.close() }
        },
    )

    println(
        "vespa-sync mirroring ${engine.upstreamCount()} relay(s)" +
            (if (engine.dynamicStreamCount() > 0) " + ${engine.dynamicStreamCount()} dynamic stream(s)" else "") +
            "  (vespa $vespaUrl, as $relayUrl)",
    )
    // Everything runs on the engine's own scopes and daemon threads.
    Thread.currentThread().join()
}
