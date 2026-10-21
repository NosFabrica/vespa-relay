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
import com.nosfabrica.vespa.relay.ingest.StoreWriter
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
import com.nosfabrica.vespa.relay.store.deployBundledSchema
import com.nosfabrica.vespa.relay.store.feedBatch
import com.nosfabrica.vespa.relay.store.providerRefreshSeconds
import com.nosfabrica.vespa.relay.store.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.store.vespaEndpoints
import com.nosfabrica.vespa.relay.sync.PressurePoller
import com.nosfabrica.vespa.relay.sync.RelaySelf
import com.nosfabrica.vespa.relay.sync.SYNC_INGEST_ENV
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.sync.SyncEngine
import com.nosfabrica.vespa.relay.sync.SyncManifest
import com.nosfabrica.vespa.relay.sync.ingestTargetFromEnv
import com.nosfabrica.vespa.relay.sync.relaySelf
import com.nosfabrica.vespa.relay.sync.shutdownStep
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

// Enough attempts to outlast the relay's own boot deploy.
private const val DEPLOY_ATTEMPTS = 5
private const val DEPLOY_RETRY_SECONDS = 5L

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

/** How long the boot waits for the relay to state its key: compose starts both at once. */
private const val SELF_KEY_ATTEMPTS = 12
private const val SELF_KEY_RETRY_SECONDS = 5L

/**
 * Stops the boot when the relay at [dial] states a key other than [ours], or none: a relay without
 * `RELAY_NSEC` runs no mirror lane and no `MirrorDelete` hook, so the audit's deletes would be
 * answered OK and do nothing. A relay that never answers is only a warning: the writer fails its
 * batches until it does, and those withhold their bands, so nothing is lost by starting.
 */
private fun refuseAnotherRelayKey(
    dial: NormalizedRelayUrl,
    ours: String,
) {
    repeat(SELF_KEY_ATTEMPTS) { attempt ->
        when (val self = relaySelf(dial)) {
            is RelaySelf.Key -> {
                if (self.pubkey != ours) {
                    error(
                        "the relay at ${dial.url} is ${self.pubkey}, but this process's RELAY_NSEC is $ours. The relay writes a socket's " +
                            "events as the mirror's only when it signs in as the relay's key, so both processes need the same RELAY_NSEC.",
                    )
                }
                return
            }

            RelaySelf.NoKey -> {
                error(
                    "the relay at ${dial.url} states no key (NIP-11 `self`): it has no RELAY_NSEC, so it runs no mirror lane and no " +
                        "delete hook. Give it the same RELAY_NSEC as this process, or unset $SYNC_INGEST_ENV to write the store from here.",
                )
            }

            RelaySelf.Unreachable -> {
                if (attempt < SELF_KEY_ATTEMPTS - 1) Thread.sleep(SELF_KEY_RETRY_SECONDS * 1_000)
            }
        }
    }
    System.err.println(
        "sync: the relay at ${dial.url} did not answer its NIP-11 document after ${SELF_KEY_ATTEMPTS * SELF_KEY_RETRY_SECONDS}s; " +
            "starting anyway — writes fail, and their bands wait, until it answers",
    )
}

/**
 * Run the sync engine as its own process against the Vespa the serving relay uses, so the
 * mirror can be restarted or lost to an OOM without the relay dropping a client. Configured
 * entirely from the environment; `docs/configuration.md` documents every variable.
 */
fun main() {
    exitOnBootFailure()
    val env = System.getenv()

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
    // The store reads it; checked here so a typo stops the boot instead of quietly turning batching off.
    feedBatch(env)?.let { n ->
        System.err.println("sync: the store writes " + (if (n > 1) "$n documents per request through the batch handler" else "one document per request (VESPA_FEED_BATCH=$n)"))
    }
    val vespa = vespaEndpoints(env)
    vespa.describe()?.let { System.err.println("sync: the store sends its $it") }

    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("sync identity: ${identity.pubKey.take(12)}… (NIP-42 auth, NIP-66 monitor)")
    } else {
        System.err.println(
            "sync identity: none (RELAY_NSEC unset) — upstream NIP-42 challenges go unanswered, so relays that " +
                "gate reads behind AUTH will serve nothing and read as empty",
        )
    }

    // Before the store opens, which it does read-only when the relay is the writer.
    val target = ingestTargetFromEnv(env, relayUrl)
    target.dial?.let { dial ->
        if (identity == null) {
            error(
                "$SYNC_INGEST_ENV=relay needs RELAY_NSEC, the relay's own key: the relay takes a socket's writes as the " +
                    "mirror's only when it signs in as that key. Set it, or unset $SYNC_INGEST_ENV to write the store from here.",
            )
        }
        refuseAnotherRelayKey(dial, identity.pubKey)
        System.err.println("sync: writes go to the relay at ${dial.url} ($SYNC_INGEST_ENV=relay); the store is opened read-only")
    }

    // This is the process whose writes a drifted schema silently discards, so it deploys too.
    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    if (env.strictFlag("AUTO_DEPLOY") != false) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        // Compose starts both processes together, so two deploys can race one config server
        // session; the loser retries.
        var attempt = 1
        while (true) {
            try {
                deployBundledSchema(vespaUrl, configUrl)
                break
            } catch (e: Exception) {
                if (attempt >= DEPLOY_ATTEMPTS) throw e
                System.err.println(
                    "schema: deploy attempt $attempt/$DEPLOY_ATTEMPTS failed (${e.message?.take(160)}); " +
                        "retrying in ${DEPLOY_RETRY_SECONDS}s — likely racing the relay's own boot deploy",
                )
                Thread.sleep(DEPLOY_RETRY_SECONDS * 1_000)
                attempt++
            }
        }
        System.err.println("schema: deployed and serving")
    } else {
        System.err.println("schema: AUTO_DEPLOY=false — not deploying; Vespa serves the application package it already has")
    }

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
            // The relay's inserts are checked against tombstones this process stores.
            writers = STORE_WRITERS,
            providerRefreshSeconds = providerRefresh,
            slowQueryThresholdMillis = pulseSlowReadMs(env, "SYNC_PULSE_SLOW_READ_MS", pulseClientDetail, "SYNC_PULSE_CLIENT_DETAIL"),
            readOnly = target.viaRelay,
        )
    val writer =
        target.dial?.let { dial ->
            // Its own scope: the writer outlives the engine's, which is cancelled first at shutdown.
            RelayWriter(dial, relayUrl, identity!!, CoroutineScope(Dispatchers.IO + SupervisorJob()))
        } ?: StoreWriter.local(store)
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
        System.err.println("router: SYNC_PRESSURE_URL unset — ingest will not yield to relay reads")
    }

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
            ingestPressure = target.ingestPressure(servingPressure),
            torSettings = torSettings,
            progress = progress,
            storeCalls = storeCalls,
            knownIds = store.knownIdsProbe(),
            newestVersions = store.newestVersionsProbe(),
            writer = writer,
        )
    // Not started yet: both status sites bind first, so a boot that stalls in `start()` is
    // exactly the state they exist to show.

    val statusPort = env.strictInt("SYNC_STATUS_PORT", 0..65_535) ?: DEFAULT_STATUS_PORT
    // The monitor's page has its own port: a different question on a different clock.
    val monitorPort = env.strictInt("MONITOR_STATUS_PORT", 0..65_535) ?: DEFAULT_MONITOR_STATUS_PORT
    val statusSite =
        if (statusPort <= 0) {
            System.err.println("router: SYNC_STATUS_PORT=$statusPort — no status page; what this mirror is doing will be visible only in this log")
            null
        } else {
            val statusSnapshot = StatsSnapshot(env["SYNC_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() })
            val everySeconds = statusInterval(env)
            // `engine::primeUnits`, not a copy: the pool rebuilds the roster on the discovery clock.
            val status = SyncStatus(bands, sweepState, progress, statusSnapshot, everySeconds, engine::primeUnits)
            // Once before the server binds, so the first request gets a document, not a 503.
            status.publish()
            StatusRollup("sync", everySeconds, status::publish).start() to
                serveStatusSite(
                    port = statusPort,
                    page = statusPage(),
                    snapshot = statusSnapshot,
                    icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
                )
        }
    if (statusSite != null) {
        println("vespa-sync status page on http://localhost:$statusPort/ (refreshed every ${statusInterval(env)}s)")
    }

    val monitorSite =
        if (monitorPort <= 0) {
            System.err.println("router: MONITOR_STATUS_PORT=$monitorPort — no monitor page; what this router has decided about each relay url will be visible only in the signed records")
            null
        } else {
            val monitorSnapshot = StatsSnapshot(env["MONITOR_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() })
            val everySeconds = statusInterval(env)
            val monitorStatus = engine.monitorStatus(everySeconds, relayUrl.url)

            fun publish() = monitorSnapshot.publish(monitorStatus.document())
            publish()
            StatusRollup("monitor", everySeconds, ::publish).start() to
                serveStatusSite(
                    port = monitorPort,
                    page = statusPage(),
                    snapshot = monitorSnapshot,
                    icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
                )
        }
    if (monitorSite != null) {
        println("vespa-sync monitor page on http://localhost:$monitorPort/")
    }

    engine.start()

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
                icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
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
