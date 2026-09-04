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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.relay.config.RelayIdentity
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.config.syncEnv
import com.nosfabrica.vespa.relay.ingest.AddressVersion
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.maintenance.STORE_WRITERS
import com.nosfabrica.vespa.relay.maintenance.deployBundledSchema
import com.nosfabrica.vespa.relay.maintenance.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.onionUpstreams
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.SyncProgress
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.status.StatusRollup
import com.nosfabrica.vespa.relay.status.SyncStatus
import com.nosfabrica.vespa.relay.sync.PressurePoller
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.sync.SyncManifest
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.serveStatusSite
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer

// Enough attempts to outlast the relay's own boot deploy; see main.
private const val DEPLOY_ATTEMPTS = 5
private const val DEPLOY_RETRY_SECONDS = 5L

/** One past the relay's 7777, so the pair is guessable from either end. */
private const val DEFAULT_STATUS_PORT = 7778

private const val DEFAULT_MONITOR_STATUS_PORT = 7779

/** Matched to the mirror's own progress tick; the document is a fold over maps already in memory. */
private const val DEFAULT_STATUS_INTERVAL_SECONDS = 30L

/** `SYNC_STATUS_INTERVAL_SECONDS`, refused rather than defaulted when it is not a number. */
private fun statusInterval(env: Map<String, String>): Long =
    env["SYNC_STATUS_INTERVAL_SECONDS"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull()?.takeIf { n -> n > 0 }
            ?: error("SYNC_STATUS_INTERVAL_SECONDS='$it' is not a positive number of seconds. Unset SYNC_STATUS_PORT to serve no page.")
    } ?: DEFAULT_STATUS_INTERVAL_SECONDS

/**
 * The status page's markup, off the classpath. One page for all three
 * services; each draws whatever document it is pointed at. Missing means a
 * broken build, so it is an error rather than a blank page.
 */
private fun statusPage(): String =
    SyncStatus::class.java.getResourceAsStream("/stats.html")?.use { it.readBytes().decodeToString() }
        ?: error("stats.html is missing from the :web jar — no status page can be served.")

/**
 * Run the sync engine as its own process against the Vespa the serving relay
 * also uses, so the mirror can be restarted, retuned or lost to an OOM without
 * the relay dropping a client.
 *
 * Configuration is entirely from the environment; `docs/configuration.md`
 * documents every variable. `RELAY_URL` and `SYNC_CONFIG`/`SYNC_CONFIG_FILE`
 * are required. Without `SYNC_PRESSURE_URL` ingest never yields to client
 * reads; without `SYNC_TOR_SOCKS` no `.onion` upstream can be reached.
 */
fun main() {
    val env = System.getenv()

    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — the served relay's own ws url; mirrored events are stored as its.")
    val relayUrl =
        RelayUrlNormalizer.normalizeOrNull(relayUrlRaw)
            ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")

    // A sync process with nothing to sync is a misconfiguration, not a mode.
    val config =
        RouterConfigLoader.fromEnv(env)
            ?: error("SYNC_CONFIG or SYNC_CONFIG_FILE is required — this process only mirrors; without streams it has no job.")

    // Read before anything slow: a `.onion` upstream with no transport would
    // time out every cycle and look like a stream that mirrors nothing.
    val torSettings = TorSettings.fromEnv(env)
    val onion = onionUpstreams(config.streams)
    if (torSettings == null && onion.isNotEmpty()) {
        error(
            "SYNC_TOR_SOCKS is unset, but ${onion.size} configured upstream(s) are hidden services " +
                "(${onion.joinToString()}) — point it at a Tor SOCKS proxy (e.g. tor:9050) or remove them.",
        )
    }

    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("sync identity: ${identity.pubKey.take(12)}… (NIP-42 auth, NIP-66 monitor)")
    } else {
        // Said out loud: an auth-gated upstream served without a signer looks
        // exactly like an empty relay.
        System.err.println(
            "sync identity: none (RELAY_NSEC unset) — upstream NIP-42 challenges go unanswered, so relays that " +
                "gate reads behind AUTH will serve nothing and read as empty",
        )
    }

    // This is the process whose writes a drifted schema silently discards, so
    // it deploys on boot too. A no-change deploy is a cheap no-op.
    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    if (env["AUTO_DEPLOY"]?.toBooleanStrictOrNull() != false) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        // Compose starts both processes together, so two deploys can race the
        // same config server session; on a fresh Vespa the loser has nothing
        // to fall back to. The race is transient, so it is retried here.
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
    }

    // STORE_WRITERS: the relay's inserts must be checked against the
    // tombstones this process stores, which its own store instance never saw.
    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = false, configUrl = configUrl, writers = STORE_WRITERS)

    val parseAudit = ParseAudit.installFromEnv(env)

    // Built from the parsed streams, not the environment alone: a stream may
    // set its own `refetchThePastSeconds`.
    val bands = SyncBands.fromEnv(env, config.streams)

    // Written once, after the config is loaded and before anything dials: a
    // `router.conf` edit is a restart, so boot is the only moment it changes.
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

    // The registry is always on; the environment only decides the log threshold.
    val storeCalls = StoreCalls.fromEnv(env)

    val sweepState = SweepState.fromEnv(env)
    val refusedIds = RefusedIds.fromEnv(env)

    // Opt-in: a sync running without a relay has no readers to yield to, and
    // the boot log says when it is off.
    val pressureUrl = env.syncEnv("SYNC_PRESSURE_URL", "ROUTER_PRESSURE_URL")?.trim()?.takeIf { it.isNotEmpty() }
    val servingPressure =
        pressureUrl?.let {
            ServingPressure(
                thresholdMs =
                    env["SERVING_PRESSURE_THRESHOLD_MS"]?.trim()?.toLongOrNull()?.coerceAtLeast(100)
                        ?: ServingPressure.DEFAULT_THRESHOLD_MS,
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
            wireLogMode = env.syncEnv("SYNC_WIRE_LOG", "ROUTER_WIRE_LOG")?.trim()?.lowercase() ?: "",
            servingPressure = servingPressure,
            torSettings = torSettings,
            progress = progress,
            storeCalls = storeCalls,
            // The raw engine index: a pure membership read, which is what
            // ingest needs to skip verifying an event it cannot write.
            knownIds = store.eventIndex::existingIds,
            // Built here so the store's query types stay at the one seam that
            // knows them. The winner rule is stage D's: newest created_at,
            // ties to the lower id.
            newestVersions = { kind, authors ->
                store.eventIndex
                    .search(EventQuery(kinds = listOf(kind), authors = authors))
                    .groupBy { it.pubkey }
                    .mapValues { (_, docs) ->
                        docs.maxWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id }).let { AddressVersion(it.createdAt, it.id) }
                    }
            },
        )
    // Not started yet: both status sites bind first. Everything they read
    // exists once the engine is constructed, and a boot that stalls in
    // `start()` is exactly the state they exist to show.

    // Unset disables the page; a fill-only box should not bind a port.
    val statusPort =
        env["SYNC_STATUS_PORT"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
            it.toIntOrNull() ?: error("SYNC_STATUS_PORT='$it' is not a port number. Unset it to serve no status page.")
        } ?: DEFAULT_STATUS_PORT
    // The monitor's page has its own port: it answers a different question on
    // a different clock, and the port survives the plane moving to its own
    // container.
    val monitorPort =
        env["MONITOR_STATUS_PORT"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
            it.toIntOrNull() ?: error("MONITOR_STATUS_PORT='$it' is not a port number. Set it to 0 to serve no monitor page.")
        } ?: DEFAULT_MONITOR_STATUS_PORT

    val statusSite =
        if (statusPort <= 0) {
            System.err.println("router: SYNC_STATUS_PORT=$statusPort — no status page; what this mirror is doing will be visible only in this log")
            null
        } else {
            val statusSnapshot = StatsSnapshot(env["SYNC_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() })
            val everySeconds = statusInterval(env)
            // `engine::primeUnits`, not a copy: the pool rebuilds the roster on
            // the discovery clock.
            val status = SyncStatus(bands, sweepState, progress, statusSnapshot, everySeconds, engine::primeUnits)
            // Once before the server binds, so the first request answers with
            // a document rather than a 503.
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

    // With both pages answering, whatever `start()` waits on is visible.
    engine.start()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Pages before the engine: they read state the engine is about to
            // stop updating.
            listOfNotNull(statusSite, monitorSite).forEach { (rollup, server) ->
                rollup.close()
                server.stop(1_000, 2_000)
            }
            // Engine before the store it writes to; the audit after the
            // engine, so its final report includes the last batch.
            engine.close()
            parseAudit?.close()
            refusedIds.close()
            poller?.close()
            bands.close()
            sweepState.close()
            store.close()
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
