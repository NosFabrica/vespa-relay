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

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.SchemaDeploy
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.identity.RelayIdentity
import com.nosfabrica.vespa.relay.ingest.RelayWriter
import com.nosfabrica.vespa.relay.ingest.refuseAnotherRelayKey
import com.nosfabrica.vespa.relay.ingest.relayDialFromEnv
import com.nosfabrica.vespa.relay.monitor.MonitorEngine
import com.nosfabrica.vespa.relay.monitor.MonitorStatus
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.pulse.StoreMetricsLog
import com.nosfabrica.vespa.relay.store.STORE_WRITERS
import com.nosfabrica.vespa.relay.store.providerRefreshSeconds
import com.nosfabrica.vespa.relay.store.vespaEndpoints
import com.nosfabrica.vespa.relay.util.BootClock
import com.nosfabrica.vespa.relay.util.BootTask
import com.nosfabrica.vespa.relay.util.StatusRollup
import com.nosfabrica.vespa.relay.util.applyQuartzLogLevel
import com.nosfabrica.vespa.relay.util.cacheDnsBriefly
import com.nosfabrica.vespa.relay.util.exitOnBootFailure
import com.nosfabrica.vespa.relay.util.loadTimeZoneRules
import com.nosfabrica.vespa.relay.util.repeatEvery
import com.nosfabrica.vespa.relay.util.shutdownStep
import com.nosfabrica.vespa.relay.util.strictChoice
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.serveStatusSite
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MessageDecoder
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlin.system.exitProcess

/** The monitor's own status page; `/stats.html` serves every plane. */
private const val DEFAULT_STATUS_PORT = 7779

/** Matched to the mirror's status tick. */
private const val DEFAULT_STATUS_INTERVAL_SECONDS = 30L

/** How often the health lines print. */
private const val HEALTH_INTERVAL_MS = 60_000L

/** The relay websocket the verdicts go to; defaults to `RELAY_URL`. */
private const val MONITOR_INGEST_URL_ENV = "MONITOR_INGEST_URL"

/** The page's markup, off the classpath. Missing means a broken build, so it is an error rather than a blank page. */
private fun statusPage(): String =
    MonitorStatus::class.java.getResourceAsStream("/stats.html")?.use { it.readBytes().decodeToString() }
        ?: error("/stats.html is missing from the :web jar — no page can be served.")

/**
 * Run the monitor as its own process: it reads the store, dials the relays its own config names,
 * and signs kind-30166 verdicts that it writes through the relay. The mirror reads them back from
 * the store and knows nothing else about it. Configured from the environment and `monitor.conf`.
 */
fun main() {
    cacheDnsBriefly()
    loadTimeZoneRules()
    BootClock.main("monitor")
    exitOnBootFailure()
    val env = System.getenv()
    // The mirror's config, aimed here, would configure nothing: said and stopped, as the sync does for ours.
    val syncSet = listOf("SYNC_CONFIG", "SYNC_CONFIG_FILE").filter { !env[it].isNullOrBlank() }
    require(syncSet.isEmpty()) {
        "${syncSet.joinToString()} set on the monitor process, which does not mirror. The mirror is its own process " +
            "(vespa-sync, the `sync` compose service): move these to it and unset them here"
    }
    // No monitor.conf is a deployment that measures nothing: said once, and a clean exit a supervisor leaves down.
    if (env["MONITOR_CONFIG"].isNullOrBlank() && env["MONITOR_CONFIG_FILE"].isNullOrBlank()) {
        println(
            "vespa-monitor: neither MONITOR_CONFIG nor MONITOR_CONFIG_FILE is set, so this deployment measures nothing and the " +
                "monitor exits. Point MONITOR_CONFIG_FILE at a monitor.conf (MONITOR_CONFIG_LOCAL under compose) to measure.",
        )
        exitProcess(0)
    }

    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — the served relay's own ws url, which its NIP-42 check reads.")
    val relayUrl = RelayUrlNormalizer.normalizeOrNull(relayUrlRaw) ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")

    applyQuartzLogLevel(env)
    val storeTuning = EngineTuning.fromEnv(env)
    val config = RouterConfigLoader.monitorFromEnv(env)
    val sources = config.asDiscovery()
    val torSettings = TorSettings.fromEnv(env)
    val providerRefresh = providerRefreshSeconds(env)
    val vespa = vespaEndpoints(env)
    vespa.describe()?.let { System.err.println("monitor: the store sends its $it") }

    val identity =
        RelayIdentity.fromEnv { env[it] }
            ?: error(
                "RELAY_NSEC is required — every verdict is signed under it, and the relay takes a socket's writes as its own " +
                    "only when it signs in as that key; it also answers the NIP-42 challenges of the relays measured.",
            )
    System.err.println("monitor identity: ${identity.pubKey.take(12)}… (NIP-66 verdicts, NIP-42 auth, the relay's writer)")
    val dial = relayDialFromEnv(env, relayUrl, MONITOR_INGEST_URL_ENV)
    val statusPort = env.strictInt("MONITOR_STATUS_PORT", 0..65_535) ?: DEFAULT_STATUS_PORT
    val statusEverySeconds = env.strictLong("MONITOR_STATUS_INTERVAL_SECONDS", 1L..Long.MAX_VALUE) ?: DEFAULT_STATUS_INTERVAL_SECONDS
    val wireLogMode = env.strictChoice("MONITOR_WIRE_LOG", setOf("sent", "full")) ?: ""
    val icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() }
    BootClock.mark("monitor: settings read")

    // Beside the boot, as in the sync: a refusal stops it the moment it is heard.
    val keyChecked =
        BootTask("relay key check") {
            refuseAnotherRelayKey(dial, identity.pubKey, process = "monitor")
            BootClock.mark("monitor: the relay's key checked")
        }
    val snapshot = if (statusPort > 0) StatsSnapshot(env["MONITOR_STATUS_FILE"]?.trim()?.takeIf { it.isNotEmpty() }).also { it.loadFromFile() } else null
    val pageBound =
        BootTask("status page") {
            snapshot?.let { serveStatusSite(port = statusPort, page = statusPage(), snapshot = it, icon = icon) }.also {
                BootClock.mark("monitor: status page bound")
            }
        }
    System.err.println("monitor: verdicts go to the relay at ${dial.url}; the store is opened read-only")

    val configUrl = env["VESPA_CONFIG_URL"] ?: VespaEventStore.deriveConfigUrl(vespaUrl)
    System.err.println("schema: waiting until an application serves at $vespaUrl — the relay deploys it (up to 10 min)")
    VespaEventStore.deploySchema(vespaUrl, configUrl, SchemaDeploy.AWAIT, storeTuning)
    BootClock.mark("monitor: Vespa serves the schema")
    val store =
        VespaEventStore.open(
            vespaUrl,
            relay = relayUrl,
            schema = SchemaDeploy.AWAIT,
            configUrl = configUrl,
            feedEndpoints = vespa.feed,
            queryEndpoints = vespa.query,
            writers = STORE_WRITERS,
            providerRefreshSeconds = providerRefresh,
            readOnly = true,
            tuning = storeTuning,
        )
    BootClock.mark("monitor: store open (read-only)")

    val storeCalls = StoreCalls()
    keyChecked.await()
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob() + storeCalls)
    // Its own scope: the writer outlives the engine's, which is cancelled first at shutdown.
    val writer = RelayWriter(dial, relayUrl, identity, CoroutineScope(Dispatchers.IO + SupervisorJob()), process = "monitor")
    val peers = PeerClient(scope, identity, torSettings, wireLogMode, config.connectionTimeoutSec, MessageDecoder.Default)
    val engine =
        MonitorEngine(
            store = store,
            settings = config,
            sources = sources,
            connectionTimeoutMs = config.connectionTimeoutSec * 1000L,
            peers = peers,
            signer = identity,
            processors = Processors(),
            // No mirror in this process: no socket to share and no stream to hand a probe's events to.
            sockets = RelaySockets(peers.client, emptySet()),
            onProbeEvent = {},
            pinnedUrls = emptySet(),
            scope = scope,
            writer = writer,
        )

    if (statusPort <= 0) {
        System.err.println("monitor: MONITOR_STATUS_PORT=$statusPort — no status page; what this monitor decided is visible only in the signed records")
    }
    val rollup =
        snapshot?.let { snap ->
            val status = engine.status(statusEverySeconds, relayUrl.url)

            fun publish() = snap.publish(status.document())
            publish()
            StatusRollup("monitor", statusEverySeconds, ::publish).start()
        }
    val server = pageBound.await()
    if (rollup != null && server != null) println("vespa-monitor status page on http://localhost:$statusPort/ (refreshed every ${statusEverySeconds}s)")

    BootClock.mark("monitor: status page published, starting the engine")
    peers.announceTor()
    peers.connect()
    if (sources == null) System.err.println("monitor: `sources = []` — this monitor measures nothing on purpose; stale verdicts are still retired")
    engine.start()
    BootClock.mark("monitor: engine started")
    scope.launch {
        repeatEvery(HEALTH_INTERVAL_MS, "health") {
            storeCalls.warnSlow().forEach { System.err.println(it) }
            engine.heldOutDead().takeIf { it > 0 }?.let { dead ->
                System.err.println("router: health $dead relay(s) carry a current `dead` verdict of ours and are held out of the probe passes")
            }
        }
    }
    StoreMetricsLog.startLogging("monitor", store, env.strictInt("STORE_METRICS_LOG_SECONDS") ?: 300)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Each step guarded: one that throws must not keep the next from running.
            shutdownStep("stop the status rollup") { rollup?.close() }
            shutdownStep("stop the status page") { server?.stop(1_000, 2_000) }
            shutdownStep("cancel the passes") { scope.cancel() }
            shutdownStep("close the peer client") { peers.close() }
            // After the passes: a verdict mid-write may still be on its way to the relay.
            shutdownStep("close the writer") { writer.close() }
            shutdownStep("close the store") { store.close() }
        },
    )

    println("vespa-monitor measuring ${sources?.sources?.size ?: 0} declared source(s)  (vespa $vespaUrl, verdicts to ${dial.url})")
    Thread.currentThread().join()
}
