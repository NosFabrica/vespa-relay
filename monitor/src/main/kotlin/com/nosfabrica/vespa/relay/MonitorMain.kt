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
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.MonitorConfig
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.identity.RelayIdentity
import com.nosfabrica.vespa.relay.ingest.RelayWriter
import com.nosfabrica.vespa.relay.ingest.refuseAnotherRelayKey
import com.nosfabrica.vespa.relay.ingest.relayDialFromEnv
import com.nosfabrica.vespa.relay.monitor.MonitorEngine
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.pulse.StoreMetricsLog
import com.nosfabrica.vespa.relay.store.ReadOnlyStore
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
import com.nosfabrica.vespa.relay.util.watchForFatals
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.bundledPage
import com.nosfabrica.vespa.relay.web.serveStatusSite
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MessageDecoder
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.exitProcess

/** Every line this process prints starts with it; `router:` is the sync's. */
private const val LOG = "monitor"

/** The monitor's own status page; `/stats.html` serves every plane. */
private const val DEFAULT_STATUS_PORT = 7779

/** Matched to the mirror's status tick. */
private const val DEFAULT_STATUS_INTERVAL_SECONDS = 30L

/** How often the health lines print. */
private const val HEALTH_INTERVAL_MS = 60_000L

/** The relay websocket the verdicts go to; defaults to `RELAY_URL`. */
private const val MONITOR_INGEST_URL_ENV = "MONITOR_INGEST_URL"

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
    RouterConfigLoader.refuseSyncEnv(env)
    exitIfUnconfigured(env)
    val boot = readSettings(env)
    BootClock.mark("monitor: settings read")

    // Beside the boot, as in the sync: a refusal stops it the moment it is heard.
    val keyChecked =
        BootTask("relay key check") {
            refuseAnotherRelayKey(boot.dial, boot.identity.pubKey, process = "monitor") { exitProcess(1) }
            BootClock.mark("monitor: the relay's key checked")
        }
    // In memory only: the document is rebuilt from what this process holds within one status interval of boot.
    val snapshot = if (boot.statusPort > 0) StatsSnapshot(null) else null
    val pageBound = startStatusPage(boot, snapshot)
    System.err.println("monitor: verdicts go to the relay at ${boot.dial.url}; the store is opened read-only")

    val configUrl = ReadOnlyStore.awaitSchema(env, boot.vespaUrl, boot.storeTuning, process = "monitor")
    val store = ReadOnlyStore.open(env, boot.vespaUrl, boot.relayUrl, configUrl, boot.storeTuning, process = "monitor")

    val storeCalls = StoreCalls(logPrefix = LOG)
    // From here on a pass runs on pool threads; one killed by an OutOfMemoryError must say so.
    val fatals = AtomicLong().also { watchForFatals("monitor", it) }
    keyChecked.await()
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob() + storeCalls)
    // Its own scope: the writer outlives the engine's, which is cancelled first at shutdown.
    val writer = RelayWriter(boot.dial, boot.relayUrl, boot.identity, CoroutineScope(Dispatchers.IO + SupervisorJob()), process = "monitor")
    val peers = PeerClient(scope, boot.identity, boot.torSettings, boot.wireLogMode, boot.config.connectionTimeoutSec, MessageDecoder.Default, logPrefix = LOG)
    val engine =
        MonitorEngine(
            store = store,
            settings = boot.config,
            sources = boot.sources,
            connectionTimeoutMs = boot.config.connectionTimeoutSec * 1000L,
            peers = peers,
            signer = boot.identity,
            processors = Processors(),
            sockets = RelaySockets(peers.client, pinnedUrls = emptySet(), logPrefix = LOG),
            scope = scope,
            writer = writer,
        )

    if (boot.statusPort <= 0) {
        System.err.println("monitor: MONITOR_STATUS_PORT=${boot.statusPort} — no status page; what this monitor decided is visible only in the signed records")
    }
    val rollup = snapshot?.let { startRollup(boot, engine, storeCalls, it) }
    val server = pageBound.await()
    if (rollup != null && server != null) println("vespa-monitor status page on http://localhost:${boot.statusPort}/ (refreshed every ${boot.statusEverySeconds}s)")

    BootClock.mark("monitor: status page published, starting the engine")
    peers.announceTor()
    peers.connect()
    if (boot.sources == null) System.err.println("monitor: `sources = []` — this monitor measures nothing on purpose; stale verdicts are still retired")
    engine.start()
    BootClock.mark("monitor: engine started")
    startHealthLine(scope, fatals, storeCalls, engine)
    StoreMetricsLog.startLogging("monitor", store, boot.storeMetricsLogSeconds)
    installShutdownHook(rollup, { server?.stop(1_000, 2_000) }, scope, peers, writer, store)

    println("vespa-monitor measuring ${boot.sources?.sources?.size ?: 0} declared source(s)  (vespa ${boot.vespaUrl}, verdicts to ${boot.dial.url})")
    Thread.currentThread().join()
}

/** No monitor.conf is a deployment that measures nothing: said once, and a clean exit a supervisor leaves down. */
private fun exitIfUnconfigured(env: Map<String, String>) {
    if (!env["MONITOR_CONFIG"].isNullOrBlank() || !env["MONITOR_CONFIG_FILE"].isNullOrBlank()) return
    println(
        "vespa-monitor: neither MONITOR_CONFIG nor MONITOR_CONFIG_FILE is set, so this deployment measures nothing and the " +
            "monitor exits. Point MONITOR_CONFIG_FILE at a monitor.conf (MONITOR_CONFIG_LOCAL under compose) to measure.",
    )
    exitProcess(0)
}

/** Everything the process reads from its environment, parsed before anything touches the network. */
private class MonitorSettings(
    val vespaUrl: String,
    val relayUrl: NormalizedRelayUrl,
    val storeTuning: EngineTuning,
    val config: MonitorConfig,
    val sources: RelayDiscoveryConfig?,
    val torSettings: TorSettings?,
    val identity: NostrSignerInternal,
    /** The relay websocket the verdicts go to. */
    val dial: NormalizedRelayUrl,
    val statusPort: Int,
    val statusEverySeconds: Long,
    val wireLogMode: String,
    val icon: String?,
    val storeMetricsLogSeconds: Int,
)

/** Reads and checks every setting; a bad value stops the boot here. */
private fun readSettings(env: Map<String, String>): MonitorSettings {
    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — the served relay's own ws url, which its NIP-42 check reads.")
    val relayUrl = RelayUrlNormalizer.normalizeOrNull(relayUrlRaw) ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")

    applyQuartzLogLevel(env)
    val storeTuning = EngineTuning.fromEnv(env)
    val config = RouterConfigLoader.monitorFromEnv(env)
    val torSettings = TorSettings.fromEnv(env)
    // Parsed before the schema wait, so a bad value refuses the boot at once.
    providerRefreshSeconds(env)
    vespaEndpoints(env).describe()?.let { System.err.println("monitor: the store sends its $it") }

    val identity =
        RelayIdentity.fromEnv { env[it] }
            ?: error(
                "RELAY_NSEC is required — every verdict is signed under it, and the relay takes a socket's writes as its own " +
                    "only when it signs in as that key; it also answers the NIP-42 challenges of the relays measured.",
            )
    System.err.println("monitor identity: ${identity.pubKey.take(12)}… (NIP-66 verdicts, NIP-42 auth, the relay's writer)")
    env["MONITOR_STATUS_FILE"]?.takeIf { it.isNotBlank() }?.let {
        System.err.println("monitor: MONITOR_STATUS_FILE was removed and does nothing — the status page is rebuilt within one interval of boot")
    }
    return MonitorSettings(
        vespaUrl = vespaUrl,
        relayUrl = relayUrl,
        storeTuning = storeTuning,
        config = config,
        sources = config.asDiscovery(),
        torSettings = torSettings,
        identity = identity,
        dial = relayDialFromEnv(env, relayUrl, MONITOR_INGEST_URL_ENV),
        statusPort = env.strictInt("MONITOR_STATUS_PORT", 0..65_535) ?: DEFAULT_STATUS_PORT,
        statusEverySeconds = env.strictLong("MONITOR_STATUS_INTERVAL_SECONDS", 1L..Long.MAX_VALUE) ?: DEFAULT_STATUS_INTERVAL_SECONDS,
        wireLogMode = env.strictChoice("MONITOR_WIRE_LOG", setOf("sent", "full")) ?: "",
        icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
        storeMetricsLogSeconds = env.strictInt("STORE_METRICS_LOG_SECONDS") ?: 300,
    )
}

/** Bind the status page beside the boot; the task answers null when there is no port. */
private fun startStatusPage(
    boot: MonitorSettings,
    snapshot: StatsSnapshot?,
) = BootTask("status page") {
    snapshot?.let { serveStatusSite(port = boot.statusPort, page = bundledPage(), snapshot = it, icon = boot.icon) }.also {
        BootClock.mark("monitor: status page bound")
    }
}

/** The engine's status document into [snapshot] now, and again on the status tick. */
private fun startRollup(
    boot: MonitorSettings,
    engine: MonitorEngine,
    storeCalls: StoreCalls,
    snapshot: StatsSnapshot,
): StatusRollup {
    val status = engine.status(boot.statusEverySeconds, boot.relayUrl.url, storeCalls)

    fun publish() = snapshot.publish(status.document())
    publish()
    return StatusRollup("monitor", boot.statusEverySeconds, ::publish, logPrefix = LOG).start()
}

/** The health line, once a minute: heap, fatals, slow store calls and the urls held out as dead. */
private fun startHealthLine(
    scope: CoroutineScope,
    fatals: AtomicLong,
    storeCalls: StoreCalls,
    engine: MonitorEngine,
) {
    scope.launch {
        repeatEvery(HEALTH_INTERVAL_MS, "health", LOG) {
            val rt = Runtime.getRuntime()
            val fatal = fatals.get()
            System.err.println(
                "monitor: health heap ${(rt.totalMemory() - rt.freeMemory()) / 1_048_576}/${rt.maxMemory() / 1_048_576}MB" +
                    (if (fatal > 0) " · $fatal FATAL error(s): degraded, restart it" else ""),
            )
            storeCalls.warnSlow().forEach { System.err.println(it) }
            engine.heldOutDead().takeIf { it > 0 }?.let { dead ->
                System.err.println("monitor: health $dead relay(s) carry a current `dead` verdict of ours and are held out of the probe passes")
            }
        }
    }
}

/** Each step guarded: one that throws must not keep the next from running. */
private fun installShutdownHook(
    rollup: StatusRollup?,
    stopPage: () -> Unit,
    scope: CoroutineScope,
    peers: PeerClient,
    writer: RelayWriter,
    store: VespaEventStore,
) {
    Runtime.getRuntime().addShutdownHook(
        Thread {
            shutdownStep("stop the status rollup", LOG) { rollup?.close() }
            shutdownStep("stop the status page", LOG) { stopPage() }
            shutdownStep("cancel the passes", LOG) { scope.cancel() }
            shutdownStep("close the peer client", LOG) { peers.close() }
            // After the passes: a verdict mid-write may still be on its way to the relay.
            shutdownStep("close the writer", LOG) { writer.close() }
            shutdownStep("close the store", LOG) { store.close() }
        },
    )
}
