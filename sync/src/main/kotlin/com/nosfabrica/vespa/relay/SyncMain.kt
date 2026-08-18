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
import com.nosfabrica.vespa.relay.monitor.MonitorStatus
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.onionUpstreams
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

// The deploy-race retry (see main): enough attempts to outlast the relay's
// own boot deploy at a pace that stays visible in the log.
private const val DEPLOY_ATTEMPTS = 5
private const val DEPLOY_RETRY_SECONDS = 5L

/**
 * Where the status page binds when nothing says otherwise.
 *
 * One past the relay's 7777, so the pair is guessable from either end. It has a
 * default at all — unlike the audits, which deliberately have none — because
 * this costs a port and nothing else: a page nobody opens is a page nobody
 * opens, whereas an unset audit period would quietly re-download a corpus.
 */
private const val DEFAULT_STATUS_PORT = 7778

/**
 * …and the monitor's, one past it. Same reasoning: the pair is guessable from
 * either end, and a page nobody opens costs a port and nothing else.
 */
private const val DEFAULT_MONITOR_STATUS_PORT = 7779

/**
 * How often the status document is rebuilt.
 *
 * Thirty seconds, matched to the mirror's own progress tick: the document is a
 * fold over maps this process already holds, so it costs no query and no dial,
 * and a page refreshing slower than the state behind it would show a rotation
 * that has already moved on.
 */
private const val DEFAULT_STATUS_INTERVAL_SECONDS = 30L

/** `SYNC_STATUS_INTERVAL_SECONDS`, refused rather than silently defaulted when it is not a number. */
private fun statusInterval(env: Map<String, String>): Long =
    env["SYNC_STATUS_INTERVAL_SECONDS"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull()?.takeIf { n -> n > 0 }
            ?: error("SYNC_STATUS_INTERVAL_SECONDS='$it' is not a positive number of seconds. Unset SYNC_STATUS_PORT to serve no page.")
    } ?: DEFAULT_STATUS_INTERVAL_SECONDS

/**
 * The status page's markup, off this module's own classpath.
 *
 * An error rather than a fallback if it is missing: the page is a resource of
 * this jar, so an absent one means a broken build, and serving a blank page
 * would hide that behind something that looks like an empty mirror.
 */
private fun statusPage(): String =
    SyncStatus::class.java.getResourceAsStream("/sync_stats.html")?.use { it.readBytes().decodeToString() }
        ?: error("sync_stats.html is missing from the :sync jar — the status page cannot be served.")

/** The monitor's page, off ITS module's classpath, on the same terms. */
private fun monitorPage(): String =
    MonitorStatus::class.java.getResourceAsStream("/monitor_stats.html")?.use { it.readBytes().decodeToString() }
        ?: error("monitor_stats.html is missing from the :monitor jar — the monitor page cannot be served.")

/**
 * Run the sync engine — "the router" — as its own process against a Vespa the
 * serving relay also uses. Its whole point is the split: the mirror can be
 * restarted with a new `router.conf`, retuned, or lost to an OOM without the
 * relay dropping a client or Vespa replaying a transaction log, and its id
 * snapshots — the biggest allocations either process makes — live in a heap
 * the serving side never shares.
 *
 * Configuration is entirely from the environment, the same `SYNC_*` names the
 * embedded router read; `docs/configuration.md` documents every variable:
 *
 *   VESPA_URL          the Vespa query endpoint (default http://localhost:8080)
 *   RELAY_URL          the served relay's own ws url (REQUIRED — events are
 *                      stored as that relay's, whichever process writes them)
 *   RELAY_NSEC         identity for NIP-42 auth and the NIP-66 monitor
 *   SYNC_CONFIG / SYNC_CONFIG_FILE   the streams to mirror (REQUIRED)
 *   SYNC_PRESSURE_URL  the relay's /pressure endpoint; unset ⇒ ingest never
 *                      yields to client reads, and the boot log says so
 *   SYNC_TOR_SOCKS     a Tor SOCKS5 proxy (host:port); unset ⇒ no transport
 *                      can reach a .onion, so discovery drops them and a
 *                      configured one refuses to boot
 */
fun main() {
    val env = System.getenv()

    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — the served relay's own ws url; mirrored events are stored as its.")
    val relayUrl =
        RelayUrlNormalizer.normalizeOrNull(relayUrlRaw)
            ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")

    // A sync process with nothing to sync is a misconfiguration, not a mode:
    // fail here with the fix, never idle in a way that reads as "syncing".
    val config =
        RouterConfigLoader.fromEnv(env)
            ?: error("SYNC_CONFIG or SYNC_CONFIG_FILE is required — this process only mirrors; without streams it has no job.")

    // Read before anything slow, so a malformed value stops the process with
    // a clear message rather than as upstreams that mysteriously serve
    // nothing. A `.onion` in a stream's `urls` with no transport to reach it
    // is that same failure typed by hand: the dial would time out every
    // cycle, and a stream mirroring nothing looks exactly like one that is
    // failing. Say which urls and what to set.
    val torSettings = TorSettings.fromEnv(env)
    val onion = onionUpstreams(config.streams)
    if (torSettings == null && onion.isNotEmpty()) {
        error(
            "SYNC_TOR_SOCKS is unset, but ${onion.size} configured upstream(s) are hidden services " +
                "(${onion.joinToString()}) — point it at a Tor SOCKS proxy (e.g. tor:9050) or remove them.",
        )
    }

    // Read before anything slow, so a malformed key stops the process with a
    // clear message rather than as upstreams that mysteriously serve nothing.
    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("sync identity: ${identity.pubKey.take(12)}… (NIP-42 auth, NIP-66 monitor)")
    }

    // Both processes deploy on boot (AUTO_DEPLOY, default true): THIS is the
    // process whose writes a drifted schema silently discards — 2.3M events
    // lost in one run while every status line read healthy — and a sync-only
    // deployment has no relay to deploy for it. A no-change deploy is a cheap
    // no-op.
    val configUrl = env["VESPA_CONFIG_URL"] ?: vespaConfigUrlFor(vespaUrl)
    if (env["AUTO_DEPLOY"]?.toBooleanStrictOrNull() != false) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        // Compose starts both processes together and provides no ordering, so
        // two deploys can race the same config server session — and on a
        // FRESH Vespa the loser has nothing serving to fall back to, so
        // deployBundledSchema rethrows and the container crash-loops through
        // its first boot. The race is transient by nature: retry it here
        // rather than hand it to `restart: unless-stopped` as a crash.
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

    // STORE_WRITERS: mirroring kind 5/62 erases what an author retracted, and
    // the erase only stays erased if the RELAY's inserts are checked against
    // the tombstones this process stored — which its own store instance never
    // watched being written.
    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = false, configUrl = configUrl, writers = STORE_WRITERS)

    // Opt-in diagnostic; also applies QUARTZ_LOG_LEVEL. The audit lives on
    // this side of the split because ingest is what feeds it.
    val parseAudit = ParseAudit.installFromEnv(env)

    // Where a paged relay's already-walked history is remembered, so a
    // restart resumes instead of re-reading the corpus. Built from the parsed
    // streams, not from the environment alone: a stream may set its own
    // `refetchThePastSeconds`, and a period learned after its first band would be
    // ignored for the life of the process.
    val bands = SyncBands.fromEnv(env, config.streams)

    // What this router mirrors, published for the relay to serve. Written from
    // `config.streams`, which is what is RUNNING (SYNC_STREAMS narrows it), and
    // written here — after the config is loaded, before anything dials — because
    // a `router.conf` edit is a restart, so boot is the only moment it changes.
    val manifest = SyncManifest.fromEnv(env)
    // Only the UNSET case is announced here. A write that fails has already said
    // so, with the path and the reason, and following that with "unset" sends
    // the operator after a config problem they do not have.
    if (manifest.publishes) {
        manifest.write(config.streams)
    } else {
        System.err.println(
            "router: SYNC_MANIFEST_FILE unset — the relay cannot say which kinds this mirror holds, " +
                "so a client comparing our count against an upstream's total will read a complete mirror as partial",
        )
    }

    // What each stream is DOING, republished on the progress tick and read by
    // this process's own status site off the same heap. It was a FILE the
    // serving relay read; see [SyncProgress] for what went with the boundary.
    SyncProgress.refuseRemovedEnv(env)
    val progress = SyncProgress()

    // One level finer than the bands: what each peer will reconcile in one
    // window, and how far down the timeline the running sweep already got.
    val sweepState = SweepState.fromEnv(env)
    val refusedIds = RefusedIds.fromEnv(env)

    // Clients first, across the process boundary: the relay serves its mean
    // read latency and ingest yields to it. Explicitly opt-in — a sync
    // running without a relay (a fill-only box) has no readers to yield to —
    // and loud when off, so nobody debugs a throttling that cannot happen.
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
            // The raw engine index, not the trust-projected store: the
            // projection's existingIds delegates straight through, and this is
            // a pure read that counts and never mutates — the use its own
            // KDoc names. Membership is what ingest needs to skip verifying an
            // event it cannot write.
            knownIds = store.eventIndex::existingIds,
            // The newest stored version per author for one plain replaceable
            // kind. Built here rather than in the pipeline so the store's query
            // types stay at the one seam that already knows them, and the
            // pipeline keeps taking a plain function it can be tested against.
            // The winner rule is stage D's, verbatim: newest created_at, ties
            // to the LOWER id (hence thenByDescending on a max).
            newestVersions = { kind, authors ->
                store.eventIndex
                    .search(EventQuery(kinds = listOf(kind), authors = authors))
                    .groupBy { it.pubkey }
                    .mapValues { (_, docs) ->
                        docs.maxWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id }).let { AddressVersion(it.createdAt, it.id) }
                    }
            },
        ).start()

    // THIS PROCESS'S OWN UI, on its own port. What the mirror has walked and
    // what it is doing were three JSON files on a shared volume the serving
    // relay read back and re-narrated; they are served here now, by the process
    // that produces them. Unset disables it: a fill-only box with nobody to
    // read a page should not bind a port, and the boot log says so.
    val statusPort =
        env["SYNC_STATUS_PORT"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
            it.toIntOrNull() ?: error("SYNC_STATUS_PORT='$it' is not a port number. Unset it to serve no status page.")
        } ?: DEFAULT_STATUS_PORT
    // The monitor's page, on its own port. Its own rather than a second panel
    // on the mirror's because it answers a different question — "what is out
    // there, and how much of it can we use" against "is the mirror keeping up"
    // — on a different clock and in a different unit. It is also what the
    // eventual process split needs: when the plane moves into its own
    // container, the port it is read at does not change.
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
            val status = SyncStatus(bands, sweepState, progress, statusSnapshot, everySeconds)
            // Once before the server binds, so the first request answers with a
            // document rather than the 503 that means "nothing computed yet" —
            // this pass reads maps that are already populated, so there is no
            // reason for a reader to wait a whole interval for them.
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

    // …and the monitor's, over ITS OWN report. Built from the engine because
    // this process composes the two planes; the document is the monitor's, and
    // the day the plane moves into its own container this block moves with it
    // unchanged.
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
                    page = monitorPage(),
                    snapshot = monitorSnapshot,
                    icon = env["RELAY_ICON"]?.trim()?.takeIf { it.isNotEmpty() },
                )
        }
    if (monitorSite != null) {
        println("vespa-sync monitor page on http://localhost:$monitorPort/")
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Before the engine: the page reads state the engine is about to
            // stop updating, and answering a request with a half-torn-down
            // document is worse than refusing the connection.
            listOfNotNull(statusSite, monitorSite).forEach { (rollup, server) ->
                rollup.close()
                server.stop(1_000, 2_000)
            }
            // Stop mirroring into the store before the store closes.
            engine.close()
            // After the engine, so the final report includes the last batch.
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
    // Everything runs on the engine's own scopes and daemon threads; the main
    // thread's only remaining job is to exist until a signal arrives.
    Thread.currentThread().join()
}
