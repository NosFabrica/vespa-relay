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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.RelayIdentity
import com.nosfabrica.vespa.relay.maintenance.ParseAudit
import com.nosfabrica.vespa.relay.maintenance.STORE_WRITERS
import com.nosfabrica.vespa.relay.maintenance.deployBundledSchema
import com.nosfabrica.vespa.relay.maintenance.vespaConfigUrlFor
import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.router.config.syncEnv
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer

// The deploy-race retry (see main): enough attempts to outlast the relay's
// own boot deploy at a pace that stays visible in the log.
private const val DEPLOY_ATTEMPTS = 5
private const val DEPLOY_RETRY_SECONDS = 5L

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
    // restart resumes instead of re-reading the corpus.
    val bands = SyncBands.fromEnv(env)

    // One level finer than the bands: what each peer will reconcile in one
    // window, and how far down the timeline the running sweep already got.
    val sweepState = SweepState.fromEnv(env)

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
            signer = identity,
            wireLogMode = env.syncEnv("SYNC_WIRE_LOG", "ROUTER_WIRE_LOG")?.trim()?.lowercase() ?: "",
            servingPressure = servingPressure,
            torSettings = torSettings,
            // The raw engine index, not the trust-projected store: the
            // projection's existingIds delegates straight through, and this is
            // a pure read that counts and never mutates — the use its own
            // KDoc names. Membership is what ingest needs to skip verifying an
            // event it cannot write.
            knownIds = store.eventIndex::existingIds,
        ).start()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // Stop mirroring into the store before the store closes.
            engine.close()
            // After the engine, so the final report includes the last batch.
            parseAudit?.close()
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
