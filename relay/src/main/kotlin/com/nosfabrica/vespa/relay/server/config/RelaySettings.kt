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
package com.nosfabrica.vespa.relay.server.config

import com.nosfabrica.vespa.relay.identity.PubKeys
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.nosfabrica.vespa.relay.server.selfIconUrl
import com.nosfabrica.vespa.relay.util.strictFlag
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File

/**
 * Where this relay answers: [ws] its NIP-42 identity, [http] the one origin a NIP-98 `u` must name
 * and the icon is served from (`RELAY_HTTP_URL`, else [ws]'s own spelling as http).
 */
class RelayEndpoint(
    val ws: NormalizedRelayUrl,
    val http: String,
) {
    /** The icon this relay serves itself, as an absolute url a stranger can reach, or null. */
    val selfIconUrl: String? get() = selfIconUrl(http)
}

/** `RELAY_URL` (required) and `RELAY_HTTP_URL`, each read once, so NIP-98 and the icon cannot disagree. */
fun relayEndpointFromEnv(env: Map<String, String>): RelayEndpoint {
    val raw =
        env.text("RELAY_URL")
            ?: error("RELAY_URL is required — this relay's own ws url (NIP-42 identity / NIP-62 vanish scope).")
    val ws = RelayUrlNormalizer.normalizeOrNull(raw) ?: error("RELAY_URL '$raw' is not a valid relay url.")
    // The operator's spelling, not the normalized one: a NIP-98 `u` is compared as text.
    return RelayEndpoint(ws, env.text("RELAY_HTTP_URL") ?: raw.httpFromWs())
}

/** The http/https origin of a ws/wss url, for NIP-98's `u` tag. */
fun String.httpFromWs(): String =
    when {
        startsWith("wss://") -> "https://" + substring(6)
        startsWith("ws://") -> "http://" + substring(5)
        else -> this
    }

/** The one description of this relay, served as NIP-11 and published as its kind 0. */
fun nip11FromEnv(
    env: Map<String, String>,
    selfPubkey: String?,
    ownIconUrl: String?,
): Nip11Info =
    Nip11Info(
        name = env.text("RELAY_NAME") ?: "vespa-relay",
        description = env.text("RELAY_DESCRIPTION"),
        icon = env.text("RELAY_ICON") ?: ownIconUrl,
        banner = env.text("RELAY_BANNER"),
        contactPubkey = PubKeys.decodeOrNull(env["RELAY_CONTACT_PUBKEY"], "RELAY_CONTACT_PUBKEY"),
        selfPubkey = selfPubkey,
        contact = env.text("RELAY_CONTACT"),
        version = env.text("RELAY_VERSION"),
        postingPolicy = env.text("RELAY_POSTING_POLICY"),
        privacyPolicy = env.text("RELAY_PRIVACY_POLICY"),
        termsOfService = env.text("RELAY_TERMS_OF_SERVICE"),
    )

/** The two rollup cadences, in seconds; 0 turns that tier off. */
class StatsIntervals(
    val chartsSeconds: Long,
    val countersSeconds: Long,
)

/** `STATS_INTERVAL_SECONDS` and `STATS_COUNTERS_INTERVAL_SECONDS`: `0s` or `off` stops the boot rather than running the default. */
fun statsIntervalsFromEnv(env: Map<String, String>): StatsIntervals =
    StatsIntervals(
        chartsSeconds = env.strictLong("STATS_INTERVAL_SECONDS", 0..MAX_INTERVAL_SECONDS) ?: 900L,
        countersSeconds = env.strictLong("STATS_COUNTERS_INTERVAL_SECONDS", 0..MAX_INTERVAL_SECONDS) ?: 60L,
    )

/** A week: a rollup slower than that is one nobody is reading, and the cap keeps seconds-to-millis whole. */
private const val MAX_INTERVAL_SECONDS = 7 * 24 * 3_600L

/** The state files the relay keeps, each an env path or where `docker-compose.yml` mounts it. */
class RelayFiles(
    val stats: File,
    val ftsCursor: File,
    /** Null when `SYNC_MANIFEST_FILE` is set empty: the mirror card is off. */
    val syncManifest: File?,
    /** Null for in-memory ban lists. */
    val banState: String?,
)

fun relayFilesFromEnv(env: Map<String, String>): RelayFiles =
    RelayFiles(
        stats = File(env.text("STATS_FILE") ?: "/var/lib/vespa-relay/stats.json"),
        ftsCursor = File(env.text("FTS_CURSOR_FILE") ?: "/var/lib/vespa-relay/fts-cursor.txt"),
        syncManifest =
            when (val raw = env["SYNC_MANIFEST_FILE"]) {
                null -> File("/var/lib/vespa-relay/sync-manifest.json")
                else -> raw.trim().takeIf { it.isNotEmpty() }?.let(::File)
            },
        banState = env.text("RELAY_STATE_FILE"),
    )

/** The switches that start background work at boot. */
class MaintenanceSwitches(
    val reindexFts: Boolean,
    val trustReconcile: Boolean,
    val logConnections: Boolean,
    val storeMetricsLogSeconds: Int,
)

fun maintenanceSwitchesFromEnv(env: Map<String, String>): MaintenanceSwitches =
    MaintenanceSwitches(
        reindexFts = env.strictFlag("REINDEX_FTS_ON_START") ?: false,
        trustReconcile = env.strictFlag("TRUST_RECONCILE_ON_START") ?: true,
        logConnections = env.strictFlag("LOG_CONNECTIONS") ?: false,
        // Any whole number, as the sync process reads it: 0 or below turns the line off.
        storeMetricsLogSeconds = env.strictInt("STORE_METRICS_LOG_SECONDS") ?: 300,
    )

/**
 * A sync or monitor config aimed at this process: a configured component that would run nothing,
 * so it stops the boot. The rest of the sync family starts nothing here and is only [syncSettingsIgnored].
 */
fun refuseSyncConfig(env: Map<String, String>) {
    val key = listOf("SYNC_CONFIG", "SYNC_CONFIG_FILE", "MONITOR_CONFIG", "MONITOR_CONFIG_FILE").firstOrNull { env.text(it) != null } ?: return
    error(
        "$key is set, but the relay no longer runs the sync engine or the monitor — they moved to their own " +
            "process (the vespa-sync binary / the `sync` service in docker-compose.yml, enabled with " +
            "`docker compose --profile sync up`). Move the SYNC_* and MONITOR_* settings there, or unset $key " +
            "to serve without mirroring.",
    )
}

/** The sync process's settings set on this one, which do nothing here; the manifest it does read. */
fun syncSettingsIgnored(env: Map<String, String>): List<String> =
    env.keys
        .filter { key ->
            // SERVING_PRESSURE_THRESHOLD_MS is read here too, since the mirror lane yields on it.
            (key.startsWith("SYNC_") || key.startsWith("PARSE_AUDIT_")) &&
                key != "SYNC_MANIFEST_FILE" &&
                env.text(key) != null
        }.sorted()

/** Set, non-blank names under [prefix], for the "these do nothing" notes. */
fun Map<String, String>.setUnder(prefix: String): List<String> = keys.filter { it.startsWith(prefix) && text(it) != null }.sorted()

/** [key]'s value trimmed, or null when unset or blank: compose delivers `""` for an unset `${X:-}`. */
private fun Map<String, String>.text(key: String): String? = this[key]?.trim()?.takeIf { it.isNotEmpty() }
