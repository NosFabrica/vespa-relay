/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File

/**
 * The router config: strfry's `streams { }` model, parsed from HOCON. Each
 * named stream mirrors a NIP-01 [filter] in a [dir]ection against a set of
 * upstream [urls]. This is the same shape strfry's router takes, so an
 * existing `routerConfigOverride` drops in unchanged:
 *
 *     connectionTimeout = 20
 *     streams {
 *       popular {
 *         dir    = "down"
 *         filter = { "kinds": [0,3,5,1984,10000,30000] }
 *         urls   = [ "wss://relay.primal.net", "wss://relay.damus.io" ]
 *       }
 *       mirrors { dir = "down"  filter = {...}  urls = [...] }
 *     }
 *
 * `dir` is `down` (mirror upstream events into our store), `up` (publish our
 * store's matching events upstream), or `both` (both directions on the same
 * relay). All three are implemented; `up` reconciles our store against the
 * upstream and pushes what the upstream is missing.
 *
 * Three fields extend strfry's schema, all optional:
 *  - `trusted` (bool, default false): skip signature verification for events
 *    from this stream's relays. Leave it off for public relays.
 *  - `backfillSeconds` (long, default from `ROUTER_BACKFILL_SECONDS`, else 0):
 *    how far back to negentropy-backfill history before the live tail takes
 *    over. 0 means live-only (strfry-router parity) — stream new events from
 *    connect, don't reach for history.
 *  - `relaySource { }` (see [RelaySource]): the stream has no static `urls` at
 *    all — its relay list is read out of the store's own relay-list events and
 *    re-synced on a period. That is the outbox stream, and its NIP-85 twin.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<MirrorStream>,
    // How often (seconds) an `up`/`both` stream re-reconciles the store against
    // its upstream to push newly-arrived local events. From ROUTER_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
    // Hard cap (seconds) on a single negentropy reconciliation. Some relays
    // advertise NIP-77 but never converge; this bounds a session that keeps
    // talking without progressing, so it fails cleanly and the live tail carries
    // that upstream. From ROUTER_NEG_TIMEOUT_SECONDS.
    //
    // The default is generous because it is not what protects against a *stuck*
    // upstream — MirrorRouter's 30s idle timeout does that, and it fires whether
    // this cap is 10 minutes or 10 hours. Real backfill windows are measured in
    // years, and a tight cap truncates those legitimate fills for no safety gain.
    val negTimeoutSec: Long = 14_400,
    // Ingest tuning. The store serializes writes through one mutex, so extra
    // workers mostly overlap verify (CPU) with the write (I/O) — a couple is
    // plenty; throughput comes from the batch size (each mutex hold amortizes a
    // read-before-write preload + a Vespa feed over the whole batch).
    // From ROUTER_INGEST_CONCURRENCY / ROUTER_INGEST_BATCH.
    val ingestConcurrency: Int = 2,
    val ingestBatch: Int = 1000,
) {
    /** Every (stream, url) pair whose direction pulls events down into our store. */
    fun downUpstreams(): List<MirrorUpstream> = upstreamsFor(MirrorDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<MirrorUpstream> = upstreamsFor(MirrorDirection.UP)

    /** The streams whose relay list is discovered from the store, not configured. */
    fun dynamicStreams(): List<MirrorStream> = streams.filter { it.relaySource != null }

    private fun upstreamsFor(want: MirrorDirection): List<MirrorUpstream> =
        streams
            .filter { it.dir == want || it.dir == MirrorDirection.BOTH }
            .flatMap { s -> s.urls.map { MirrorUpstream(s.name, it, s.filter, s.trusted, s.backfillSeconds) } }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class MirrorUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
    val backfillSeconds: Long,
)

data class MirrorStream(
    val name: String,
    val dir: MirrorDirection,
    val filter: Filter,
    val urls: List<NormalizedRelayUrl>,
    val trusted: Boolean,
    val backfillSeconds: Long,
    // Null for an ordinary stream (its relays are the `urls` above). Set for a
    // stream whose relays come out of the store instead — see [RelaySource].
    val relaySource: RelaySource? = null,
)

/**
 * A stream's relay list, read from the relay-list events our own store already
 * holds instead of from a hand-written `urls` array. The store fills with 10002s
 * and 10040s (an ordinary `down` stream on the indexer relays does that), and
 * this turns those into the fan-out the stream syncs against:
 *
 *     outbox {
 *       dir    = "down"
 *       filter = { "kinds": [0, 3, 10002] }
 *       relaySource {
 *         kind           = 10002    # or 10040
 *         marker         = "write"  # 10002 only: write / read / any
 *         refreshSeconds = 21600
 *         concurrency    = 8
 *         exclude        = [ "wss://relay.example" ]
 *       }
 *     }
 *
 * Every refresh the router re-reads every list of that kind in the store and
 * negentropy-syncs (or paged-REQ-fetches) the stream filter against every relay
 * they name — the whole set, however large it has grown. [concurrency] paces
 * that fan-out; nothing truncates it. There is no live tail either: a set this
 * size is synced on a period, not held open.
 *
 * @param kind which relay-list event to read the urls out of.
 * @param role for [RelayListKind.OUTBOX], which NIP-65 marker to keep. Unmarked
 *   `r` tags mean both read and write, so they match every role.
 * @param refreshSeconds how often the whole cycle (re-read the lists, re-sync
 *   every relay) runs again.
 * @param concurrency how many of those relays sync at the same time.
 * @param exclude relays to skip however many lists name them.
 */
data class RelaySource(
    val kind: RelayListKind,
    val role: RelayRole,
    val refreshSeconds: Long,
    val concurrency: Int,
    val exclude: Set<NormalizedRelayUrl>,
)

/** The relay-list events a [RelaySource] knows how to read urls out of. */
enum class RelayListKind(
    val kind: Int,
) {
    /** NIP-65 relay list metadata: `["r", "<url>", "read"|"write"|absent]`. */
    OUTBOX(10002),

    /** NIP-85 trusted assertions: `["<kind>:<type>", "<pubkey>", "<url>"]`. */
    TRUST_PROVIDERS(10040),
    ;

    companion object {
        fun parse(kind: Int): RelayListKind =
            entries.firstOrNull { it.kind == kind }
                ?: error("router: relaySource kind $kind has no relay-list reader (expected ${entries.joinToString(" / ") { it.kind.toString() }})")
    }
}

/** Which side of a NIP-65 relay list a [RelaySource] pulls from. */
enum class RelayRole(
    val wire: String,
) {
    WRITE("write"),
    READ("read"),
    ANY("any"),
    ;

    /** Unmarked `r` tags are read *and* write, so they match whatever we asked for. */
    fun matches(marker: String?): Boolean = this == ANY || marker.isNullOrEmpty() || marker == wire

    companion object {
        fun parse(raw: String): RelayRole =
            when (val v = raw.trim().lowercase()) {
                WRITE.wire -> WRITE
                READ.wire -> READ
                ANY.wire, "both", "all" -> ANY
                else -> error("router: unknown relaySource marker '$v' (expected write / read / any)")
            }
    }
}

/** Env-level fallbacks for the per-stream `relaySource { }` knobs. */
data class RelaySourceDefaults(
    val refreshSeconds: Long = 21_600,
    val concurrency: Int = 8,
)

enum class MirrorDirection(
    val wire: String,
) {
    DOWN("down"),
    UP("up"),
    BOTH("both"),
    ;

    companion object {
        fun parse(raw: String): MirrorDirection =
            entries.firstOrNull { it.wire.equals(raw.trim(), ignoreCase = true) }
                ?: error("router: unknown stream dir '$raw' (expected down / up / both)")
    }
}

/**
 * Loads [RouterConfig] from the environment. `ROUTER_CONFIG` holds the HOCON
 * inline; `ROUTER_CONFIG_FILE` points at a file holding it. Neither set ⇒ no
 * router (returns null; the relay serves without mirroring). `ROUTER_BACKFILL_SECONDS`
 * sets the default backfill window for streams that don't state their own;
 * `ROUTER_UP_INTERVAL_SECONDS` sets how often up/both streams re-reconcile.
 *
 * `ROUTER_DYNAMIC_REFRESH_SECONDS` and `ROUTER_DYNAMIC_CONCURRENCY` do the same
 * for [RelaySource] streams — the per-stream `relaySource { }` keys override both.
 */
object RouterConfigLoader {
    fun fromEnv(env: Map<String, String>): RouterConfig? {
        val inline = env["ROUTER_CONFIG"]?.takeIf { it.isNotBlank() }
        val fromFile = env["ROUTER_CONFIG_FILE"]?.takeIf { it.isNotBlank() }?.let { File(it).readText() }
        val raw = inline ?: fromFile ?: return null
        val backfillDefault = env["ROUTER_BACKFILL_SECONDS"]?.trim()?.toLongOrNull() ?: 0L
        val upInterval = env["ROUTER_UP_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 300L
        val negTimeout = env["ROUTER_NEG_TIMEOUT_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 14_400L
        val ingestConcurrency = env["ROUTER_INGEST_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 64) ?: 2
        val ingestBatch = env["ROUTER_INGEST_BATCH"]?.trim()?.toIntOrNull()?.coerceIn(1, 20_000) ?: 1000
        val fallback = RelaySourceDefaults()
        val relaySourceDefaults =
            RelaySourceDefaults(
                refreshSeconds = env["ROUTER_DYNAMIC_REFRESH_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(60L) ?: fallback.refreshSeconds,
                concurrency = env["ROUTER_DYNAMIC_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 256) ?: fallback.concurrency,
            )
        return parse(raw, backfillDefault, upInterval, negTimeout, ingestConcurrency, ingestBatch, relaySourceDefaults)
    }

    fun parse(
        hocon: String,
        backfillDefault: Long = 0L,
        upIntervalSec: Long = 300L,
        negTimeoutSec: Long = 14_400L,
        ingestConcurrency: Int = 2,
        ingestBatch: Int = 1000,
        relaySourceDefaults: RelaySourceDefaults = RelaySourceDefaults(),
    ): RouterConfig {
        val cfg = ConfigFactory.parseString(hocon)
        val connTimeout = if (cfg.hasPath("connectionTimeout")) cfg.getLong("connectionTimeout") else 20L
        require(cfg.hasPath("streams")) { "router: config has no `streams { }` block" }
        val streamsCfg = cfg.getConfig("streams")
        val streams =
            streamsCfg.root().keys.map { name ->
                val s = streamsCfg.getConfig(quote(name))
                val urls = if (s.hasPath("urls")) normalizeUrls(name, s.getStringList("urls")) else emptyList()
                val dir = MirrorDirection.parse(if (s.hasPath("dir")) s.getString("dir") else "down")
                val relaySource =
                    if (s.hasPath("relaySource")) parseRelaySource(name, s.getConfig("relaySource"), relaySourceDefaults) else null

                require(relaySource != null || s.hasPath("urls")) {
                    "router: stream '$name' has neither `urls` nor a `relaySource { }` block"
                }
                require(relaySource == null || urls.isEmpty()) {
                    "router: stream '$name' cannot mix `relaySource { }` with static `urls` — split them into two streams"
                }
                require(relaySource == null || dir == MirrorDirection.DOWN) {
                    "router: stream '$name' is `relaySource { }`, which only pulls down — set dir = \"down\""
                }

                MirrorStream(
                    name = name,
                    dir = dir,
                    filter = parseFilter(s.getConfig("filter")),
                    urls = urls,
                    trusted = s.hasPath("trusted") && s.getBoolean("trusted"),
                    backfillSeconds = if (s.hasPath("backfillSeconds")) s.getLong("backfillSeconds") else backfillDefault,
                    relaySource = relaySource,
                )
            }
        return RouterConfig(connTimeout, streams, upIntervalSec, negTimeoutSec, ingestConcurrency, ingestBatch)
    }

    private fun normalizeUrls(
        stream: String,
        raw: List<String>,
    ): List<NormalizedRelayUrl> =
        raw.mapNotNull { url ->
            RelayUrlNormalizer.normalizeOrNull(url).also {
                if (it == null) System.err.println("router: stream '$stream' skips invalid url '$url'")
            }
        }

    /** The `relaySource { }` block: which relay list to read, and how hard to fan out over it. */
    private fun parseRelaySource(
        stream: String,
        s: Config,
        defaults: RelaySourceDefaults,
    ): RelaySource {
        require(s.hasPath("kind")) { "router: stream '$stream' relaySource needs a `kind` (10002 or 10040)" }
        val kind = RelayListKind.parse(s.getInt("kind"))
        val role =
            if (s.hasPath("marker")) {
                RelayRole.parse(s.getString("marker"))
            } else if (kind == RelayListKind.OUTBOX) {
                RelayRole.WRITE
            } else {
                // Only NIP-65 marks its relays; a 10040's urls are all there is.
                RelayRole.ANY
            }
        return RelaySource(
            kind = kind,
            role = role,
            refreshSeconds = (if (s.hasPath("refreshSeconds")) s.getLong("refreshSeconds") else defaults.refreshSeconds).coerceAtLeast(60L),
            concurrency = (if (s.hasPath("concurrency")) s.getInt("concurrency") else defaults.concurrency).coerceIn(1, 256),
            exclude = if (s.hasPath("exclude")) normalizeUrls(stream, s.getStringList("exclude")).toSet() else emptySet(),
        )
    }

    /**
     * Turn a HOCON filter object (`{ "kinds": [0,3], "authors": [...], "#t": [...] }`)
     * into a quartz [Filter]. Standard NIP-01 fields plus `#x` tag filters. `since`
     * and `limit` are read but [MirrorRouter] manages them per-phase, so an operator
     * pinning them here doesn't fight the live tail's windowing.
     */
    fun parseFilter(f: Config): Filter {
        fun strs(k: String) = if (f.hasPath(quote(k))) f.getStringList(quote(k)) else null

        fun ints(k: String) = if (f.hasPath(quote(k))) f.getIntList(quote(k)).map { it.toInt() } else null

        val tags =
            f
                .root()
                .keys
                .filter { it.startsWith("#") && it.length == 2 }
                .associate { it.substring(1) to f.getStringList(quote(it)) }
                .ifEmpty { null }

        return Filter(
            ids = strs("ids"),
            authors = strs("authors"),
            kinds = ints("kinds"),
            tags = tags,
            since = if (f.hasPath("since")) f.getLong("since") else null,
            until = if (f.hasPath("until")) f.getLong("until") else null,
            limit = if (f.hasPath("limit")) f.getInt("limit") else null,
            search = if (f.hasPath("search")) f.getString("search") else null,
        )
    }

    /** HOCON path segments with dots/hashes/special chars must be quoted for get*(). */
    private fun quote(key: String): String = "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
