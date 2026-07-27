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
 * Two fields extend strfry's schema, both optional:
 *  - `trusted` (bool, default false): skip signature verification for events
 *    from this stream's relays. Leave it off for public relays.
 *  - `backfillSeconds` (long, default from `ROUTER_BACKFILL_SECONDS`, else 0):
 *    how far back to negentropy-backfill history before the live tail takes
 *    over. 0 means live-only (strfry-router parity) — stream new events from
 *    connect, don't reach for history.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<MirrorStream>,
    // How often (seconds) an `up`/`both` stream re-reconciles the store against
    // its upstream to push newly-arrived local events. From ROUTER_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
    // Hard cap (seconds) on a single negentropy reconciliation. Some relays
    // advertise NIP-77 but never converge; this bounds a stuck session so it
    // fails cleanly and the live tail carries that upstream. From
    // ROUTER_NEG_TIMEOUT_SECONDS. Raise it for genuinely large historical fills.
    val negTimeoutSec: Long = 600,
) {
    /** Every (stream, url) pair whose direction pulls events down into our store. */
    fun downUpstreams(): List<MirrorUpstream> = upstreamsFor(MirrorDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<MirrorUpstream> = upstreamsFor(MirrorDirection.UP)

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
 */
object RouterConfigLoader {
    fun fromEnv(env: Map<String, String>): RouterConfig? {
        val inline = env["ROUTER_CONFIG"]?.takeIf { it.isNotBlank() }
        val fromFile = env["ROUTER_CONFIG_FILE"]?.takeIf { it.isNotBlank() }?.let { File(it).readText() }
        val raw = inline ?: fromFile ?: return null
        val backfillDefault = env["ROUTER_BACKFILL_SECONDS"]?.trim()?.toLongOrNull() ?: 0L
        val upInterval = env["ROUTER_UP_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 300L
        val negTimeout = env["ROUTER_NEG_TIMEOUT_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 600L
        return parse(raw, backfillDefault, upInterval, negTimeout)
    }

    fun parse(
        hocon: String,
        backfillDefault: Long = 0L,
        upIntervalSec: Long = 300L,
        negTimeoutSec: Long = 600L,
    ): RouterConfig {
        val cfg = ConfigFactory.parseString(hocon)
        val connTimeout = if (cfg.hasPath("connectionTimeout")) cfg.getLong("connectionTimeout") else 20L
        require(cfg.hasPath("streams")) { "router: config has no `streams { }` block" }
        val streamsCfg = cfg.getConfig("streams")
        val streams =
            streamsCfg.root().keys.map { name ->
                val s = streamsCfg.getConfig(quote(name))
                val urls =
                    s.getStringList("urls").mapNotNull { url ->
                        RelayUrlNormalizer.normalizeOrNull(url).also {
                            if (it == null) System.err.println("router: stream '$name' skips invalid url '$url'")
                        }
                    }
                MirrorStream(
                    name = name,
                    dir = MirrorDirection.parse(if (s.hasPath("dir")) s.getString("dir") else "down"),
                    filter = parseFilter(s.getConfig("filter")),
                    urls = urls,
                    trusted = s.hasPath("trusted") && s.getBoolean("trusted"),
                    backfillSeconds = if (s.hasPath("backfillSeconds")) s.getLong("backfillSeconds") else backfillDefault,
                )
            }
        return RouterConfig(connTimeout, streams, upIntervalSec, negTimeoutSec)
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
