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
 *  - `relaySource = [ ]` (see [DynamicRelayList]): the stream has no static `urls` at
 *    all — its relay list is read out of the store's own relay-list events and
 *    re-synced on a period. That is the outbox stream, and its NIP-85 twin.
 *
 * ## How far back a stream reaches
 *
 * The [filter]'s own `since`/`until`, and nothing else. They are an ordinary
 * NIP-01 filter and mean what NIP-01 says: absent is unbounded, so a stream that
 * names neither backfills the upstream's whole history.
 *
 * The backfill phase runs the filter as written. The live tail runs it from
 * connect forward — that is what a tail *is*, not a knob — but it keeps the
 * filter's `until`, so a stream bounded on the right stops there in both phases.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<MirrorStream>,
    // How often (seconds) an `up`/`both` stream re-reconciles the store against
    // its upstream to push newly-arrived local events. From ROUTER_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
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
    fun dynamicStreams(): List<MirrorStream> = streams.filter { it.dynamic != null }

    private fun upstreamsFor(want: MirrorDirection): List<MirrorUpstream> =
        streams
            .filter { it.dir == want || it.dir == MirrorDirection.BOTH }
            .flatMap { s -> s.urls.map { MirrorUpstream(s.name, it, s.filter, s.trusted) } }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class MirrorUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
)

data class MirrorStream(
    val name: String,
    val dir: MirrorDirection,
    val filter: Filter,
    val urls: List<NormalizedRelayUrl>,
    val trusted: Boolean,
    // Null for an ordinary stream (its relays are the `urls` above). Set for a
    // stream whose relays come out of the store instead — see [DynamicRelayList].
    val dynamic: DynamicRelayList? = null,
)

/**
 * A stream's relay list, read from events our own store already holds instead of
 * from a hand-written `urls` array. `relaySource` is a *list* of places to read
 * urls from — relay lists, trust-provider lists, and the relay hints riding on
 * ordinary tags — all merged into one fan-out:
 *
 *     outbox {
 *       dir            = "down"
 *       filter         = { "kinds": [0, 3, 10002] }
 *       refreshSeconds = 21600
 *       concurrency    = 8
 *       exclude        = [ "wss://relay.example" ]
 *       relaySource = [
 *         {
 *           select = [
 *             {
 *               kind = 10002
 *               tag = "r"
 *               marker = "write"
 *             }
 *             {
 *               kind = 10040
 *               tag = "30382:rank"
 *               index = 2
 *             }
 *           ]
 *           filter = { "kinds": [10002, 10040] }
 *         }
 *         {
 *           select = [
 *             {
 *               tag = "e"
 *               index = 2
 *             }
 *           ]
 *           filter = { "kinds": [1], "limit": 100000 }
 *         }
 *       ]
 *     }
 *
 * Every refresh the router runs each scan, unions the relays they name, and
 * negentropy-syncs (or paged-REQ-fetches) the stream filter against every one of
 * them — the whole set, however large it has grown. [concurrency] paces that
 * fan-out; nothing truncates it. There is no live tail either: a set this size
 * is synced on a period, not held open.
 *
 * @param sources every scan to read relay urls from, merged.
 * @param refreshSeconds how often the whole cycle (re-read the sources, re-sync
 *   every relay) runs again.
 * @param concurrency how many of those relays sync at the same time.
 *
 *   A relay's sync has no wall-clock cap. Every timeout in the client is measured
 *   from the last message, so a relay that goes quiet is already dropped in
 *   seconds — and one that is still delivering is doing the work the slot exists
 *   for, however long it takes. A deadline could only ever fire on the healthy
 *   case.
 * @param exclude relays to skip however many sources name them.
 */
data class DynamicRelayList(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val concurrency: Int,
    val exclude: Set<NormalizedRelayUrl>,
)

/**
 * One scan of the store: the [selects] saying which relay urls to pull out, and
 * a NIP-01 [filter] saying which events to pull them from. The filter runs once
 * and every select is applied to what it returns, so a whole shelf of relay-list
 * kinds costs one query rather than one each:
 *
 *     {
 *       select = [
 *         {                          NIP-65 outbox
 *           kind = 10002
 *           tag = "r"
 *           marker = "write"
 *         }
 *         {                          NIP-85 providers
 *           kind = 10040
 *           tag = "30382:rank"
 *           index = 2
 *         }
 *         {                          NIP-66 monitor reports
 *           kind = 30166
 *           tag = "d"
 *         }
 *         {                          everything else in the filter
 *           tag = "relay"
 *         }
 *       ]
 *       filter = { "kinds": [10002, 10050, 30002, 30166, 10040] }
 *     }
 *
 * The filter is an ordinary NIP-01 filter — `authors`, `since`, `until`,
 * `limit`, `#t`-style tag filters — so a scan can be narrowed however you like.
 * `since`/`until` are absolute unix seconds, as everywhere else in NIP-01; on a
 * repeating cycle `limit` is the bound that stays meaningful, since a fixed
 * `since` only ages.
 */
data class RelaySource(
    val selects: List<RelaySelect>,
    val filter: Filter,
)

/**
 * Where a relay url sits in a tag. Every relay list in the protocol is some tag
 * with a url at a fixed offset, so this covers all of them with no per-kind code.
 *
 * Note the two different tag notions: [tag] names the tag urls are *read from*,
 * while a `"#e" = [...]` entry in the [RelaySource.filter] narrows which events
 * are scanned at all. They are unrelated.
 *
 * @param kind apply this select only to events of that kind, or null to apply it
 *   to everything the filter returned. A kind the filter never collects simply
 *   never matches, so listing selects the scan can't reach is harmless.
 * @param tag the tag name to read, or null for any tag. Leaving it out is how
 *   you take a whole family (NIP-85's `<kind>:<type>` service tags) without
 *   naming each one — at the cost of a stricter url check, see [RelayDiscovery].
 * @param index which element of the tag holds the url. 1 for nearly everything;
 *   2 for NIP-85 service tags and for `e`/`p`/`a`/`q` relay hints, which put an
 *   id or a pubkey first.
 * @param where conditions on the rest of the tag, see [TagCondition]. Empty
 *   keeps every tag. The config's `marker = "write" / "read" / "any"` is sugar
 *   that expands into this list.
 */
data class RelaySelect(
    val kind: Int?,
    val tag: String?,
    val index: Int,
    val where: List<TagCondition> = emptyList(),
)

/**
 * One alternative in a select's `where` list. The list is NIP-01's own boolean
 * shape pointed at a tag instead of an event: entries OR together, and the
 * fields inside one entry AND. A tag passes an empty list outright.
 *
 * [equals] is exact — no case folding, no trimming — and an element that does
 * not exist matches nothing, not even `""`. Structure is the size bounds' job:
 * `maxSize = 2` is the `["r", url]` tag with no marker slot, and [minSize] the
 * "slot exists, whatever it says" side.
 *
 * NIP-65's write side in these terms (what `marker = "write"` expands to):
 * marked write, marked empty, or no marker slot at all —
 *
 *     where = [
 *       { index = 2, equals = "write" }
 *       { index = 2, equals = "" }
 *       { maxSize = 2 }
 *     ]
 *
 * @param index which element [equals] tests. The two only mean something
 *   together, so the parser demands both or neither.
 * @param equals the element at [index] is exactly this string.
 * @param minSize the tag has at least this many elements.
 * @param maxSize the tag has at most this many elements.
 */
data class TagCondition(
    val index: Int? = null,
    val equals: String? = null,
    val minSize: Int? = null,
    val maxSize: Int? = null,
) {
    fun matches(tag: Array<String>): Boolean {
        if (equals != null && (index == null || tag.getOrNull(index) != equals)) return false
        if (minSize != null && tag.size < minSize) return false
        if (maxSize != null && tag.size > maxSize) return false
        return true
    }
}

/** Env-level fallbacks for the per-stream dynamic-relay knobs. */
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
 * `ROUTER_DYNAMIC_REFRESH_SECONDS`, `ROUTER_DYNAMIC_CONCURRENCY` and
 * do the same for dynamic streams — the
 * per-stream keys of the same name override each of them.
 */
object RouterConfigLoader {
    fun fromEnv(env: Map<String, String>): RouterConfig? {
        val inline = env["ROUTER_CONFIG"]?.takeIf { it.isNotBlank() }
        val fromFile = env["ROUTER_CONFIG_FILE"]?.takeIf { it.isNotBlank() }?.let { File(it).readText() }
        val raw = inline ?: fromFile ?: return null
        val upInterval = env["ROUTER_UP_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 300L
        val ingestConcurrency = env["ROUTER_INGEST_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 64) ?: 2
        val ingestBatch = env["ROUTER_INGEST_BATCH"]?.trim()?.toIntOrNull()?.coerceIn(1, 20_000) ?: 1000
        val fallback = RelaySourceDefaults()
        val relaySourceDefaults =
            RelaySourceDefaults(
                refreshSeconds = env["ROUTER_DYNAMIC_REFRESH_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(60L) ?: fallback.refreshSeconds,
                concurrency = env["ROUTER_DYNAMIC_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 256) ?: fallback.concurrency,
            )
        return parse(raw, upInterval, ingestConcurrency, ingestBatch, relaySourceDefaults)
    }

    fun parse(
        hocon: String,
        upIntervalSec: Long = 300L,
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
                val dynamic = parseDynamic(name, s, relaySourceDefaults)

                require(dynamic != null || s.hasPath("urls")) {
                    "router: stream '$name' has neither `urls` nor a `relaySource` list"
                }
                require(dynamic == null || urls.isEmpty()) {
                    "router: stream '$name' cannot mix `relaySource` with static `urls` — split them into two streams"
                }
                require(dynamic == null || dir == MirrorDirection.DOWN) {
                    "router: stream '$name' has a `relaySource`, which only pulls down — set dir = \"down\""
                }

                MirrorStream(
                    name = name,
                    dir = dir,
                    filter = parseFilter(s.getConfig("filter")),
                    urls = urls,
                    trusted = s.hasPath("trusted") && s.getBoolean("trusted"),
                    dynamic = dynamic,
                )
            }
        return RouterConfig(connTimeout, streams, upIntervalSec, ingestConcurrency, ingestBatch)
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

    /** The `relaySource = [ ... ]` list plus the stream-level knobs pacing its cycle. */
    private fun parseDynamic(
        stream: String,
        s: Config,
        defaults: RelaySourceDefaults,
    ): DynamicRelayList? {
        if (!s.hasPath("relaySource")) return null
        val sources = s.getConfigList("relaySource").map { parseRelaySource(stream, it) }
        require(sources.isNotEmpty()) { "router: stream '$stream' has an empty `relaySource` list" }
        return DynamicRelayList(
            sources = sources,
            refreshSeconds = (if (s.hasPath("refreshSeconds")) s.getLong("refreshSeconds") else defaults.refreshSeconds).coerceAtLeast(60L),
            concurrency = (if (s.hasPath("concurrency")) s.getInt("concurrency") else defaults.concurrency).coerceIn(1, 256),
            exclude = if (s.hasPath("exclude")) normalizeUrls(stream, s.getStringList("exclude")).toSet() else emptySet(),
        )
    }

    /** One `{ select = [ ], filter = { } }` entry of that list: what to pull out, and the scan to pull it from. */
    private fun parseRelaySource(
        stream: String,
        s: Config,
    ): RelaySource {
        require(s.hasPath("filter")) { "router: stream '$stream' has a relaySource entry with no `filter { }`" }
        require(s.hasPath("select")) { "router: stream '$stream' has a relaySource entry with no `select [ ]`" }
        val filter = parseFilter(s.getConfig("filter"))
        val selects = s.getConfigList("select").map { parseRelaySelect(stream, it) }
        require(selects.isNotEmpty()) { "router: stream '$stream' has a relaySource entry with an empty `select`" }

        val kinds = filter.kinds
        require(!kinds.isNullOrEmpty()) { "router: stream '$stream' relaySource filter needs `kinds`" }
        // A regular kind is unbounded — scanning all of kind 1 means loading every
        // note in the store into one list. The replaceable/addressable kinds are
        // one event per author (or per author+d), which is what makes them safe to
        // scan whole. Anything else has to narrow itself. `until` alone doesn't
        // count: it caps the top of the window and leaves all of history below it.
        val narrowed = filter.limit != null || filter.since != null || filter.authors != null || filter.ids != null
        require(narrowed || kinds.all { isBoundedKind(it) }) {
            "router: stream '$stream' relaySource filter ${kinds.joinToString("/")} scans a regular kind unbounded — " +
                "add `limit` (the bound that stays meaningful on a repeating cycle), `since`, or `authors`, " +
                "or it would load every matching event in the store at once"
        }
        return RelaySource(selects = selects, filter = filter)
    }

    /** One `{ kind = ..., tag = ..., index = ..., where = [ ] }` entry of a source's `select` list. */
    private fun parseRelaySelect(
        stream: String,
        s: Config,
    ): RelaySelect {
        val index = if (s.hasPath("index")) s.getInt("index") else 1
        require(index >= 1) {
            "router: stream '$stream' has a select with index $index — element 0 is the tag name, so the url is at 1 or later"
        }
        require(!(s.hasPath("marker") && s.hasPath("where"))) {
            "router: stream '$stream' has a select with both `marker` and `where` — `marker` is sugar for a `where`, write one or the other"
        }
        val where =
            when {
                s.hasPath("where") -> s.getConfigList("where").map { parseTagCondition(stream, index, it) }
                s.hasPath("marker") -> markerSugar(stream, index, s.getString("marker"))
                else -> emptyList()
            }
        return RelaySelect(
            // No kind: the select applies to everything the filter collected.
            kind = if (s.hasPath("kind")) s.getInt("kind") else null,
            tag = if (s.hasPath("tag")) s.getString("tag").trim().takeIf { it.isNotEmpty() } else null,
            index = index,
            where = where,
        )
    }

    /**
     * NIP-65's rule spelled as a `where`: keep tags marked the asked-for side,
     * marked empty, or too short to carry a marker at all — an unmarked `r` tag
     * is read *and* write. `any` keeps everything, so it expands to no
     * conditions.
     */
    private fun markerSugar(
        stream: String,
        urlIndex: Int,
        raw: String,
    ): List<TagCondition> =
        when (val v = raw.trim().lowercase()) {
            "write", "read" -> {
                listOf(
                    TagCondition(index = urlIndex + 1, equals = v),
                    TagCondition(index = urlIndex + 1, equals = ""),
                    TagCondition(maxSize = urlIndex + 1),
                )
            }

            "any", "both", "all" -> {
                emptyList()
            }

            else -> {
                error("router: stream '$stream' has an unknown relaySource marker '$v' (expected write / read / any)")
            }
        }

    /** One `{ index = ..., equals = ..., minSize = ..., maxSize = ... }` entry of a select's `where` list. */
    private fun parseTagCondition(
        stream: String,
        urlIndex: Int,
        c: Config,
    ): TagCondition {
        val index = if (c.hasPath("index")) c.getInt("index") else null
        val equals = if (c.hasPath("equals")) c.getString("equals") else null
        val minSize = if (c.hasPath("minSize")) c.getInt("minSize") else null
        val maxSize = if (c.hasPath("maxSize")) c.getInt("maxSize") else null
        require(equals != null || minSize != null || maxSize != null) {
            "router: stream '$stream' has a where entry with no predicate — set `equals` (with `index`), `minSize`, or `maxSize`"
        }
        require((equals == null) == (index == null)) {
            "router: stream '$stream' has a where entry with `index` and `equals` apart — they only mean something together"
        }
        require(index == null || index >= 0) {
            "router: stream '$stream' has a where entry with a negative index"
        }
        // The select itself already demands the url at urlIndex, so a bound the
        // url can't fit under is a condition that can never match.
        require(maxSize == null || maxSize >= urlIndex + 1) {
            "router: stream '$stream' has a where entry with maxSize $maxSize, but the url at index $urlIndex already needs ${urlIndex + 1} elements — it can never match"
        }
        require(minSize == null || maxSize == null || minSize <= maxSize) {
            "router: stream '$stream' has a where entry with minSize $minSize > maxSize $maxSize — it can never match"
        }
        // The same clash inside one entry: equals demands the element exist, so
        // its index has to fit under the entry's own maxSize.
        require(equals == null || maxSize == null || index!! < maxSize) {
            "router: stream '$stream' has a where entry whose equals at index $index needs ${index!! + 1} elements but maxSize is $maxSize — it can never match"
        }
        // And the mirror image: every tag reaching a where already has the url,
        // so a minSize at or under that floor holds for every tag — and one
        // always-true entry in an OR list silently disables the other entries.
        require(minSize == null || minSize > urlIndex + 1) {
            "router: stream '$stream' has a where entry with minSize $minSize, which the url at index $urlIndex already guarantees — it matches every tag"
        }
        return TagCondition(index = index, equals = equals, minSize = minSize, maxSize = maxSize)
    }

    /** Replaceable (0, 3, 10000-19999) and addressable (30000-39999) kinds hold one event per author. */
    private fun isBoundedKind(kind: Int): Boolean = kind == 0 || kind == 3 || kind in 10_000..19_999 || kind in 30_000..39_999

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
