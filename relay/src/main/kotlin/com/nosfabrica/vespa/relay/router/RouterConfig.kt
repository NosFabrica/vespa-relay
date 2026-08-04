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

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File

/**
 * The router config: strfry's `streams { }` model, parsed from HOCON, so an
 * existing strfry `routerConfigOverride` drops in unchanged:
 *
 *     connectionTimeout = 20
 *     streams {
 *       popular {
 *         dir    = "down"
 *         filter = { "kinds": [0,3,10002] }
 *         urls   = [ "wss://relay.primal.net", "wss://relay.damus.io" ]
 *       }
 *     }
 *
 * `dir` is `down` (mirror upstream events into our store), `up` (publish our
 * matching events upstream), or `both`. Beyond strfry's schema: `trusted`
 * (skip signature verification for this stream), `sync` ([SyncMode]),
 * `deleteMissing` ([DeleteMissing]) and `relaySource` ([DynamicRelayList]).
 *
 * How far back a stream reaches is the [MirrorStream.filter]'s own
 * `since`/`until`, exactly as NIP-01 reads them: absent is unbounded. The live
 * tail runs from connect forward but keeps the filter's `until`.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<MirrorStream>,
    // How often (seconds) an `up`/`both` stream re-reconciles to push newly
    // arrived local events. From ROUTER_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
    // Ingest tuning. The store serializes writes through one mutex, so
    // throughput comes from the batch size, not the worker count.
    // From ROUTER_INGEST_CONCURRENCY / ROUTER_INGEST_BATCH.
    val ingestConcurrency: Int = 2,
    val ingestBatch: Int = 1000,
    // How much overlap makes a negentropy reconcile worth its id exchange, and
    // how long a relay gets to answer the NIP-45 COUNT that measures it.
    // From ROUTER_NEG_MIN_EVENTS / ROUTER_COUNT_TIMEOUT_MS.
    val negMinEvents: Int = 100_000,
    val countTimeoutMs: Long = 5_000,
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
            .flatMap { s -> s.urls.map { MirrorUpstream(s.name, it, s.filter, s.trusted, s.sync) } }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class MirrorUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
    val sync: SyncMode = SyncMode.AUTO,
)

data class MirrorStream(
    val name: String,
    val dir: MirrorDirection,
    val filter: Filter,
    val urls: List<NormalizedRelayUrl>,
    val trusted: Boolean,
    // Null for an ordinary stream; set when its relays come out of the store.
    val dynamic: DynamicRelayList? = null,
    // Whether this stream's relays share events with each other — see [SyncMode].
    val sync: SyncMode = SyncMode.AUTO,
    // Whether an upstream dropping a record means we drop it too.
    val deleteMissing: DeleteMissing = DeleteMissing.OFF,
)

/**
 * What to do with records WE hold that the upstream no longer serves.
 *
 * Only meaningful when the upstream is the source of truth for its records —
 * a NIP-85 provider's own relay for its own scores. For a general mirror,
 * absence means almost nothing: relays hold different subsets by design.
 *
 * This asks and deletes; it never writes upstream (unlike NIP-09 propagation,
 * which would require uploading our events to read the rejections). Absence
 * has innocent causes — a retention window, AUTH-gated reads, an outage — so
 * [DRY_RUN] and the guardrails in the router are the safety net.
 */
enum class DeleteMissing {
    /** Never delete. The default, and correct for every ordinary mirror stream. */
    OFF,

    /** Report what would be deleted, delete nothing. */
    DRY_RUN,

    /** Delete. */
    ON,
}

/**
 * A stream's relay list, read from events our own store already holds instead
 * of a hand-written `urls` array. `relaySource` is a list of places to read
 * urls from — relay lists, trust-provider lists, relay hints — merged into one
 * fan-out:
 *
 *     outbox {
 *       dir            = "down"
 *       filter         = { "kinds": [0, 3, 10002] }
 *       refreshSeconds = 21600
 *       concurrency    = 8
 *       relaySource = [
 *         {
 *           select = [ { kind = 10002, tag = "r", marker = "write" } ]
 *           filter = { "kinds": [10002] }
 *         }
 *       ]
 *     }
 *
 * Every refresh the router runs each scan, unions the relays they name, and
 * syncs the stream filter against every one of them — nothing truncates the
 * set, [concurrency] only paces it. There is no live tail: a set this size is
 * synced on a period, not held open.
 *
 * @param sources every scan to read relay urls from, merged.
 * @param refreshSeconds how often the whole cycle runs again.
 * @param concurrency how many relays sync at the same time. A relay's sync has
 *   no wall-clock cap: every client timeout is measured from the last message,
 *   so a silent relay is dropped in seconds and a delivering one is doing the
 *   work the slot exists for.
 * @param exclude relays to skip however many sources name them.
 */
data class DynamicRelayList(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val concurrency: Int,
    val exclude: Set<NormalizedRelayUrl>,
    /**
     * How many bound `authors` go into ONE ask, and therefore into one cursor
     * band. Null keeps them all in a single filter.
     *
     * A band is keyed on its filter, so an author set that changes invalidates
     * it and re-walks that relay's history. At 1 the band is `(relay, one
     * author)` and stays valid forever — right for a small pairing like NIP-85
     * providers. An outbox stream pairing millions of authors has to chunk,
     * and accept that a chunk re-walks when its membership shifts.
     */
    val authorsPerLeg: Int? = null,
)

/**
 * One scan of the store: the [selects] saying which relay urls to pull out,
 * and a NIP-01 [filter] saying which events to pull them from. The filter runs
 * once and every select is applied to what it returns, so a whole shelf of
 * relay-list kinds costs one query rather than one each.
 */
data class RelaySource(
    val selects: List<RelaySelect>,
    val filter: Filter,
)

/**
 * Where a relay url sits in a tag. Every relay list in the protocol is some
 * tag with a url at a fixed offset, so this covers all of them with no
 * per-kind code.
 *
 * @param kind apply this select only to events of that kind, or null for
 *   everything the filter returned.
 * @param tag the tag name to read, or null for any tag — at the cost of a
 *   stricter url check, see [RelayDiscovery].
 * @param index which element of the tag holds the url. 1 for nearly
 *   everything; 2 for NIP-85 service tags and `e`/`p`/`a`/`q` relay hints.
 * @param where conditions on the rest of the tag, see [TagCondition]. The
 *   config's `marker = "write" / "read" / "any"` is sugar that expands into
 *   this list.
 */
data class RelaySelect(
    val kind: Int?,
    val tag: String?,
    val index: Int,
    val where: List<TagCondition> = emptyList(),
    /**
     * Extra NIP-01 filter fields read out of the SAME tag, so the relay this
     * select found is asked only for what that tag paired it with.
     *
     * Keyed by destination — `authors`, `ids`, `kinds`, or a `#x` tag filter.
     * A value is read per TAG OCCURRENCE, not gathered into a global set:
     * collecting the slots independently would produce the cross product
     * (measured: 5,928 asks standing in for the 256 pairs that exist).
     */
    val bindings: Map<String, Slot> = emptyMap(),
)

/**
 * Where one value of a binding comes from. Usually a slot in the tag being
 * read; [EventPubkey] is what makes NIP-65's outbox model expressible — "fetch
 * THIS AUTHOR's events from the relays their own 10002 marks write".
 */
sealed interface Slot {
    /** Element [index] of the tag this select matched. */
    data class OfTag(
        val index: Int,
    ) : Slot

    /** The scanned event's own author. */
    data object EventPubkey : Slot

    /** The scanned event's own id. */
    data object EventId : Slot
}

/**
 * One alternative in a select's `where` list: entries OR together, the fields
 * inside one entry AND — NIP-01's boolean shape pointed at a tag. A tag passes
 * an empty list outright.
 *
 * [equals] is exact — no case folding, no trimming — and an element that does
 * not exist matches nothing, not even `""`. NIP-65's write side (what
 * `marker = "write"` expands to) is: marked write, marked empty, or no marker
 * slot at all.
 *
 * @param index which element [equals] tests; the parser demands both or neither.
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

/**
 * How a stream asks a relay for what it is missing.
 *
 * A property of the DATA, which is why it is declared rather than measured:
 * negentropy pays for itself in proportion to how much of a relay's set we
 * already hold, and no count can reveal that.
 *
 *  - [NEGENTROPY] — the same event lives on many relays (profiles, relay
 *    lists, follow lists). Reconciling id sets moves almost nothing; paging
 *    moves all of it.
 *  - [FETCH] — each relay holds its own events and no one else's (NIP-85
 *    assertions are per-provider by construction). Comparing millions of ids
 *    for a near-empty intersection is the expensive way to learn that.
 *  - [AUTO] — decide by size (see [StaticBackfill.worthReconciling]). Safe
 *    only where overlap tracks volume.
 */
enum class SyncMode(
    val wire: String,
) {
    AUTO("auto"),
    NEGENTROPY("negentropy"),
    FETCH("fetch"),
    ;

    companion object {
        fun parse(raw: String): SyncMode =
            entries.firstOrNull { it.wire.equals(raw.trim(), ignoreCase = true) }
                ?: error("router: unknown stream sync '$raw' (expected auto / negentropy / fetch)")
    }
}

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
 * inline; `ROUTER_CONFIG_FILE` points at a file. Neither set ⇒ no router.
 * `ROUTER_DYNAMIC_REFRESH_SECONDS` / `ROUTER_DYNAMIC_CONCURRENCY` are the
 * defaults for dynamic streams; `ROUTER_STREAMS` narrows the run to a subset
 * of the config's streams (see [select]).
 */
object RouterConfigLoader {
    fun fromEnv(env: Map<String, String>): RouterConfig? {
        val inline = env["ROUTER_CONFIG"]?.takeIf { it.isNotBlank() }
        val fromFile = env["ROUTER_CONFIG_FILE"]?.takeIf { it.isNotBlank() }?.let { File(it).readText() }
        val raw = inline ?: fromFile ?: return null
        val upInterval = env["ROUTER_UP_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(10L) ?: 300L
        val ingestConcurrency = env["ROUTER_INGEST_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 64) ?: 2
        val ingestBatch = env["ROUTER_INGEST_BATCH"]?.trim()?.toIntOrNull()?.coerceIn(1, 20_000) ?: 1000
        val negMinEvents = env["ROUTER_NEG_MIN_EVENTS"]?.trim()?.toIntOrNull()?.coerceAtLeast(0) ?: 100_000
        val countTimeoutMs = env["ROUTER_COUNT_TIMEOUT_MS"]?.trim()?.toLongOrNull()?.coerceIn(500, 60_000) ?: 5_000
        val fallback = RelaySourceDefaults()
        val only = env["ROUTER_STREAMS"]?.trim()?.takeIf { it.isNotBlank() }
        val relaySourceDefaults =
            RelaySourceDefaults(
                refreshSeconds = env["ROUTER_DYNAMIC_REFRESH_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(60L) ?: fallback.refreshSeconds,
                concurrency = env["ROUTER_DYNAMIC_CONCURRENCY"]?.trim()?.toIntOrNull()?.coerceIn(1, 256) ?: fallback.concurrency,
            )
        return parse(raw, upInterval, ingestConcurrency, ingestBatch, relaySourceDefaults, negMinEvents, countTimeoutMs).let {
            if (only == null) it else it.copy(streams = select(it.streams, only))
        }
    }

    /**
     * `ROUTER_STREAMS=dataViaOutbox` — run only the named streams, so one part
     * of the sync can be measured without the others competing for the same
     * sockets, heap and ingest queue. A name that matches nothing is a hard
     * error: a typo would otherwise look exactly like a relay that mirrors
     * nothing.
     */
    fun select(
        streams: List<MirrorStream>,
        only: String,
    ): List<MirrorStream> {
        val wanted =
            only
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toSet()
        val known = streams.map { it.name }.toSet()
        val unknown = wanted - known
        require(unknown.isEmpty()) {
            "router: ROUTER_STREAMS names ${unknown.joinToString()}, which the config does not define (has: ${known.joinToString()})"
        }
        val (on, off) = streams.partition { it.name in wanted }
        // Said out loud, every startup: a stream that is off because someone
        // was measuring last week must never look like one that is failing.
        System.err.println(
            "router: ROUTER_STREAMS is set — running ${on.joinToString { it.name }};" +
                " NOT running ${off.joinToString { it.name }.ifEmpty { "nothing else" }}",
        )
        return on
    }

    fun parse(
        hocon: String,
        upIntervalSec: Long = 300L,
        ingestConcurrency: Int = 2,
        ingestBatch: Int = 1000,
        relaySourceDefaults: RelaySourceDefaults = RelaySourceDefaults(),
        negMinEvents: Int = 100_000,
        countTimeoutMs: Long = 5_000,
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
                    sync = if (s.hasPath("sync")) SyncMode.parse(s.getString("sync")) else SyncMode.AUTO,
                    deleteMissing = parseDeleteMissing(name, s),
                )
            }
        return RouterConfig(connTimeout, streams, upIntervalSec, ingestConcurrency, ingestBatch, negMinEvents, countTimeoutMs)
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

    /**
     * `deleteMissing = false | "dryRun" | true`.
     *
     * Refused outright on a `fetch` stream: a paged fetch asks only OUTSIDE
     * its cursor band, so "not seen" there mostly means "not asked for" —
     * deleting on it would take the whole history below the band. Only a
     * reconcile compares whole sets.
     */
    private fun parseDeleteMissing(
        stream: String,
        s: Config,
    ): DeleteMissing {
        if (!s.hasPath("deleteMissing")) return DeleteMissing.OFF
        val raw = s.getValue("deleteMissing").unwrapped()
        val mode =
            when (raw) {
                false -> {
                    DeleteMissing.OFF
                }

                true -> {
                    DeleteMissing.ON
                }

                "dryRun", "dryrun" -> {
                    DeleteMissing.DRY_RUN
                }

                else -> {
                    throw IllegalArgumentException(
                        "router: stream '$stream' has deleteMissing = '$raw' — expected true, false, or \"dryRun\"",
                    )
                }
            }
        if (mode != DeleteMissing.OFF) {
            val sync = if (s.hasPath("sync")) SyncMode.parse(s.getString("sync")) else SyncMode.AUTO
            require(sync == SyncMode.NEGENTROPY) {
                "router: stream '$stream' sets deleteMissing with sync = \"${sync.name.lowercase()}\" — it needs sync = \"negentropy\". " +
                    "A paged fetch asks only outside its cursor band, so \"not seen\" there means \"not asked for\", " +
                    "and deleting on it would take the whole history below the band"
            }
        }
        return mode
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
            authorsPerLeg = if (s.hasPath("authorsPerLeg")) s.getInt("authorsPerLeg").coerceAtLeast(1) else null,
        )
    }

    /** One `{ select = [ ], filter = { } }` entry: what to pull out, and the scan to pull it from. */
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
        // A regular kind is unbounded — scanning all of kind 1 means loading
        // every note in the store into one list. Replaceable/addressable kinds
        // are one event per author, which is what makes them safe to scan
        // whole. `until` alone doesn't narrow: it leaves all of history below.
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
        require(!(s.hasPath("index") && s.hasPath("relay"))) {
            "router: stream '$stream' has a select with both `index` and `relay` — they name the same slot, write one"
        }
        // `relay = N` is the name to use once a select binds more than one
        // field; `index = N` is the original spelling and keeps working.
        val index =
            when {
                s.hasPath("relay") -> s.getInt("relay")
                s.hasPath("index") -> s.getInt("index")
                else -> 1
            }
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
            bindings = parseBindings(stream, s),
        )
    }

    /**
     * The destination fields a select may bind, beyond the relay url itself. A
     * closed list on purpose: a typo that silently bound nothing would show up
     * as a stream quietly syncing the wrong thing. `#x` tag filters are
     * accepted for any single letter, as NIP-01 allows.
     */
    private val BINDABLE = setOf("authors", "ids", "kinds")

    private fun isBindable(key: String) = key in BINDABLE || (key.length == 2 && key[0] == '#' && key[1].isLetter())

    /**
     * `{ tag = "30382:rank", relay = 2, authors = 1 }` — which tag slot feeds
     * which filter field. A value is either an Int (that element of the tag)
     * or `"pubkey"`/`"id"` for the scanned event's own.
     */
    private fun parseBindings(
        stream: String,
        s: Config,
    ): Map<String, Slot> {
        val out = LinkedHashMap<String, Slot>()
        for (entry in s.root().keys) {
            if (!isBindable(entry)) continue
            // Quoted: a `#p` key is a HOCON path expression otherwise, and `#`
            // starts a comment there.
            val v = s.getValue(quote(entry)).unwrapped()
            val slot =
                when (v) {
                    is Number -> {
                        val i = v.toInt()
                        require(i >= 1) {
                            "router: stream '$stream' binds `$entry` to tag element $i — element 0 is the tag name, so a value is at 1 or later"
                        }
                        Slot.OfTag(i)
                    }

                    "pubkey" -> {
                        Slot.EventPubkey
                    }

                    "id" -> {
                        Slot.EventId
                    }

                    else -> {
                        throw IllegalArgumentException(
                            "router: stream '$stream' binds `$entry` to '$v' — expected a tag element number, or \"pubkey\"/\"id\" for the scanned event's own",
                        )
                    }
                }
            out[entry] = slot
        }
        return out
    }

    /**
     * NIP-65's rule spelled as a `where`: keep tags marked the asked-for side,
     * marked empty, or too short to carry a marker at all — an unmarked `r`
     * tag is read *and* write. `any` keeps everything.
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
        // The select itself already demands the url at urlIndex, so a bound
        // the url can't fit under is a condition that can never match.
        require(maxSize == null || maxSize >= urlIndex + 1) {
            "router: stream '$stream' has a where entry with maxSize $maxSize, but the url at index $urlIndex already needs ${urlIndex + 1} elements — it can never match"
        }
        require(minSize == null || maxSize == null || minSize <= maxSize) {
            "router: stream '$stream' has a where entry with minSize $minSize > maxSize $maxSize — it can never match"
        }
        // equals demands the element exist, so its index has to fit under the
        // entry's own maxSize.
        require(equals == null || maxSize == null || index!! < maxSize) {
            "router: stream '$stream' has a where entry whose equals at index $index needs ${index!! + 1} elements but maxSize is $maxSize — it can never match"
        }
        // Every tag reaching a where already has the url, so a minSize at or
        // under that floor holds for every tag — and one always-true entry in
        // an OR list silently disables the others.
        require(minSize == null || minSize > urlIndex + 1) {
            "router: stream '$stream' has a where entry with minSize $minSize, which the url at index $urlIndex already guarantees — it matches every tag"
        }
        return TagCondition(index = index, equals = equals, minSize = minSize, maxSize = maxSize)
    }

    /** Replaceable (0, 3, 10000-19999) and addressable (30000-39999) kinds hold one event per author. */
    private fun isBoundedKind(kind: Int): Boolean = kind == 0 || kind == 3 || kind in 10_000..19_999 || kind in 30_000..39_999

    /**
     * Turn a HOCON filter object (`{ "kinds": [0,3], "#t": [...] }`) into a
     * quartz [Filter]. Standard NIP-01 fields plus `#x` tag filters.
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
