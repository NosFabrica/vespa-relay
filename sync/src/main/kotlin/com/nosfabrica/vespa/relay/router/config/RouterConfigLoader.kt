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
package com.nosfabrica.vespa.relay.router.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File

/**
 * Read a SYNC_* setting, honoring its pre-rename ROUTER_* spelling. The old
 * name still works — a stale compose file must never silently disable the
 * sync engine — but it announces itself so the config gets updated.
 */
internal fun Map<String, String>.syncEnv(
    name: String,
    legacy: String,
): String? {
    this[name]?.let { return it }
    val value = this[legacy] ?: return null
    System.err.println("router: $legacy was renamed to $name — the old name still works; update your config")
    return value
}

/**
 * Loads [RouterConfig] from the environment. `SYNC_CONFIG` holds the HOCON
 * inline; `SYNC_CONFIG_FILE` points at a file. Neither set ⇒ null, and what
 * that means is the caller's: SyncMain refuses to start on it — a sync
 * process with nothing to sync is a misconfiguration, not a mode.
 * `SYNC_DYNAMIC_REFRESH_SECONDS` / `SYNC_DYNAMIC_CONCURRENCY` are the
 * defaults for dynamic streams; `SYNC_STREAMS` narrows the run to a subset
 * of the config's streams (see [select]).
 */
object RouterConfigLoader {
    fun fromEnv(env: Map<String, String>): RouterConfig? {
        val inline = env.syncEnv("SYNC_CONFIG", "ROUTER_CONFIG")?.takeIf { it.isNotBlank() }
        val fromFile = env.syncEnv("SYNC_CONFIG_FILE", "ROUTER_CONFIG_FILE")?.takeIf { it.isNotBlank() }?.let { File(it).readText() }
        val raw = inline ?: fromFile ?: return null
        val upInterval =
            env
                .syncEnv("SYNC_UP_INTERVAL_SECONDS", "ROUTER_UP_INTERVAL_SECONDS")
                ?.trim()
                ?.toLongOrNull()
                ?.coerceAtLeast(10L) ?: 300L
        val ingestConcurrency =
            env
                .syncEnv("SYNC_INGEST_CONCURRENCY", "ROUTER_INGEST_CONCURRENCY")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceIn(1, 64) ?: 2
        val ingestBatch =
            env
                .syncEnv("SYNC_INGEST_BATCH", "ROUTER_INGEST_BATCH")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceIn(1, 20_000) ?: 1000
        val negMinEvents =
            env
                .syncEnv("SYNC_NEG_MIN_EVENTS", "ROUTER_NEG_MIN_EVENTS")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceAtLeast(0) ?: 5_000
        val fallback = RelaySourceDefaults()
        val only = env.syncEnv("SYNC_STREAMS", "ROUTER_STREAMS")?.trim()?.takeIf { it.isNotBlank() }
        val relaySourceDefaults =
            RelaySourceDefaults(
                refreshSeconds =
                    env
                        .syncEnv("SYNC_DYNAMIC_REFRESH_SECONDS", "ROUTER_DYNAMIC_REFRESH_SECONDS")
                        ?.trim()
                        ?.toLongOrNull()
                        ?.coerceAtLeast(60L) ?: fallback.refreshSeconds,
                concurrency =
                    env
                        .syncEnv("SYNC_DYNAMIC_CONCURRENCY", "ROUTER_DYNAMIC_CONCURRENCY")
                        ?.trim()
                        ?.toIntOrNull()
                        ?.coerceIn(1, 256) ?: fallback.concurrency,
            )
        return parse(raw, upInterval, ingestConcurrency, ingestBatch, relaySourceDefaults, negMinEvents).let {
            if (only == null) it else it.copy(streams = select(it.streams, only))
        }
    }

    /**
     * `SYNC_STREAMS=contentViaOutbox` — run only the named streams, so one part
     * of the sync can be measured without the others competing for the same
     * sockets, heap and ingest queue. A name that matches nothing is a hard
     * error: a typo would otherwise look exactly like a relay that mirrors
     * nothing.
     */
    fun select(
        streams: List<SyncStream>,
        only: String,
    ): List<SyncStream> {
        val wanted =
            only
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toSet()
        val known = streams.map { it.name }.toSet()
        val unknown = wanted - known
        require(unknown.isEmpty()) {
            "router: SYNC_STREAMS names ${unknown.joinToString()}, which the config does not define (has: ${known.joinToString()})"
        }
        val (on, off) = streams.partition { it.name in wanted }
        // Said out loud, every startup: a stream that is off because someone
        // was measuring last week must never look like one that is failing.
        System.err.println(
            "router: SYNC_STREAMS is set — running ${on.joinToString { it.name }};" +
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
        negMinEvents: Int = 5_000,
    ): RouterConfig {
        val cfg = ConfigFactory.parseString(hocon)
        val connTimeout = if (cfg.hasPath("connectionTimeout")) cfg.getLong("connectionTimeout") else 20L
        require(cfg.hasPath("streams")) { "router: config has no `streams { }` block" }
        val streamsCfg = cfg.getConfig("streams")
        val streams =
            streamsCfg.root().keys.map { name ->
                val s = streamsCfg.getConfig(quote(name))
                val urls = if (s.hasPath("urls")) normalizeUrls(name, s.getStringList("urls")) else emptyList()
                val dir = SyncDirection.parse(if (s.hasPath("dir")) s.getString("dir") else "down")
                val dynamic = parseDynamic(name, s, relaySourceDefaults)

                require(dynamic != null || s.hasPath("urls")) {
                    "router: stream '$name' has neither `urls` nor a `relaySource` list"
                }
                require(dynamic == null || urls.isEmpty()) {
                    "router: stream '$name' cannot mix `relaySource` with static `urls` — split them into two streams"
                }
                require(dynamic == null || dir == SyncDirection.DOWN) {
                    "router: stream '$name' has a `relaySource`, which only pulls down — set dir = \"down\""
                }

                val filter = parseFilter(s.getConfig("filter"))
                val deleteMissing = parseDeleteMissing(name, s)
                SyncStream(
                    name = name,
                    dir = dir,
                    filter = filter,
                    urls = urls,
                    trusted = s.hasPath("trusted") && s.getBoolean("trusted"),
                    dynamic = dynamic,
                    sync = if (s.hasPath("sync")) SyncMode.parse(s.getString("sync")) else SyncMode.AUTO,
                    deleteMissing = deleteMissing,
                    ownedKinds = parseOwnedKinds(name, s, filter, deleteMissing),
                )
            }
        return RouterConfig(connTimeout, streams, upIntervalSec, ingestConcurrency, ingestBatch, negMinEvents)
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
     * its band, so "not seen" there mostly means "not asked for" —
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
                    "A paged fetch asks only outside its band, so \"not seen\" there means \"not asked for\", " +
                    "and deleting on it would take the whole history below the band"
            }
        }
        return mode
    }

    /**
     * `ownedKinds = [30382]` — which of the stream's kinds its upstreams are
     * authoritative for, and therefore the only ones absence may delete.
     *
     * Required, non-empty, and a subset of the filter's kinds whenever
     * `deleteMissing` is on: the whole point is that turning on deletion makes
     * you write down what it is allowed to reach, in the same file. Refused
     * outright when deletion is off, so a stream can never carry a stale
     * licence that a later `deleteMissing = true` silently activates.
     */
    private fun parseOwnedKinds(
        stream: String,
        s: Config,
        filter: Filter,
        deleteMissing: DeleteMissing,
    ): Set<Int> {
        val declared = if (s.hasPath("ownedKinds")) s.getIntList("ownedKinds").map { it.toInt() }.toSet() else null
        if (deleteMissing == DeleteMissing.OFF) {
            require(declared == null) {
                "router: stream '$stream' sets ownedKinds without deleteMissing — remove one. " +
                    "A licence to delete sitting in a stream that does not delete is a trap for whoever turns it on"
            }
            return emptySet()
        }
        require(!declared.isNullOrEmpty()) {
            "router: stream '$stream' sets deleteMissing but no ownedKinds. " +
                "Name the kinds its upstreams are the source of truth for — e.g. ownedKinds = [30382] — " +
                "because every other kind in the filter would otherwise be deleted for being absent from a relay " +
                "that was never supposed to serve it"
        }
        // A kind-less filter means "every kind", and then the attached set —
        // what deletion must NOT touch — has no enumerable shape to protect.
        val streamKinds =
            requireNotNull(filter.kinds) {
                "router: stream '$stream' sets deleteMissing on a filter with no `kinds` — " +
                    "it would own some kinds and delete from an open-ended rest. List the kinds it mirrors"
            }
        val stray = declared - streamKinds.toSet()
        require(stray.isEmpty()) {
            "router: stream '$stream' owns kind(s) ${stray.sorted()} that its filter never asks for " +
                "(filter kinds: ${streamKinds.sorted()}) — nothing would ever be compared, let alone deleted"
        }
        return declared
    }

    /** The `relaySource = [ ... ]` list plus the stream-level knobs pacing its cycle. */
    private fun parseDynamic(
        stream: String,
        s: Config,
        defaults: RelaySourceDefaults,
    ): RelayDiscoveryConfig? {
        if (!s.hasPath("relaySource")) return null
        val sources = s.getConfigList("relaySource").map { parseRelaySource(stream, it) }
        require(sources.isNotEmpty()) { "router: stream '$stream' has an empty `relaySource` list" }
        return RelayDiscoveryConfig(
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
