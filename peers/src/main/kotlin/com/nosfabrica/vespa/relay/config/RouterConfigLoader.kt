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
package com.nosfabrica.vespa.relay.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import java.io.File
import java.util.regex.PatternSyntaxException

/** Read a SYNC_* setting, honoring its pre-rename ROUTER_* spelling with a nudge on stderr. */
fun Map<String, String>.syncEnv(
    name: String,
    // Oldest last: the first spelling set wins.
    vararg legacy: String,
): String? {
    this[name]?.let { return it }
    for (old in legacy) {
        val value = this[old] ?: continue
        System.err.println("router: $old was renamed to $name — the old name still works; update your config")
        return value
    }
    return null
}

/**
 * Loads [RouterConfig] from the environment: `SYNC_CONFIG` holds the HOCON inline,
 * `SYNC_CONFIG_FILE` points at a file. Neither set is null. `SYNC_STREAMS` narrows the run.
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
        // Removed settings are refused, never ignored.
        require(env["SYNC_NEG_MIN_EVENTS"].isNullOrBlank() && env["ROUTER_NEG_MIN_EVENTS"].isNullOrBlank()) {
            "router: SYNC_NEG_MIN_EVENTS is set — it sized the `auto` transport choice, and there is no transport " +
                "choice any more: the pool pages forward and reconciles the past on its own clock. Unset it"
        }
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
            )
        val pageTarget =
            env
                .syncEnv("SYNC_NEG_PAGE_TARGET", "ROUTER_NEG_PAGE_TARGET")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceAtLeast(0) ?: 100_000
        val pageMin =
            env
                .syncEnv("SYNC_NEG_PAGE_MIN", "ROUTER_NEG_PAGE_MIN")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceAtLeast(1) ?: 1_000
        val pageMax =
            env
                .syncEnv("SYNC_NEG_PAGE_MAX", "ROUTER_NEG_PAGE_MAX")
                ?.trim()
                ?.toIntOrNull()
                ?.coerceAtLeast(pageMin) ?: 1_000_000
        val pageSlack =
            env
                .syncEnv("SYNC_NEG_PAGE_SLACK_SECONDS", "ROUTER_NEG_PAGE_SLACK_SECONDS")
                ?.trim()
                ?.toLongOrNull()
                ?.coerceAtLeast(0L) ?: 60L
        return parse(raw, upInterval, ingestConcurrency, ingestBatch, relaySourceDefaults)
            .copy(
                negPageTarget = pageTarget,
                negPageMin = pageMin,
                negPageMax = pageMax.coerceAtLeast(pageMin),
                negPageSlackSec = pageSlack,
            ).let {
                if (only == null) it else it.copy(streams = narrowToStreams(it.streams, only))
            }
    }

    /** Run only the streams `SYNC_STREAMS` names. A name that matches nothing is a hard error. */
    fun narrowToStreams(
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
        // Said every startup: a stream switched off must never look like one that is failing.
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
                val discovery = parseDiscovery(name, s, relaySourceDefaults)

                require(discovery != null || s.hasPath("urls")) {
                    "router: stream '$name' has neither `urls` nor a `relaySource` list"
                }
                require(discovery == null || urls.isEmpty()) {
                    "router: stream '$name' cannot mix `relaySource` with static `urls` — split them into two streams"
                }
                require(discovery == null || dir == SyncDirection.DOWN) {
                    "router: stream '$name' has a `relaySource`, which only pulls down — set dir = \"down\""
                }

                val filter = parseFilter(s.getConfig("filter"))
                val deleteMissing = parseDeleteMissing(name, s)
                require(!s.hasPath("sync")) {
                    "router: stream '$name' sets `sync` — gone with the legacy backfill. Every stream is visited " +
                        "the same way now: page forward from the band's edge, live-tail, and re-check the past on " +
                        "`negentropySyncThePastSeconds` (reconcile) and `refetchThePastSeconds` (re-fetch)"
                }
                // `auditSeconds` and `verifySeconds` are the knob's older names.
                val negentropySyncThePastSeconds =
                    when {
                        s.hasPath("negentropySyncThePastSeconds") -> {
                            s.getLong("negentropySyncThePastSeconds")
                        }

                        s.hasPath("auditSeconds") -> {
                            System.err.println(
                                "router: stream '$name' uses auditSeconds — renamed to negentropySyncThePastSeconds " +
                                    "(it clocks the reconcile of the whole past, against relays that answer a NEG-OPEN); " +
                                    "the old name still works",
                            )
                            s.getLong("auditSeconds")
                        }

                        s.hasPath("verifySeconds") -> {
                            System.err.println(
                                "router: stream '$name' uses verifySeconds — renamed to negentropySyncThePastSeconds; " +
                                    "the old name still works",
                            )
                            s.getLong("verifySeconds")
                        }

                        else -> {
                            null
                        }
                    }?.coerceAtLeast(3600L)
                // Warned about at or below the audit, where it re-downloads what the audit would reconcile.
                val refetchThePastSeconds =
                    if (s.hasPath("refetchThePastSeconds")) {
                        s.getLong("refetchThePastSeconds").coerceAtLeast(3600L).also {
                            if (negentropySyncThePastSeconds != null && it <= negentropySyncThePastSeconds) {
                                System.err.println(
                                    "router: stream '$name' re-fetches the past every ${it}s but negentropy-syncs it " +
                                        "every ${negentropySyncThePastSeconds}s — the re-fetch pages the whole history the " +
                                        "reconcile was about to compare for the difference alone, so the reconcile can " +
                                        "never be the cheaper path",
                                )
                            }
                        }
                    } else {
                        null
                    }
                if (deleteMissing != DeleteMissing.OFF) {
                    // The comparison runs as the pool's audit, so it needs a relay list and the audit clock.
                    require(discovery != null) {
                        "router: stream '$name' sets deleteMissing without a `relaySource` — the retraction " +
                            "comparison runs as the pool's audit, over asks a scan paired with their owners"
                    }
                    require(negentropySyncThePastSeconds != null) {
                        "router: stream '$name' sets deleteMissing without `negentropySyncThePastSeconds` — " +
                            "the retraction comparison IS that reconcile, and that knob is its clock"
                    }
                    // The delete's licence is per (relay, provider), so every source must bind `authors`.
                    discovery.sources.forEach { source ->
                        require(source.selects.isNotEmpty() && source.selects.all { it.bindings.containsKey("authors") }) {
                            "router: stream '$name' sets deleteMissing but a relaySource select binds no `authors` — " +
                                "the retraction only ever judges a (relay, provider) pairing, and a select without an " +
                                "`authors` binding produces an unbound ask that would judge every provider at once. " +
                                "Add `authors = <tag slot>` to every select on this stream"
                        }
                    }
                }
                SyncStream(
                    name = name,
                    dir = dir,
                    filter = filter,
                    urls = urls,
                    trusted = s.hasPath("trusted") && s.getBoolean("trusted"),
                    discovery = discovery,
                    deleteMissing = deleteMissing,
                    ownedKinds = parseOwnedKinds(name, s, filter, deleteMissing),
                    refetchThePastSeconds = refetchThePastSeconds,
                    healContent = s.hasPath("healContent") && s.getBoolean("healContent"),
                    healRetractions = s.hasPath("healRetractions") && s.getBoolean("healRetractions"),
                    negentropySyncThePastSeconds = negentropySyncThePastSeconds,
                    refetchConcurrency = cap(s, "refetchConcurrency"),
                    negentropyConcurrency = cap(s, "negentropyConcurrency"),
                    maxLiveConcurrency = cap(s, "maxLiveConcurrency"),
                    visitConcurrency = cap(s, "visitConcurrency"),
                )
            }
        // Advisory: a kind deleted here and mirrored there is re-mirrored and re-deleted every audit.
        for (retracting in streams.filter { it.deleteMissing != DeleteMissing.OFF }) {
            for (other in streams) {
                if (other.name == retracting.name) continue
                val overlap =
                    retracting.ownedKinds intersect
                        other.filter.kinds
                            .orEmpty()
                            .toSet()
                if (overlap.isNotEmpty()) {
                    System.err.println(
                        "router: stream '${retracting.name}' deletes kind(s) $overlap that stream '${other.name}' also mirrors — " +
                            "a retracted record can be re-mirrored there and re-deleted on every audit",
                    )
                }
            }
        }
        refuseRouterWidePoolWidths(cfg)
        return RouterConfig(
            connTimeout,
            streams,
            upIntervalSec,
            ingestConcurrency,
            ingestBatch,
            monitor = parseMonitor(cfg),
        )
    }

    /** The pool's socket widths live inside the streams; the old top-level spelling is refused, not dropped. */
    private fun refuseRouterWidePoolWidths(cfg: Config) {
        require(!cfg.hasPath("visitConcurrency")) {
            "router: top-level `visitConcurrency` — moved inside each stream, because the four jobs a visit does " +
                "are budgeted per stream and its dial width belongs beside them. The pool's worker count is now " +
                "the sum of the streams' own `visitConcurrency`"
        }
        require(!cfg.hasPath("tailBudget")) {
            "router: top-level `tailBudget` — moved inside each stream and renamed `maxLiveConcurrency`. Every " +
                "stream says how many live subscriptions it may keep open, and their sum bounds what this process " +
                "holds"
        }
    }

    /** One of a stream's workload caps: absent, or at least one. Zero is floored, not honored. */
    private fun cap(
        cfg: Config,
        path: String,
    ): Int? = if (cfg.hasPath(path)) cfg.getInt(path).coerceAtLeast(1) else null

    /** The `monitor { }` block. A monitor source is a relay source, so it reuses the stream-side parsers. */
    private fun parseMonitor(cfg: Config): MonitorConfig? {
        if (!cfg.hasPath("monitor")) return null
        val m = cfg.getConfig("monitor")
        val sources =
            if (m.hasPath("sources")) {
                m.getConfigList("sources").map { parseRelaySource("monitor", it) }
            } else {
                emptyList()
            }
        return MonitorConfig(
            sources = sources,
            exclude = if (m.hasPath("exclude")) parseExcludes("monitor", m.getStringList("exclude")) else RelayExcludes.NONE,
            sweepSeconds =
                (if (m.hasPath("sweepSeconds")) m.getLong("sweepSeconds") else MonitorConfig.DEFAULT_SWEEP_SECONDS)
                    .coerceAtLeast(300L),
            fastLaneSeconds =
                run {
                    // `newUrlSeconds` is the knob's old name.
                    val key =
                        when {
                            m.hasPath("fastLaneSeconds") -> {
                                "fastLaneSeconds"
                            }

                            m.hasPath("newUrlSeconds") -> {
                                System.err.println("router: monitor uses newUrlSeconds — renamed to fastLaneSeconds; the old name still works")
                                "newUrlSeconds"
                            }

                            else -> {
                                null
                            }
                        }
                    when {
                        key == null -> MonitorConfig.DEFAULT_FAST_LANE_SECONDS

                        // 0 is the documented off switch.
                        m.getLong(key) <= 0L -> null

                        else -> m.getLong(key).coerceAtLeast(30L)
                    }
                },
            // Floored at 1: zero dials is an off switch no operator asked this knob to be.
            dialConcurrency =
                when {
                    m.hasPath("dialConcurrency") -> {
                        m.getInt("dialConcurrency").coerceAtLeast(1)
                    }

                    m.hasPath("concurrency") -> {
                        System.err.println("router: monitor uses concurrency — renamed to dialConcurrency (it bounds the probe passes' dials); the old name still works")
                        m.getInt("concurrency").coerceAtLeast(1)
                    }

                    else -> {
                        MonitorConfig.DEFAULT_DIAL_CONCURRENCY
                    }
                },
        )
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

    /** `deleteMissing = false | "dryRun" | true`. */
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
        return mode
    }

    /**
     * The kinds the upstreams are authoritative for. Required, non-empty and a subset of the
     * filter's kinds when `deleteMissing` is on; refused when it is off.
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
        // A kind-less filter is every kind, so the set deletion must not touch would have no shape.
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

    /** The `relaySource = [ ... ]` list plus the stream-level knobs pacing its discovery. */
    private fun parseDiscovery(
        stream: String,
        s: Config,
        defaults: RelaySourceDefaults,
    ): RelayDiscoveryConfig? {
        if (!s.hasPath("relaySource")) return null
        require(!s.hasPath("authorsPerLeg")) {
            "router: stream '$stream' sets authorsPerLeg — gone with the cycle engine. The pool makes one ask " +
                "per bound author, which is what authorsPerLeg = 1 configured; other values invalidated bands"
        }
        require(!s.hasPath("catchUpConcurrency")) {
            "router: stream '$stream' sets catchUpConcurrency — removed, because `visitConcurrency` already " +
                "bounds it. A catch-up runs INSIDE a visit and one visit walks its legs one at a time, so a " +
                "separate cap could only bite below the dial width, and then only by making an already-dialled " +
                "visit do less work. Lower `visitConcurrency` instead"
        }
        require(!s.hasPath("concurrency")) {
            "router: stream '$stream' sets a per-stream concurrency — gone with the cycle engine. The dial " +
                "width is this stream's own `visitConcurrency`, and the pool runs the SUM of them"
        }
        require(!s.hasPath("recycleSeconds")) {
            "router: stream '$stream' sets recycleSeconds — gone with the cycle engine. The pool has no laps: " +
                "revisits are paced per relay by recent yield, and `refreshSeconds` paces rediscovery"
        }
        require(!s.hasPath("sync")) {
            "router: stream '$stream' sets `sync` beside a relaySource — the pool has one shape for every " +
                "stream: page forward from the band's edge, reconcile the whole past on the " +
                "negentropySyncThePastSeconds " +
                "audit. `sync` chooses transport for static `urls` streams only; writing it here would claim " +
                "a choice nothing reads"
        }
        val sources = s.getConfigList("relaySource").map { parseRelaySource(stream, it) }
        require(sources.isNotEmpty()) { "router: stream '$stream' has an empty `relaySource` list" }
        val gatedBy =
            if (s.hasPath("gatedBy")) {
                s.getConfigList("gatedBy").map { parseRelaySource(stream, it, what = "gatedBy") }.also {
                    require(it.isNotEmpty()) {
                        "router: stream '$stream' has an empty `gatedBy` — leave it off to gate on nothing, since " +
                            "an empty list and no list would otherwise be the same text for opposite intents"
                    }
                }
            } else {
                emptyList()
            }
        // Said, not refused: which tag and value constitute a vouching is the operator's knowledge.
        if (gatedBy.isEmpty()) {
            System.err.println(
                "router: stream '$stream' has no `gatedBy` — every url its relaySource names will be dialled, " +
                    "and a relay list is as writable as the event carrying it. Right where the sources are " +
                    "already somebody's vetted list; otherwise gate the stream.",
            )
        }
        return RelayDiscoveryConfig(
            sources = sources,
            gatedBy = gatedBy,
            refreshSeconds = (if (s.hasPath("refreshSeconds")) s.getLong("refreshSeconds") else defaults.refreshSeconds).coerceAtLeast(60L),
            exclude = if (s.hasPath("exclude")) parseExcludes(stream, s.getStringList("exclude")) else RelayExcludes.NONE,
            maxRelaysPerList = if (s.hasPath("maxRelaysPerList")) s.getInt("maxRelaysPerList").coerceAtLeast(1) else null,
        )
    }

    /** Compiled here so a broken regex refuses the config naming the stream, not mid-cycle. */
    private fun parseExcludes(
        stream: String,
        raw: List<String>,
    ): RelayExcludes =
        try {
            RelayExcludes.parse(raw) { url ->
                System.err.println("router: stream '$stream' skips invalid exclude url '$url'")
            }
        } catch (e: PatternSyntaxException) {
            throw IllegalArgumentException(
                "router: stream '$stream' has an exclude entry that does not compile as a regex — ${e.message}",
            )
        }

    /** One `{ select = [ ], filter = { } }` entry, for a stream's `relaySource` and its `gatedBy` alike. */
    private fun parseRelaySource(
        stream: String,
        s: Config,
        what: String = "relaySource",
    ): RelaySource {
        require(s.hasPath("filter")) { "router: stream '$stream' has a $what entry with no `filter { }`" }
        val written = parseFilter(s.getConfig("filter"))
        val kinds = written.kinds
        require(!kinds.isNullOrEmpty()) { "router: stream '$stream' $what filter needs `kinds`" }
        // NIP-66 fixes the url in a 30166's `d` tag; nothing else about its tags is ours to read.
        val isNip66Record = kinds == listOf(RelayDiscoveryEvent.KIND)
        val filter = written
        require(!(s.hasPath("maxAgeSeconds") && (filter.since != null || filter.until != null))) {
            "router: stream '$stream' bounds a $what entry with BOTH `maxAgeSeconds` and since/until — they are " +
                "two spellings of one bound and the relative one wins, so the absolute one would be read by a " +
                "human and by nothing else"
        }
        // Only replaceable and addressable kinds may be scanned whole. Checked on what was written.
        val narrowed =
            filter.limit != null || filter.since != null || filter.authors != null || filter.ids != null || s.hasPath("maxAgeSeconds")
        require(narrowed || kinds.all { isBoundedKind(it) }) {
            "router: stream '$stream' $what filter ${kinds.joinToString("/")} scans a regular kind unbounded — " +
                "add `limit` (the bound that stays meaningful on a repeating cycle), `maxAgeSeconds`, `since` or " +
                "`authors`, or it would load every matching event in the store at once"
        }

        val selects =
            if (s.hasPath("select")) {
                s.getConfigList("select").map { parseRelaySelect(stream, it) }.also {
                    require(it.isNotEmpty()) { "router: stream '$stream' has a $what entry with an empty `select`" }
                }
            } else {
                require(isNip66Record) {
                    "router: stream '$stream' has a $what entry over kinds ${kinds.joinToString("/")} with no " +
                        "`select` — only kind ${RelayDiscoveryEvent.KIND} has its url fixed by the protocol " +
                        "(the `d` tag); say where the urls sit"
                }
                listOf(RelaySelect(kind = RelayDiscoveryEvent.KIND, tag = "d", urlIndex = 1))
            }
        require(!s.hasPath("certified")) {
            "router: stream '$stream' uses `certified { }`, which is gone — it could only ever mean `a fresh " +
                "prime from our own monitor`, and both halves of that are now expressible. Write the gate as " +
                "a stream-level `gatedBy = [ { filter = { \"kinds\": [${RelayDiscoveryEvent.KIND}], " +
                "\"#l\": [\"prime\"] } } ]` — naming whatever tag and value the monitor you trust writes"
        }
        require(!s.hasPath("resultsFilteredBy")) {
            "router: stream '$stream' puts `resultsFilteredBy` on a $what entry — it was renamed to `gatedBy` and " +
                "moved beside `exclude` on the stream, because which urls may be dialled is not a property of " +
                "how one was discovered"
        }
        return RelaySource(
            selects = selects,
            filter = filter,
            maxAgeSeconds = if (s.hasPath("maxAgeSeconds")) s.getLong("maxAgeSeconds").coerceAtLeast(60L) else null,
            refreshSeconds = if (s.hasPath("refreshSeconds")) s.getLong("refreshSeconds").coerceAtLeast(10L) else null,
        )
    }

    /** One `{ kind = ..., tag = ..., index = ..., where = [ ] }` entry of a source's `select` list. */
    private fun parseRelaySelect(
        stream: String,
        s: Config,
    ): RelaySelect {
        require(!(s.hasPath("index") && s.hasPath("relay"))) {
            "router: stream '$stream' has a select with both `index` and `relay` — they name the same slot, write one"
        }
        // `index = N` is the original spelling of `relay = N`.
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
            kind = if (s.hasPath("kind")) s.getInt("kind") else null,
            tag = if (s.hasPath("tag")) s.getString("tag").trim().takeIf { it.isNotEmpty() } else null,
            urlIndex = index,
            where = where,
            bindings = parseBindings(stream, s),
        )
    }

    /** The fields a select may bind. A closed list, so a typo cannot bind nothing quietly. */
    private val BINDABLE = setOf("authors", "ids", "kinds")

    private fun isBindable(key: String) = key in BINDABLE || (key.length == 2 && key[0] == '#' && key[1].isLetter())

    /**
     * Which tag slot feeds which filter field. A value is an Int (that element of the tag) or
     * `"pubkey"`/`"id"` for the scanned event's own.
     */
    private fun parseBindings(
        stream: String,
        s: Config,
    ): Map<String, BindingSlot> {
        val out = LinkedHashMap<String, BindingSlot>()
        for (entry in s.root().keys) {
            if (!isBindable(entry)) continue
            // Quoted: unquoted, a `#p` key is a HOCON path expression and `#` starts a comment.
            val v = s.getValue(quote(entry)).unwrapped()
            val slot =
                when (v) {
                    is Number -> {
                        val i = v.toInt()
                        require(i >= 1) {
                            "router: stream '$stream' binds `$entry` to tag element $i — element 0 is the tag name, so a value is at 1 or later"
                        }
                        BindingSlot.OfTag(i)
                    }

                    "pubkey" -> {
                        BindingSlot.EventPubkey
                    }

                    "id" -> {
                        BindingSlot.EventId
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
     * NIP-65's rule spelled as a `where`: keep tags marked the asked-for side, marked empty, or
     * too short to carry a marker, since an unmarked `r` tag is read and write.
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
        // A bound the url at urlIndex cannot fit under can never match.
        require(maxSize == null || maxSize >= urlIndex + 1) {
            "router: stream '$stream' has a where entry with maxSize $maxSize, but the url at index $urlIndex already needs ${urlIndex + 1} elements — it can never match"
        }
        require(minSize == null || maxSize == null || minSize <= maxSize) {
            "router: stream '$stream' has a where entry with minSize $minSize > maxSize $maxSize — it can never match"
        }
        // equals demands the element exist, so its index has to fit under the entry's own maxSize.
        require(equals == null || maxSize == null || index!! < maxSize) {
            "router: stream '$stream' has a where entry whose equals at index $index needs ${index!! + 1} elements but maxSize is $maxSize — it can never match"
        }
        // A minSize the url already guarantees is always true, which silently disables the OR list.
        require(minSize == null || minSize > urlIndex + 1) {
            "router: stream '$stream' has a where entry with minSize $minSize, which the url at index $urlIndex already guarantees — it matches every tag"
        }
        return TagCondition(index = index, equals = equals, minSize = minSize, maxSize = maxSize)
    }

    /** Replaceable (0, 3, 10000-19999) and addressable (30000-39999) kinds hold one event per author. */
    private fun isBoundedKind(kind: Int): Boolean = kind == 0 || kind == 3 || kind in 10_000..19_999 || kind in 30_000..39_999

    /** Turn a HOCON filter object into a quartz [Filter]: standard NIP-01 fields plus `#x` tag filters. */
    fun parseFilter(f: Config): Filter {
        fun strs(k: String) = if (f.hasPath(quote(k))) f.getStringList(quote(k)) else null

        fun ints(k: String) = if (f.hasPath(quote(k))) f.getIntList(quote(k)).map { it.toInt() } else null

        // Relays answer a negative bound with a killed subscription, silence, or their newest page.
        fun nonNegative(
            key: String,
            value: Long?,
        ): Long? =
            value?.also {
                require(it >= 0) {
                    "router: filter at ${f.origin().description()} has `$key = $it` — " +
                        "NIP-01 has no negative timestamps or limits, and relays answer one with " +
                        "a killed subscription, silence, or their newest events instead"
                }
            }

        /** `since = 0` is the absence of a floor, normalised to null; `until = 0` is a real bound. */
        fun epochAsAbsent(value: Long?): Long? = value?.takeIf { it != 0L }

        val tags =
            f
                .root()
                .keys
                .filter { it.startsWith("#") && it.length == 2 }
                .associate { it.substring(1) to f.getStringList(quote(it)) }
                .ifEmpty { null }

        val since = epochAsAbsent(nonNegative("since", if (f.hasPath("since")) f.getLong("since") else null))
        val until = nonNegative("until", if (f.hasPath("until")) f.getLong("until") else null)
        // The empty page a relay answers an inverted window with would be recorded as a settled past.
        require(since == null || until == null || since <= until) {
            "router: filter at ${f.origin().description()} has `since = $since` after `until = $until` — " +
                "an inverted window matches nothing, and a relay answers it with the empty page that " +
                "makes this repo record the history it never walked as settled"
        }

        return Filter(
            ids = hexKeys(f, "ids", strs("ids")),
            authors = hexKeys(f, "authors", strs("authors")),
            kinds = ints("kinds"),
            tags = tags,
            since = since,
            until = until,
            // getInt, not getLong().toInt(): HOCON range-checks the int. Zero stays legal.
            limit = if (f.hasPath("limit")) f.getInt("limit").also { nonNegative("limit", it.toLong()) } else null,
            search = if (f.hasPath("search")) f.getString("search") else null,
        )
    }

    /**
     * A filter's `ids` and `authors`, validated as raw hex: a `filter { }` block is a NIP-01
     * filter, so bech32 is refused rather than decoded. An `nsec1` is called out by name.
     */
    private fun hexKeys(
        f: Config,
        field: String,
        raw: List<String>?,
    ): List<String>? =
        raw?.map { entry ->
            val key = entry.trim().lowercase()
            require(!key.startsWith("nsec1")) {
                "router: filter at ${f.origin().description()} has an nsec in `$field` — that is a PRIVATE key, " +
                    "and it does not belong in a config at all, let alone in a NIP-01 filter"
            }
            require(!key.startsWith("npub1") && !key.startsWith("nprofile1") && !key.startsWith("note1") && !key.startsWith("nevent1")) {
                "router: filter at ${f.origin().description()} has a bech32 `$field` entry — a `filter { }` block " +
                    "IS a NIP-01 filter and NIP-01 speaks hex, so this one is the 64-character hex. Bech32 stays " +
                    "in the settings that are ours to define, not inside the protocol's own object"
            }
            require(key.length == 64 && key.all { it in "0123456789abcdef" }) {
                "router: filter at ${f.origin().description()} has `$field` entry '$entry', which is not " +
                    "64 characters of hex — NIP-01 matches these exactly, so a malformed one selects nothing " +
                    "and says nothing"
            }
            key
        }

    /** HOCON path segments with dots or hashes must be quoted for get*(). */
    private fun quote(key: String): String = "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
