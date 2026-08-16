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
import com.vitorpamplona.quartz.nip19Bech32.decodePublicKeyAsHexOrNull
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import java.io.File
import java.util.regex.PatternSyntaxException

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
            )
        // Window sizing for the automatic negentropy pager. Applied with copy()
        // rather than threaded through parse(): they are runtime tuning, not
        // part of the strfry-shaped config the parser reads.
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
        return parse(raw, upInterval, ingestConcurrency, ingestBatch, relaySourceDefaults, negMinEvents)
            .copy(
                negPageTarget = pageTarget,
                negPageMin = pageMin,
                negPageMax = pageMax.coerceAtLeast(pageMin),
                negPageSlackSec = pageSlack,
            ).let {
                if (only == null) it else it.copy(streams = narrowToStreams(it.streams, only))
            }
    }

    /**
     * `SYNC_STREAMS=contentViaOutbox` — run only the named streams, so one part
     * of the sync can be measured without the others competing for the same
     * sockets, heap and ingest queue. A name that matches nothing is a hard
     * error: a typo would otherwise look exactly like a relay that mirrors
     * nothing.
     */
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
                val sync = if (s.hasPath("sync")) SyncMode.parse(s.getString("sync")) else SyncMode.AUTO
                // An hour is the floor because the audit re-reconciles the
                // WHOLE covered history: a knob under it is a re-walk loop
                // wearing an audit's name. `verifySeconds` is the knob's old
                // name, honored with a nudge — a renamed key must never
                // silently turn a deployment's audits off.
                val auditSeconds =
                    when {
                        s.hasPath("auditSeconds") -> {
                            s.getLong("auditSeconds")
                        }

                        s.hasPath("verifySeconds") -> {
                            System.err.println(
                                "router: stream '$name' uses verifySeconds — renamed to auditSeconds (it clocks the history audit); the old name still works",
                            )
                            s.getLong("verifySeconds")
                        }

                        else -> {
                            null
                        }
                    }?.coerceAtLeast(3600L)
                if (deleteMissing != DeleteMissing.OFF) {
                    // The comparison runs as the pool's history audit, so the
                    // config must give it a relay list the monitor answers
                    // for and the audit clock it runs on. Nothing else can
                    // carry it: a static stream has no discovery to pair
                    // authors with, and a paged fetch asks only outside its
                    // band, so "not seen" there means "not asked for".
                    require(discovery != null) {
                        "router: stream '$name' sets deleteMissing without a `relaySource` — the retraction " +
                            "comparison runs as the pool's audit, over asks a scan paired with their owners"
                    }
                    require(auditSeconds != null) {
                        "router: stream '$name' sets deleteMissing without `auditSeconds` — " +
                            "the retraction comparison runs as the history audit, and that knob is its clock"
                    }
                    // The delete's whole licence is per (relay, provider):
                    // "this relay no longer serves this provider" is the only
                    // retraction one relay can prove. An UNBOUND ask would
                    // reconcile EVERY author's owned records against a single
                    // relay and delete whatever that relay happens not to
                    // hold — so every source must be a scan whose selects
                    // bind `authors`. A verdict source cannot carry it: it
                    // fans the stream's one filter to every certified relay.
                    discovery.sources.forEach { source ->
                        // Catches the kind-30166 source too, and by the rule
                        // that matters rather than by its shape: its `d`-tag
                        // select binds nothing, so it fans the stream's one
                        // unbound filter to every relay it admits.
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
                    sync = sync,
                    deleteMissing = deleteMissing,
                    ownedKinds = parseOwnedKinds(name, s, filter, deleteMissing),
                    healContent = s.hasPath("healContent") && s.getBoolean("healContent"),
                    healRetractions = s.hasPath("healRetractions") && s.getBoolean("healRetractions"),
                    auditSeconds = auditSeconds,
                )
            }
        // Advisory, never a refusal — the overlap can be deliberate. A kind a
        // retracting stream deletes and another stream mirrors oscillates:
        // the audit deletes it, the other stream re-mirrors it, the next
        // audit deletes it again. Nothing broken, endlessly busy — said at
        // boot so the operator who configured it can recognize the churn.
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
        return RouterConfig(
            connTimeout,
            streams,
            upIntervalSec,
            ingestConcurrency,
            ingestBatch,
            negMinEvents,
            monitor = parseMonitor(cfg),
            // The pool's two socket numbers, floored at 1 for the same reason
            // the monitor's dial gate is: zero of either is an off switch
            // wearing a tuning knob's name.
            visitConcurrency =
                if (cfg.hasPath("visitConcurrency")) {
                    cfg.getInt("visitConcurrency").coerceAtLeast(1)
                } else {
                    RouterConfig.DEFAULT_VISIT_CONCURRENCY
                },
            tailBudget =
                if (cfg.hasPath("tailBudget")) {
                    cfg.getInt("tailBudget").coerceAtLeast(1)
                } else {
                    RouterConfig.DEFAULT_TAIL_BUDGET
                },
        )
    }

    /**
     * The `monitor { }` block — the plane that owns relay-list parsing and the
     * probe passes' clocks. Reuses the stream-side parsers on purpose: a
     * monitor source IS a relay source, just feeding verdicts instead of a
     * fan-out.
     */
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
                    // `newUrlSeconds` is the knob's old name — every log line
                    // and progress row already says "fast lane".
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

                        // 0 is the documented off switch: a fast lane that fired
                        // every zero seconds would be a busy loop, not a setting.
                        m.getLong(key) <= 0L -> null

                        else -> m.getLong(key).coerceAtLeast(30L)
                    }
                },
            // Floored at 1: zero dials is a monitor that never certifies
            // anything, which is an off switch no operator asked this knob
            // to be. The ceiling is the operator's own arithmetic — the
            // dispatcher budget minus the pool's — and is not second-guessed.
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

    /**
     * The `relaySource = [ ... ]` list plus the stream-level knobs pacing its
     * discovery. Every source must answer to the monitor — a kind-30166
     * verdict source, or a scan gated `certified` — because the pool is the
     * only engine: an ungated scan has nothing to run it, and admitting it
     * silently would dial every dead url every relay list ever spammed.
     *
     * The legacy fan-out's knobs are refused BY NAME rather than ignored: a
     * config that still says them was written for the cycle engine, and the
     * operator deserves the migration note, not a silently different
     * behavior.
     */
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
        require(!s.hasPath("concurrency")) {
            "router: stream '$stream' sets a per-stream concurrency — gone with the cycle engine. The pool's " +
                "dial width is the top-level `visitConcurrency`, shared by every stream"
        }
        require(!s.hasPath("recycleSeconds")) {
            "router: stream '$stream' sets recycleSeconds — gone with the cycle engine. The pool has no laps: " +
                "revisits are paced per relay by recent yield, and `refreshSeconds` paces rediscovery"
        }
        require(!s.hasPath("sync")) {
            "router: stream '$stream' sets `sync` beside a relaySource — the pool has one shape for every " +
                "stream: page forward from the band's edge, reconcile the covered past on the auditSeconds " +
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
        // SAID, NOT REFUSED. An ungated stream dials whatever its sources name,
        // and relay lists are as writable as the events carrying them — each
        // dead url costs a dial and a timeout every cycle forever. That used
        // to be a parse error, on the rule "unless every source is a verdict
        // query"; stating that rule meant this module deciding which tag, and
        // which value in it, constitutes a vouching — the operator's choice
        // and another monitor's spelling. A filter that gates and a filter
        // that scans are indistinguishable from here, so the config is the
        // authority and this is a line at boot naming the stream.
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

    /**
     * `exclude` entries are plain urls or regexes — see [RelayExcludes] for
     * how they are told apart and matched. Compiled here, at the one place a
     * human types them, so a broken pattern refuses the config naming the
     * stream instead of surfacing as a stack trace mid-cycle, and an
     * unusable plain url warns the way a `urls` entry does.
     */
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

    /**
     * ONE `{ select = [ ], filter = { } }` ENTRY, and there is only one kind.
     *
     * It used to be two. A filter asking for kind 30166 was parsed into a
     * `VerdictSource` and read back through a separate verified path, while
     * everything else was a scan; the two differed in the questions they were
     * allowed to answer and in almost nothing else. Now that the verified read
     * has nothing private left in it — no rules epoch, no tag-stamp freshness
     * — a verdict query is a scan whose select is the `d` tag, and saying so
     * removes a type, a read path, and a set of rules that applied to one of
     * them for reasons that no longer hold.
     *
     * Used both for a stream's `relaySource` entries and for the entries of
     * its `gatedBy`: "where urls come from" and "which urls are permitted" are
     * the same shape of question asked at two points, so they take the same
     * shape of answer.
     */
    private fun parseRelaySource(
        stream: String,
        s: Config,
        what: String = "relaySource",
    ): RelaySource {
        require(s.hasPath("filter")) { "router: stream '$stream' has a $what entry with no `filter { }`" }
        // Same npub-only rule everywhere a key is typed, restated as hex for
        // the Filter: bare hex has no checksum, so a typo is a nobody whose
        // source is empty and whose gate holds everything out, with no error.
        val written = parseFilter(s.getConfig("filter"))
        val kinds = written.kinds
        require(!kinds.isNullOrEmpty()) { "router: stream '$stream' $what filter needs `kinds`" }
        // KIND, not semantics. NIP-66 fixes two things about a 30166 that this
        // module may rely on without guessing anyone's vocabulary: the url
        // lives in the `d` tag, and the author is a monitor identity. What the
        // other tags are called and what their values mean is not ours.
        val isNip66Record = kinds == listOf(RelayDiscoveryEvent.KIND)
        // npub-ONLY where the authors are MONITOR IDENTITIES, for the reason
        // the relay side's PubKeys spells out: hex has no checksum, so one
        // mistyped character is a valid-looking key that is nobody, and a
        // roster or gate built on it is empty with no error anywhere. Other
        // kinds keep NIP-01's own spelling — a scan's `authors` is an ordinary
        // filter field and narrowing it is not a trust statement.
        val filter =
            if (isNip66Record) {
                written.copy(authors = decodeNpubs(stream, written.authors.orEmpty()).takeIf { it.isNotEmpty() })
            } else {
                written
            }
        require(!(s.hasPath("maxAgeSeconds") && (filter.since != null || filter.until != null))) {
            "router: stream '$stream' bounds a $what entry with BOTH `maxAgeSeconds` and since/until — they are " +
                "two spellings of one bound and the relative one wins, so the absolute one would be read by a " +
                "human and by nothing else"
        }
        // A regular kind is unbounded — scanning all of kind 1 means loading
        // every note in the store into one list. Replaceable/addressable kinds
        // are one event per author, which is what makes them safe to scan
        // whole. Checked on what the operator WROTE: `maxAgeSeconds` becomes a
        // `since` at read time, and letting that satisfy the bound would make
        // this guard vacuous for every source.
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
                "syncable from our own monitor`, and both halves of that are now expressible. Write the gate as " +
                "a stream-level `gatedBy = [ { filter = { \"kinds\": [${RelayDiscoveryEvent.KIND}], " +
                "\"#s\": [\"syncable\"] } } ]` — naming whatever tag and value the monitor you trust writes"
        }
        require(!s.hasPath("resultsFilteredBy")) {
            "router: stream '$stream' puts `resultsFilteredBy` on a $what entry — it was renamed to `gatedBy` and " +
                "moved beside `exclude` on the stream, because which urls may be dialled is not a property of " +
                "how one was discovered"
        }
        return RelaySource(
            selects = selects,
            filter = filter,
            // Unbounded unless written. Which filters describe a measurement
            // that goes stale, and which describe a list that does not, is the
            // operator's knowledge — see [RelaySource.maxAgeSeconds].
            maxAgeSeconds = if (s.hasPath("maxAgeSeconds")) s.getLong("maxAgeSeconds").coerceAtLeast(60L) else null,
            refreshSeconds = if (s.hasPath("refreshSeconds")) s.getLong("refreshSeconds").coerceAtLeast(10L) else null,
        )
    }

    /**
     * Monitor identities as the operator wrote them — npub-ONLY, for the
     * reason the relay side's PubKeys spells out: hex has no checksum, so one
     * mistyped character is a valid-looking key that simply is not anybody,
     * and a roster or gate built on it is empty with no error anywhere.
     * Absent is unscoped, never a substituted identity — see
     * [com.nosfabrica.vespa.relay.router.config.RelaySource.filter]'s authors.
     */
    private fun decodeNpubs(
        stream: String,
        raw: List<String>,
    ): List<String> =
        raw.map { entry ->
            val trimmed = entry.trim().let { if (it.none(Char::isLowerCase)) it.lowercase() else it }
            require(!trimmed.startsWith("nsec1")) {
                "router: stream '$stream' has an nsec where a monitor npub belongs — that is a PRIVATE key; " +
                    "put the monitor's npub there"
            }
            val hex = if (trimmed.startsWith("n")) decodePublicKeyAsHexOrNull(trimmed) else null
            requireNotNull(hex?.takeIf { it.length == 64 }) {
                "router: stream '$stream' monitor identity does not decode as an npub — " +
                    if (entry.trim().length == 64) {
                        "bare hex has no checksum, so a typo is a nobody with an empty roster and no error; " +
                            "convert it to its npub form and use that"
                    } else {
                        "recopy the monitor's npub1…"
                    }
            }
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
            urlIndex = index,
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
    ): Map<String, BindingSlot> {
        val out = LinkedHashMap<String, BindingSlot>()
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

        // NIP-01 timestamps are unsigned and a `limit` counts events, so none of
        // the three can be negative — and what a relay DOES with a negative one is
        // never what the config meant. Measured across the five `indexers`:
        // strfry kills the subscription (`CLOSED: bad req: error parsing until`),
        // three answer a NOTICE and then never EOSE, so every page burns a full
        // idle timeout, and purplepag.es silently drops the bound and serves its
        // NEWEST page — the opposite end of the relay from the one asked for. A
        // negative `limit` is quieter and worse: quartz drops the filter before
        // the first REQ, so the stream reports LIMIT_REACHED having downloaded
        // nothing, every cycle, looking like a relay with no events.
        //
        // Caught here, at the one place a human types it, because none of those
        // failures name the config that caused them.
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

        /**
         * `since = 0` is NOT a floor, it is the absence of one — the epoch is
         * the bottom of an unsigned `created_at`, so it asks for exactly what
         * omitting `since` asks for. Normalised to null here so the rest of the
         * router sees the two spellings as one thing.
         *
         * It matters at two places that both read `since != null` as "bounded":
         *  - [flooredForPaging] passes a filter with its OWN `since` through
         *    untouched, so `since = 0` walks unfloored. On the pinned quartz
         *    that cannot run past zero any more, but it ends the leg UNPAGEABLE
         *    against a relay like purplepag.es — no coverage recorded, re-walked
         *    every boot.
         *  - the `narrowed` check on a relaySource counts a non-null `since` as
         *    narrowing the scan. Zero narrows nothing, so it bought a regular
         *    kind an unbounded scan past a guard written to stop exactly that.
         *
         * Normalising here rather than clamping in [flooredForPaging] is
         * deliberate: `drainSettlesThePast` compares the leg's floor against the
         * FILTER's, so a leg clamped above the floor its filter asked for has
         * not reached bottom and could never settle history.
         *
         * `until = 0` gets no such treatment — it is a real, if near-empty,
         * bound ("nothing after the epoch"), not the absence of one.
         */
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
        // An inverted window asks for events after X and before Y with X > Y,
        // which nothing can satisfy. It is not caught anywhere downstream: the
        // relay EOSEs an empty page, so the walk reports DRAINED, and
        // `drainSettlesThePast` compares the leg's floor against the filter's —
        // the same value — and returns true. The band then records a settled
        // past from a window that could never have returned an event.
        // `PagingProgress.begin` already refuses this shape ("an inverted window
        // is not a walk"), so today such a leg is also invisible to the progress
        // line while it runs.
        require(since == null || until == null || since <= until) {
            "router: filter at ${f.origin().description()} has `since = $since` after `until = $until` — " +
                "an inverted window matches nothing, and a relay answers it with the empty page that " +
                "makes this repo record the history it never walked as settled"
        }

        return Filter(
            ids = strs("ids"),
            authors = strs("authors"),
            kinds = ints("kinds"),
            tags = tags,
            since = since,
            until = until,
            // getInt, not getLong().toInt(): HOCON range-checks an int here, and
            // going through Long would silently truncate an out-of-range limit
            // into a plausible-looking one.
            //
            // Zero stays legal and is NOT normalised away: `limit = 0` is the
            // NIP-01 idiom for "no stored events, just the live tail", and this
            // router honours it — `SyncEngine`'s down tail reuses this same
            // filter, overriding `since` but not `limit`, so the live
            // subscription still streams. On the PAGED path quartz drops it
            // before the first REQ (`matchCountPerFilter[i] < limit` is `0 < 0`)
            // and reports LIMIT_REACHED, which is not DRAINED — so it claims no
            // coverage, and "downloaded 0 history" is the truth for a stream
            // configured not to want any.
            limit = if (f.hasPath("limit")) f.getInt("limit").also { nonNegative("limit", it.toLong()) } else null,
            search = if (f.hasPath("search")) f.getString("search") else null,
        )
    }

    /** HOCON path segments with dots/hashes/special chars must be quoted for get*(). */
    private fun quote(key: String): String = "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
