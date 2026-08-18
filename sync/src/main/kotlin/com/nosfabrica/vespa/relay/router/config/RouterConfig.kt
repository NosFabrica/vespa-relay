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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

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
 * (skip signature verification for this stream), `deleteMissing`
 * ([DeleteMissing]) and `relaySource` ([RelayDiscoveryConfig]). `sync` is
 * refused: the pool has ONE shape for every stream, so a transport choice here
 * would claim a decision nothing reads.
 *
 * How far back a stream reaches is the [SyncStream.filter]'s own
 * `since`/`until`, exactly as NIP-01 reads them: absent is unbounded. The live
 * tail runs from connect forward but keeps the filter's `until`.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<SyncStream>,
    // How often (seconds) an `up`/`both` stream re-reconciles to push newly
    // arrived local events. From SYNC_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
    // Ingest tuning. The store serializes writes through one mutex, so
    // throughput comes from the batch size, not the worker count.
    // From SYNC_INGEST_CONCURRENCY / SYNC_INGEST_BATCH.
    val ingestConcurrency: Int = 2,
    val ingestBatch: Int = 1000,
    // How many matching events WE must already hold before a negentropy
    // reconcile beats paging — our own count decides alone; the NIP-45 COUNT
    /**
     * Automatic negentropy paging: how many events one reconcile window aims to
     * hold, the floor and ceiling the learned per-peer size moves between, and
     * how far below `now` a sweep stops (the seam with the live tail).
     *
     * A stream holding more than [negPageTarget] events reconciles in windows
     * instead of one whole-filter pass — see `NegentropyPager`. `0` turns
     * paging off and restores the single shared snapshot per stream, which is
     * correct but holds the stream's entire id set for the length of the sync.
     * From SYNC_NEG_PAGE_TARGET / _MIN / _MAX / _SLACK_SECONDS.
     */
    val negPageTarget: Int = 100_000,
    val negPageMin: Int = 1_000,
    val negPageMax: Int = 1_000_000,
    val negPageSlackSec: Long = 60,
    /**
     * The monitor plane's own configuration — see [MonitorConfig]. Null runs
     * the probe passes exactly as before: candidates derived from the streams'
     * parsed sources, on the default six-hour clock.
     */
    val monitor: MonitorConfig? = null,
    /**
     * The visit pool's dial width: how many relays are visited — and
     * therefore dialled — at once, across every visit-mode stream. Router-wide
     * rather than per stream because the pool is one shared engine; the
     * per-stream `concurrency` knob paces only the legacy fan-out.
     */
    val visitConcurrency: Int = DEFAULT_VISIT_CONCURRENCY,
    /**
     * The visit pool's steady state: how many live-tail sockets it may hold
     * open at once. Visit width plus this is the most sockets the pool ever
     * owns — size the pair against the dispatcher ceiling, leaving room for
     * the static upstreams, the monitor's dials and the healer.
     */
    val tailBudget: Int = DEFAULT_TAIL_BUDGET,
) {
    companion object {
        /**
         * Matches the monitor's dial width: the same arithmetic — simultaneous
         * TLS handshakes against their own connect timeout — sizes both. The
         * first 440-relay integration run let the pool dial its whole socket
         * budget at once and watched 436 dials time out inside a minute.
         */
        const val DEFAULT_VISIT_CONCURRENCY = 128

        /**
         * Sized to the measured prime population (~600 responsive hosts
         * after folding): the whole point is that every certified relay is
         * effectively always connected.
         */
        const val DEFAULT_TAIL_BUDGET = 600
    }

    /** Every (stream, url) pair whose direction pulls events down into our store. */
    fun downUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.UP)

    /** The streams whose relay list is discovered from the store, not configured. */
    fun discoveryStreams(): List<SyncStream> = streams.filter { it.discovery != null }

    private fun upstreamsFor(want: SyncDirection): List<SyncUpstream> =
        streams
            .filter { it.dir == want || it.dir == SyncDirection.BOTH }
            .flatMap { s -> s.urls.map { SyncUpstream(s.name, it, s.filter, s.trusted, s.healContent, s.healRetractions) } }
}

/**
 * The monitor plane's configuration: where candidate urls come from, and the
 * clocks its passes run on.
 *
 * The `monitor { }` block is what makes the config file "routers + monitor"
 * rather than router-only. Its [sources] use the same select syntax a stream's
 * `relaySource` does — every relay list in the protocol is a tag with a url at
 * a fixed offset — but they feed the PROBE PASSES (fold, consistency,
 * fitness), whose verdicts land on kind-30166 records, where a
 * a verdict-query stream then finds its relay list. A deployment can thus
 * move every ounce of relay-list parsing off the streams and onto this block.
 *
 * Candidates derived here UNION with whatever the streams' own parsed sources
 * still yield — the migration posture everywhere in this config.
 */
data class MonitorConfig(
    /** Where candidate urls come from — same shape as a stream's `relaySource`. */
    val sources: List<RelaySource>,
    val exclude: RelayExcludes = RelayExcludes.NONE,
    /**
     * The full-sweep cadence: how often every candidate is re-verdicted.
     * The default is the probe passes' historical six hours.
     */
    val sweepSeconds: Long = DEFAULT_SWEEP_SECONDS,
    /**
     * The fast lane: how often the monitor looks for urls that have NEVER
     * been measured and verdicts just those. This is what bounds a new
     * relay's wait for its first `prime` at minutes instead of a sweep —
     * the price of "unmeasured urls are not dialled by streams" is paid here.
     * Null turns the lane off.
     *
     * Cheap by construction: the derivation is `since`-bounded to relay-list
     * events ingested after the last look, so it reads minutes of events, not
     * the store.
     */
    val fastLaneSeconds: Long? = DEFAULT_FAST_LANE_SECONDS,
    /**
     * How many relays a probe pass dials at once — the sweep's wall clock,
     * since the corpus is mostly dead relays whose cost is a timeout. Shared
     * by all three dialling passes, which run serialized, so this is also the
     * most sockets the monitor plane ever holds; size it against the
     * dispatcher ceiling minus the visit pool's budget.
     */
    val dialConcurrency: Int = DEFAULT_DIAL_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_SWEEP_SECONDS = 6L * 60 * 60

        /** Two minutes: a new relay is graded prime before its author refreshes the page. */
        const val DEFAULT_FAST_LANE_SECONDS = 120L

        /**
         * This was 16, with a note calling the probe work "a side quest"
         * that must stay below the fan-out's concurrency — true when the
         * fold shared its sockets with the streams' fan-out, and a relic
         * after the split. Nothing certifies until the passes finish, and a
         * mostly-dead corpus costs timeouts, not bandwidth: a 929-url sweep
         * measured at 16 spent half an hour in the fitness dials alone,
         * nearly all of it waiting.
         */
        const val DEFAULT_DIAL_CONCURRENCY = 128
    }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class SyncUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
    val healContent: Boolean = false,
    val healRetractions: Boolean = false,
)

data class SyncStream(
    val name: String,
    val dir: SyncDirection,
    val filter: Filter,
    val urls: List<NormalizedRelayUrl>,
    val trusted: Boolean,
    // Null for an ordinary stream; set when its relays come out of the store.
    val discovery: RelayDiscoveryConfig? = null,
    // Whether an upstream dropping a record means we drop it too.
    val deleteMissing: DeleteMissing = DeleteMissing.OFF,
    /**
     * Push our newer replaceable/addressable version at a relay that served us
     * a stale one. Separate from [healRetractions] because the two differ in
     * whether the author asked: this is a version update to a relay that
     * already carries them, which is why it is still opt-in.
     */
    val healContent: Boolean = false,
    /**
     * Push the kind 5, or the `ALL_RELAYS` kind 62, at a relay still serving
     * what our stored tombstone retracts. These are instructions the author
     * already addressed to every relay and most relays never received.
     */
    val healRetractions: Boolean = false,
    /**
     * Page forward, RECONCILE the past: when set, a relay whose covered
     * history was last verified longer ago than this gets a windowed
     * negentropy audit on its next visit — the whole covered range compared,
     * only the difference downloaded. A week is the intended magnitude.
     * Staggering is free (each relay's band ages on its own clock), so no herd
     * and no cap.
     *
     * Only for relays that speak NIP-77, and the router does not guess which:
     * the monitor measures it and signs the answer onto the same 30166 record
     * the roster admits the relay by. A relay measured as NOT answering is
     * never asked — the attempt cannot succeed, and a failed audit advances no
     * clock, so it used to be retried every six hours forever. Their past is
     * re-checked by [refetchThePastSeconds] instead, which is the other half
     * of this pair and the reason both knobs name their transport: they are
     * one job over two mechanisms, and which one a relay gets is a fact about
     * the relay.
     *
     * Null reconciles nothing, which leaves history exactly as complete as the
     * paged walks left it.
     */
    val negentropySyncThePastSeconds: Long? = null,
    /**
     * How often this stream's bands EXPIRE, putting its whole filter back on
     * the walk — `SYNC_REFETCH_THE_PAST_SECONDS` for the streams that do not
     * name one, and NEVER under that — no schedule this expensive is a default.
     *
     * The coarse safety net, and the only full re-check a stream without
     * [negentropySyncThePastSeconds] has: band arithmetic can only widen what
     * a walk observed, so nothing else would ever re-read a window a relay
     * back-filled after we passed it. Where a reconcile DOES run it is the
     * expensive twin of one — the
     * audit reconciles the same history and downloads the difference, this
     * re-downloads the history — which is why a stream that audits wants a
     * period well above its [negentropySyncThePastSeconds] rather than beside
     * it.
     *
     * Named for what it costs on the pool, where an expired band is always
     * re-PAGED. On a static stream it is re-walked by whatever that stream's
     * `sync` chose, which for `negentropy` is a reconcile rather than a fetch —
     * the one place the name is generous. `fullResyncSeconds` was the old
     * spelling, and said neither which direction nor how.
     */
    val refetchThePastSeconds: Long? = null,
    /**
     * The kinds this stream's upstreams are the source of truth for — the only
     * kinds [deleteMissing] may delete on their own absence. Required whenever
     * it is on, and checked against [filter]: turning on deletion without
     * saying what it may delete is a config error, not a default.
     *
     * Everything else the same authors publish is left alone, by this class
     * and by everything else: an absence upstream is only ever evidence about
     * the kinds the upstream owns.
     */
    val ownedKinds: Set<Int> = emptySet(),
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

enum class SyncDirection(
    val wire: String,
) {
    DOWN("down"),
    UP("up"),
    BOTH("both"),
    ;

    companion object {
        fun parse(raw: String): SyncDirection =
            entries.firstOrNull { it.wire.equals(raw.trim(), ignoreCase = true) }
                ?: error("router: unknown stream dir '$raw' (expected down / up / both)")
    }
}
