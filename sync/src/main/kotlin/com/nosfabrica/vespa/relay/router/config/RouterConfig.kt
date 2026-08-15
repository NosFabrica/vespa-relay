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
 * (skip signature verification for this stream), `sync` ([SyncMode]),
 * `deleteMissing` ([DeleteMissing]) and `relaySource` ([RelayDiscoveryConfig]).
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
    // How much overlap makes a negentropy reconcile worth its id exchange, and
    // how long a relay gets to answer the NIP-45 COUNT that measures it.
    // From SYNC_NEG_MIN_EVENTS.
    val negMinEvents: Int = 5_000,
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
) {
    /** Every (stream, url) pair whose direction pulls events down into our store. */
    fun downUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.UP)

    /** The streams whose relay list is discovered from the store, not configured. */
    fun dynamicStreams(): List<SyncStream> = streams.filter { it.dynamic != null }

    private fun upstreamsFor(want: SyncDirection): List<SyncUpstream> =
        streams
            .filter { it.dir == want || it.dir == SyncDirection.BOTH }
            .flatMap { s -> s.urls.map { SyncUpstream(s.name, it, s.filter, s.trusted, s.sync, s.healContent, s.healRetractions) } }
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
 * [SyncableSource] stream then finds its relay list. A deployment can thus
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
     * relay's wait for its first `syncable` at minutes instead of a sweep —
     * the price of "unmeasured urls are not dialled by streams" is paid here.
     * Null turns the lane off.
     *
     * Cheap by construction: the derivation is `since`-bounded to relay-list
     * events ingested after the last look, so it reads minutes of events, not
     * the store.
     */
    val newUrlSeconds: Long? = DEFAULT_NEW_URL_SECONDS,
    /**
     * How many relays a probe pass dials at once — the sweep's wall clock,
     * since the corpus is mostly dead relays whose cost is a timeout. Shared
     * by all three dialling passes, which run serialized, so this is also the
     * most sockets the monitor plane ever holds; size it against the
     * dispatcher ceiling minus the visit pool's budget.
     */
    val concurrency: Int = DEFAULT_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_SWEEP_SECONDS = 6L * 60 * 60

        /** Two minutes: a new relay is syncable before its author refreshes the page. */
        const val DEFAULT_NEW_URL_SECONDS = 120L

        /**
         * This was 16, with a note calling the probe work "a side quest"
         * that must stay below the fan-out's concurrency — true when the
         * fold shared its sockets with the streams' fan-out, and a relic
         * after the split. Nothing certifies until the passes finish, and a
         * mostly-dead corpus costs timeouts, not bandwidth: a 929-url sweep
         * measured at 16 spent half an hour in the fitness dials alone,
         * nearly all of it waiting.
         */
        const val DEFAULT_CONCURRENCY = 128
    }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class SyncUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
    val sync: SyncMode = SyncMode.AUTO,
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
    val dynamic: RelayDiscoveryConfig? = null,
    // Whether this stream's relays share events with each other — see [SyncMode].
    val sync: SyncMode = SyncMode.AUTO,
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
     * Fetch forward, audit the past: when set, a relay whose band's last full
     * pass is older than this gets a windowed negentropy audit on its next
     * visit — the covered history reconciled, only the diff downloaded, and
     * `fullAt` re-stamped. A week is the intended magnitude. Staggering is
     * free (each relay's band ages on its own clock), so no herd and no cap.
     * Null audits nothing, which leaves history exactly as complete as the
     * paged walks left it.
     */
    val verifySeconds: Long? = null,
    /**
     * The kinds this stream's upstreams are the source of truth for — the only
     * kinds [deleteMissing] may delete on their own absence. Required whenever
     * it is on, and checked against [filter]: turning on deletion without
     * saying what it may delete is a config error, not a default.
     *
     * Everything else in [filter] is ATTACHED: fetched from the same relay,
     * never deleted for being missing there, and dropped only when the owned
     * set for that author is retracted wholesale. NIP-85 says a provider
     * should publish its service key's kind 0 and 10002, but measured on 12
     * (service, relay) pairs not one provider relay actually serves them —
     * they come from the indexers instead. Deleting on their absence here
     * would erase the profile of every healthy provider on the stream.
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
