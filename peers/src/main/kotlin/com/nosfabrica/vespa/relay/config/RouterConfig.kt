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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * The router config: strfry's `streams { }` model parsed from HOCON, so an existing strfry
 * `routerConfigOverride` drops in unchanged. Beyond strfry's schema a stream may set `trusted`,
 * `deleteMissing` and `relaySource`; `sync` is refused. A filter's `since`/`until` are NIP-01's.
 */
data class RouterConfig(
    val connectionTimeoutSec: Long,
    val streams: List<SyncStream>,
    // Seconds between an `up`/`both` stream's re-reconciles. SYNC_UP_INTERVAL_SECONDS.
    val upIntervalSec: Long = 300,
    // The store serializes writes, so ingest throughput comes from the batch size, not the workers.
    val ingestConcurrency: Int = 2,
    val ingestBatch: Int = 1000,
    /**
     * Negentropy paging: the events one reconcile window aims to hold, the floor and ceiling
     * the learned per-peer size moves between, and how far below `now` a sweep stops.
     */
    val negPageTarget: Int = 100_000,
    val negPageMin: Int = 1_000,
    val negPageMax: Int = 1_000_000,
    val negPageSlackSec: Long = 60,
    /** Null is a deployment with no monitor: nothing is measured and no verdict is signed. */
    val monitor: MonitorConfig? = null,
) {
    companion object {
        const val DEFAULT_VISIT_CONCURRENCY = 128

        /** What a stream with no `visitConcurrency` contributes to the pool's worker count. */
        const val UNCAPPED_STREAM_VISITS = DEFAULT_VISIT_CONCURRENCY

        /** What a stream with no `maxLiveConcurrency` contributes to the sockets the pool may hold open. */
        const val DEFAULT_MAX_LIVE_CONCURRENCY = 600
    }

    /** Every (stream, url) pair whose direction pulls events down into our store. */
    fun downUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.UP)

    /** The streams whose relay list is discovered from the store, not configured. */
    fun discoveryStreams(): List<SyncStream> = streams.filter { it.discovery != null }

    /**
     * What the monitor measures, or null where it measures nothing. Its own `sources` and only
     * those: the monitor's config names relays in its own terms, and never points at a stream.
     */
    fun monitorSources(): RelayDiscoveryConfig? = monitor?.asDiscovery()

    private fun upstreamsFor(want: SyncDirection): List<SyncUpstream> =
        streams
            .filter { it.dir == want || it.dir == SyncDirection.BOTH }
            .flatMap { s -> s.urls.map { SyncUpstream(s.name, it, s.filter, s.trusted, s.healContent, s.healRetractions) } }
}

/**
 * The monitor's config — its own file, or the `monitor { }` block of the sync config: where
 * candidate urls come from, and the clocks the probe passes run on. It is the whole of what this
 * deployment measures, written in its own terms; no stream contributes to it.
 */
data class MonitorConfig(
    /**
     * Where candidate urls come from; the same shape as a stream's `relaySource`. Null is a block
     * that never said — a config that tunes the clocks and leaves the corpus unanswered, which the
     * boot refuses. An empty list is the answer "measure nothing", and boots.
     */
    val sources: List<RelaySource>?,
    val exclude: RelayExcludes = RelayExcludes.NONE,
    /** How often every candidate is re-verdicted. */
    val sweepSeconds: Long = DEFAULT_SWEEP_SECONDS,
    /**
     * How often the monitor verdicts urls that have never been measured, bounding a new relay's
     * wait for its first `prime`. Null turns the lane off.
     */
    val fastLaneSeconds: Long? = DEFAULT_FAST_LANE_SECONDS,
    /**
     * Relays a probe pass dials at once. The dialling passes run serialized, so this is also
     * the most sockets the monitor plane holds.
     */
    val dialConcurrency: Int = DEFAULT_DIAL_CONCURRENCY,
) {
    /** This block's own sources as a discovery config; the cadence fields carry the sweep. */
    fun asDiscovery(): RelayDiscoveryConfig? =
        sources
            ?.takeIf { it.isNotEmpty() }
            ?.let { RelayDiscoveryConfig(sources = it, refreshSeconds = sweepSeconds, exclude = exclude) }

    companion object {
        const val DEFAULT_SWEEP_SECONDS = 6L * 60 * 60

        const val DEFAULT_FAST_LANE_SECONDS = 120L

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

/**
 * One age band of a stream's past and how often it is re-checked. Bands tile the timeline
 * youngest-first: a band holds records up to [maxAgeSeconds] old, and the band before it holds
 * everything younger, so every record falls in exactly one band and no band re-walks another's.
 *
 * The point is that age and churn correlate. Records from the last month still move; records
 * from three years ago do not, and re-reconciling them weekly is most of what a mirror spends
 * its CPU on. A band's cost is paid at its own cadence instead of the whole past's.
 */
data class SyncTier(
    /**
     * The age of the oldest record this band holds, in seconds — and so the band's older edge.
     * Null is no maximum: everything below the band before it, down to the corpus floor.
     */
    val maxAgeSeconds: Long?,
    /**
     * How stale this band may get before it is re-checked. Zero is always due, which
     * `attemptSpacingSeconds` still floors at 15 minutes — for a past whose upstream is the
     * source of truth and must be re-read as of today.
     */
    val everySeconds: Long,
) {
    /**
     * This band's identity, stable as its window slides. It keys the verified clock and the
     * paging cursor, neither of which may be tied to the window: the window moves with `now`,
     * and a key that moved with it would orphan the very record of having walked the band.
     */
    val id: String get() = "tier:" + (maxAgeSeconds?.toString() ?: "all")

    companion object {
        /**
         * A band's window at [now]. Edges snap down to a day so a sweep interrupted and resumed
         * within a day resumes on the same window rather than a hair-shifted one.
         */
        fun windowAt(
            now: Long,
            olderEdge: Long?,
            newerEdge: Long?,
        ): Pair<Long?, Long?> = Pair(edge(now, olderEdge), edge(now, newerEdge))

        private const val EDGE_QUANTUM_SECONDS = 86_400L

        private fun edge(
            now: Long,
            maxAgeSeconds: Long?,
        ): Long? = maxAgeSeconds?.let { ((now - it) / EDGE_QUANTUM_SECONDS) * EDGE_QUANTUM_SECONDS }

        /**
         * [filter] narrowed to one band. The band's edges are intersected with whatever bounds
         * the filter already carries, so a band can only narrow a stream's window, never widen
         * it past what the stream asked for.
         */
        fun windowedFilter(
            filter: Filter,
            now: Long,
            olderEdge: Long?,
            newerEdge: Long?,
        ): Filter {
            val (older, newer) = windowAt(now, olderEdge, newerEdge)
            return filter.copy(
                since = listOfNotNull(filter.since, older).maxOrNull(),
                until = listOfNotNull(filter.until, newer).minOrNull(),
            )
        }

        /**
         * Whether [tiers] is the one degenerate schedule that is not really banded: a single
         * band over the whole past, which is what a bare period resolves to. Its keys stay the
         * unqualified ones it has always used, so turning tiers on for one stream never orphans
         * another's state.
         */
        fun isUnbanded(tiers: List<SyncTier>): Boolean = tiers.size == 1 && tiers[0].maxAgeSeconds == null

        /** A band's discriminator for the audit's verified clock; empty when [isUnbanded]. */
        fun bandIdOf(
            tiers: List<SyncTier>,
            tier: SyncTier,
        ): String = if (isUnbanded(tiers)) "" else tier.id

        /**
         * The coverage key for one band of [stream]. The re-fetch keeps a coverage per band —
         * quartz's `SyncCoverage` carries one period for its life — so the band rides in the
         * name the coverage is filed under.
         */
        fun keyFor(
            stream: String,
            tiers: List<SyncTier>,
            tier: SyncTier,
        ): String = if (isUnbanded(tiers)) stream else "$stream#${tier.id}"

        /**
         * [tiers] as (band, olderEdge, newerEdge) youngest-first, each band's newer edge being
         * the previous band's older one. The first band is open at the top: it must stay open
         * so records arriving during the walk are inside it.
         */
        fun tile(tiers: List<SyncTier>): List<Triple<SyncTier, Long?, Long?>> =
            tiers.mapIndexed { i, tier ->
                Triple(tier, tier.maxAgeSeconds, if (i == 0) null else tiers[i - 1].maxAgeSeconds)
            }
    }
}

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
     * Push our newer replaceable/addressable version at a relay that served a stale one.
     * Opt-in: unlike [healRetractions], the author never asked.
     */
    val healContent: Boolean = false,
    /** Push the kind 5, or the `ALL_RELAYS` kind 62, at a relay still serving what our stored tombstone retracts. */
    val healRetractions: Boolean = false,
    /**
     * How stale a relay's verified history may get before its next visit runs a windowed
     * negentropy audit. Only relays measured as answering NEG-OPEN are asked; the rest fall to
     * [refetchThePastSeconds]. Null audits nothing.
     */
    val negentropySyncThePastSeconds: Long? = null,
    /**
     * This stream's share of the pool's jobs. Null is uncapped, leaving the job bounded by
     * [visitConcurrency] alone; there is no router-wide ceiling above these.
     */
    val refetchConcurrency: Int? = null,
    val negentropyConcurrency: Int? = null,
    /**
     * Live subscriptions this stream may hold open between visits. Null is not uncapped: it
     * resolves to [RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY], read through [liveBudget].
     */
    val maxLiveConcurrency: Int? = null,
    /**
     * Relays visited for this stream at once, as admission on the shared pool. The pool's
     * worker count is the sum of these.
     */
    val visitConcurrency: Int? = null,
    /**
     * How often this stream's bands expire, putting its whole filter back on the walk. The only
     * full re-check a stream without [negentropySyncThePastSeconds] has; where an audit runs,
     * set it well above the audit period. Null never expires.
     */
    val refetchThePastSeconds: Long? = null,
    /**
     * The kinds this stream's upstreams are the source of truth for: the only kinds
     * [deleteMissing] may delete on absence. Required whenever deletion is on.
     */
    val ownedKinds: Set<Int> = emptySet(),
    /**
     * Age bands for the negentropy audit, youngest-first, in place of the single period in
     * [negentropySyncThePastSeconds]. Empty falls back to that scalar; setting both is refused.
     */
    val negentropyTiers: List<SyncTier> = emptyList(),
    /** Age bands for the re-fetch, youngest-first, in place of [refetchThePastSeconds]. */
    val refetchTiers: List<SyncTier> = emptyList(),
) {
    /** The live budget resolved; the one expression the gate and the boot warning both read. */
    val liveBudget: Int get() = maxLiveConcurrency ?: RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY

    /**
     * The audit's bands, the one expression every caller reads: the explicit tiers, else the
     * scalar as a single band over the whole past, else nothing to audit.
     */
    val negentropySchedule: List<SyncTier>
        get() =
            negentropyTiers.ifEmpty {
                negentropySyncThePastSeconds?.let { listOf(SyncTier(maxAgeSeconds = null, everySeconds = it)) } ?: emptyList()
            }

    /** The re-fetch's bands, resolved the same way from [refetchThePastSeconds]. */
    val refetchSchedule: List<SyncTier>
        get() =
            refetchTiers.ifEmpty {
                refetchThePastSeconds?.let { listOf(SyncTier(maxAgeSeconds = null, everySeconds = it)) } ?: emptyList()
            }
}

/**
 * What to do with records we hold that the upstream no longer serves. Only meaningful when the
 * upstream is the source of truth for its records; absence has innocent causes, so [DRY_RUN]
 * and the router's guardrails are the safety net.
 */
enum class DeleteMissing {
    /** Never delete. The default. */
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
