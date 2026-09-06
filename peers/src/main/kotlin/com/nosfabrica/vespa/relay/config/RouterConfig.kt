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
    /** Where candidate urls come from; the same shape as a stream's `relaySource`. */
    val sources: List<RelaySource>,
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
            .takeIf { it.isNotEmpty() }
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
) {
    /** The live budget resolved; the one expression the gate and the boot warning both read. */
    val liveBudget: Int get() = maxLiveConcurrency ?: RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY
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
