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
    fun downUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.DOWN)

    /** Every (stream, url) pair whose direction pushes our events up to the upstream. */
    fun upUpstreams(): List<SyncUpstream> = upstreamsFor(SyncDirection.UP)

    /** The streams whose relay list is discovered from the store, not configured. */
    fun dynamicStreams(): List<SyncStream> = streams.filter { it.dynamic != null }

    private fun upstreamsFor(want: SyncDirection): List<SyncUpstream> =
        streams
            .filter { it.dir == want || it.dir == SyncDirection.BOTH }
            .flatMap { s -> s.urls.map { SyncUpstream(s.name, it, s.filter, s.trusted, s.sync) } }
}

/** One upstream connection: a single relay url with the filter/flags of its stream. */
data class SyncUpstream(
    val streamName: String,
    val url: NormalizedRelayUrl,
    val filter: Filter,
    val trusted: Boolean,
    val sync: SyncMode = SyncMode.AUTO,
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
