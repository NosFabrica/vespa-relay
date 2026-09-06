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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.peers.DiscoveredRelay
import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import java.util.concurrent.ConcurrentHashMap

/**
 * What the pool should be syncing: one [rebuild] reads every stream's sources out of the
 * store, intersects them with the stream's gate, and answers url → stream → asks. Store reads
 * only, never a socket.
 */
internal class RosterBuilder(
    private val store: IEventStore,
    /** The visit-mode streams, every one carrying at least one relaySource. */
    private val streams: List<SyncStream>,
    private val bands: SyncBands,
    /** The duplicate-url fold, read-only: url → the survivor standing in for it. */
    private val foldedAway: suspend (List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl> = { emptyMap() },
    /** Urls a static subscription holds; their bands are never dropped. */
    private val keepBands: Set<NormalizedRelayUrl> = emptySet(),
    private val tor: TorTransport? = null,
    /**
     * What our own monitor stands behind, per url: the NIP-77 answer each ask paces on, and which
     * urls it has measured at all. Empty (every ask keeps trying) for probes and unsigned routers.
     */
    private val verdicts: suspend (List<NormalizedRelayUrl>) -> RelayVerdictRecord.Verdicts = { RelayVerdictRecord.Verdicts() },
    /**
     * Whether this deployment measures anything at all: it signs verdicts and its `monitor { }`
     * block names a source. False makes [Roster.measured] meaningless rather than empty — a
     * router that runs no monitor is not a router whose every relay is ungraded.
     */
    private val watching: Boolean = false,
) {
    /** One unit of work against one relay: the stream asking and the exact filter. Bands, audits and tails key on it. */
    internal data class Ask(
        val stream: SyncStream,
        val filter: Filter,
    )

    /** What one stream asks of one relay, and the identity of that ask set. */
    internal class UnitAsks(
        val asks: List<Ask>,
        /** Each ask's filter as JSON, for change detection across rebuilds; [Filter] compares by reference. */
        val identity: Set<String>,
    )

    /** One rebuild's whole answer. */
    internal class Roster(
        /** url → stream → that unit's asks. */
        val asks: Map<NormalizedRelayUrl, Map<String, UnitAsks>>,
        /** Per stream: authors found at more than one relay. The retraction audit never judges their asks. */
        val sharedAuthors: Map<String, Set<String>>,
        /** url → whether the monitor measured it answering a NEG-OPEN; absent where unmeasured. */
        val speaksNegentropy: Map<NormalizedRelayUrl, Boolean> = emptyMap(),
        /**
         * The urls here our own monitor holds a current verdict about. Every other url on this
         * roster is one the mirror syncs and the monitor is not watching — the shape a monitor
         * whose `sources` have drifted from the streams takes.
         */
        val measured: Set<NormalizedRelayUrl> = emptySet(),
        /** Whether [measured] means anything; false on a deployment that measures nothing on purpose. */
        val watching: Boolean = false,
    ) {
        /**
         * Whether a verdict of ours stands behind [url] — or nothing here was ever going to, which
         * is not the same absence: a router running no monitor has no drift to report.
         */
        fun watches(url: NormalizedRelayUrl): Boolean = !watching || url in measured
    }

    /** One source's discovery, held for its own `refreshSeconds`. */
    private class ScannedList(
        val expiresAtMs: Long,
        val relays: List<DiscoveredRelay>,
    )

    private val scans = ConcurrentHashMap<String, ScannedList>()

    suspend fun rebuild(): Roster {
        // Two halves, sealed into [UnitAsks] at the end: the identity dedups `want()` while it grows.
        val asksByUrl = HashMap<NormalizedRelayUrl, HashMap<String, MutableList<Ask>>>()
        val wantsByUrl = HashMap<NormalizedRelayUrl, HashMap<String, MutableSet<String>>>()

        fun want(
            url: NormalizedRelayUrl,
            ask: Ask,
        ) {
            val mine = wantsByUrl.getOrPut(url) { HashMap() }.getOrPut(ask.stream.name) { LinkedHashSet() }
            if (mine.add(ask.filter.toJson())) {
                asksByUrl.getOrPut(url) { HashMap() }.getOrPut(ask.stream.name) { mutableListOf() } += ask
            }
        }
        for (stream in streams) {
            // Declared urls skip discovery, the gate and the fold.
            for (url in stream.urls) {
                for (filter in asksOf(stream.filter, DiscoveredRelay(url))) want(url, Ask(stream, filter))
            }
            val discovery = stream.discovery ?: continue
            for (relay in permitted(stream, discovery)) {
                for (filter in asksOf(stream.filter, relay)) want(relay.url, Ask(stream, filter))
            }
        }
        // Shared authors are read off every ask in the built roster, not only the scan branch.
        val byAuthor = HashMap<String, HashMap<String, MutableSet<NormalizedRelayUrl>>>()
        for ((url, byStream) in asksByUrl) {
            for (ask in byStream.values.flatten()) {
                ask.filter.authors?.forEach { author ->
                    byAuthor.getOrPut(ask.stream.name) { HashMap() }.getOrPut(author) { mutableSetOf() } += url
                }
            }
        }
        val shared = byAuthor.mapValues { (_, authors) -> authors.filterValues { it.size > 1 }.keys }
        val standing =
            try {
                verdicts(asksByUrl.keys.toList())
            } catch (e: CancellationException) {
                // A rebuild cancelled at shutdown is not a store that could not answer.
                throw e
            } catch (e: Exception) {
                // An unread verdict is unmeasured, so every ask keeps trying. `measured` stays
                // empty with it, which reads as "not watched" rather than inventing coverage.
                System.err.println("router: could not read the NIP-77 verdicts (${e.message?.take(120)}) — audits will try every relay this rebuild")
                RelayVerdictRecord.Verdicts()
            }
        val units =
            asksByUrl.mapValues { (url, byStream) ->
                byStream.mapValues { (stream, asks) -> UnitAsks(asks, wantsByUrl[url]?.get(stream).orEmpty()) }
            }
        return Roster(
            asks = units,
            sharedAuthors = shared,
            speaksNegentropy = standing.speaksNegentropy,
            measured = standing.measured,
            watching = watching,
        )
    }

    /** One stream's diallable relays: everything its sources found, intersected with what its `gatedBy` vouches for. */
    private suspend fun permitted(
        stream: SyncStream,
        discovery: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> {
        val found = discovered(stream, discovery)
        if (discovery.gatedBy.isEmpty()) return found
        // Cached on the same per-source clock the sources use.
        val vouched = cached(stream.name, "gate", discovery.gatedBy, discovery).mapTo(HashSet()) { it.url }
        val kept = found.filter { it.url in vouched }
        if (kept.size != found.size) {
            System.err.println(
                "router: ${stream.name} — ${found.size - kept.size} of ${found.size} discovered relay(s) held out " +
                    "by `gatedBy`; an ungated url waits like any new relay, for the monitor's fast lane and its " +
                    "first verdict",
            )
        }
        return kept
    }

    /** A stream's discovery, cached per source for that source's own `refreshSeconds`, with the fold applied. */
    private suspend fun discovered(
        stream: SyncStream,
        discovery: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> {
        val perSource = cached(stream.name, "source", discovery.sources, discovery)
        // Over the whole discovered universe: `dropFolded` diffs against last time, and a missing url un-hides.
        val all = perSource
        val folded = foldedAway(all.map { it.url }.distinct())
        bands.dropFolded(stream.name, folded.keys, keep = keepBands)
        return if (folded.isEmpty()) all else all.filter { it.url !in folded }
    }

    /** [sources] read out of the store, each held for its own `refreshSeconds`; [what] keeps a gate's cache apart. */
    private suspend fun cached(
        streamName: String,
        what: String,
        sources: List<RelaySource>,
        discovery: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> {
        val nowMs = System.currentTimeMillis()
        return sources.flatMapIndexed { index, source ->
            val key = "$streamName#$what$index"
            scans[key]?.takeIf { it.expiresAtMs > nowMs }?.relays ?: run {
                val relays = urlsFound(listOf(source), discovery)
                val ttl = source.refreshSeconds ?: discovery.refreshSeconds
                // An empty result is held only briefly: the next ingested event ends "no lists yet".
                val holdMs = if (relays.isEmpty()) VisitPool.EMPTY_ROSTER_RETRY_MS else ttl * 1000L
                scans[key] = ScannedList(nowMs + holdMs, relays)
                relays
            }
        }
    }

    /** Every relay [sources] name, unioned, with the stream's excludes and this relay itself dropped. */
    private suspend fun urlsFound(
        sources: List<RelaySource>,
        discovery: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> =
        RelayDiscovery.discover(
            store,
            discovery.copy(sources = sources, gatedBy = emptyList()),
            skip = setOfNotNull(store.relay),
            allowOnion = tor != null,
        )

    companion object {
        /**
         * The ask filters one discovered relay contributes: one per bound author, so a new
         * provider naming the relay is a new band beside the old ones rather than an
         * invalidation. Sorted, since a band is keyed on the filter's serialised form.
         */
        internal fun asksOf(
            base: Filter,
            discovered: DiscoveredRelay,
        ): List<Filter> {
            val authors = discovered.bindings["authors"]
            if (authors.isNullOrEmpty()) return listOf(discovered.narrowed(base))
            return authors.sorted().map { author ->
                DiscoveredRelay(discovered.url, discovered.bindings + ("authors" to setOf(author))).narrowed(base)
            }
        }
    }
}
