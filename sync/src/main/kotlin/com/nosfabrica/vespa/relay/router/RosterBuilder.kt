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

import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.discovery.DiscoveredRelay
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import java.util.concurrent.ConcurrentHashMap

/**
 * WHAT THE POOL SHOULD BE SYNCING — the roster derivation, apart from the
 * machinery that runs it.
 *
 * One [rebuild] reads the monitor's standing verdicts and the streams'
 * certified scans out of the store and answers url → asks. No sockets, ever:
 * everything here is store walks and record reads, which is what makes the
 * derivation testable without a client and keeps [VisitPool] itself a
 * scheduler — the pool calls [rebuild] on its roster clock and owns
 * everything after: queueing, visits, tails, pacing.
 */
internal class RosterBuilder(
    private val store: IEventStore,
    /** The visit-mode streams: every relaySource entry a kind-30166 verdict source or a certified scan. */
    private val streams: List<SyncStream>,
    /**
     * Whose 30166 verdicts admit a relay where a source names no `authors` —
     * this process's own signer. See [RelayDiscovery.syncable].
     */
    private val monitorAuthor: String?,
    /** For [SyncBands.dropFolded], as the fold is applied to a scan — see [certifiedScan]. */
    private val bands: SyncBands,
    /**
     * The duplicate-url fold, applied READ-ONLY: url → the survivor standing
     * in for it, over one scan's candidates ([AliasFolding.apply] — standing
     * verdicts, no sockets). Used for what the legacy cycle did as the fold
     * took hold: the duplicate out of the dial list, and its stale bands out
     * of the state file ([SyncBands.dropFolded]) — left in, the coverage
     * card names a dozen urls of one host as separately walked while one of
     * them is synced. Defaults to folding nothing: the probes, and a router
     * with no signer to read verdicts by.
     */
    private val foldedAway: suspend (List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl> = { emptyMap() },
    /** Urls a static subscription holds — their bands are never dropped, see [SyncBands.dropFolded]. */
    private val keepBands: Set<NormalizedRelayUrl> = emptySet(),
    private val tor: TorTransport? = null,
) {
    /**
     * ONE UNIT OF WORK against one relay: the stream asking, and the exact
     * filter it asks — the stream's own for a verdict source, or the
     * scan-paired narrow for a bound scan (one Ask PER BOUND AUTHOR, fixed:
     * the tag structure already decided the granularity, and a per-author ask
     * is what keeps a `(relay, provider)` band valid forever — see the
     * `asksOf` arithmetic). Bands, catch-ups, audits and tails all key on
     * this filter, which is why the port is a type change and not an engine.
     */
    internal data class Ask(
        val stream: SyncStream,
        val filter: Filter,
    )

    /** One rebuild's whole answer. */
    internal class Roster(
        /** url → the asks that want it. */
        val asks: Map<NormalizedRelayUrl, List<Ask>>,
        /**
         * url → [wants] of its asks, computed once here. The pool compares
         * these across rebuilds and keys tails on them; recomputing both
         * sides per url per tick serialized every filter to JSON twice for
         * an answer that is almost always "unchanged".
         */
        val wants: Map<NormalizedRelayUrl, Set<String>> = emptyMap(),
        /**
         * Per stream: authors found at MORE THAN ONE relay. One relay's empty
         * answer does not retract what a sibling relay may still be serving,
         * so the retraction audit never judges their asks — the same rule the
         * legacy cycle applied, computed on the roster clock.
         */
        val sharedAuthors: Map<String, Set<String>>,
    )

    /** One certified scan's discovery, held for its stream's `refreshSeconds` — a store walk is not a poll. */
    private class ScannedList(
        val expiresAtMs: Long,
        val relays: List<DiscoveredRelay>,
    )

    private val scans = ConcurrentHashMap<String, ScannedList>()

    suspend fun rebuild(): Roster {
        val next = HashMap<NormalizedRelayUrl, MutableList<Ask>>()
        // One identity set per url, reused three ways: it dedups want() by
        // VALUE (Ask equality degrades to Filter reference equality, so the
        // old `ask !in wanting` linear-scanned and matched nothing for
        // freshly built asks), and it IS the per-url wants set the Roster
        // carries out.
        val seen = HashMap<NormalizedRelayUrl, MutableSet<String>>()

        fun want(
            url: NormalizedRelayUrl,
            ask: Ask,
        ) {
            if (seen.getOrPut(url) { LinkedHashSet() }.add("${ask.stream.name} ${ask.filter.toJson()}")) {
                next.getOrPut(url) { mutableListOf() } += ask
            }
        }
        // Memoized per rebuild: identical verdict sources across streams are
        // the common config, and each un-memoized read materializes every
        // syncable record in the store. Excludes are applied per stream on
        // the way out — they are the only per-stream part of the read.
        val syncableMemo = HashMap<Pair<List<String>, Long>, List<DiscoveredRelay>>()

        suspend fun syncableFor(
            authors: List<String>,
            maxAgeSeconds: Long,
        ) = syncableMemo.getOrPut(authors to maxAgeSeconds) {
            RelayDiscovery.syncable(
                store,
                monitorAuthors = authors,
                maxAgeSeconds = maxAgeSeconds,
                skip = setOfNotNull(store.relay),
                allowOnion = tor != null,
            )
        }
        for (stream in streams) {
            val discovery = stream.discovery ?: continue
            for (source in discovery.verdictSources) {
                val authors = monitorIdentity(source.authors, stream.name, "has a verdict source") ?: continue
                val certified = syncableFor(authors, source.maxAgeSeconds).filter { it.url !in discovery.exclude }
                for (relay in certified) want(relay.url, Ask(stream, stream.filter))
            }
            if (discovery.scanSources.isNotEmpty()) {
                for (relay in certifiedScan(stream, discovery)) {
                    for (filter in asksOf(stream.filter, relay)) {
                        want(relay.url, Ask(stream, filter))
                    }
                }
            }
        }
        // Shared authors are read off EVERY ask in the built roster — verdict
        // sources and scans alike. They used to be counted only in the scan
        // branch, so an author-bound stream filter fanned to N relays by a
        // verdict source ran its retraction audits with an empty shared set,
        // and one relay's answer could retract what its siblings still serve.
        val byAuthor = HashMap<String, HashMap<String, MutableSet<NormalizedRelayUrl>>>()
        for ((url, asks) in next) {
            for (ask in asks) {
                ask.filter.authors?.forEach { author ->
                    byAuthor.getOrPut(ask.stream.name) { HashMap() }.getOrPut(author) { mutableSetOf() } += url
                }
            }
        }
        val shared = byAuthor.mapValues { (_, authors) -> authors.filterValues { it.size > 1 }.keys }
        return Roster(asks = next, wants = seen, sharedAuthors = shared)
    }

    /**
     * The verdict-writing identity one source trusts: its configured
     * `authors`, or our own signer where none are named. Null — with the
     * warning said on EVERY rebuild rather than once ever, so an operator
     * fixing the signer sees it take effect — when there is neither:
     * verdicts nobody here can write admit nothing.
     */
    private fun monitorIdentity(
        named: List<String>,
        stream: String,
        what: String,
    ): List<String>? {
        val authors = named.ifEmpty { listOfNotNull(monitorAuthor) }
        if (authors.isEmpty()) {
            System.err.println(
                "router: $stream $what, no `authors` and no signer — no monitor identity, nothing is admitted",
            )
            return null
        }
        return authors
    }

    /**
     * A certified scan's relay list, cached for the stream's `refreshSeconds`
     * — deriving a scan is a store walk, and the roster loop ticks far more
     * often than the list changes. An EMPTY result is cached only briefly:
     * "no provider lists yet" is the state the next ingested event ends.
     *
     * Discovered and gated PER SOURCE: each scan's own `certified` block —
     * its authors AND its freshness — judges only the relays that source
     * supplied. One gate across the union (the old "strictest" arithmetic,
     * chosen by maxAge alone) enforced the winning gate's authors against
     * every source's relays, silently discarding a second source's trust
     * binding: relays certified only by ITS named monitor were held out for
     * lacking the other's verdict.
     */
    private suspend fun certifiedScan(
        stream: SyncStream,
        discovery: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> {
        val nowMs = System.currentTimeMillis()
        scans[stream.name]?.takeIf { it.expiresAtMs > nowMs }?.let { return it.relays }
        val perSource =
            discovery.scanSources.map { source ->
                source to
                    RelayDiscovery.discover(
                        store,
                        discovery.copy(sources = listOf(source)),
                        skip = setOfNotNull(store.relay),
                        allowOnion = tor != null,
                    )
            }
        // The fold, applied where the legacy cycle applied it — over the
        // WHOLE discovered universe, because `dropFolded` diffs against last
        // time and a url missing from the set un-hides. Called on the scan
        // clock, never with an increment.
        val folded = foldedAway(perSource.flatMap { (_, found) -> found.map { it.url } }.distinct())
        bands.dropFolded(stream.name, folded.keys, keep = keepBands)
        val relays =
            perSource.flatMap { (source, found) ->
                val scanned = if (folded.isEmpty()) found else found.filter { it.url !in folded }
                val gate = source.certified ?: return@flatMap scanned
                val authors = monitorIdentity(gate.authors, stream.name, "gates its scan on verdicts") ?: return@flatMap emptyList()
                RelayDiscovery
                    .certifiedOnly(store, scanned, authors, gate.maxAgeSeconds, allowOnion = tor != null)
                    .also {
                        if (it.size != scanned.size) {
                            System.err.println(
                                "router: ${stream.name} — ${scanned.size - it.size} of ${scanned.size} scanned relay(s) " +
                                    "held out uncertified (no fresh syncable verdict); the monitor's fast lane is their way in",
                            )
                        }
                    }
            }
        val holdMs = if (relays.isEmpty()) VisitPool.EMPTY_ROSTER_RETRY_MS else discovery.refreshSeconds * 1000L
        scans[stream.name] = ScannedList(nowMs + holdMs, relays)
        return relays
    }

    companion object {
        /**
         * The ask filters one discovered relay contributes: ONE PER BOUND
         * AUTHOR, fixed — no knob. The tag structure already decided the
         * granularity (a `30382:rank` tag pairs one provider with one relay),
         * and the per-author split is what keeps each `(relay, provider)`
         * band's filter — and therefore the band — valid forever: a new
         * provider naming the relay is a new band beside the old ones, never
         * an invalidation of them. This is `authorsPerLeg = 1` made
         * structural; every other value of that knob answered a question the
         * data answers better. A select that binds nothing keeps one ask with
         * the stream's whole filter; narrow keys other than `authors` ride
         * along in every split, sorted by the same argument
         * [DiscoveredRelay.narrowed] sorts — a band is keyed on the filter's
         * serialized form.
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

        /**
         * The IDENTITY of an ask set, for change detection: stream name plus
         * the filter's own JSON — the `toJson` keying [SyncBands] already
         * trusts. quartz's [Filter] compares by reference, so two rebuilds
         * that derive the very same roster produce unequal [Ask]s; comparing
         * those directly would requeue the whole roster on every tick, and
         * comparing nothing left a new ask waiting out the tailed revisit.
         */
        internal fun wants(asks: List<Ask>): Set<String> = asks.mapTo(mutableSetOf()) { "${it.stream.name} ${it.filter.toJson()}" }
    }
}
