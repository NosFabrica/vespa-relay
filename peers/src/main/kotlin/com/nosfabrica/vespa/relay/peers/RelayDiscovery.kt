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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.BindingSlot
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent

/**
 * One relay a [RelayDiscoveryConfig] found, and what the tags that named it
 * paired it with. [bindings] is empty for a select that binds nothing but the
 * url; the stream then asks this relay for its whole filter.
 */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
    val bindings: Map<String, Set<String>> = emptyMap(),
) {
    /**
     * [base] narrowed by everything this relay was paired with. Values are
     * sorted because a band is keyed on the filter's serialized form.
     */
    fun narrowed(base: Filter): Filter {
        if (bindings.isEmpty()) return base
        var f = base
        bindings["authors"]?.let { f = f.copy(authors = it.sorted()) }
        bindings["ids"]?.let { f = f.copy(ids = it.sorted()) }
        bindings["kinds"]?.let { v -> f = f.copy(kinds = v.mapNotNull { it.toIntOrNull() }.sorted()) }
        // Filter.tags keys drop the '#': `#p` on the wire is `p` in the map.
        val tags = bindings.filterKeys { it.startsWith("#") }
        if (tags.isNotEmpty()) {
            f = f.copy(tags = (f.tags ?: emptyMap()) + tags.map { (k, v) -> k.substring(1) to v.sorted() })
        }
        return f
    }
}

/**
 * Reads a dynamic stream's relay list out of the store. Every relay list in
 * the protocol is a tag with a url at a fixed offset, so one extraction path
 * driven by [RelaySource] covers NIP-65 outboxes, NIP-51 sets, NIP-66 reports,
 * NIP-85 provider lists and the hints on `e`/`p`/`a`/`q` tags. Every source is
 * read and unioned; only [RelayDiscoveryConfig.exclude] and the caller's
 * `skip` set leave relays out.
 */
object RelayDiscovery {
    /**
     * Whether the engine's aggregate may answer this source, instead of a
     * paged walk that reads every matching event.
     *
     * PURE AND PUBLIC so the conditions can be tested one at a time: a miss
     * here does not throw, it answers a SUPERSET — urls nobody named, dialled
     * as if they had been — so each condition is worth its own case.
     *
     * The aggregate reads `tag_index`, which is lossy in three ways at once
     * (single-letter names, first values only, nothing of the rest of the
     * tag). Every clause below is a way the caller's question stops being the
     * one that survives all three:
     *
     *  - `maxRelaysPerList` drops an EVENT for naming too many relays, and an
     *    aggregate has no events to drop — the ~9k-entry synthetic lists this
     *    deployment has seen would come straight back in.
     *  - a `where` is a condition on another element of the tag (NIP-65's
     *    write marker); `tag_index` does not carry one.
     *  - `bindings` pair a url with a value from the SAME tag occurrence
     *    (the outbox model's author); an aggregate has no occurrences.
     *  - a multi-character tag name is not in `tag_index` at all, and
     *    `urlIndex != 1` reads a value it does not keep.
     */
    fun aggregable(
        dynamic: RelayDiscoveryConfig,
        source: RelaySource,
    ): Boolean =
        dynamic.maxRelaysPerList == null &&
            source.selects.isNotEmpty() &&
            source.selects.all {
                it.tag != null && it.tag.length == 1 && it.urlIndex == 1 && it.where.isEmpty() && it.bindings.isEmpty()
            }

    /** Every relay [dynamic]'s sources point at right now, sorted by url for a stable fan-out. */
    suspend fun discover(
        store: IEventStore,
        dynamic: RelayDiscoveryConfig,
        skip: Set<NormalizedRelayUrl> = emptySet(),
        pageSize: Int = SCAN_PAGE,
        // Whether this deployment has a Tor transport; the default drops onions.
        allowOnion: Boolean = false,
        now: Long = nowSeconds(),
        /**
         * Called as each of [RelayDiscoveryConfig.sources] finishes. A source
         * is the only unit this walk can be counted in from outside.
         */
        onSource: () -> Unit = {},
    ): List<DiscoveredRelay> {
        val found = LinkedHashSet<NormalizedRelayUrl>()
        // url -> destination -> values, unioned across every select and source.
        val narrowing = HashMap<NormalizedRelayUrl, MutableMap<String, MutableSet<String>>>()
        // Reported rather than silently applied: a cap set too low reads from
        // outside exactly like a store that holds nothing.
        var oversizedLists = 0
        for (source in dynamic.sources) {
            // [RelaySource.maxAgeSeconds] applied here and once, combined with
            // any `since` the caller already set: two floors on one field mean
            // the later one, or the fast lane's narrowing becomes a full sweep.
            val floor = source.maxAgeSeconds?.let { now - it }
            val bounded =
                if (floor == null) source.filter else source.filter.copy(since = maxOf(floor, source.filter.since ?: floor))
            // ONE INDEXED WALK PER SOURCE, ANSWERING EVERY SELECT IT CARRIES,
            // and never the store's tags projection — which reads ONE tag name
            // per call off a `document/v1` visit whose selection is evaluated
            // per document, with no index behind it.
            //
            // That visit's cost is the CORPUS, not the answer, so it is the one
            // read here that gets worse for free. Measured on the staging store
            // (#182), reading the 364 kind-10040 declarations in a 319,426,563
            // event corpus:
            //
            //   /search/  yql=select id from sources * where kind=10040   0.0058s
            //   /document/v1 …?selection=(event.kind==10040)             75.0260s
            //
            // 12,800x, once per tag name, and the monitor's 10040 source names
            // 38 of them — 38 corpus walks per pass, on the document API the
            // ingest dedup probe shares, which is the mechanism behind the
            // wedges in #167.
            //
            // NO SIZE RULE DECIDES THIS, because at the scale this relay is
            // built for there is nothing left to decide. The two costs scale
            // differently — paging is a function of the MATCH SET, the visit of
            // the CORPUS — so the match count at which the visit would win
            // rises with the store: ~100k events at 319M, ~33M at 10^11.
            // Reaching it means parking the document API for the ~6.5 hours a
            // walk of 10^11 documents takes, which is not a price any relay
            // list is worth. A relay-list kind is measured in millions; the
            // corpus is heading for hundreds of billions, and the gap only
            // widens. A rule that can only ever answer one way is not a rule.
            //
            // So the fork is gone, and with it the reason a BOUND select was
            // the odd one out: a binding could never take the projection (it
            // hands back a SET of values, and the pairing a binding exists to
            // keep is gone by the time it returns), which quietly made
            // `authors = 1` the fast path for reasons no operator could see.
            // Every select of a source now rides the same walk, and
            // [RelayDiscoveryConfig.maxRelaysPerList] always applies — it is a
            // per-EVENT cap and this is the only path where the event is still
            // whole, so setting it no longer costs a source its read.
            // THE ENGINE ANSWERS IT WHERE IT CAN, and the preconditions are
            // the whole of the safety. `tag_index` is a lossy projection —
            // single-letter names, first values only, nothing of the rest of
            // the tag — so an aggregate over it can answer one question:
            // every url of one short tag, unconditionally. Each check below
            // is a way that question stops being the one being asked, and a
            // miss costs a SUPERSET rather than an error, which is why they
            // are enumerated rather than summarised.
            val aggregate = if (aggregable(dynamic, source)) (store as? VespaEventStore)?.store else null
            val stillPaged = if (aggregate != null) emptyList() else source.selects
            if (aggregate != null) {
                // One grouping per select instead of a walk of the corpus:
                // measured at ~1s against ~157s for 3.27M NIP-65 lists. The
                // urls come back raw, so they take the SAME `normalize` the
                // paged path applies per tag — over tens of thousands of
                // distinct values rather than millions of documents.
                for (select in source.selects) {
                    aggregate
                        .distinctTagValues(bounded, select.tag!!, unconditional = true)
                        .forEach { raw -> normalize(raw, allowOnion)?.let { found += it } }
                }
            }
            if (stillPaged.isNotEmpty()) {
                scan(store, bounded, pageSize) { event ->
                    if (oversized(event, stillPaged, dynamic.maxRelaysPerList)) {
                        oversizedLists++
                        return@scan
                    }
                    for (select in stillPaged) {
                        if (select.kind != null && select.kind != event.kind) continue
                        bindingsIn(event, select, allowOnion) { url, bound ->
                            found += url
                            if (bound.isNotEmpty()) {
                                val per = narrowing.getOrPut(url) { HashMap() }
                                for ((dest, value) in bound) per.getOrPut(dest) { HashSet() }.add(value)
                            }
                        }
                    }
                }
            }
            onSource()
        }

        // Nothing downstream can tell "no authors were paired" from "the
        // relays had nothing", so say what the pairing built.
        if (narrowing.isNotEmpty()) {
            val perRelay = narrowing.values.map { it["authors"]?.size ?: 0 }
            val distinctAuthors =
                narrowing.values
                    .flatMap { it["authors"].orEmpty() }
                    .toSet()
                    .size
            System.err.println(
                "router: discovery paired ${narrowing.size} relay(s) with $distinctAuthors distinct author(s); " +
                    "authors per relay min=${perRelay.minOrNull()} median=${perRelay.sorted().getOrNull(perRelay.size / 2)} " +
                    "max=${perRelay.maxOrNull()} total=${perRelay.sum()}; " +
                    "${found.size - narrowing.size} relay(s) found with NO authors attached",
            )
        }

        if (oversizedLists > 0) {
            System.err.println(
                "router: discovery skipped $oversizedLists relay list(s) over ${dynamic.maxRelaysPerList} entries " +
                    "— see RelayDiscoveryConfig.maxRelaysPerList",
            )
        }

        return found
            .asSequence()
            .filter { it !in dynamic.exclude && it !in skip }
            .map { url ->
                DiscoveredRelay(url, narrowing[url]?.mapValues { (_, v) -> v.toSet() }.orEmpty())
            }.sortedBy { it.url.url }
            .toList()
    }

    /**
     * The hold-out read: urls one of [monitorAuthors] currently calls `dead`,
     * so a probe pass does not spend a connect timeout re-learning it.
     *
     * `dead` alone, never the other refusals: those are verdicts a relay
     * earned by answering, and holding them out would stop the fold and the
     * stability gate from ever re-measuring them. Empty authors holds nothing
     * out, the opposite of what an absent `authors` means to a gate: unscoped,
     * any stranger whose 30166s we mirror could take a relay out of every pass.
     */
    suspend fun undialable(
        store: IEventStore,
        monitorAuthors: List<String>,
        maxAgeSeconds: Long,
        allowOnion: Boolean = false,
        now: Long = nowSeconds(),
        /**
         * The urls the caller is asking about, or null for the whole dead set.
         * The fast lane asks about a dozen urls every couple of minutes and
         * must not materialize every dead record to answer.
         */
        among: Collection<NormalizedRelayUrl>? = null,
    ): Set<NormalizedRelayUrl> {
        if (monitorAuthors.isEmpty()) return emptySet()
        // Empty is a caller with no urls; null is a caller wanting all of them.
        if (among != null && among.isEmpty()) return emptySet()
        return verdicts(store, Verdict.DEAD, monitorAuthors, maxAgeSeconds, now, among)
            .mapNotNullTo(HashSet()) { urlOf(it, allowOnion) }
    }

    /**
     * Every url this router holds a current record about, whatever it says:
     * the corpus behind the coverage card's "every relay url this router
     * knows of". [self] alone, not [undialable]'s trust set, or a mirrored
     * foreign monitor's world would become the mouth of our coverage tree.
     * Null [self] is a router with no signer, which holds no records.
     */
    suspend fun recorded(
        store: IEventStore,
        self: String?,
        maxAgeSeconds: Long,
        allowOnion: Boolean = false,
        now: Long = nowSeconds(),
    ): Set<NormalizedRelayUrl> {
        if (self == null) return emptySet()
        val filter = Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), since = now - maxAgeSeconds)
        val found = HashSet<NormalizedRelayUrl>()
        scan(store, filter, SCAN_PAGE) { event -> urlOf(event, allowOnion)?.let(found::add) }
        return found
    }

    /**
     * Records carrying [verdict] in our vocabulary, signed by these identities,
     * re-checked since the floor. The tag index answers on the label's value
     * alone, and `l` on a 30166 also carries other monitors' country codes and
     * ASNs, so the namespace is re-checked on what comes back.
     */
    private suspend fun verdicts(
        store: IEventStore,
        verdict: Verdict,
        monitorAuthors: List<String>,
        maxAgeSeconds: Long,
        now: Long,
        among: Collection<NormalizedRelayUrl>? = null,
    ): List<Event> {
        // Subject-bound when `among` is given: chunked by `#d`, and the label
        // tag comes off because the check below reads the verdict either way.
        val queried =
            if (among != null) {
                among
                    .map { it.url }
                    .chunked(RelayVerdictRecord.QUERY_CHUNK)
                    .flatMap { chunk ->
                        read(
                            store,
                            Filter(
                                kinds = listOf(RelayDiscoveryEvent.KIND),
                                authors = monitorAuthors.takeIf { it.isNotEmpty() },
                                tags = mapOf("d" to chunk),
                                since = now - maxAgeSeconds,
                            ),
                        )
                    }
            } else {
                read(
                    store,
                    Filter(
                        kinds = listOf(RelayDiscoveryEvent.KIND),
                        // Absent, not empty: an empty `authors` list matches nothing.
                        authors = monitorAuthors.takeIf { it.isNotEmpty() },
                        tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(verdict.value)),
                        since = now - maxAgeSeconds,
                    ),
                )
            }
        return queried.filter { event ->
            val s =
                event.tags.firstOrNull {
                    it.size > RelayVerdictRecord.LABEL_NAMESPACE_INDEX &&
                        it[0] == RelayVerdictRecord.LABEL_TAG &&
                        it[RelayVerdictRecord.LABEL_NAMESPACE_INDEX] == RelayVerdictRecord.FITNESS_NAMESPACE
                }
            // The author check re-states the store's `authors` predicate on the
            // returned events, so a stranger's verdict cannot slip through.
            (monitorAuthors.isEmpty() || event.pubKey in monitorAuthors) &&
                s != null &&
                s[1] == verdict.value
        }
    }

    /**
     * One page of a [scan], booked as [caller]'s. A function because [scan]
     * asks for the same page twice when it is entirely one `created_at`, and
     * each ask is a round trip worth booking.
     */
    private suspend fun queryPage(
        store: IEventStore,
        caller: String,
        filter: Filter,
        until: Long?,
        ask: Int,
    ): List<Event> {
        val asked = filter.copy(until = until, limit = ask)
        return storeCall(caller, StoreCalls.OP_QUERY, StoreCalls.summarise(asked)) { store.query(asked) }
    }

    /**
     * One verdict read, booked as the round-up's: [verdicts] serves the url
     * round-up, not the monitor, whose own reads book under `RelayVerdictRecord`.
     */
    private suspend fun read(
        store: IEventStore,
        filter: Filter,
    ): List<Event> =
        storeCall(StoreCalls.CALLER_SOURCE_RELAY_LISTS, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
            store.query<Event>(filter)
        }

    /** A verdict record's subject: the `d` tag, normalized like every other discovered url. */
    private fun urlOf(
        event: Event,
        allowOnion: Boolean,
    ): NormalizedRelayUrl? =
        event.tags
            .firstOrNull { it.size > 1 && it[0] == "d" }
            ?.get(1)
            ?.let { normalize(it, allowOnion) }

    /**
     * Walk everything [filter] matches a page at a time, oldest-ward, so
     * memory is a page rather than the corpus. `until` is inclusive, so a run
     * of events sharing the boundary `created_at` is carried forward and
     * skipped on the next page; a page that is entirely one timestamp grows
     * until it spans two, the one place a page may exceed [pageSize].
     *
     * Internal because [RelayVerdictRecord.loadAll] pages a corpus too, and a
     * second cursor is a second place to get the boundary wrong.
     */
    suspend fun scan(
        store: IEventStore,
        filter: Filter,
        pageSize: Int,
        /** Who the pages are booked to; see [StoreCalls]. The round-up and the monitor's corpus walk are different clocks. */
        caller: String = StoreCalls.CALLER_SOURCE_RELAY_LISTS,
        onEach: (Event) -> Unit,
    ) {
        // An explicit `limit` is the caller's budget for the whole scan, and
        // only events actually handed to [onEach] spend it.
        var remaining = filter.limit ?: Int.MAX_VALUE
        var until = filter.until
        var boundaryIds = emptySet<String>()
        while (remaining > 0) {
            val budget = remaining.toLong() + boundaryIds.size
            var ask = minOf(pageSize.toLong(), budget).toInt()
            var page: List<Event> = queryPage(store, caller, filter, until, ask)
            if (page.isEmpty()) return

            // Results are newest-first, so a page whose ends share a
            // `created_at` is one timestamp and the cursor has nowhere to go.
            while (page.size == ask && ask < budget && page.first().createdAt == page.last().createdAt) {
                // Clamped to Int range before narrowing: a wrapped, negative
                // `ask` is the store's matches-nothing sentinel and the walk
                // would restart from the top forever.
                ask = minOf(ask.toLong() * 2, budget).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
                page = queryPage(store, caller, filter, until, ask)
            }

            var oldest = Long.MAX_VALUE
            for (event in page) {
                if (remaining <= 0) break
                if (event.id !in boundaryIds) {
                    onEach(event)
                    remaining--
                }
                if (event.createdAt < oldest) oldest = event.createdAt
            }
            // Short page: the store had nothing older left to give.
            if (page.size < ask) return

            val newBoundary = HashSet<String>()
            for (event in page) if (event.createdAt == oldest) newBoundary.add(event.id)
            if (newBoundary.size == page.size) {
                // Still one timestamp after growing, out of budget to widen:
                // step below it.
                until = oldest - 1
                boundaryIds = emptySet()
            } else {
                until = oldest
                boundaryIds = newBoundary
            }
        }
    }

    /** Events per page: one or two round trips for a normal relay-list scan. */
    private const val SCAN_PAGE = 10_000

    /**
     * Is this event too long to be a relay list? Counted over the tags a
     * select would extract, not the whole tag array. An event over the cap
     * is dropped whole: taking the first N would let the author choose which
     * relays we see by ordering them. Null disables it.
     */
    private fun oversized(
        event: Event,
        selects: List<RelaySelect>,
        cap: Int?,
    ): Boolean {
        if (cap == null) return false
        var seen = 0
        for (tag in event.tags) {
            // Counted once however many selects claim it, and only if one would
            // extract it: `where` is part of that test, so a 10002 with 200
            // read relays and three write ones does not trip a write select's cap.
            val extracted =
                selects.any { select ->
                    (select.kind == null || select.kind == event.kind) &&
                        tag.size > select.urlIndex &&
                        (select.tag == null || tag[0] == select.tag) &&
                        (select.where.isEmpty() || select.where.any { it.matches(tag) })
                }
            if (extracted) {
                seen++
                if (seen > cap) return true
            }
        }
        return false
    }

    /** The distinct relay urls one event advertises across every applicable select. */
    fun urlsIn(
        event: Event,
        selects: List<RelaySelect>,
    ): Set<NormalizedRelayUrl> = LinkedHashSet<NormalizedRelayUrl>().also { urlsIn(event, selects, it) }

    private fun urlsIn(
        event: Event,
        selects: List<RelaySelect>,
        into: MutableSet<NormalizedRelayUrl>,
    ) {
        for (select in selects) {
            if (select.kind != null && select.kind != event.kind) continue
            urlsIn(event, select, into)
        }
    }

    /** The relay urls one event advertises for a single [select]. */
    fun urlsIn(
        event: Event,
        select: RelaySelect,
        allowOnion: Boolean = false,
    ): Set<NormalizedRelayUrl> = LinkedHashSet<NormalizedRelayUrl>().also { urlsIn(event, select, it, allowOnion) }

    private fun urlsIn(
        event: Event,
        select: RelaySelect,
        into: MutableSet<NormalizedRelayUrl>,
        allowOnion: Boolean = false,
    ) = bindingsIn(event, select, allowOnion) { url, _ -> into.add(url) }

    /**
     * Every (url, bound values) pair one event yields for one select. The tag
     * is the unit: each matching tag calls [onMatch] once with its own slots,
     * so the pairing survives. Gathering slots into separate sets would
     * produce the cross product.
     */
    private inline fun bindingsIn(
        event: Event,
        select: RelaySelect,
        allowOnion: Boolean,
        onMatch: (NormalizedRelayUrl, Map<String, String>) -> Unit,
    ) {
        for (tag in event.tags) {
            if (tag.size <= select.urlIndex) continue
            if (select.tag != null && tag[0] != select.tag) continue
            // `where` entries OR together and each ANDs its own fields.
            if (select.where.isNotEmpty() && select.where.none { it.matches(tag) }) continue
            val url = normalize(tag[select.urlIndex], allowOnion) ?: continue
            if (select.bindings.isEmpty()) {
                onMatch(url, emptyMap())
                continue
            }
            val bound = HashMap<String, String>(select.bindings.size)
            var complete = true
            for ((dest, slot) in select.bindings) {
                val raw =
                    when (slot) {
                        is BindingSlot.OfTag -> tag.getOrNull(slot.index)
                        BindingSlot.EventPubkey -> event.pubKey
                        BindingSlot.EventId -> event.id
                    }
                // A tag that cannot fill a binding is dropped whole: half-applied,
                // it would widen the ask back to every author on that relay.
                val ok = raw?.takeIf { it.isNotBlank() }?.takeIf { valid(dest, it) }
                if (ok == null) {
                    complete = false
                    break
                }
                bound[dest] = ok
            }
            if (complete) onMatch(url, bound)
        }
    }

    /**
     * Whether a value can be what the destination says it is. A malformed
     * one off a stranger's event is skipped, not fatal, but an `authors`
     * entry that is not a key would make a filter no relay can answer.
     */
    private fun valid(
        dest: String,
        value: String,
    ): Boolean =
        when {
            dest == "kinds" -> value.toIntOrNull() != null
            dest == "authors" || dest == "ids" || dest == "#p" || dest == "#e" -> isHex64(value)
            else -> true
        }

    private fun isHex64(v: String): Boolean = v.length == 64 && v.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }

    /**
     * A tag value as a dialable url, or null. The rules live in
     * [RelayUrlCache.decide]: a `ws`/`wss` scheme demanded before and after
     * the normalizer's repairs, no whitespace, no loopback, and no `.onion`
     * unless [allowOnion].
     */
    private fun normalize(
        raw: String,
        allowOnion: Boolean,
    ): NormalizedRelayUrl? = RelayUrlCache.Default.normalize(raw, allowOnion)
}
