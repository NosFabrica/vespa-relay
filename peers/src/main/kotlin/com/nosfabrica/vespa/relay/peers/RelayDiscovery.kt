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

import com.nosfabrica.vespa.relay.config.BindingSlot
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent

/**
 * One relay a [RelayDiscoveryConfig] found, and what the tags that named it paired
 * it with. [bindings] is empty for a select that binds nothing but the url, and
 * the stream then asks this relay for its whole filter.
 */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
    val bindings: Map<String, Set<String>> = emptyMap(),
) {
    /**
     * [base] narrowed by everything this relay was paired with. Values are
     * sorted, because a band is keyed on the filter's serialized form —
     * an unordered set would key the same ask two different ways on two runs
     * and re-walk history for nothing.
     */
    fun narrowed(base: Filter): Filter {
        if (bindings.isEmpty()) return base
        var f = base
        bindings["authors"]?.let { f = f.copy(authors = it.sorted()) }
        bindings["ids"]?.let { f = f.copy(ids = it.sorted()) }
        bindings["kinds"]?.let { v -> f = f.copy(kinds = v.mapNotNull { it.toIntOrNull() }.sorted()) }
        // Filter.tags keys drop the '#' — `#p` on the wire is `p` in the map.
        val tags = bindings.filterKeys { it.startsWith("#") }
        if (tags.isNotEmpty()) {
            f = f.copy(tags = (f.tags ?: emptyMap()) + tags.map { (k, v) -> k.substring(1) to v.sorted() })
        }
        return f
    }
}

/**
 * Reads a dynamic stream's relay list out of the store. Every relay list in
 * the protocol is the same shape — a tag with a url at a fixed offset — so one
 * extraction path driven by [RelaySource] covers NIP-65 outboxes, NIP-51 relay
 * sets, NIP-66 monitor reports, NIP-85 provider lists, and the relay hints on
 * ordinary `e`/`p`/`a`/`q` tags.
 *
 * Every source is read and their relays unioned; nothing truncates the set —
 * the only relays left out are [RelayDiscoveryConfig.exclude] and the caller's
 * [discover] `skip` set.
 */
object RelayDiscovery {
    /** Every relay [dynamic]'s sources point at right now, sorted by url for a stable fan-out. */
    suspend fun discover(
        store: IEventStore,
        dynamic: RelayDiscoveryConfig,
        skip: Set<NormalizedRelayUrl> = emptySet(),
        pageSize: Int = SCAN_PAGE,
        // Whether this deployment has a Tor transport. Defaults to the
        // clearnet answer so a caller that has not thought about it drops
        // them, which is what dialling them without Tor amounts to anyway.
        allowOnion: Boolean = false,
        now: Long = nowSeconds(),
        /**
         * Called as each of [RelayDiscoveryConfig.sources] finishes, for a
         * caller reporting a position.
         *
         * A SOURCE is the only unit this walk can be counted in from outside:
         * the store reads inside it stream a projection or page an unknown
         * number of events, so nothing here knows a total until it has the
         * answer. It is per source rather than per CONFIG because a deployment
         * that has moved its relay-list parsing into `monitor { sources }` has
         * exactly one config — and a position of "0 of 1" that goes to "1 of 1"
         * is not a position. See [StreamWorld.candidates].
         */
        onSource: () -> Unit = {},
    ): List<DiscoveredRelay> {
        val found = LinkedHashSet<NormalizedRelayUrl>()
        // url -> destination -> values, unioned across every select and source.
        val narrowing = HashMap<NormalizedRelayUrl, MutableMap<String, MutableSet<String>>>()
        // Relay lists refused for being too long to be relay lists — reported
        // rather than silently applied, because a cap set too low reads from
        // outside exactly like a store that holds nothing.
        var oversizedLists = 0
        for (source in dynamic.sources) {
            // [RelaySource.maxAgeSeconds] applied, here and once: a config
            // cannot write an absolute `since` that keeps meaning what it said,
            // so a source carries the span and the read turns it into the
            // instant. Null is no bound — see there for why a relay list is
            // timeless and a verdict is not.
            //
            // COMBINED with any `since` the caller already set, never
            // overwriting it. The loader refuses both in one config entry, but
            // [StreamWorld.candidatesSince] narrows these filters at RUN time —
            // the fast lane asking "what arrived in the last two minutes" — and
            // a bound that replaced that one turned the lane into a full sweep:
            // every source carrying `maxAgeSeconds` handed back its whole
            // window, so the fitness pass re-dialled the entire roster every
            // `fastLaneSeconds` instead of the handful of new urls. Two floors
            // on one field mean the later one.
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
            val stillPaged = source.selects
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
            // This source is behind the walk, whatever it yielded.
            onSource()
        }

        // DIAGNOSTIC: what the pairing actually built. A bound select is only
        // worth anything if the authors reach the relays that named them, and
        // nothing downstream can tell "no authors were paired" from "the relays
        // had nothing".
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
     * so a probe pass does not spend a connect timeout re-learning what a
     * record already says.
     *
     * THE ONE PLACE A VERDICT VALUE IS NAMED IN CODE, and it is not a config
     * surface: this is the MONITOR plane reading records the monitor itself
     * wrote, in the vocabulary it defines ([Verdict]), to decide
     * what its own next pass should dial. Nothing an operator writes reaches
     * it, and no foreign monitor's spelling has to. The STREAM plane — which
     * relays a sync stream may dial — names nothing: it is filters all the way
     * down, and see [RelayDiscoveryConfig.gatedBy].
     *
     * Not symmetric with admitting, which is the asymmetry
     * [StreamWorld.monitorAuthors] exists to state: this stays scoped where a
     * gate may be unscoped.
     *
     * `dead` alone, never the other refusals: `alias`, `inconsistent`,
     * `unpageable`, `auth-refused` and `restricted` are all verdicts a relay
     * earned by ANSWERING, and holding those out would stop the fold and the
     * stability gate from ever re-measuring the relays they exist to judge.
     * Only the transport saying no is a reason not to dial.
     *
     * EMPTY AUTHORS HOLDS NOTHING OUT — the opposite of what an absent
     * `authors` means to a gate, and settled here rather than left to each
     * caller to remember. Unscoped, this is the starvation vector: any stranger whose
     * 30166s we mirror could take a relay out of every pass for good. A router
     * with no signer and no named monitors has no standing to call anything
     * dead, and dialling a corpse costs one timeout — which is the cheaper of
     * the two mistakes available here.
     */
    suspend fun undialable(
        store: IEventStore,
        monitorAuthors: List<String>,
        maxAgeSeconds: Long,
        allowOnion: Boolean = false,
        now: Long = nowSeconds(),
        /**
         * The urls the caller is ASKING ABOUT, or null for the whole dead set.
         *
         * A sweep wants all of it — it is about to walk the corpus, and the
         * hold-out applies to every url in it. The FAST LANE is the opposite
         * shape: it derives the handful of urls named since its last look, two
         * minutes ago, and asks only whether those are dead. Unbounded there,
         * this materialized every dead record in the store — five figures on a
         * discovered corpus — every `fastLaneSeconds`, to answer a question
         * about a dozen urls. That is the read the store bump's `maxHits` cap
         * names, run thirty times an hour against a lane whose own KDoc calls
         * it "bounded by construction".
         *
         * Bounded by `#d` in [QUERY_CHUNK]-sized chunks, the same shape
         * [RelayVerdictRecord.load] is: `d` is a single-letter tag and the only
         * part of these records the index answers on. The `l` tag comes off the
         * query when this is set, because the label check below is what decides
         * a verdict either way and one tag key is the query shape already
         * proven on this store.
         */
        among: Collection<NormalizedRelayUrl>? = null,
    ): Set<NormalizedRelayUrl> {
        if (monitorAuthors.isEmpty()) return emptySet()
        // Nothing asked about is nothing held out, and it is not a reason to
        // read the whole dead set: `among` being EMPTY is a caller saying it
        // has no urls, where null is a caller saying it wants all of them.
        if (among != null && among.isEmpty()) return emptySet()
        return verdicts(store, Verdict.DEAD, monitorAuthors, maxAgeSeconds, now, among)
            .mapNotNullTo(HashSet()) { urlOf(it, allowOnion) }
    }

    /**
     * Every url THIS ROUTER holds a current record about, whatever that record
     * says.
     *
     * Not a verdict read and not a gate: this is the answer to "how many relay
     * urls does this router know of", which is a wider question than "how many
     * did a relay list name this round" and used to have no answer at all. A url
     * leaves the streams' relay lists for reasons of its own — the author who
     * listed it revised their 10002, the source that carried it was
     * reconfigured — and the moment it does, the corpus the coverage card draws
     * silently loses it, while every measurement this router ever took of it is
     * still in the store. The card's own caption promises "every relay url this
     * router knows of"; this is what makes that true rather than aspirational.
     *
     * **[self] ALONE, and not [undialable]'s wider trust set.** That one takes
     * every monitor identity the operator vouched for, because a hold-out is a
     * decision an operator may delegate. This is not a decision at all, it is
     * the size of our own corpus — and unscoped it is somebody else's: a
     * deployment mirroring a busy foreign monitor's 30166s would draw that
     * monitor's whole world as the mouth of ITS coverage tree, shrinking every
     * bar under it to a sliver of a corpus this router has never touched. It is
     * also exactly the population [RelayVerdictRecord.loadAll] hands the fold,
     * so the card's mouth and the fold's world stay one corpus.
     *
     * Null [self] is a router with no signer: it holds no records, and the
     * honest count of what it knows beyond today's relay lists is none.
     *
     * Paged off the search index like every other read here, and NOT through
     * the store's tags projection it used to prefer. Materializing five figures
     * of records to read one `d` off each looks like the cost that projection
     * exists to remove, right up until you price the projection: it is a corpus
     * visit, and this filter — `authors = [us]` plus a `since` — is the
     * narrowest shape the index answers. See [discover] for the measurement.
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
     * One indexed query for "records carrying THIS grade in OUR vocabulary,
     * signed by these identities, re-checked since the floor" — [undialable]'s
     * whole read, and the only place the sync plane names a verdict value.
     *
     * The tag index answers on the label's VALUE, which is shared ground:
     * `["l", …]` on a 30166 also carries country codes, ISPs and ASNs from
     * other monitors, so the namespace is re-checked on what comes back. The
     * value `dead` is unlikely to collide, but "unlikely" is not the standard
     * for a read that can starve a relay out of every pass permanently.
     *
     * NOTHING PRIVATE IS READ HERE. There was a rules-epoch check on the tag's
     * fifth element, which meant every read enforced our own versioning scheme
     * and no standard NIP-66 record could ever satisfy it — the gate stayed
     * shut against foreign monitors however the config was written. A verdict
     * we no longer stand behind is ours to RETRACT, and
     * [FitnessPass.retireStaleEpochs] does exactly that at boot, so what
     * survives to be read is a claim its author still makes. The question left
     * here is the only one a reader should ask: does this url hold this
     * verdict, from someone we trust, recently enough.
     */
    private suspend fun verdicts(
        store: IEventStore,
        verdict: Verdict,
        monitorAuthors: List<String>,
        maxAgeSeconds: Long,
        now: Long,
        among: Collection<NormalizedRelayUrl>? = null,
    ): List<Event> {
        // THE SUBJECT-BOUND READ, for a caller asking about a known handful —
        // see [undialable]'s `among`. Chunked, because a filter carrying every
        // url of a wide ask is a query nobody has sized; the label tag comes
        // off, because `#d` is the bound that makes this small and the check
        // below is what reads a verdict either way.
        val queried =
            if (among != null) {
                among
                    .map { it.url }
                    .chunked(RelayVerdictRecord.QUERY_CHUNK)
                    .flatMap { chunk ->
                        store.query<Event>(
                            Filter(
                                kinds = listOf(RelayDiscoveryEvent.KIND),
                                authors = monitorAuthors.takeIf { it.isNotEmpty() },
                                tags = mapOf("d" to chunk),
                                since = now - maxAgeSeconds,
                            ),
                        )
                    }
            } else {
                store.query<Event>(
                    Filter(
                        kinds = listOf(RelayDiscoveryEvent.KIND),
                        // Absent, not empty: a NIP-01 filter with no `authors` key
                        // is the unscoped read. An EMPTY list would be a predicate
                        // nothing satisfies.
                        authors = monitorAuthors.takeIf { it.isNotEmpty() },
                        tags = mapOf(RelayVerdictRecord.LABEL_TAG to listOf(verdict.value)),
                        // The freshness bound, indexed — the record's own clock says
                        // when this monitor last re-checked the relay again, now
                        // that no passive writer bumps it on every socket.
                        since = now - maxAgeSeconds,
                    ),
                )
            }
        return queried.filter { event ->
            // OUR NAMESPACE'S label, not any label carrying this value.
            // `l` is shared — the same records carry country codes and ASNs
            // from other monitors — and the store's tag index answers on the
            // value alone, so the namespace check is what makes this read a
            // read of fitness grades rather than of every label in the store.
            val s =
                event.tags.firstOrNull {
                    it.size > RelayVerdictRecord.LABEL_NAMESPACE_INDEX &&
                        it[0] == RelayVerdictRecord.LABEL_TAG &&
                        it[RelayVerdictRecord.LABEL_NAMESPACE_INDEX] == RelayVerdictRecord.FITNESS_NAMESPACE
                }
            // Where the read IS scoped, the store's `authors` filter is the
            // trust boundary and this one string compare re-states it on the
            // returned events, so a query layer that ever treated `authors`
            // as a hint rather than a predicate cannot hand a stranger's
            // verdict through. Unscoped there is nothing to re-state.
            (monitorAuthors.isEmpty() || event.pubKey in monitorAuthors) &&
                s != null &&
                s[1] == verdict.value
        }
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
     * Walk everything [filter] matches, a page at a time, oldest-ward, so
     * memory is a page rather than the corpus (the store answers an unbounded
     * query with the whole match set in one list).
     *
     * A page boundary can fall inside a run of events sharing one
     * `created_at`. `until` is inclusive, so the next page re-sees them;
     * [scan]'s boundary set carries exactly that run forward to skip it. A
     * page that is entirely one timestamp grows until it spans two — the one
     * place a page may exceed [pageSize], and the only way not to lose the
     * run.
     *
     * Internal rather than private since [RelayVerdictRecord.loadAll] walks a
     * corpus too: an unbounded read of somebody's whole kind is the shape this
     * exists to page, and a second hand-rolled cursor beside it is a second
     * place for the boundary handling above to be got wrong.
     */
    suspend fun scan(
        store: IEventStore,
        filter: Filter,
        pageSize: Int,
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
            var page: List<Event> = store.query(filter.copy(until = until, limit = ask))
            if (page.isEmpty()) return

            // Results are newest-first, so a page whose ends share a
            // `created_at` is entirely one timestamp and the cursor has
            // nowhere to go. Grow the page until it spans two.
            while (page.size == ask && ask < budget && page.first().createdAt == page.last().createdAt) {
                // `budget` is a Long and an unbounded scan makes it
                // Int.MAX_VALUE + boundaryIds.size, so the doubling must be
                // clamped to Int range BEFORE narrowing: the wrap lands on a
                // negative `ask`, which the store reads as its matches-nothing
                // sentinel, and the walk then restarts from the top forever.
                ask = minOf(ask.toLong() * 2, budget).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
                page = store.query(filter.copy(until = until, limit = ask))
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
                // Still one timestamp even after growing — out of budget to
                // widen. Step below it.
                until = oldest - 1
                boundaryIds = emptySet()
            } else {
                until = oldest
                boundaryIds = newBoundary
            }
        }
    }

    /**
     * Events per page: one or two round trips for a normal relay-list scan,
     * and a bounded allocation.
     */
    private const val SCAN_PAGE = 10_000

    /**
     * Is this event too long to be a relay list?
     *
     * Counted per SELECT-matching tag rather than over the whole tag array, so
     * an event that is mostly something else — a NIP-51 set with hundreds of
     * `p` tags and four relays — is judged on the relays it names, which is the
     * only part this reads.
     *
     * Measured on this store: 9,418 pubkeys named a relay list of 6-20 entries
     * and 148 named one of 100-10,591, the largest carrying no other tag and no
     * content. A kind 10002 with ten thousand relays in it is not one user's
     * outbox; it is a pool for someone else to draw from, and every entry costs
     * this router a dial.
     *
     * Null disables it. An event that trips the cap is dropped WHOLE — no
     * prefix of it is read — because taking the first N would let the author
     * choose which relays we see by ordering them.
     */
    private fun oversized(
        event: Event,
        selects: List<RelaySelect>,
        cap: Int?,
    ): Boolean {
        if (cap == null) return false
        var seen = 0
        for (tag in event.tags) {
            // Counted once however many selects claim it — and only if one of
            // them would actually EXTRACT it. `where` is part of that test,
            // which it was not: a NIP-65 select carrying `marker = "write"`
            // counted the read-only `r` tags it then discards, so a 10002
            // listing 200 read relays and three write ones tripped a cap of 50
            // and lost all three. The cap exists to refuse a pool somebody is
            // handing us to dial; a tag we never dial is not part of one.
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
     * Every (url, bound values) pair one event yields for one select. The TAG
     * is the unit: each matching tag calls [onMatch] once with the url it
     * names and the values its own slots hold, so the pairing survives —
     * gathering the slots into separate sets would produce the cross product,
     * and the pairing cannot be recovered once it is gone.
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
            // With no tag name to go on, only take values that already say
            // they are a relay.
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
                // A tag that cannot fill a binding is dropped WHOLE rather
                // than half-applied: a `["30382:rank", relay]` missing its
                // service would otherwise widen the ask back to every author
                // on that relay — the opposite of what binding it was for.
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
     * Whether a value can be what the destination says it is. These come off
     * strangers' events, so a malformed one is expected traffic (skipped, not
     * fatal) — but an `authors` entry that is not a key would make a filter no
     * relay can answer.
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
     * Relay lists in the wild carry prose where a url belongs, and the
     * normalizer is forgiving by design — so anything blank or with
     * whitespace is dropped before it gets there. Two more classes are
     * dropped because dialling them cannot work: loopback/private hosts
     * (`ws://localhost` in someone else's relay list means THEIR machine),
     * and `.onion` unless [allowOnion] — a deployment with no Tor transport
     * has nothing that can resolve one, so every dial is a guaranteed
     * failure AND a hidden service name handed to the local resolver.
     *
     * MEMOIZED PER SPELLING in [RelayUrlCache], because a relay-list scan asks
     * this the same few thousand strings once per author. The rules and the
     * measurements that produced them moved there with it; this is the name
     * the extraction path calls them by.
     *
     * The rules, for the record, and each one bought with a live run:
     *  - `ws://` or `wss://`, ALWAYS. This used to be demanded only when a
     *    select named no tag, on the reasoning that a named tag makes a bare
     *    host safe to coerce. It does not: the normalizer is forgiving by
     *    design, so anything a relay-list author typed becomes a url and a
     *    dynamic stream then dials it every cycle. Measured on this store's
     *    kind-10002s — 1,749 wss, 103 ws, 2 with no scheme, 0 http/https — so
     *    demanding it costs 2 urls in 1,854 and buys out every http:// entry
     *    riding in on OTHER sources, which the 10040s do carry (a live one
     *    names http://localhost:7778).
     *  - The scheme checked AGAIN on what we will actually dial. The
     *    normalizer repairs as well as canonicalises: 143 urls in this corpus
     *    carry a NESTED scheme — `wss://https//nostr.watch/relay/x` — which
     *    passes the first test and comes out as an `https://` web page ABOUT a
     *    relay, dialled once a cycle forever, answering nothing. A diagnostic
     *    run caught us doing exactly that to `https://kbin.social/`,
     *    `https://nostr.watch/relays/find` and `//nos.lol/` — 116 live
     *    "relays" returning 0 events between them.
     *  - A redundant `:443` on `wss` / `:80` on `ws` stripped, shared with the
     *    exclude list's plain entries so the two meet on one spelling.
     */
    private fun normalize(
        raw: String,
        allowOnion: Boolean,
    ): NormalizedRelayUrl? = RelayUrlCache.Default.normalize(raw, allowOnion)
}
