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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.coroutines.CancellationException

/**
 * How much of a subscription's feed the expansion may be, and how much store
 * work it may cost: a hit that nominates thousands of subjects — a
 * 2,000-member Trusted List is a normal one — must not turn a five-hit search
 * page into a five-thousand-frame flood, and a page of five such lists must
 * not turn one REQ into ten thousand key lookups.
 *
 * Both caps bound what is LOOKED UP, not what is found, and they truncate in
 * the pointer's OWN ORDER — the first N members it names. That is the
 * deterministic reading, and it is the useful one: a publisher orders a
 * Trusted List's members by the score it computed for them, so the first N are
 * the N it ranks highest. The alternative — "the first N we happen to hold" —
 * would make the answer depend on what the mirror had caught up on, and would
 * let a run of members this relay does not have cost a lookup each anyway.
 * Members past the cap are not looked up at all, which is the point.
 *
 * Both are TRUNCATIONS, not refusals: the pointer itself is served either way,
 * so a client that wants the whole membership reads the member tags and asks
 * for them by `#p` / `#e` / `#a` recall, which is what that recall is for and
 * what it already served before this feature existed.
 *
 * [enabled] false is the relay this repo shipped before the expansion: a
 * search answers with exactly its own hits.
 */
data class SearchExpansionLimits(
    val enabled: Boolean = true,
    /** Subjects looked up per referencing hit — the first this many it names. */
    val maxPerEvent: Int = 100,
    /** Subjects looked up across a whole REQ, however many hits nominate them. */
    val maxPerRequest: Int = 1_000,
) {
    companion object {
        val Default = SearchExpansionLimits()

        val Off = SearchExpansionLimits(enabled = false)
    }
}

/**
 * THE SUBJECT TRAVELS WITH THE POINTER. A NIP-50 search that matches a Trusted
 * List, a NIP-85 Trusted Assertion or a NIP-32 label answers with the record
 * that pointer is ABOUT as well as the pointer itself, spliced into the feed
 * immediately behind it — same rank, same page, no second round trip.
 *
 * WHY IT HAS TO BE THE RELAY THAT DOES THIS. The three families in
 * [SearchReferences] carry text that is about something else: a list's title,
 * an assertion's petname, a label's value. The record on the other end holds
 * none of that text, so no amount of ranking will ever recall it from the same
 * search — "podcaster" finds the Podcaster Trust List and cannot find a single
 * podcaster. A client can of course read the member tags and ask again, and
 * that is exactly the round trip (and the lost ranking context) this removes.
 *
 * ## The admission rule: every filter but its search
 *
 * A subject is added only when it matches at least one of the REQ's own
 * filters with the NIP-50 `search` field taken out of the test — Quartz's
 * [Filter.match] is precisely that predicate, ignoring `search` and `limit`
 * and enforcing everything else (`ids`, `authors`, `kinds`, `#tags`, `since`,
 * `until`). The REQ's filters are ORed as NIP-01 says, so matching any one of
 * them admits the subject.
 *
 * That rule has a consequence worth stating plainly, because it looks like a
 * bug from the outside: `{"kinds":[1985],"search":"nsfw"}` gets back labels
 * and NOTHING ELSE, because a kind-1 note cannot match `kinds:[1985]`. This is
 * the rule working. A REQ's filter is the client's statement of what it is
 * prepared to receive, and a relay that answered a kinds-constrained
 * subscription with other kinds would be lying about its own protocol. A
 * client that wants the subjects asks for their kinds too —
 * `{"kinds":[0,1,1985],"search":"nsfw"}`.
 *
 * ## A list or an assertion expands only for the reader who ENROLLED it
 *
 * A Trusted List and a Trusted Assertion are a trust provider's computed
 * output, and NIP-85 says how a reader chooses providers: they publish a
 * kind-10040 naming them. So those two families expand only when the hit is
 * signed by one of THIS read's observer's services, or by the observer
 * themselves. A list from a service nobody named is a stranger's computation,
 * and splicing its members into a feed would put it in front of a reader as
 * if they had asked for it.
 *
 * The consequence, and it is the intended one: a read with NO observer — an
 * anonymous `include:spam`, which is a legitimate ask here — gets no list or
 * assertion expansion at all, because there is nobody whose services to check.
 * Its hits are still served; only the splice is withheld. NIP-32 labels are
 * not gated this way (see [SearchReferences.DECLARATIONS] for why).
 *
 * The 10040 lookup is the one recall here that is deliberately UNGATED
 * (`include:spam`). Reading a reader's own statement of whom they trust
 * through the trust that statement establishes is circular, and it fails in
 * the worst direction: a reader whose provider has not scored the reader
 * personally would silently lose the whole feature. Being lensless is also
 * what lets [EnrolledSigners] cache it across connections, which is what keeps
 * this from being a second store round trip on every expanded search.
 *
 * ## The lens travels with it
 *
 * The subject lookup is answered through the SAME lens as the search that
 * produced it: [lensOf] carries the originating filter's `include:spam`,
 * `observer:` and `filter:rank:` tokens onto the lookup filters, and the
 * lookup runs inside the [ObserverBackend] read's `StoreQueryContext`. This
 * store treats a web-of-trust lens as a FILTER (see [ObserverBackend]), so
 * without that the expansion would be a hole in the gate on an `observer:`
 * read and a needlessly EMPTY answer on an `include:spam` one — the two
 * mistakes are in opposite directions and one carries them both.
 *
 * ## Two things it deliberately does not answer for
 *
 * **`limit` bounds the HITS, not the frames.** A `limit:10` search still gets
 * ten hits; the subjects ride in over and above them, capped by
 * [SearchExpansionLimits] instead. Counting a subject against the limit would
 * mean a page of ten labels served nine labels and one note, which is the
 * client's question answered less well rather than more — the subject is an
 * attachment to a hit, not a competitor for its slot.
 *
 * **A NIP-45 `COUNT` counts the hits.** The store's contract is that a COUNT
 * is exactly the number a client could verify by running the REQ and counting
 * what arrives (`NostrSemanticsStore.count`), and for a search this now
 * under-reports by the splice. That is the honest trade: an exact count would
 * have to resolve every subject of the whole match set — not the page, the
 * MATCH SET — which is the one thing a count exists to avoid. A COUNT here
 * answers "how many hits", and the REQ that follows it answers with those hits
 * and what they are about.
 *
 * ## What it does not do
 *
 * Only the STORED replay expands. A label that arrives live on an open
 * subscription is delivered as-is: the live fanout runs on the ingest queue's
 * drain coroutine, where a store lookup would stall the batch writer for every
 * other subscriber. It also falls out of WHERE this runs rather than having to
 * be arranged: [ExpandingEventStore] wraps the store, and a live event never
 * touches the store on its way out — `LiveEventStore` fans it out through its
 * own filter index. Live delivery is a tail, not a page, and the ranked page a
 * client reads is the replay.
 */
internal class SearchReferenceExpansion(
    /** The REQ's filters, verbatim: both the admission test and the lens come from these. */
    private val filters: List<Filter>,
    /**
     * The subset of [filters] that actually searches ([searching]). A pointer
     * expands only if it matches one of THESE — a subscription's other half is
     * a plain recall, and a plain recall is answered exactly as it was before
     * this feature existed.
     */
    private val searchingFilters: List<Filter>,
    /**
     * The NIP-42 pubkey of the connection, if any. A searching filter's own
     * `observer:` wins over it, per filter — the store's precedence, mirrored.
     */
    private val connectionObserver: HexKey?,
    /**
     * Who may cause an expansion for those observers. Shared across the whole
     * relay and cached there, because it is a property of the READER rather
     * than of this page — see [EnrolledSigners].
     */
    private val enrolment: EnrolledSigners,
    private val limits: SearchExpansionLimits,
    /**
     * The store recall the lookups run through — [ObserverBackend] hands in
     * the store's own multi-filter query, which is concurrent and deduped.
     * It inherits this read's `StoreQueryContext`, so an authenticated
     * connection's lens applies here exactly as it does to the search.
     */
    private val recall: suspend (List<Filter>) -> List<Event>,
) {
    /**
     * Ids this subscription has already put on the wire — the hits it echoed
     * and the subjects it spliced. NIP-01 asks a relay not to send the same
     * event twice on one subscription, and a subject is very often ALSO a hit
     * further down the same page (a label and the note it labels both matching
     * "bitcoin"), so without this the duplicate is the common case rather than
     * the corner one. Bounded by the REQ's own `limit` plus
     * [SearchExpansionLimits.maxPerRequest], and freed with the subscription.
     */
    private val sent = HashSet<String>()

    private var budget = limits.maxPerRequest

    /**
     * ONE WAY THIS SUBSCRIPTION IS READ: the NIP-50 tokens a subject lookup
     * must carry, and whose enrolment gates a Trusted List or Assertion found
     * through it.
     *
     * A subscription can carry several, because NIP-01 ORs its filters and
     * each may declare its own lens. That is not a corner case to wave away —
     * it is a HOLE if it is: a filter saying `include:spam` beside one saying
     * `observer:X` would otherwise let the whole page's subjects be recalled
     * with the trust floor off, and a search that asked to be ranked through X
     * would come back carrying records X's web of trust excludes. Every lookup
     * is therefore made under the lens of the searching filter that matched
     * the pointer, and never under a sibling's.
     */
    private class Lens(
        /** What rides on the lookup filters' `search`; null is a plain recall. */
        val tokens: String?,
        /** Whose 10040 admits a declaration found through this lens. */
        val observers: Set<HexKey>,
    )

    /** The distinct lenses this REQ declares, over its SEARCHING filters only. */
    private val lenses: List<Lens>

    /** `searchingFilters[i]` is read through `lenses[lensOfFilter[i]]`. */
    private val lensOfFilter: IntArray

    /** Memoized [enrolment] per lens; a page of labels never fills one in. */
    private val enrolled: Array<EnrolledSigners.Enrolment?>

    init {
        val perFilter =
            searchingFilters.map { filter ->
                Lens(lensOf(filter.search), setOfNotNull(filter.observerLens() ?: connectionObserver))
            }
        val distinct = ArrayList<Lens>()
        lensOfFilter =
            IntArray(perFilter.size) { i ->
                val already = distinct.indexOfFirst { it.tokens == perFilter[i].tokens && it.observers == perFilter[i].observers }
                if (already >= 0) {
                    already
                } else {
                    distinct.add(perFilter[i])
                    distinct.size - 1
                }
            }
        lenses = distinct
        enrolled = arrayOfNulls(lenses.size)
    }

    /**
     * Whether any of the REQ's filters would accept a kind-0 profile at all.
     * Read ONCE and applied in [plan] rather than after the lookup: a pubkey
     * subject a REQ can never be answered with must not spend the request
     * budget that an `e` or an `a` subject further down the page needs.
     */
    private val admitsProfiles = filters.any { it.kinds?.contains(MetadataEvent.KIND) != false }

    /**
     * The subjects of each row in [rows], index-aligned with it and already
     * admitted by [Filter.match]. The caller emits `rows[i]` and then
     * `result[i]`, and that is the whole protocol.
     *
     * [pointerOf] hands a row back as an [Event] only when its KIND nominates
     * something, which is how the zero-decode path keeps its splice for every
     * other row; [idOf] is the row's id, which this needs to keep a hit that is
     * also a subject at its own rank.
     *
     * ONE ROUND TRIP PER PAGE, not one per row: every row's pointers are
     * gathered first and recalled together, so a page of 500 labels costs the
     * three lookup shapes below rather than 500 of them. That is also why this
     * takes the whole page rather than being a per-event call.
     */
    suspend fun <T> expand(
        rows: List<T>,
        idOf: (T) -> String,
        pointerOf: (T) -> Event?,
    ): Expanded {
        // Recorded BEFORE anything is planned, and that is what keeps each row
        // at its own ranked position: a subject that is also a hit further down
        // the page must be served where the SEARCH put it, not spliced in early
        // behind whatever pointer happens to name it.
        //
        // `add` answering false is the OTHER half of the same set: the row's id
        // has already gone out, so the row must not. That cannot happen from
        // the store — `recallOrdered` dedups by id across a REQ's filters — but
        // this class is the one ADDING events to a page, and a page it makes
        // must be free of duplicates because of what it does rather than
        // because of what its store promises. NIP-01 asks a relay not to send
        // one event twice on a subscription, and on this corpus the near-miss
        // is routine: 76 targets in a production sample are named by more than
        // one label, ten of them by ten labels each.
        val fresh = BooleanArray(rows.size) { i -> sent.add(idOf(rows[i])) }
        val nothing = Expanded(fresh, rows.map { emptyList() })
        if (budget <= 0 || lenses.isEmpty()) return nothing

        // ONE PASS over the batch, and it stops materializing the moment the
        // request budget is spent. Reading a row's pointers costs a tags parse
        // and an `EventFactory` dispatch on the zero-decode path, and a page of
        // 500 lists exhausts the default 1,000-subject budget on the first
        // fifty of them — so a separate read-them-all-then-plan pass would pay
        // 450 parses for rows it had already decided to take nothing from.
        var any = false
        val planned = ArrayList<References>(rows.size)
        val lensOfRow = IntArray(rows.size) { NO_LENS }
        for ((i, row) in rows.withIndex()) {
            val pointer = if (budget > 0) pointerOf(row)?.takeIf { it.kind in SearchReferences.KINDS } else null

            // WHICH SEARCH FOUND IT, and so which lens its subjects are read
            // through. A subscription ORs its filters and the store answers
            // with one union, so a row cannot say which filter fetched it — but
            // it can say which would ACCEPT it, and `Filter.match` is that
            // question with the search text left out. A row no searching filter
            // accepts came from the plain half of a mixed REQ and is served
            // exactly as it always was.
            val matched = if (pointer == null) -1 else searchingFilters.indexOfFirst { it.match(pointer) }
            val lens = if (matched < 0) NO_LENS else lensOfFilter[matched]

            val refs =
                when {
                    pointer == null || matched < 0 -> References.NONE

                    // Resolved on the first list or assertion this lens sees
                    // and memoized after: a page of labels never asks for it.
                    SearchReferences.isDeclaration(pointer.kind) && !enrolment(lens).admits(pointer.kind, pointer.pubKey) -> References.NONE

                    else -> plan(SearchReferences.of(pointer))
                }
            if (!refs.isEmpty()) {
                any = true
                lensOfRow[i] = lens
            }
            planned.add(refs)
        }
        if (!any) return nothing

        val found = lookUp(planned, lensOfRow)
        if (found.all(Found::isEmpty)) return nothing

        return Expanded(
            fresh,
            planned.mapIndexed { i, refs -> if (lensOfRow[i] == NO_LENS) emptyList() else admit(refs, found[lensOfRow[i]]) },
        )
    }

    /**
     * What a page becomes: which of its rows may still go out, and what rides
     * in behind each. Index-aligned with the rows the caller handed in.
     */
    class Expanded(
        /** False for a row whose id this subscription has already sent. */
        val fresh: BooleanArray,
        val subjects: List<List<Event>>,
    )

    /**
     * What this row may bring, under both caps, in the order it named them.
     * Spends the request budget on what it PLANS rather than on what is later
     * found — see [SearchExpansionLimits] for why the caps are on the lookup.
     */
    private fun plan(raw: References): References {
        // Dropped BEFORE the budget is charged, not after the lookup: a
        // 2,000-member list of pubkeys under a REQ that admits no kind 0 would
        // otherwise spend the whole request budget on subjects that can never
        // be served, and starve an `e` or `a` subject further down the page.
        val refs = if (admitsProfiles || raw.pubKeys.isEmpty()) raw else References(raw.eventIds, emptyList(), raw.addresses)
        val room = minOf(limits.maxPerEvent, budget)
        if (room <= 0 || refs.isEmpty()) return References.NONE
        if (refs.size <= room) {
            budget -= refs.size
            return refs
        }
        // In practice only one of the three is ever non-empty on a Trusted
        // List, and a label names one or two things in total, so this order
        // decides nothing that a real pointer would notice.
        val ids = refs.eventIds.take(room)
        val keys = refs.pubKeys.take(room - ids.size)
        budget -= room
        return References(ids, keys, refs.addresses.take(room - ids.size - keys.size))
    }

    /**
     * This row's planned subjects, as far as the store actually holds them,
     * minus anything this subscription has already sent — a subject is very
     * often ALSO a hit of the same search, and NIP-01 asks a relay not to send
     * one event twice on one subscription.
     */
    private fun admit(
        refs: References,
        found: Found,
    ): List<Event> {
        if (refs.isEmpty()) return emptyList()
        val out = ArrayList<Event>(refs.size)
        refs.eventIds.forEach { key -> found.byId[key]?.let { if (sent.add(it.id)) out.add(it) } }
        refs.pubKeys.forEach { key -> found.profiles[key]?.let { if (sent.add(it.id)) out.add(it) } }
        refs.addresses.forEach { key -> found.byAddress[key]?.let { if (sent.add(it.id)) out.add(it) } }
        return out
    }

    /** The recalled subjects, keyed the three ways a pointer names one. */
    private class Found(
        val byId: Map<HexKey, Event>,
        val profiles: Map<HexKey, Event>,
        val byAddress: Map<String, Event>,
    ) {
        fun isEmpty() = byId.isEmpty() && profiles.isEmpty() && byAddress.isEmpty()
    }

    /**
     * The subjects, looked up ONCE PER LENS and never across one.
     *
     * A single lens — the overwhelmingly common shape — is a single store call
     * for the whole page: the store takes a LIST of filters and recalls them
     * concurrently under its own fan-out bound, deduping across them
     * (`NostrSemanticsStore.recallOrdered`). Handing them over one at a time
     * would serialize what the engine is built to parallelize.
     *
     * A REQ that declares TWO lenses costs two, and that is the honest price
     * rather than an oversight: the results cannot be pooled, because a subject
     * recalled with the trust floor off must not be handed to a pointer whose
     * search asked to be ranked through somebody's web of trust. One call per
     * lens is what keeps that separation, and a client only pays it by asking
     * two different questions in one subscription.
     */
    private suspend fun lookUp(
        planned: List<References>,
        lensOfRow: IntArray,
    ): List<Found> {
        val ids = List(lenses.size) { LinkedHashSet<HexKey>() }
        val keys = List(lenses.size) { LinkedHashSet<HexKey>() }
        val addresses = List(lenses.size) { LinkedHashSet<String>() }
        for ((i, refs) in planned.withIndex()) {
            val lens = lensOfRow[i]
            if (lens == NO_LENS) continue
            ids[lens].addAll(refs.eventIds)
            keys[lens].addAll(refs.pubKeys)
            addresses[lens].addAll(refs.addresses)
        }

        return lenses.indices.map { lens -> lookUpUnder(lenses[lens].tokens, ids[lens], keys[lens], addresses[lens]) }
    }

    private suspend fun lookUpUnder(
        tokens: String?,
        ids: Set<HexKey>,
        keys: Set<HexKey>,
        addresses: Set<String>,
    ): Found {
        val lookups = ArrayList<Filter>()
        ids.inChunks { lookups.add(Filter(ids = it, limit = it.size, search = tokens)) }
        keys.inChunks { lookups.add(Filter(kinds = METADATA, authors = it, limit = it.size, search = tokens)) }
        // An a-coordinate is (kind, author, `d`), so coordinates sharing a kind
        // and an author recall as one exact filter. Exact rather than one
        // filter per kind with every author and every `d` in it: that shape
        // recalls the CROSS PRODUCT, and two authors who both publish a `d` of
        // "index" would drag in each other's. The price is a filter per owner,
        // which is why the fan-out is capped — a page whose pointers name more
        // than [MAX_LOOKUPS] owners expands the first of them.
        addresses
            .mapNotNull(Address::parse)
            .groupBy { Owner(it.kind, it.pubKeyHex) }
            .forEach { (owner, coords) ->
                coords.map(Address::dTag).distinct().inChunks { ds ->
                    lookups.add(
                        Filter(
                            kinds = listOf(owner.kind),
                            authors = listOf(owner.pubKey),
                            tags = mapOf("d" to ds),
                            limit = ds.size,
                            search = tokens,
                        ),
                    )
                }
            }
        if (lookups.isEmpty()) return EMPTY

        val byId = HashMap<HexKey, Event>()
        val profiles = HashMap<HexKey, Event>()
        val byAddress = HashMap<String, Event>()
        for (candidate in recallOrEmpty(lookups.take(MAX_LOOKUPS))) {
            // The admission rule, and the only place it is applied: every
            // filter of the REQ except its search.
            if (filters.none { it.match(candidate) }) continue
            if (candidate.id in ids) byId[candidate.id] = candidate
            if (candidate.kind == MetadataEvent.KIND && candidate.pubKey in keys) profiles[candidate.pubKey] = candidate
            if (addresses.isNotEmpty()) {
                // Both sides are the CANONICAL `kind:lowercase-hex:d`, which is
                // what [SearchReferences] normalizes a pointer's `a` value to.
                // Comparing against the raw tag string instead would fetch an
                // `naddr1…` or upper-case member and then silently drop it.
                val coord = Address.assemble(candidate.kind, candidate.pubKey, candidate.tags.dTag())
                if (coord in addresses) byAddress[coord] = candidate
            }
        }
        return Found(byId, profiles, byAddress)
    }

    /** The (kind, author) half of an a-coordinate — what one addressable lookup filter is keyed on. */
    private data class Owner(
        val kind: Int,
        val pubKey: HexKey,
    )

    /**
     * WHICH SIGNERS THIS READ ASKED FOR, PER KIND. Memoized per REQ over a
     * cache that is memoized per reader, so a page of 500 lists asks once and a
     * session of many searches usually asks not at all — [EnrolledSigners]
     * carries the staleness argument and the per-kind reasoning.
     *
     * EVERY service a 10040 names counts, not just `30382:rank` — a Trusted
     * List of events is published by a `30383:` service and a list of addresses
     * by a `30384:` one, so filtering to the ranking service (the right
     * question for [TrustNotice], where the subject IS ranking) would drop
     * three quarters of the family. What the entries do NOT do is vouch for
     * each other: the row below asks with the kind it is about to unpack, so a
     * `30382:rank` delegation opens 30382 and leaves the other seven shut.
     *
     * The private half of a 10040 (NIP-44 in `content`) cannot be read by a
     * relay and so names nothing: a reader whose providers are all private gets
     * no list expansion, the same answer the store's own provider map gives for
     * the same reason.
     */
    private suspend fun enrolment(lens: Int): EnrolledSigners.Enrolment = enrolled[lens] ?: enrolment.of(lenses[lens].observers).also { enrolled[lens] = it }

    /**
     * A recall whose failure costs the splice and nothing else. An expansion
     * that throws would take down a REQ that had already answered — the hits
     * are resolved and waiting — so a store that cannot answer the second
     * question degrades to the relay without this feature rather than to a
     * `CLOSED`.
     */
    private suspend fun recallOrEmpty(lookups: List<Filter>): List<Event> =
        try {
            recall(lookups)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // One line: a store that has stopped answering produces one of
            // these per search, and the search itself still went out.
            println("search-expansion: subject lookup failed, serving hits only: ${e.message}")
            emptyList()
        }

    private fun <T> Collection<T>.inChunks(block: (List<T>) -> Unit) {
        if (isEmpty()) return
        toList().chunked(LOOKUP_CHUNK).forEach(block)
    }

    companion object {
        private val METADATA = listOf(MetadataEvent.KIND)

        /** [lensOfRow] for a row no searching filter accepts — it expands nothing. */
        private const val NO_LENS = -1

        private val EMPTY = Found(emptyMap(), emptyMap(), emptyMap())

        /** Ids per lookup filter. Each key matches at most one event, so the filter's `limit` is exact. */
        private const val LOOKUP_CHUNK = 500

        /**
         * Filters one batch's lookup may fan out into. The id and profile
         * shapes need two apiece at the request budget's default, so this is
         * effectively a ceiling on distinct a-coordinate OWNERS — and one
         * pointer naming coordinates from sixty different authors is already
         * far outside anything the three families publish.
         */
        private const val MAX_LOOKUPS = 64

        /**
         * The NIP-50 tokens that decide WHICH CORPUS a read sees, and the only
         * ones carried onto a subject lookup. `include:spam` turns the trust
         * floor off, `observer:` names the web of trust that ranks and gates,
         * `filter:rank:` raises the floor — all three are in the store's
         * `FilterMapping`. Everything else is dropped on purpose: the terms
         * are what the SUBJECT provably does not contain (that is the whole
         * reason it needs fetching), and `sort:` orders a result set the
         * splice does not order — a subject sits where its pointer sits.
         *
         * Null means no tokens at all, which is a plain recall — and for an
         * authenticated read that is the same lens and the same default floor
         * a termless search would have taken.
         */
        fun lensOf(search: String?): String? {
            if (search.isNullOrEmpty()) return null
            val parsed = SearchQuery.parse(search)
            return parsed
                .extensions
                .filterKeys { it in LENS_KEYS }
                .map { (key, value) -> "$key:$value" }
                .ifEmpty { return null }
                .joinToString(" ")
        }

        private val LENS_KEYS = setOf("include", "observer", "filter")

        /**
         * Whether any hit of the SEARCHING filters could be a pointer at all,
         * decided from the filters alone and before a single row is read.
         *
         * Collecting a page before writing it out is not free —
         * `SearchExpansionCostBench` prices it at ~0.06ms per REQ on the
         * in-memory index, and invisible against a real engine — and most
         * searches cannot possibly need it: a client looking for notes or
         * profiles names
         * `kinds` that hold no pointer, so no row it can be answered with will
         * ever nominate anything. Those REQs take the plain delegating path and
         * pay nothing at all.
         *
         * A filter naming NO kinds admits every kind, pointer kinds included,
         * and so keeps the expansion. This is the same reading of `kinds` that
         * [Filter.match] uses — absent is no constraint, present-and-empty
         * matches nothing — and it has to be, or a REQ would be gated one way
         * here and admitted another way there.
         */
        fun couldPoint(filters: List<Filter>): Boolean = filters.any { filter -> filter.kinds?.any { it in SearchReferences.KINDS } != false }

        /**
         * The filters of this REQ that actually SEARCH: free-text terms or a
         * quoted phrase. Everything the expansion does is driven by these and
         * by nothing else, so a REQ with none of them is left alone entirely.
         *
         * NOT "carries a `search` field", and the difference is the whole gate.
         * With `REQUIRE_READ_LENS` on, EVERY anonymous read has to carry one:
         * the web page's own `shared/lens.js` stamps `include:spam` onto a
         * dozen plain reference reads — names, faces, scores, reply parents —
         * and a mirror's paging and a NIP-77 catch-up carry it too. Gating on
         * non-empty would put every one of those behind the expansion, which is
         * exactly the traffic that must not pay for it.
         *
         * Exclusions alone (`-spam`) do not count either: the store reads those
         * as plain recall minus the words, and so does this.
         *
         * Leaving a termless read alone costs nothing in ANSWERS, only in work:
         * a termless recall already matches the very predicate the admission
         * rule uses, so there is nothing an expansion could add to one that the
         * recall did not already return.
         */
        fun searching(filters: List<Filter>): List<Filter> =
            filters.filter { filter ->
                filter.search?.takeIf { it.isNotEmpty() }?.let { SearchQuery.parse(it).hasText } == true
            }
    }
}
