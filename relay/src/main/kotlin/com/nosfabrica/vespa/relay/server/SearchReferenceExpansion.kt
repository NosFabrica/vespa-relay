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
 * other subscriber (see [ObserverBackend]'s gate). Live delivery is a tail,
 * not a page — the ranked page a client reads is the replay.
 */
internal class SearchReferenceExpansion(
    /** The REQ's filters, verbatim: both the admission test and the lens come from these. */
    private val filters: List<Filter>,
    /**
     * Whose web of trust this read is answered through — the NIP-42 connection
     * or the filters' `observer:`, per filter, unioned. Empty is an anonymous
     * `include:spam` read, and it means no list or assertion expands.
     */
    private val observers: Set<HexKey>,
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

    /** Memoized [enrolledSigners]; null until the first list or assertion on the page asks. */
    private var enrolled: Set<HexKey>? = null

    /**
     * The subjects of each row in [rows], index-aligned with it and already
     * admitted by [Filter.match].
     *
     * [pointerOf] is the caller's reader: it hands back the row as an [Event]
     * only when the row's KIND nominates something, which is how the
     * zero-decode path keeps its splice for every other row.
     *
     * ONE ROUND TRIP PER BATCH, not one per row: every row's pointers are
     * gathered first and recalled together, so a page of 500 labels costs the
     * three lookup shapes below rather than 500 of them. That is also why this
     * takes the whole batch instead of being a per-event call.
     */
    suspend fun <T> expand(
        rows: List<T>,
        pointerOf: (T) -> Event?,
    ): List<List<Event>> {
        val nothing = rows.map { emptyList<Event>() }
        if (budget <= 0) return nothing

        // ONE PASS over the batch, and it stops materializing the moment the
        // request budget is spent. Reading a row's pointers costs a tags parse
        // and an `EventFactory` dispatch on the zero-decode path, and a page of
        // 500 lists exhausts the default 1,000-subject budget on the first
        // fifty of them — so a separate read-them-all-then-plan pass would pay
        // 450 parses for rows it had already decided to take nothing from.
        var any = false
        val planned = ArrayList<References>(rows.size)
        for (row in rows) {
            val pointer = if (budget > 0) pointerOf(row)?.takeIf { it.kind in SearchReferences.KINDS } else null
            val refs =
                when {
                    pointer == null -> References.NONE

                    // Resolved on the first list or assertion of the whole REQ
                    // and memoized after: a page of labels never asks for it.
                    SearchReferences.isDeclaration(pointer.kind) && pointer.pubKey !in enrolledSigners() -> References.NONE

                    else -> plan(SearchReferences.of(pointer))
                }
            any = any || !refs.isEmpty()
            planned.add(refs)
        }
        if (!any) return nothing

        val found = lookUp(planned)
        if (found.isEmpty()) return nothing

        return planned.map { admit(it, found) }
    }

    /**
     * Marks [id] as gone out on this subscription and answers whether that is
     * the FIRST time — exactly the question the caller has about a replay row.
     * A row already spliced as an earlier pointer's subject must not be sent
     * again, and neither must two copies of one id inside one batch, which the
     * replay and a concurrent live delivery can both carry.
     *
     * Recording rows BEFORE their subjects are resolved is what keeps a row at
     * its own ranked position: a pointer earlier on the same page cannot
     * splice it in ahead of where the search put it.
     */
    fun record(id: String): Boolean = sent.add(id)

    /**
     * What this row may bring, under both caps, in the order it named them.
     * Spends the request budget on what it PLANS rather than on what is later
     * found — see [SearchExpansionLimits] for why the caps are on the lookup.
     */
    private fun plan(refs: References): References {
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
     * ONE store call for the whole batch, and one on purpose: the store takes
     * a LIST of filters and recalls them concurrently under its own fan-out
     * bound, deduping across them (`NostrSemanticsStore.recallOrdered`).
     * Handing them over one at a time would serialize what the engine is built
     * to parallelize, which is the difference between a page of labels costing
     * one round trip and costing one per label.
     */
    private suspend fun lookUp(planned: List<References>): Found {
        val ids = LinkedHashSet<HexKey>()
        val keys = LinkedHashSet<HexKey>()
        val addresses = LinkedHashSet<String>()
        for (refs in planned) {
            ids.addAll(refs.eventIds)
            keys.addAll(refs.pubKeys)
            addresses.addAll(refs.addresses)
        }

        // A REQ that admits no kind-0 cannot be answered with a profile, so
        // the profile lookup is skipped rather than run and then discarded by
        // `match` — the pubkey side is the bulk of a Trusted List's membership
        // and this is the one shape whose kind is known before the recall.
        if (!admitsProfiles()) keys.clear()

        val lookups = ArrayList<Filter>()
        for (lens in lenses()) {
            ids.inChunks { lookups.add(Filter(ids = it, limit = it.size, search = lens)) }
            keys.inChunks { lookups.add(Filter(kinds = METADATA, authors = it, limit = it.size, search = lens)) }
            // An a-coordinate is (kind, author, `d`), so coordinates sharing a
            // kind and an author recall as one exact filter. Exact rather than
            // one filter per kind with every author and every `d` in it: that
            // shape recalls the CROSS PRODUCT, and two authors who both
            // publish a `d` of "index" would drag in each other's. The price
            // is a filter per owner, which is why the fan-out is capped —
            // a page whose pointers name more than [MAX_LOOKUPS] owners
            // expands the first of them.
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
                                search = lens,
                            ),
                        )
                    }
                }
            if (lookups.size >= MAX_LOOKUPS) break
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

    /** Whether any of the REQ's filters would accept a kind-0 profile at all. */
    private fun admitsProfiles() = filters.any { it.kinds?.contains(MetadataEvent.KIND) != false }

    /**
     * The distinct lens declarations among the REQ's filters — normally one.
     * Each is run as its own set of lookups because two filters may be read
     * through two different observers, and answering both through whichever
     * one happened to come first would serve one client's question through
     * another's eyes.
     */
    private fun lenses(): List<String?> = filters.map { lensOf(it.search) }.distinct()

    /**
     * The pubkeys whose Trusted Lists and Trusted Assertions this read has
     * asked for. Memoized per REQ over a cache that is memoized per reader, so
     * a page of 500 lists asks once and a session of many searches usually asks
     * not at all — [EnrolledSigners] carries the staleness argument.
     *
     * EVERY service a 10040 names counts, not just `30382:rank`. A Trusted List
     * of events is published by a `30383:` service and a list of addresses by a
     * `30384:` one, so filtering to the ranking service — which is the right
     * question for [TrustNotice], where the subject IS ranking — would silently
     * drop three quarters of the family here.
     *
     * The private half of a 10040 (NIP-44 in `content`) cannot be read by a
     * relay and so names nothing: a reader whose providers are all private gets
     * no list expansion, the same answer the store's own provider map gives for
     * the same reason.
     */
    private suspend fun enrolledSigners(): Set<HexKey> = enrolled ?: enrolment.of(observers).also { enrolled = it }

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
         * Whose eyes this read is answered through, per filter and unioned.
         *
         * The store's rule, mirrored here so the gate cannot come to a
         * different reading of a REQ than the query planner does: a filter's
         * own `observer:` WINS over the connection's NIP-42 identity
         * (`NostrSemanticsStore.toExpiryQuery` is `it.observer ?: observer`),
         * and the connection's applies to every filter that names none. A
         * signed-in reader who deliberately reads through somebody else's lens
         * therefore gets that somebody's providers on that filter, not their
         * own — which is the whole point of asking through another lens.
         *
         * The token is read by [observerLens], the same acceptance test
         * [LensRequiredPolicy] gates on, so a REQ cannot be understood one way
         * by the gate and another way here.
         */
        fun observersOf(
            filters: List<Filter>,
            connection: HexKey?,
        ): Set<HexKey> = filters.mapNotNullTo(LinkedHashSet()) { it.observerLens() ?: connection }

        /**
         * Whether any hit of this REQ could be a pointer at all, decided from
         * the filters alone and before a single row is read.
         *
         * The buffered path is not free — `SearchExpansionCostBench` prices it
         * at a flat ~0.1ms per REQ on the in-memory index — and most searches
         * cannot possibly need it: a client looking for notes or profiles names
         * `kinds` that hold no pointer, so no row it can be answered with will
         * ever nominate anything. Those REQs take the untouched delegating path
         * and pay nothing.
         *
         * A filter naming NO kinds admits every kind, pointer kinds included,
         * and so keeps the expansion. This is the same reading of `kinds` that
         * [Filter.match] uses — absent is no constraint, present-and-empty
         * matches nothing — and it has to be, or a REQ would be gated one way
         * here and admitted another way there.
         */
        fun couldPoint(filters: List<Filter>): Boolean = filters.any { filter -> filter.kinds?.any { it in SearchReferences.KINDS } != false }

        /**
         * Whether this REQ is a SEARCH — free-text terms or a quoted phrase on
         * any filter — and therefore something to expand.
         *
         * Not "carries a `search` field": with `REQUIRE_READ_LENS` on, every
         * anonymous read carries one, so that test would put the expansion in
         * front of a mirror's whole-corpus paging and a NIP-77 catch-up, which
         * nominate nothing and would pay the buffering for it. Exclusions
         * alone (`-spam`) do not count either — the store reads those as plain
         * recall minus the words, and so does this.
         */
        fun isSearch(filters: List<Filter>): Boolean =
            filters.any { filter ->
                filter.search?.takeIf { it.isNotEmpty() }?.let { SearchQuery.parse(it).hasText } == true
            }
    }
}
