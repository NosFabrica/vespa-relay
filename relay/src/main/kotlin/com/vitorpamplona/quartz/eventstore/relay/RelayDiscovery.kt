/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore

/** One relay a [DynamicRelayList] found. */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
)

/**
 * Reads a dynamic stream's relay list out of the store. Every relay list in the
 * protocol is the same shape — a tag with a url at a fixed offset — so one
 * extraction path driven by [RelaySource] covers NIP-65 outboxes, NIP-51 relay
 * sets, NIP-17 DM inboxes, NIP-66 monitor reports, NIP-85 provider lists, and
 * the relay hints riding on ordinary `e`/`p`/`a`/`q` tags. Nothing here knows
 * about a specific kind: each source brings its own NIP-01 filter for which
 * events to scan, and a list of selects for where the urls sit in them.
 *
 * The store is the crawl: an ordinary `down` stream on a few relays fills it,
 * and this turns what landed into the fan-out the dynamic stream syncs against.
 * Every source in the list is read and their relays unioned, so one stream can
 * pull from relay lists and hints at once.
 *
 * Nothing truncates that set. Every relay any source names is returned; the only
 * ones left out are [DynamicRelayList.exclude] and the caller's [skip] set.
 *
 * ## Why there is no reference count any more
 *
 * The set used to carry how many tags named each relay, and the fan-out went
 * most-referenced first. Getting that number meant materializing every matching
 * event — content, tags, sig — to read one tag off each: 2.6M kind-10002 events
 * per cycle on this deployment, ~265 pages, on a 6-hour timer, for a set that
 * barely moves.
 *
 * The store now answers the question directly ([NostrSemanticsStore.distinctTagValues],
 * a tags-only visit projection), and it returns a SET — the count is not
 * recoverable from it. The ordering was a heuristic, not correctness: every
 * discovered relay is synced in the cycle either way, and [DynamicRelayList.concurrency]
 * decides how many at once, not which. Trading it for a walk that reads one
 * field instead of whole documents is the deal on offer.
 */
object RelayDiscovery {
    /**
     * Every relay [dynamic]'s sources point at right now, most-referenced first.
     * Ties break on the url so a cycle's fan-out is stable between refreshes.
     */
    suspend fun discover(
        store: IEventStore,
        dynamic: DynamicRelayList,
        skip: Set<NormalizedRelayUrl> = emptySet(),
        pageSize: Int = SCAN_PAGE,
    ): List<DiscoveredRelay> {
        val found = LinkedHashSet<NormalizedRelayUrl>()
        for (source in dynamic.sources) {
            // A NAMED tag goes to the store's projection: it streams the tags
            // field alone, where paging events materialized all of each one.
            val named = source.selects.filter { it.tag != null }
            val anyTag = source.selects.filter { it.tag == null }
            val semantics = (store as? VespaEventStore)?.store
            if (semantics != null) {
                for (select in named) {
                    // A select naming a kind narrows the scan to it; the source
                    // filter already carries the rest.
                    val filter = select.kind?.let { source.filter.copy(kinds = listOf(it)) } ?: source.filter
                    val raw =
                        semantics.distinctTagValues(
                            filter = filter,
                            tagName = select.tag!!,
                            valueIndex = select.index,
                            // The whole tag, so a positional condition on another
                            // element still applies — NIP-65's marker at 2.
                            where = { tag -> select.where.isEmpty() || select.where.any { it.matches(tag.toTypedArray()) } },
                        )
                    for (v in raw) normalize(v, requireScheme = false)?.let(found::add)
                }
            }
            // A select with NO tag name can match anything in an event, which the
            // projection cannot express — it needs a tag to ask about. Those keep
            // the paging scan, and it is the rare shape: relay hints on ordinary
            // e/p/a/q tags.
            val stillPaged = if (semantics == null) source.selects else anyTag
            if (stillPaged.isNotEmpty()) {
                val perEvent = LinkedHashSet<NormalizedRelayUrl>()
                scan(store, source.filter, pageSize) { event ->
                    perEvent.clear()
                    urlsIn(event, stillPaged, perEvent)
                    found += perEvent
                }
            }
        }

        return found
            .asSequence()
            .filter { it !in dynamic.exclude && it !in skip }
            .map { DiscoveredRelay(it) }
            .sortedBy { it.url.url }
            .toList()
    }

    /**
     * Walk everything [filter] matches, a page at a time, oldest-ward.
     *
     * The store answers an unbounded query with the whole match set in one list,
     * and a relay-list kind on a large relay is millions of events — a scan that
     * asked for all of it at once would size the heap to the corpus. Only the url
     * counts need to survive a page, so this pages by a `until` cursor and lets
     * each page go: memory is a page plus the counts, whatever the corpus.
     *
     * A page boundary can fall inside a run of events sharing one `created_at`.
     * `until` is inclusive, so the next page re-sees them; [boundaryIds] carries
     * exactly that run forward to skip it, which is bounded by the page rather
     * than by the scan. A page that is *entirely* one timestamp can't advance the
     * cursor at all — then it steps below the timestamp, the one case where a
     * scan may miss same-second events beyond a page's worth.
     */
    private suspend fun scan(
        store: IEventStore,
        filter: Filter,
        pageSize: Int,
        onEach: (Event) -> Unit,
    ) {
        // An explicit `limit` is the caller's budget for the whole scan, not a
        // per-page size — and only events actually handed to [onEach] spend it,
        // so re-reading a boundary doesn't quietly eat into it.
        var remaining = filter.limit ?: Int.MAX_VALUE
        var until = filter.until
        var boundaryIds = emptySet<String>()
        while (remaining > 0) {
            // Room for what we still owe the caller, plus the boundary run we
            // are about to re-read and discard.
            val budget = remaining.toLong() + boundaryIds.size
            var ask = minOf(pageSize.toLong(), budget).toInt()
            var page: List<Event> = store.query(filter.copy(until = until, limit = ask))
            if (page.isEmpty()) return

            // Results are newest-first, so a page whose ends share a `created_at`
            // is entirely one timestamp — and then the cursor has nowhere to go:
            // `until` is inclusive, so repeating it re-reads the same page, and
            // stepping below it drops the events in that run we haven't seen.
            // Ask for a bigger page until it spans two timestamps. This is the
            // one place the page may exceed [pageSize]; a run longer than a page
            // is rare, and reading it is the only way not to lose it.
            while (page.size == ask && ask < budget && page.first().createdAt == page.last().createdAt) {
                ask = minOf(ask.toLong() * 2, budget).toInt()
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
                // Still one timestamp even after growing — we ran out of budget
                // to widen. Step below it; the remainder of that second is what
                // the caller's own `limit` chose to stop at.
                until = oldest - 1
                boundaryIds = emptySet()
            } else {
                until = oldest
                boundaryIds = newBoundary
            }
        }
    }

    /**
     * Events per page. Big enough that a normal relay-list scan is one or two
     * round trips, small enough that a page is a bounded allocation.
     */
    private const val SCAN_PAGE = 10_000

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
            // A select with no kind applies to everything the scan collected.
            if (select.kind != null && select.kind != event.kind) continue
            urlsIn(event, select, into)
        }
    }

    /**
     * The relay urls one event advertises for a single [select]: every tag named
     * [RelaySelect.tag] (or every tag at all, when it is null) that carries a url
     * at [RelaySelect.index] and passes the select's [RelaySelect.where] list.
     */
    fun urlsIn(
        event: Event,
        select: RelaySelect,
    ): Set<NormalizedRelayUrl> = LinkedHashSet<NormalizedRelayUrl>().also { urlsIn(event, select, it) }

    private fun urlsIn(
        event: Event,
        select: RelaySelect,
        into: MutableSet<NormalizedRelayUrl>,
    ) {
        for (tag in event.tags) {
            if (tag.size <= select.index) continue
            if (select.tag != null && tag[0] != select.tag) continue
            // `where` entries OR together and each ANDs its own fields — NIP-01's
            // boolean shape pointed at the tag. An empty list keeps everything;
            // NIP-65's read/write rule is the three-entry OR `marker` expands to.
            if (select.where.isNotEmpty() && select.where.none { it.matches(tag) }) continue
            // With no tag name to go on, anything in the event could land here, so
            // only take values that already say they are a relay.
            normalize(tag[select.index], requireScheme = select.tag == null)?.let { into.add(it) }
        }
    }

    /**
     * Relay lists in the wild carry prose and pet names where a url belongs. The
     * normalizer is forgiving by design — it will happily turn `not a url` into
     * `wss://not/` — so anything blank or with whitespace in it is dropped before
     * it gets there. A scheme-less host is still fine when the source named a tag:
     * that one the normalizer fixes correctly.
     *
     * Two more classes are dropped here because dialling them cannot work rather
     * than merely being unlikely to, and a url that cannot work is worse than
     * useless: it burns a connect timeout and a concurrency permit every cycle,
     * forever, and would be recorded as an unreachable relay when the truth is
     * that we were never able to ask.
     *
     *  - `.onion`, with no Tor transport configured on this client. Every dial is
     *    a guaranteed timeout.
     *  - loopback and private hosts. `ws://localhost:4869` in someone else's
     *    relay list means THEIR machine; from in here it resolves to us or to
     *    nothing, and following it is a request we were never invited to make.
     */
    private fun normalize(
        raw: String,
        requireScheme: Boolean,
    ): NormalizedRelayUrl? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty() || trimmed.any { it.isWhitespace() }) return null
        if (requireScheme && !trimmed.startsWith("ws://", true) && !trimmed.startsWith("wss://", true)) return null
        val url = RelayUrlNormalizer.normalizeOrNull(trimmed) ?: return null
        if (RelayUrlNormalizer.isOnion(url.url) || RelayUrlNormalizer.isLocalHost(url.url)) return null
        return url
    }
}
