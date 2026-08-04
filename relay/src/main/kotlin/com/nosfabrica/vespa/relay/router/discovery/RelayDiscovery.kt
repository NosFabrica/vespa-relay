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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.RelaySelect
import com.nosfabrica.vespa.relay.router.config.Slot
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore

/**
 * One relay a [RelayDiscoveryConfig] found, and what the tags that named it paired
 * it with. [narrow] is empty for a select that binds nothing but the url, and
 * the stream then asks this relay for its whole filter.
 */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
    val narrow: Map<String, Set<String>> = emptyMap(),
) {
    /**
     * [base] narrowed by everything this relay was paired with. Values are
     * sorted, because a band is keyed on the filter's serialized form —
     * an unordered set would key the same ask two different ways on two runs
     * and re-walk history for nothing.
     */
    fun narrowed(base: Filter): Filter {
        if (narrow.isEmpty()) return base
        var f = base
        narrow["authors"]?.let { f = f.copy(authors = it.sorted()) }
        narrow["ids"]?.let { f = f.copy(ids = it.sorted()) }
        narrow["kinds"]?.let { v -> f = f.copy(kinds = v.mapNotNull { it.toIntOrNull() }.sorted()) }
        // Filter.tags keys drop the '#' — `#p` on the wire is `p` in the map.
        val tags = narrow.filterKeys { it.startsWith("#") }
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
    ): List<DiscoveredRelay> {
        val found = LinkedHashSet<NormalizedRelayUrl>()
        // url -> destination -> values, unioned across every select and source.
        val narrowing = HashMap<NormalizedRelayUrl, MutableMap<String, MutableSet<String>>>()
        for (source in dynamic.sources) {
            // A named tag with no bindings goes to the store's tags-only
            // projection, which streams one field instead of materializing
            // whole events (a 2.6M-event scan became the projection's walk).
            // A binding select must page: the projection returns a SET of
            // values, and the pairing a binding exists to keep is gone by the
            // time it returns.
            val named = source.selects.filter { it.tag != null && it.bindings.isEmpty() }
            val anyTag = source.selects.filter { it.tag == null || it.bindings.isNotEmpty() }
            val semantics = (store as? VespaEventStore)?.store
            if (semantics != null) {
                for (select in named) {
                    // A select naming a kind narrows the scan to it; the
                    // source filter already carries the rest.
                    val filter = select.kind?.let { source.filter.copy(kinds = listOf(it)) } ?: source.filter
                    val raw =
                        semantics.distinctTagValues(
                            filter = filter,
                            tagName = select.tag!!,
                            valueIndex = select.index,
                            // The whole tag, so a positional condition on
                            // another element still applies (NIP-65's marker).
                            where = { tag -> select.where.isEmpty() || select.where.any { it.matches(tag.toTypedArray()) } },
                        )
                    for (v in raw) normalize(v, requireScheme = false)?.let(found::add)
                }
            }
            // A select with no tag name can match anything in an event, which
            // the projection cannot express. Those keep the paging scan.
            val stillPaged = if (semantics == null) source.selects else anyTag
            if (stillPaged.isNotEmpty()) {
                scan(store, source.filter, pageSize) { event ->
                    for (select in stillPaged) {
                        if (select.kind != null && select.kind != event.kind) continue
                        bindingsIn(event, select) { url, bound ->
                            found += url
                            if (bound.isNotEmpty()) {
                                val per = narrowing.getOrPut(url) { HashMap() }
                                for ((dest, value) in bound) per.getOrPut(dest) { HashSet() }.add(value)
                            }
                        }
                    }
                }
            }
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
     */
    private suspend fun scan(
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
    ): Set<NormalizedRelayUrl> = LinkedHashSet<NormalizedRelayUrl>().also { urlsIn(event, select, it) }

    private fun urlsIn(
        event: Event,
        select: RelaySelect,
        into: MutableSet<NormalizedRelayUrl>,
    ) = bindingsIn(event, select) { url, _ -> into.add(url) }

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
        onMatch: (NormalizedRelayUrl, Map<String, String>) -> Unit,
    ) {
        for (tag in event.tags) {
            if (tag.size <= select.index) continue
            if (select.tag != null && tag[0] != select.tag) continue
            // `where` entries OR together and each ANDs its own fields.
            if (select.where.isNotEmpty() && select.where.none { it.matches(tag) }) continue
            // With no tag name to go on, only take values that already say
            // they are a relay.
            val url = normalize(tag[select.index], requireScheme = select.tag == null) ?: continue
            if (select.bindings.isEmpty()) {
                onMatch(url, emptyMap())
                continue
            }
            val bound = HashMap<String, String>(select.bindings.size)
            var complete = true
            for ((dest, slot) in select.bindings) {
                val raw =
                    when (slot) {
                        is Slot.OfTag -> tag.getOrNull(slot.index)
                        Slot.EventPubkey -> event.pubKey
                        Slot.EventId -> event.id
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
     * dropped because dialling them cannot work: `.onion` (no Tor transport
     * here — every dial is a guaranteed timeout) and loopback/private hosts
     * (`ws://localhost` in someone else's relay list means THEIR machine).
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
