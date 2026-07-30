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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore

/** One relay a [DynamicRelayList] found, and how many tags across it named the relay. */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
    val references: Int,
)

/**
 * Reads a dynamic stream's relay list out of the store. Every relay list in the
 * protocol is the same shape — a tag with a url at a fixed offset — so one
 * extraction path driven by [RelaySource] covers NIP-65 outboxes, NIP-51 relay
 * sets, NIP-17 DM inboxes, NIP-66 monitor reports, NIP-85 provider lists, and
 * the relay hints riding on ordinary `e`/`p`/`a`/`q` tags. Nothing here knows
 * about a specific kind.
 *
 * The store is the crawl: an ordinary `down` stream on a few relays fills it,
 * and this turns what landed into the fan-out the dynamic stream syncs against.
 * Every source in the list is read and their relays unioned, so one stream can
 * pull from relay lists and hints at once.
 *
 * Nothing truncates that set. Every relay any source names is returned; the only
 * ones left out are [DynamicRelayList.exclude] and the caller's [skip] set. The
 * reference count rides along to order the fan-out (relays the most tags agree
 * on go first) and to make the logs legible.
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
    ): List<DiscoveredRelay> {
        val counts = HashMap<NormalizedRelayUrl, Int>()
        for (source in dynamic.sources) {
            val since = if (source.sinceSeconds > 0) System.currentTimeMillis() / 1000 - source.sinceSeconds else null
            val events: List<Event> = store.query(Filter(kinds = listOf(source.kind), since = since))
            for (event in events) {
                // One event counts a relay once, however many of its tags repeat it.
                for (url in urlsIn(event, source)) counts[url] = (counts[url] ?: 0) + 1
            }
        }

        return counts
            .asSequence()
            .filter { it.key !in dynamic.exclude && it.key !in skip }
            .map { DiscoveredRelay(it.key, it.value) }
            .sortedWith(compareByDescending<DiscoveredRelay> { it.references }.thenBy { it.url.url })
            .toList()
    }

    /**
     * The distinct relay urls one event advertises for [source]: every tag named
     * [RelaySource.tag] (or every tag at all, when it is null) that carries a url
     * at [RelaySource.urlIndex] and passes the marker check at the slot after it.
     */
    fun urlsIn(
        event: Event,
        source: RelaySource,
    ): Set<NormalizedRelayUrl> {
        val urls = LinkedHashSet<NormalizedRelayUrl>()
        for (tag in event.tags) {
            if (tag.size <= source.urlIndex) continue
            if (source.tag != null && tag[0] != source.tag) continue
            // NIP-65 marks its relays `read` or `write` in the slot after the url;
            // an unmarked tag is both, so it matches whichever side we asked for.
            val role = source.role
            if (role != null && !role.matches(tag.getOrNull(source.urlIndex + 1)?.trim()?.lowercase())) continue
            // With no tag name to go on, anything in the event could land here, so
            // only take values that already say they are a relay.
            normalize(tag[source.urlIndex], requireScheme = source.tag == null)?.let { urls.add(it) }
        }
        return urls
    }

    /**
     * Relay lists in the wild carry prose and pet names where a url belongs. The
     * normalizer is forgiving by design — it will happily turn `not a url` into
     * `wss://not/` — so anything blank or with whitespace in it is dropped before
     * it gets there. A scheme-less host is still fine when the source named a tag:
     * that one the normalizer fixes correctly.
     */
    private fun normalize(
        raw: String,
        requireScheme: Boolean,
    ): NormalizedRelayUrl? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty() || trimmed.any { it.isWhitespace() }) return null
        if (requireScheme && !trimmed.startsWith("ws://", true) && !trimmed.startsWith("wss://", true)) return null
        return RelayUrlNormalizer.normalizeOrNull(trimmed)
    }
}
