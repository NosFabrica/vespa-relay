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

/** One relay a [RelaySource] found, and how many lists in the store name it. */
data class DiscoveredRelay(
    val url: NormalizedRelayUrl,
    val references: Int,
)

/**
 * Reads a dynamic stream's relay list out of the store: pull every relay-list
 * event of the source's kind and take the urls each one advertises. The store is
 * the crawl — an ordinary `down` stream on a handful of indexer relays fills it
 * with 10002s and 10040s, and this turns that into the fan-out the dynamic
 * stream syncs against.
 *
 * Nothing here truncates that set. Every relay named by any list is returned, so
 * a cycle covers the whole network the store knows about; the only relays left
 * out are the ones the config named in [RelaySource.exclude] and the caller's
 * own [skip] set. The reference count rides along to order the fan-out (the
 * relays most lists agree on go first) and to make the logs legible.
 */
object RelayDiscovery {
    /**
     * Every relay [source] points at right now, most-referenced first. Ties break
     * on the url so a cycle's fan-out is stable between refreshes.
     */
    suspend fun discover(
        store: IEventStore,
        source: RelaySource,
        skip: Set<NormalizedRelayUrl> = emptySet(),
    ): List<DiscoveredRelay> {
        val counts = HashMap<NormalizedRelayUrl, Int>()
        val lists: List<Event> = store.query(Filter(kinds = listOf(source.kind.kind)))
        for (list in lists) {
            // One list counts a relay once, however many times it repeats the tag.
            for (url in urlsIn(list, source)) counts[url] = (counts[url] ?: 0) + 1
        }

        return counts
            .asSequence()
            .filter { it.key !in source.exclude && it.key !in skip }
            .map { DiscoveredRelay(it.key, it.value) }
            .sortedWith(compareByDescending<DiscoveredRelay> { it.references }.thenBy { it.url.url })
            .toList()
    }

    /** The distinct relay urls one relay-list event advertises for [source]. */
    fun urlsIn(
        event: Event,
        source: RelaySource,
    ): Set<NormalizedRelayUrl> =
        when (source.kind) {
            RelayListKind.OUTBOX -> outboxUrls(event, source.role)
            RelayListKind.TRUST_PROVIDERS -> trustProviderUrls(event)
        }

    /**
     * NIP-65: `["r", "<url>"]`, optionally marked `read` or `write`. An unmarked
     * tag is both, so it belongs to whichever side we asked for — that is the
     * "write or empty" rule the outbox model runs on.
     */
    private fun outboxUrls(
        event: Event,
        role: RelayRole,
    ): Set<NormalizedRelayUrl> {
        val urls = LinkedHashSet<NormalizedRelayUrl>()
        for (tag in event.tags) {
            if (tag.size < 2 || tag[0] != "r") continue
            if (!role.matches(tag.getOrNull(2)?.trim()?.lowercase())) continue
            normalize(tag[1])?.let { urls.add(it) }
        }
        return urls
    }

    /**
     * NIP-85: `["<kind>:<type>", "<pubkey of the provider>", "<relay url>"]` —
     * the relay is where that provider publishes its assertions, so it is the
     * relay we want to sync. Anything that isn't a `kind:type` service tag with
     * a url in it is skipped.
     */
    private fun trustProviderUrls(event: Event): Set<NormalizedRelayUrl> {
        val urls = LinkedHashSet<NormalizedRelayUrl>()
        for (tag in event.tags) {
            if (tag.size < 3 || !isServiceTag(tag[0])) continue
            normalize(tag[2])?.let { urls.add(it) }
        }
        return urls
    }

    /** `30382:rank` and friends: a kind, a colon, and a non-empty type. */
    private fun isServiceTag(name: String): Boolean {
        val colon = name.indexOf(':')
        if (colon <= 0 || colon == name.length - 1) return false
        return name.take(colon).all { it.isDigit() }
    }

    /**
     * Relay lists in the wild carry prose where a url belongs. The normalizer is
     * forgiving by design — it will happily turn `not a url` into `wss://not/` —
     * so anything blank or with whitespace in it is dropped before it gets there.
     * A scheme-less host is still fine: that one the normalizer fixes correctly.
     */
    private fun normalize(raw: String): NormalizedRelayUrl? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty() || trimmed.any { it.isWhitespace() }) return null
        return RelayUrlNormalizer.normalizeOrNull(trimmed)
    }
}
