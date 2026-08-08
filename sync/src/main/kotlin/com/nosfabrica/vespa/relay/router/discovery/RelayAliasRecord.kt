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

import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent

/**
 * The NIP-66 half of [RelayAliases]: a fold verdict, written down where the
 * next boot — and anyone else running an outbox crawler — can read it.
 *
 * This is the same monitor that already signs "I could not reach this relay",
 * saying the other thing a dial can prove: "this url and that url served me the
 * same events, so they are one relay". It rides on kind 30166, whose `d` tag is
 * already the relay url, and adds one tag:
 *
 * ```json
 * ["redirect", "wss://nos.lol/", "1000 newest events, 0.98 shared"]
 * ```
 *
 * Unknown tags are ignored by every other NIP-66 consumer, so a monitor that
 * has never heard of this reads the record as an ordinary relay observation.
 *
 * Three reasons the verdict lives in the store as an event rather than in a
 * state file beside the bands:
 *
 *  - it is a claim about a relay, which is what kind 30166 IS, and the monitor
 *    is already the thing in this process licensed to make those;
 *  - 30166 is addressable, so re-probing a url REPLACES its verdict instead of
 *    appending — the store does the deduplication a file would need code for;
 *  - it is served. An operator can ask this relay why it stopped syncing a url
 *    and get a signed answer with the evidence in it.
 *
 * Read back with [load], which drops anything older than [ttlSeconds]: a url
 * that is a duplicate today may be a distinct relay in a month, and a verdict
 * nobody re-measures is a relay silently missing from the fan-out.
 */
class RelayAliasRecord(
    private val store: IEventStore,
    private val signer: NostrSigner?,
    private val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
) {
    /**
     * Every verdict this monitor published and still stands behind, as
     * `alias url -> canonical url`.
     *
     * Queried by `#d` rather than walked, because `d` is a single-letter tag
     * and therefore the only part of these records the tag index can answer on
     * — the `redirect` tag is not queryable and has to be read off the event.
     * [candidates] bounds the query to the urls this cycle actually discovered.
     */
    suspend fun load(candidates: Collection<NormalizedRelayUrl>): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val self = signer?.pubKey ?: return emptyMap()
        if (candidates.isEmpty()) return emptyMap()
        val floor = nowSeconds() - ttlSeconds
        val out = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            val held: List<Event> =
                runCatching {
                    store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk)))
                }.getOrNull() ?: continue
            for (event in held) {
                if (event.createdAt < floor) continue
                val alias = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: continue
                val canonical = event.tags.firstOrNull { it.size > 1 && it[0] == REDIRECT_TAG }?.get(1) ?: continue
                val from = RelayUrlNormalizer.normalizeOrNull(alias) ?: continue
                val to = RelayUrlNormalizer.normalizeOrNull(canonical) ?: continue
                if (from != to) out[from] = to
            }
        }
        return out
    }

    /**
     * Sign and store one verdict. Returns the event so a caller can push it
     * upstream; null when there is no signer, which is also when the router
     * runs without a NIP-66 monitor at all.
     *
     * The evidence goes in the tag and the content, not because anything parses
     * it, but because this is a public statement about somebody else's server
     * and the reader deserves to see what it rests on.
     */
    suspend fun publish(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? {
        val signer = signer ?: return null
        val evidence = "$sampled newest events, $shared shared with ${canonical.url}"
        val template =
            RelayDiscoveryEvent.build(alias, "", nowSeconds()) {
                add(arrayOf(REDIRECT_TAG, canonical.url, evidence))
            }
        return runCatching {
            val event = signer.sign(template)
            store.insert(event)
            event
        }.getOrNull()
    }

    companion object {
        /**
         * The tag that carries the fold. Not a NIP-66 tag — this monitor
         * defines it — so it is spelled out rather than abbreviated, and every
         * other consumer skips it as an unknown tag.
         */
        const val REDIRECT_TAG = "redirect"

        /**
         * Thirty days. Long enough that the probe is a one-off per url rather
         * than a recurring cost, short enough that a host which splits one
         * endpoint into several real relays is noticed within a month.
         */
        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /** Urls per `#d` query. The fan-out is five figures wide; the filter should not be. */
        private const val QUERY_CHUNK = 500
    }
}
