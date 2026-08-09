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
 * saying the other thing a dial can prove: which urls are ONE relay. It rides on
 * kind 30166, whose `d` tag is already the relay url, and adds one tag in two
 * forms:
 *
 * ```json
 * ["same-as", "wss://nos.lol/",             "500 newest events, 498 shared with wss://nos.lol/"]
 * ["same-as", "wss://nostr.ac/v1",          "500 newest events, best 2 shared of 19 peer(s) on this host"]
 * ```
 *
 * The first says this url and that url are the same relay. The second — where
 * the value IS the record's own url — says it was measured and found equivalent
 * to nothing but itself, which is trivially true and therefore safe for a
 * reader that does not know the tag.
 *
 * **`same-as` rather than `redirect`, which this used to be called.** A redirect
 * is a directed edge carrying authority: the server told you to go elsewhere,
 * and the url you asked for is not the endpoint. Both halves are false here —
 * the relay said no such thing, we measured it, and the alias serves perfectly
 * well. What a fingerprint establishes is an EQUIVALENCE, and equivalence is
 * symmetric: a consumer running union-find over these tags gets the right
 * partition without having to share our opinion about which member to dial.
 * That opinion is [RelayAliases.PREFERENCE] and it stays ours.
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
     * Both halves of what this monitor has decided and still stands behind.
     *
     * [Verdicts.aliases] are the folds; [Verdicts.distinct] are the urls a probe
     * cleared as their own relay. The second is why this returns a pair rather
     * than a map: without persisting "measured, and it is nobody's duplicate",
     * every boot re-fingerprints all the NON-duplicates forever — 59 of them in
     * the live run against a store already holding 128 folds.
     */
    data class Verdicts(
        /** Folded url -> the url that stands in for it. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls proven to be their own relay. Never a key in [aliases]. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
    )

    /**
     * Read back every verdict covering [candidates].
     *
     * Queried by `#d` rather than walked, because `d` is a single-letter tag
     * and therefore the only part of these records the tag index can answer on
     * — `same-as` is not queryable and has to be read off the event.
     * [candidates] bounds the query to the urls this cycle actually discovered.
     *
     * The two forms are told apart by comparing the tag's value to the record's
     * own `d`: pointing elsewhere is a fold, pointing at itself is the cleared
     * verdict. Normalising both sides first, so `wss://nos.lol` and
     * `wss://nos.lol/` cannot read as a fold of a url onto itself.
     */
    suspend fun load(candidates: Collection<NormalizedRelayUrl>): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        if (candidates.isEmpty()) return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            val held: List<Event> =
                runCatching {
                    store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk)))
                }.getOrNull() ?: continue
            for (event in held) {
                if (event.createdAt < floor) continue
                val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: continue
                val sameAs = event.tags.firstOrNull { it.size > 1 && it[0] == SAME_AS_TAG }?.get(1) ?: continue
                val from = RelayUrlNormalizer.normalizeOrNull(subject) ?: continue
                val to = RelayUrlNormalizer.normalizeOrNull(sameAs) ?: continue
                if (from == to) distinct += from else aliases[from] = to
            }
        }
        return Verdicts(aliases, distinct)
    }

    /**
     * Sign and store one verdict. Returns the event so a caller can push it
     * upstream; null when there is no signer, which is also when the router
     * runs without a NIP-66 monitor at all.
     *
     * The evidence goes in the tag's third element — not the content, which
     * belongs to the relay's own NIP-11-ish document and is carried across
     * edits untouched. Nothing parses the evidence; it is there because this is
     * a public statement about somebody else's server and the reader deserves
     * to see what it rests on.
     */
    suspend fun publish(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "$sampled newest events, $shared shared with ${canonical.url}")

    /**
     * Sign and store the other verdict: this url was fingerprinted against the
     * other urls on its host and matched none of them.
     *
     * Written as `same-as` pointing at the record's OWN url, which is a true
     * statement rather than a placeholder — the equivalence class of this relay
     * contains only itself, as far as this measurement saw.
     *
     * **What it does not claim.** Every url in a group is compared to the
     * group's leader, not to each other, so this says "not the leader" and not
     * "not any of them". Two paths on a host that are duplicates OF EACH OTHER
     * but not of the leader are both recorded distinct and both keep getting
     * dialled. That is a property of leader-based grouping, present within a
     * single pass as much as across boots, and persisting the verdict neither
     * causes it nor makes it worse.
     */
    suspend fun publishDistinct(
        url: NormalizedRelayUrl,
        sampled: Int,
        peers: Int,
        bestShared: Int,
    ): Event? = write(url, url, "$sampled newest events, best $bestShared shared of $peers peer(s) on this host")

    private suspend fun write(
        subject: NormalizedRelayUrl,
        sameAs: NormalizedRelayUrl,
        evidence: String,
    ): Event? = edit(subject, owned = setOf(SAME_AS_TAG), add = listOf(arrayOf(SAME_AS_TAG, sameAs.url, evidence)))

    /**
     * Edit a replaceable record: read what is there, keep everything this
     * writer does not own, apply [add], and store it one second past whatever
     * it replaced.
     *
     * **A replaceable event has one address and more than one writer, so
     * writing is always an edit.** NIP-66's relay record is addressed by
     * `d` = the relay url, and the passive monitor updates it every time a
     * connection is opened. A writer that builds the record from its own tags
     * alone silently deletes everyone else's — measured here, `[d, n,
     * rtt-open]` became `[d, redirect]` — and nothing about the result looks
     * wrong: still signed, still a valid NIP-66 record, just saying less than
     * it did. Anything that reads that record downstream loses information it
     * had no way to know was ever there.
     *
     * [owned] is the small set of tag names this writer is allowed to replace.
     * Everything else is carried forward untouched, whoever wrote it and
     * whatever it means.
     *
     * The timestamp is `max(now, existing + 1)` rather than `now`, because a
     * store enforcing replaceable semantics REJECTS an edit that is not newer
     * — and two writers seconds apart, or a clock that has not moved, are
     * ordinary. Silently losing the write to `replaced: a newer version
     * exists` is how a repair pass reports success having done nothing.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owned: Set<String>,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        val current = currentRecord(url)
        val kept = current?.tags?.filterNot { it.firstOrNull() == "d" || it.firstOrNull() in owned }.orEmpty()
        val at = maxOf(nowSeconds(), (current?.createdAt ?: 0L) + 1)
        val template =
            RelayDiscoveryEvent.build(url, current?.content.orEmpty(), at) {
                for (tag in kept) add(tag)
                for (tag in add) add(tag)
            }
        return runCatching {
            val event = signer.sign(template)
            store.insert(event)
            event
        }.getOrNull()
    }

    /** This url's current record, or null when nothing holds one yet. */
    private suspend fun currentRecord(url: NormalizedRelayUrl): Event? {
        val self = signer?.pubKey ?: return null
        val held: List<Event> =
            runCatching {
                store.query<Event>(
                    Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to listOf(url.url))),
                )
            }.getOrNull().orEmpty()
        return held.maxByOrNull { it.createdAt }
    }

    companion object {
        /**
         * The tag that carries the verdict, in both forms. Not a NIP-66 tag —
         * this monitor defines it — so it is spelled out rather than
         * abbreviated, and every other consumer skips it as an unknown tag.
         *
         * Named for the relation it states rather than the action we take on
         * it: `same-as` is an equivalence, which is what a matching fingerprint
         * proves, while `redirect` (what this was called first) would smuggle
         * in both an instruction the relay never gave and our own opinion about
         * which member of the class to dial.
         */
        const val SAME_AS_TAG = "same-as"

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
