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
package com.nosfabrica.vespa.relay.router.presence

import com.nosfabrica.vespa.relay.router.config.PresenceConfig
import com.nosfabrica.vespa.relay.router.config.PresenceSource
import com.nosfabrica.vespa.relay.router.config.withoutDefaultPort
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.serviceProviders
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes

/**
 * One relay a presence stream should be listening to, and the exact question to
 * ask it.
 *
 * The pair travels together for the reason a `relaySource` binding does: the
 * relay was named ALONGSIDE something (this author writes there; this service
 * publishes there), and asking it for the stream's whole filter instead would
 * turn one person signing in into a corpus download from somebody's personal
 * relay.
 *
 * [key] is the identity of the subscription, so two readers whose lists name the
 * same (relay, question) share one REQ rather than opening two. Derived from
 * the filter's own serialized form because that is what a relay actually
 * receives — two filters that differ only in field order are one question, and
 * `Filter.toJson` is where quartz already fixes that order.
 */
internal data class PresenceTarget(
    val url: NormalizedRelayUrl,
    val filter: Filter,
) {
    val key: String get() = url.url + " " + filter.toJson()
}

/**
 * A signed-in reader's own lists, turned into the relays to listen to.
 *
 * Split from the loop on purpose: this half is pure — an event in, targets out
 * — which is what lets `PresenceTargetsTest` pin the two shapes that matter
 * (which tags count, and what the filter is narrowed BY) against real event
 * bodies rather than against a store.
 *
 * Neither reader-facing kind is trusted to be sane. A kind 10002 in this corpus
 * has been measured at 10,591 entries, and a 10040 may name a service with no
 * relay hint, a followers-only service, or nothing at all. So every path here
 * ends in "produce fewer targets", never in an error: a reader with an unusable
 * list is mirrored for by nothing, exactly as they were before this existed.
 */
internal object PresenceTargets {
    /**
     * The store reads that back [of] — one replaceable event per reader.
     *
     * `limit = 1` because both kinds are replaceable and the store holds the
     * current version, and because this runs once per signed-in reader per
     * poll: a read that could return history would make the cost of the feature
     * a function of how much somebody has edited their relay list.
     */
    fun listFilter(
        source: PresenceSource,
        reader: HexKey,
    ) = Filter(
        kinds = listOf(if (source == PresenceSource.OUTBOX) AdvertisedRelayListEvent.KIND else TrustProviderListEvent.KIND),
        authors = listOf(reader),
        limit = 1,
    )

    /**
     * What [list] — the reader's own 10002 or 10040 — says this stream should
     * listen to.
     *
     * @param list the reader's list event, as stored. Its author is NOT
     *   re-checked here: [listFilter] asked by `authors`, and a store handing
     *   back an event by somebody else is a store bug rather than a case to
     *   silently work around.
     * @param base the stream's configured filter, which supplies the kinds. Only
     *   `authors` is overwritten — a stream that set its own `authors` would be
     *   asking a question presence cannot narrow, and the loader has nothing to
     *   say about that because the value is per reader.
     */
    fun of(
        source: PresenceSource,
        list: Event,
        base: Filter,
        config: PresenceConfig,
    ): List<PresenceTarget> {
        val targets =
            when (source) {
                PresenceSource.OUTBOX -> outbox(list, base)
                PresenceSource.SCORES -> scores(list, base)
            }
        return targets
            .asSequence()
            .filter { it.url !in config.exclude }
            .distinctBy { it.key }
            // The cap is applied LAST, after excludes, so an operator excluding
            // a url gives the reader another slot rather than spending one on a
            // relay we were never going to dial.
            .take(config.maxRelaysPerReader)
            .toList()
    }

    /**
     * NIP-65's write side, asked for this author and nobody else.
     *
     * The marker rule is quartz's own `AdvertisedRelayInfo.parseWriteNorm` —
     * marked `write`, or unmarked, which NIP-65 reads as both — rather than a
     * second copy of it here. The router already learned that lesson on the
     * band arithmetic: a fork of somebody else's rule is a fork that silently
     * stops matching theirs.
     */
    private fun outbox(
        list: Event,
        base: Filter,
    ): List<PresenceTarget> {
        val narrowed = base.copy(authors = listOf(list.pubKey))
        return list.tags
            .asSequence()
            .filter { it.isNotEmpty() && it[0] == AdvertisedRelayInfo.TAG_NAME }
            .mapNotNull { AdvertisedRelayInfo.parseWriteNorm(it) }
            .map { PresenceTarget(withoutDefaultPort(it), narrowed) }
            .toList()
    }

    /**
     * The `30382:rank` entries of a NIP-85 trust provider list: one target per
     * (service, relay hint) PAIR.
     *
     * Paired, never gathered. Collecting the services into one set and the
     * relays into another and asking each relay for every service is the cross
     * product — the same mistake `RelaySelect.bindings` exists to prevent — and
     * here it would also be wrong on the merits: a provider's cards live on the
     * relay their own list names, and asking someone else's provider relay for
     * them is a REQ that can only come back empty.
     *
     * `rank` only. A 10040 may name a `followerCount` service too, and those
     * cards order a set rather than rank one — `TrustProjection` builds no
     * influence cell from them, so mirroring them would not move ranked search
     * one row. The other entries are somebody else's stream to configure.
     *
     * A hintless entry produces nothing, which is quartz's decision rather than
     * one taken here: `ServiceProviderTag.parse` requires all three fields, so
     * such a tag is not in `serviceProviders()` at all — the same reason
     * `TrustNotice` tells a reader their list "names no usable 30382:rank
     * service".
     */
    private fun scores(
        list: Event,
        base: Filter,
    ): List<PresenceTarget> =
        list.tags
            .serviceProviders()
            .asSequence()
            .filter { it.service == ProviderTypes.rank }
            .map { PresenceTarget(withoutDefaultPort(it.relayUrl), base.copy(authors = listOf(it.pubkey))) }
            .toList()
}
