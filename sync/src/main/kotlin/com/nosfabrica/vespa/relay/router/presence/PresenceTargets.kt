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

import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.RelaySource
import com.nosfabrica.vespa.relay.router.config.withoutDefaultPort
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * One relay a presence stream should be listening to, and the exact question to
 * ask it.
 *
 * The pair travels together for the reason a `relaySource` binding does: the
 * relay was named ALONGSIDE something — this author writes there, this author is
 * read there, this service publishes there — and asking it for the stream's
 * whole filter instead would turn one person signing in into a corpus download
 * from somebody's personal relay.
 *
 * [key] is the identity of the subscription, so two readers whose lists resolve
 * to the same (relay, question) share one REQ rather than opening two. Derived
 * from the filter's own serialized form because that is what a relay actually
 * receives — two filters differing only in field order are one question, and
 * `Filter.toJson` is where quartz already fixes that order.
 */
internal data class PresenceTarget(
    val url: NormalizedRelayUrl,
    val filter: Filter,
) {
    val key: String get() = url.url + " " + filter.toJson()
}

/**
 * A signed-in reader's own events, turned into the relays to listen to.
 *
 * **Nothing about which tag means what lives here.** That is [RelaySelect] and
 * its bindings, read through `RelayDiscovery.relaysIn` — the same grammar and
 * the same code the corpus-scanning streams use, because a second reading of
 * NIP-65's marker rule is a second reading that silently stops matching the
 * first. What this adds is the scope: the scan is one reader, and the narrowing
 * a tag produced is applied to the stream's own filter.
 *
 * Split from the loop on purpose: this half is pure — events in, targets out —
 * which is what lets `PresenceTargetsTest` pin the shapes that matter (an
 * outbox asking for its owner, an inbox asking for mentions OF its owner, a
 * provider relay asking for its own service) against real event bodies rather
 * than against a store.
 *
 * Neither reader-facing kind is trusted to be sane. A kind 10002 in this corpus
 * has been measured at 10,591 entries, and a 10040 may name a service with no
 * relay hint or nothing at all. So every path here ends in "produce fewer
 * targets", never in an error: a reader with an unusable list is mirrored for by
 * nothing, exactly as they were before this existed.
 */
internal object PresenceTargets {
    /**
     * The store read for one source, scoped to one signed-in reader.
     *
     * `authors` is SET rather than merged, and the config loader refuses a
     * presence source that writes its own — the two would otherwise silently
     * disagree about whose lists are being read.
     *
     * Everything else in the source's filter is the operator's and is left
     * alone, including `limit`: a source over a replaceable kind is one event
     * per reader by nature, and one over a regular kind is where a `limit` is
     * how they said what a reader's scan may cost.
     */
    fun scanFor(
        source: RelaySource,
        reader: HexKey,
    ): Filter = source.filter.copy(authors = listOf(reader))

    /**
     * What [events] — one reader's own lists, as stored — say this stream should
     * listen to, in the order their tags were written.
     *
     * @param events the reader's own list events for ONE source, paired with
     *   that source so each is read through its own selects.
     * @param base the stream's configured filter, which supplies the kinds. The
     *   bindings replace `authors` / `ids` / `kinds` / `#x` on it, exactly as
     *   `DiscoveredRelay.narrowed` does on the dynamic path.
     */
    fun of(
        events: List<Pair<RelaySource, Event>>,
        base: Filter,
        dynamic: RelayDiscoveryConfig,
        maxRelaysPerReader: Int,
        allowOnion: Boolean = false,
        // Relay lists refused for being too long to be relay lists. Reported by
        // the caller rather than dropped silently, because a cap set too low
        // reads from outside exactly like a reader who publishes nothing.
        onOversized: () -> Unit = {},
    ): List<PresenceTarget> {
        val out = LinkedHashMap<String, PresenceTarget>()
        for ((source, event) in events) {
            if (RelayDiscovery.oversized(event, source.selects, dynamic.maxRelaysPerList)) {
                onOversized()
                continue
            }
            for (found in RelayDiscovery.relaysIn(event, source.selects, allowOnion)) {
                // `wss://x/` and `wss://x:443/` are one server written two ways
                // and the normalizer keeps both, so without this a reader naming
                // both costs two sockets, two bands and two subscriptions.
                val url = withoutDefaultPort(found.url)
                if (url in dynamic.exclude) continue
                val target = PresenceTarget(url, found.narrowed(base))
                out.putIfAbsent(target.key, target)
            }
        }
        // Applied LAST, after the excludes, so an operator excluding a url gives
        // the reader another slot rather than spending one on a relay we were
        // never going to dial.
        return out.values.take(maxRelaysPerReader)
    }
}
