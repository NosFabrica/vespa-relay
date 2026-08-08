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
package com.nosfabrica.vespa.relay.router.config

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * A stream's relay list, read from events our own store already holds instead
 * of a hand-written `urls` array. `relaySource` is a list of places to read
 * urls from — relay lists, trust-provider lists, relay hints — merged into one
 * fan-out:
 *
 *     outbox {
 *       dir            = "down"
 *       filter         = { "kinds": [0, 3, 10002] }
 *       refreshSeconds = 21600
 *       concurrency    = 8
 *       relaySource = [
 *         {
 *           select = [ { kind = 10002, tag = "r", marker = "write" } ]
 *           filter = { "kinds": [10002] }
 *         }
 *       ]
 *     }
 *
 * Every refresh the router runs each scan, unions the relays they name, and
 * syncs the stream filter against every one of them — nothing truncates the
 * set, [concurrency] only paces it. There is no live tail: a set this size is
 * synced on a period, not held open.
 *
 * @param sources every scan to read relay urls from, merged.
 * @param refreshSeconds how often the whole cycle runs again.
 * @param concurrency how many relays sync at the same time. A relay's sync has
 *   no wall-clock cap: every client timeout is measured from the last message,
 *   so a silent relay is dropped in seconds and a delivering one is doing the
 *   work the slot exists for.
 * @param exclude relays to skip however many sources name them.
 */
data class RelayDiscoveryConfig(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val concurrency: Int,
    val exclude: Set<NormalizedRelayUrl>,
    /**
     * How many bound `authors` go into ONE ask, and therefore into one cursor
     * band. Null keeps them all in a single filter.
     *
     * A band is keyed on its filter, so an author set that changes invalidates
     * it and re-walks that relay's history. At 1 the band is `(relay, one
     * author)` and stays valid forever — right for a small pairing like NIP-85
     * providers. An outbox stream pairing millions of authors has to chunk,
     * and accept that a chunk re-walks when its membership shifts.
     */
    val authorsPerLeg: Int? = null,
    /**
     * The most relays one event may name before the whole event is ignored as
     * a relay list, or null for no limit.
     *
     * A real NIP-65 outbox is single digits; the ones this exists for run to
     * five.
     *
     * SETTING THIS GIVES UP THE TAG PROJECTION for the whole source: a
     * per-event limit needs the event, and the projection hands back values
     * already flattened across every event it matched. That is a real cost on
     * a large store — see
     * [com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery.discover] —
     * which is why it is opt-in and null by default. Duplicate urls are
     * handled downstream by
     * [com.nosfabrica.vespa.relay.router.discovery.RelayAliases] either way;
     * this is only about refusing to read an implausible list at all.
     */
    val maxRelaysPerList: Int? = null,
)

/**
 * One scan of the store: the [selects] saying which relay urls to pull out,
 * and a NIP-01 [filter] saying which events to pull them from. The filter runs
 * once and every select is applied to what it returns, so a whole shelf of
 * relay-list kinds costs one query rather than one each.
 */
data class RelaySource(
    val selects: List<RelaySelect>,
    val filter: Filter,
)

/**
 * Where a relay url sits in a tag. Every relay list in the protocol is some
 * tag with a url at a fixed offset, so this covers all of them with no
 * per-kind code.
 *
 * @param kind apply this select only to events of that kind, or null for
 *   everything the filter returned.
 * @param tag the tag name to read, or null for any tag — at the cost of a
 *   stricter url check, see [RelayDiscovery].
 * @param index which element of the tag holds the url. 1 for nearly
 *   everything; 2 for NIP-85 service tags and `e`/`p`/`a`/`q` relay hints.
 * @param where conditions on the rest of the tag, see [TagCondition]. The
 *   config's `marker = "write" / "read" / "any"` is sugar that expands into
 *   this list.
 */
data class RelaySelect(
    val kind: Int?,
    val tag: String?,
    val index: Int,
    val where: List<TagCondition> = emptyList(),
    /**
     * Extra NIP-01 filter fields read out of the SAME tag, so the relay this
     * select found is asked only for what that tag paired it with.
     *
     * Keyed by destination — `authors`, `ids`, `kinds`, or a `#x` tag filter.
     * A value is read per TAG OCCURRENCE, not gathered into a global set:
     * collecting the slots independently would produce the cross product
     * (measured: 5,928 asks standing in for the 256 pairs that exist).
     */
    val bindings: Map<String, Slot> = emptyMap(),
)

/**
 * Where one value of a binding comes from. Usually a slot in the tag being
 * read; [EventPubkey] is what makes NIP-65's outbox model expressible — "fetch
 * THIS AUTHOR's events from the relays their own 10002 marks write".
 */
sealed interface Slot {
    /** Element [index] of the tag this select matched. */
    data class OfTag(
        val index: Int,
    ) : Slot

    /** The scanned event's own author. */
    data object EventPubkey : Slot

    /** The scanned event's own id. */
    data object EventId : Slot
}

/**
 * One alternative in a select's `where` list: entries OR together, the fields
 * inside one entry AND — NIP-01's boolean shape pointed at a tag. A tag passes
 * an empty list outright.
 *
 * [equals] is exact — no case folding, no trimming — and an element that does
 * not exist matches nothing, not even `""`. NIP-65's write side (what
 * `marker = "write"` expands to) is: marked write, marked empty, or no marker
 * slot at all.
 *
 * @param index which element [equals] tests; the parser demands both or neither.
 * @param equals the element at [index] is exactly this string.
 * @param minSize the tag has at least this many elements.
 * @param maxSize the tag has at most this many elements.
 */
data class TagCondition(
    val index: Int? = null,
    val equals: String? = null,
    val minSize: Int? = null,
    val maxSize: Int? = null,
) {
    fun matches(tag: Array<String>): Boolean {
        if (equals != null && (index == null || tag.getOrNull(index) != equals)) return false
        if (minSize != null && tag.size < minSize) return false
        if (maxSize != null && tag.size > maxSize) return false
        return true
    }
}

/** Env-level fallbacks for the per-stream dynamic-relay knobs. */
data class RelaySourceDefaults(
    val refreshSeconds: Long = 21_600,
    val concurrency: Int = 8,
)
