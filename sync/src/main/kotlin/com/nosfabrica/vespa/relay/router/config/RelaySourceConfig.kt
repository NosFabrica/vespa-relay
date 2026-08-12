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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer

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
 * @param refreshSeconds how often the relay list is READ OUT OF THE STORE
 *   again. With [recycleSeconds] unset this is also the cycle period, because
 *   every cycle rediscovers.
 * @param concurrency how many relays sync at the same time. A relay's sync has
 *   no wall-clock cap: every client timeout is measured from the last message,
 *   so a silent relay is dropped in seconds and a delivering one is doing the
 *   work the slot exists for.
 * @param exclude patterns for relays to skip however many sources name them —
 *   see [RelayExcludes] for how an entry matches.
 */
data class RelayDiscoveryConfig(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val concurrency: Int,
    val exclude: RelayExcludes,
    /**
     * How soon after a pass finishes HANDING OUT its relay list the next one
     * starts, on the list the last one used. Null keeps the old behaviour
     * exactly: one pass per [refreshSeconds], each of them rediscovering.
     *
     * **"Finishes handing out" is not "finishes".** A pass ends when the walk
     * has given its last url to a worker; the slow relays keep their slots and
     * run on into the next pass, which is the entire point of the rotation. So
     * this is a FLOOR ON THE GAP between laps and not a cycle period — the
     * period is the walk plus this, and the walk is paced by the worker pool
     * rather than by any clock. Measured on live relays, 18,687 urls took 26:29
     * to hand out; against that a 30-second gap is a tail. Setting it to 5 does
     * not buy a five-second cycle, it buys a five-second pause between laps as
     * long as the network makes them.
     *
     * Two consequences worth knowing before tuning it. A pass whose whole list
     * is still busy hands out NOTHING and ends immediately — ordinary on a short
     * list, where every url is still with a worker from the last pass — and the
     * loop then ticks at this interval until slots free, which is why the floor
     * is seconds rather than zero. And the walk can still block, at the
     * admission gate: a stream stops only when every one of its 128–512 slots is
     * held at once, where the join this replaced needed exactly one straggler.
     *
     * The two knobs answer different questions, and conflating them is what
     * made a 6h refresh mean 6h of idling. Deriving the fan-out set is a store
     * walk (every relay-list event, or the tag projection over them), a
     * normalisation pass over tens of thousands of strings, and one `#d` query
     * per 500 urls to read the alias verdicts back — minutes on a full store,
     * paid to produce a list that is nearly identical to the previous cycle's.
     * The DOWNLOAD is what should repeat often; the derivation is what should
     * not. So the list is held in memory for [refreshSeconds], and every pass
     * inside that window runs on it.
     *
     * What it does NOT stale, because neither is derived here: the NIP-66
     * known-dead set is re-read at the top of every pass from the monitor's own
     * memory, and host strikes are pass-local and rebuilt each time. A cached
     * list is a list of urls to consider, not a decision to dial them.
     *
     * The floor is 5s rather than the 60s [refreshSeconds] carries: this paces
     * the pause between laps, not a store walk, and "start again shortly" is the
     * whole point of setting it.
     */
    val recycleSeconds: Long? = null,
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
) {
    /**
     * How long the stream sleeps after a cycle it actually ran — the recycle
     * gap where one is configured, the refresh period otherwise.
     *
     * One property because it is also what the router SAYS it will do ("next in
     * Ns", `Phase.Idle`), and a countdown that names a different number from the
     * one the loop sleeps is worse than no countdown.
     */
    val nextCycleSeconds: Long get() = recycleSeconds ?: refreshSeconds
}

/**
 * A stream's `exclude` list compiled. Two kinds of entry, told apart by
 * whether the text carries a regex metacharacter — any of `\ [ ] ( ) { } * + ?
 * | ^ $`; a dot does NOT count, urls are full of them:
 *
 * - A plain url is normalized exactly like a `urls` entry and excluded by
 *   EQUALITY with a discovered url's normalized form. That makes it exact —
 *   `"wss://purplepag.es"` can never take out a look-alike host such as
 *   `wss://purplepagXes/`, which its dots read as a regex would — while still
 *   matching every spelling a config could use (`purplepag.es`,
 *   `wss://PURPLEPAG.ES:443`), because both sides normalize to one string.
 * - Everything else is a regex that must match the WHOLE normalized url,
 *   ignoring case, with the url's trailing slash optional:
 *   `"wss://filter.nostr.wine/npub.*"` drops every per-user url that host
 *   mints (`wss://filter.nostr.wine/npub1…`) without touching the relay
 *   itself — a shape no literal list can keep up with, because every relay
 *   list in the wild names a different one.
 */
class RelayExcludes(
    val urls: Set<NormalizedRelayUrl>,
    val patterns: List<Regex>,
) {
    operator fun contains(url: NormalizedRelayUrl): Boolean {
        if (url in urls) return true
        if (patterns.isEmpty()) return false
        // Hoisted: the slash-stripped spelling is per url, not per pattern.
        val bare = url.url.removeSuffix("/")
        return patterns.any { it.matches(url.url) || it.matches(bare) }
    }

    companion object {
        val NONE = RelayExcludes(emptySet(), emptyList())

        private const val REGEX_MARKERS = "\\[](){}*+?|^\$"

        /**
         * Compile [raw], reporting each plain url the normalizer refuses
         * through [onInvalidUrl] (the entry is dropped, matching what a
         * `urls` list does with it), and letting a broken regex throw for
         * the caller to name its stream.
         */
        fun parse(
            raw: List<String>,
            onInvalidUrl: (String) -> Unit = {},
        ): RelayExcludes {
            val urls = LinkedHashSet<NormalizedRelayUrl>()
            val patterns = ArrayList<Regex>()
            for (entry in raw) {
                if (entry.any { it in REGEX_MARKERS }) {
                    patterns += Regex(entry, RegexOption.IGNORE_CASE)
                } else {
                    RelayUrlNormalizer.normalizeOrNull(entry)?.let { urls += withoutDefaultPort(it) } ?: onInvalidUrl(entry)
                }
            }
            return RelayExcludes(urls, patterns)
        }
    }
}

/**
 * `wss://relay/` and `wss://relay:443/` are the same url written two ways,
 * and the normalizer keeps both — so a relay list naming both costs two
 * dials, two cursor bands and two sets of NIP-66 records for one server.
 * Measured on this store: 861 discovered urls carried a redundant default
 * port and 362 of them duplicated a portless url already in the set.
 *
 * Done here rather than left to
 * [com.nosfabrica.vespa.relay.router.discovery.RelayAliases] because it needs
 * no evidence: 443 on `wss` and 80 on `ws` are the scheme's own default, and
 * dropping them is what every URL parser already does. The fold is for urls
 * that only MEASUREMENT can prove equal.
 *
 * Applied to every discovered url AND to [RelayExcludes]' plain entries, so
 * an exclude typed with the redundant port still meets the url discovery
 * hands out.
 */
internal fun withoutDefaultPort(url: NormalizedRelayUrl): NormalizedRelayUrl {
    val default = if (url.url.startsWith("wss://", true)) ":443" else ":80"
    val scheme = url.url.substringBefore("://", "") + "://"
    val rest = url.url.removePrefix(scheme)
    val authority = rest.substringBefore('/')
    // An IPv6 literal's colons live inside brackets; only a port sits after
    // the closing one, so anything unbracketed is checked as written.
    if (authority.startsWith("[") && !authority.substringAfter(']').startsWith(":")) return url
    if (!authority.endsWith(default)) return url
    val trimmed = scheme + authority.removeSuffix(default) + rest.substring(authority.length)
    return RelayUrlNormalizer.normalizeOrNull(trimmed) ?: url
}

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
    /** Null keeps a rediscovery per cycle — see [RelayDiscoveryConfig.recycleSeconds]. */
    val recycleSeconds: Long? = null,
)
