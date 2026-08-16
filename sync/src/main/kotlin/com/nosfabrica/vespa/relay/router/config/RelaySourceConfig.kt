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
 *       relaySource = [
 *         {
 *           select = [ { kind = 10002, tag = "r", marker = "write" } ]
 *           filter = { "kinds": [10002] }
 *         }
 *       ]
 *     }
 *
 * Every refresh the pool re-derives each scan, unions the relays they name,
 * gates them on the monitor's verdicts, and asks — nothing truncates the set.
 *
 * @param sources every source to read relay urls from, merged: kind-30166
 *   verdict sources and `certified`-gated scans.
 * @param refreshSeconds how often a scan's relay list is READ OUT OF THE
 *   STORE again — deriving one is a store walk, so the pool caches it this
 *   long. The verdict half rebuilds on its own freshness clock.
 * @param exclude patterns for relays to skip however many sources name them —
 *   see [RelayExcludes] for how an entry matches.
 */
data class RelayDiscoveryConfig(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val exclude: RelayExcludes,
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
    /** Every source that consults the monitor's kind-30166 verdicts — see [RelaySource.verdicts]. */
    val verdictSources: List<VerdictSource> get() = sources.mapNotNull { it.verdicts }

    /** Every source that scans relay-list events with selects — the certified-scan half. */
    val scanSources: List<RelaySource> get() = sources.filter { it.verdicts == null }
}

/**
 * A relaySource that consults the monitor's own NIP-66 records: the verified
 * read behind a `relaySource` entry whose filter asks for kind 30166.
 *
 * The knob is how stale a verdict may be and still admit its relay. Freshness
 * is read off the VERDICT TAG's own measured-at stamp, never the record's
 * `createdAt` — quartz's passive monitor rewrites the record on every
 * connection it opens, so `createdAt` says "we talked recently", which for a
 * relay in the fan-out is always true and for a verdict is no evidence at all.
 * A stale `syncable` is no verdict: the relay simply waits for the monitor's
 * next sweep, the same as a url the monitor has never seen.
 */
data class VerdictSource(
    val maxAgeSeconds: Long = DEFAULT_MAX_AGE_SECONDS,
    /**
     * Whose verdicts to trust, as 64-char lowercase hex — decoded from the
     * `authors = ["npub1…"]` the operator wrote. EMPTY means this process's
     * own signer, which is the single-process deployment where the monitor
     * and the router share one identity and the operator has nothing to copy.
     * Named explicitly, it is a deliberate trust statement: the deployment
     * where the monitor runs as its own process under its own key, and every
     * router consuming its verdicts writes that key here.
     */
    val authors: List<String> = emptyList(),
) {
    companion object {
        /**
         * Two of the monitor's 6h sweeps plus slack: one missed sweep must not
         * empty a stream's relay list, and three missed sweeps is a monitor
         * whose silence SHOULD empty it — mirroring off verdicts nobody is
         * re-taking is how a dead relay gets dialled for a month.
         */
        const val DEFAULT_MAX_AGE_SECONDS = 14 * 60 * 60L
    }
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
 * One entry of a stream's `relaySource` list, in one of two shapes.
 *
 * A SCAN: the [selects] saying which relay urls to pull out, and a NIP-01
 * [filter] saying which events to pull them from. The filter runs once and
 * every select is applied to what it returns, so a whole shelf of relay-list
 * kinds costs one query rather than one each.
 *
 * A VERDICT SOURCE ([verdicts] set): the filter asks for the monitor's own
 * kind-30166 records — `{ "kinds": [30166], "#s": ["syncable"] }` — and the
 * relay list is every url whose record carries a fresh `syncable` from OUR
 * monitor identity. No selects: NIP-66 fixes the url in the `d` tag, and the
 * read is the VERIFIED path ([discovery.RelayDiscovery.syncable] — epoch,
 * measured-at freshness, the one admitting value), not a generic tag scan; a
 * generic scan would admit a verdict whose evidence rules have since changed,
 * or one nobody has re-taken for a month. This is not a gate in front of a
 * source — it IS the source: the monitor earns the verdicts on its own clock
 * and the stream's discovery collapses to one indexed query.
 */
data class RelaySource(
    val selects: List<RelaySelect>,
    val filter: Filter,
    val verdicts: VerdictSource? = null,
    /**
     * A LIVENESS GATE on a scan: keep only the discovered urls that ALSO hold
     * a fresh `syncable` verdict — the monitor's, or the identity the block
     * names. Intersection, never union: the scan supplies the pairing (which
     * relay, narrowed to which authors) and the verdicts supply the right to
     * be dialled at all.
     *
     * This is what makes an author-bound source safe at scale. A 10040 is as
     * writable as a 10002 — the same dead hosts and spammed urls, multiplied
     * by every future user — and without the gate each of them costs the
     * stream a dial and a timeout per cycle, forever. With it, an uncertified
     * url waits exactly as a new relay does: the monitor's fast lane probes
     * it within minutes, and its first `syncable` is its admission. Meaningless
     * beside [verdicts] — a verdict source IS certified.
     */
    val certified: VerdictSource? = null,
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

/** Env-level fallback for the one per-stream discovery knob that survived the pool. */
data class RelaySourceDefaults(
    val refreshSeconds: Long = 21_600,
)
