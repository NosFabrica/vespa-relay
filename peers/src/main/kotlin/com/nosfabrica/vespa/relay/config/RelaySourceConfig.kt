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
package com.nosfabrica.vespa.relay.config

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer

/**
 * A stream's relay list, read from events our own store holds instead of a
 * hand-written `urls` array. `relaySource` lists places to read urls from,
 * merged into one fan-out:
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
 * gates them on the monitor's verdicts, and asks; nothing truncates the set.
 *
 * @param gatedBy the urls this stream may dial at all; every source's
 *   discovery is intersected with what these find. Empty gates nothing.
 * @param refreshSeconds how often a source's relay list is re-read from the
 *   store. A source may set its own shorter one, see [RelaySource.refreshSeconds].
 * @param exclude relays to skip however many sources name them, see [RelayExcludes].
 */
data class RelayDiscoveryConfig(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val exclude: RelayExcludes,
    val gatedBy: List<RelaySource> = emptyList(),
    /**
     * The most relays one event may name before the whole event is ignored as
     * a relay list, or null for no limit. A real NIP-65 outbox is single digits.
     */
    val maxRelaysPerList: Int? = null,
)

/**
 * A stream's `exclude` list compiled. An entry with a regex metacharacter
 * (any of `\ [ ] ( ) { } * + ?  | ^ $`; a dot does not count) is a regex that
 * must match the whole normalized url, ignoring case, trailing slash optional.
 * Anything else is a plain url, normalized like a `urls` entry and matched by
 * equality, so `"wss://purplepag.es"` can never take out a look-alike host.
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
         * Compile [raw]. A plain url the normalizer refuses is dropped and
         * reported through [onInvalidUrl]; a broken regex throws for the caller
         * to name its stream.
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
 * Drops the scheme's default port (`:443` on `wss`, `:80` on `ws`), which the
 * normalizer keeps, so a list naming both spellings costs one dial. Applied to
 * every discovered url and to [RelayExcludes]' plain entries alike.
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
 * One entry of a stream's `relaySource` list: the [selects] saying which relay
 * urls to pull out, and a NIP-01 [filter] saying which events to pull them
 * from. The filter runs once and every select is applied to what it returns.
 *
 * A monitor's verdicts are the same shape, not a second kind of thing:
 * `{ "kinds": [30166], "#l": ["prime"] }` with the `d`-tag select the loader
 * supplies. Nothing here knows that `l` is the tag or `prime` the value;
 * another monitor's spelling needs only a different filter.
 */
data class RelaySource(
    /**
     * Where the url sits in what [filter] returns. Defaults to the `d` tag of a
     * kind-30166 record, the one position the protocol fixes.
     */
    val selects: List<RelaySelect>,
    val filter: Filter,
    /**
     * How recently the event must have been published, or null for no bound;
     * becomes `since = now - maxAgeSeconds` at read time. Unbounded by default:
     * a relay list is timeless, a monitor's verdict goes stale, and which of
     * those a filter asks for is the operator's knowledge, not ours.
     * [DEFAULT_MAX_AGE_SECONDS] is a number to reach for, not one anything infers.
     */
    val maxAgeSeconds: Long? = null,
    /**
     * How often to re-read this source from the store, or null for the
     * stream's [RelayDiscoveryConfig.refreshSeconds]. Set it short on the cheap
     * reads: a kind-30166 query cached six hours holds a newly-verdicted relay
     * out of the fan-out for six hours.
     */
    val refreshSeconds: Long? = null,
) {
    companion object {
        /**
         * Two of the monitor's 6h sweeps plus slack: one missed sweep must not
         * empty a stream's relay list, and three missed sweeps should.
         */
        const val DEFAULT_MAX_AGE_SECONDS = 14 * 60 * 60L
    }
}

/**
 * Where a relay url sits in a tag. Every relay list in the protocol is some
 * tag with a url at a fixed offset, so this covers all of them.
 *
 * @param kind apply this select only to events of that kind, or null for all.
 * @param tag the tag name to read, or null for any tag at the cost of a
 *   stricter url check, see [RelayDiscovery].
 * @param urlIndex which element of the tag holds the url. 1 for nearly
 *   everything; 2 for NIP-85 service tags and `e`/`p`/`a`/`q` relay hints.
 * @param where conditions on the rest of the tag, see [TagCondition]. The
 *   config's `marker = "write" / "read" / "any"` expands into this list.
 */
data class RelaySelect(
    val kind: Int?,
    val tag: String?,
    val urlIndex: Int,
    val where: List<TagCondition> = emptyList(),
    /**
     * Extra NIP-01 filter fields read out of the same tag, keyed by
     * destination (`authors`, `ids`, `kinds`, or a `#x` tag filter). A value is
     * read per tag occurrence, never gathered into a global set: collecting the
     * slots independently would produce the cross product.
     */
    val bindings: Map<String, BindingSlot> = emptyMap(),
)

/**
 * Where one value of a binding comes from. [EventPubkey] is what makes the
 * outbox model expressible: this author's events from their own write relays.
 */
sealed interface BindingSlot {
    /** Element [index] of the tag this select matched. */
    data class OfTag(
        val index: Int,
    ) : BindingSlot

    /** The scanned event's own author. */
    data object EventPubkey : BindingSlot

    /** The scanned event's own id. */
    data object EventId : BindingSlot
}

/**
 * One alternative in a select's `where` list: entries OR together, the fields
 * inside one entry AND. A tag passes an empty list outright. [equals] is
 * exact, and an element that does not exist matches nothing, not even `""`.
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

/** Env-level fallback for a stream's discovery `refreshSeconds`. */
data class RelaySourceDefaults(
    val refreshSeconds: Long = 21_600,
)
