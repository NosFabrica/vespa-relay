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
 * A stream's relay list, read from events our own store holds instead of a hand-written
 * `urls` array. Every refresh the pool re-derives each source, unions the relays they name,
 * gates them on [gatedBy] and asks; nothing truncates the set.
 *
 * @param gatedBy the urls this stream may dial at all. Empty gates nothing.
 * @param refreshSeconds how often a source's relay list is re-read; a source may set a shorter one.
 * @param exclude relays to skip however many sources name them, see [RelayExcludes].
 */
data class RelayDiscoveryConfig(
    val sources: List<RelaySource>,
    val refreshSeconds: Long,
    val exclude: RelayExcludes,
    val gatedBy: List<RelaySource> = emptyList(),
    /** The most relays one event may name before the whole event is ignored, or null for no limit. */
    val maxRelaysPerList: Int? = null,
)

/**
 * A stream's `exclude` list compiled. An entry with a regex metacharacter (a dot does not
 * count) is a regex that must match the whole normalized url, ignoring case, trailing slash
 * optional; anything else is a plain url matched by equality.
 */
class RelayExcludes(
    val urls: Set<NormalizedRelayUrl>,
    val patterns: List<Regex>,
) {
    operator fun contains(url: NormalizedRelayUrl): Boolean {
        if (url in urls) return true
        if (patterns.isEmpty()) return false
        val bare = url.url.removeSuffix("/")
        return patterns.any { it.matches(url.url) || it.matches(bare) }
    }

    companion object {
        val NONE = RelayExcludes(emptySet(), emptyList())

        private const val REGEX_MARKERS = "\\[](){}*+?|^\$"

        /** Compile [raw]. An unparseable plain url goes to [onInvalidUrl]; a broken regex throws. */
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
 * Drops the scheme's default port (`:443` on `wss`, `:80` on `ws`), which the normalizer
 * keeps, so a list naming both spellings costs one dial.
 */
internal fun withoutDefaultPort(url: NormalizedRelayUrl): NormalizedRelayUrl {
    val default = if (url.url.startsWith("wss://", true)) ":443" else ":80"
    val scheme = url.url.substringBefore("://", "") + "://"
    val rest = url.url.removePrefix(scheme)
    val authority = rest.substringBefore('/')
    // An IPv6 literal's colons live inside brackets; only a port sits after the closing one.
    if (authority.startsWith("[") && !authority.substringAfter(']').startsWith(":")) return url
    if (!authority.endsWith(default)) return url
    val trimmed = scheme + authority.removeSuffix(default) + rest.substring(authority.length)
    return RelayUrlNormalizer.normalizeOrNull(trimmed) ?: url
}

/**
 * One entry of a stream's `relaySource` list: the [selects] saying which relay urls to pull
 * out of what the NIP-01 [filter] returns. A monitor's verdicts are the same shape with the
 * `d`-tag select the loader supplies; nothing here knows the tag or the value.
 */
data class RelaySource(
    /** Where the url sits in what [filter] returns; defaults to the `d` tag of a kind-30166 record. */
    val selects: List<RelaySelect>,
    val filter: Filter,
    /**
     * How recently the event must have been published, or null for no bound; becomes
     * `since = now - maxAgeSeconds` at read time. Nothing infers it from the filter.
     */
    val maxAgeSeconds: Long? = null,
    /**
     * How often to re-read this source, or null for the stream's
     * [RelayDiscoveryConfig.refreshSeconds]. Set it short on a verdict query, or a new verdict
     * waits out the stream's period.
     */
    val refreshSeconds: Long? = null,
) {
    companion object {
        /** Two of the monitor's default sweeps plus slack, so one missed sweep does not empty a relay list. */
        const val DEFAULT_MAX_AGE_SECONDS = 14 * 60 * 60L
    }
}

/**
 * Where a relay url sits in a tag; every relay list in the protocol is some tag with a url at
 * a fixed offset.
 *
 * @param kind apply this select only to events of that kind, or null for all.
 * @param tag the tag name to read, or null for any tag at the cost of a stricter url check.
 * @param urlIndex which element holds the url: 1 for nearly everything, 2 for NIP-85 service
 *   tags and `e`/`p`/`a`/`q` relay hints.
 * @param where conditions on the rest of the tag; the config's `marker` expands into this list.
 */
data class RelaySelect(
    val kind: Int?,
    val tag: String?,
    val urlIndex: Int,
    val where: List<TagCondition> = emptyList(),
    /**
     * Extra NIP-01 filter fields read out of the same tag, keyed by destination (`authors`,
     * `ids`, `kinds`, or a `#x` tag filter). Read per tag occurrence, never as independent sets.
     */
    val bindings: Map<String, BindingSlot> = emptyMap(),
)

/** Where one value of a binding comes from. [EventPubkey] is what makes the outbox model expressible. */
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
 * One alternative in a select's `where` list: entries OR together, the fields inside one
 * entry AND. [equals] is exact, and a missing element matches nothing, not even `""`.
 *
 * @param index which element [equals] tests; the parser demands both or neither.
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
