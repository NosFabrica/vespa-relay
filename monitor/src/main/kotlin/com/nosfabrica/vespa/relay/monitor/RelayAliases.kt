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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.DiscoveredRelay
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which discovered urls are one relay under different names, decided by comparing
 * the newest [probeTarget] events at each url. A wrong fold silently stops mirroring
 * a relay, so silence and a thin window never count as evidence.
 */
class RelayAliases(
    /** How many newest events a fingerprint holds. */
    val probeTarget: Int = DEFAULT_PROBE_TARGET,
    /** The smallest window a general filter may decide on, and the floor for every negative claim. */
    private val minSample: Int = DEFAULT_MIN_SAMPLE,
    /** The smaller floor for a window taken through [GROUP_METADATA_KINDS], a relay's complete list of groups. */
    private val groupMetadataMinSample: Int = DEFAULT_GROUP_METADATA_MIN_SAMPLE,
    /** How much of the smaller window must appear in the larger one for the two to be one relay. */
    private val minOverlap: Double = DEFAULT_MIN_OVERLAP,
    /** How much of its own window a relay must hand back on a second walk, see [reproducible]. */
    private val minSelfOverlap: Double = DEFAULT_MIN_SELF_OVERLAP,
) {
    /** Folded url -> the url dialled in its place. */
    private val folded = ConcurrentHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()

    /** Urls a probe cleared as their own relay, so they are not re-probed. */
    private val distinct = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Urls something has folded onto. */
    private val canonicals = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** The url to dial in place of this one. */
    fun canonicalOf(url: NormalizedRelayUrl): NormalizedRelayUrl = folded[url] ?: url

    fun size(): Int = folded.size

    /** Every fold held. */
    fun verdicts(): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folded.toMap()

    /**
     * Set the verdicts for [candidates] to exactly what the store holds. One url at a
     * time, because streams and the monitor read this map while it changes.
     */
    fun replace(
        candidates: Collection<NormalizedRelayUrl>,
        known: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        cleared: Set<NormalizedRelayUrl>,
    ) {
        // Chains are resolved against `known`, not the live map, so the outcome does not
        // depend on the order the store returned verdicts in.
        val ends = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>(known.size)
        for ((alias, canonical) in known) {
            if (alias == canonical) continue
            val end = endOf(known, canonical) ?: continue
            if (end == alias) continue
            ends[alias] = end
        }
        val heads = ends.values.toHashSet()
        for (url in candidates) {
            val end = ends[url]
            if (end != null) {
                folded[url] = end
                distinct -= url
            } else {
                folded.remove(url)
                // A fold outranks a cleared verdict for the same url.
                if (url in cleared) distinct += url else distinct -= url
            }
            if (url !in heads) canonicals -= url
        }
        canonicals += heads
    }

    /** The url at the end of a chain of verdicts in [known], or null for a loop. */
    private fun endOf(
        known: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        from: NormalizedRelayUrl,
    ): NormalizedRelayUrl? {
        var at = from
        repeat(MAX_CHAIN) {
            val next = known[at] ?: return at
            if (next == at) return at
            at = next
        }
        return null
    }

    /** The url at the end of a chain of live verdicts, bounded by [MAX_CHAIN]. */
    private fun resolve(url: NormalizedRelayUrl): NormalizedRelayUrl {
        var at = url
        repeat(MAX_CHAIN) {
            at = folded[at] ?: return at
        }
        return at
    }

    /** Drop every verdict held about these urls. */
    fun forget(urls: Collection<NormalizedRelayUrl>) {
        for (url in urls) {
            folded.remove(url)
            distinct.remove(url)
            canonicals.remove(url)
        }
    }

    /** What a fold has to clear. */
    private data class Bar(
        /** The smallest window either side may bring. */
        val window: Int,
        /** The fewest ids the two must have in common. Nonzero only where the window floor is too small for a ratio. */
        val shared: Int,
    )

    /** The bar for windows taken through [kinds]; null is the general filter. */
    private fun foldBar(kinds: List<Int>?): Bar =
        if (kinds == GROUP_METADATA_KINDS) {
            val floor = minOf(minSample, groupMetadataMinSample)
            Bar(window = floor, shared = floor)
        } else {
            Bar(window = minSample, shared = 0)
        }

    /** Is this window big enough to measure against, given the filter it came through? */
    fun usableWindow(
        print: Set<String>?,
        kinds: List<Int>? = null,
    ): Boolean = print != null && print.size >= foldBar(kinds).window

    /**
     * Does this relay hand back the same window twice? Gates only the negative claim
     * "these are different relays".
     */
    fun reproducible(
        first: Set<String>,
        second: Set<String>,
    ): Boolean {
        val smaller = minOf(first.size, second.size)
        if (smaller < minSample) return false
        val shared = if (first.size <= second.size) first.count { it in second } else second.count { it in first }
        return shared.toDouble() / smaller >= minSelfOverlap
    }

    /** Has anything been decided about this url? False is "no verdict", not "distinct". */
    fun measured(url: NormalizedRelayUrl): Boolean = url in distinct || url in canonicals || folded.containsKey(url)

    /** This url was probed and is nobody's duplicate. */
    fun markDistinct(url: NormalizedRelayUrl) {
        distinct += url
    }

    /**
     * The candidates grouped by hostname, keeping the groups a probe could still learn
     * from. Hostname alone, so one pass folds the path, the default port and the scheme.
     */
    fun unresolved(candidates: Collection<NormalizedRelayUrl>): List<List<NormalizedRelayUrl>> =
        candidates
            .groupBy { hostOf(it.url) }
            .values
            .map { group -> group.sortedWith(PREFERENCE) }
            .filter { group -> group.size > 1 && group.any { !measured(it) } }

    /**
     * Which urls of one group need a fingerprint: the leader, every url without a verdict,
     * and the `wss://` twin of each unmeasured `ws://` url so [schemeTwins] can pair them.
     */
    fun toProbe(group: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        val leader = leaderOf(group)
        val wanted = group.filter { it != leader && it !in distinct && !folded.containsKey(it) }
        val plain = wanted.filter { !isSecure(it.url) }
        if (plain.isEmpty()) return (listOf(leader) + wanted).distinct()
        val secure = secureTwins(group)
        val twins = plain.mapNotNull { endpointKey(it.url)?.let(secure::get) }.filter { !folded.containsKey(it) }
        if (twins.isEmpty()) return (listOf(leader) + wanted).distinct()
        return (listOf(leader) + (wanted + twins).sortedWith(PREFERENCE)).distinct()
    }

    /** What one group's fingerprints proved. */
    data class Learned(
        /** Folded url -> the url it folded onto. */
        val folded: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls found to be their own relay, the leader included. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /** The subset of [folded] decided by [schemeTwins], whose evidence is the pair of urls and not a score. */
        val twins: Set<NormalizedRelayUrl> = emptySet(),
    )

    /**
     * Fold one group against the fingerprints just taken and return what this call learned.
     * A url with no usable fingerprint is left as it was: a thin window can refuse to fold
     * but cannot prove distinctness. [leader] is the url [toProbe] fingerprinted.
     */
    fun learn(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
        /** The filter every print was taken through; null is the general filter. */
        kinds: List<Int>? = null,
    ): Learned {
        val leaderPrint = prints[leader] ?: return Learned()
        // Only sameRelay gets the filter's bar; every negative claim keeps minSample.
        val bar = foldBar(kinds)
        val folds = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val cleared = HashSet<NormalizedRelayUrl>()
        var compared = 0
        val twins = schemeTwins(group, leader, prints)
        val unmatched = ArrayList<NormalizedRelayUrl>()
        for (url in group) {
            if (url == leader || url in twins || folded.containsKey(url)) continue
            val print = prints[url] ?: continue
            if (sameRelay(leaderPrint, print, bar)) {
                compared++
                folded[url] = leader
                canonicals += leader
                distinct -= url
                folds[url] = leader
            } else if (print.size >= minSample && leaderPrint.size >= minSample) {
                compared++
                unmatched += url
            }
        }
        // Not matching the leader is not being distinct: the unmatched are compared
        // to each other, against cluster heads so a host of many endpoints stays linear.
        val heads = ArrayList<NormalizedRelayUrl>()
        for (url in unmatched) {
            val print = prints.getValue(url)
            val head = heads.firstOrNull { sameRelay(prints.getValue(it), print, bar) }
            if (head == null) {
                heads += url
                markDistinct(url)
                cleared += url
            } else {
                folded[url] = head
                canonicals += head
                distinct -= url
                folds[url] = head
            }
        }
        // Twins fold last, onto wherever the secure url itself ended up.
        for ((plain, secure) in twins) {
            val canonical = resolve(secure)
            if (canonical == plain) continue
            folded[plain] = canonical
            canonicals += canonical
            distinct -= plain
            folds[plain] = canonical
        }
        // The leader is cleared only if something was measured against it and nothing folded onto it.
        if (compared > 0 && leaderPrint.size >= minSample && leader !in canonicals) {
            markDistinct(leader)
            cleared += leader
        }
        return Learned(folds, cleared, twins.keys)
    }

    /**
     * Fold a group nothing could be read from onto its leader, on the shared hostname alone.
     * The one fold that rests on no measurement; every url must have been asked and been silent.
     */
    fun foldUnreadable(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val folds = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        for (url in group) {
            if (url == leader || folded.containsKey(url)) continue
            folded[url] = leader
            canonicals += leader
            distinct -= url
            folds[url] = leader
        }
        return folds
    }

    /**
     * `ws://x` -> `wss://x` for every pair where both answered and the plain url's window
     * is already served by the secure one. A scheme never names a different endpoint.
     */
    private fun schemeTwins(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        if (group.none { !isSecure(it.url) }) return emptyMap()
        val secure = secureTwins(group)
        if (secure.isEmpty()) return emptyMap()
        val twins = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        for (url in group) {
            // Never the leader: it is the yardstick, and canonicalOf follows one hop only.
            if (url == leader || isSecure(url.url) || folded.containsKey(url)) continue
            val twin = endpointKey(url.url)?.let { secure[it] } ?: continue
            if (twin == url) continue
            val plainPrint = prints[url] ?: continue
            val twinPrint = prints[twin] ?: continue
            if (!alreadyServedBy(plainPrint, twinPrint)) continue
            twins[url] = twin
        }
        return twins
    }

    /** Is everything in [print] already in [survivor]? An empty window has nothing to lose. */
    private fun alreadyServedBy(
        print: Set<String>,
        survivor: Set<String>,
    ): Boolean = print.isEmpty() || print.count { it in survivor }.toDouble() / print.size >= minOverlap

    /** The unfolded `ws://` url in [group] naming the same endpoint as [secure], if any. */
    fun plainTwinIn(
        group: List<NormalizedRelayUrl>,
        secure: NormalizedRelayUrl,
    ): NormalizedRelayUrl? {
        if (!isSecure(secure.url)) return null
        val key = endpointKey(secure.url) ?: return null
        return group.firstOrNull { !isSecure(it.url) && endpointKey(it.url) == key && !folded.containsKey(it) }
    }

    /** The `wss://` urls of a group, by endpoint. */
    private fun secureTwins(group: List<NormalizedRelayUrl>): Map<String, NormalizedRelayUrl> {
        val secure = HashMap<String, NormalizedRelayUrl>()
        for (url in group) {
            if (!isSecure(url.url)) continue
            endpointKey(url.url)?.let { secure.putIfAbsent(it, url) }
        }
        return secure
    }

    /** Do these two windows come from one relay? */
    private fun sameRelay(
        a: Set<String>,
        b: Set<String>,
        bar: Bar,
    ): Boolean {
        val smaller = minOf(a.size, b.size)
        if (smaller < bar.window) return false
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        if (shared < bar.shared) return false
        return shared.toDouble() / smaller >= minOverlap
    }

    /**
     * The url the rest of a group is compared against. A url already folded onto wins,
     * so a new alias cannot re-point an existing verdict; otherwise the best by [PREFERENCE].
     */
    private fun leaderOf(group: List<NormalizedRelayUrl>): NormalizedRelayUrl {
        group.firstOrNull { it in canonicals }?.let { return it }
        return group.minWith(PREFERENCE)
    }

    /** The best url of [urls] by [PREFERENCE], which stays private so every caller picks the same one. */
    fun preferred(urls: Collection<NormalizedRelayUrl>): NormalizedRelayUrl? = urls.minWithOrNull(PREFERENCE)

    companion object {
        /** How many newest events a fingerprint holds, paged backwards to reach it. */
        const val DEFAULT_PROBE_TARGET = 500

        /** Events per REQ. Asking over a relay's advertised cap risks a refusal, not a truncation. */
        const val DEFAULT_PROBE_PAGE = 500

        /** The page tried once when the first ask comes back empty, which may have been a refusal. */
        const val FALLBACK_PROBE_PAGE = 100

        const val DEFAULT_MIN_SAMPLE = 20

        /** NIP-29 group metadata, the one window a groups relay serves unscoped and unauthenticated. */
        val GROUP_METADATA_KINDS = listOf(39000)

        const val DEFAULT_GROUP_METADATA_MIN_SAMPLE = 3

        const val DEFAULT_MIN_OVERLAP = 0.5

        const val DEFAULT_MIN_SELF_OVERLAP = 0.9

        /** Apply an alias map to a discovered set. Each url's bindings move with it onto the survivor. */
        fun foldOnto(
            relays: List<DiscoveredRelay>,
            aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        ): List<DiscoveredRelay> {
            if (aliases.isEmpty()) return relays
            val merged = LinkedHashMap<NormalizedRelayUrl, MutableMap<String, MutableSet<String>>>()
            for (relay in relays) {
                val into = merged.getOrPut(aliases[relay.url] ?: relay.url) { HashMap() }
                for ((dest, values) in relay.bindings) into.getOrPut(dest) { HashSet() }.addAll(values)
            }
            return merged.map { (url, narrow) -> DiscoveredRelay(url, narrow.mapValues { (_, v) -> v.toSet() }) }
        }

        private const val MAX_CHAIN = 8

        /**
         * Which url of a group to keep, best first: no path, then `wss`, then no explicit port,
         * then the shortest, then the url itself so the order is total.
         */
        private val PREFERENCE =
            compareBy<NormalizedRelayUrl>(
                { if (pathOf(it.url).isEmpty()) 0 else 1 },
                { if (it.url.startsWith("wss://", true)) 0 else 1 },
                { if (hasExplicitPort(it.url)) 1 else 0 },
                { it.url.length },
                { it.url },
            )

        /** The hostname, lowercased, without the port. */
        fun hostOf(url: String): String {
            val authority = afterScheme(url).substringBefore('/')
            // An IPv6 literal keeps its brackets; only a colon after the bracket is the port separator.
            val host = if (authority.startsWith("[")) authority.substringBefore(']') + "]" else authority.substringBefore(':')
            return host.lowercase()
        }

        /** The path without its leading slash; empty for a bare host. */
        fun pathOf(url: String): String = afterScheme(url).substringAfter('/', "").trim('/')

        private fun isSecure(url: String): Boolean = url.startsWith("wss://", true)

        /**
         * `host/path` without the scheme, so `ws://x/p` and `wss://x/p` share a key. Null for
         * an explicit non-default port, which is a deliberately chosen second endpoint.
         */
        private fun endpointKey(url: String): String? {
            if (!isDefaultPort(url)) return null
            return hostOf(url) + "/" + pathOf(url)
        }

        /** No port at all, or the one this url's scheme implies. */
        private fun isDefaultPort(url: String): Boolean {
            val authority = afterScheme(url).substringBefore('/')
            val port =
                if (authority.startsWith("[")) authority.substringAfter(']').removePrefix(":") else authority.substringAfter(':', "")
            return port.isEmpty() || port == (if (isSecure(url)) "443" else "80")
        }

        private fun hasExplicitPort(url: String): Boolean {
            val authority = afterScheme(url).substringBefore('/')
            return if (authority.startsWith("[")) authority.substringAfter(']').startsWith(":") else authority.contains(':')
        }

        private fun afterScheme(url: String): String =
            when {
                url.startsWith("wss://", true) -> url.substring(6)
                url.startsWith("ws://", true) -> url.substring(5)
                else -> url
            }
    }
}
