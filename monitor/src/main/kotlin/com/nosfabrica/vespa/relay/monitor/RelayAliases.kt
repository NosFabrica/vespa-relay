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
 * Which discovered urls are the same relay wearing a different url.
 *
 * Most relay software serves its websocket on every path, so a relay list can
 * mint `wss://nos.lol/alpha` beside `wss://nos.lol` without limit, and every
 * one of them answers. The duplicate is only visible in what comes back: the
 * newest [probeTarget] events at each url, paged from one shared anchor. Two
 * urls whose newest window is the same window are the same relay.
 *
 * The verdict is a measurement, which is what makes it publishable as a
 * NIP-66 record ([RelayVerdictRecord]). Folding is deliberately hard to
 * trigger, because a wrong fold silently stops mirroring a relay; silence and
 * a thin window are never evidence of anything.
 */
class RelayAliases(
    /** How deep a fingerprint walks. The whole window goes to ingest, so the probe is also a sync. */
    val probeTarget: Int = DEFAULT_PROBE_TARGET,
    /**
     * The smallest window a general filter may decide on, and the floor every
     * negative claim uses whatever the filter was. See [groupMetadataMinSample].
     */
    private val minSample: Int = DEFAULT_MIN_SAMPLE,
    /**
     * The floor for a window taken through [GROUP_METADATA_KINDS] alone: a
     * NIP-29 relay's complete list of groups, which is short. It is both halves
     * of the [Bar] at that width. See [foldBar].
     */
    private val groupMetadataMinSample: Int = DEFAULT_GROUP_METADATA_MIN_SAMPLE,
    /**
     * How much of the smaller window must appear in the larger one. Containment
     * rather than a symmetric ratio: one dial may be cut short by the peer's
     * `default_limit`, and a truncated window is still the same window.
     */
    private val minOverlap: Double = DEFAULT_MIN_OVERLAP,
    /** How much of its own window a relay must hand back on a second walk. See [reproducible]. */
    private val minSelfOverlap: Double = DEFAULT_MIN_SELF_OVERLAP,
) {
    /** alias url -> the url we actually dial for it. Only ever holds folded urls. */
    private val folded = ConcurrentHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()

    /** Urls a probe cleared as their own relay. Without this the non-duplicates are re-probed forever. */
    private val distinct = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** The urls something has folded onto. Its own set because [leaderOf] asks per group per cycle. */
    private val canonicals = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** The url to dial in place of this one. */
    fun canonicalOf(url: NormalizedRelayUrl): NormalizedRelayUrl = folded[url] ?: url

    fun size(): Int = folded.size

    /** Every verdict held, for the monitor to publish and a restart to reload. */
    fun verdicts(): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folded.toMap()

    /**
     * Set this candidate set's verdicts to exactly what the store holds, one
     * url at a time so no url is ever transiently without its fold. This map
     * is shared by every stream and the monitor's pass, all running at once.
     *
     * Chains are resolved against [known] rather than the live map, so the
     * result does not depend on the order the store returned verdicts in, and
     * a cycle resolves to nothing.
     */
    fun replace(
        candidates: Collection<NormalizedRelayUrl>,
        known: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        cleared: Set<NormalizedRelayUrl>,
    ) {
        // Every fold resolved to the url the events are actually at, before
        // anything is written, so the fan-out never dials through two hops.
        val ends = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>(known.size)
        for ((alias, canonical) in known) {
            if (alias == canonical) continue
            val end = endOf(known, canonical) ?: continue
            // A url pinned as its own duplicate would be `measured` forever
            // and never revisited.
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
                // A fold is the stronger statement, so it wins over a cleared
                // verdict for the same url.
                if (url in cleared) distinct += url else distinct -= url
            }
            // A canonical usually has no record of its own; it is a canonical
            // only because something else points at it.
            if (url !in heads) canonicals -= url
        }
        canonicals += heads
    }

    /**
     * The url at the end of a chain of verdicts in [known], or null for a loop.
     * Kept apart from [resolve]: this reads the store's answer while the live
     * map is mid-update, that one reads the live map while learning.
     */
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

    /**
     * Follow a chain of verdicts through the live map to its end. Bounded so a
     * hand-edited verdict, or two runs that disagreed, cannot spin here.
     */
    private fun resolve(url: NormalizedRelayUrl): NormalizedRelayUrl {
        var at = url
        repeat(MAX_CHAIN) {
            at = folded[at] ?: return at
        }
        return at
    }

    /**
     * Drop every verdict held about these urls. The store is the record and
     * this map is a cache of it; a cache with no expiry outlives what it caches.
     */
    fun forget(urls: Collection<NormalizedRelayUrl>) {
        for (url in urls) {
            folded.remove(url)
            distinct.remove(url)
            canonicals.remove(url)
        }
    }

    /** What a fold has to clear, in the two places a thin window can cheat. */
    private data class Bar(
        /** The smallest window either side may have brought. */
        val window: Int,
        /**
         * The fewest ids the two must actually have in common. Zero for a
         * general window, where [minOverlap] alone decides; nonzero once the
         * window floor is small, because a ratio cannot refuse a two-id
         * coincidence.
         */
        val shared: Int,
    )

    /**
     * What a fold must clear, given the filter that produced the windows. A
     * [GROUP_METADATA_KINDS] window is a relay's complete list of groups, so
     * its floor drops and [Bar.shared] rises to meet it. `minOf` so a caller
     * that set [minSample] below the group floor keeps its own number.
     */
    private fun foldBar(kinds: List<Int>?): Bar =
        if (kinds == GROUP_METADATA_KINDS) {
            val floor = minOf(minSample, groupMetadataMinSample)
            Bar(window = floor, shared = floor)
        } else {
            Bar(window = minSample, shared = 0)
        }

    /**
     * Is this window big enough to be measured against at all? [kinds] is the
     * filter it came through; null is the general one and the strict floor.
     */
    fun usableWindow(
        print: Set<String>?,
        kinds: List<Int>? = null,
    ): Boolean = print != null && print.size >= foldBar(kinds).window

    /**
     * Does this relay give the same answer twice: two walks of one url from one
     * anchor, compared at [minSelfOverlap].
     *
     * It gates only the negative claim "these are different relays". Noise in
     * the yardstick drives containment down, so a sibling that still clears
     * [minOverlap] did so in spite of it; the same noise pushes urls over the
     * bar in the other direction for free.
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

    /**
     * Has anything been decided about this url: folded, cleared, or folded
     * onto? False is "no verdict", not "distinct", and such a url must still
     * be dialled.
     */
    fun measured(url: NormalizedRelayUrl): Boolean = url in distinct || url in canonicals || folded.containsKey(url)

    /** This url was probed and is nobody's duplicate. Never fingerprint it again. */
    fun markDistinct(url: NormalizedRelayUrl) {
        distinct += url
    }

    /**
     * The candidate urls grouped by hostname, keeping only the groups a probe
     * could still learn something from: more than one member, and at least
     * one without a verdict.
     *
     * Hostname alone, not `host:port`, so one pass folds the path, the
     * redundant default port and the scheme. "Has a verdict" is [measured] and
     * nothing narrower: a leader everything folded onto is a canonical, which
     * a narrower test would keep returning as unfinished.
     */
    fun unresolved(candidates: Collection<NormalizedRelayUrl>): List<List<NormalizedRelayUrl>> =
        candidates
            .groupBy { hostOf(it.url) }
            .values
            .map { group -> group.sortedWith(PREFERENCE) }
            .filter { group -> group.size > 1 && group.any { !measured(it) } }

    /**
     * Which urls of one group still need a fingerprint: the ones with no
     * verdict, the group's [leaderOf] (re-measured because the window it is
     * compared to has moved), and the secure twin of any unmeasured `ws://`
     * url, since [schemeTwins] can only fold a pair that both answered this
     * pass.
     */
    fun toProbe(group: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        val leader = leaderOf(group)
        val wanted = group.filter { it != leader && it !in distinct && !folded.containsKey(it) }
        // The common case, out before anything is allocated.
        val plain = wanted.filter { !isSecure(it.url) }
        if (plain.isEmpty()) return (listOf(leader) + wanted).distinct()
        val secure = secureTwins(group)
        // A twin that is itself folded already points somewhere else.
        val twins = plain.mapNotNull { endpointKey(it.url)?.let(secure::get) }.filter { !folded.containsKey(it) }
        if (twins.isEmpty()) return (listOf(leader) + wanted).distinct()
        // Sorted back into PREFERENCE order, which is what the yardstick search
        // walks down; a `wss://` twin is the best yardstick the group has.
        return (listOf(leader) + (wanted + twins).sortedWith(PREFERENCE)).distinct()
    }

    /** What one group's fingerprints proved: the folds, and the urls cleared. */
    data class Learned(
        /** Folded url -> the leader it folded onto. */
        val folded: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls compared and found to be their own relay, the leader included. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /**
         * The subset of [folded] decided by [schemeTwins]. Their evidence is
         * the pair of urls, not a containment score, and the record must not
         * quote a number the verdict was not based on.
         */
        val twins: Set<NormalizedRelayUrl> = emptySet(),
    )

    /**
     * Fold one group against the fingerprints just taken, and return only what
     * this call learned.
     *
     * A url with no fingerprint, or one under [minSample], is left as it was:
     * refusing to fold and proving distinctness are different claims, and only
     * the first can be made from a thin window. The leader is cleared too when
     * nothing folded onto it, or the group would come back forever.
     *
     * [leader] is passed in rather than recomputed: [leaderOf] reads
     * `canonicals`, which concurrent passes mutate, and the yardstick must be
     * the url [toProbe] fingerprinted.
     */
    fun learn(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
        /** The filter every print was taken through; null is the general filter. */
        kinds: List<Int>? = null,
    ): Learned {
        val leaderPrint = prints[leader] ?: return Learned()
        // Folding and clearing do not get the same floor. Only [sameRelay] is
        // given the filter's bar; every path that writes a negative claim
        // keeps [minSample].
        val bar = foldBar(kinds)
        val folds = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val cleared = HashSet<NormalizedRelayUrl>()
        var compared = 0
        // Held out of both passes below: a `ws://`/`wss://` pair is not a
        // question for the containment test. See [schemeTwins].
        val twins = schemeTwins(group, leader, prints)
        // Measured against the leader and found to be something else. Not yet
        // a verdict: the leader is one endpoint on this host, not the only one.
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
        // Then against each other: "not the leader" is not "its own relay" on
        // a host whose preferred url is a different endpoint (`/inbox`).
        // Against the cluster heads rather than every pair, so a host of many
        // distinct endpoints stays linear. PREFERENCE order makes the best url
        // of each cluster its head, and `sameRelay` is symmetric.
        val heads = ArrayList<NormalizedRelayUrl>()
        for (url in unmatched) {
            val print = prints.getValue(url)
            // [bar] is moot here: entry to `unmatched` already demanded
            // [minSample] on both sides.
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
        // Only now the scheme twins, onto wherever the secure url itself ended
        // up: [canonicalOf] is one hop, so a chain here is a surviving duplicate.
        for ((plain, secure) in twins) {
            val canonical = resolve(secure)
            // Two disagreeing verdicts could walk the chain back to the url
            // being folded, which would pin it as its own duplicate.
            if (canonical == plain) continue
            folded[plain] = canonical
            canonicals += canonical
            distinct -= plain
            folds[plain] = canonical
        }
        // Only when something was held up against it, and only when nothing
        // folded onto it. The twin folds land above this check so a group of
        // nothing but a scheme pair still gives its leader a verdict.
        if (compared > 0 && leaderPrint.size >= minSample && leader !in canonicals) {
            markDistinct(leader)
            cleared += leader
        }
        return Learned(folds, cleared, twins.keys)
    }

    /**
     * Fold a group nothing could be read from onto its preferred survivor, on
     * the shared hostname alone.
     *
     * The only fold here that rests on no measurement: a policy, not a verdict.
     * A url nothing can be read from is mirroring nothing, so nothing is lost
     * today; what is lost is the day the relay starts answering, until the
     * verdict expires. Every url must have answered, or our own outage becomes
     * a claim about their server. See [AliasFolding.foldUnreadableGroups].
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
     * The one fold the urls decide by themselves: `ws://x` and `wss://x` name
     * one endpoint, so when both answer, the plain one folds onto the secure
     * one. A path is routinely a different endpoint; a scheme never is.
     *
     * Both must have answered: a url missing from [prints] is no evidence
     * about the pair. The windows keep a veto in the one direction that can
     * lose data: everything the plain url served must already be on the secure
     * one, at [minOverlap], with no floor. Where both windows clear
     * [minSample] this folds a strict subset of what [sameRelay] would.
     */
    private fun schemeTwins(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        // Runs for every group of every pass, so the cheap test comes first.
        if (group.none { !isSecure(it.url) }) return emptyMap()
        val secure = secureTwins(group)
        if (secure.isEmpty()) return emptyMap()
        val twins = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        for (url in group) {
            // Never the leader: it is the yardstick every other url was just
            // measured against, and [canonicalOf] does not follow a chain.
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

    /**
     * Is everything [print] handed over already in [survivor]'s window? An
     * empty window passes vacuously: a url that served nothing has nothing to
     * lose, and this only decides whether to stop dialling it.
     */
    private fun alreadyServedBy(
        print: Set<String>,
        survivor: Set<String>,
    ): Boolean = print.isEmpty() || print.count { it in survivor }.toDouble() / print.size >= minOverlap

    /**
     * The `ws://` url of the same endpoint as [secure], if [group] holds one
     * that nothing has folded. Asked of a yardstick too thin to measure
     * against, which can still settle its own pair.
     */
    fun plainTwinIn(
        group: List<NormalizedRelayUrl>,
        secure: NormalizedRelayUrl,
    ): NormalizedRelayUrl? {
        if (!isSecure(secure.url)) return null
        val key = endpointKey(secure.url) ?: return null
        return group.firstOrNull { !isSecure(it.url) && endpointKey(it.url) == key && !folded.containsKey(it) }
    }

    /** The `wss://` urls of a group, by the endpoint each of them reaches. */
    private fun secureTwins(group: List<NormalizedRelayUrl>): Map<String, NormalizedRelayUrl> {
        val secure = HashMap<String, NormalizedRelayUrl>()
        for (url in group) {
            if (!isSecure(url.url)) continue
            endpointKey(url.url)?.let { secure.putIfAbsent(it, url) }
        }
        return secure
    }

    /**
     * Do these two windows come from one relay? Both must hold [Bar.window]
     * ids, share [Bar.shared] outright, and the smaller must be [minOverlap]
     * contained in the larger.
     */
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
     * The url of a group everything else is compared against. A url already
     * folded onto wins, so a new alias next cycle cannot re-point an existing
     * verdict; otherwise [PREFERENCE], a total order, picks the same leader
     * every time.
     */
    private fun leaderOf(group: List<NormalizedRelayUrl>): NormalizedRelayUrl {
        group.firstOrNull { it in canonicals }?.let { return it }
        return group.minWith(PREFERENCE)
    }

    /**
     * The best url of [urls] under the order [leaderOf] falls back to. The
     * comparator stays private so no caller can sort by it and pick differently.
     */
    fun preferred(urls: Collection<NormalizedRelayUrl>): NormalizedRelayUrl? = urls.minWithOrNull(PREFERENCE)

    companion object {
        /**
         * How deep a fingerprint goes. A depth, not a REQ limit: [AliasProbe]
         * pages `until` backwards to reach it. Matching [DEFAULT_PROBE_PAGE]
         * means a relay serving a full page answers in one round trip.
         */
        const val DEFAULT_PROBE_TARGET = 500

        /**
         * Events per REQ. The cap most hosts advertising `max_limit` publish;
         * asking over a relay's cap risks a refusal rather than a truncation.
         */
        const val DEFAULT_PROBE_PAGE = 500

        /**
         * The humbler page, tried once when the first ask comes back empty. A
         * refusal and an empty relay are indistinguishable at that layer, so
         * the retry is unconditional.
         */
        const val FALLBACK_PROBE_PAGE = 100

        /** Below 20 shared ids a match is a coincidence, not a measurement. */
        const val DEFAULT_MIN_SAMPLE = 20

        /**
         * What a relay is asked once it has refused both a bare filter and
         * [AliasProbe.FALLBACK_KINDS]: its NIP-29 group metadata, the one window
         * a groups relay serves unscoped and unauthenticated. Addressable
         * events, so it barely drifts between two dials.
         */
        val GROUP_METADATA_KINDS = listOf(39000)

        /** The floor for a [GROUP_METADATA_KINDS] window. See [groupMetadataMinSample]. */
        const val DEFAULT_GROUP_METADATA_MIN_SAMPLE = 3

        /**
         * Half. The two windows are seconds apart against a moving feed and one
         * may be truncated, so near-identity would fold nothing on the busy
         * relays where duplication costs most.
         */
        const val DEFAULT_MIN_OVERLAP = 0.5

        /** What a url must score against itself for its host to be measurable. See [reproducible]. */
        const val DEFAULT_MIN_SELF_OVERLAP = 0.9

        /**
         * Apply an alias map to a discovered set. What each url was paired with
         * moves with it: dropping an alias without moving its bound authors
         * onto the survivor would stop asking for those authors entirely.
         */
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

        /** How far [resolve] will follow a verdict chain before giving up. */
        private const val MAX_CHAIN = 8

        /**
         * Which url of a group is worth keeping, best first: no path over a
         * path, `wss` over `ws`, no explicit port over one, then the shortest,
         * then the url itself so the order is total. The pathless url is the
         * one everyone else's relay lists name, so folding onto it keeps every
         * participant's bands and records pointing at one string.
         */
        private val PREFERENCE =
            compareBy<NormalizedRelayUrl>(
                { if (pathOf(it.url).isEmpty()) 0 else 1 },
                { if (it.url.startsWith("wss://", true)) 0 else 1 },
                { if (hasExplicitPort(it.url)) 1 else 0 },
                { it.url.length },
                { it.url },
            )

        /** Everything between the scheme and the first `/`, lowercased, port removed. */
        fun hostOf(url: String): String {
            val authority = afterScheme(url).substringBefore('/')
            // An IPv6 literal keeps its brackets; only a colon after the
            // bracket is the port separator.
            val host = if (authority.startsWith("[")) authority.substringBefore(']') + "]" else authority.substringBefore(':')
            return host.lowercase()
        }

        /** The path, without its leading slash; empty for a bare host. */
        fun pathOf(url: String): String = afterScheme(url).substringAfter('/', "").trim('/')

        /** TLS, the half of a scheme pair we keep. See [schemeTwins]. */
        private fun isSecure(url: String): Boolean = url.startsWith("wss://", true)

        /**
         * `host/path` with the scheme taken out, so `ws://x/p` and `wss://x/p`
         * land on one key. Null when the url names a port its scheme would not
         * default to: `wss://x:8443` is a deliberately chosen second endpoint,
         * and only the fingerprint can say whether it matches `ws://x`.
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
