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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which discovered urls are the SAME relay wearing a different url.
 *
 * Most relay software serves its websocket on every path, so `wss://nos.lol`,
 * `wss://nos.lol/alpha` and `wss://nos.lol:443/beacon-glyph` are one server
 * behind three urls — and a relay list can mint them without limit. Measured on
 * this store: 7,333 discovered urls stood for 1,147 distinct `host:port`
 * endpoints, and weighting each endpoint by how many lists name it, the popular
 * relays were dialled 10.7x over. That is not a fan-out, it is the same relay
 * downloaded ten times.
 *
 * [HostStrikes] cannot help: it evicts an authority that goes SILENT, and every
 * one of these answers perfectly well. The duplicate is only visible in what
 * comes back, so that is what this asks for — the newest [probeTarget] events
 * at each url, PAGED so a relay's own REQ cap sets the page size rather than the
 * depth. Two urls whose newest window is the same window are the same relay.
 *
 * The verdict is a measurement, not a guess about someone's config, which is
 * what makes it publishable: see [RelayAliasRecord] for the NIP-66 record the
 * monitor signs. The fold is deliberately hard to trigger — see [sameRelay] —
 * because a wrong one silently stops mirroring a relay nobody will notice is
 * missing.
 */
class RelayAliases(
    /**
     * How deep a fingerprint walks. The whole window is submitted to ingest, so
     * this is a sync that also identifies — the only waste is the ids that turn
     * out to be a duplicate's.
     */
    val probeTarget: Int = DEFAULT_PROBE_TARGET,
    /**
     * The smallest window worth deciding on. Two relays that both answered with
     * four events look identical and are not evidence of anything; below this,
     * nothing is folded and both urls stay in the fan-out.
     */
    private val minSample: Int = DEFAULT_MIN_SAMPLE,
    /**
     * How much of the smaller window must appear in the larger one. Containment
     * rather than a symmetric ratio, because the two dials are seconds apart on
     * a live relay and one of them may be cut short by the peer's own
     * `default_limit` — a truncated window is still the same window.
     */
    private val minOverlap: Double = DEFAULT_MIN_OVERLAP,
) {
    /** alias url -> the url we actually dial for it. Only ever holds folded urls. */
    private val folded = ConcurrentHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()

    /**
     * Urls a probe already cleared: they are their own relay and must not be
     * fingerprinted again. Without this, every url that is NOT a duplicate is
     * the one thing we re-probe forever.
     */
    private val distinct = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /**
     * The urls something has been folded ONTO. Held as its own set rather than
     * read out of [folded]'s values: [leaderOf] asks this question once per
     * group per cycle, and a `containsValue` walks every verdict ever made to
     * answer it.
     */
    private val canonicals = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** The url to dial in place of this one — itself, unless it was folded. */
    fun canonicalOf(url: NormalizedRelayUrl): NormalizedRelayUrl = folded[url] ?: url

    /** How many urls are currently folded away. */
    fun size(): Int = folded.size

    /** Every verdict held, for the monitor to publish and a restart to reload. */
    fun verdicts(): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folded.toMap()

    /**
     * Adopt verdicts a previous run published. [RelayAliasRecord] drops the
     * stale ones before they get here, so anything in [known] is still within
     * its TTL.
     */
    fun adopt(known: Map<NormalizedRelayUrl, NormalizedRelayUrl>) {
        for ((alias, canonical) in known) {
            // A verdict pointing at a url that is ITSELF folded would leave the
            // fan-out dialling a duplicate through two hops. Resolve to the end
            // of the chain, which is where the events are.
            if (alias == canonical) continue
            val end = resolve(canonical)
            folded[alias] = end
            canonicals += end
        }
    }

    /**
     * Has anything been decided about this url — folded, or probed and cleared?
     *
     * False is "no verdict", which is NOT "distinct": a url nothing has looked
     * at yet and a url whose relay would not answer land here together, and
     * both must still be dialled. The caller is told which urls these are so it
     * can treat them as its policy requires rather than as a silent default.
     */
    fun measured(url: NormalizedRelayUrl): Boolean = url in distinct || url in canonicals || folded.containsKey(url)

    /** This url was probed and is nobody's duplicate. Never fingerprint it again. */
    fun markDistinct(url: NormalizedRelayUrl) {
        distinct += url
    }

    /**
     * Adopt cleared verdicts a previous run published, the negative half of
     * [adopt]. A url this run has since folded wins — a fold is the stronger
     * statement, and re-clearing it would put a duplicate back in the fan-out.
     */
    fun adoptDistinct(known: Set<NormalizedRelayUrl>) {
        for (url in known) {
            if (folded.containsKey(url)) continue
            distinct += url
        }
    }

    /**
     * The candidate urls grouped by the host they reach, keeping only the
     * groups a probe could still learn something from.
     *
     * Grouped by HOSTNAME alone — not by quartz's `host:port` authority — so
     * one pass folds all three shapes this pollution comes in: the path
     * (`/beacon-glyph`), the redundant default port (`:443`), and the scheme
     * (`ws://` beside `wss://` on a host that serves both). They are the same
     * server or they are not, and only the fingerprint gets to say.
     *
     * A group is skipped when every member already has a verdict, and when it
     * has only one member — there is nothing to be a duplicate OF.
     *
     * "Has a verdict" is [measured] and nothing narrower. Spelling the test out
     * here instead cost a fingerprint per fully folded group per pass, forever:
     * a leader everything else folded ONTO is a canonical, which is a verdict,
     * but it is not a key in `folded` and not in `distinct`, so a hand-written
     * predicate kept returning its group as unfinished. [toProbe] would then
     * re-dial the leader alone, learn nothing — there is nothing left to compare
     * it to — and do it again next pass. A new url on that host still reopens
     * the group, because the new url is the thing without a verdict.
     */
    fun unresolved(candidates: Collection<NormalizedRelayUrl>): List<List<NormalizedRelayUrl>> =
        candidates
            .groupBy { hostOf(it.url) }
            .values
            .map { group -> group.sortedWith(PREFERENCE) }
            .filter { group -> group.size > 1 && group.any { !measured(it) } }

    /**
     * Which urls of one group still need a fingerprint: the ones with no
     * verdict, plus the group's [leaderOf] — the yardstick they are measured
     * against, which has to be re-measured because the window it is compared
     * to has moved on since last cycle.
     */
    fun toProbe(group: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        val leader = leaderOf(group)
        return (listOf(leader) + group.filter { it != leader && it !in distinct && !folded.containsKey(it) }).distinct()
    }

    /** What one group's fingerprints proved: the folds, and the urls cleared. */
    data class Learned(
        /** Folded url -> the leader it folded onto. */
        val folded: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls compared and found to be their own relay, the leader included. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
    )

    /**
     * Fold one group against the fingerprints just taken, and return only what
     * this call learned. A url with no fingerprint (unreachable, refused,
     * answered nothing) is left exactly as it was: silence is not evidence of
     * duplication, and a relay that is merely down must come back into the
     * fan-out when it recovers.
     *
     * **The leader is cleared too, when nothing folded onto it.** It is the one
     * member never compared against anything — everything is compared against
     * IT — so without this it would be the only url in a fully decided group
     * still carrying no verdict, [unresolved] would keep returning the group
     * forever, and persisting the members' verdicts would save nothing.
     * Clearing it is sound for the same reason theirs is: it was measured
     * against every member that answered, and matched none of them.
     */
    fun learn(
        group: List<NormalizedRelayUrl>,
        prints: Map<NormalizedRelayUrl, Set<String>>,
    ): Learned {
        val leader = leaderOf(group)
        val leaderPrint = prints[leader] ?: return Learned()
        val folds = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val cleared = HashSet<NormalizedRelayUrl>()
        var compared = 0
        for (url in group) {
            if (url == leader || folded.containsKey(url)) continue
            val print = prints[url] ?: continue
            compared++
            if (sameRelay(leaderPrint, print)) {
                folded[url] = leader
                canonicals += leader
                distinct -= url
                folds[url] = leader
            } else {
                // Probed, and it is its own relay. Recorded so the next cycle
                // spends its budget on urls we know nothing about.
                markDistinct(url)
                cleared += url
            }
        }
        // Only when something was actually held up against it: a leader whose
        // whole group was unreachable has been compared to nothing and has
        // proved nothing. And only when it is not a canonical — if anything
        // folded onto it, it is the class, not a singleton.
        if (compared > 0 && leader !in canonicals) {
            markDistinct(leader)
            cleared += leader
        }
        return Learned(folds, cleared)
    }

    /**
     * Do these two windows come from one relay?
     *
     * Both sides must have handed over at least [minSample] ids — the guard
     * against calling two quiet relays identical because neither said much —
     * and the smaller window must be [minOverlap] contained in the larger.
     */
    private fun sameRelay(
        a: Set<String>,
        b: Set<String>,
    ): Boolean {
        val smaller = minOf(a.size, b.size)
        if (smaller < minSample) return false
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        return shared.toDouble() / smaller >= minOverlap
    }

    /** Follow a chain of verdicts to the url at the end of it. */
    private fun resolve(url: NormalizedRelayUrl): NormalizedRelayUrl {
        var at = url
        // Bounded rather than `while`: a verdict file edited by hand, or two
        // runs that disagreed, must not spin here.
        repeat(MAX_CHAIN) {
            at = folded[at] ?: return at
        }
        return at
    }

    /**
     * The url of a group everything else is compared against.
     *
     * A url this run already folded ONTO wins, so the yardstick stays put while
     * a group grows: a new alias appearing next cycle must not re-point an
     * existing verdict at a different member and re-key every band that
     * mentions it. Otherwise [PREFERENCE] decides, which is a total order on
     * the url, so a group with no history picks the same leader every time.
     */
    private fun leaderOf(group: List<NormalizedRelayUrl>): NormalizedRelayUrl {
        group.firstOrNull { it in canonicals }?.let { return it }
        return group.minWith(PREFERENCE)
    }

    companion object {
        /**
         * How deep a fingerprint goes: the newest 500 events.
         *
         * A DEPTH, not a REQ limit — [AliasProbe] pages `until` backwards to
         * reach it, so this is the same depth at every relay regardless of what
         * any one of them caps a single REQ at.
         *
         * 500 because 1,000 bought nothing. Re-measured over 35 hosts and 112
         * fold decisions: 500 agreed with 1,000 on 108 of them, and all four
         * disagreements were `espelho.girino.org` — the one relay that cannot
         * reproduce its own answers — where the shallower window happens to
         * fold urls that genuinely are the same relay. What it costs is not
         * marginal: median 1.4s and 562 KB against 3.4s and 1,464 KB.
         *
         * Depth was never what made the fingerprint stable; the shared anchor
         * was. Going deeper than one page only lengthens the walk, and a longer
         * walk is a longer drift.
         *
         * Matching [DEFAULT_PROBE_PAGE] is the point: a relay that serves a
         * full page answers in ONE round trip. Going below it saves nothing —
         * 200 measured the same 520 KB, because the page is asked whole either
         * way.
         */
        const val DEFAULT_PROBE_TARGET = 500

        /**
         * Events per REQ on the way to [DEFAULT_PROBE_TARGET]. 500 because that
         * is the cap half the sampled hosts advertising `max_limit` publish,
         * and asking over a relay's cap risks an outright refusal rather than a
         * truncation — purplepag.es answers `{"limit": 1000}` with `CLOSED
         * blocked: limit too high: 1000 (max 500)`.
         */
        const val DEFAULT_PROBE_PAGE = 500

        /**
         * The humbler page, tried once when the first ask comes back empty.
         * Relays capping under [DEFAULT_PROBE_PAGE] exist (one sampled host
         * advertises 0), and a refusal is indistinguishable from an empty relay
         * at that layer — so the retry is unconditional rather than parsed out
         * of a CLOSED message.
         */
        const val FALLBACK_PROBE_PAGE = 100

        /** Below 20 shared ids a match is a coincidence, not a measurement. */
        const val DEFAULT_MIN_SAMPLE = 20

        /**
         * Half. The two windows are taken seconds apart against a moving feed
         * and one may be truncated by the peer's `default_limit`, so demanding
         * near-identity would fold nothing on exactly the busy relays where the
         * duplication costs the most.
         */
        const val DEFAULT_MIN_OVERLAP = 0.5

        /**
         * Apply an alias map to a discovered set — the one line a caller of
         * [AliasFolding.clean] needs, kept here because getting it wrong is
         * silent.
         *
         * What each url was PAIRED with moves with it: an outbox stream binds
         * authors to the url that named them, and dropping
         * `wss://nos.lol/alpha` without moving its authors onto
         * `wss://nos.lol` would stop asking for those authors entirely — a fold
         * that loses data instead of duplicates.
         *
         * Pure, and takes the map rather than reading instance state, so the
         * component that decides aliases and the one that applies them need not
         * be the same object.
         */
        fun foldOnto(
            relays: List<DiscoveredRelay>,
            aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        ): List<DiscoveredRelay> {
            if (aliases.isEmpty()) return relays
            val merged = LinkedHashMap<NormalizedRelayUrl, MutableMap<String, MutableSet<String>>>()
            for (relay in relays) {
                val into = merged.getOrPut(aliases[relay.url] ?: relay.url) { HashMap() }
                for ((dest, values) in relay.narrow) into.getOrPut(dest) { HashSet() }.addAll(values)
            }
            return merged.map { (url, narrow) -> DiscoveredRelay(url, narrow.mapValues { (_, v) -> v.toSet() }) }
        }

        /** How far [resolve] will follow a verdict chain before giving up. */
        private const val MAX_CHAIN = 8

        /**
         * Which url of a group is worth keeping, best first: no path over a
         * path, `wss` over `ws`, no explicit port over one, then the shortest
         * and finally the url itself so the order is total and stable.
         *
         * The pathless url is preferred rather than merely likelier to be real:
         * it is the one the relay's own NIP-11 and everyone else's relay lists
         * name, so folding onto it keeps the bands, the cursors and the NIP-66
         * records of every other participant pointing at the same string.
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
            // An IPv6 literal keeps its brackets; the colons inside them are
            // not the port separator, so only a colon AFTER the bracket is.
            val host = if (authority.startsWith("[")) authority.substringBefore(']') + "]" else authority.substringBefore(':')
            return host.lowercase()
        }

        /** The path, without its leading slash — empty for a bare host. */
        fun pathOf(url: String): String = afterScheme(url).substringAfter('/', "").trim('/')

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
