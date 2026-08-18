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
 * what makes it publishable: see [RelayVerdictRecord] for the NIP-66 record the
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
     *
     * This is the floor for a window taken through a GENERAL filter, and it is
     * the floor every NEGATIVE claim uses whatever the filter was — see
     * [groupMetadataMinSample] for the one window that folds below it.
     */
    private val minSample: Int = DEFAULT_MIN_SAMPLE,
    /**
     * The floor for a window taken through [GROUP_METADATA_KINDS] alone.
     *
     * A NIP-29 relay refuses a general window outright, so the only fingerprint
     * to be had at one is its list of groups — and that list is SHORT. Measured
     * over 21 live NIP-29 hosts discovered from kind-10009 group lists, the
     * kind-39000 window is min 1, median **9**, max 1,302; [DEFAULT_MIN_SAMPLE]
     * admits 7 of the 21 and this admits 16. A floor built for a slice of a
     * firehose is simply the wrong instrument for a list of groups.
     *
     * Three rather than one, because a single shared id is the definition of the
     * coincidence [minSample] exists to refuse, and the hosts it would add are
     * the ones serving one or two groups.
     *
     * It is BOTH halves of the bar at this width: the smallest window either url
     * may bring, and — via [Bar.shared] — the fewest ids they must genuinely
     * have in common, since [minOverlap] alone would settle for two of them. See
     * [foldBar].
     */
    private val groupMetadataMinSample: Int = DEFAULT_GROUP_METADATA_MIN_SAMPLE,
    /**
     * How much of the smaller window must appear in the larger one. Containment
     * rather than a symmetric ratio, because the two dials are seconds apart on
     * a live relay and one of them may be cut short by the peer's own
     * `default_limit` — a truncated window is still the same window.
     */
    private val minOverlap: Double = DEFAULT_MIN_OVERLAP,
    /**
     * How much of its own window a relay must hand back on a second walk before
     * anything it says is treated as evidence. See [reproducible].
     */
    private val minSelfOverlap: Double = DEFAULT_MIN_SELF_OVERLAP,
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

    /** The url to dial in place of this one. */
    fun canonicalOf(url: NormalizedRelayUrl): NormalizedRelayUrl = folded[url] ?: url

    fun size(): Int = folded.size

    /** Every verdict held, for the monitor to publish and a restart to reload. */
    fun verdicts(): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folded.toMap()

    /**
     * Set this candidate set's verdicts to exactly what the store holds, in ONE
     * pass per url rather than a bulk [forget] followed by a bulk [adopt].
     *
     * **The two-step version had a window in which a fold did not exist.**
     * `forget` walks the whole candidate set removing verdicts and `adopt` walks
     * it putting them back; between those two walks — five figures of urls on a
     * real fan-out — every url reads as its own relay. This object is SHARED by
     * every stream and the monitor's pass, all of which run concurrently, so
     * another stream landing inside that window sees a store's worth of folds
     * missing and dials the duplicates for a whole cycle: a socket, a band and a
     * cursor each, for events the survivor is already delivering. Nothing
     * anywhere reports it, because by the time anyone looks the map is correct
     * again. [RelayConsistency.replace] is the same fix for the same reason on
     * the sibling verdict; this is the half that did not get it.
     *
     * Each url moves straight from its old verdict to its new one and is never
     * transiently absent. A url the store no longer has a verdict for is
     * cleared, which is what gives the record's TTL — and the rules epoch — their
     * teeth.
     *
     * Chains are resolved against [known] rather than against the live map, so
     * the result does not depend on the order the store happened to return
     * verdicts in, and a cycle (A says B, B says A) resolves to nothing rather
     * than to whichever edge was read first.
     */
    fun replace(
        candidates: Collection<NormalizedRelayUrl>,
        known: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        cleared: Set<NormalizedRelayUrl>,
    ) {
        // Every fold this candidate set implies, resolved to the url the events
        // are actually at, BEFORE anything is written. A verdict pointing at a
        // url that is itself folded would otherwise leave the fan-out dialling
        // a duplicate through two hops.
        val ends = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>(known.size)
        for ((alias, canonical) in known) {
            if (alias == canonical) continue
            val end = endOf(known, canonical) ?: continue
            // A url pinned as its own duplicate is not a fold: `measured` would
            // start answering true, `unresolved` would drop the group, and
            // nothing would ever revisit it.
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
                // verdict for the same url — which is why this is the `else`.
                if (url in cleared) distinct += url else distinct -= url
            }
            // Decided by the incoming map and never by the order of this walk:
            // a canonical usually has no record of its own, so it is only ever
            // a canonical because something else points AT it.
            if (url !in heads) canonicals -= url
        }
        canonicals += heads
    }

    /**
     * The url at the end of a chain of verdicts, or null when there is no end —
     * a loop, or a hand-edited file. Read from [known] alone, so the answer does
     * not depend on what any other stream has adopted.
     *
     * **Not the same question as [resolve], and the two must not be merged.**
     * This one is asked while ADOPTING what the store returned, where the live
     * map is mid-update and reading it would make the result depend on the order
     * verdicts arrived in. [resolve] is asked while LEARNING, where the live map
     * is the only place this pass's own folds exist yet.
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
     * Follow a chain of verdicts through the LIVE map to the url at the end of
     * it — what [learn] needs when it folds a `ws://` twin onto a secure url
     * that this very call may already have folded somewhere else.
     *
     * Bounded rather than `while`: a verdict edited by hand, or two runs that
     * disagreed, must not spin here. See [endOf] for the sibling that reads the
     * store's answer instead, and why they stay separate.
     */
    private fun resolve(url: NormalizedRelayUrl): NormalizedRelayUrl {
        var at = url
        repeat(MAX_CHAIN) {
            at = folded[at] ?: return at
        }
        return at
    }

    /**
     * Drop every verdict held about these urls.
     *
     * The store is the record; this map is a cache of it, and a cache with no
     * expiry quietly outlives what it caches. [RelayVerdictRecord.load] already
     * refuses anything past its TTL, but a verdict adopted before it expired
     * stayed here for the life of the process: `measured` kept answering true,
     * so the url was never re-probed and the expired verdict was never
     * republished, and the fan-out went on folding on evidence that no longer
     * existed anywhere. Forgetting before each adopt makes the store
     * authoritative every pass, which is what gives the TTL its teeth.
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
         * The fewest ids the two must actually have IN COMMON.
         *
         * Zero for a general window, which is this class's long-standing
         * behaviour: [minOverlap] alone decides, and a 500-id window folding at
         * 0.5 already shares 250 ids, so a separate count buys nothing there.
         *
         * **It cannot stay zero once the window floor is small, and that is a
         * hole a pure RATIO cannot close.** At a window floor of 3 against
         * [DEFAULT_MIN_OVERLAP] the least a fold could ever rest on is TWO
         * shared ids: a path serving `{a, b, x}` scores 0.667 against a leader
         * serving `{a, b, c, d, e, f, g}` and folds — taking `x`, a group
         * nothing else on the host serves, out of the fan-out for
         * [RelayVerdictRecord.DEFAULT_TTL_SECONDS]. That is the fold's one
         * unforgivable failure, silently not mirroring something, bought for a
         * two-id coincidence.
         */
        val shared: Int,
    )

    /**
     * What a fold must clear, given the FILTER that produced the windows.
     *
     * **The bar is a property of the question we had to ask, not a constant.**
     * [minSample] is calibrated for a slice of a general event feed, where 20
     * shared ids out of a firehose is the line between a measurement and a
     * coincidence. A window of [GROUP_METADATA_KINDS] is not that: it is a
     * relay's complete list of groups, addressable, one event per group, and a
     * host with nine groups hands over nine ids and has told us everything it
     * has. Holding that to the firehose floor refuses the entire NIP-29 corpus —
     * measured, 14 of 21 live hosts.
     *
     * So the window floor drops and [Bar.shared] rises to meet it. The concern
     * was never that a SHORT window is untrustworthy; it is that a fold resting
     * on one or two ids is a coincidence, and only the second of those is worth
     * a guard. Every live pair measured shares its list entirely — containment
     * 1.000, 6 of 6 — so demanding three ids genuinely in common costs the
     * honest case nothing at all.
     *
     * `minOf` on the window floor, so this only ever LOWERS it: a caller that
     * set [minSample] below the group floor keeps its own number rather than
     * having it raised here.
     */
    private fun foldBar(kinds: List<Int>?): Bar =
        if (kinds == GROUP_METADATA_KINDS) {
            val floor = minOf(minSample, groupMetadataMinSample)
            Bar(window = floor, shared = floor)
        } else {
            Bar(window = minSample, shared = 0)
        }

    /**
     * Is this window big enough to be measured against at all?
     *
     * [kinds] is the filter the window came through — null for the general one,
     * which is the strict floor and the safe default for any caller that does
     * not track it.
     */
    fun usableWindow(
        print: Set<String>?,
        kinds: List<Int>? = null,
    ): Boolean = print != null && print.size >= foldBar(kinds).window

    /**
     * Does this relay give the same answer twice — the question that has to be
     * settled before ANY verdict is published about the host it leads.
     *
     * Two walks of the SAME url from the SAME anchor, and the answer gates ONE
     * of the fold's two conclusions rather than both.
     *
     * **It gates "these are different relays" and deliberately not "these are
     * one relay",** which this used to claim it gated in either direction. Noise
     * in the yardstick is not symmetric between them. A relay handing back a
     * shuffled subset of its own window drives every containment DOWN — so a
     * sibling that still clears [minOverlap] against it did so in spite of the
     * noise, and folding is the conservative reading. The same noise pushes urls
     * over the bar in the other direction for free, which is how two paths of
     * one server get signed as separate relays for a month.
     *
     * Measured, and this is what corrected the claim: `fiatjaf.com` self-scores
     * 0.638 — far under the bar — while its two minted paths score 0.787 and
     * 0.730 against it, and [AliasFolding.measure] publishes both folds. That is
     * the right answer for a host serving one pool of events on three urls, and
     * demanding reproducibility first would have kept all three in the fan-out.
     * `multiplexer.huszonegy.world` is the same shape: self 0.594, siblings
     * 0.622-0.870, four folds.
     *
     * So the guard is paid only where a NEGATIVE claim is about to be made —
     * see the call site — and a group that folds cleanly never pays for it.
     *
     * The bar sits in an empty gap in the measurements rather than at a round
     * number. Stable relays self-score at the top of the range — nos.lol 0.998,
     * nostr.oxtr.dev 1.000 — while the two hosts found in this state score far
     * below it: `espelho.girino.org` at 0.435, and `fiatjaf.com` at 0.694-0.720
     * over a paged walk and **0.000** on a single page, where the same url asked
     * twice seconds apart shared none of its ten events. Nothing has been
     * measured between 0.8 and 0.99, which is why 0.9 costs nothing to demand.
     *
     * Note what this is NOT: [minOverlap] is about two DIFFERENT urls and is
     * deliberately generous, because two dials seconds apart on a live relay
     * legitimately disagree at the edges. One url against itself has no such
     * excuse.
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
     * The candidate urls grouped by the host they reach, keeping only the
     * groups a probe could still learn something from.
     *
     * Grouped by HOSTNAME alone — not by quartz's `host:port` authority — so
     * one pass folds all three shapes this pollution comes in: the path
     * (`/beacon-glyph`), the redundant default port (`:443`), and the scheme
     * (`ws://` beside `wss://` on a host that serves both). They are the same
     * server or they are not, and — with the one exception a url can settle by
     * itself, see [schemeTwins] — only the fingerprint gets to say.
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
     *
     * …plus the SECURE TWIN of anything in that list, even when it already
     * carries a verdict of its own. [schemeTwins] folds `ws://x` onto `wss://x`
     * only when BOTH answered this pass, and a twin nobody re-dials can never be
     * one of the two — so on a host whose `wss://x/inbox` was cleared last month
     * and whose `ws://x/inbox` turned up today, the cheapest verdict available
     * would go unmade and the pair would be dialled separately forever.
     *
     * The bill is one fingerprint per pass per pair whose plain half has no
     * verdict — including, on a host whose `ws://` url has simply stopped
     * answering, a pair that will never produce one. That is the same shape the
     * group already pays for the plain url itself, it is bounded by the 103
     * `ws://` urls in 1,852 this corpus carries, and it only exists at all where
     * the pair does: a group of nothing but `wss://` urls does not build so much
     * as a map here.
     */
    fun toProbe(group: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        val leader = leaderOf(group)
        val wanted = group.filter { it != leader && it !in distinct && !folded.containsKey(it) }
        // The overwhelmingly common case, out before anything is allocated: a
        // host wearing nothing but `wss://` urls has no pair to look for.
        val plain = wanted.filter { !isSecure(it.url) }
        if (plain.isEmpty()) return (listOf(leader) + wanted).distinct()
        val secure = secureTwins(group)
        // A twin that is itself folded is not worth the dial: its verdict already
        // points somewhere else, and the plain url is compared to the leader in
        // the ordinary way.
        val twins = plain.mapNotNull { endpointKey(it.url)?.let(secure::get) }.filter { !folded.containsKey(it) }
        if (twins.isEmpty()) return (listOf(leader) + wanted).distinct()
        // Sorted back into PREFERENCE order rather than appended, because that
        // order is what [AliasFolding]'s yardstick search walks down: a twin
        // dropped at the end of the queue would be tried after urls it outranks,
        // and it is by construction a `wss://` url — the best kind of yardstick
        // the group has.
        return (listOf(leader) + (wanted + twins).sortedWith(PREFERENCE)).distinct()
    }

    /** What one group's fingerprints proved: the folds, and the urls cleared. */
    data class Learned(
        /** Folded url -> the leader it folded onto. */
        val folded: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls compared and found to be their own relay, the leader included. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /**
         * The subset of [folded] decided by [schemeTwins] — a `ws://` url folded
         * onto the `wss://` url of the same endpoint. Named separately because
         * the EVIDENCE is different: these rest on the pair of urls and on both
         * having answered, not on a containment score, and a record that quoted
         * a containment for them would be quoting a number the verdict was not
         * based on.
         */
        val twins: Set<NormalizedRelayUrl> = emptySet(),
    )

    /**
     * Fold one group against the fingerprints just taken, and return only what
     * this call learned. A url with no fingerprint (unreachable, refused,
     * answered nothing) is left exactly as it was: silence is not evidence of
     * duplication, and a relay that is merely down must come back into the
     * fan-out when it recovers.
     *
     * **Silence includes a window too small to mean anything, not just a
     * missing one.** [sameRelay] already refuses to FOLD below [minSample],
     * which used to be the end of it — the url fell through to the else branch
     * and was called its own relay on the strength of nine events, or of none.
     * That was survivable while the verdict lived in memory and evaporated on
     * restart. It is not survivable now that it is published: measured live,
     * `relay.damus.io/lantern-oscar-dynamo` answered with ZERO events and was
     * about to be recorded, for thirty days, as a relay in its own right.
     * Refusing to fold and proving distinctness are different claims and only
     * one of them can be made from a thin window.
     *
     * **The leader is cleared too, when nothing folded onto it.** It is the one
     * member never compared against anything — everything is compared against
     * IT — so without this it would be the only url in a fully decided group
     * still carrying no verdict, [unresolved] would keep returning the group
     * forever, and persisting the members' verdicts would save nothing.
     * Clearing it is sound for the same reason theirs is: it was measured
     * against every member that answered enough to be measured against.
     *
     * **One pair is settled without a window: `ws://x` against `wss://x`.** See
     * [schemeTwins] — those are held out of both passes below and folded at the
     * end, because a containment test is not the question to ask about two urls
     * that name one endpoint.
     *
     * [leader] is passed in rather than recomputed. [leaderOf] prefers a url
     * something has already folded onto, and `canonicals` is mutated by every
     * concurrent pass — so recomputing here could name a DIFFERENT url than the
     * one [toProbe] had fingerprinted as the yardstick, and the whole group's
     * work would be thrown away on a map that changed underneath it.
     */
    fun learn(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
        /**
         * The filter every print in [prints] was taken through — the group's
         * leader decided it, and it is what [foldBar] reads. Null is the
         * general filter and the strict floor.
         */
        kinds: List<Int>? = null,
    ): Learned {
        val leaderPrint = prints[leader] ?: return Learned()
        // FOLDING and CLEARING do not get the same floor, and that asymmetry is
        // the whole safety argument for lowering one of them.
        //
        // A thin window can support "these two urls are the same relay": the
        // NIP-29 host handed over its complete list of groups and two of its
        // urls returned the same list. It cannot support "this url is a relay in
        // its own right", which is a signed statement about somebody else's
        // server that stands for [RelayVerdictRecord.DEFAULT_TTL_SECONDS] — and
        // which a thin window manufactures for free, since sharing none of three
        // ids is exactly what a url that answered almost nothing looks like.
        // That is the `relay.damus.io/lantern-oscar-dynamo` lie in miniature.
        //
        // So [minSample] stays on every path below that WRITES a negative claim
        // — entry to `unmatched`, and the leader's own clear — and only
        // [sameRelay] is given the filter's bar.
        val bar = foldBar(kinds)
        val folds = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val cleared = HashSet<NormalizedRelayUrl>()
        var compared = 0
        // Decided before a single window is compared, and held out of both
        // passes below: a url whose only difference from another candidate is
        // `ws://` against `wss://` is not something the containment test should
        // be asked about. See [schemeTwins].
        val twins = schemeTwins(group, leader, prints)
        // Measured against the leader and found to be something else. NOT yet a
        // verdict — see the second pass.
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
                // Both sides said enough to be compared, and they disagreed —
                // with the LEADER. Held, not published: the leader is one
                // endpoint on this host and not the only one it could be.
                compared++
                unmatched += url
            }
        }
        // …AND THEN AGAINST EACH OTHER, which is the half that was missing.
        //
        // "Not the leader" was being published as "its own relay", and on a host
        // whose preferred url is a genuinely DIFFERENT endpoint that is a claim
        // about nothing. `/inbox` is the common shape — haven splits inbox from
        // the public pool, and NIP-17 made the split ordinary — and it also sorts
        // first under [PREFERENCE], so it leads its group and every minted path
        // behind it disagrees with it correctly and gets signed as a separate
        // relay for a month.
        //
        // Measured live on `haven.calva.dev`: seven urls, `/inbox` leading, six
        // minted paths each sharing 96 of 500 with it — all six published as
        // distinct relays. Those six are ONE relay at containment **1.000**
        // against each other. Seven dials for two endpoints, re-proved every
        // time the TTL lapsed. `h.codingarena.top`, `relay.dergigi.com` and
        // `relay.shawnyeager.com` are the same shape in the same pass.
        //
        // The comparison is free: every print is already in hand, so this costs
        // set intersections and not one dial. Against the cluster HEADS rather
        // than every pair, so a host of genuinely distinct endpoints (measured:
        // `nostr.ac`, 20 of them) stays linear in the number of endpoints rather
        // than quadratic in urls. [PREFERENCE] order is the group's order, so the
        // best url of each cluster is the one that becomes its head — and since
        // `sameRelay` is symmetric, every head really has been held up against
        // every other head as well as against the leader.
        val heads = ArrayList<NormalizedRelayUrl>()
        for (url in unmatched) {
            val print = prints.getValue(url)
            // [bar] is moot here and passed only so one rule decides every
            // fold: entry to `unmatched` already demanded [minSample] on both
            // sides, so nothing thin can reach this loop.
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
        // …AND ONLY NOW THE SCHEME TWINS, onto wherever the secure url itself
        // ended up rather than onto the secure url as it was found.
        //
        // `wss://x/inbox` may have folded onto the leader two lines ago, and
        // pointing `ws://x/inbox` at it regardless would leave the fan-out
        // dialling a url that is itself an alias — [canonicalOf] is one hop, by
        // design, so a chain here is a duplicate that survives the fold.
        for ((plain, secure) in twins) {
            val canonical = resolve(secure)
            // Two verdicts that disagree could walk the chain back to the url we
            // are folding. Same guard, same reason as [adopt]: a url pinned as
            // its own duplicate is `measured` forever and never revisited.
            if (canonical == plain) continue
            folded[plain] = canonical
            canonicals += canonical
            distinct -= plain
            folds[plain] = canonical
        }
        // Only when something was actually held up against it: a leader whose
        // whole group was unreachable, or answered too thinly to compare, has
        // been measured against nothing and has proved nothing. And only when
        // it is not a canonical — if anything folded onto it, it is the class,
        // not a singleton. Which is why the twin folds land ABOVE this check: a
        // group of nothing but a `ws://`/`wss://` pair compares nothing, so the
        // leader is never cleared here, and being folded onto is the only thing
        // that gives it a verdict and stops the group coming back every pass.
        if (compared > 0 && leaderPrint.size >= minSample && leader !in canonicals) {
            markDistinct(leader)
            cleared += leader
        }
        return Learned(folds, cleared, twins.keys)
    }

    /**
     * Fold a group NOTHING could be read from onto its preferred survivor, on the
     * strength of the shared hostname alone.
     *
     * **This is the only fold in this class that rests on no measurement, and it
     * must be read as the policy choice it is rather than as a verdict.** Every
     * other path here refuses to conclude anything from silence, on the ground
     * that a path is routinely a different endpoint. This one concludes the
     * opposite by default: the urls share a DNS name, every one of them answered,
     * none of them served anything through any filter we know, so nothing
     * distinguishes them and they collapse.
     *
     * What makes it defensible is what it costs when it is wrong. A fold that
     * silently stops mirroring a relay is this component's cardinal sin — but a
     * url nothing can be read from is mirroring nothing, so there is no stream to
     * lose today. What IS lost is the day the relay starts answering us: the
     * verdict stands until it expires, and the other urls are not dialled again
     * until then.
     *
     * **And it can be flatly wrong about somebody's server.** Measured over 45
     * multi-url hosts taken from live relay lists, the rule fires on three, and
     * one of them is `filter.nostr.wine` — whose urls are
     * `/npub1…?broadcast=true`, a PER-USER filtered endpoint behind
     * `auth_required` and `payment_required`. Those four paths are four different
     * users' feeds; nothing about them is the same relay, and only the payment
     * wall makes them look alike. See [AliasFolding.foldUnreadableGroups] for the
     * switch and the argument.
     *
     * Every url must have ANSWERED. A group holding one url our transport merely
     * failed to reach is not "all alike", it is "we do not know", and folding it
     * would turn our own outage into a claim about their server.
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
     * The one fold the urls decide by themselves: `ws://x` and `wss://x` are one
     * endpoint reached two ways, so when BOTH of them answer, the plain one
     * folds onto the secure one.
     *
     * Everything else here refuses to read anything off a url, and for good
     * reason — a path is routinely a different endpoint (haven splits `/inbox`
     * from the public pool, and NIP-17 made that ordinary), so `/inbox` folding
     * onto the bare host on the strength of its spelling would silently stop
     * mirroring half a relay. **A scheme is not an endpoint.** It is how we
     * reach one, and no relay software in this corpus serves a DIFFERENT event
     * pool on plain `ws` than it serves on TLS at the same host and path.
     *
     * Which is why this is not merely a shortcut for something the fingerprint
     * would have reached anyway. Two twins whose windows agree already fold on
     * containment; what this adds is the pairs that containment cannot answer
     * for, and there are two shapes of those:
     *
     *  - **the window is too thin to decide.** A relay holding nine events hands
     *    both twins the same nine, and nine is under [minSample] — so nothing
     *    folds, nothing is cleared, the group is handed back by [unresolved] on
     *    every pass, and the host spends a pass's wall clock forever to learn
     *    what its two urls already said. (`groups.satsdisco.com` was the example
     *    here; it turned out to be answerable through
     *    [GROUP_METADATA_KINDS] and no longer needs the pairing to be decided.)
     *  - **only one twin has a verdict.** [toProbe] now re-dials the secure twin
     *    of an unmeasured plain url precisely so this can fire; without the
     *    pairing the plain url would be compared to the group's leader — a
     *    genuinely different endpoint — disagree with it correctly, and be
     *    published as a relay in its own right.
     *
     * **"Both work" is the whole condition, and it is what keeps this honest.**
     * A print in hand means the url was dialled and the relay answered; a url
     * missing from [prints] was silent, refused, or never asked (our own
     * transport can decline it), and none of those is evidence about the pair.
     * So a `ws://` url whose secure twin said nothing is left exactly where it
     * was — we would be folding a live url onto a dead one — and a `ws://` url
     * that says nothing itself is left to [HostStrikes], which is what evicts a
     * url that has stopped answering.
     *
     * **THE WINDOWS STILL HAVE A VETO, IN THE ONE DIRECTION THAT CAN LOSE
     * DATA.** Everything the plain url served must already be on the secure one,
     * at the same [minOverlap] the ordinary fold uses. Note what that is not: it
     * is not [sameRelay], which is symmetric and floored at [minSample], and
     * both differences are the point.
     *
     *  - **No floor**, because the pairing is the argument and nine events are
     *    plenty to show they do not contradict it. The floor exists so that two
     *    quiet relays are not called identical on a coincidence; here the urls
     *    have already said they are one endpoint.
     *  - **One direction**, because that is the asymmetry the fold cares about.
     *    A plain url whose window is contained in its secure twin's is a url we
     *    can stop dialling for free — everything it had, the survivor has. The
     *    reverse, a `ws://` serving 500 events beside a `wss://` serving nine or
     *    none, is the shape that would quietly stop mirroring a relay, and it is
     *    refused here whatever the two urls are called.
     *
     * Which yields the property that answers "so the url overrules the
     * measurement?" — it never does. **Where both windows clear [minSample],
     * this folds a strict SUBSET of what [sameRelay] would fold.** The two
     * divide the same intersection by different denominators: by the plain
     * window here, by the smaller of the pair there. When the plain window IS
     * the smaller they are the same test; when it is the larger, dividing by it
     * is the stricter one. So the pairing never contradicts a containment that
     * had the sample to speak — it lowers the floor for the pairs that did not,
     * and folds a plain url that served nothing at all.
     */
    private fun schemeTwins(
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        prints: Map<NormalizedRelayUrl, Set<String>>,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        // Nothing plain to fold, and therefore nothing to build a map for. This
        // runs for every group of every pass against a corpus that is 94%
        // `wss://`, so the cheap test comes first.
        if (group.none { !isSecure(it.url) }) return emptyMap()
        val secure = secureTwins(group)
        if (secure.isEmpty()) return emptyMap()
        val twins = LinkedHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        for (url in group) {
            // Never the leader, even when its group holds a secure twin of it.
            // The leader is the yardstick every other url was just measured
            // against; folding it away would leave a group of verdicts pointing
            // at an alias, which [canonicalOf] does not follow. What that costs
            // is narrow and worth naming: a host whose `wss://` url was silent
            // on the pass that first folded the group has a `ws://` survivor
            // ([leaderOf] keeps a canonical in place deliberately, so the bands
            // and records of every other participant keep naming one string),
            // and it keeps it until those records lapse.
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
     * Is everything [print] handed over already in [survivor]'s window — i.e.
     * would folding the first url away cost us nothing?
     *
     * An empty window passes vacuously: a url that served nothing has nothing to
     * lose. That is the same reading of silence the rest of this class takes,
     * and it is safe HERE — where it decides only whether to stop dialling a url
     * whose twin is already dialled — in a way it would not be if it were being
     * asked to prove two urls equal.
     */
    private fun alreadyServedBy(
        print: Set<String>,
        survivor: Set<String>,
    ): Boolean = print.isEmpty() || print.count { it in survivor }.toDouble() / print.size >= minOverlap

    /**
     * The `ws://` url of the same endpoint as [secure], if [group] holds one and
     * nothing has folded it already.
     *
     * Asked by [AliasFolding] of a yardstick whose window came back under
     * [minSample]. Such a url can measure nothing — but it is still one half of
     * a pair [schemeTwins] can decide, and the other half is one dial away.
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
     * Do these two windows come from one relay?
     *
     * Both sides must have handed over at least [Bar.window] ids — the guard
     * against calling two quiet relays identical because neither said much —
     * they must share at least [Bar.shared] of them outright, and the smaller
     * window must be [minOverlap] contained in the larger.
     *
     * [bar] rather than [minSample] directly because both halves of it depend
     * on the filter the two windows came through; see [foldBar].
     */
    private fun sameRelay(
        a: Set<String>,
        b: Set<String>,
        bar: Bar,
    ): Boolean {
        val smaller = minOf(a.size, b.size)
        if (smaller < bar.window) return false
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        // The ratio is not enough on its own once the window floor is small —
        // see [Bar.shared], which is what stops a two-id coincidence folding a
        // path that serves a group nobody else does.
        if (shared < bar.shared) return false
        return shared.toDouble() / smaller >= minOverlap
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
         * What a relay is asked once it has refused both a bare filter and
         * [AliasProbe.FALLBACK_KINDS]: its NIP-29 group metadata.
         *
         * That pair of refusals is not a generic failure — it is a SIGNATURE. A
         * relay running khatru's groups mode answers any unscoped query with
         * `CLOSED blocked: invalid query, must have 'h', 'e' or 'a' tag`,
         * whatever kinds are named, so both existing rungs of the ladder come
         * back empty and the host has no yardstick and can never fold. Kind
         * 39000 is the one window such a relay serves unscoped and
         * unauthenticated — verified on `groups.satsdisco.com` (55 ids),
         * `groups.0xchat.com` (1,302), `groups.fiatjaf.com` (16) and
         * `relay29.notoshi.win` (27), every one of which had been refusing the
         * first two rungs.
         *
         * A list of groups is a fine fingerprint for the one question the fold
         * asks: every minted path measured on these hosts served the IDENTICAL
         * list, containment 1.000. It is also unusually stable — addressable
         * events that change when somebody edits a group, not a moving window —
         * so it is the rare fingerprint that barely drifts between two dials.
         */
        val GROUP_METADATA_KINDS = listOf(39000)

        /**
         * The floor for a [GROUP_METADATA_KINDS] window — see
         * [groupMetadataMinSample] for why it is not [DEFAULT_MIN_SAMPLE].
         */
        const val DEFAULT_GROUP_METADATA_MIN_SAMPLE = 3

        /**
         * Half. The two windows are taken seconds apart against a moving feed
         * and one may be truncated by the peer's `default_limit`, so demanding
         * near-identity would fold nothing on exactly the busy relays where the
         * duplication costs the most.
         */
        const val DEFAULT_MIN_OVERLAP = 0.5

        /**
         * What a url must score against ITSELF for its host to be measurable at
         * all — see [reproducible] for the two clusters this sits between.
         */
        const val DEFAULT_MIN_SELF_OVERLAP = 0.9

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
                for ((dest, values) in relay.bindings) into.getOrPut(dest) { HashSet() }.addAll(values)
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

        /** TLS, the half of a scheme pair we keep — see [schemeTwins]. */
        private fun isSecure(url: String): Boolean = url.startsWith("wss://", true)

        /**
         * What a url reaches with the scheme taken out of it — `host/path` — so
         * that `ws://x/p` and `wss://x/p` land on one key.
         *
         * Null when the url names a port its own scheme would not have used
         * anyway, and that refusal is the point rather than an omission. The
         * schemes carry different default ports (80 against 443), so "same host,
         * same path" is already two different sockets and the pairing rests
         * entirely on those two being the same SERVICE. `wss://x:8443` is
         * somebody's second endpoint on a port they chose deliberately, and
         * nothing about `ws://x` says it is the same one — so it is left to the
         * fingerprint, which can actually tell.
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
