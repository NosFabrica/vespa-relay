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
 * Does a relay answer the same question the same way twice — and if it does not,
 * it is not worth dialling.
 *
 * ## What this is measuring
 *
 * One filter, one anchor, asked TWICE. A relay that holds a fixed set of events
 * below a fixed timestamp has exactly one correct answer, so the two must match;
 * a relay that returns a different slice each time is not answering the filter,
 * it is answering something else. [ANCHOR_LAG_SECONDS] puts the window a week
 * into the past precisely so that "the answer changed" cannot be explained by
 * new events arriving, by indexing lag, or by a shard that has not caught up.
 *
 * ## What it costs to leave one in
 *
 * Everything. A relay whose window is a fresh random slice has NO stable band:
 * every cycle the cursor covers a different part of its history, so the next
 * cycle re-downloads what the last one already took, forever. Measured on this
 * mirror: millions of duplicated events, and cycles stretched from two hours to
 * five by relays doing nothing but re-serving what we already hold. The fold
 * cannot help — these are not duplicate URLS, they are one url that is a
 * different relay every time you ask.
 *
 * That is the whole justification for excluding rather than downgrading. The
 * measurement does not say the operator is dishonest and this does not claim it
 * does; it says the relay cannot be synced against, which is a property of the
 * server and not of the person, and it is the property the fan-out actually
 * needs. A relay that starts answering consistently is picked up on the next
 * re-measure, because the verdict expires — see [RelayAliasRecord].
 *
 * ## The three answers, and why the third is not the second
 *
 * [Verdict.CONSISTENT] and [Verdict.INCONSISTENT] are claims. [Verdict.UNMEASURABLE]
 * is the absence of one: a relay that did not answer, or answered with fewer
 * than [minSample] events, has told us nothing either way. It must NOT be
 * excluded — a small relay holding nine events is not misbehaving, and a relay
 * that was down during the pass is [HostStrikes]' business, not this one's. The
 * only url this component removes from a fan-out is one it has positively
 * measured and positively failed.
 */
class RelayConsistency(
    /**
     * The smallest window worth deciding on. Two answers of four events look
     * alike whatever the relay does, and a relay that simply holds very little
     * must never be called inconsistent for it.
     */
    private val minSample: Int = RelayAliases.DEFAULT_MIN_SAMPLE,
    /**
     * How much of the smaller answer must appear in the larger before the two
     * count as the same answer.
     *
     * The same bar the fold uses against a leader, and for the same reason:
     * measured, the gap between a relay that agrees with itself and one that
     * does not is enormous and empty. `nos.lol`, `nostr.oxtr.dev` and
     * `relay.lightning.pub` score 1.000 at every anchor depth in every run,
     * while `fiatjaf.com` (0.618-0.826) and `multiplexer.huszonegy.world`
     * (0.446-0.916) never reach it and do not even score consistently across
     * runs. Nothing measured lands between 0.92 and 1.000, which is why 0.9
     * costs the good actors nothing — see [ANCHOR_LAG_SECONDS] for the numbers.
     */
    private val minOverlap: Double = RelayAliases.DEFAULT_MIN_SELF_OVERLAP,
) {
    /** What one pair of answers proved. */
    enum class Verdict {
        /** The two answers matched. Dial it. */
        CONSISTENT,

        /** The two answers did not. Do not dial it. */
        INCONSISTENT,

        /** Nothing was proved either way — too little came back, or nothing did. */
        UNMEASURABLE,
    }

    /** Urls measured and found stable. */
    private val consistent = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Urls measured and found NOT stable — the ones that leave the fan-out. */
    private val inconsistent = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Has anything been decided about this url? False is "no verdict", not "bad". */
    fun measured(url: NormalizedRelayUrl): Boolean = url in consistent || url in inconsistent

    /**
     * Should the fan-out dial this url?
     *
     * True for everything except a url positively measured as inconsistent —
     * including every url nothing has looked at yet. A fan-out must not be
     * narrowed by silence.
     */
    fun usable(url: NormalizedRelayUrl): Boolean = url !in inconsistent

    /** The urls of [candidates] this component refuses, in the order given. */
    fun unusable(candidates: Collection<NormalizedRelayUrl>): List<NormalizedRelayUrl> = candidates.filter { !usable(it) }

    /** How many urls are currently refused. */
    fun refusedCount(): Int = inconsistent.size

    /**
     * Which verdict this url carries, asked one at a time so a caller can
     * partition a candidate set in a single walk.
     *
     * Asked of the CANDIDATES rather than reported as the sizes of the two sets:
     * [replace] only rewrites the urls it is handed, so both sets can hold
     * verdicts about urls no stream discovers any more, and only counting within
     * the candidate set makes `candidates = folded + consistent + inconsistent +
     * unmeasured` close — see [ConsistencyPass.report].
     */
    fun isConsistent(url: NormalizedRelayUrl): Boolean = url in consistent

    fun isInconsistent(url: NormalizedRelayUrl): Boolean = url in inconsistent

    /** Which urls still need measuring: everything with no verdict yet. */
    fun toProbe(candidates: Collection<NormalizedRelayUrl>): List<NormalizedRelayUrl> = candidates.filter { !measured(it) }

    /**
     * Compare two answers to one filter.
     *
     * Null is a relay that could not be asked at all, which is not the same as
     * one that answered with nothing — but both land on [Verdict.UNMEASURABLE]
     * here, because neither is evidence about how the relay answers.
     */
    fun decide(
        first: Set<String>?,
        second: Set<String>?,
    ): Verdict {
        if (first == null || second == null) return Verdict.UNMEASURABLE
        val smaller = minOf(first.size, second.size)
        // Below the sample floor nothing is decidable — and a relay that holds
        // almost nothing is the commonest way to land here, which is exactly the
        // case that must not be excluded.
        if (smaller < minSample) return Verdict.UNMEASURABLE
        return if (containment(first, second) >= minOverlap) Verdict.CONSISTENT else Verdict.INCONSISTENT
    }

    /**
     * How much of the smaller answer is inside the larger.
     *
     * Containment rather than a symmetric ratio, matching [RelayAliases]: a
     * relay entitled to truncate at its own `default_limit` may answer one of
     * the two asks more shallowly, and a prefix of the same answer is still the
     * same answer.
     */
    fun containment(
        first: Set<String>,
        second: Set<String>,
    ): Double {
        val smaller = minOf(first.size, second.size)
        if (smaller == 0) return 0.0
        val shared = if (first.size <= second.size) first.count { it in second } else second.count { it in first }
        return shared.toDouble() / smaller
    }

    /** Shared ids between two answers, for the evidence string on the record. */
    fun shared(
        first: Set<String>,
        second: Set<String>,
    ): Int = if (first.size <= second.size) first.count { it in second } else second.count { it in first }

    /** Record a verdict this pass just measured. [Verdict.UNMEASURABLE] is not one. */
    fun learn(
        url: NormalizedRelayUrl,
        verdict: Verdict,
    ) {
        when (verdict) {
            Verdict.CONSISTENT -> {
                inconsistent -= url
                consistent += url
            }

            Verdict.INCONSISTENT -> {
                consistent -= url
                inconsistent += url
            }

            Verdict.UNMEASURABLE -> {
                Unit
            }
        }
    }

    /**
     * Adopt verdicts a previous run published. [RelayAliasRecord] drops the
     * stale ones before they get here, so anything arriving is still current.
     */
    fun adopt(
        stable: Set<NormalizedRelayUrl>,
        unstable: Set<NormalizedRelayUrl>,
    ) {
        for (url in stable) {
            inconsistent -= url
            consistent += url
        }
        for (url in unstable) {
            consistent -= url
            inconsistent += url
        }
    }

    /**
     * Set this candidate set's verdicts to exactly what the store holds, in ONE
     * pass per url rather than a bulk forget followed by a bulk adopt.
     *
     * The two-step version had a window — multi-second, on a fan-out of
     * thousands — in which every refusal had been dropped and none re-adopted.
     * This object is shared by every stream, so another stream's [unusable] call
     * landing inside that window saw an empty refusal set and put every
     * inconsistent relay back into its cycle, which is the precise thing the
     * gate exists to prevent. Each url now moves straight from its old verdict
     * to its new one and is never transiently absent.
     */
    fun replace(
        candidates: Collection<NormalizedRelayUrl>,
        stable: Set<NormalizedRelayUrl>,
        unstable: Set<NormalizedRelayUrl>,
    ) {
        for (url in candidates) {
            when (url) {
                in unstable -> {
                    inconsistent += url
                    consistent -= url
                }

                in stable -> {
                    consistent += url
                    inconsistent -= url
                }

                // No verdict in the store — expired, or never taken. Forgetting
                // it here is what gives the record's TTL its teeth.
                else -> {
                    consistent -= url
                    inconsistent -= url
                }
            }
        }
    }

    /**
     * Drop every verdict held about these urls.
     *
     * Called before each adopt so the store stays authoritative and the record's
     * TTL keeps its teeth — a relay excluded on a verdict that has since expired
     * must come back into the fan-out, and it can only do that if this cache
     * cannot outlive what it caches. The same rule, and the same reasoning, as
     * [RelayAliases.forget].
     */
    fun forget(urls: Collection<NormalizedRelayUrl>) {
        for (url in urls) {
            consistent -= url
            inconsistent -= url
        }
    }

    companion object {
        /**
         * How far back the pair of asks is anchored: SEVEN DAYS.
         *
         * The fold's own anchor is one minute ([AliasProbe.ANCHOR_LAG_SECONDS]),
         * which settles indexing lag and nothing else. That is enough to compare
         * two DIFFERENT urls seconds apart; it is not enough to accuse a relay of
         * being unusable, because at that depth a perfectly good sharded backend
         * still disagrees with itself while its replicas catch up.
         *
         * Measured, walking each url twice at four anchor depths, TWICE — and
         * running it a second time is what produced the actual finding:
         *
         * ```
         *                              1min   1hour   1day   7days
         * nos.lol                  #1  1.000  1.000   1.000  1.000
         *                          #2  1.000  1.000   1.000  1.000
         * nostr.oxtr.dev           #1  1.000  1.000   1.000  1.000
         *                          #2  1.000  1.000   1.000  1.000
         * relay.lightning.pub      #1  1.000  1.000   1.000  1.000
         *                          #2  1.000  1.000   1.000  1.000
         * multiplexer.huszonegy    #1  0.446  0.712   0.770  0.964
         *                          #2  0.782  0.908   0.916  0.654
         * fiatjaf.com              #1  0.803  0.664   0.694  0.715
         *                          #2  0.719  0.720   0.618  0.826
         * ```
         *
         * **A stable relay is 1.000 at every depth in every run.** That is the
         * property the depth is chosen for: the good actors never come near the
         * bar, so nothing is bought by being shallower and nothing is risked by
         * being deeper.
         *
         * The first run alone suggested huszonegy merely needed time to
         * replicate — 0.446 climbing to 0.964. The second run scored the same
         * relay at the same depth 0.654, which kills that reading: its SCORE is
         * not reproducible either, and a relay whose disagreement with itself
         * varies by 0.31 between runs is not a relay that has settled. Both
         * hosts are unstable at every depth; the week simply removes our own
         * anchor from the list of explanations, which is all it was ever for.
         *
         * A single run would have shipped the wrong constant here. If this bar
         * or this depth is ever moved, run `RelaySelfConsistencyProbe` more than
         * once before believing it.
         *
         * Cheap, because depth costs nothing here: a week-old window is exactly
         * as easy to serve as a fresh one, and the events go to ingest either
         * way. `RelaySelfConsistencyProbe` is how these numbers were taken and
         * how to re-take them if this bar is ever moved.
         */
        const val ANCHOR_LAG_SECONDS = 7L * 24 * 60 * 60

        /** The newest `created_at` a stability walk will look at, given the clock. */
        fun settledAnchor(now: Long): Long = now - ANCHOR_LAG_SECONDS
    }
}
