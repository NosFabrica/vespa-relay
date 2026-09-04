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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Does a relay answer the same question the same way twice.
 *
 * One filter, one anchor, asked twice. The window sits [ANCHOR_LAG_SECONDS]
 * into the past so "the answer changed" cannot be explained by new events,
 * indexing lag or a shard catching up. The two answers are compared to each
 * other and never to the filter, so a relay serving the same wrong slice every
 * time passes here; that question is [RelayCompliance]'s.
 *
 * A relay with no stable window has no stable band, so every cycle re-downloads
 * what the last one took. That is why a failure excludes rather than
 * downgrades; the verdict expires with [RelayVerdictRecord], so a relay that
 * settles comes back. [Verdict.UNMEASURABLE] is the absence of a claim and
 * never excludes: a relay holding nine events is not misbehaving.
 */
class RelayConsistency(
    /** The smallest window worth deciding on; two answers of four events look alike whatever the relay does. */
    private val minSample: Int = RelayAliases.DEFAULT_MIN_SAMPLE,
    /** How much of the smaller answer must appear in the larger. The fold's bar, for the fold's reason. */
    private val minOverlap: Double = RelayAliases.DEFAULT_MIN_SELF_OVERLAP,
) {
    /** What one pair of answers proved. */
    enum class Verdict {
        CONSISTENT,

        INCONSISTENT,

        /** Nothing was proved either way: too little came back, or nothing did. */
        UNMEASURABLE,
    }

    private val consistent = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    private val inconsistent = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** False is "no verdict", not "bad". */
    fun measured(url: NormalizedRelayUrl): Boolean = url in consistent || url in inconsistent

    /** True for everything but a url positively measured inconsistent; a fan-out is not narrowed by silence. */
    fun usable(url: NormalizedRelayUrl): Boolean = url !in inconsistent

    /** The urls of [candidates] this component refuses, in the order given. */
    fun unusable(candidates: Collection<NormalizedRelayUrl>): List<NormalizedRelayUrl> = candidates.filter { !usable(it) }

    fun refusedCount(): Int = inconsistent.size

    /**
     * Asked per candidate rather than reported as set sizes: [replace] only
     * rewrites the urls it is handed, so the sets can hold verdicts about urls
     * no stream discovers any more. See [ConsistencyPass.report].
     */
    fun isConsistent(url: NormalizedRelayUrl): Boolean = url in consistent

    fun isInconsistent(url: NormalizedRelayUrl): Boolean = url in inconsistent

    /** Everything with no verdict yet. */
    fun toProbe(candidates: Collection<NormalizedRelayUrl>): List<NormalizedRelayUrl> = candidates.filter { !measured(it) }

    /** Null is a relay that could not be asked; like an empty answer it is not evidence about how it answers. */
    fun decide(
        first: Set<String>?,
        second: Set<String>?,
    ): Verdict {
        if (first == null || second == null) return Verdict.UNMEASURABLE
        val smaller = minOf(first.size, second.size)
        // A relay that holds almost nothing is the commonest way here, and must not be excluded.
        if (smaller < minSample) return Verdict.UNMEASURABLE
        return if (containment(first, second) >= minOverlap) Verdict.CONSISTENT else Verdict.INCONSISTENT
    }

    /**
     * How much of the smaller answer is inside the larger. Containment rather
     * than a symmetric ratio, as in [RelayAliases]: a relay may truncate one ask
     * at its own limit, and a prefix of the same answer is the same answer.
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

    /** Adopt verdicts a previous run published. [RelayVerdictRecord] drops the stale ones first. */
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
     * Set this candidate set's verdicts to exactly what the store holds, one
     * url at a time. This object is shared by every stream, so a bulk [forget]
     * followed by a bulk [adopt] would leave a window in which another stream's
     * [unusable] sees no refusals at all.
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

                // No verdict in the store: expired, or never taken. Forgetting
                // it is what gives the record's TTL its teeth.
                else -> {
                    consistent -= url
                    inconsistent -= url
                }
            }
        }
    }

    /**
     * Drop every verdict held about these urls, so this cache cannot outlive
     * the store it caches. Same rule as [RelayAliases.forget].
     */
    fun forget(urls: Collection<NormalizedRelayUrl>) {
        for (url in urls) {
            consistent -= url
            inconsistent -= url
        }
    }

    companion object {
        /**
         * How far back the pair of asks is anchored. The fold's one-minute
         * anchor settles indexing lag; a week also removes a sharded backend's
         * replication from the explanations, which is all the depth is for.
         * Re-take with `RelaySelfConsistencyProbe`, more than once, before moving it.
         */
        const val ANCHOR_LAG_SECONDS = 7L * 24 * 60 * 60

        /** The newest `created_at` a stability walk will look at, given the clock. */
        fun settledAnchor(now: Long): Long = now - ANCHOR_LAG_SECONDS
    }
}
