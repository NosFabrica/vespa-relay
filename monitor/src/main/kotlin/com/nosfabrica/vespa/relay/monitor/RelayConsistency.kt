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

import com.nosfabrica.vespa.relay.fold.RelayAliases

/**
 * Does a relay answer the same question the same way twice: one filter, one anchor, asked twice,
 * the answers compared to each other and never to the filter. A failure excludes rather than
 * downgrades; [Verdict.UNMEASURABLE] is the absence of a claim and never excludes. Holds no verdicts: the store does.
 */
class RelayConsistency(
    /** The smallest window worth deciding on. */
    private val minSample: Int = RelayAliases.DEFAULT_MIN_SAMPLE,
    /** How much of the smaller answer must appear in the larger. */
    private val minOverlap: Double = RelayAliases.DEFAULT_MIN_SELF_OVERLAP,
) {
    /** What one pair of answers proved. */
    enum class Verdict {
        CONSISTENT,

        INCONSISTENT,

        /** Nothing was proved either way: too little came back, or nothing did. */
        UNMEASURABLE,
    }

    /** Null is a relay that could not be asked, which like an empty answer is no evidence. */
    fun decide(
        first: Set<String>?,
        second: Set<String>?,
    ): Verdict = judge(first, second)?.verdict ?: Verdict.UNMEASURABLE

    /**
     * A pair of answers that proved something: the two windows, the ids they share, and how much of the
     * smaller is inside the larger. Containment, as in [RelayAliases], because a relay may truncate one ask at its own limit.
     */
    data class Decided(
        val verdict: Verdict,
        val first: Set<String>,
        val second: Set<String>,
        val shared: Int,
        val score: Double,
    )

    /** [decide], carrying the windows and the counts when it proved something; null is [Verdict.UNMEASURABLE]. */
    fun judge(
        first: Set<String>?,
        second: Set<String>?,
    ): Decided? {
        if (first == null || second == null) return null
        val smaller = minOf(first.size, second.size)
        if (smaller < minSample) return null
        val shared = if (first.size <= second.size) first.count { it in second } else second.count { it in first }
        val score = if (smaller == 0) 0.0 else shared.toDouble() / smaller
        return Decided(if (score >= minOverlap) Verdict.CONSISTENT else Verdict.INCONSISTENT, first, second, shared, score)
    }

    companion object {
        /**
         * How far back the pair of asks is anchored: deep enough to take a sharded backend's
         * replication out of the explanations. Re-take with `RelaySelfConsistencyProbe` before moving it.
         */
        const val ANCHOR_LAG_SECONDS = 7L * 24 * 60 * 60

        /** The newest `created_at` a stability walk will look at, given the clock. */
        fun settledAnchor(now: Long): Long = now - ANCHOR_LAG_SECONDS
    }
}
