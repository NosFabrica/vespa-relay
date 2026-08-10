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
package com.nosfabrica.vespa.relay.router.heal

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which relays will take our repairs, learned by asking.
 *
 * This is what makes pushing to a 16k-relay fan-out affordable: the cost is
 * **one-time per relay**, not per cycle. Probe until the relay says no on
 * policy (once is enough — that is its configuration speaking) or stays silent
 * across enough separate passes, mark it, and never spend another publish on
 * it.
 *
 * The silence rule is deliberately slow. An unanswered `EVENT` is ambiguous —
 * quartz reports it as a transport failure, which covers a relay that never
 * connected, one that dropped, and one that simply ignores writes — so strikes
 * must accumulate across **at least two separate drain passes** before they
 * close anything. One bad session cannot close a relay, which is the same
 * conservatism the NIP-45 idle-window trap in AGENTS.md teaches: a relay
 * steadily answering can look exactly like one refusing if the window is sized
 * off the queue instead of the slowest single answer.
 *
 * Write-closed gates only the PUSH. The refused-ids filter still suppresses
 * that relay's stale offers, which is precisely why the two structures are
 * separate.
 */
class WriteCapability(
    private val strikeThreshold: Int = DEFAULT_STRIKES,
) {
    data class State(
        val closed: Boolean = false,
        val reason: String? = null,
        val strikes: Int = 0,
        /** Distinct drain passes that produced a strike. */
        val passes: Set<Long> = emptySet(),
    )

    private val relays = ConcurrentHashMap<NormalizedRelayUrl, State>()

    fun state(url: NormalizedRelayUrl): State = relays[url] ?: State()

    fun isClosed(url: NormalizedRelayUrl): Boolean = relays[url]?.closed == true

    fun closedCount(): Int = relays.values.count { it.closed }

    fun probedCount(): Int = relays.size

    /** A policy refusal. One is enough: it is their configuration, not their mood. */
    fun close(
        url: NormalizedRelayUrl,
        reason: String,
    ) {
        val before = relays.put(url, State(closed = true, reason = reason))
        if (before?.closed != true) {
            System.err.println("router: heal ${url.url} closed for writes — $reason")
        }
    }

    /** The relay took it. Clears any accumulated doubt. */
    fun succeeded(url: NormalizedRelayUrl) {
        relays[url]?.takeIf { !it.closed && it.strikes > 0 }?.let {
            relays[url] = State()
        }
    }

    /**
     * An unanswered publish in drain pass [passId]. Closes the relay only once
     * the strikes have come from at least [MIN_DISTINCT_PASSES] different
     * passes AND reached the threshold.
     */
    fun strike(
        url: NormalizedRelayUrl,
        passId: Long,
    ) {
        val next =
            relays.compute(url) { _, before ->
                val prior = before ?: State()
                if (prior.closed) return@compute prior
                State(strikes = prior.strikes + 1, passes = prior.passes + passId)
            } ?: return
        if (!next.closed && next.strikes >= strikeThreshold && next.passes.size >= MIN_DISTINCT_PASSES) {
            relays[url] = State(closed = true, reason = "no OK across ${next.passes.size} passes (${next.strikes} strikes)")
            System.err.println(
                "router: heal ${url.url} closed for writes — never answered an OK across " +
                    "${next.passes.size} separate passes (${next.strikes} publishes)",
            )
        }
    }

    companion object {
        private const val DEFAULT_STRIKES = 3

        /**
         * Strikes must span this many drain passes. Two is the smallest number
         * that makes "one bad session closed a relay forever" impossible.
         */
        private const val MIN_DISTINCT_PASSES = 2
    }
}
