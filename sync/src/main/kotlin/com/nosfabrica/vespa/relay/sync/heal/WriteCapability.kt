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
package com.nosfabrica.vespa.relay.sync.heal

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which relays will take our repairs, learned once per relay. A policy
 * refusal closes the relay at once; silence closes it only after
 * [strikeThreshold] unanswered publishes spread over at least
 * [MIN_DISTINCT_PASSES] drain passes, because one bad session must not close
 * a relay. Closed gates only the push; the refused-ids filter still
 * suppresses the relay's stale offers.
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

    /** Relays pushed to and heard back from; the health line prints `closed/probed`. */
    fun probedCount(): Int = relays.size

    /** A policy refusal. One is enough: it is the relay's configuration, not its mood. */
    fun close(
        url: NormalizedRelayUrl,
        reason: String,
    ) {
        val before = relays.put(url, State(closed = true, reason = reason))
        if (before?.closed != true) {
            System.err.println("router: heal ${url.url} closed for writes — $reason")
        }
    }

    /**
     * The relay answered: clears any strikes and records the relay as probed.
     * One `compute`, so a concurrent [strike] is not lost. A closed relay
     * stays closed.
     */
    fun succeeded(url: NormalizedRelayUrl) {
        relays.compute(url) { _, before -> if (before?.closed == true) before else State() }
    }

    /**
     * An unanswered publish in drain pass [passId]. Closes the relay once the
     * strikes reach the threshold and span at least [MIN_DISTINCT_PASSES] passes.
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

        /** Strikes must span this many drain passes, so one bad session cannot close a relay. */
        private const val MIN_DISTINCT_PASSES = 2
    }
}
