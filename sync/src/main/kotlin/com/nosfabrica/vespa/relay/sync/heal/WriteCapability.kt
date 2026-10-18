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
 * Which relays will take our repairs. A policy refusal closes the relay at once; silence closes it
 * only after [strikeThreshold] unanswered publishes over at least [MIN_DISTINCT_PASSES] drain
 * passes. A closure gates only the push, and lapses after [closedForMs].
 */
class WriteCapability(
    private val strikeThreshold: Int = DEFAULT_STRIKES,
    private val closedForMs: Long = CLOSURE_TTL_MS,
    private val nowMs: () -> Long = System::currentTimeMillis,
    private val log: (String) -> Unit = System.err::println,
) {
    data class State(
        val closed: Boolean = false,
        val reason: String? = null,
        val strikes: Int = 0,
        /** Distinct drain passes that produced a strike. */
        val passes: Set<Long> = emptySet(),
        /** When the relay was closed; the closure lapses [CLOSURE_TTL_MS] later. */
        val closedAtMs: Long = 0,
    )

    private val relays = ConcurrentHashMap<NormalizedRelayUrl, State>()

    private fun State.standing(now: Long): Boolean = closed && now - closedAtMs < closedForMs

    /** [before] with a lapsed closure read as a clean slate. */
    private fun current(
        before: State?,
        now: Long,
    ): State = if (before == null || (before.closed && !before.standing(now))) State() else before

    fun state(url: NormalizedRelayUrl): State = current(relays[url], nowMs())

    fun isClosed(url: NormalizedRelayUrl): Boolean = relays[url]?.standing(nowMs()) == true

    fun closedCount(): Int {
        val now = nowMs()
        return relays.values.count { it.standing(now) }
    }

    /** Relays pushed to and heard back from; the health line prints `closed/probed`. */
    fun probedCount(): Int = relays.size

    /** A policy refusal. One is enough: it is the relay's configuration, not its mood. */
    fun close(
        url: NormalizedRelayUrl,
        reason: String,
    ) {
        val now = nowMs()
        var announce = false
        relays.compute(url) { _, before ->
            announce = before?.standing(now) != true
            State(closed = true, reason = reason, closedAtMs = now)
        }
        if (announce) log("router: heal ${url.url} closed for writes for ${closedForMs / 3_600_000}h — $reason")
    }

    /** The relay answered: clears any strikes and records the relay as probed. A standing closure stays. */
    fun succeeded(url: NormalizedRelayUrl) {
        val now = nowMs()
        relays.compute(url) { _, before -> if (before?.standing(now) == true) before else State() }
    }

    /**
     * An unanswered publish in drain pass [passId]. Closes the relay, in the same step, once the
     * strikes reach the threshold and span at least [MIN_DISTINCT_PASSES] passes.
     */
    fun strike(
        url: NormalizedRelayUrl,
        passId: Long,
    ) {
        val now = nowMs()
        var closedNow: State? = null
        relays.compute(url) { _, before ->
            val prior = current(before, now)
            if (prior.closed) return@compute prior
            val strikes = prior.strikes + 1
            val passes = prior.passes + passId
            if (strikes >= strikeThreshold && passes.size >= MIN_DISTINCT_PASSES) {
                State(closed = true, reason = "no OK across ${passes.size} passes ($strikes strikes)", strikes = strikes, passes = passes, closedAtMs = now)
                    .also { closedNow = it }
            } else {
                State(strikes = strikes, passes = passes)
            }
        }
        closedNow?.let {
            log(
                "router: heal ${url.url} closed for writes for ${closedForMs / 3_600_000}h — never answered an OK across " +
                    "${it.passes.size} separate passes (${it.strikes} publishes)",
            )
        }
    }

    companion object {
        private const val DEFAULT_STRIKES = 3

        /** Strikes must span this many drain passes, so one bad session cannot close a relay. */
        private const val MIN_DISTINCT_PASSES = 2

        /** How long a closure stands: a relay's write policy changes rarely, and one retry a shift costs one push. */
        const val CLOSURE_TTL_MS = 6L * 60 * 60 * 1000
    }
}
