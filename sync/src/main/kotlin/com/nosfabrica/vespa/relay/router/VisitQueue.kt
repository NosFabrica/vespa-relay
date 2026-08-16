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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * THE POOL'S ROTATION BOOKKEEPING — one home for the invariants five bare
 * collections used to share by convention, and where two audited race bugs
 * lived: the requeue a worker collision swallowed, and the revisit timers
 * that stacked into double-cadence chains.
 *
 * - [offer] dedups against the QUEUE, never against a running visit — a url
 *   can be wanted again while it is being visited.
 * - A url drawn while its visit is still running is PARKED, and the visit's
 *   own worker re-offers it the moment it finishes: the promptness a changed
 *   ask set was promised, kept through the race instead of dropped by it.
 * - A finished visit that consumed no parked requeue arms exactly ONE
 *   revisit timer, however many out-of-band offers (evictions, rebuilds)
 *   landed meanwhile.
 *
 * The queue knows nothing about relays, rosters or tails: [visitLoop]'s three
 * callbacks are the whole contract, which is what makes the invariants a
 * hermetically testable surface rather than probe-only choreography.
 */
internal class VisitQueue(
    private val scope: CoroutineScope,
) {
    private val channel = Channel<NormalizedRelayUrl>(Channel.UNLIMITED)
    private val queued = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val inFlight = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val parked = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val armed = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Urls waiting for a worker. */
    val waiting: Int get() = queued.size

    /** Urls a worker is on right now. */
    val visiting: Int get() = inFlight.size

    /** Queue [url] now. False when it is already waiting (running is fine). */
    fun offer(url: NormalizedRelayUrl): Boolean {
        if (!queued.add(url)) return false
        channel.trySend(url)
        return true
    }

    /**
     * One worker's forever-loop: draw urls and run [visit] on each, keeping
     * the invariants above. [revisitDelayMs] is read as the visit finishes —
     * the delay depends on what it delivered — and [stillWanted] gates both
     * the visit and every requeue, so a url the roster dropped mid-wait dies
     * quietly. [visit] is expected to contain its own failure handling; a
     * throw that escapes it (cancellation) ends the worker.
     */
    suspend fun visitLoop(
        stillWanted: (NormalizedRelayUrl) -> Boolean,
        revisitDelayMs: (NormalizedRelayUrl) -> Long,
        visit: suspend (NormalizedRelayUrl) -> Unit,
    ) {
        for (url in channel) {
            queued.remove(url)
            if (!inFlight.add(url)) {
                parked.add(url)
                continue
            }
            try {
                if (stillWanted(url)) visit(url)
            } finally {
                inFlight.remove(url)
            }
            if (parked.remove(url)) {
                // A requeue arrived mid-visit: back on the queue now, and no
                // timer — the prompt visit will arm its own.
                offer(url)
            } else {
                armRevisit(url, revisitDelayMs, stillWanted)
            }
        }
    }

    private fun armRevisit(
        url: NormalizedRelayUrl,
        revisitDelayMs: (NormalizedRelayUrl) -> Long,
        stillWanted: (NormalizedRelayUrl) -> Boolean,
    ) {
        if (!armed.add(url)) return
        val delayMs = revisitDelayMs(url)
        scope.launch {
            delay(delayMs)
            armed.remove(url)
            if (stillWanted(url)) offer(url)
        }
    }
}
