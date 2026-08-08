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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * The probing half of the fold, on its own clock.
 *
 * A stream tells this what it just discovered ([submit]) and gets on with its
 * download. Some time later, on this monitor's schedule and not the stream's,
 * the urls are fingerprinted and the verdicts signed into NIP-66. The stream
 * reads them back through [AliasFolding.apply] on a subsequent cycle.
 *
 * The alternative — the one this replaces — was to fold inline, which put a
 * multi-minute probe pass between "discovery finished" and "the first byte is
 * downloading" on EVERY cycle. Measured in the Docker run, that was 1:19 for a
 * 225-url list, and a production list is two orders of magnitude wider. The
 * mirror was waiting on a side quest.
 *
 * ## What it holds, and why that is a set of urls rather than a queue
 *
 * [pending] keeps the LATEST candidate set per stream, replacing rather than
 * appending. A stream re-discovers its whole world every cycle, so an append
 * queue would hold the same urls once per cycle and the monitor would spend its
 * budget re-reading verdicts it already has. Replacing means a pass always
 * works from the current shape of the network, and a stream that has stopped
 * discovering a url stops paying for it.
 *
 * Work is kept per stream and not merged into one set because the two callbacks
 * belong to the stream: [Work.canDial] is its transport guard (a stream without
 * Tor must not probe a `.onion`) and [Work.onEvent] is its ingest, filtered by
 * its own window. Merging them would mean guessing which stream a downloaded
 * event belongs to.
 */
class AliasMonitor(
    private val pass: Pass,
    private val scope: CoroutineScope,
    private val intervalMs: Long = DEFAULT_INTERVAL_MS,
    private val startupDelayMs: Long = DEFAULT_STARTUP_DELAY_MS,
) {
    /**
     * All this needs of the fold: measure one stream's urls, say how many new
     * aliases that proved. [AliasFolding.measure] is the implementation and
     * `AliasMonitor(folding::measure, scope)` is how they are joined.
     *
     * An interface rather than the class itself because the two have nothing
     * else to say to each other — the monitor never reads a verdict, never
     * publishes one, and should not be able to.
     */
    fun interface Pass {
        suspend fun measure(
            label: String,
            candidates: List<NormalizedRelayUrl>,
            canDial: suspend (NormalizedRelayUrl) -> Boolean,
            onEvent: suspend (Event) -> Unit,
        ): Int
    }

    /** One stream's standing offer of work: what it sees, and how to reach it. */
    private class Work(
        val candidates: List<NormalizedRelayUrl>,
        val canDial: suspend (NormalizedRelayUrl) -> Boolean,
        val onEvent: suspend (Event) -> Unit,
    )

    private val pending = ConcurrentHashMap<String, Work>()

    /**
     * Hand the monitor this stream's current candidate set. Returns
     * immediately — nothing is dialled here, which is the entire point.
     *
     * Called every cycle with the whole discovered set, not a delta: working out
     * what is new is [AliasFolding]'s job (it already skips anything with a
     * verdict), and a caller that tried to compute the delta itself would have
     * to duplicate the TTL rule to know when a url became interesting again.
     */
    fun submit(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit = {},
    ) {
        if (candidates.size < 2) return
        pending[label] = Work(candidates, canDial, onEvent)
    }

    /**
     * Start the loop. Idempotent only in the sense that the caller builds one of
     * these; calling it twice launches two loops.
     *
     * The first pass waits [startupDelayMs] rather than running at boot. A
     * router restarting has every stream discovering at once, and joining that
     * with a probe pass is how a restart turns into a thundering herd against
     * the same relays the fan-out is already dialling.
     */
    fun start(): AliasMonitor {
        scope.launch {
            delay(startupDelayMs)
            while (scope.isActive) {
                runPass()
                delay(intervalMs)
            }
        }
        return this
    }

    /**
     * One pass over every stream's standing work, streams in sequence.
     *
     * Sequential on purpose: [AliasFolding] enforces its own probe budget and
     * concurrency per call, and running two streams' passes at once would
     * multiply both behind its back. This is background work — it has all the
     * time it needs and none of the urgency.
     *
     * A stream whose pass throws must not end the loop or skip the streams after
     * it: a probe pass talks to arbitrary third-party relays, and any of them
     * can fail in ways this cannot enumerate. Cancellation is NOT swallowed —
     * that is the scope shutting down, and catching it here would keep the
     * monitor alive through a stop.
     */
    suspend fun runPass(): Int {
        var learned = 0
        for ((label, work) in pending.entries.map { it.key to it.value }) {
            try {
                learned += pass.measure(label, work.candidates, work.canDial, work.onEvent)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: $label alias pass failed: ${e.javaClass.simpleName}: ${e.message}")
            }
        }
        return learned
    }

    companion object {
        /**
         * How often the fold re-probes. Six hours, matching the default stream
         * refresh: a url is discovered, dialled unfolded once, and measured
         * before the cycle that would dial it a second time.
         */
        const val DEFAULT_INTERVAL_MS = 6L * 60 * 60 * 1000

        /**
         * How long after boot the first pass waits. Two minutes — long enough
         * for the streams that started with the process to be past discovery and
         * into their downloads, so the probe competes with a fan-out in progress
         * rather than with every stream's opening burst.
         */
        const val DEFAULT_STARTUP_DELAY_MS = 2L * 60 * 1000
    }
}
