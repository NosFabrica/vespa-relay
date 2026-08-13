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

import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

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
 * Work is kept per stream and not merged into one set because the callbacks
 * belong to the stream: [Work.canDial] is its transport guard (a stream without
 * Tor must not probe a `.onion`), [Work.onEvent] is its ingest, filtered by
 * its own window, and [Work.sockets] is its connection refcount — the one thing
 * that can close a probe's websocket without closing it under another stream's
 * transfer. Merging them would mean guessing which stream a downloaded event
 * belongs to.
 */
class AliasMonitor(
    /**
     * The passes to run over each stream's urls, IN THIS ORDER and never
     * concurrently.
     *
     * Sequential because they share a destination: both [AliasFolding] and
     * [ConsistencyPass] write tags onto the same addressable kind-30166 record
     * for a url, and [RelayAliasRecord.edit] is a read-modify-write with no
     * compare-and-set. Two of them writing one url at once would drop whichever
     * tag was written between the other's read and its store — silently, since
     * the result is still a valid signed record that simply says less. Running
     * them one after another inside this loop is what makes that impossible
     * within this process.
     *
     * The loop runs one PASS over every stream before starting the next pass —
     * see [runPass] — which strengthens the same property: the fold finishes on
     * every stream before any stability walk begins, so none of them is spent on
     * a url another stream's fold was about to remove.
     */
    private val passes: List<Pass>,
    private val scope: CoroutineScope,
    private val intervalMs: Long = DEFAULT_INTERVAL_MS,
    private val startupDelayMs: Long = DEFAULT_STARTUP_DELAY_MS,
    private val emptyRetryMs: Long = DEFAULT_EMPTY_RETRY_MS,
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
            sockets: AliasFolding.Sockets,
        ): Int

        /**
         * Where this pass says what it is doing, or null for one that says
         * nothing.
         *
         * The monitor owns the CLOCK — when a pass ran, how long it took, when
         * the next one is due — and the pass itself owns the WORK, so both write
         * to the same handle from opposite sides. Null by default so a test or a
         * caller with nothing to report is still one lambda, which is what keeps
         * this a `fun interface`.
         */
        val progress: Processors.Handle? get() = null
    }

    /** One stream's standing offer of work: what it sees, and how to reach it. */
    private class Work(
        val candidates: List<NormalizedRelayUrl>,
        val canDial: suspend (NormalizedRelayUrl) -> Boolean,
        val onEvent: suspend (Event) -> Unit,
        val sockets: AliasFolding.Sockets,
    )

    private val pending = ConcurrentHashMap<String, Work>()

    private val learnedTotal = AtomicLong()

    /**
     * How many verdicts this monitor has learned since the process started —
     * folds and stability answers together — used as a VERSION for the cached
     * relay list, not as a statistic.
     *
     * Both belong here because both change what a cached list should contain: a
     * fold removes a duplicate url, a stability verdict removes a url that
     * cannot be synced against. A list built before either was published goes on
     * dialling it.
     *
     * A stream that holds its discovered relay list in memory across cycles
     * ([RelayDiscoveryConfig.recycleSeconds]) is holding a list the fold has
     * since had something to say about: a url that folded away between two
     * cycles goes on being dialled, taking a socket and a band for events its
     * survivor already delivers, until the list is rebuilt. This number
     * changing is the cheapest possible signal that rebuilding it would now
     * produce something different.
     *
     * Monotonic and global on purpose. It is bumped by whichever stream's pass
     * learned something, and every stream re-reads its list — a verdict is
     * about a url, and two streams routinely discover the same one. The rest of
     * what makes a stale list stale (a verdict aged past its TTL, one written by
     * another router signing with the same key) is invisible from here, which is
     * why age remains the primary expiry and this is only an early one.
     */
    fun generation(): Long = learnedTotal.get()

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
        sockets: AliasFolding.Sockets = AliasFolding.Sockets.NONE,
    ) {
        if (candidates.size < 2) return
        pending[label] = Work(candidates, canDial, onEvent, sockets)
    }

    /**
     * Start the loop. Idempotent only in the sense that the caller builds one of
     * these; calling it twice launches two loops.
     *
     * The first pass waits [startupDelayMs] rather than running at boot. A
     * router restarting has every stream discovering at once, and joining that
     * with a probe pass is how a restart turns into a thundering herd against
     * the same relays the fan-out is already dialling.
     *
     * A pass with NOTHING SUBMITTED sleeps [emptyRetryMs] instead of the full
     * interval. Discovery on a cold store is minutes — longer than the startup
     * delay — so the first pass can easily land before any stream has submitted
     * anything, and waiting the whole interval on that would push the first
     * measurement six hours out on exactly the boot that has the most to learn.
     * "Nothing to do yet" and "nothing to do" are different, and only the first
     * one is worth retrying for.
     */
    fun start(): AliasMonitor {
        // Registered before the first sleep, so a reader sees the passes exist
        // and when they are due rather than an absence for the first two
        // minutes — the same rule `StreamPhases.register` follows, and for the
        // same reason: silence must never read as "not configured".
        dueAtMs = System.currentTimeMillis() + startupDelayMs
        for (pass in passes) {
            pass.progress?.nextPassAt { dueAtMs }
            pass.progress?.phase(Processors.IDLE)
        }
        scope.launch {
            delay(startupDelayMs)
            while (scope.isActive) {
                val hadWork = pending.isNotEmpty()
                runPass()
                val wait = if (hadWork) intervalMs else emptyRetryMs
                dueAtMs = System.currentTimeMillis() + wait
                delay(wait)
            }
        }
        return this
    }

    /**
     * When the next pass is due, in epoch millis, asked live by whatever is
     * reporting — see [Processors.Handle.nextPassAt].
     *
     * Written by the loop rather than derived from the interval, because the two
     * are not the same number: a pass with nothing submitted sleeps
     * [emptyRetryMs] instead of [intervalMs], and a pass that ran for a quarter
     * of an hour pushes the next one that much later. A countdown computed from
     * the constant would be wrong in both cases.
     */
    @Volatile
    private var dueAtMs: Long? = null

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
        val work = pending.entries.map { it.key to it.value }
        // ONE PASS AT A TIME, over every stream — rather than one stream at a
        // time over every pass, which is how this used to read.
        //
        // The ordering property is the same one [passes] documents and it is
        // strengthened rather than weakened: the fold now finishes on EVERY
        // stream before the stability gate measures anything, so no stability
        // walk can be spent on a url another stream's fold was about to remove.
        // What it buys is that a pass is a clocked unit: `lastPassAt`,
        // `lastPassSec` and the `measuring` phase describe the whole pass rather
        // than whichever stream happened to be last, which is what a reader
        // waiting on the fold is actually asking about.
        for (pass in passes) {
            pass.progress?.begin()
            try {
                for ((label, w) in work) {
                    // Each stream guarded on its own, so one failing does not
                    // cost the others: a probe pass talks to arbitrary
                    // third-party relays and any of them can fail in ways this
                    // cannot enumerate.
                    try {
                        val n = pass.measure(label, w.candidates, w.canDial, w.onEvent, w.sockets)
                        learned += n
                        // Per stream, not once at the end: a pass over many streams can
                        // run for a quarter of an hour, and a stream whose fan-out
                        // starts in the middle of it should see the verdicts already
                        // published rather than wait out the whole pass.
                        if (n > 0) learnedTotal.addAndGet(n.toLong())
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: $label alias pass failed: ${e.javaClass.simpleName}: ${e.message}")
                    }
                }
            } finally {
                // From a `finally`, so a cancelled or thrown pass still stamps
                // its clock: a pass whose `lastPassAt` stopped moving and one
                // that is failing every time are different faults, and only the
                // timestamp separates them.
                pass.progress?.finish()
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

        /**
         * How long to wait before looking again when no stream has submitted
         * anything yet. A minute — the question is only "has discovery finished
         * on a cold store", and asking it costs a map lookup.
         */
        const val DEFAULT_EMPTY_RETRY_MS = 60L * 1000
    }
}
