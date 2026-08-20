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

import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicLong

/**
 * The probing half of the fold, on its own clock.
 *
 * The monitor derives every stream's world itself ([Source]) on its own
 * schedule and not on any stream's, fingerprints it, and signs the verdicts
 * into NIP-66. The streams read them back through [AliasFolding.apply] on a
 * subsequent cycle and never wait on a probe.
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
     * for a url, and [RelayVerdictRecord.edit] is a read-modify-write with no
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
    /**
     * Where the candidate set comes from, or null to fall back on whatever the
     * streams have pushed through [submit] — see [Source].
     */
    private val source: CandidateSource? = null,
    /**
     * The fast lane: how often to look for urls named by relay-list events
     * ingested since the last look, and the passes to run over them, IN ORDER
     * and on the same terms as [passes].
     *
     * **This is the STABILITY GATE and then FITNESS, and it used to be fitness
     * alone.** A first `prime` is what a new relay is waiting on, which is why
     * the lane exists and why the fold still rides the sweep — a fold needs a
     * host's WHOLE group to elect a leader, and a since-bound set holds
     * whichever urls of that host were named in the last two minutes.
     *
     * The stability gate has no such requirement: it is two REQs per url,
     * compared against nothing but that url's own second answer. Leaving it on
     * the sweep meant a new relay could be graded `prime` — and admitted to
     * every visit-mode stream's roster — before anything had asked whether it
     * answers the same question twice. `Verdict.INCONSISTENT` exists because
     * such a relay "would poison bands and coverage", so that window was up to
     * a whole [intervalMs] of a relay writing bands it cannot reproduce.
     *
     * It costs nothing over a sweep: a stability verdict stands for
     * [RelayVerdictRecord.DEFAULT_TTL_SECONDS], so a url measured here is one
     * the sweep then skips. The work moves earlier, it does not repeat.
     *
     * Null [fastLaneEveryMs] or an empty list turns the lane off.
     */
    private val fastLaneEveryMs: Long? = null,
    private val fastLanePasses: List<Pass> = emptyList(),
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
            sockets: Sockets,
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

    /**
     * Where the candidate set comes from — see [StreamWorld].
     *
     * [submit] made it a function of when each stream finished discovering, and
     * the first pass after a boot lost that race: measured, 34,997 urls waited
     * six hours for a pass they missed by three minutes. A source is derived by
     * the pass itself, so there is no race to lose.
     */
    interface CandidateSource {
        /** Every url worth measuring, across every configured stream. */
        suspend fun candidates(): List<NormalizedRelayUrl>

        /**
         * Where the derivation says what it is doing, or null for a source
         * that says nothing.
         *
         * The same split [Pass.progress] describes, for the same reason: the
         * monitor owns the CLOCK — when the derivation ran, how long it took,
         * when the next sweep is due — and the source owns the WORK, so both
         * write to one handle from opposite sides.
         *
         * It earns a row of its own because it is the state nothing could see:
         * `candidates()` walks the whole store, which is MINUTES on a live one,
         * and for all of them every pass below reads `idle` with no countdown —
         * the sweep has started and has not reached a pass yet, which is
         * indistinguishable from a sweep that is not running at all.
         */
        val progress: Processors.Handle? get() = null

        /**
         * Only the urls named by relay-list events ingested at or after
         * [since] — the fast lane's derivation. Bounded by construction: it
         * reads minutes of events where [candidates] walks the store. The
         * default is "unsupported", which turns the lane into a no-op rather
         * than an error — a Source is allowed not to know how.
         */
        suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> = emptyList()

        /** Whether this process can reach that url at all — transport, not policy. */
        suspend fun canDial(url: NormalizedRelayUrl): Boolean

        /** A probe's events are still events: offered to whichever streams want them. */
        suspend fun onEvent(event: Event)

        /** The cross-stream socket refcount, so a probe cannot close a live transfer. */
        val sockets: Sockets
    }

    /** One stream's standing offer of work: what it sees, and how to reach it. */
    private class Work(
        val candidates: List<NormalizedRelayUrl>,
        val canDial: suspend (NormalizedRelayUrl) -> Boolean,
        val onEvent: suspend (Event) -> Unit,
        val sockets: Sockets,
    )

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
     * ([the certified scans' refresh cache]) is holding a list the fold has
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
        // The derivation is on the passes' clock and not on one of its own: it
        // runs once per sweep, at the head of it, so "when is the next one due"
        // has exactly one answer for the whole card.
        source?.progress?.let { p ->
            p.nextPassAt { dueAtMs }
            p.phase(Processors.IDLE)
        }
        for (pass in passes) {
            pass.progress?.nextPassAt { dueAtMs }
            pass.progress?.phase(Processors.IDLE)
        }
        scope.launch {
            delay(startupDelayMs)
            while (scope.isActive) {
                // UNSET WHILE A PASS RUNS. The next one is not scheduled until
                // this one returns — a pass takes as long as it takes — so a
                // countdown here would be a promise about a time nobody has
                // computed. It rendered as "measuring · next pass in 0s", which
                // reads as a pass that is late rather than one in progress.
                dueAtMs = null
                passGate.withLock { runPass() }
                // Asked AFTER the pass, because only the pass knows: the
                // candidate set is derived inside it now, so "was there
                // anything to do" cannot be answered before it runs.
                val wait = if (lastPassHadWork) intervalMs else emptyRetryMs
                dueAtMs = System.currentTimeMillis() + wait
                delay(wait)
            }
        }
        // THE FAST LANE, under the same gate as the sweep. The record edits
        // are read-modify-write with no CAS, and "passes never overlap" is the
        // whole discipline that makes them safe — a lane that ran fitness over
        // a url mid-sweep would race the sweep's own edit of the same record.
        // Sharing the mutex serializes them; a lane tick that lands mid-sweep
        // simply waits, and its since-bound means it then reads the same
        // minutes of events it would have.
        val everyMs = fastLaneEveryMs
        if (everyMs != null && fastLanePasses.isNotEmpty() && source != null) {
            scope.launch {
                delay(startupDelayMs)
                // From the boot, not from zero: everything older is the
                // sweep's job, and a store-wide derivation is exactly what
                // this lane exists not to do.
                var lastLookSec = System.currentTimeMillis() / 1000
                while (scope.isActive) {
                    delay(everyMs)
                    val lookFrom = lastLookSec
                    lastLookSec = System.currentTimeMillis() / 1000
                    try {
                        passGate.withLock { runFastLane(lookFrom) }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: fast lane failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
                    }
                }
            }
        }
        return this
    }

    /**
     * One lane tick: the urls named since [sinceSec], through every
     * [fastLanePasses] entry in order.
     *
     * Public for the same reason [runPass] is — the loop that drives it sleeps
     * for minutes, and a test that had to wait one out could only assert that
     * the lane exists. The caller holds [passGate]; this does not take it,
     * exactly as [runPass] does not.
     *
     * NOT bracketed onto the source's row, though it is the same object
     * deriving. That row is the SWEEP's derivation: the lane reads minutes of
     * events where a sweep walks the store, so counting a lane tick as a pass
     * there would run `passesRun` up by the hour and overwrite a `lastPassSec`
     * measured in minutes with one measured in milliseconds. The passes it runs
     * are bracketed, on their own rows, by the same begin/finish [runPass] uses.
     *
     * **Which does stamp those rows with lane-sized numbers**, and that is not
     * new: [FitnessPass.measure] has always bracketed itself, so the fitness row
     * has been reporting a `lastPassSec` measured in milliseconds ever since the
     * lane existed. The stability row now joins it. The alternative is worse —
     * a pass publishes its position from inside ([Processors.Handle.measuring]),
     * so an unbracketed lane tick would draw a position on a row whose phase
     * reads `idle`.
     */
    suspend fun runFastLane(sinceSec: Long): Int {
        val src = source ?: return 0
        val fresh = src.candidatesSince(sinceSec)
        if (fresh.isEmpty()) return 0
        System.err.println("router: fast lane — ${fresh.size} url(s) named since the last look")
        var learned = 0
        for (pass in fastLanePasses) {
            pass.progress?.begin()
            try {
                // Guarded one at a time, for the reason [runPass] guards each
                // stream: a lane that let the stability gate's failure skip
                // fitness would cost a new relay its first `prime` over a fault
                // in the pass before it.
                learned += pass.measure(FAST_LANE, fresh, src::canDial, src::onEvent, src.sockets)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: fast lane pass failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            } finally {
                pass.progress?.finish()
            }
        }
        // DELIBERATELY NOT [learnedTotal], which is what [generation] reports.
        // The lane has never moved it and this change does not start: the
        // counter has no reader in the router today, so making the lane bump it
        // would be an unobservable behaviour change smuggled in beside a fix.
        // Worth revisiting the day something reads a generation — a lane that
        // learns a `prime` and leaves the number still is arguably wrong.
        return learned
    }

    /** Serializes the sweep and the fast lane — see the lane's comment in [start]. */
    private val passGate = Mutex()

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
     * Did the last pass have anything to measure at all?
     *
     * "Nothing to do YET" and "nothing to do" want different waits — a cold
     * store has no relay lists to derive from and is usually fine moments
     * later, so that case retries at [emptyRetryMs] rather than pushing the
     * first measurement a whole interval out on exactly the boot with the most
     * to learn.
     */
    @Volatile
    private var lastPassHadWork = false

    /**
     * One pass over every stream's standing work, streams in sequence.
     *
     * Sequential on purpose: [AliasFolding] bounds its own concurrency and
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
        // DERIVED AT THE MOMENT THE PASS RUNS, so no stream's discovery clock
        // can decide what this pass sees. A monitor with no source measures
        // nothing, which is the honest answer rather than a silent half-set:
        // the streams used to push their worlds in and the pass saw whichever
        // of them had finished discovering first.
        val work =
            source?.let { src ->
                // BRACKETED LIKE A PASS, because from a reader's side it is
                // one: it has a clock, it takes minutes, and the three rows
                // under it cannot start until it returns. Its own phase word,
                // because it dials nothing — see [Processors.COLLECTING].
                src.progress?.begin(Processors.COLLECTING)
                val urls =
                    try {
                        src.candidates()
                    } catch (e: CancellationException) {
                        // Shutdown, not a derivation failure. `runCatching`
                        // catches it and would turn a cancelled pass into an
                        // empty one that schedules another.
                        throw e
                    } catch (e: Exception) {
                        System.err.println("router: alias source failed to derive: ${e.message}")
                        emptyList()
                    } finally {
                        // From a `finally` for the reason the passes' own
                        // `finish` is: a derivation that throws every sweep and
                        // one that never returns are different faults, and only
                        // a clock that keeps moving separates them.
                        src.progress?.finish()
                    }
                // Nothing derived is not nothing to do — a cold store has no
                // relay lists yet — so this reads as an empty pass and retries
                // at [emptyRetryMs] rather than sleeping the full interval.
                //
                // EMPTY, not "fewer than two", and the difference is a mirror.
                // The guard was written when this class ran the FOLD and only
                // the fold: one url cannot be held up against another, so a
                // world of one was genuinely nothing to do. Two per-url passes
                // joined the list since — the stability gate and fitness — and
                // neither compares urls to anything, so the guard was skipping
                // the pass that writes `prime` on the one candidate set that
                // most needs it. A router discovering exactly one relay never
                // graded it, so every visit-mode stream selecting on that grade
                // had a permanently empty roster: #139's symptom by another
                // route. The fold still refuses a world of one — inside
                // [AliasFolding.measure], where the world it refuses is the one
                // it actually assembled rather than this pass's candidates.
                if (urls.isEmpty()) emptyList() else listOf(ALL_STREAMS to Work(urls, src::canDial, src::onEvent, src.sockets))
            } ?: emptyList()
        lastPassHadWork = work.isNotEmpty()
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
         * The label a [Source]-derived pass reports under. One row, because the
         * set is a UNION — a per-stream row would double-count what two streams
         * share. It also closes the hole the per-stream shape had: grouping
         * happened WITHIN a stream, so a host whose urls were split across two
         * was never folded.
         */
        const val ALL_STREAMS = "all streams"

        /** What a lane tick reports under, where a sweep reports under [ALL_STREAMS]. */
        const val FAST_LANE = "fast lane"

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
