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
package com.nosfabrica.vespa.relay.router.progress

import com.nosfabrica.vespa.relay.util.fmtCount
import com.nosfabrica.vespa.relay.util.fmtDay
import com.nosfabrica.vespa.relay.util.fmtDuration
import java.util.concurrent.ConcurrentHashMap

/**
 * What each stream is doing right now, so an operator can tell a stream that
 * is working from one that never started.
 *
 * Two rules, both violated by the completion-only logging this replaced:
 *
 *  - **A phase in progress reports elapsed time.** The two longest phases —
 *    walking the local id set, discovering relays — produce no completion for
 *    minutes and used to print nothing at all.
 *  - **A configured stream is never absent.** It appears on every tick even
 *    when idle, so silence can never be read as "not configured".
 *
 * A third rule arrived later, from a 45-minute ingest drain that produced no
 * output at all:
 *
 *  - **Every phase reports elapsed time, including the ones that are not
 *    "progress".** `Idle` and `Failed` used not to, so a stream that went idle
 *    forty-five minutes ago and one that finished a second ago printed the
 *    identical line. Silence and stillness are different states and only the
 *    clock tells them apart.
 *
 * It also holds each stream's [CycleTally], because the phase and the
 * disposition are two halves of one answer — what a stream is doing, and what
 * became of everything it took on. [snapshot] hands both to [SyncProgress],
 * which is what publishes them off this process.
 */
class StreamPhases {
    sealed interface Phase {
        /** Registered, not yet started — the only honest thing to say before the first phase. */
        data object Starting : Phase

        /** Configured, nothing to do yet — its sources have named no relays. */
        data class Waiting(
            val sources: String,
            val retrySec: Long,
        ) : Phase

        /** Ready to reconcile, waiting for another stream to release the gate. */
        data class Queued(
            val relays: Int,
        ) : Phase

        /** Reading relay urls out of the store. */
        data class Discovering(
            val sources: String,
        ) : Phase

        /**
         * Building the local id set the reconcile compares against.
         * [total] is null when it could not be counted — an unknown
         * denominator is better than a wrong one.
         */
        data class Snapshotting(
            val collected: Int,
            val total: Int?,
            val relays: Int,
        ) : Phase

        /** Paging relays that are not reconciling — no id set involved. */
        data class Fetching(
            val done: Int,
            val total: Int,
            val events: Long,
            /** Time-axis progress of the relays still walking — see [PagingProgress]. */
            val fraction: Double? = null,
            val etaMs: Long? = null,
            /**
             * Oldest `created_at` the walk has reached. The percentage alone
             * cannot show a deep walk moving — days of progress on an
             * unbounded walk round to 0% — but the date moves every page.
             */
            val reachedSeconds: Long? = null,
            /**
             * Relays with a WORKER on them right now, across every pass —
             * probing, queued for a transfer slot, or transferring.
             *
             * Beside [done], never instead of it, because the two stopped
             * agreeing when the fan-out became a rotation: [done] is how far
             * the current WALK got over its list, and this is how much of the
             * admission gate is committed — including legs handed out by passes
             * that ended long ago. A walk that has finished handing out while
             * ten relays are still transferring is neither "done" nor "idle".
             */
            val running: Int = 0,
            /**
             * …and of those, how many hold a transfer SLOT.
             *
             * The slot, not the socket: the websocket connect happens inside the
             * slot, so a url that never connects counts here while it tries
             * (measured — `InFlightReportProbe`). Published separately because
             * the gap to [running] is large and reporting the wider number alone
             * overstated the work: a stream with 8 transfer slots routinely
             * shows 128 workers, of which 120 are in the guards or queued. One
             * number for both read as "128 relays syncing" on a stream that
             * cannot sync more than 8.
             */
            val transferring: Int = 0,
        ) : Phase

        /** Fanning out. */
        data class Syncing(
            val done: Int,
            val total: Int,
            val events: Long,
            val skipped: Long,
            val unreachable: Long,
            /** Relays with a worker right now, across every pass — see [Fetching.running]. */
            val running: Int = 0,
            /** …and of those, how many are on a socket — see [Fetching.transferring]. */
            val transferring: Int = 0,
        ) : Phase

        /**
         * The refresh interval came round and the next pass is being HELD BACK,
         * because too much of the transfer pool is still committed to the last
         * one.
         *
         * Its own phase rather than a longer `Idle`: idle is "nothing to do
         * until the timer", this is "the timer fired and we are declining", and
         * an operator reading the two as one cannot tell a mirror waiting out
         * its interval from one whose pool never frees up. The elapsed clock on
         * it is the whole diagnostic — a few seconds is the rotation breathing,
         * an hour is a stream that has stopped and needs [oldest] looked at.
         */
        data class Holding(
            /** Transfer slots free right now. */
            val free: Int,
            /** …and how many this stream will not start a pass without. */
            val needed: Int,
            /** Relays with a worker at all, which is far wider — see [Fetching.running]. */
            val running: Int,
            /** The leg holding a slot longest, named, or null if none is. */
            val oldest: InFlight.Relay?,
        ) : Phase

        /** Cycle finished; nothing more until the next refresh. */
        data class Idle(
            val events: Long,
            /**
             * Seconds until the next cycle, or null when there is no next cycle.
             *
             * A dynamic stream re-runs on a timer and can say when. A STATIC one
             * cannot: it backfills once and then live-tails, so there is nothing
             * to count down to. That case used to pass 0, which rendered as
             * "next in 0s" — indistinguishable from a stream about to re-run
             * immediately, and it was read as a busy loop more than once.
             */
            val nextInSec: Long?,
            /**
             * Relays with a worker STILL RUNNING from the pass that just ended.
             *
             * The gap between passes is not quiet on a rotation: the walk ends
             * when the last url is handed out, and the slowest legs run on
             * through the wait. Reported here because "idle" otherwise claims
             * the opposite of what is happening, and a hung leg would be
             * invisible for exactly as long as it lasted.
             */
            val running: Int = 0,
            /** …and of those, how many are on a socket — see [Fetching.transferring]. */
            val transferring: Int = 0,
        ) : Phase

        /** The last attempt threw. */
        data class Failed(
            val reason: String,
            val retrySec: Long,
        ) : Phase

        /**
         * Riding the visit pool: the stream's relay list is the monitor's
         * verdicts and its engine is the rotation, so the fan-out phases above
         * never happen to it. One long-lived phase whose numbers move, rather
         * than a cycle of phases — there is no walk to be a phase OF. The
         * per-relay truth (which relays a worker is on, what each is doing) is
         * the in-flight list, registered beside this.
         */
        data class Rotating(
            /** Certified relays this stream is riding right now. */
            val relays: Int,
            /** …of which this many hold a live tail. */
            val tailed: Int,
        ) : Phase
    }

    /**
     * ONE PASS over a stream's relay list, and what became of the urls it took
     * on.
     *
     * Kept as a list rather than a slot because passes OVERLAP: a walk ends when
     * its last url is handed out, not when its last worker returns, so the next
     * pass is routinely walking while the previous one's stragglers are still
     * downloading. Published as a single `cycle` that was replaced on every
     * `beginCycle`, that is exactly the state nothing could show — the moment
     * the new pass opened, the old one's counters stopped being published and
     * its live legs were absorbed into the new pass's `busy`, so "the old walk
     * is still finishing" was a sentence the document could not say.
     *
     * ONE stream name can also carry both `urls` and `relaySource`, so a static
     * backfill and a dynamic fan-out can each own a live pass under it at the
     * same time. That used to be arbitrated — first writer wins, the loser
     * publishes nothing — and with a list it does not have to be: [owner] says
     * which half of the router opened each one.
     */
    class Cycle(
        /** The pass number within its owner, starting at 1. */
        val number: Long,
        /** [STATIC] or [DYNAMIC] — which half of the router opened it. */
        val owner: String,
        val tally: CycleTally,
        val startedSec: Long,
        /** When the WALK ended; null while it is still handing out. The workers outlive it. */
        @Volatile var endedSec: Long? = null,
        /** `running` / `completed` / `failed` — see [Stream.outcome]. */
        @Volatile var outcome: String = "running",
    ) {
        /**
         * Is this pass finished with, i.e. may it be dropped from the report?
         *
         * BOTH halves are required and that is the whole point: a pass whose
         * walk `completed` an hour ago but which still has urls unaccounted for
         * has legs in the pool right now, and dropping it is how its events
         * stopped being attributed to anything. `pending` reaching zero is what
         * says its last worker returned.
         */
        fun retired(): Boolean = outcome != "running" && tally.pending() == 0L
    }

    private class Entry(
        @Volatile var phase: Phase,
        @Volatile var sinceMs: Long,
        /**
         * The passes still worth reporting, oldest first — see [Cycle].
         *
         * Bounded by [MAX_TRACKED_CYCLES] on top of the retirement rule, because
         * a pass whose legs never return never retires and a rotation that keeps
         * passing would otherwise grow this without limit.
         */
        val cycles: MutableList<Cycle> = mutableListOf(),
        /**
         * WHICH relays this stream has workers on, asked live rather than
         * pushed.
         *
         * A supplier because the set changes on every dial and this class is
         * read on a tick: mirroring it here would be a second copy of
         * `RelayRotation`'s state, kept in step by hand, which is the shape that
         * produces a report disagreeing with the thing it reports on. Null for a
         * stream with no rotation — a static backfill has none, and inventing an
         * empty one would claim it has nothing running.
         */
        @Volatile var inFlight: (() -> InFlight)? = null,
    )

    /**
     * THE PHASE'S OWN NUMBERS, which used to reach a log line and nothing else.
     *
     * `phase` published the WORD — `fetching`, `holding` — while the object
     * behind it carried how far the walk had got, how deep it had reached, when
     * it expected to finish, and how much of each pool was committed. The card
     * said "fetching for 19m" beside a log line reading "2385/7927 returned …
     * back to 2023-07-12, 33.8% of the window walked, ETA ~1:01".
     *
     * Every member is nullable and only the ones its phase can answer are set.
     * Two were deliberately left OUT after checking what they are:
     *
     *  - the walk's `total`, because `PassProgress.total` is the cleaned relay
     *    list and `CycleTally.taken` is the same set by construction — it is
     *    already published, once, as `urls.taken`;
     *  - the phase's `events`, because `Fetching.events` and `cycle.received`
     *    are incremented from the same line with the same value.
     */
    class Detail(
        /**
         * Fan-out legs that STARTED AND CAME BACK — including unreachable,
         * capped, out of budget. Not progress; see the glossary's `returned`.
         */
        val returned: Int? = null,
        /** Relays with a worker at all, across every pass — the admission gate's commitment. */
        val running: Int? = null,
        /** …and of those, how many hold a TRANSFER SLOT. The two are far apart by design. */
        val transferring: Int? = null,
        /** How much of the time window the paged walks have covered, 0..1. */
        val fraction: Double? = null,
        /** …and what that rate implies for finishing, in milliseconds. */
        val etaMs: Long? = null,
        /**
         * The oldest `created_at` the walk has reached.
         *
         * The one number that shows a DEEP walk moving: on an unbounded walk the
         * percentage rounds to zero for hours while this date moves every page.
         * It is the live counterpart of a band's floor, on the same axis.
         */
        val reachedSeconds: Long? = null,
        /** Local ids collected so far while building the reconcile snapshot. */
        val collected: Int? = null,
        /** …of how many, when the store could be counted. */
        val collectedTotal: Int? = null,
        /** Transfer slots free right now, while a pass is being HELD BACK. */
        val free: Int? = null,
        /** …and how many this stream will not start a pass without. */
        val needed: Int? = null,
        /** Seconds until the next pass, when the stream is idle and there is one. */
        val nextInSec: Long? = null,
        /** Seconds until a failed or waiting stream tries again. */
        val retrySec: Long? = null,
        /** What the last attempt threw, when it threw. A stream that FAILED said only "failed". */
        val reason: String? = null,
    )

    /**
     * One stream's state, flattened for a reader outside this process.
     *
     * A snapshot, not a view: [SyncProgress] serialises it on a timer while the
     * fan-out keeps moving, and handing out the live [Entry] would publish a
     * document whose members were read at different instants.
     */
    class Stream(
        val name: String,
        /** The phase's own word — `fetching`, `syncing`, `idle`, … — never the rendered line. */
        val phase: String,
        /** How long it has been in that phase, in seconds. */
        val phaseForSec: Long,
        /**
         * Every pass still worth reporting, oldest first — see [Cycle].
         *
         * Usually one. Two or more is a rotation whose stragglers outlived their
         * walk, which is the ordinary way a wide fan-out behaves and was
         * previously indistinguishable from a single cycle with a large `busy`.
         */
        val cycles: List<Cycle>,
        /** The current phase's own numbers — see [Detail]. */
        val detail: Detail,
        /**
         * The relays this stream has workers on right now, quietest first —
         * the names behind `running`, `pending` and `busy`, which were counts
         * and nothing else. Null for a stream that has no rotation to ask.
         */
        val inFlight: InFlight? = null,
    ) {
        /**
         * The pass that started most recently, which is what a reader asking
         * "what is it doing now" means — and what the single `cycle` member of
         * the progress document still carries, so a page that has not learned
         * about [cycles] keeps working.
         */
        val newest: Cycle? get() = cycles.lastOrNull()

        /** Passes that have not finished handing out. Two of them is the overlap, visible. */
        fun running(): Int = cycles.count { it.outcome == "running" }
    }

    private val phases = ConcurrentHashMap<String, Entry>()

    /** Ordered, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    @Synchronized
    fun register(name: String) {
        if (phases.putIfAbsent(name, Entry(Phase.Starting, System.currentTimeMillis())) == null) {
            order += name
        }
    }

    /**
     * A pass has started; [tally] is what it will fill in as urls settle.
     *
     * APPENDED, not substituted. The previous pass keeps its counters and keeps
     * being published until its own workers have all returned ([Cycle.retired]),
     * because that is the state a rotation is in most of the time and the one
     * this report existed to hide: the new walk's `busy` counted the old walk's
     * legs and nothing said what they were still doing there.
     *
     * Retired passes are dropped here rather than on a timer — this is the only
     * moment the list is known to be growing — and the whole list is capped at
     * [MAX_TRACKED_CYCLES] on top of that, since a pass with a leg that never
     * returns never retires. The cap drops the OLDEST, which is the one whose
     * events are furthest in the past.
     */
    @Synchronized
    fun beginCycle(
        name: String,
        owner: String,
        number: Long,
        tally: CycleTally,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        register(name)
        phases[name]?.let { entry ->
            entry.cycles.removeAll { it.retired() }
            entry.cycles += Cycle(number = number, owner = owner, tally = tally, startedSec = nowSeconds)
            while (entry.cycles.size > MAX_TRACKED_CYCLES) entry.cycles.removeAt(0)
        }
    }

    /**
     * The pass ended. [outcome] is `completed` or `failed` — the caller knows
     * which, and guessing from the counters would report a cycle that legitimately
     * reached nothing as a failure.
     *
     * Stamps the newest RUNNING pass of that owner and nothing else. A stream
     * that fails during DISCOVERY has opened no pass, and stamping the previous
     * one would age a finished run every time the next one threw; a static
     * backfill finishing must not stamp `completed` on a dynamic fan-out running
     * under the same name.
     */
    @Synchronized
    fun endCycle(
        name: String,
        owner: String,
        outcome: String,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        phases[name]
            ?.cycles
            ?.lastOrNull { it.outcome == "running" && it.owner == owner }
            ?.let {
                it.endedSec = nowSeconds
                it.outcome = outcome
            }
    }

    /**
     * Where to ask [name] which relays it has workers on.
     *
     * Registered once, by whoever owns the rotation. A stream that never calls
     * this publishes no in-flight list at all, which is the honest outcome for a
     * static backfill: it has no rotation, so there is nothing to ask.
     */
    @Synchronized
    fun namesInFlight(
        name: String,
        source: () -> InFlight,
    ) {
        register(name)
        phases[name]?.inFlight = source
    }

    /** The two halves of the router that can own a stream's cycle slot. */
    companion object {
        const val STATIC = "static"
        const val DYNAMIC = "dynamic"

        /**
         * Past this, a leg is not slow, it is stuck — and every progress line
         * for the stream holding it names it until it lets go.
         *
         * Ten minutes because that is comfortably longer than the slowest
         * HEALTHY leg measured here: the full `indexers` walk on purplepag.es
         * downloads 1,490,010 events in ~10.8 minutes, and directory.yabu.me
         * serves a 1.2M-event backlog below its floor. Anything below that
         * threshold would print a line about legs doing exactly what they are
         * supposed to, which is how a warning stops being read.
         */
        const val STUCK_LEG_SECONDS = 600L

        /**
         * How many passes one stream reports at once.
         *
         * Four, and it is a backstop rather than a policy: passes retire
         * themselves the moment their last worker returns, so the ordinary
         * steady state is one or two. What this bounds is the pathological case
         * — a leg that never returns keeps its pass alive forever, and a stream
         * on a short `recycleSeconds` would then accumulate one row per pass for
         * the life of the process.
         */
        const val MAX_TRACKED_CYCLES = 4
    }

    /** Every registered stream, in registration order, as of now. */
    @Synchronized
    fun snapshot(): List<Stream> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            Stream(
                name = name,
                phase = word(e.phase),
                phaseForSec = (System.currentTimeMillis() - e.sinceMs) / 1000,
                // Copied, not handed out: the list is mutated by the fan-out
                // under this lock and a reader iterating the live one would be
                // serialising a document while it changed underneath.
                cycles = e.cycles.toList(),
                detail = detail(e.phase),
                // Asked at the moment the snapshot is taken, like everything
                // else here — the whole class is a view flattened for a reader
                // outside this process, and a member read a tick earlier would
                // date a stuck leg's clock from the wrong instant.
                inFlight = e.inFlight?.invoke(),
            )
        }

    /**
     * What this phase can answer, and nothing it cannot.
     *
     * Read at snapshot time from the phase object itself, so the numbers and the
     * word can never describe two different instants.
     */
    private fun detail(phase: Phase): Detail =
        when (phase) {
            is Phase.Fetching -> {
                Detail(
                    returned = phase.done,
                    running = phase.running,
                    transferring = phase.transferring,
                    fraction = phase.fraction,
                    etaMs = phase.etaMs,
                    reachedSeconds = phase.reachedSeconds,
                )
            }

            is Phase.Syncing -> {
                // No `skipped`/`unreachable` here: they are per-url dispositions
                // and the cycle's partition already publishes them by name, where
                // they sum to something.
                Detail(returned = phase.done, running = phase.running, transferring = phase.transferring)
            }

            is Phase.Snapshotting -> {
                Detail(collected = phase.collected, collectedTotal = phase.total)
            }

            is Phase.Holding -> {
                Detail(free = phase.free, needed = phase.needed, running = phase.running)
            }

            is Phase.Idle -> {
                Detail(nextInSec = phase.nextInSec, running = phase.running, transferring = phase.transferring)
            }

            is Phase.Waiting -> {
                Detail(retrySec = phase.retrySec)
            }

            is Phase.Failed -> {
                Detail(retrySec = phase.retrySec, reason = phase.reason)
            }

            is Phase.Rotating -> {
                // `running`'s glossary entry — relays with a worker on them —
                // is exactly what the pool's visit set is, so the member is
                // reused rather than a synonym invented beside it. The tailed
                // count is NOT forced into `transferring`: a held tail is not
                // a transfer slot, and the pool's own row already counts it.
                Detail(running = phase.relays)
            }

            is Phase.Queued, is Phase.Discovering, is Phase.Starting -> {
                Detail()
            }
        }

    /**
     * The phase's machine-readable name.
     *
     * Spelled out rather than derived from the class name: these strings are
     * published, and a reader charting them must not have a series renamed by a
     * Kotlin refactor.
     */
    private fun word(phase: Phase): String =
        when (phase) {
            is Phase.Starting -> "starting"
            is Phase.Waiting -> "waiting"
            is Phase.Queued -> "queued"
            is Phase.Discovering -> "discovering"
            is Phase.Snapshotting -> "snapshotting"
            is Phase.Fetching -> "fetching"
            is Phase.Syncing -> "syncing"
            is Phase.Holding -> "holding"
            is Phase.Idle -> "idle"
            is Phase.Failed -> "failed"
            is Phase.Rotating -> "rotating"
        }

    fun set(
        name: String,
        phase: Phase,
    ) {
        val existing = phases[name]
        if (existing == null) {
            register(name)
            phases[name]?.phase = phase
            return
        }
        // The clock restarts only when the KIND of phase changes: snapshotting
        // that reports a new count every page is still the same phase, and
        // resetting elapsed there would hide exactly the duration worth seeing.
        if (existing.phase::class != phase::class) existing.sinceMs = System.currentTimeMillis()
        existing.phase = phase
    }

    /** One line per stream, in registration order. */
    @Synchronized
    fun report(): List<String> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            val elapsed = System.currentTimeMillis() - e.sinceMs
            "router: $name ${describe(e.phase, elapsed)}${stuck(e)}"
        }

    /**
     * ` — wss://slow.example held 11h 20m, 2 event(s), quiet 11h 19m`, or
     * nothing.
     *
     * Appended to whatever the phase says, because the phase cannot say it: a
     * stream is `fetching` or `idle` whether its pool is turning over or wedged
     * on one relay, and the counts in the line ([Fetching.running] and friends)
     * name nobody. The url only ever reached stderr for the ONE stream
     * `SYNC_DIAGNOSE` points at, and container logs here rotate inside the hour
     * — so a leg that had been holding a slot since the small hours left no
     * trace at all by the time anyone looked.
     *
     * Silent below [STUCK_LEG_SECONDS]: every healthy pass has legs in flight,
     * and a line on each is the log rather than a finding.
     */
    private fun stuck(e: Entry): String {
        val oldest =
            e.inFlight
                ?.invoke()
                ?.relays
                ?.firstOrNull() ?: return ""
        if (oldest.heldForSec < STUCK_LEG_SECONDS) return ""
        return " — ${oldest.relay} held ${fmtDuration(oldest.heldForSec * 1000)}" +
            // The two that separate a real backlog from a wedge. Both, always:
            // "0 events" alone reads as a dead socket on a leg that is merely
            // reconciling, and a large count alone reads as healthy on a walk
            // that stopped hours ago.
            ", ${oldest.events} event(s), quiet ${fmtDuration(oldest.quietForSec * 1000)}" +
            (if (oldest.transferringForSec == null) " (not on a socket)" else "")
    }

    private fun describe(
        phase: Phase,
        elapsedMs: Long,
    ): String {
        val elapsed = fmtDuration(elapsedMs)
        return when (phase) {
            is Phase.Starting -> {
                "starting ($elapsed elapsed)"
            }

            is Phase.Waiting -> {
                "waiting — no relays in [${phase.sources}] yet, retry in ${phase.retrySec}s ($elapsed elapsed)"
            }

            is Phase.Queued -> {
                "queued behind another stream — ${phase.relays} relay(s) ready ($elapsed elapsed)"
            }

            is Phase.Discovering -> {
                "discovering relays from [${phase.sources}] ($elapsed elapsed)"
            }

            is Phase.Snapshotting -> {
                val of = phase.total?.let { "/${fmtCount(it)}" } ?: ""
                val pct = phase.total?.takeIf { it > 0 }?.let { " (${(phase.collected * 100L / it)}%)" } ?: ""
                "snapshotting ${fmtCount(phase.collected)}$of local ids$pct for ${phase.relays} relay(s) ($elapsed elapsed)"
            }

            is Phase.Fetching -> {
                // `returned`, not `done`. This counts fan-out legs that came
                // BACK — including the ones that came back unreachable, capped
                // or out of budget — and reading it as progress is the single
                // most misread number this router publishes. What settled is in
                // the cycle's disposition ([CycleTally]); what is COVERED is in
                // the bands. Three different questions, and only this one is
                // cheap enough to tick every second.
                "fetching ${phase.done}/${phase.total} relay(s) returned, ${phase.events} event(s) received" +
                    rate(phase.events, elapsedMs) +
                    (if (phase.running > 0) ", ${phase.running} in flight (${phase.transferring} transferring)" else "") +
                    (phase.reachedSeconds?.let { " — back to ${fmtDay(it)}" } ?: "") +
                    (phase.fraction?.let { ", %.1f%% of the window walked".format(it * 100) } ?: "") +
                    (phase.etaMs?.let { ", ETA ~${fmtDuration(it)}" } ?: "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Syncing -> {
                "syncing ${phase.done}/${phase.total} relay(s) returned, ${phase.events} event(s) received" +
                    rate(phase.events, elapsedMs) +
                    (if (phase.running > 0) ", ${phase.running} in flight (${phase.transferring} transferring)" else "") +
                    // "skipped as dead" said nothing about what dead MEANT or
                    // when it is retried. It is a NIP-66 record this router (or
                    // another signing with the same key) wrote earlier saying a
                    // relay could not be reached; the set is re-read at the top
                    // of every cycle, so the retry is the refresh interval.
                    (if (phase.skipped > 0) ", ${phase.skipped} not dialled (struck out, no route, or no transport)" else "") +
                    (if (phase.unreachable > 0) ", ${phase.unreachable} dialled and failed" else "") +
                    " ($elapsed elapsed)"
            }

            is Phase.Holding -> {
                // Says what it is waiting FOR and what it is waiting ON, in that
                // order. Without the first this is indistinguishable from a
                // stalled stream; without the second an operator has the
                // symptom and no subject.
                "holding the next pass — ${phase.free}/${phase.needed} transfer slot(s) free," +
                    " ${phase.running} relay(s) still running" +
                    (
                        phase.oldest?.let {
                            ", longest ${it.relay} at ${fmtDuration(it.heldForSec * 1000)}" +
                                " (${it.events} event(s), quiet ${fmtDuration(it.quietForSec * 1000)})"
                        } ?: ""
                    ) +
                    " ($elapsed elapsed)"
            }

            is Phase.Idle -> {
                // The elapsed clock is the point here, not decoration: a stream
                // that went idle forty-five minutes ago and one that finished a
                // second ago printed the same line, and an ingest queue draining
                // behind a finished cycle looked exactly like a stalled router.
                val tail = if (phase.running > 0) ", ${phase.running} relay(s) still running (${phase.transferring} transferring)" else ""
                phase.nextInSec?.let { "idle — ${phase.events} event(s) received last pass$tail, next in ${it}s ($elapsed ago)" }
                    ?: "backfilled ${phase.events} event(s); live tail only — no further cycles ($elapsed ago)"
            }

            is Phase.Failed -> {
                "failed: ${phase.reason} — retry in ${phase.retrySec}s ($elapsed ago)"
            }

            is Phase.Rotating -> {
                "riding the pool — ${phase.relays} certified relay(s), ${phase.tailed} tailed ($elapsed elapsed)"
            }
        }
    }

    /**
     * `, 2350/s received` — throughput for the phase, or nothing when it is too
     * early to mean anything.
     *
     * Two things this is NOT, both of which it has been read as. The clock is
     * the PHASE's, so it is the rate of the work being described rather than a
     * lifetime average. And the numerator is events RECEIVED FROM UPSTREAMS by
     * this one stream — the health line's `ev/s` is events reaching ingest
     * across every stream, after dedup drops the copies the other relays already
     * delivered, so the two are counted differently on purpose and disagreeing
     * is not a fault in either.
     */
    private fun rate(
        events: Long,
        elapsedMs: Long,
    ): String {
        // Under a second the divisor is noise and the answer is a wild number.
        if (elapsedMs < 1_000 || events <= 0) return ""
        return ", ${events * 1000 / elapsedMs}/s received"
    }
}
