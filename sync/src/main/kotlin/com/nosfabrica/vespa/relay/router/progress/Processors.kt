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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * THE WORK THIS ROUTER DOES THAT IS NOT A STREAM.
 *
 * ## Why the streams were never the whole picture
 *
 * `StreamPhases` answers "what is each configured stream doing", and every
 * report this process publishes was built on it. But a stream is only the part
 * of the router that an operator CONFIGURED, and several long-running jobs sit
 * beside them with nothing configured about them at all:
 *
 *  - the **alias source**, which walks the store for every url the relay lists
 *    name and hands the three passes below the set they work on — minutes of a
 *    sweep, in front of every one of them;
 *  - the **alias fold** and the **stability pass**, which run on
 *    `AliasMonitor`'s own six-hour clock and decide which discovered urls the
 *    fan-out is allowed to stop dialling;
 *  - the **fitness pass**, which signs the `prime` certificate every
 *    visit-mode stream's relay list is made of, on the same clock;
 *  - the **rotating pool**, which is that list turned into sockets;
 *  - **ingest**, which is where every mirrored event actually lands, and whose
 *    queue is the first thing to look at when the streams look busy and the
 *    store is not growing;
 *  - the **healer** and the **upstream push**, which send events OUT.
 *
 * Every one of them was reachable only from container stderr, on the same
 * rotation-inside-the-hour that made `InFlight` necessary. The symptom was
 * specific and repeated: a fan-out that keeps dialling forty urls of one server
 * is the alias fold not having got to that host yet, and there was no way to
 * tell that from the fold being broken, off, or finished and wrong.
 *
 * ## The shape, and why it is one shape for all of them
 *
 * Two kinds of job are described here and they are deliberately not two
 * schemas. A PASS-shaped processor (the fold, the stability gate, fitness) has
 * a clock, a last run and a next one, and its progress is per stream — each
 * stream submits its own candidate set. A COUNTER-shaped one (the pool, ingest,
 * the healer, the push) has no passes at all; it is always running and what it
 * has is gauges. Both publish `phase` and `phaseForSec` like a stream does, so
 * the card can draw them the same way, and each fills in only the members it
 * can honestly answer.
 *
 * The counters are read through a supplier at snapshot time rather than pushed:
 * they are live atomics owned by the component itself, and a copy kept in step
 * by hand is the shape that produces a report disagreeing with the thing it
 * reports on. [StreamPhases.namesInFlight] takes a supplier for the same
 * reason.
 */
class Processors {
    /**
     * How far one pass got over ONE stream's candidate set.
     *
     * Per stream because that is how the work arrives — `AliasMonitor` holds the
     * latest candidate set per stream and measures each in turn — and because
     * two streams discover different corners of the network. Summing them would
     * double-count every url they share, which is most of them.
     */
    class Work(
        /** The stream whose candidates were measured. */
        val stream: String,
        /**
         * Urls that stream submitted, before anything was decided.
         *
         * The word is the code's own — `AliasFolding.measure(label, candidates,
         * …)` and `AliasMonitor.submit(candidates)` — and that is the whole
         * reason for it. A published member named for a synonym is a member
         * nobody can grep their way from the document back to.
         */
        val candidates: Int,
        /**
         * …of those, how many arrived with NO VERDICT AT ALL — the pass's own
         * subject, and the denominator the card counts against.
         *
         * `candidates` is the wrong one to show a reader watching the fold
         * work. Most of a settled candidate set carries a verdict from weeks ago
         * and is never asked again until it ages out, so a position counted
         * against it moves by a rounding error however much a pass achieves, and
         * every url the fold removed from the fan-out counted as one it had just
         * checked. This is the set the pass exists to decide, so `newUrls -
         * unmeasured` is what it decided: the same population before and after,
         * which is what makes the pair a fraction rather than two numbers next
         * to each other.
         *
         * Null on a pass that does not report it, for the reason [foldedAway]
         * is nullable: a zero would be the claim that nothing arrived undecided,
         * which is the state both passes are working towards and must not be
         * manufactured by a router that never counted.
         */
        val newUrls: Int? = null,
        /**
         * …of those, how many a FOLD has already taken out of the fan-out.
         *
         * First member of the partition and first in precedence, because a
         * folded url is never measured for stability — see [ConsistencyPass.report].
         *
         * **Null rather than zero on a pass that does not measure it**, and the
         * three members below are nullable for the same reason. The alias fold
         * publishes no stability verdicts at all, so a zero from its row would
         * be a measurement it never took: the card would draw "0 consistent · 0
         * refused" beside the fold, and a reader would have no way to tell that
         * from a gate that had measured everything and found nothing. Absent is
         * "this pass does not answer that"; zero is "it does, and the answer is
         * none".
         */
        val foldedAway: Int? = null,
        /**
         * …and of the rest, the two standing verdicts.
         *
         * STANDING, not "reached by this pass": [decided] counts what this pass
         * learned, these count what the whole candidate set currently carries,
         * including verdicts read back from the store at boot. A reader watching
         * coverage grow wants the second; a reader watching a pass work wants
         * the first, and conflating them made a pass that decided nothing
         * indistinguishable from a gate that knows nothing.
         */
        val consistent: Int? = null,
        val inconsistent: Int? = null,
        /**
         * …of those, how many still have no verdict after this pass.
         *
         * THE PROGRESS NUMBER. A pass measures its whole set, but a group can
         * be held on a cooldown and a host that cannot be measured never
         * resolves at all — so this falling pass over pass is what "the fold is
         * getting somewhere" looks like, and this sitting still while [dialled]
         * climbs is the state that used to be invisible.
         *
         * Named for [AliasFolding.Collapsed.unmeasured], which is the same set
         * computed by the same call. It is deliberately NOT the complement of
         * [dialled]: that one counts what this PASS spent, this one counts what
         * the whole candidate set still lacks.
         */
        val unmeasured: Int,
        /**
         * Dials this pass spent — fingerprints for the fold, paired walks for
         * the self-consistency gate.
         *
         * `dialled` rather than `measured`, because the router's own verb for
         * reaching a relay is dial ("never dialled", "dialled again next
         * cycle") and because `measured`/`unmeasured` would read as a pair that
         * sums to the candidates, which they do not.
         */
        val dialled: Int,
        /** New verdicts it reached and published. Zero with `measured` high is a pass that decided nothing. */
        val decided: Int,
        /**
         * WHY the hosts it could not decide were not decided, bounded and
         * counted — the same list the fold prints to stderr, published so it
         * outlives the log.
         */
        val undecided: List<Undecided> = emptyList(),
        /**
         * Reasons left out of [undecided]. Never silent, for [InFlight]'s reason.
         *
         * ZERO, always, since the list stopped being cut — and the member stays
         * because absent cannot be told from "nothing dropped".
         *
         * It WAS sixteen, and that cap was the file's own rule broken twice
         * over. A reason is an enum value in this source — five from the fold,
         * thirteen from the stability gate — so the network cannot grow the
         * list and a cap could only ever pick which reasons an operator is not
         * shown. Worse, truncating did not merely shorten it: the rows sum to
         * [unmeasured], so a cut tail surfaced on the card as `not accounted
         * for` in the fault tone — an arithmetic error reported against a pass
         * that was working. The cap was one short twice, at six and at eight,
         * both times because a reason list grew and the number did not. Nothing
         * can be short of an enumeration it does not bound.
         */
        val undecidedOmitted: Int = 0,
    )

    /**
     * HOW FAR THE PASS RUNNING RIGHT NOW HAS GOT — the live half of [Work].
     *
     * [Work] is written when a pass RETURNS, which on the stability gate is
     * hours after it started and on the fold a quarter of an hour. Between those
     * two moments the row said `measuring` and nothing else: no size, no
     * position, no clock. `phaseForSec` counted up beside it, which answers "how
     * long has this been going" and not the question anyone was asking, which is
     * "how much longer". Worse, the countdown that IS published is deliberately
     * unset while a pass runs (see `AliasMonitor.start`), so the one number on
     * the line disappeared exactly when the pass became interesting.
     *
     * So each pass declares what it set out to walk ([Handle.measuring]) and
     * ticks a unit off as each one ends ([Handle.attempted]), and this is those
     * two numbers with the clock the entry already holds.
     *
     * The UNIT is carried rather than assumed because the passes do not count
     * the same thing: the stability gate and the fitness pass decide a URL at a
     * time, the fold decides a HOST at a time — a group of urls is its unit of
     * work and a group is a host. One word per unit, the same rule [Breakdown]
     * follows.
     */
    class Measuring(
        /** [UNIT_URL] or [UNIT_HOST] — what the two counts below are counts OF. */
        val unit: String,
        /**
         * …of which this many have ended, however they ended.
         *
         * ATTEMPTED, not decided: a url the transport declined and a host with
         * no yardstick are both behind the pass, and a position that only moved
         * on success would sit still through the half of a corpus that cannot be
         * measured at all. What was LEARNED is [Work.decided], published when the
         * pass ends.
         */
        val attempted: Int,
        /**
         * How many this pass set out to walk — [RelayConsistency.toProbe]'s
         * result and the fold's uncooled groups, whose name it borrows.
         *
         * Not `candidates`: both passes drop what already carries a verdict
         * before dialling anything, so on a settled corpus this is a small
         * fraction of the candidate set and the ratio a reader is watching is
         * this one, not that one.
         */
        val toProbe: Int,
        /**
         * Seconds left at the rate so far, or null before the first unit lands.
         *
         * Null rather than a guess, for the reason the paging ETA carries in
         * AGENTS.md: its predecessor divided a number by itself and printed
         * `100%, ETA ~0:00` for hours. There is a real denominator here —
         * [toProbe] is known before the first dial — so the estimate is honest
         * as far as it goes, with one bias worth knowing: the fold walks its
         * groups WIDEST FIRST, so its early units are its slowest and this reads
         * long and improves. Pessimistic-then-improving is the safe direction
         * for a number an operator uses to decide whether to wait.
         */
        val etaSec: Long?,
        /**
         * How long since a unit last ENDED — the number that tells a pass about
         * to finish from one that has stopped.
         *
         * [etaSec] cannot: it is honest arithmetic on the rate so far, so a
         * pass whose last url has wedged reports `0` — correct, and
         * indistinguishable from a pass a second from done. A production
         * fitness pass sat at `attempted: 12373, toProbe: 12374, etaSec: 0` for
         * 74 minutes and every number on the row agreed with every other one.
         *
         * Beside the position rather than replacing it, because the pair is the
         * disclosure: `12373 of 12374` with this at 4 seconds is a pass
         * finishing, and the same position with this at 4,000 is a pass that is
         * not going to.
         *
         * Zero before the first unit lands, which is the pass's own age at that
         * point and not a claim that something just finished — [attempted] is
         * `0` beside it and says so.
         */
        val quietForSec: Long,
    )

    /**
     * WHICH URLS A PASS IS HOLDING RIGHT NOW, and what it is doing with each —
     * the counts' missing half, on the same terms [InFlight] states them for a
     * stream.
     *
     * ## The question this exists to answer
     *
     * A fitness pass held one url of 12,374 for 74 minutes, and that url was
     * not nameable from anywhere in the system: not from the position (which
     * publishes a number), not from the log (420 router lines over twenty
     * minutes, none about fitness), and not from a thread dump, since a
     * suspended coroutine has no frame. It was recoverable from OkHttp thread
     * names, which is not a diagnostic anyone should need. The pass knew the
     * url perfectly well the whole time.
     *
     * ## Its own shape rather than [InFlight]'s, and why
     *
     * A stream leg is a TRANSFER and is decided by three clocks — has it a
     * slot, has it delivered, is it still delivering. A probe leg is a LADDER:
     * it delivers events only incidentally, holds no transfer slot, and what
     * decides it is which step it is on. Filling `events`/`quietForSec` here
     * would be manufacturing the two numbers a reader would then act on, which
     * is the one thing [Work]'s nullable members exist to refuse.
     *
     * ## LONGEST-HELD FIRST, which is the opposite of [InFlight]'s order
     *
     * There, held is not risk: the healthiest thing the router does is hold one
     * relay for an hour while it streams two million events. Here every leg is
     * bounded by [AliasProbe.deadlineMs] by construction, so a leg near that
     * bound is the anomaly and the front of the list is where it belongs.
     *
     * ## WHOLE, like [InFlight] and for the same reason
     *
     * A row is a JOB, so the monitor's `dialConcurrency` already bounds the set
     * — 128 by default, and whatever an operator chose when it is not. That
     * puts it on the side of the line this file draws elsewhere: a cap is for a
     * list the network can grow, and capping one our own configuration bounds
     * only picks which rows an operator is not shown. It was cut to twenty
     * once, on the argument that 499 of 500 rows are ordinary dials a second
     * old — which is true of the ROWS and false of the LIST: `omitted: 480`
     * says nothing about whether those 480 are healthy, while the whole set
     * sorted by age is the distribution, and the distribution is the finding.
     * The page still draws a few of them; that is an editorial cut, it belongs
     * at the display layer, and the record it is drawn from is complete.
     */
    class Holding(
        /** Urls with a live job, longest-held first. */
        val relays: List<Held>,
        /**
         * How many more had a job and are not named here.
         *
         * ZERO, always, and kept for the reason [InFlight.omitted] is kept from
         * the pool: a list that does not disclose its truncation reads as the
         * whole answer, and a reader finding the member absent cannot tell
         * "nothing dropped" from "does not say".
         *
         * This was a cut of twenty, and the cut broke the rule the rest of this
         * file holds — a cap is for a list the NETWORK can grow, and a held row
         * is a JOB, so `dialConcurrency` already bounds it. Capping it only
         * picked which rows an operator is not shown, and picked them from the
         * one list that exists because a row was not shown.
         */
        val omitted: Int,
    ) {
        /** One url a probe job is holding, and what it is doing with it. */
        class Held(
            val relay: String,
            /**
             * Since the job took its permit — which is after the wait for one,
             * not before. A pass at `dialConcurrency` has urls queued behind
             * every leg here and their wait is the pass's shape rather than any
             * relay's, so the queue is deliberately outside this clock and
             * outside the deadline it is read against.
             */
            val heldForSec: Long,
            /**
             * WHICH STEP, in the words the pass's own code uses. The clock
             * above says how long; only this says what for, and the steps fail
             * for unrelated reasons — a name that will not resolve stalls the
             * pre-probe, a relay that never stops sending stalls the ladder,
             * a full ingest queue stalls whichever step is delivering.
             */
            val stage: String,
        )
    }

    /** One server under a reason, and how many of its urls ended there. */
    class HostCount(
        val host: String,
        val urls: Int,
    )

    /** One reason a pass ended some hosts with nothing written down, and who they were. */
    class Undecided(
        /** The reason, in the words the router's own log uses. */
        val reason: String,
        /**
         * …and the reason this one REFINES, where it refines one.
         *
         * `never answered a REQ` is the largest thing a probe pass reports and
         * it covers four different findings — a name that no longer resolves, a
         * refused connection, a failed TLS handshake, a window that lapsed — so
         * the rows for those name it here instead of appearing beside it as
         * peers. The list stays FLAT and still sums to [Work.unmeasured]: naming
         * the parent is what lets a reader nest the rows without the arithmetic
         * having to survive a tree on the wire, and a reader that ignores this
         * member still sees every url exactly once.
         *
         * Null on a row that refines nothing, which is most of them.
         */
        val parent: String? = null,
        /** How many hosts ended the pass that way. */
        val hosts: Int,
        /**
         * Them by name, for a pass that has only NAMES to give — the fold,
         * which decides a host at a time. A pass that can count fills [top]
         * instead: the same disclosure with the number that ranks it.
         *
         * Bounded at [MAX_UNDECIDED_EXAMPLES], which is a ceiling and not a
         * sample: this used to be three, and three names under a reason
         * holding twenty-eight hosts made the one actionable thing about the
         * row — which servers — the thing it withheld.
         */
        val examples: List<String> = emptyList(),
        /**
         * …or the widest few WITH their url counts, for a pass that measures
         * urls: the fourth level of the funnel.
         *
         * Bounded at [MAX_UNDECIDED_HOSTS] and never summing to the reason on
         * its own — the tail is deliberately left for the reader to see as a
         * remainder, because "3,902 urls on 2,201 hosts and no host above 12"
         * and "3,902 urls of which one host is 3,000" are opposite findings and
         * a truncated list that closes would hide the difference.
         */
        val top: List<HostCount> = emptyList(),
        /**
         * How many URLS ended the pass that way — the count that makes the
         * partition close.
         *
         * Beside [hosts] rather than instead of it, because the two answer
         * different questions and their gap is itself the disclosure: 4,300 urls
         * on 900 hosts is a corpus of aliases, 4,300 on 4,300 is not, and only
         * the url count sums back to [Work.unmeasured]. Zero on a pass that
         * counts in hosts alone — the fold, whose subject genuinely is the
         * server — which reads as "not reported" rather than as none.
         */
        val urls: Int = 0,
    )

    /**
     * One reason a total breaks down by, counted in EVENTS.
     *
     * Its own class rather than [Undecided] with a different meaning poured into
     * it: that one counts HOSTS, because what a probe pass decides is a server.
     * Ingest decides events. One word per unit, including here.
     */
    class Breakdown(
        val reason: String,
        val events: Long,
    )

    /** One gauge a counter-shaped processor publishes, read live at snapshot time. */
    class Count(
        val name: String,
        val value: Long,
    )

    /**
     * One processor's state, flattened for a reader outside this process — the
     * same contract [StreamPhases.Stream] has, and for the same reason.
     */
    class Snapshot(
        val name: String,
        val phase: String,
        val phaseForSec: Long,
        /** Passes run since this process started, or null for a processor that has none. */
        val passes: Long?,
        /** When the last pass ENDED, in epoch seconds. Null before the first one finishes. */
        val lastPassAt: Long?,
        /** How long that pass took. Beside [lastPassAt] because a pass here can run for a quarter of an hour. */
        val lastPassSec: Long?,
        /** Seconds until the next pass, or null when nothing is scheduled. */
        val nextInSec: Long?,
        /**
         * How far the pass IN FLIGHT has got, or null when none is — see
         * [Measuring]. The only member here that moves while a pass runs.
         */
        val measuring: Measuring? = null,
        /**
         * …and WHICH urls it is holding while it gets there — see [Holding].
         * Null when the pass is holding nothing, which is every processor
         * between passes and every one that does not report a held set.
         */
        val inFlight: Holding? = null,
        val work: List<Work>,
        val counts: List<Count>,
        /**
         * Reasons a counter-shaped processor can break its own total down by —
         * ingest's rejections, today. The same shape [Work.undecided] uses, for
         * the same reason: a total nobody can decompose is a total nobody can
         * act on.
         */
        val reasons: List<Breakdown> = emptyList(),
    )

    /**
     * One pass's live position: the set it declared, and a counter for what is
     * behind it. Written by the pass, read by whoever is reporting.
     *
     * The counter lives INSIDE the object rather than beside it so that reading
     * a position is one volatile read: unit, size and count are then the same
     * pass's by construction, and no reader can straddle a pass boundary.
     */
    internal class Run(
        val unit: String,
        val toProbe: Int,
        /**
         * When the WALK started, which is not when the pass did.
         *
         * A pass spends its first stretch deriving what to walk — the stability
         * gate reads every candidate's stored verdicts a page of 500 at a time,
         * the fold groups the whole corpus by host — and none of that is dials.
         * Timed from [Handle.begin], the derivation lands in the numerator of
         * every rate this position implies, so the estimate carries a constant
         * that has nothing to do with how fast the relays are answering. It is
         * worst exactly where the estimate matters most: on the first few units,
         * where there is least else to divide by.
         */
        val startedMs: Long,
    ) {
        val attempted = AtomicInteger()

        /**
         * When a unit last ENDED, or the walk's start before the first one —
         * see [Measuring.quietForSec].
         *
         * Beside [attempted] rather than derived from it: a counter says how
         * many are behind the pass and nothing at all about when the last one
         * got there, and the whole difference between a pass finishing and a
         * pass stopped is in the second question.
         */
        val lastUnitMs = AtomicLong(startedMs)
    }

    // Internal rather than private only because [Handle] holds one: a private
    // nested type cannot be a parameter of a class published outside it, even
    // through an internal constructor.
    internal class Entry(
        val name: String,
        @Volatile var phase: String,
        @Volatile var sinceMs: Long,
    ) {
        val passes = AtomicLong()

        @Volatile
        var startedMs: Long? = null

        @Volatile
        var lastPassAtSec: Long? = null

        @Volatile
        var lastPassSec: Long? = null

        /** Epoch millis the next pass is due, asked live — see the class header. */
        @Volatile
        var nextAt: (() -> Long?)? = null

        /**
         * What the pass now running set out to walk, and how much of it is
         * behind it — or null, which is a processor between passes, one that
         * has not derived its set yet, and one that never reports a position.
         * All three are silent rather than claiming `0 of 0`.
         *
         * ONE reference holding all three numbers, swapped whole by
         * [Handle.measuring] and cleared by [Handle.begin] and [Handle.finish]
         * — not three fields kept in step. As three, a reader arriving between
         * two of the writes could pair one pass's unit with the next pass's
         * size: the snapshot is taken on a timer from another thread, and the
         * writes happen on a pass boundary that a wide fan-out crosses while
         * the report is being built. It is the same rule the [Roster] swap
         * follows in `VisitPool` and for the same reason — a set of members
         * that must agree is one object, not a convention.
         */
        @Volatile
        var run: Run? = null

        /**
         * WHAT THIS PROCESSOR IS HOLDING RIGHT NOW — url to (taken at, step).
         *
         * A plain map rather than a swapped-whole reference like [run], because
         * unlike the position it is not a set of members that have to agree:
         * every entry is one job's own, written by that job and removed by it,
         * and a reader arriving mid-write sees one row more or fewer rather
         * than two passes at once. Bounded by the pass's dial concurrency, and
         * cleared on both pass boundaries so a row can never outlive the pass
         * that took it.
         */
        val held = ConcurrentHashMap<String, Pair<Long, String>>()

        @Volatile
        var counts: (() -> List<Count>)? = null

        /** Live reasons for a counter-shaped processor — see [Handle.reasons]. */
        @Volatile
        var reasons: (() -> List<Breakdown>)? = null

        /** Latest result per stream, replacing rather than appending. */
        val work = ConcurrentHashMap<String, Work>()
    }

    private val entries = ConcurrentHashMap<String, Entry>()

    /** Registration order, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    /**
     * The handle [name] reports through. Idempotent: asking twice hands back the
     * same entry, which is what lets a component be wired from one place and
     * driven from another.
     */
    @Synchronized
    fun of(name: String): Handle {
        val entry =
            entries.getOrPut(name) {
                order += name
                Entry(name, STARTING, System.currentTimeMillis())
            }
        return Handle(entry)
    }

    /** Every registered processor, in registration order, as of now. */
    @Synchronized
    fun snapshot(nowMs: Long = System.currentTimeMillis()): List<Snapshot> =
        order.mapNotNull { name ->
            val e = entries[name] ?: return@mapNotNull null
            Snapshot(
                name = e.name,
                phase = e.phase,
                phaseForSec = ((nowMs - e.sinceMs) / 1000).coerceAtLeast(0),
                passes = e.passes.get().takeIf { it > 0 || e.startedMs != null },
                lastPassAt = e.lastPassAtSec,
                lastPassSec = e.lastPassSec,
                // Never negative: a pass that is overdue has a due time in the
                // past, and "-40s" reads as a bug rather than as a queue.
                nextInSec = e.nextAt?.invoke()?.let { ((it - nowMs) / 1000).coerceAtLeast(0) },
                measuring = measuring(e, nowMs),
                inFlight = holding(e, nowMs),
                // Ordered, for the same reason every other list here is: two
                // rollups of one state must produce the same document.
                work = e.work.values.sortedBy { it.stream },
                counts = e.counts?.invoke().orEmpty(),
                reasons = e.reasons?.invoke().orEmpty(),
            )
        }

    /**
     * The pass in flight, as of [nowMs] — or null, which is every processor
     * between passes and every one that does not report a position.
     *
     * Read off the entry rather than pushed, like the counters and for the same
     * reason: the numbers move on every dial and a copy kept in step by hand is
     * the shape that produces a report disagreeing with the thing it reports on.
     *
     * The guard is [Entry.run] and only that: it is set when a pass declares
     * its set and cleared by both [Handle.begin] and [Handle.finish], so its
     * presence IS "a pass is walking right now". A stale position published
     * under `idle` would read as a pass that stopped halfway, which is a fault
     * report rather than a measurement.
     */
    private fun measuring(
        e: Entry,
        nowMs: Long,
    ): Measuring? {
        // Read ONCE into a local: the field is swapped whole on a pass
        // boundary, and re-reading it per member is how a report ends up
        // describing two passes at once.
        val run = e.run ?: return null
        val attempted = run.attempted.get()
        val toProbe = run.toProbe
        val elapsedMs = (nowMs - run.startedMs).coerceAtLeast(0)
        return Measuring(
            unit = run.unit,
            attempted = attempted,
            toProbe = toProbe,
            // Nothing finished, nothing to extrapolate from — and a pass whose
            // last unit has landed has no remainder to estimate, so the number
            // goes away rather than converging on zero from above.
            etaSec =
                if (attempted in 1 until toProbe) {
                    ((elapsedMs.toDouble() / attempted) * (toProbe - attempted) / 1000).toLong()
                } else {
                    null
                },
            quietForSec = ((nowMs - run.lastUnitMs.get()) / 1000).coerceAtLeast(0),
        )
    }

    /**
     * What the pass is holding as of [nowMs] — or null, which is a processor
     * holding nothing.
     *
     * Read off the entry rather than pushed, like the position and the
     * counters. Not guarded by [Entry.run]: the fold takes its first dial
     * before it can declare a set, and a leg held by a pass whose position is
     * not yet published is exactly the leg worth naming.
     */
    private fun holding(
        e: Entry,
        nowMs: Long,
    ): Holding? {
        if (e.held.isEmpty()) return null
        // Snapshotted before it is sorted: the map moves under a wide pass on
        // every dial, and a comparator reading a value that changes mid-sort is
        // the one way this could throw into a report.
        val rows = e.held.entries.map { (relay, taken) -> Triple(relay, taken.first, taken.second) }
        val named =
            rows
                // Longest-held first — see [Holding]. Then by name, so two legs
                // taken in the same millisecond do not swap places between two
                // rollups of one state.
                .sortedWith(compareBy({ it.second }, { it.first }))
                .map { (relay, takenMs, stage) ->
                    Holding.Held(
                        relay = relay,
                        heldForSec = ((nowMs - takenMs) / 1000).coerceAtLeast(0),
                        stage = stage,
                    )
                }
        // Whole — see [Holding]. `omitted` is the schema's promise rather than
        // a count this side can make non-zero.
        return Holding(named, 0)
    }

    /** What a processor holds to report through. Cheap to keep; safe to call from anywhere. */
    class Handle internal constructor(
        private val entry: Entry,
    ) {
        /**
         * Say what this processor is doing.
         *
         * The clock restarts only when the WORD changes, exactly as
         * [StreamPhases.set] does it: a processor re-stating the same phase on
         * every tick would otherwise report an elapsed time of zero forever,
         * which is the one number worth having here.
         */
        fun phase(word: String) {
            if (entry.phase != word) {
                entry.phase = word
                entry.sinceMs = System.currentTimeMillis()
            }
        }

        /**
         * A pass has started.
         *
         * [nowMs] is a parameter for the same reason [finish]'s is: the elapsed
         * time between the two is what [Measuring]'s estimate is computed from,
         * and a test that cannot set both ends of it can only assert that the
         * estimate exists.
         */
        fun begin(
            word: String = MEASURING,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.startedMs = nowMs
            // The previous pass's position, dropped before this one can be read
            // against it. A pass derives its set some way in — the store reads
            // and the verdict adoption come first — and until it does, the last
            // pass's `4,728 of 4,728` sitting under a fresh `measuring` reads as
            // a pass that finished instantly.
            entry.run = null
            // …and the previous pass's held urls with it. Nothing should be
            // left — every job releases in a `finally`, and every job is
            // bounded — but a row surviving into the next pass would name a leg
            // that is not running, which is worse than naming none.
            entry.held.clear()
            phase(word)
        }

        /**
         * …and it has ended, however it ended.
         *
         * Called from a `finally`, so a pass that threw still stamps its clock:
         * a fold that fails every time is a fold whose `lastPassAt` should keep
         * moving while it learns nothing, and one whose clock froze in
         * `measuring` is a pass that never returned at all. Those are different
         * faults and the timestamp is what separates them.
         *
         * **A FINISH WITH NO BEGIN DOES NOTHING**, and that is what keeps a
         * self-bracketing pass from being counted twice. [FitnessPass] brackets
         * its own `measure` — it has to, because the fast lane calls it outside
         * the monitor's loop entirely — while the sweep ALSO brackets every
         * pass it runs. Both finishes landed, so the fitness row reported two
         * `passesRun` per sweep and an operator reading it counted twice as
         * many passes as ran. Ignoring the outer one keeps the inner clock,
         * which is the honest one: it times the measure and not the loop
         * around it.
         */
        fun finish(
            word: String = IDLE,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            val startedMs = entry.startedMs ?: return
            entry.lastPassSec = ((nowMs - startedMs) / 1000).coerceAtLeast(0)
            entry.startedMs = null
            // The position goes with the pass that had it. What it reached is
            // [Work], which this pass has already recorded and which stands
            // until the next one replaces it.
            entry.run = null
            entry.held.clear()
            entry.lastPassAtSec = nowMs / 1000
            entry.passes.incrementAndGet()
            phase(word)
        }

        /**
         * What this pass set out to walk, declared the moment it knows — see
         * [Measuring].
         *
         * Called INSIDE the pass rather than by the monitor that starts it,
         * because the size is not knowable from outside: both probe passes read
         * their stored verdicts first and walk only what has none, so the set
         * exists a store read after [begin] and is a small fraction of the
         * candidates the monitor handed over. That store read is also why the
         * rate is timed from HERE and not from [begin] — see [Run.startedMs].
         */
        fun measuring(
            toProbe: Int,
            unit: String,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.run = Run(unit, toProbe, nowMs)
        }

        /**
         * One more unit of this pass is behind it, however it ended.
         *
         * Counted from the unit's own completion — including the ones that
         * decided nothing, see [Measuring.attempted] — so the position measures
         * the pass rather than its luck.
         */
        fun attempted(
            units: Int = 1,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            // Silent when no pass has declared a set — a caller that counts
            // without declaring is publishing a numerator with no denominator,
            // and dropping it is better than inventing one.
            val run = entry.run ?: return
            run.attempted.addAndGet(units)
            run.lastUnitMs.set(nowMs)
        }

        /**
         * This pass has taken [relay] and is [stage] on it — see [Holding].
         *
         * Called again as the job moves on, which UPDATES the step and keeps
         * the clock: the row's whole job is to say how long this url has been
         * held, and a leg that restarted its clock at every rung would report
         * the last step's age as the leg's.
         *
         * Cheap enough to call per step and safe to call from anywhere: one map
         * write on a map bounded by the pass's dial concurrency.
         */
        fun holding(
            relay: String,
            stage: String,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.held.compute(relay) { _, existing -> (existing?.first ?: nowMs) to stage }
        }

        /**
         * …and it is done with it, however it ended.
         *
         * From a `finally`, on the same terms [finish] is: a job that threw, was
         * cancelled, or ran out its deadline has stopped holding the url either
         * way, and a row that outlives its job is a fault report about a leg
         * that is not there.
         */
        fun released(relay: String) {
            entry.held.remove(relay)
        }

        /** Where to ask when the next pass is due, in epoch millis. Null means nothing is scheduled. */
        fun nextPassAt(supplier: () -> Long?) {
            entry.nextAt = supplier
        }

        /** Where to read this processor's live counters — see the class header on why it is a supplier. */
        fun counts(supplier: () -> List<Count>) {
            entry.counts = supplier
        }

        /** …and where to read the breakdown of whatever total those counters carry. */
        fun reasons(supplier: () -> List<Breakdown>) {
            entry.reasons = supplier
        }

        /** What the last pass over [Work.stream]'s candidates achieved, replacing that stream's previous row. */
        fun record(work: Work) {
            entry.work[work.stream] = work
        }
    }

    companion object {
        /** Registered, nothing said yet — the honest word before the first pass or the first tick. */
        const val STARTING = "starting"

        /** A pass is dialling right now. */
        const val MEASURING = "measuring"

        /**
         * A pass is reading the STORE to work out what to dial — the alias
         * source's whole job, and the first minutes of a sweep.
         *
         * Its own word rather than [MEASURING], which this document defines as
         * "a pass is dialling right now". The derivation opens no socket at
         * all: it walks every relay-list source, drops what an operator
         * excluded and what a signed record calls dead, and hands what is left
         * to the three passes. Calling that `measuring` would put a reader on a
         * relay that is not answering when the router has not dialled one yet,
         * and the two states want opposite next moves.
         */
        const val COLLECTING = "collecting"

        /**
         * What a pass counts its progress in — see [Measuring.unit].
         *
         * Three words, because the passes decide different things: the stability
         * gate and the fitness pass answer about a URL, the fold answers about a
         * HOST and dials every url of one to do it. Publishing them all as
         * "relays" would put three quantities under one word on adjacent rows,
         * which is the exact overload the published glossary exists to stop.
         */
        const val UNIT_URL = "url"

        const val UNIT_HOST = "host"

        /**
         * …and the alias source counts neither: it walks one configured RELAY
         * LIST SOURCE at a time — each stream's `relaySource` block plus the
         * monitor's own — and how many urls that yields is the thing it is
         * finding out. A position counted in urls would need a denominator that
         * only exists once the walk it is meant to describe has finished.
         */
        const val UNIT_SOURCE = "source"

        /** Between passes: the last one finished and the next is on the clock. */
        const val IDLE = "idle"

        /** Always on, no passes — ingest, the healer, the push. */
        const val RUNNING = "running"

        /**
         * Built, and never started, because this deployment gives it nothing to
         * do — the two probe passes on a router whose upstreams are all named by
         * hand.
         *
         * Its own word rather than leaving the row at [STARTING] forever, which
         * is what it did: "waiting to begin" and "there is nothing here to
         * measure" are different states, and only the second one is permanent.
         */
        const val OFF = "off"

        /**
         * Named hosts per reason.
         *
         * It WAS three — "enough to recognise the pattern, not an inventory" —
         * and an inventory turns out to be the thing this list is for. The
         * fold's undecided rows are the answer to "which servers will not
         * fold", which is where an investigation of a roster inflated by
         * unfolded aliases starts, and three names out of twenty-eight cannot
         * start one: the pattern was already legible from the reason string,
         * so the only information the names carried was WHICH, and that was
         * the part being cut.
         *
         * A hundred, not unbounded, and that differs from `inFlight` on
         * purpose: a row there is a worker and the pool bounds it, while this
         * is bounded only by the host universe, which has no ceiling but
         * discovery. So this stays a safety ceiling rather than an editorial
         * one — it covers the fold's whole enumeration today with room over,
         * and the remainder past it is still readable as [Undecided.hosts]
         * minus the list.
         */
        const val MAX_UNDECIDED_EXAMPLES = 100

        /**
         * …and how many are named WITH their url counts, where a pass can count
         * them — see [Undecided.top].
         *
         * It was six — ranked, so a rank of three was not one, and six was
         * enough head to see the shape of a tail. That answered the SHAPE
         * question ("concentrated on a few servers, or spread across
         * thousands") and only that one. The other question these rows get
         * asked is WHICH, and six names out of a hundred and eighty-six is
         * not an answer to it — the reason a url will not stabilise is a
         * property of the server, so the names are the actionable half.
         *
         * A hundred for the same reason as [MAX_UNDECIDED_EXAMPLES], and
         * with the same rule: a safety ceiling, not an editorial one. Still
         * bounded twice, since the relay re-caps it, and still DELIBERATELY
         * NOT SUMMING to the reason's urls — a long head is still a head,
         * and the card draws the remainder as its own slice so a cut list
         * can never read as the whole one.
         */
        const val MAX_UNDECIDED_HOSTS = 100
    }
}
