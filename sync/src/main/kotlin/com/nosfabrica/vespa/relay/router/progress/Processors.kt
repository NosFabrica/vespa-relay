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
        /** Reasons left out of [undecided]. Never silent, for [InFlight]'s reason. */
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
    )

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
         * A couple of them by name, for a pass that has only NAMES to give —
         * the fold, which decides a host at a time. A pass that can count fills
         * [top] instead: the same disclosure with the number that ranks it.
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
        )
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
        fun attempted(units: Int = 1) {
            // Silent when no pass has declared a set — a caller that counts
            // without declaring is publishing a numerator with no denominator,
            // and dropping it is better than inventing one.
            entry.run?.attempted?.addAndGet(units)
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
         * What a pass counts its progress in — see [Measuring.unit].
         *
         * Two words, because the passes decide different things: the stability
         * gate and the fitness pass answer about a URL, the fold answers about a
         * HOST and dials every url of one to do it. Publishing both as "relays"
         * would put two quantities under one word on adjacent rows, which is the
         * exact overload the published glossary exists to stop.
         */
        const val UNIT_URL = "url"

        const val UNIT_HOST = "host"

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
         * How many `undecided` reasons a work row publishes.
         *
         * Sixteen covers both enumerations whole with headroom: five from the
         * fold, and thirteen from the stability gate — six reasons plus the
         * SEVEN causes `never answered a REQ` splits into. Truncating here does
         * not merely shorten a list, it breaks the property the url counts
         * exist for: the rows sum to [Work.unmeasured], and a cut tail surfaces
         * on the card as `not accounted for` in the fault tone — an arithmetic
         * error reported against a pass that was working. This has been one
         * short twice, once at six and once at eight, both times because a
         * reason list grew and the cap did not.
         */
        const val MAX_UNDECIDED_REASONS = 16

        /** Named hosts per reason. Enough to recognise the pattern, not an inventory. */
        const val MAX_UNDECIDED_EXAMPLES = 3

        /**
         * …and how many are named WITH their url counts, where a pass can count
         * them — see [Undecided.top].
         *
         * Six rather than three, because these are ranked and a rank of three is
         * not one: the question they answer is whether a reason is concentrated
         * on a few servers or spread across thousands, and three rows is too
         * short a head to see the shape of the tail. Still an inventory nobody
         * has to scroll, and still bounded twice — the relay re-caps it.
         */
        const val MAX_UNDECIDED_HOSTS = 6
    }
}
