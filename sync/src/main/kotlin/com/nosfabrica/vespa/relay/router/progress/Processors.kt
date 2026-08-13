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
 *  - the **NIP-66 reachability monitor**, quartz's, which watches every socket
 *    this client opens and signs what it learns into the same kind-30166
 *    records the two passes above write their tags onto;
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
 * schemas. A PASS-shaped processor (the fold, the stability gate) has a clock,
 * a last run and a next one, and its progress is per stream — each stream
 * submits its own candidate set. A COUNTER-shaped one (ingest, the healer, the
 * push, the reachability monitor) has no passes at all; it is always running
 * and what it has is gauges. Both publish `phase` and `phaseForSec` like a
 * stream does, so the card can draw them the same way, and each fills in only
 * the members it can honestly answer.
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
        /** Urls that stream submitted, before anything was decided. */
        val subjects: Int,
        /**
         * …of those, how many still have no verdict after this pass.
         *
         * THE PROGRESS NUMBER. A pass is capped (`probesPerCycle`), a group can
         * be held on a cooldown, and a host that cannot be measured never
         * resolves at all — so this falling pass over pass is what "the fold is
         * getting somewhere" looks like, and this sitting still while `measured`
         * climbs is the state that used to be invisible.
         */
        val outstanding: Int,
        /** Dials this pass spent — fingerprints for the fold, url walks for the stability gate. */
        val measured: Int,
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

    /** One reason a pass ended some hosts with nothing written down, and who they were. */
    class Undecided(
        /** The reason, in the words the router's own log uses. */
        val reason: String,
        /** How many hosts ended the pass that way. */
        val hosts: Int,
        /** A couple of them by name, so the reason has a subject to chase. */
        val examples: List<String>,
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
        val work: List<Work>,
        val counts: List<Count>,
    )

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

        @Volatile
        var counts: (() -> List<Count>)? = null

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
                // Ordered, for the same reason every other list here is: two
                // rollups of one state must produce the same document.
                work = e.work.values.sortedBy { it.stream },
                counts = e.counts?.invoke().orEmpty(),
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

        /** A pass has started. */
        fun begin(word: String = MEASURING) {
            entry.startedMs = System.currentTimeMillis()
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
         */
        fun finish(
            word: String = IDLE,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.startedMs?.let { entry.lastPassSec = ((nowMs - it) / 1000).coerceAtLeast(0) }
            entry.startedMs = null
            entry.lastPassAtSec = nowMs / 1000
            entry.passes.incrementAndGet()
            phase(word)
        }

        /** Where to ask when the next pass is due, in epoch millis. Null means nothing is scheduled. */
        fun nextPassAt(supplier: () -> Long?) {
            entry.nextAt = supplier
        }

        /** Where to read this processor's live counters — see the class header on why it is a supplier. */
        fun counts(supplier: () -> List<Count>) {
            entry.counts = supplier
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

        /** Between passes: the last one finished and the next is on the clock. */
        const val IDLE = "idle"

        /** Always on, no passes — ingest, the healer, the push. */
        const val RUNNING = "running"

        /** Always on and OBSERVING rather than working: the NIP-66 monitor rides other people's sockets. */
        const val WATCHING = "watching"

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
         * Six is the whole enumeration the fold can produce today, so this cuts
         * nothing in practice and still refuses to be unbounded if that list
         * grows — the same bargain `foldedOnto` and `inFlight` make.
         */
        const val MAX_UNDECIDED_REASONS = 6

        /** Named hosts per reason. Enough to recognise the pattern, not an inventory. */
        const val MAX_UNDECIDED_EXAMPLES = 3
    }
}
