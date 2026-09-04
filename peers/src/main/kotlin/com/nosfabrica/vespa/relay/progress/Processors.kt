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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The work this router does that is not a stream: the alias source, the
 * alias fold, the stability and fitness passes, the rotating pool, ingest,
 * the healer and the upstream push.
 *
 * Two kinds of job share one schema. A pass-shaped processor (fold, stability
 * gate, fitness) has a clock, a last run and a next one, with progress per
 * stream. A counter-shaped one (pool, ingest, healer, push) is always running
 * and has gauges. Both publish `phase` and `phaseForSec` like a stream, and
 * each fills in only the members it can honestly answer.
 *
 * Counters are read through a supplier at snapshot time rather than pushed:
 * they are live atomics owned by the component, and a hand-kept copy is how
 * a report comes to disagree with the thing it reports on.
 */
class Processors {
    /**
     * How far one pass got over one stream's candidate set. Per stream because
     * two streams discover different corners of the network and summing them
     * would double-count every shared url.
     */
    class Work(
        val stream: String,
        /** Urls that stream submitted, before anything was decided. The code's own word. */
        val candidates: Int,
        /**
         * Of those, how many arrived with no verdict: the pass's subject and
         * the denominator the card counts against. Null on a pass that does
         * not report it; a zero would claim nothing arrived undecided.
         */
        val newUrls: Int? = null,
        /**
         * Of those, how many a fold has taken out of the fan-out. First in
         * the partition: a folded url is never measured for stability. Null,
         * with the two below, on a pass that does not measure it; absent is
         * "does not answer", zero is "answered none".
         */
        val foldedAway: Int? = null,
        /**
         * The two standing verdicts across the whole candidate set, including
         * those read back from the store at boot. [decided] counts what this
         * pass learned.
         */
        val consistent: Int? = null,
        val inconsistent: Int? = null,
        /**
         * How many still have no verdict after this pass: the progress number.
         * Not the complement of [dialled], which counts what this pass spent.
         */
        val unmeasured: Int,
        /** Dials this pass spent: fingerprints for the fold, paired walks for the stability gate. */
        val dialled: Int,
        /** New verdicts it reached and published. */
        val decided: Int,
        /** Why the hosts it could not decide were not decided. Rows sum to [unmeasured]. */
        val undecided: List<Undecided> = emptyList(),
        /** Reasons left out of [undecided]. Always zero; kept so absent cannot read as "nothing dropped". */
        val undecidedOmitted: Int = 0,
    )

    /**
     * How far the pass running right now has got, the live half of [Work],
     * which is written only when a pass returns. The unit is carried because
     * the passes count different things: a url for the gate and fitness, a
     * host for the fold.
     */
    class Measuring(
        /** [UNIT_URL] or [UNIT_HOST]: what the two counts are counts of. */
        val unit: String,
        /** How many have ended, however they ended. What was learned is [Work.decided]. */
        val attempted: Int,
        /** How many this pass set out to walk, after dropping what already carries a verdict. */
        val toProbe: Int,
        /**
         * Seconds left at the rate so far, or null before the first unit
         * lands. The fold walks widest first, so its estimate reads long and improves.
         */
        val etaSec: Long?,
        /**
         * Seconds since a unit last ended. Tells a pass about to finish from
         * one whose last unit has wedged; [etaSec] reads 0 for both.
         */
        val quietForSec: Long,
    )

    /**
     * The urls a pass is holding right now and what it is doing with each.
     *
     * Its own shape rather than [InFlight]'s: a probe leg is a ladder, not a
     * transfer, so it has no slot and no delivery clocks. Longest-held first,
     * since every leg is bounded by a deadline and one near it is the anomaly.
     * Published whole, because the pass's own dial gates bound the set.
     */
    class Holding(
        /** Urls with a live job, longest-held first. */
        val relays: List<Held>,
        /** Always zero; kept so absent cannot read as "nothing dropped". */
        val omitted: Int,
    ) {
        /** One url a probe job is holding, and what it is doing with it. */
        class Held(
            val relay: String,
            /** Since the job took its permit, which is after the wait for one. */
            val heldForSec: Long,
            /** Which step, in the pass's own words. */
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
         * The reason this one refines, or null. The list stays flat and still
         * sums to [Work.unmeasured]; the parent lets a reader nest rows
         * without the arithmetic having to survive a tree.
         */
        val parent: String? = null,
        val hosts: Int,
        /** Hosts by name, for a pass that has only names to give (the fold). Bounded at [MAX_UNDECIDED_EXAMPLES]. */
        val examples: List<String> = emptyList(),
        /**
         * The widest few with their url counts, for a pass that measures urls.
         * Bounded at [MAX_UNDECIDED_HOSTS] and never summing to the reason; the
         * remainder is the reader's to see.
         */
        val top: List<HostCount> = emptyList(),
        /**
         * How many urls ended the pass that way; the count that sums back to
         * [Work.unmeasured]. Zero on a pass that counts hosts alone.
         */
        val urls: Int = 0,
    )

    /** One reason a total breaks down by, counted in events. [Undecided] counts hosts. */
    class Breakdown(
        val reason: String,
        val events: Long,
    )

    /** One gauge a counter-shaped processor publishes, read live at snapshot time. */
    class Count(
        val name: String,
        val value: Long,
    )

    /** One processor's state, flattened for a reader outside this process. */
    class Snapshot(
        val name: String,
        val phase: String,
        val phaseForSec: Long,
        /** Passes run since this process started, or null for a processor that has none. */
        val passes: Long?,
        /** When the last pass ended, in epoch seconds. Null before the first one finishes. */
        val lastPassAt: Long?,
        val lastPassSec: Long?,
        /** Seconds until the next pass, or null when nothing is scheduled. */
        val nextInSec: Long?,
        /** The pass in flight, or null when none is. The only member that moves while a pass runs. */
        val measuring: Measuring? = null,
        /** Which urls it is holding, or null when nothing is held. */
        val inFlight: Holding? = null,
        val work: List<Work>,
        val counts: List<Count>,
        /** Reasons a counter-shaped processor breaks its total down by; ingest's rejections, today. */
        val reasons: List<Breakdown> = emptyList(),
    )

    /**
     * One pass's live position. The counter lives inside the object so a
     * reader takes unit, size and count from one pass in one volatile read.
     */
    internal class Run(
        val unit: String,
        val toProbe: Int,
        /**
         * When the walk started, not when the pass did. A pass derives its set
         * first, and that derivation must not land in the rate's numerator.
         */
        val startedMs: Long,
    ) {
        val attempted = AtomicInteger()

        /** When a unit last ended, or the walk's start before the first one. See [Measuring.quietForSec]. */
        val lastUnitMs = AtomicLong(startedMs)
    }

    // Internal rather than private because [Handle] takes one as a parameter.
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

        /** Epoch millis the next pass is due, asked live. */
        @Volatile
        var nextAt: (() -> Long?)? = null

        /**
         * The running pass's position, or null between passes and before a
         * pass has derived its set. One reference swapped whole, so a snapshot
         * taken from another thread cannot pair one pass's unit with the next's size.
         */
        @Volatile
        var run: Run? = null

        /**
         * What this processor is holding: url to (taken at, step). A plain map
         * rather than a swapped reference because each entry is one job's own.
         * Cleared on both pass boundaries.
         */
        val held = ConcurrentHashMap<String, Pair<Long, String>>()

        @Volatile
        var counts: (() -> List<Count>)? = null

        @Volatile
        var reasons: (() -> List<Breakdown>)? = null

        /** Latest result per stream, replacing rather than appending. */
        val work = ConcurrentHashMap<String, Work>()
    }

    private val entries = ConcurrentHashMap<String, Entry>()

    /** Registration order, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    /** The handle [name] reports through. Idempotent, so a component can be wired from one place and driven from another. */
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
                // Never negative: an overdue pass has a due time in the past.
                nextInSec = e.nextAt?.invoke()?.let { ((it - nowMs) / 1000).coerceAtLeast(0) },
                measuring = measuring(e, nowMs),
                inFlight = holding(e, nowMs),
                // Ordered so two rollups of one state produce the same document.
                work = e.work.values.sortedBy { it.stream },
                counts = e.counts?.invoke().orEmpty(),
                reasons = e.reasons?.invoke().orEmpty(),
            )
        }

    /**
     * The pass in flight as of [nowMs], or null. Guarded by [Entry.run] alone:
     * it is set when a pass declares its set and cleared by both [Handle.begin]
     * and [Handle.finish], so a stale position is never published under `idle`.
     */
    private fun measuring(
        e: Entry,
        nowMs: Long,
    ): Measuring? {
        // Read once: the field is swapped whole on a pass boundary.
        val run = e.run ?: return null
        val attempted = run.attempted.get()
        val toProbe = run.toProbe
        val elapsedMs = (nowMs - run.startedMs).coerceAtLeast(0)
        return Measuring(
            unit = run.unit,
            attempted = attempted,
            toProbe = toProbe,
            // Null before the first unit and again after the last: nothing to
            // extrapolate from, and no remainder to estimate.
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
     * What the pass is holding as of [nowMs], or null. Not guarded by
     * [Entry.run]: the fold takes its first dial before it can declare a set.
     */
    private fun holding(
        e: Entry,
        nowMs: Long,
    ): Holding? {
        if (e.held.isEmpty()) return null
        // Snapshotted before sorting: the map moves under a wide pass on every
        // dial, and a comparator reading a moving value could throw.
        val rows = e.held.entries.map { (relay, taken) -> Triple(relay, taken.first, taken.second) }
        val named =
            rows
                // Longest-held first, then by name so one state rolls up one way.
                .sortedWith(compareBy({ it.second }, { it.first }))
                .map { (relay, takenMs, stage) ->
                    Holding.Held(
                        relay = relay,
                        heldForSec = ((nowMs - takenMs) / 1000).coerceAtLeast(0),
                        stage = stage,
                    )
                }
        return Holding(named, 0)
    }

    /** What a processor holds to report through. Cheap to keep; safe to call from anywhere. */
    class Handle internal constructor(
        private val entry: Entry,
    ) {
        /** Say what this processor is doing. The clock restarts only when the word changes. */
        fun phase(word: String) {
            if (entry.phase != word) {
                entry.phase = word
                entry.sinceMs = System.currentTimeMillis()
            }
        }

        /** A pass has started. [nowMs] is a parameter so a test can set both ends of the elapsed time. */
        fun begin(
            word: String = MEASURING,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.startedMs = nowMs
            // The previous pass's position and held urls go before this one can
            // be read against them; a pass derives its set some way in.
            entry.run = null
            entry.held.clear()
            phase(word)
        }

        /**
         * A pass has ended, however it ended. Called from a `finally`, so a
         * pass that threw still stamps its clock. A finish with no begin does
         * nothing, which keeps a self-bracketing pass from being counted twice.
         */
        fun finish(
            word: String = IDLE,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            val startedMs = entry.startedMs ?: return
            entry.lastPassSec = ((nowMs - startedMs) / 1000).coerceAtLeast(0)
            entry.startedMs = null
            entry.run = null
            entry.held.clear()
            entry.lastPassAtSec = nowMs / 1000
            entry.passes.incrementAndGet()
            phase(word)
        }

        /**
         * What this pass set out to walk, declared from inside the pass once
         * its store reads have said. The rate is timed from here, not [begin].
         */
        fun measuring(
            toProbe: Int,
            unit: String,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.run = Run(unit, toProbe, nowMs)
        }

        /** One more unit of this pass is behind it, however it ended. */
        fun attempted(
            units: Int = 1,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            // Silent when no pass has declared a set: a numerator with no
            // denominator is better dropped than invented.
            val run = entry.run ?: return
            run.attempted.addAndGet(units)
            run.lastUnitMs.set(nowMs)
        }

        /**
         * This pass has taken [relay] and is [stage] on it. Called again as the
         * job moves on, which updates the step and keeps the clock.
         */
        fun holding(
            relay: String,
            stage: String,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.held.compute(relay) { _, existing -> (existing?.first ?: nowMs) to stage }
        }

        /** The job is done with [relay], however it ended. Call from a `finally`. */
        fun released(relay: String) {
            entry.held.remove(relay)
        }

        /** Where to ask when the next pass is due, in epoch millis. Null means nothing is scheduled. */
        fun nextPassAt(supplier: () -> Long?) {
            entry.nextAt = supplier
        }

        /** Where to read this processor's live counters. */
        fun counts(supplier: () -> List<Count>) {
            entry.counts = supplier
        }

        /** Where to read the breakdown of whatever total those counters carry. */
        fun reasons(supplier: () -> List<Breakdown>) {
            entry.reasons = supplier
        }

        /** What the last pass over [Work.stream]'s candidates achieved, replacing that stream's previous row. */
        fun record(work: Work) {
            entry.work[work.stream] = work
        }
    }

    companion object {
        /**
         * One processor row, as both status documents publish it. Here rather
         * than in either plane's builder so there is one shape for the card.
         */
        fun published(p: Snapshot): JsonObject =
            buildJsonObject {
                put("name", p.name)
                put("phase", p.phase)
                put("phaseForSec", p.phaseForSec)
                // `passesRun`, not `passes`: a stream's `passes` is a list.
                p.passes?.let { put("passesRun", it) }
                p.lastPassAt?.let { put("lastPassAt", it) }
                p.lastPassSec?.let { put("lastPassSec", it) }
                p.nextInSec?.let { put("nextInSec", it) }
                // Not exclusive with the countdown: a fast-lane pass runs
                // between sweeps, so the fitness row can carry both.
                p.measuring?.let { m ->
                    put(
                        "measuring",
                        buildJsonObject {
                            put("unit", m.unit)
                            put("attempted", m.attempted)
                            put("toProbe", m.toProbe)
                            m.etaSec?.let { put("etaSec", it) }
                            put("quietForSec", m.quietForSec)
                        },
                    )
                }
                // Longest-held first, the reverse of a stream's [InFlight].
                p.inFlight?.takeIf { it.relays.isNotEmpty() }?.let { f ->
                    put(
                        "inFlight",
                        buildJsonObject {
                            putJsonArray("relays") {
                                for (r in f.relays) {
                                    add(
                                        buildJsonObject {
                                            put("relay", r.relay)
                                            put("heldForSec", r.heldForSec)
                                            put("stage", r.stage)
                                        },
                                    )
                                }
                            }
                            put("omitted", f.omitted)
                        },
                    )
                }
                for (c in p.counts) put(c.name, c.value)
                // `rejected` is mostly the pipeline working: a mirror is offered
                // an event once per relay holding it. The split is what says so.
                p.reasons.takeIf { it.isNotEmpty() }?.let { rows ->
                    put(
                        "rejections",
                        buildJsonObject {
                            putJsonArray("reasons") {
                                for (r in rows) {
                                    add(
                                        buildJsonObject {
                                            put("reason", r.reason)
                                            put("events", r.events)
                                        },
                                    )
                                }
                            }
                        },
                    )
                }
                p.work.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("streams") {
                        for (w in rows) {
                            add(
                                buildJsonObject {
                                    put("name", w.stream)
                                    put("candidates", w.candidates)
                                    w.newUrls?.let { put("newUrls", it) }
                                    // The partition, in precedence order:
                                    // `candidates = foldedAway + consistent + inconsistent + unmeasured`.
                                    // Written at zero so a reader can sum them, but absent
                                    // from a pass that does not measure them (the fold).
                                    w.foldedAway?.let { put("foldedAway", it) }
                                    w.consistent?.let { put("consistent", it) }
                                    w.inconsistent?.let { put("inconsistent", it) }
                                    put("unmeasured", w.unmeasured)
                                    put("dialled", w.dialled)
                                    put("decided", w.decided)
                                    w.undecided.takeIf { it.isNotEmpty() }?.let { reasons ->
                                        put(
                                            "undecided",
                                            buildJsonObject {
                                                putJsonArray("reasons") {
                                                    for (u in reasons) {
                                                        add(
                                                            buildJsonObject {
                                                                put("reason", u.reason)
                                                                u.parent?.let { put("parent", it) }
                                                                // Urls sum back to `unmeasured`; hosts name who to chase.
                                                                put("urls", u.urls)
                                                                put("hosts", u.hosts)
                                                                u.examples.takeIf { it.isNotEmpty() }?.let { names ->
                                                                    putJsonArray("examples") { for (h in names) add(h) }
                                                                }
                                                                u.top.takeIf { it.isNotEmpty() }?.let { rows ->
                                                                    putJsonArray("top") {
                                                                        for (h in rows) {
                                                                            add(
                                                                                buildJsonObject {
                                                                                    put("host", h.host)
                                                                                    put("urls", h.urls)
                                                                                },
                                                                            )
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                        )
                                                    }
                                                }
                                                put("omitted", w.undecidedOmitted)
                                            },
                                        )
                                    }
                                },
                            )
                        }
                    }
                }
            }

        /** Registered, nothing said yet. */
        const val STARTING = "starting"

        /** A pass is dialling right now. */
        const val MEASURING = "measuring"

        /**
         * A pass is reading the store to work out what to dial: the alias
         * source's whole job and the first minutes of a sweep. No socket is
         * open, which is why it is not [MEASURING].
         */
        const val COLLECTING = "collecting"

        /**
         * What a pass counts its progress in. See [Measuring.unit]. The gate
         * and fitness answer about a url; the fold answers about a host.
         */
        const val UNIT_URL = "url"

        const val UNIT_HOST = "host"

        /**
         * The alias source walks one configured relay-list source at a time;
         * how many urls it yields is only known once the walk has finished.
         */
        const val UNIT_SOURCE = "source"

        /** Between passes: the last one finished and the next is on the clock. */
        const val IDLE = "idle"

        /** Always on, no passes: ingest, the healer, the push. */
        const val RUNNING = "running"

        /** Built and never started because this deployment gives it nothing to do. Permanent, unlike [STARTING]. */
        const val OFF = "off"

        /**
         * Named hosts per reason. A safety ceiling, not an editorial one: the
         * host universe has no bound but discovery, and which servers will not
         * fold is the actionable half of the row.
         */
        const val MAX_UNDECIDED_EXAMPLES = 100

        /**
         * Hosts named with their url counts, where a pass can count them. See
         * [Undecided.top]. A safety ceiling on the same terms as
         * [MAX_UNDECIDED_EXAMPLES]; the list never sums to the reason's urls.
         */
        const val MAX_UNDECIDED_HOSTS = 100
    }
}
