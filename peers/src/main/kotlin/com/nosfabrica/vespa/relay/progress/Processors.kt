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
import java.util.concurrent.atomic.AtomicReference

/**
 * The work this router does that is not a stream: the probe passes, the rotating pool, ingest,
 * the healer and the push. A pass-shaped processor has a clock and progress per stream; a
 * counter-shaped one is always running and has gauges read live through a supplier at snapshot time.
 */
class Processors {
    /** How far one pass got over one stream's candidate set. Per stream, since streams share urls. */
    class Work(
        val stream: String,
        /** Urls that stream submitted, before anything was decided. */
        val candidates: Int,
        /** Of those, how many arrived with no verdict: the denominator. Null on a pass that does not report it. */
        val newUrls: Int? = null,
        /**
         * Of those, how many a fold has taken out of the fan-out. Null, with the two below, on a
         * pass that does not measure it: absent is "does not answer", zero is "answered none".
         */
        val foldedAway: Int? = null,
        /** The two standing verdicts across the whole candidate set, including those read back at boot. */
        val consistent: Int? = null,
        val inconsistent: Int? = null,
        /** How many still have no verdict after this pass. Not the complement of [dialled]. */
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

    /** How far the pass running right now has got, the live half of [Work]. */
    class Measuring(
        /** [UNIT_URL] or [UNIT_HOST]: what the two counts are counts of. */
        val unit: String,
        /** How many have ended, however they ended. */
        val attempted: Int,
        /** How many this pass set out to walk, after dropping what already carries a verdict. */
        val toProbe: Int,
        /** Seconds left at the rate so far, or null before the first unit lands. */
        val etaSec: Long?,
        /** Seconds since a unit last ended; tells a finishing pass from a wedged last unit. */
        val quietForSec: Long,
    )

    /**
     * The urls a pass is holding right now and what it is doing with each. Not [InFlight]'s
     * shape: a probe leg has no slot and no delivery clocks. Published whole, longest-held first.
     */
    class Holding(
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
        /** The reason this one refines, or null. The list stays flat and still sums to [Work.unmeasured]. */
        val parent: String? = null,
        val hosts: Int,
        /** Hosts by name, for a pass that has only names to give. Bounded at [MAX_UNDECIDED_EXAMPLES]. */
        val examples: List<String> = emptyList(),
        /** The widest few with their url counts. Bounded at [MAX_UNDECIDED_HOSTS]; never sums to the reason. */
        val top: List<HostCount> = emptyList(),
        /** How many urls ended the pass that way. Zero on a pass that counts hosts alone. */
        val urls: Int = 0,
    )

    /**
     * The servers graded `prime` under more than one url as the last sweep left them, so one server
     * on the roster several times is a number rather than a hand count.
     */
    class SharedHosts(
        /** Servers carrying more than one `prime` url. */
        val hosts: Int,
        /** The `prime` urls on them. */
        val urls: Int,
        /** Of those servers, how many hold two or more `prime` urls the fold has not proven their own relays. */
        val unproven: Int,
        /** The worst few, unproven first. Bounded at [MAX_SHARED_HOSTS]. */
        val top: List<SharedHost>,
    )

    /** One server's `prime` urls and what the fold holds about each. */
    class SharedHost(
        val host: String,
        val urls: Int,
        /** Two or more of its `prime` urls are not measured as their own relays; decided over all of them, not [relays]. */
        val unproven: Boolean,
        /** Bounded at [MAX_SHARED_HOST_URLS]; [urls] is the whole count. */
        val relays: List<FoldOf>,
    )

    /** One url and the fold's word on it: [FOLD_ALIAS], [FOLD_CANONICAL], [FOLD_DISTINCT], [FOLD_STALE] or [FOLD_NONE]. */
    class FoldOf(
        val relay: String,
        val fold: String,
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
        /** The fitness pass's census of servers graded more than once, or null before a sweep has taken one. */
        val sharedHosts: SharedHosts? = null,
    )

    /** One pass's live position, swapped whole so a reader takes unit, size and count from one pass. */
    internal class Run(
        val unit: String,
        val toProbe: Int,
        /** When the walk started, not when the pass did; deriving the set must not land in the rate. */
        val startedMs: Long,
    ) {
        val attempted = AtomicInteger()

        /** When a unit last ended, or the walk's start before the first one. */
        val lastUnitMs = AtomicLong(startedMs)
    }

    /** The phase and the pass bookkeeping, swapped whole so a reader never sees half a [Handle.finish]. */
    internal data class State(
        val phase: String,
        val sinceMs: Long,
        val passes: Long = 0,
        val startedMs: Long? = null,
        val lastPassAtSec: Long? = null,
        val lastPassSec: Long? = null,
    ) {
        /** The clock restarts only when the word changes. */
        fun phased(
            word: String,
            nowMs: Long,
        ): State = if (phase == word) this else copy(phase = word, sinceMs = nowMs)
    }

    internal class Entry(
        val name: String,
        phase: String,
        sinceMs: Long,
    ) {
        val state = AtomicReference(State(phase, sinceMs))

        /** Epoch millis the next pass is due, asked live. */
        @Volatile
        var nextAt: (() -> Long?)? = null

        /** The running pass's position, or null between passes and before a pass has derived its set. */
        @Volatile
        var run: Run? = null

        /** What this processor is holding: url to (taken at, step). Cleared on both pass boundaries. */
        val held = ConcurrentHashMap<String, Pair<Long, String>>()

        @Volatile
        var counts: (() -> List<Count>)? = null

        @Volatile
        var reasons: (() -> List<Breakdown>)? = null

        /** Latest result per stream, replacing rather than appending. */
        val work = ConcurrentHashMap<String, Work>()

        @Volatile
        var sharedHosts: SharedHosts? = null
    }

    private val entries = ConcurrentHashMap<String, Entry>()

    /** Registration order, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    /** The handle [name] reports through. Idempotent. */
    @Synchronized
    fun of(name: String): Handle {
        val entry =
            entries.getOrPut(name) {
                order += name
                Entry(name, STARTING, System.currentTimeMillis())
            }
        return Handle(entry)
    }

    /** Every registered processor, in registration order, as of now. The suppliers run outside the lock. */
    fun snapshot(nowMs: Long = System.currentTimeMillis()): List<Snapshot> {
        val registered = synchronized(this) { order.mapNotNull { entries[it] } }
        return registered.map { e ->
            val s = e.state.get()
            Snapshot(
                name = e.name,
                phase = s.phase,
                phaseForSec = ((nowMs - s.sinceMs) / 1000).coerceAtLeast(0),
                passes = s.passes.takeIf { it > 0 || s.startedMs != null },
                lastPassAt = s.lastPassAtSec,
                lastPassSec = s.lastPassSec,
                nextInSec = e.nextAt?.invoke()?.let { ((it - nowMs) / 1000).coerceAtLeast(0) },
                measuring = measuring(e, nowMs),
                inFlight = holding(e, nowMs),
                work = e.work.values.sortedBy { it.stream },
                counts = e.counts?.invoke().orEmpty(),
                reasons = e.reasons?.invoke().orEmpty(),
                sharedHosts = e.sharedHosts,
            )
        }
    }

    /** The pass in flight as of [nowMs], or null. */
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
            // Null before the first unit and again after the last.
            etaSec =
                if (attempted in 1 until toProbe) {
                    ((elapsedMs.toDouble() / attempted) * (toProbe - attempted) / 1000).toLong()
                } else {
                    null
                },
            quietForSec = ((nowMs - run.lastUnitMs.get()) / 1000).coerceAtLeast(0),
        )
    }

    /** What the pass is holding as of [nowMs], or null. Not guarded by [Entry.run]: the fold dials before it declares a set. */
    private fun holding(
        e: Entry,
        nowMs: Long,
    ): Holding? {
        if (e.held.isEmpty()) return null
        // Snapshotted before sorting: a comparator reading the moving map could throw.
        val rows = e.held.entries.map { (relay, taken) -> Triple(relay, taken.first, taken.second) }
        val named =
            rows
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

    /** What a processor holds to report through. Safe to call from anywhere. */
    class Handle internal constructor(
        private val entry: Entry,
    ) {
        /** Say what this processor is doing. The clock restarts only when the word changes. */
        fun phase(word: String) {
            val now = System.currentTimeMillis()
            entry.state.updateAndGet { it.phased(word, now) }
        }

        /** A pass has started. */
        fun begin(
            word: String = MEASURING,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.run = null
            entry.held.clear()
            val now = System.currentTimeMillis()
            entry.state.updateAndGet { it.copy(startedMs = nowMs).phased(word, now) }
        }

        /** A pass has ended, however it ended. Call from a `finally`. A finish with no begin does nothing. */
        fun finish(
            word: String = IDLE,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            val now = System.currentTimeMillis()
            while (true) {
                val s = entry.state.get()
                val startedMs = s.startedMs ?: return
                val ended =
                    s
                        .copy(
                            passes = s.passes + 1,
                            startedMs = null,
                            lastPassAtSec = nowMs / 1000,
                            lastPassSec = ((nowMs - startedMs) / 1000).coerceAtLeast(0),
                        ).phased(word, now)
                if (entry.state.compareAndSet(s, ended)) break
            }
            entry.run = null
            entry.held.clear()
        }

        /** What this pass set out to walk, declared once its store reads have said. The rate is timed from here. */
        fun measuring(
            toProbe: Int,
            unit: String,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            entry.run = Run(unit, toProbe, nowMs)
        }

        /** Drop the position of work that was not a pass, such as a boot retraction, without counting a pass. */
        fun clearMeasuring() {
            entry.run = null
        }

        /** One more unit of this pass is behind it, however it ended. */
        fun attempted(
            units: Int = 1,
            nowMs: Long = System.currentTimeMillis(),
        ) {
            // Silent when no pass has declared a set.
            val run = entry.run ?: return
            run.attempted.addAndGet(units)
            run.lastUnitMs.set(nowMs)
        }

        /** This pass has taken [relay] and is [stage] on it. Calling again updates the step and keeps the clock. */
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

        /** What the last pass over [Work.stream]'s candidates achieved, replacing the previous row. */
        fun record(work: Work) {
            entry.work[work.stream] = work
        }

        /** The last sweep's census of servers graded more than once, replacing the previous one. */
        fun sharedHosts(census: SharedHosts) {
            entry.sharedHosts = census
        }
    }

    companion object {
        /** One processor row, as both status documents publish it. */
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
                // Not exclusive with the countdown: a fast-lane pass runs between sweeps.
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
                p.sharedHosts?.let { census ->
                    put(
                        "sharedHosts",
                        buildJsonObject {
                            put("hosts", census.hosts)
                            put("urls", census.urls)
                            put("unproven", census.unproven)
                            putJsonArray("top") {
                                for (h in census.top) {
                                    add(
                                        buildJsonObject {
                                            put("host", h.host)
                                            put("urls", h.urls)
                                            put("unproven", h.unproven)
                                            putJsonArray("relays") {
                                                for (r in h.relays) {
                                                    add(
                                                        buildJsonObject {
                                                            put("relay", r.relay)
                                                            put("fold", r.fold)
                                                        },
                                                    )
                                                }
                                            }
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
                                    // `candidates = foldedAway + consistent + inconsistent + unmeasured`;
                                    // written at zero, absent from a pass that does not measure them.
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

        /** A pass is reading the store to work out what to dial; no socket is open. */
        const val COLLECTING = "collecting"

        /** What a pass counts its progress in: the gate and fitness answer about a url, the fold about a host. */
        const val UNIT_URL = "url"

        const val UNIT_HOST = "host"

        /** The alias source walks one configured relay-list source at a time. */
        const val UNIT_SOURCE = "source"

        /** A verdict written. The fitness pass re-arms on it: its writes outnumber its dials. */
        const val UNIT_VERDICT = "verdict"

        /** Between passes: the last one finished and the next is on the clock. */
        const val IDLE = "idle"

        /** Always on, no passes: ingest, the healer, the push. */
        const val RUNNING = "running"

        /** Built and never started because this deployment gives it nothing to do. Permanent. */
        const val OFF = "off"

        /** Named hosts per reason: a safety ceiling, not an editorial one. */
        const val MAX_UNDECIDED_EXAMPLES = 100

        /** Hosts named with their url counts, on the same terms as [MAX_UNDECIDED_EXAMPLES]. */
        const val MAX_UNDECIDED_HOSTS = 100

        /** Servers named in [SharedHosts.top]. */
        const val MAX_SHARED_HOSTS = 25

        /** Urls named per server in [SharedHost.relays]. */
        const val MAX_SHARED_HOST_URLS = 25

        /** The fold's words for a url in [FoldOf]: folded onto another url. */
        const val FOLD_ALIAS = "alias"

        /** Other urls fold onto it. */
        const val FOLD_CANONICAL = "canonical"

        /** Measured and matched nothing it was compared to. */
        const val FOLD_DISTINCT = "distinct"

        /** A fold answer exists, under older rules or past its TTL. */
        const val FOLD_STALE = "stale"

        /** No fold answer at all. */
        const val FOLD_NONE = "none"

        /** The boot retraction is taking back grades signed under older rules; no pass runs until it ends. */
        const val RETIRING = "retiring"
    }
}
