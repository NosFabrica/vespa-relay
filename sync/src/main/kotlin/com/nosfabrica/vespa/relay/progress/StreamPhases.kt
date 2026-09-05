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

import com.nosfabrica.vespa.relay.util.fmtDuration
import java.util.concurrent.ConcurrentHashMap

/**
 * What each stream is doing right now, for the progress document and the log. A configured
 * stream is never absent, and every phase reports its elapsed time so stillness can be told
 * from silence.
 */
class StreamPhases {
    sealed interface Phase {
        /** Registered, not yet visiting. */
        data object Starting : Phase

        /**
         * Riding the visit pool. Zero relays is a report, not an absence: the roster is
         * waiting on its first certified relay.
         */
        data class Rotating(
            val relays: Int,
            val tailed: Int,
            /** Relays waiting for a worker rather than on a revisit timer. */
            val queued: Int,
        ) : Phase
    }

    /** One stream's state at one instant, so the document's members are not read at different times. */
    class Stream(
        val name: String,
        /** The phase's own word, never the rendered line. */
        val phase: String,
        val phaseForSec: Long,
        /** This stream's units, one per relay; [tails] and [queued] partition it. */
        val roster: Int? = null,
        val tails: Int? = null,
        val queued: Int? = null,
        /** Relays with a worker on them, quietest first; null when no source is registered. */
        val inFlight: InFlight? = null,
        /** Empty for a stream whose engine caps nothing. */
        val limits: List<Limit> = emptyList(),
        /** Empty for a stream that schedules no re-read of the past. */
        val schedule: List<Scheduled> = emptyList(),
    )

    /**
     * One scheduled job's clock over every ask this stream has. [neverRun] is counted apart
     * from [due] because an ask with no completed pass is always due.
     */
    class Scheduled(
        /** The pool word this clocks: `negentropy` or `re-fetching`. */
        val job: String,
        val everySec: Long,
        /** Asks whose clock has run out, waiting for a visit. */
        val due: Int,
        /** Asks with no completed pass behind them. */
        val neverRun: Int,
        /** Asks inside their period. */
        val waiting: Int,
        /** Seconds until the nearest waiting ask comes due, or null when none is waiting. */
        val nextInSec: Long?,
    )

    /**
     * One stream's share of one pool job: the cap, what is out against it, and how much work
     * the cap has turned away.
     */
    class Limit(
        /** The pool word this bounds: `visiting`, `live`, `re-fetching`, `negentropy`. */
        val job: String,
        /** Null where the stream has no share of its own and is bounded by the dial width. */
        val cap: Int?,
        /** Permits out against [cap]; null when there is no cap. */
        val inUse: Int?,
        /** Permits refused for this job since boot. */
        val deferred: Long,
    )

    private class Entry(
        @Volatile var phase: Phase,
        @Volatile var sinceMs: Long,
        @Volatile var inFlight: (() -> InFlight)? = null,
        @Volatile var limits: (() -> List<Limit>)? = null,
        @Volatile var schedule: (() -> List<Scheduled>)? = null,
    )

    private val phases = ConcurrentHashMap<String, Entry>()

    /** Registration order, so the report reads the same way every tick. */
    private val order = mutableListOf<String>()

    @Synchronized
    fun register(name: String) {
        if (phases.putIfAbsent(name, Entry(Phase.Starting, System.currentTimeMillis())) == null) {
            order += name
        }
    }

    /**
     * Where to ask [name] about itself. Each source is invoked at snapshot time; an omitted
     * argument leaves that source alone rather than clearing it.
     */
    @Synchronized
    fun names(
        name: String,
        inFlight: (() -> InFlight)? = null,
        limits: (() -> List<Limit>)? = null,
        schedule: (() -> List<Scheduled>)? = null,
    ) {
        register(name)
        val entry = phases[name] ?: return
        inFlight?.let { entry.inFlight = it }
        limits?.let { entry.limits = it }
        schedule?.let { entry.schedule = it }
    }

    /** Move [name] to [phase]. The elapsed clock restarts only when the phase changes kind. */
    @Synchronized
    fun set(
        name: String,
        phase: Phase,
    ) {
        register(name)
        val existing = phases[name] ?: return
        if (existing.phase::class != phase::class) existing.sinceMs = System.currentTimeMillis()
        existing.phase = phase
    }

    /** Every registered stream, in registration order, as of now. */
    @Synchronized
    fun snapshot(): List<Stream> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            val rotating = e.phase as? Phase.Rotating
            Stream(
                name = name,
                phase = word(e.phase),
                phaseForSec = (System.currentTimeMillis() - e.sinceMs) / 1000,
                roster = rotating?.relays,
                tails = rotating?.tailed,
                queued = rotating?.queued,
                inFlight = e.inFlight?.invoke(),
                limits = e.limits?.invoke().orEmpty(),
                schedule = e.schedule?.invoke().orEmpty(),
            )
        }

    /** One line per stream, in registration order. */
    @Synchronized
    fun report(): List<String> =
        order.mapNotNull { name ->
            val e = phases[name] ?: return@mapNotNull null
            val elapsed = System.currentTimeMillis() - e.sinceMs
            "router: $name ${describe(e.phase, elapsed)}${stuck(e)}"
        }

    /** The oldest held leg, named, once it has been held past [STUCK_LEG_SECONDS]; empty below that. */
    private fun stuck(e: Entry): String {
        val oldest =
            e.inFlight
                ?.invoke()
                ?.relays
                ?.firstOrNull() ?: return ""
        if (oldest.heldForSec < STUCK_LEG_SECONDS) return ""
        return " — ${oldest.relay} held ${fmtDuration(oldest.heldForSec * 1000)}" +
            // Both counts: zero events alone reads as a dead socket on a reconciling leg.
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

            is Phase.Rotating -> {
                "rotating over ${phase.relays} relay(s), ${phase.tailed} live tail(s) ($elapsed elapsed)"
            }
        }
    }

    private fun word(phase: Phase): String =
        when (phase) {
            is Phase.Starting -> "starting"
            is Phase.Rotating -> "rotating"
        }

    companion object {
        /** Past this a held leg is stuck, not slow. */
        const val STUCK_LEG_SECONDS = 600L
    }
}
