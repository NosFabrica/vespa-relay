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

import com.nosfabrica.vespa.relay.util.fmtDuration
import java.util.concurrent.ConcurrentHashMap

/**
 * What each stream is doing right now, so an operator can tell a stream that
 * is working from one that never started.
 *
 * Three rules, each written against a way this went wrong before:
 *
 *  - **A phase in progress reports elapsed time**, including the phases that
 *    are not "progress". A stream that went idle forty-five minutes ago and one
 *    that finished a second ago used to print the identical line; silence and
 *    stillness are different states and only the clock tells them apart.
 *  - **A configured stream is never absent.** It appears on every tick even
 *    before it has done anything, so silence can never be read as "not
 *    configured".
 *  - **Counts do not name anybody**, so the per-relay truth lives beside the
 *    phase rather than inside it — see [namesInFlight] and [InFlight]. A stream
 *    held on two relays for eleven hours published the number 2 and no url.
 *
 * **There is one phase now, and that is the point.** This class used to carry a
 * fan-out's whole vocabulary — discovering, snapshotting, fetching, syncing,
 * holding, idle, failed — plus the passes each of those cycled through, because
 * a stream's engine was a pass over its relay list and a reader needed to know
 * which step it was on. Every stream rides the visit pool now: there is no walk
 * to be a phase OF, so a stream is [Phase.Rotating] from its first visit to the
 * end of the process and what moves is the numbers. What each relay is doing is
 * the in-flight list, which is the question the phase words were standing in
 * for all along.
 */
class StreamPhases {
    sealed interface Phase {
        /** Registered, not yet visiting — the only honest thing to say before the first roster. */
        data object Starting : Phase

        /**
         * Riding the visit pool. One long-lived phase whose numbers move: the
         * relays this stream is on, and how many of them hold a live tail.
         *
         * Zero is a REPORT and not an absence — a stream whose roster is empty
         * is one waiting on the fitness pass to certify its first relay, and a
         * stream that looks busy while dialling nothing is exactly the state
         * this pair exists to show.
         */
        data class Rotating(
            val relays: Int,
            val tailed: Int,
        ) : Phase
    }

    /**
     * One stream's state, flattened for a reader outside this process.
     *
     * A snapshot, not a view: [SyncProgress] serialises it on a timer while the
     * pool keeps moving, and handing out the live [Entry] would publish a
     * document whose members were read at different instants.
     */
    class Stream(
        val name: String,
        /** The phase's own word — never the rendered line. */
        val phase: String,
        /** How long it has been in that phase, in seconds. */
        val phaseForSec: Long,
        /** Relays this stream is riding, and of those how many are tailed. */
        val roster: Int? = null,
        val tails: Int? = null,
        /**
         * The relays this stream has workers on right now, quietest first.
         * Null for a stream nothing has registered a source for.
         */
        val inFlight: InFlight? = null,
    )

    private class Entry(
        @Volatile var phase: Phase,
        @Volatile var sinceMs: Long,
        @Volatile var inFlight: (() -> InFlight)? = null,
    )

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
     * Where to ask [name] which relays it has workers on.
     *
     * Registered once, by whoever owns the rotation, and INVOKED at snapshot
     * time — the whole class is a view flattened for a reader outside this
     * process, and a list read a tick earlier would date a stuck leg's clock
     * from the wrong instant.
     */
    @Synchronized
    fun namesInFlight(
        name: String,
        source: () -> InFlight,
    ) {
        register(name)
        phases[name]?.inFlight = source
    }

    /**
     * Move [name] to [phase].
     *
     * The elapsed clock restarts only when the phase CHANGES KIND — a
     * [Phase.Rotating] whose numbers moved is the same phase it was a second
     * ago, and resetting elapsed there would hide exactly the duration worth
     * seeing.
     */
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
                inFlight = e.inFlight?.invoke(),
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

    /**
     * ` — wss://slow.example held 11h 20m, 2 event(s), quiet 11h 19m`, or
     * nothing.
     *
     * Appended to whatever the phase says, because the phase cannot say it: a
     * stream is rotating whether its pool is turning over or wedged on one
     * relay, and the counts beside it name nobody. The url only ever reached
     * stderr for the ONE stream `SYNC_DIAGNOSE` points at, and container logs
     * here rotate inside the hour — so a leg that had been holding a slot since
     * the small hours left no trace at all by the time anyone looked.
     *
     * Silent below [STUCK_LEG_SECONDS]: every healthy rotation has legs in
     * flight, and a line on each is the log rather than a finding.
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

            is Phase.Rotating -> {
                "rotating over ${phase.relays} relay(s), ${phase.tailed} live tail(s) ($elapsed elapsed)"
            }
        }
    }

    /** The phase's own word, which is what the document publishes. */
    private fun word(phase: Phase): String =
        when (phase) {
            is Phase.Starting -> "starting"
            is Phase.Rotating -> "rotating"
        }

    companion object {
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
    }
}
