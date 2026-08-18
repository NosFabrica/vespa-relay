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

import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The report for the work that is not a stream.
 *
 * What is under test is that a row says only what its processor can honestly
 * answer: a counter-shaped job has no pass clock and must not publish one, and a
 * pass-shaped one must keep its clock moving even when the pass achieves
 * nothing — a fold that fails every six hours and a fold that stopped running
 * are different faults, and the timestamp is what separates them.
 */
class ProcessorsTest {
    @Test
    fun `a counter-shaped processor publishes gauges and no pass clock`() {
        val p = Processors()
        val queue = AtomicLong(12)
        p.of("ingest").let {
            it.phase(Processors.RUNNING)
            it.counts { listOf(Processors.Count("queued", queue.get()), Processors.Count("capacity", 20_000)) }
        }

        val row = p.snapshot().single()

        assertEquals("running", row.phase)
        assertNull(row.passes, "a job with no passes must not claim to have run any")
        assertNull(row.nextInSec, "…nor that anything is scheduled")
        assertEquals(listOf(12L, 20_000L), row.counts.map { it.value })

        // Read at snapshot time, not copied at registration: a gauge kept in
        // step by hand is the shape that produces a report disagreeing with the
        // thing it reports on.
        queue.set(19_999)
        assertEquals(
            19_999L,
            p
                .snapshot()
                .single()
                .counts
                .first()
                .value,
        )
    }

    @Test
    fun `a pass stamps its clock however it ended`() {
        val p = Processors()
        val fold = p.of("aliasFold")
        fold.begin()
        assertEquals(Processors.MEASURING, p.snapshot().single().phase)
        assertNull(p.snapshot().single().lastPassAt, "nothing has finished yet")

        fold.finish(nowMs = 100_000)

        val row = p.snapshot().single()
        assertEquals(Processors.IDLE, row.phase)
        assertEquals(100L, row.lastPassAt)
        assertEquals(1L, row.passes)
        assertTrue(row.lastPassSec != null, "how long it took is beside when it ended, because a pass here runs for minutes")
    }

    @Test
    fun `the countdown is asked live and never runs negative`() {
        // Overdue is the ordinary state at the moment a pass is about to start,
        // and "-40s" reads as a bug in the reader rather than as a queue.
        val p = Processors()
        val due = AtomicLong(0)
        p.of("aliasFold").nextPassAt { due.get() }

        due.set(60_000)
        assertEquals(30L, p.snapshot(nowMs = 30_000).single().nextInSec)
        due.set(10_000)
        assertEquals(0L, p.snapshot(nowMs = 30_000).single().nextInSec)
    }

    @Test
    fun `a pass in flight publishes where it has got to, and what that rate implies`() {
        // The state this exists for: a stability pass runs for hours, its
        // countdown is deliberately unset while it does, and every other number
        // on the row describes the pass BEFORE it. `measuring` is the only
        // member that moves, so it has to carry both halves of the position and
        // the estimate they imply.
        val p = Processors()
        val gate = p.of("consistency")
        gate.begin(nowMs = 1_000)
        // The walk's own clock starts here, not at `begin` — the pass spends
        // its first stretch reading stored verdicts, and that is not dialling.
        gate.measuring(10, Processors.UNIT_URL, nowMs = 1_000)

        assertNull(
            p
                .snapshot(nowMs = 6_000)
                .single()
                .measuring!!
                .etaSec,
            "nothing has landed, so there is no rate to extrapolate from",
        )

        gate.attempted()
        gate.attempted()

        val run = p.snapshot(nowMs = 11_000).single().measuring!!
        assertEquals(Processors.UNIT_URL, run.unit)
        assertEquals(2, run.attempted)
        assertEquals(10, run.toProbe)
        // Ten seconds for two urls is five seconds each, and eight are left.
        assertEquals(40L, run.etaSec)
        // …and the reading the estimate cannot give. Timed from the last unit
        // that ENDED, so it is zero the moment one does and climbs from there.
        assertEquals(0L, run.quietForSec)
    }

    @Test
    fun `a pass that has stopped is told from one about to finish, which the estimate cannot do`() {
        // The production shape: `attempted: 12,373 of 12,374, etaSec: 0` held
        // for 74 minutes on one wedged url. The estimate was CORRECT arithmetic
        // — one unit left at the rate so far rounds to nothing — and every
        // number on the row agreed with every other one. Nothing published said
        // the pass had stopped.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        // The production numbers, so the estimate is the one that was actually
        // published: 12,373 units in the first second of the pass leaves one,
        // and one unit at that rate rounds to no seconds at all.
        fitness.measuring(12_374, Processors.UNIT_URL, nowMs = 0)
        fitness.attempted(12_373, nowMs = 1_000)

        val stalled = p.snapshot(nowMs = 4_455_000).single().measuring!!
        assertEquals(0L, stalled.etaSec, "the estimate is still honest arithmetic, and still says nothing")
        assertEquals(4_454L, stalled.quietForSec, "…so this is the one member that separates the two readings")
    }

    @Test
    fun `a pass names the urls it is holding, longest first, and lets go of them with the pass`() {
        // The whole diagnostic gap this closes: the url that held a fitness
        // pass for 74 minutes was not nameable from the progress document, the
        // log, or a thread dump — a suspended coroutine has no frame. The pass
        // knew it the entire time.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        fitness.holding("wss://slow.example/", "ask ladder", nowMs = 1_000)
        fitness.holding("wss://quick.example/", "pre-probe", nowMs = 3_000)

        val held = p.snapshot(nowMs = 5_000).single().inFlight!!
        // Longest-held FIRST, which is the reverse of a stream's legs: there,
        // held is not risk; here every leg is bounded by a deadline, so a long
        // one is the anomaly and belongs at the top.
        assertEquals(listOf("wss://slow.example/", "wss://quick.example/"), held.relays.map { it.relay })
        assertEquals(4L, held.relays[0].heldForSec)
        assertEquals("ask ladder", held.relays[0].stage)
        assertEquals(0, held.omitted)

        // A later call MOVES THE STEP AND KEEPS THE CLOCK. A leg that restarted
        // its clock at every rung would report the last step's age as its own,
        // which is exactly the number being looked up.
        fitness.holding("wss://slow.example/", "neg-open", nowMs = 4_000)
        val moved =
            p
                .snapshot(nowMs = 5_000)
                .single()
                .inFlight!!
                .relays
                .first()
        assertEquals("neg-open", moved.stage)
        assertEquals(4L, moved.heldForSec)

        fitness.released("wss://slow.example/")
        assertEquals(
            listOf("wss://quick.example/"),
            p
                .snapshot(nowMs = 5_000)
                .single()
                .inFlight!!
                .relays
                .map { it.relay },
        )

        // Nothing outlives the pass. A row left standing under `idle` would be
        // a fault report about a leg that is not running.
        fitness.finish(nowMs = 6_000)
        assertNull(p.snapshot(nowMs = 7_000).single().inFlight)
    }

    @Test
    fun `a wide pass publishes every url it is holding, not a head of them`() {
        // A row here is a JOB, so the monitor's `dialConcurrency` already
        // bounds the set — which puts it on the far side of this repo's rule
        // that a cap is for a list the NETWORK can grow. It was cut to twenty
        // once, on the argument that most rows are ordinary dials a second old:
        // true of the ROWS and false of the LIST, since `omitted: 480` says
        // nothing about whether those 480 are healthy while the whole set
        // sorted by age is the distribution, and the distribution is what an
        // operator is reading the list for.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        for (i in 0 until 500) {
            fitness.holding("wss://r$i.example/", "ask ladder", nowMs = 1_000L + i)
        }

        val held = p.snapshot(nowMs = 2_000).single().inFlight!!
        assertEquals(500, held.relays.size)
        assertEquals(0, held.omitted, "the member stays as the schema's promise; nothing is being dropped")
        // …and the order is still oldest first, so the wedged leg is at the top
        // for whatever the page chooses to draw.
        assertEquals("wss://r0.example/", held.relays.first().relay)
    }

    @Test
    fun `a position belongs to the pass that had it`() {
        // Both directions of the same rule. A stale position under `idle` reads
        // as a pass that stopped halfway, which is a fault report rather than a
        // measurement; the previous pass's `10 of 10` under a fresh `measuring`
        // reads as one that finished instantly. Neither may survive its pass.
        val p = Processors()
        val fold = p.of("aliasFold")
        fold.begin(nowMs = 1_000)
        fold.measuring(10, Processors.UNIT_HOST, nowMs = 1_000)
        fold.attempted(10)
        fold.finish(nowMs = 11_000)

        assertNull(p.snapshot(nowMs = 12_000).single().measuring, "no pass is running, so there is no position")

        fold.begin(nowMs = 20_000)
        assertNull(
            p.snapshot(nowMs = 20_001).single().measuring,
            "a pass derives its set some way in — until it does, the last pass's position is not this one's",
        )
    }

    @Test
    fun `a pass that brackets itself inside another bracket is one pass, not two`() {
        // The fitness pass brackets its own `measure` — it must, because the
        // fast lane calls it outside the monitor's loop — and the sweep
        // brackets every pass it runs. Both finishes landed, so the row
        // reported two `passesRun` per sweep and the clock came from the loop
        // rather than from the measure.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 1_000) // the monitor's bracket
        fitness.begin("measuring fitness", nowMs = 2_000) // the pass's own
        fitness.finish(nowMs = 8_000) // …which ends it
        fitness.finish(nowMs = 8_100) // and the monitor's, arriving after

        val row = p.snapshot(nowMs = 9_000).single()
        assertEquals(1L, row.passes, "one pass ran")
        assertEquals(6L, row.lastPassSec, "and it is timed from the measure, not from the loop around it")
    }

    @Test
    fun `a pass that reports no position is silent rather than empty`() {
        // Three of the router's processors have no set to walk at all — ingest,
        // the healer, the push — and `0 of 0` from them would be a measurement
        // they never took, drawn on the card as a pass that has nothing to do.
        val p = Processors()
        p.of("ingest").phase(Processors.RUNNING)

        assertNull(p.snapshot().single().measuring)
    }

    @Test
    fun `work is kept per stream, replacing rather than accumulating`() {
        // A stream re-submits its whole candidate set every cycle, so appending
        // would publish the same stream once per pass forever.
        val p = Processors()
        val fold = p.of("aliasFold")
        fold.record(Processors.Work(stream = "content", candidates = 100, unmeasured = 90, dialled = 10, decided = 5))
        fold.record(Processors.Work(stream = "content", candidates = 100, unmeasured = 80, dialled = 10, decided = 6))
        fold.record(Processors.Work(stream = "assertions", candidates = 4, unmeasured = 0, dialled = 4, decided = 4))

        val work = p.snapshot().single().work

        assertEquals(listOf("assertions", "content"), work.map { it.stream }, "ordered, so two rollups of one state agree")
        assertEquals(80, work.last().unmeasured, "the latest pass over that stream, not the first")
    }

    @Test
    fun `asking for a handle twice hands back the same row`() {
        // The fold is constructed with its handle and driven by the monitor,
        // which asks for it by name — two objects reporting into one row.
        val p = Processors()
        p.of("aliasFold").phase(Processors.MEASURING)
        p.of("aliasFold").record(Processors.Work(stream = "content", candidates = 2, unmeasured = 1, dialled = 1, decided = 1))

        val rows = p.snapshot()

        assertEquals(1, rows.size)
        assertEquals(Processors.MEASURING, rows.single().phase)
        assertEquals(1, rows.single().work.size)
    }

    @Test
    fun `the phase clock restarts on the WORD, not on every tick`() {
        // Same rule as `StreamPhases.set`: a processor re-stating its phase
        // every tick would report an elapsed time of zero forever, and elapsed
        // time is the one number worth having on a row that is otherwise static.
        val p = Processors()
        val ingest = p.of("ingest")
        ingest.phase(Processors.RUNNING)
        val first = p.snapshot().single().phaseForSec
        ingest.phase(Processors.RUNNING)

        assertTrue(p.snapshot().single().phaseForSec >= first)
    }
}
