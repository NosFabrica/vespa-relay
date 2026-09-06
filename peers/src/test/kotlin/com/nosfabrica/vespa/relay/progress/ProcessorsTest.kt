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
 * Rows for the work that is not a stream. A row says only what its processor can answer:
 * a counter job has no pass clock, and a pass job's clock moves even on an empty pass.
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

        // Read at snapshot time, not copied at registration.
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
        // Overdue is the ordinary state as a pass is about to start, and "-40s" reads as a bug in the reader.
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
        // `measuring` is the only member that moves during a pass; the rest describe the pass before it.
        val p = Processors()
        val gate = p.of("consistency")
        gate.begin(nowMs = 1_000)
        // The walk's clock starts here, not at `begin`: reading stored verdicts is not dialling.
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
        // Timed from the last unit that ended, so zero the moment one does.
        assertEquals(0L, run.quietForSec)
    }

    @Test
    fun `a pass that has stopped is told from one about to finish, which the estimate cannot do`() {
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        // One unit left at the rate so far rounds to no seconds at all, however long it has been stuck.
        fitness.measuring(12_374, Processors.UNIT_URL, nowMs = 0)
        fitness.attempted(12_373, nowMs = 1_000)

        val stalled = p.snapshot(nowMs = 4_455_000).single().measuring!!
        assertEquals(0L, stalled.etaSec, "the estimate is still honest arithmetic, and still says nothing")
        assertEquals(4_454L, stalled.quietForSec, "…so this is the one member that separates the two readings")
    }

    @Test
    fun `a pass names the urls it is holding, longest first, and lets go of them with the pass`() {
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        fitness.holding("wss://slow.example/", "ask ladder", nowMs = 1_000)
        fitness.holding("wss://quick.example/", "pre-probe", nowMs = 3_000)

        val held = p.snapshot(nowMs = 5_000).single().inFlight!!
        // Longest-held first: every leg here has a deadline, so a long one is the anomaly.
        assertEquals(listOf("wss://slow.example/", "wss://quick.example/"), held.relays.map { it.relay })
        assertEquals(4L, held.relays[0].heldForSec)
        assertEquals("ask ladder", held.relays[0].stage)
        assertEquals(0, held.omitted)

        // A later call moves the step and keeps the clock; the age being looked up is the leg's, not the step's.
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

        // Nothing outlives the pass: a row under `idle` would be a fault report about a leg that is not running.
        fitness.finish(nowMs = 6_000)
        assertNull(p.snapshot(nowMs = 7_000).single().inFlight)
    }

    @Test
    fun `a wide pass publishes every url it is holding, not a head of them`() {
        // The monitor's `dialConcurrency` already bounds this list, so it is not one the network can grow.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 0)
        for (i in 0 until 500) {
            fitness.holding("wss://r$i.example/", "ask ladder", nowMs = 1_000L + i)
        }

        val held = p.snapshot(nowMs = 2_000).single().inFlight!!
        assertEquals(500, held.relays.size)
        assertEquals(0, held.omitted, "the member stays as the schema's promise; nothing is being dropped")
        assertEquals("wss://r0.example/", held.relays.first().relay)
    }

    @Test
    fun `a position belongs to the pass that had it`() {
        // A stale position reads as a pass stopped halfway, or as one that finished instantly.
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
        // The fast lane calls `measure` outside the monitor's loop, so the pass brackets itself too.
        val p = Processors()
        val fitness = p.of("fitness")
        fitness.begin(nowMs = 1_000) // the monitor's bracket
        fitness.begin("measuring fitness", nowMs = 2_000) // the pass's own
        fitness.finish(nowMs = 8_000) // ends the pass
        fitness.finish(nowMs = 8_100) // the monitor's, arriving after

        val row = p.snapshot(nowMs = 9_000).single()
        assertEquals(1L, row.passes, "one pass ran")
        assertEquals(6L, row.lastPassSec, "and it is timed from the measure, not from the loop around it")
    }

    @Test
    fun `a pass that reports no position is silent rather than empty`() {
        // Ingest, the healer and the push walk no set; `0 of 0` from them would be a measurement they never took.
        val p = Processors()
        p.of("ingest").phase(Processors.RUNNING)

        assertNull(p.snapshot().single().measuring)
    }

    @Test
    fun `work is kept per stream, replacing rather than accumulating`() {
        // A stream re-submits its whole candidate set every cycle.
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
        // The fold is constructed with its handle and the monitor asks for it by name: two objects, one row.
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
        // Same rule as `StreamPhases.set`: re-stating the phase every tick would report zero elapsed forever.
        val p = Processors()
        val ingest = p.of("ingest")
        ingest.phase(Processors.RUNNING)
        val first = p.snapshot().single().phaseForSec
        ingest.phase(Processors.RUNNING)

        assertTrue(p.snapshot().single().phaseForSec >= first)
    }
}
