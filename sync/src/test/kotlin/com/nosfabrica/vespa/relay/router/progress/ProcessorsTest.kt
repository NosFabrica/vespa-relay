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
        gate.measuring(10, Processors.UNIT_URL)

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
        fold.measuring(10, Processors.UNIT_HOST)
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
