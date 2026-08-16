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

import com.nosfabrica.vespa.relay.util.fmtCount
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The output an operator reads to decide whether to wait or intervene. The two
 * failures worth testing are the ones seen in production: a stream that is
 * working but reports nothing, and a stream that is absent from the report
 * entirely — which reads as "not configured" rather than "not started".
 */
class StreamPhasesTest {
    @Test
    fun `a registered stream appears before it has done anything`() {
        val p = StreamPhases()
        p.register("assertions")
        p.register("dataViaOutbox")

        val report = p.report()
        assertEquals(2, report.size, "a configured stream must never be missing from the report")
        assertTrue(report.all { it.startsWith("router: ") })
    }

    @Test
    fun `registration order is stable, so the report reads the same each tick`() {
        val p = StreamPhases()
        p.register("first")
        p.register("second")
        p.set("second", StreamPhases.Phase.Idle(0, 60))
        p.set("first", StreamPhases.Phase.Idle(0, 60))

        assertTrue(p.report()[0].contains("first"))
        assertTrue(p.report()[1].contains("second"))
    }

    @Test
    fun `snapshotting shows the count, the total and a percentage`() {
        val p = StreamPhases()
        p.set("assertions", StreamPhases.Phase.Snapshotting(4_200_000, 14_900_000, 19))

        val line = p.report().single()
        assertTrue(line.contains("4.2M/14.9M"), "got: $line")
        assertTrue(line.contains("(28%)"), "got: $line")
        assertTrue(line.contains("19 relay(s)"), "got: $line")
        assertTrue(line.contains("elapsed"), "a phase in progress must say how long it has been running")
    }

    @Test
    fun `an uncountable filter reports progress without inventing a denominator`() {
        val p = StreamPhases()
        p.set("assertions", StreamPhases.Phase.Snapshotting(4_200_000, null, 19))

        val line = p.report().single()
        assertTrue(line.contains("4.2M"), "got: $line")
        assertTrue(!line.contains("%"), "an unknown total must not become a fake percentage: $line")
    }

    @Test
    fun `the elapsed clock survives progress within one phase`() {
        // Snapshotting reports a new count every page. Restarting the clock there
        // would reset the one number that says how long the walk has taken —
        // exactly what makes a slow phase indistinguishable from a stuck one.
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Snapshotting(1_000, 10_000, 2))
        Thread.sleep(1100)
        p.set("s", StreamPhases.Phase.Snapshotting(2_000, 10_000, 2))

        assertTrue(p.report().single().contains("(0:01 elapsed)"), "got: ${p.report().single()}")
    }

    @Test
    fun `changing phase restarts the clock`() {
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Snapshotting(1_000, 10_000, 2))
        Thread.sleep(1100)
        p.set("s", StreamPhases.Phase.Syncing(0, 19, 0, 0, 0))

        assertTrue(p.report().single().contains("(0:00 elapsed)"), "got: ${p.report().single()}")
    }

    @Test
    fun `every phase says something an operator can act on`() {
        val p = StreamPhases()
        listOf(
            StreamPhases.Phase.Waiting("kinds 10040 x2 select(s)", 30) to "waiting",
            StreamPhases.Phase.Discovering("kinds 10002 x1 select(s)") to "discovering",
            StreamPhases.Phase.Syncing(6, 19, 1204, 3, 12) to "syncing 6/19",
            StreamPhases.Phase.Idle(1204, 21600) to "idle",
            StreamPhases.Phase.Failed("connection reset", 30) to "failed",
        ).forEach { (phase, expect) ->
            p.set("s", phase)
            assertTrue(p.report().single().contains(expect), "got: ${p.report().single()}")
        }
    }

    @Test
    fun `a stream waiting its turn says so rather than looking idle`() {
        // Streams reconcile one at a time, because each holds its whole local id
        // set for its entire cycle and two resident at once is what reached the
        // heap ceiling. A stream waiting for the gate has discovered its relays
        // and is doing nothing — indistinguishable from stuck without this.
        val p = StreamPhases()
        p.set("assertions", StreamPhases.Phase.Queued(16507))

        val line = p.report().single()
        assertTrue(line.contains("queued behind another stream"), "got: $line")
        assertTrue(line.contains("16507 relay(s) ready"), "got: $line")
        assertTrue(line.contains("elapsed"), "waiting is a phase too, and its duration is the point")
    }

    @Test
    fun `syncing reports what was not dialled and what failed, only when there are any`() {
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Syncing(6, 19, 1204, 0, 0))
        assertTrue(!p.report().single().contains("not dialled"), "zero should not be noise")

        p.set("s", StreamPhases.Phase.Syncing(6, 19, 1204, 19104, 412))
        val line = p.report().single()
        // "skipped as dead" said nothing about what dead MEANT or when it is
        // retried, and it was the phrase an operator quoted back when asking.
        assertTrue(line.contains("19104 not dialled (struck out, no route, or no transport)"), "got: $line")
        assertTrue(line.contains("412 dialled and failed"), "got: $line")
    }

    @Test
    fun `the count of returned legs says it is legs returning, not work finished`() {
        // The single most misread number this router publishes: it includes
        // every leg that came back unreachable, capped or out of budget.
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Fetching(16747, 16752, 900))

        val line = p.report().single()
        assertTrue(line.contains("16747/16752 relay(s) returned"), "got: $line")
        assertTrue(line.contains("event(s) received"), "an event count has to say what it counted: $line")
    }

    @Test
    fun `idle and failed carry an elapsed clock, because stillness and silence differ`() {
        // A 45-minute ingest drain behind a finished cycle printed the same line
        // as a cycle that ended a second ago.
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Idle(12, 60))
        assertTrue(p.report().single().contains("ago"), "got: ${p.report().single()}")

        p.set("t", StreamPhases.Phase.Failed("connection reset", 30))
        assertTrue(p.report().last().contains("ago"), "got: ${p.report().last()}")
    }

    @Test
    fun `a cycle's outcome survives its phase, so failed and finished stop reading alike`() {
        val p = StreamPhases()
        val tally = CycleTally(discovered = 10, foldedOntoAnother = 2, hosts = 5)
        p.beginCycle("s", StreamPhases.DYNAMIC, 1, tally, nowSeconds = 1_000)

        assertEquals(
            "running",
            p
                .snapshot()
                .single()
                .newest
                ?.outcome,
        )
        assertNull(
            p
                .snapshot()
                .single()
                .newest
                ?.endedSec,
            "a running cycle has not ended",
        )

        p.endCycle("s", StreamPhases.DYNAMIC, "failed", nowSeconds = 1_100)
        assertEquals(
            "failed",
            p
                .snapshot()
                .single()
                .newest
                ?.outcome,
        )
        assertEquals(
            1_100L,
            p
                .snapshot()
                .single()
                .newest
                ?.endedSec,
        )

        // A stream that fails during DISCOVERY has no cycle running, and
        // stamping an end on the finished one would re-date it every retry.
        p.endCycle("s", StreamPhases.DYNAMIC, "completed", nowSeconds = 1_200)
        assertEquals(
            "failed",
            p
                .snapshot()
                .single()
                .newest
                ?.outcome,
        )
        assertEquals(
            1_100L,
            p
                .snapshot()
                .single()
                .newest
                ?.endedSec,
        )
    }

    @Test
    fun `a pass that has finished handing out keeps being published while its legs run`() {
        // THE STATE THE SINGLE SLOT COULD NOT SHOW. A walk ends when its last
        // url is handed out, not when its last worker returns, so the next pass
        // routinely opens while the previous one is still downloading — and the
        // moment it did, the older pass stopped being published: its counters
        // went on moving against a partition nobody could see, and its live legs
        // surfaced only as the new pass's `busy`.
        val p = StreamPhases()
        val first = CycleTally(discovered = 10, foldedOntoAnother = 0, hosts = 10)
        first.delivered.set(6)
        p.beginCycle("s", StreamPhases.DYNAMIC, 11, first, nowSeconds = 1_000)
        p.endCycle("s", StreamPhases.DYNAMIC, "completed", nowSeconds = 1_050)

        val second = CycleTally(discovered = 10, foldedOntoAnother = 0, hosts = 10)
        p.beginCycle("s", StreamPhases.DYNAMIC, 12, second, nowSeconds = 1_060)

        val cycles = p.snapshot().single().cycles
        assertEquals(listOf(11L, 12L), cycles.map { it.number }, "both walks, oldest first")
        assertEquals(4L, cycles.first().tally.pending(), "the old walk's legs are still out")
        assertEquals(
            12L,
            p
                .snapshot()
                .single()
                .newest
                ?.number,
            "and `cycle` is still the current one",
        )

        // …and it retires itself the moment its last worker lands, rather than
        // on a timer: the next `beginCycle` drops it.
        first.nothingNew.set(4)
        p.beginCycle("s", StreamPhases.DYNAMIC, 13, CycleTally(discovered = 1, foldedOntoAnother = 0, hosts = 1), nowSeconds = 1_120)
        assertEquals(
            listOf(12L, 13L),
            p
                .snapshot()
                .single()
                .cycles
                .map { it.number },
        )
    }

    @Test
    fun `a pass whose legs never return cannot grow the list without limit`() {
        // The retirement rule is not enough on its own: a leg that never comes
        // back keeps `pending` above zero forever, so a stream on a short
        // recycle would accumulate one row per pass for the life of the process.
        val p = StreamPhases()
        repeat(StreamPhases.MAX_TRACKED_CYCLES + 3) { i ->
            p.beginCycle("s", StreamPhases.DYNAMIC, i + 1L, CycleTally(discovered = 5, foldedOntoAnother = 0, hosts = 5), nowSeconds = 1_000L + i)
        }

        val cycles = p.snapshot().single().cycles
        assertEquals(StreamPhases.MAX_TRACKED_CYCLES, cycles.size)
        assertEquals(7L, cycles.last().number, "the newest is kept")
        assertEquals(4L, cycles.first().number, "and the oldest is what goes")
    }

    @Test
    fun `one stream name, two owners, each half's pass is its own, and neither closes the other`() {
        // A stream can carry BOTH `urls` and `relaySource`, so StaticBackfill and
        // DynamicSync open a cycle under the same name, at boot, at once. With
        // one slot that had to be arbitrated — first writer wins, the loser
        // published nothing at all — and with a list it does not: they are two
        // passes with two partitions, and `owner` says which is which.
        val p = StreamPhases()
        val dynamic = CycleTally(discovered = 300, foldedOntoAnother = 0, hosts = 300)
        val static = CycleTally(discovered = 2, foldedOntoAnother = 0, hosts = 2)

        p.beginCycle("both", StreamPhases.DYNAMIC, 1, dynamic, nowSeconds = 1_000)
        p.beginCycle("both", StreamPhases.STATIC, 1, static, nowSeconds = 1_010)
        assertEquals(
            listOf(300 to StreamPhases.DYNAMIC, 2 to StreamPhases.STATIC),
            p
                .snapshot()
                .single()
                .cycles
                .map { it.tally.discovered to it.owner },
            "neither half's work is dropped for the other's",
        )

        // …and one half finishing does not close the other's.
        p.endCycle("both", StreamPhases.STATIC, "completed", nowSeconds = 1_020)
        val afterStatic = p.snapshot().single().cycles
        assertEquals("completed", afterStatic.single { it.owner == StreamPhases.STATIC }.outcome)
        assertEquals("running", afterStatic.single { it.owner == StreamPhases.DYNAMIC }.outcome)
        assertNull(afterStatic.single { it.owner == StreamPhases.DYNAMIC }.endedSec)

        p.endCycle("both", StreamPhases.DYNAMIC, "completed", nowSeconds = 1_030)
        assertEquals(0, p.snapshot().single().running(), "nothing is handing out now")
    }

    @Test
    fun `the snapshot names phases in words a published series can keep`() {
        val p = StreamPhases()
        p.set("s", StreamPhases.Phase.Fetching(1, 2, 3))
        assertEquals("fetching", p.snapshot().single().phase)
        p.set("s", StreamPhases.Phase.Idle(3, null))
        assertEquals("idle", p.snapshot().single().phase)
    }

    @Test
    fun `a stuck leg is named on every line until it lets go`() {
        // The logs here rotate inside the hour and the diagnostic line only ever
        // fires for the one stream SYNC_DIAGNOSE points at, so a leg holding a
        // slot since the small hours left nothing at all to find. The phase
        // cannot say it — a stream is `fetching` whether its pool is turning
        // over or wedged on one relay — so it is appended to whatever the phase
        // says.
        val p = StreamPhases()
        p.set("content", StreamPhases.Phase.Fetching(done = 5, total = 5, events = 2))
        p.namesInFlight("content") {
            InFlight(listOf(InFlight.Relay("wss://slow.example/", heldForSec = 41_400, transferringForSec = 41_390, events = 2, quietForSec = 41_000)), 0)
        }

        val line = p.report().single()

        assertTrue(line.contains("wss://slow.example/"), "the url is the whole point: $line")
        assertTrue(line.contains("2 event(s)"), "and whether it is still delivering: $line")
        assertTrue(line.contains("quiet"), "which is what separates a backlog from a wedge: $line")
    }

    @Test
    fun `an ordinary leg in flight says nothing`() {
        // Every healthy pass has relays in flight. A line on each is the log
        // rather than a finding, which is how a warning stops being read.
        val p = StreamPhases()
        p.set("content", StreamPhases.Phase.Fetching(done = 5, total = 9, events = 2))
        p.namesInFlight("content") {
            InFlight(listOf(InFlight.Relay("wss://busy.example/", heldForSec = 12, transferringForSec = 10, events = 900, quietForSec = 0)), 0)
        }

        assertFalse(p.report().single().contains("wss://busy.example/"))
    }

    @Test
    fun `holding is its own phase, because it is not idle`() {
        // Idle is "nothing to do until the timer"; holding is "the timer fired
        // and we are declining". Read as one, a mirror waiting out its interval
        // and one whose pool never frees up are the same line.
        val p = StreamPhases()
        p.set(
            "content",
            StreamPhases.Phase.Holding(
                free = 2,
                needed = 4,
                running = 130,
                oldest = InFlight.Relay("wss://slow.example/", heldForSec = 41_400, transferringForSec = 41_390, events = 2, quietForSec = 41_000),
            ),
        )

        assertEquals("holding", p.snapshot().single().phase)
        val line = p.report().single()
        assertTrue(line.contains("2/4 transfer slot(s) free"), "what it is waiting FOR: $line")
        assertTrue(line.contains("wss://slow.example/"), "and what it is waiting ON: $line")
    }

    @Test
    fun `counts are abbreviated to the magnitude that matters`() {
        assertEquals("24.8M", fmtCount(24_819_118))
        assertEquals("15k", fmtCount(15_000))
        assertEquals("412", fmtCount(412))
    }
}
