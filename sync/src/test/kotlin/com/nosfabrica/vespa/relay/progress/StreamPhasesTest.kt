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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The output an operator reads to decide whether to wait or intervene. The two
 * failures worth testing are the ones seen in production: a stream that is
 * working but reports nothing, and a stream that is absent from the report
 * entirely — which reads as "not configured" rather than "not started".
 */
class StreamPhasesTest {
    private fun rotating(
        relays: Int,
        tailed: Int,
        queued: Int = 0,
    ) = StreamPhases.Phase.Rotating(relays, tailed, queued)

    @Test
    fun `a registered stream appears before it has done anything`() {
        val p = StreamPhases()
        p.register("assertions")
        p.register("dataViaOutbox")

        val report = p.report()
        assertEquals(2, report.size, "a configured stream must never be missing from the report")
        assertTrue(report.all { it.startsWith("router: ") })
        assertTrue(report.all { it.contains("starting") }, "and says so rather than looking idle: $report")
    }

    @Test
    fun `registration order is stable, so the report reads the same each tick`() {
        val p = StreamPhases()
        p.register("first")
        p.register("second")
        p.set("second", rotating(4, 2))
        p.set("first", rotating(9, 9))

        assertTrue(p.report()[0].contains("first"))
        assertTrue(p.report()[1].contains("second"))
    }

    @Test
    fun `the rotation reports both of its numbers, and zero is one of them`() {
        // A stream riding four hundred relays and one riding none both read
        // `rotating for 58m` before this pair was published — and the second is
        // the state worth seeing, a stream that looks busy and dials nothing.
        val p = StreamPhases()
        p.set("content", rotating(412, 128))
        assertTrue(p.report().single().contains("412 relay(s)"))
        assertTrue(p.report().single().contains("128 live tail(s)"))

        p.set("content", rotating(0, 0))
        val empty = p.snapshot().single()
        assertEquals(0, empty.roster, "zero is a report, not an absence")
        assertEquals(0, empty.tails)
    }

    @Test
    fun `the elapsed clock survives progress within one phase`() {
        // The rotation is one phase for the life of the process and its numbers
        // move constantly; restarting the clock on each would hide the only
        // duration worth seeing.
        val p = StreamPhases()
        p.set("s", rotating(10, 1))
        Thread.sleep(1100)
        p.set("s", rotating(11, 2))

        assertTrue(p.snapshot().single().phaseForSec >= 1, "the clock is the phase's, not the last update's")
    }

    @Test
    fun `changing phase restarts the clock`() {
        val p = StreamPhases()
        p.register("s")
        Thread.sleep(1100)
        p.set("s", rotating(3, 0))

        assertEquals(0L, p.snapshot().single().phaseForSec, "a new phase starts its own clock")
    }

    @Test
    fun `the snapshot names phases in words a published series can keep`() {
        val p = StreamPhases()
        p.register("s")
        assertEquals("starting", p.snapshot().single().phase)
        p.set("s", rotating(1, 1))
        assertEquals("rotating", p.snapshot().single().phase)
    }

    @Test
    fun `a stuck leg is named on every line until it lets go`() {
        // The logs here rotate inside the hour and the diagnostic line only ever
        // fires for the one stream SYNC_DIAGNOSE points at, so a leg holding a
        // slot since the small hours left nothing at all to find. The phase
        // cannot say it — a stream is rotating whether its pool is turning over
        // or wedged on one relay — so it is appended to whatever the phase says.
        val p = StreamPhases()
        p.set("content", rotating(5, 5))
        p.names("content", inFlight = {
            InFlight(listOf(InFlight.Relay("wss://slow.example/", heldForSec = 41_400, transferringForSec = 41_390, events = 2, quietForSec = 41_000)), 0)
        })

        val line = p.report().single()

        assertTrue(line.contains("wss://slow.example/"), "the url is the whole point: $line")
        assertTrue(line.contains("2 event(s)"), "and whether it is still delivering: $line")
        assertTrue(line.contains("quiet"), "which is what separates a backlog from a wedge: $line")
    }

    @Test
    fun `an ordinary leg in flight says nothing`() {
        // Every healthy rotation has relays in flight. A line on each is the log
        // rather than a finding, which is how a warning stops being read.
        val p = StreamPhases()
        p.set("content", rotating(5, 4))
        p.names("content", inFlight = {
            InFlight(listOf(InFlight.Relay("wss://busy.example/", heldForSec = 12, transferringForSec = 10, events = 900, quietForSec = 0)), 0)
        })

        assertFalse(p.report().single().contains("wss://busy.example/"))
    }

    @Test
    fun `a leg with no socket says so, because absent is not zero`() {
        val p = StreamPhases()
        p.set("content", rotating(5, 1))
        p.names("content", inFlight = {
            InFlight(listOf(InFlight.Relay("wss://queued.example/", heldForSec = 40_000, transferringForSec = null, events = 0, quietForSec = 40_000)), 0)
        })

        assertTrue(p.report().single().contains("not on a socket"))
    }
}
