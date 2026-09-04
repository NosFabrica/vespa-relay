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
package com.nosfabrica.vespa.relay.ingest

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The gate decides whether a probe's round trip still earns its cost: it does not
 * judge without evidence, does not latch off, and tracks the present, not the past.
 */
class ProbeGateTest {
    private fun feed(
        gate: ProbeGate,
        events: Int,
        hitRate: Double,
        batch: Int = 1000,
    ) {
        repeat(events / batch) { gate.record(batch, (batch * hitRate).toInt()) }
    }

    @Test
    fun `probes while it still has nothing to go on`() {
        val gate = ProbeGate(minHitRate = 0.20)
        // The first batches are the measurement.
        repeat(20) { assertTrue(gate.worthIt(), "gave up before it had evidence") }
        feed(gate, events = 10_000, hitRate = 0.0)
        assertTrue(gate.worthIt(), "10k events is one moment, not a verdict")
    }

    @Test
    fun `keeps probing a stream that keeps paying`() {
        val gate = ProbeGate(minHitRate = 0.20)
        feed(gate, events = 200_000, hitRate = 0.94)
        repeat(50) { assertTrue(gate.worthIt(), "switched off a probe dropping 94%") }
    }

    @Test
    fun `stops probing a converged stream, but never stops sampling it`() {
        val gate = ProbeGate(minHitRate = 0.20)
        feed(gate, events = 200_000, hitRate = 0.01)

        val probed = (1..320).count { gate.worthIt() }
        // One batch in 32 keeps the door open for a stream that starts producing duplicates again.
        assertEquals(10, probed, "expected one sampled batch in 32, got $probed of 320")
    }

    @Test
    fun `a backfill's hit rate does not keep the probe on for the rest of the process`() {
        val gate = ProbeGate(minHitRate = 0.20)
        // Days of catch-up, then convergence; without decay the backfill's
        // counters would outvote the present indefinitely.
        feed(gate, events = 2_000_000, hitRate = 0.94)
        assertTrue(gate.worthIt())
        feed(gate, events = 4_000_000, hitRate = 0.0)
        val probed = (1..320).count { gate.worthIt() }
        assertTrue(probed <= 20, "still probing $probed batches in 320 after the stream went quiet")
    }
}
