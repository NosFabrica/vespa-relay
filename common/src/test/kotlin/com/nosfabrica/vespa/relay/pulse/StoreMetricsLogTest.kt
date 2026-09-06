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
package com.nosfabrica.vespa.relay.pulse

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.HeavyHitters
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The log line carries the pulse's OPERATIONAL numbers and none of the
 * material the pulse is gated for. A log is not gated: anything that can read
 * container logs would read whatever this prints.
 */
class StoreMetricsLogTest {
    private fun snapshotWithSecrets(): CostLedger.Snapshot =
        CostLedger.Snapshot(
            ports = emptyList(),
            outcomes = emptyMap(),
            engine = emptyList(),
            gauges = mapOf("trust.pending.subjects" to 139_524L),
            topObservers = listOf(HeavyHitters.Hit("cafebabecafebabe", 9, 0)),
            topTerms = listOf(HeavyHitters.Hit("someone's private search", 7, 0)),
            slowReads = emptyList(),
        )

    /**
     * THE REASON THE PAGE IS GATED. `topTerms` quotes what people searched
     * for and `topObservers` names who searched; a line that spliced either
     * into stdout would publish them to every log reader, which is a wider
     * audience than the admin gate ever allowed.
     */
    @Test
    fun `the line never carries search terms or observer keys`() {
        val line = StoreMetricsLog.line("relay", snapshotWithSecrets(), stages = emptyMap(), held = emptyList())
        assertFalse(line.contains("someone's private search"), "a search term reached the log: $line")
        assertFalse(line.contains("cafebabecafebabe"), "an observer key reached the log: $line")
    }

    /** And it does carry the numbers it exists for, or it is not worth logging. */
    @Test
    fun `the line carries gauges, stages and what holds the lock`() {
        val line =
            StoreMetricsLog.line(
                "relay",
                snapshotWithSecrets(),
                stages = mapOf("proj.fetch.derive" to IngestStats.Stage(totalNanos = 9_569_653_000_000, calls = 665, maxNanos = 66_420_191_000)),
                held = listOf(IngestStats.Held("lock.gate.hold", System.nanoTime() - 10_000_000_000, "derive 500 subject(s) in 10 chunk(s)", "trustGate")),
            )
        assertContains(line, "trust.pending.subjects=139524")
        assertContains(line, "proj.fetch.derive")
        assertContains(line, "lock.gate.hold")
        assertTrue(line.contains("derive 500 subject(s)"), "the hold's DETAIL is the useful half — it names the work, not just the mutex: $line")
        assertTrue(line.startsWith("store-metrics[relay]"), "the role is what attributes load between the relay and the mirror")
    }
}
