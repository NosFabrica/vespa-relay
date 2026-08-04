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
package com.nosfabrica.vespa.relay

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ServingPressureTest {
    private fun feed(
        p: ServingPressure,
        ms: Long,
        times: Int,
    ) = repeat(times) { p.record(ms) }

    @Test
    fun `a relay nobody queries mirrors at full speed`() {
        // No reads means no serving latency to protect. Throttling the mirror
        // here would be pure superstition.
        val p = ServingPressure()

        assertEquals(0, p.backoffMs())
        assertNull(p.describe())
    }

    @Test
    fun `healthy reads cost the mirror nothing`() {
        // ~400ms is a normal read against 52M documents on this deployment.
        val p = ServingPressure()

        feed(p, 400, 60)

        assertEquals(0, p.backoffMs(), "well under the threshold")
        assertNull(p.describe(), "nothing to report when nothing is yielding")
    }

    @Test
    fun `sustained slow reads make ingest yield`() {
        val p = ServingPressure(thresholdMs = 2_000)

        feed(p, 5_000, 60)

        assertTrue(p.backoffMs() > 0, "reads are slow, the mirror must give way")
        assertTrue(p.describe()!!.contains("yielding"))
    }

    @Test
    fun `an ordinary slow read is absorbed`() {
        // A cold cache or an expensive filter must not throttle the mirror. At
        // alpha = 1/8, one 3s read against a 200ms baseline moves the mean to
        // ~550ms — well under the threshold.
        val p = ServingPressure(thresholdMs = 2_000)
        feed(p, 200, 60)

        p.record(3_000)

        assertEquals(0, p.backoffMs(), "one ordinary outlier should not trip it")
    }

    @Test
    fun `a single catastrophic read does yield, and that is intended`() {
        // 30s is not variance — it is a client that has already given up. An
        // eighth of it clears any sane threshold on its own, and reacting is the
        // point: the mean decays back over the next few healthy reads, so the
        // mirror pauses briefly rather than being held down.
        val p = ServingPressure(thresholdMs = 2_000)
        feed(p, 200, 60)

        p.record(30_000)

        assertTrue(p.backoffMs() > 0, "a 30s read is evidence, not noise")

        feed(p, 200, 30)
        assertEquals(0, p.backoffMs(), "and it recovers once reads are healthy again")
    }

    @Test
    fun `a few samples are not yet evidence`() {
        // Below MIN_SAMPLES this is one client's first query, not a trend.
        val p = ServingPressure(thresholdMs = 2_000)

        feed(p, 20_000, ServingPressure.MIN_SAMPLES - 1)

        assertEquals(0, p.backoffMs())
    }

    @Test
    fun `the backoff is bounded however bad it gets`() {
        // Ingest yields; it does not stop. A pause long enough to look like a
        // hang would be a worse failure than the one it is avoiding.
        val p = ServingPressure(thresholdMs = 1_000, maxBackoffMs = 2_000)

        feed(p, 600_000, 200)

        assertEquals(2_000, p.backoffMs())
    }
}
