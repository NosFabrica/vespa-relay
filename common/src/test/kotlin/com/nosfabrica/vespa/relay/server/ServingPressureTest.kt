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
package com.nosfabrica.vespa.relay.server

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
        val p = ServingPressure()

        assertEquals(0, p.backoffMs())
        assertNull(p.describe())
    }

    @Test
    fun `healthy reads cost the mirror nothing`() {
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
        // At alpha = 1/8, one 3s read over a 200ms baseline moves the mean to about 550ms.
        val p = ServingPressure(thresholdMs = 2_000)
        feed(p, 200, 60)

        p.record(3_000)

        assertEquals(0, p.backoffMs(), "one ordinary outlier should not trip it")
    }

    @Test
    fun `a single catastrophic read does yield, and that is intended`() {
        // An eighth of 30s clears any sane threshold; the mean then decays over the next healthy reads.
        val p = ServingPressure(thresholdMs = 2_000)
        feed(p, 200, 60)

        p.record(30_000)

        assertTrue(p.backoffMs() > 0, "a 30s read is evidence, not noise")

        feed(p, 200, 30)
        assertEquals(0, p.backoffMs(), "and it recovers once reads are healthy again")
    }

    @Test
    fun `a straggler after a run of instant reads is dampened, not adopted`() {
        // A zero mean that read as "no samples yet" would let the next straggler be adopted wholesale.
        val p = ServingPressure(thresholdMs = 2_000)
        feed(p, 0, 100)

        p.record(30_000)

        assertTrue(
            p.meanMs() <= 30_000 / 8 + 1,
            "the EWMA damping must still apply after instant reads; mean was ${p.meanMs()}ms",
        )
    }

    @Test
    fun `a few samples are not yet evidence`() {
        val p = ServingPressure(thresholdMs = 2_000)

        feed(p, 20_000, ServingPressure.MIN_SAMPLES - 1)

        assertEquals(0, p.backoffMs())
    }

    @Test
    fun `an adopted mean replaces rather than smooths`() {
        // The EWMA already happened on the relay side.
        val p = ServingPressure(thresholdMs = 2_000)

        p.adopt(5_000, ServingPressure.MIN_SAMPLES.toLong())

        assertEquals(5_000, p.meanMs())
        assertTrue(p.backoffMs() > 0, "an adopted slow mean must throttle like a recorded one")
    }

    @Test
    fun `adopting zero stops throttling on a number from the past`() {
        val p = ServingPressure(thresholdMs = 2_000)
        p.adopt(10_000, 100)
        assertTrue(p.backoffMs() > 0)

        p.adopt(0, 0)

        assertEquals(0, p.backoffMs(), "a dead feed has no clients to protect")
    }

    @Test
    fun `an adopted mean below the sample gate is not yet evidence`() {
        val p = ServingPressure(thresholdMs = 2_000)

        p.adopt(10_000, (ServingPressure.MIN_SAMPLES - 1).toLong())

        assertEquals(0, p.backoffMs())
    }

    @Test
    fun `the backoff is bounded however bad it gets`() {
        // Ingest yields; it does not stop.
        val p = ServingPressure(thresholdMs = 1_000, maxBackoffMs = 2_000)

        feed(p, 600_000, 200)

        assertEquals(2_000, p.backoffMs())
    }
}
