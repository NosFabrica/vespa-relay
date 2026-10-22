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
package com.nosfabrica.vespa.relay.sync

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** What the health line says about each state of the process, and that a quiet one raises no alarm. */
class HealthLineTest {
    private val calm =
        HealthReading(
            heapUsedMb = 512,
            heapMaxMb = 4_096,
            queueDepth = 10,
            queueCapacity = 4_096,
            bottleneck = "mixed",
            arrivingPerSec = 40,
            eventsPerSec = 38,
            connected = 120,
            fatals = 0,
            retracted = 0,
            pressure = null,
            ingestWorkers = 2,
            ingestWorkersRunning = 2,
            lostToStore = 0,
            inBatch = 1,
            oldestBatchSec = 0,
            oldestStoreCall = null,
        )

    @Test
    fun `a healthy process raises no alarm`() {
        val line = healthLine(calm)
        assertTrue(line.startsWith("router: health heap 512/4096MB (12%), ingest queue 10/4096, 40 ev/s in, 38 ev/s out"), line)
        assertFalse(line.any { it == '!' } || Regex("[A-Z]{4,}").containsMatchIn(line), "no shouting on a calm line: $line")
    }

    @Test
    fun `a wedged queue points at the store and names the call it is stuck in`() {
        val line = healthLine(calm.copy(bottleneck = "wedged", queueDepth = 4_096, inBatch = 2, oldestBatchSec = 300, oldestStoreCall = "ingest write 290s"))
        assertTrue("NOT DRAINING" in line && "2/2 worker(s) in a batch, the oldest for 300s" in line, line)
        assertTrue(line.contains("Longest store call: ingest write 290s"), line)
        val noCall = healthLine(calm.copy(bottleneck = "wedged", oldestStoreCall = null))
        assertTrue("held somewhere other than the store" in noCall, "and says so when no store call explains it: $noCall")
    }

    @Test
    fun `the failures that lose data or capacity are each named`() {
        val line =
            healthLine(
                calm.copy(heapUsedMb = 3_900, fatals = 1, retracted = 7, ingestWorkersRunning = 1, lostToStore = 3, pressure = "relay reads 140ms"),
            )
        for (said in listOf("AT THE CEILING", "1 FATAL error(s)", "7 record(s) DELETED", "1 of 2 ingest worker(s) HAVE STOPPED", "3 event(s) FAILED to reach the store", "relay reads 140ms")) {
            assertTrue(said in line, "missing '$said': $line")
        }
    }

    @Test
    fun `the bottleneck separates a stopped queue from a busy one`() {
        assertEquals("wedged", bottleneckOf(wedged = true, depth = 4_096, capacity = 4_096, rate = 0))
        assertEquals("ingest", bottleneckOf(wedged = false, depth = 4_096, capacity = 4_096, rate = 900))
        assertEquals("upstream", bottleneckOf(wedged = false, depth = 0, capacity = 4_096, rate = 0))
        assertEquals("downloads", bottleneckOf(wedged = false, depth = 0, capacity = 4_096, rate = 50))
    }

    @Test
    fun `rates are per second over the window since the last reading`() {
        val rates = IngestRates(lastAtMs = 0)
        assertEquals(100 to 200, rates.next(events = 6_000, submitted = 12_000, nowMs = 60_000))
        assertEquals(0 to 50, rates.next(events = 6_000, submitted = 15_000, nowMs = 120_000), "a full queue at 0 ev/s still shows what arrived")
    }
}
