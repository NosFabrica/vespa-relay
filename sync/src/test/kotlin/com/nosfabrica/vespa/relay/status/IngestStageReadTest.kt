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
package com.nosfabrica.vespa.relay.status

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The ingest-stage split the health loop publishes, pinned end to end: the store's
 * structured read, and the rows the page draws from it.
 */
class IngestStageReadTest {
    /** Mirrors the private `SyncEngine.stageSplit`; the library call it makes is what must not drift. */
    private fun stageSplit(): List<SyncProgress.StageDetail> =
        IngestStats
            .snapshot()
            .map { (name, st) ->
                SyncProgress.StageDetail(
                    stage = name,
                    ms = st.totalNanos / 1_000_000,
                    calls = st.calls,
                    meanMs = st.meanNanos / 1_000_000,
                    maxMs = st.maxNanos / 1_000_000,
                )
            }.sortedByDescending { it.ms }

    @Test
    fun `a stage booked through the real IngestStats reads back out of snapshot`() {
        // A unique name, so other tests sharing the JVM can neither pass nor fail this one.
        val stage = "readtest.${System.nanoTime()}"
        IngestStats.add(stage, 2_500_000_000L)

        val row = assertNotNull(stageSplit().firstOrNull { it.stage == stage }, "the read did not find a stage the library was just given")

        assertEquals(2_500L, row.ms, "2.5 seconds must read back as 2500ms")
        // `add` books a duration measured elsewhere, so there is no call count to average over.
        assertEquals(0L, row.calls, "a stage added to but never timed reports no calls")
    }

    @Test
    fun `a sub-millisecond stage survives the read the parser rounded away`() {
        // `dump()` formats `%.2fs`, so a sub-millisecond total survives only through `snapshot()`.
        val stage = "readtest.tiny.${System.nanoTime()}"
        IngestStats.add(stage, 400_000L) // 0.4ms

        val st = assertNotNull(IngestStats.snapshot()[stage])
        assertEquals(400_000L, st.totalNanos, "the structured read carries nanoseconds, not a rounded string")
    }

    @Test
    fun `the shape of a timed stage rides beside its total`() =
        kotlinx.coroutines.runBlocking {
            val stage = "readtest.timed.${System.nanoTime()}"
            IngestStats.timed(stage) { }
            IngestStats.timed(stage) { }

            val row = assertNotNull(stageSplit().firstOrNull { it.stage == stage })
            assertEquals(2L, row.calls, "only `timed` books calls, and it booked two")
            assertTrue(row.maxMs >= 0, "the worst single call is published even when it rounds to nothing")
        }

    @Test
    fun `stage rows are published busiest first, with a mean only where one exists`() {
        val h =
            SyncProgress.Health(
                bottleneck = "ingest",
                eventsPerSec = 0,
                arrivingPerSec = 0,
                heapUsedMb = 1,
                heapMaxMb = 2,
                sockets = 0,
                socketCeiling = 1,
                socketsRunning = 0,
                socketsQueued = 0,
                servingMs = null,
                stageDetail =
                    listOf(
                        SyncProgress.StageDetail("proj.fetch.derive", ms = 41_000, calls = 12, meanMs = 3_416, maxMs = 20_100),
                        // Booked from a duration measured elsewhere, so it carries no denominator.
                        SyncProgress.StageDetail("lock.ingest.wait", ms = 38_000, calls = 0, meanMs = 0, maxMs = 0),
                    ),
            )

        val rows = SyncProgress.document(emptyList(), health = h, nowSeconds = 1_000)["health"]!!.jsonObject["stages"] as JsonArray

        assertEquals(2, rows.size)
        val first = rows[0].jsonObject
        assertEquals("proj.fetch.derive", first["stage"]!!.jsonPrimitive.content)
        assertEquals(12L, first["calls"]!!.jsonPrimitive.long)
        assertEquals(20_100L, first["maxMs"]!!.jsonPrimitive.long)

        val second = rows[1].jsonObject
        assertEquals("lock.ingest.wait", second["stage"]!!.jsonPrimitive.content)
        assertEquals(38_000L, second["ms"]!!.jsonPrimitive.long)
        assertNull(second["calls"], "a stage with no call count publishes none rather than a mean over a fiction")
        assertNull(second["meanMs"])
    }

    @Test
    fun `no stages booked publishes no rows rather than one junk row`() {
        val h =
            SyncProgress.Health(
                bottleneck = "upstream",
                eventsPerSec = 0,
                arrivingPerSec = 0,
                heapUsedMb = 1,
                heapMaxMb = 2,
                sockets = 0,
                socketCeiling = 1,
                socketsRunning = 0,
                socketsQueued = 0,
                servingMs = null,
            )
        val health = SyncProgress.document(emptyList(), health = h, nowSeconds = 1_000)["health"]!!.jsonObject
        assertNull(health["stages"])
    }
}
