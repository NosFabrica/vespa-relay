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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The visit model's two pieces of pure arithmetic: which streams ride the
 * rotating pool, and when a relay's history is due its audit. Everything else
 * in [VisitPool] is sockets and clocks, which the probes cover.
 */
class VisitPoolTest {
    @Test
    fun `a band that never had a full pass is audited on its first visit`() {
        // fullAt = 0 is quartz's "never": a fresh relay's history should be
        // verified as soon as there is a covered range to verify, not a week
        // after it appeared.
        assertTrue(VisitPool.auditDue(fullAt = 0L, now = 1_000_000, verifySeconds = 604_800))
    }

    @Test
    fun `the audit fires when the last full pass ages past the knob, and not before`() {
        val week = 604_800L
        val now = 2_000_000L
        assertFalse(VisitPool.auditDue(fullAt = now - week + 1, now = now, verifySeconds = week))
        assertTrue(VisitPool.auditDue(fullAt = now - week, now = now, verifySeconds = week))
        assertTrue(VisitPool.auditDue(fullAt = now - 2 * week, now = now, verifySeconds = week))
    }

    @Test
    fun `more content lately means a sooner revisit, on both bases`() {
        // The priority rule: yield divides the wait. Fifty decayed events
        // halves it, five hundred pins it near the floor — and the tailed
        // base stays above the untailed one at every score, because a tail
        // is already carrying that relay's present.
        val quietTailed = VisitPool.revisitDelayMs(0.0, tailed = true)
        val quietUntailed = VisitPool.revisitDelayMs(0.0, tailed = false)
        assertEquals(VisitPool.REVISIT_TAILED_MS, quietTailed)
        assertEquals(VisitPool.REVISIT_UNTAILED_MS, quietUntailed)
        assertEquals(quietTailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = true))
        assertEquals(quietUntailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = false))
        // Five hundred decayed events takes an untailed relay all the way to
        // the floor — base/11 sits under it — while the tailed base, six times
        // longer, still has room to divide.
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(500.0, tailed = false))
        assertTrue(VisitPool.revisitDelayMs(500.0, tailed = true) > VisitPool.revisitDelayMs(500.0, tailed = false))
    }

    @Test
    fun `a firehose relay is a frequent guest, never a busy loop`() {
        // The floor: however much a relay delivers, its revisit never drops
        // under a minute — the queue wait and the visit itself are the real
        // pacing below that, and a zero delay would be a spin on one relay.
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = false))
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = true))
    }

    @Test
    fun `only a purely verdict-built stream rides the pool`() {
        // The fork's arithmetic, spelled as config: syncableRelays alone is
        // visit-mode; syncableRelays beside a parsed relaySource is the
        // mid-migration union, which stays on the legacy engine.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    pure {
                        dir    = "down"
                        sync   = "fetch"
                        filter = { "kinds": [1] }
                        syncableRelays = {}
                        verifySeconds  = 604800
                    }
                    mixed {
                        dir    = "down"
                        sync   = "fetch"
                        filter = { "kinds": [1] }
                        syncableRelays = {}
                        relaySource = [
                            {
                                select = [ { kind = 10009, tag = "group", index = 2 } ]
                                filter = { "kinds": [10009] }
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val visit = cfg.streams.filter { it.dynamic?.syncable != null && it.dynamic!!.sources.isEmpty() }
        assertEquals(listOf("pure"), visit.map { it.name })
        assertEquals(604_800L, visit.single().verifySeconds)
    }

    @Test
    fun `verifySeconds has an hour floor — an audit is not a re-walk loop`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir    = "down"
                        sync   = "fetch"
                        filter = { "kinds": [1] }
                        syncableRelays = {}
                        verifySeconds  = 5
                    }
                }
                """.trimIndent(),
            )
        assertEquals(3600L, cfg.streams.single().verifySeconds)
    }
}
