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
