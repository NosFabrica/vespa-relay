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
import com.nosfabrica.vespa.relay.router.discovery.DiscoveredRelay
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    fun `every dynamic stream rides the pool — verdicts, certified scans, retracting streams`() {
        // The fork is gone with the engine it forked to: the loader refuses
        // an ungated scan outright, so every dynamic stream that parses
        // rides the pool. Three shapes, one per rule: a verdict source; a
        // certified scan; a retracting stream whose deleteMissing comparison
        // runs as its verifySeconds audit.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    pure {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#s": ["syncable"] }
                            }
                        ]
                        verifySeconds  = 604800
                    }
                    gatedScan {
                        dir    = "down"
                        filter = { "kinds": [30382] }
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                                certified = {}
                            }
                        ]
                    }
                    retracting {
                        dir    = "down"
                        filter = { "kinds": [0, 30382] }
                        deleteMissing = "dryRun"
                        ownedKinds = [30382]
                        verifySeconds = 86400
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                                certified = {}
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val visit = cfg.streams.filter { VisitPool.ridesThePool(it) }
        // As a SET: HOCON hands back a map, and the block order is not a promise.
        assertEquals(setOf("pure", "gatedScan", "retracting"), visit.map { it.name }.toSet())
        assertEquals(604_800L, visit.single { it.name == "pure" }.verifySeconds)
    }

    @Test
    fun `a retracting pool stream must say when its comparison runs`() {
        // The deleteMissing comparison rides the verifySeconds audit, so a
        // pool-shaped retracting stream without the knob has no clock for the
        // one decision that destroys data — refused where it is typed.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [0, 30382] }
                        deleteMissing = "dryRun"
                        ownedKinds = [30382]
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                                certified = {}
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        }
    }

    @Test
    fun `a walk that was refused with nothing delivered ends the relay's visit`() {
        // quartz already names why each walk ended; this is believing it. A
        // refusal ends the whole visit rather than re-opening the same
        // conversation once per remaining ask — an idle window of silence
        // apiece.
        for (end in listOf(
            PagedFetchResult.End.IDLE,
            PagedFetchResult.End.CLOSED,
            PagedFetchResult.End.AUTH_REQUIRED,
            PagedFetchResult.End.CANNOT_CONNECT,
            PagedFetchResult.End.UNPAGEABLE,
        )) {
            assertTrue(VisitPool.refusedOutright(PagedFetchResult(0, end)), "$end with nothing delivered is a refusal")
        }
    }

    @Test
    fun `a drained or self-limited walk is not a refusal, and neither is one that delivered`() {
        // DRAINED is the relay honestly EOSEing an empty page — the one ending
        // that proves absence — and LIMIT_REACHED stopped on our own
        // instruction. Neither says the next ask is futile. And a walk that
        // carried events did real work whatever ended it: a CLOSED after 4,000
        // events is a rate limit, not a dead relay.
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.DRAINED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.LIMIT_REACHED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(4_000, PagedFetchResult.End.CLOSED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(1, PagedFetchResult.End.IDLE)))
    }

    @Test
    fun `one ask per bound author, the pairing the tags already made`() {
        // authorsPerLeg = 1 made structural: a relay two providers name
        // yields two single-author filters — each its own immortal band —
        // and a third provider later is a third ask beside them, never an
        // invalidation. Other narrow keys ride along in every split.
        val base = Filter(kinds = listOf(0, 30382))
        val url = RelayUrlNormalizer.normalize("wss://provider.example")
        val p1 = "a".repeat(64)
        val p2 = "b".repeat(64)
        val paired =
            VisitPool.asksOf(
                base,
                DiscoveredRelay(url, narrow = mapOf("authors" to setOf(p2, p1), "kinds" to setOf("30382"))),
            )
        assertEquals(2, paired.size)
        assertEquals(listOf(listOf(p1), listOf(p2)), paired.map { it.authors }, "sorted, so the band's serialized key is stable")
        assertTrue(paired.all { it.kinds == listOf(30382) }, "the other narrow keys ride along in each split")
        // A select that binds nothing keeps one ask: the stream's own filter.
        val unbound = VisitPool.asksOf(base, DiscoveredRelay(url))
        assertEquals(listOf(base), unbound)
    }

    @Test
    fun `verifySeconds has an hour floor — an audit is not a re-walk loop`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#s": ["syncable"] }
                            }
                        ]
                        verifySeconds  = 5
                    }
                }
                """.trimIndent(),
            )
        assertEquals(3600L, cfg.streams.single().verifySeconds)
    }
}
