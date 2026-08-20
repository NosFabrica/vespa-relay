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
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * WHEN EACH RELAY WAS LAST SYNCED, and the readings that must not be confused
 * with each other.
 *
 * The three this is written against, because each was a real reading of the
 * mirror's numbers before there was a row to take it from:
 *
 *  - **"it stopped syncing" meaning a broken relay**, when as often it is this
 *    router that stopped dialling — the roster is the monitor's certified set.
 *  - **a quiet relay as a failing one**: a clean visit that delivered nothing is
 *    the normal state of a tailed relay, whose tail already had the events.
 *  - **the last ATTEMPT as the last SUCCESS**. A relay failing every visit for a
 *    week has a very recent `lastVisitAt` and a very old `syncedAt`, and one
 *    number cannot say both.
 */
class VisitLedgerTest {
    private val sec = 1_000L

    @Test
    fun `a clean visit is the only thing that moves the synced clock`() {
        val ledger = VisitLedger()

        ledger.visited("wss://a.example/", startedMs = 10 * sec, VisitLedger.Ending.SYNCED, events = 40, nowMs = 12 * sec)
        val synced = ledger.snapshot(nowMs = 20 * sec).relays.single()
        assertEquals(12L, synced.syncedAt)
        assertEquals("synced", synced.outcome)
        assertEquals(0, synced.failures)
        // The visit's OWN start, not when the record was written: a five-hour
        // wedge must not read as having begun when it finally gave up.
        assertEquals(10L, synced.lastVisitAt)

        ledger.visited("wss://a.example/", startedMs = 30 * sec, VisitLedger.Ending.REFUSED, events = 0, nowMs = 31 * sec)
        val refused = ledger.snapshot(nowMs = 40 * sec).relays.single()
        assertEquals(12L, refused.syncedAt, "a failed visit does not re-stamp a sync that did not happen")
        assertEquals(30L, refused.lastVisitAt, "…and the ATTEMPT is still recorded, which is the pair that tells them apart")
        assertEquals("refused", refused.outcome)
        assertEquals(1, refused.failures)
    }

    @Test
    fun `an ending that needs no words carries none, and one that does carries the router's own`() {
        val ledger = VisitLedger()
        ledger.visited("wss://ok.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = sec)
        ledger.visited("wss://no.example/", startedMs = sec, VisitLedger.Ending.QUIET, events = 0, nowMs = sec)
        ledger.visited(
            "wss://throw.example/",
            startedMs = sec,
            VisitLedger.Ending.FAILED,
            events = 0,
            detail = "The visit threw and was abandoned — SocketTimeoutException: timeout",
            nowMs = sec,
        )

        val rows = ledger.snapshot(nowMs = sec).relays.associateBy { it.relay }
        assertNull(rows["wss://ok.example/"]!!.detail, "\"synced\" is the whole story")
        assertTrue(rows["wss://no.example/"]!!.detail!!.contains("gave up"), "an ending with a standing explanation carries it")
        // FAILED has no standing explanation — the exception IS the message —
        // so a caller that passes none would publish a fault with no cause.
        assertTrue(rows["wss://throw.example/"]!!.detail!!.contains("SocketTimeoutException"))
    }

    @Test
    fun `failures count consecutively and a clean visit clears them`() {
        val ledger = VisitLedger()
        repeat(3) { ledger.visited("wss://a.example/", startedMs = sec, VisitLedger.Ending.REFUSED, events = 0, nowMs = sec) }
        assertEquals(
            3,
            ledger
                .snapshot(nowMs = sec)
                .relays
                .single()
                .failures,
        )

        ledger.visited("wss://a.example/", startedMs = 2 * sec, VisitLedger.Ending.SYNCED, events = 0, nowMs = 2 * sec)
        assertEquals(
            0,
            ledger
                .snapshot(nowMs = 2 * sec)
                .relays
                .single()
                .failures,
            "a blip and a week of failures must not read alike",
        )
    }

    @Test
    fun `a relay the roster dropped says so instead of looking merely quiet`() {
        val ledger = VisitLedger()
        ledger.roster(mapOf("wss://a.example/" to listOf("content"), "wss://b.example/" to listOf("content")))
        ledger.visited("wss://a.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = sec)
        ledger.visited("wss://b.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = sec)

        // The monitor stopped certifying b: nothing will visit it again, and
        // its row is the only place that can say so.
        ledger.roster(mapOf("wss://a.example/" to listOf("content")))

        val rows = ledger.snapshot(nowMs = sec).relays.associateBy { it.relay }
        assertTrue(rows["wss://a.example/"]!!.onRoster)
        assertFalse(rows["wss://b.example/"]!!.onRoster)
        assertEquals(
            listOf("content"),
            rows["wss://b.example/"]!!.streams,
            "the streams that USED to ask for it stay — a dropped relay nobody can attribute is unfindable",
        )
    }

    @Test
    fun `an event from a tail moves the freshness clock without a visit`() {
        val ledger = VisitLedger()
        ledger.visited("wss://a.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 0, nowMs = sec)
        // Half an hour later the tail delivers. No visit has run — a tailed
        // relay's revisit base is thirty minutes — and freshness read off the
        // visit alone would call this relay stale while events are landing.
        ledger.received("wss://a.example/", nowMs = 1_800 * sec)

        val row = ledger.snapshot(nowMs = 1_800 * sec).relays.single()
        assertEquals(1_800L, row.lastEventAt)
        assertEquals(1L, row.syncedAt, "an arrival is not a visit")
        assertEquals(0L, row.events, "…and it does not rewrite what the last visit itself carried")
    }

    @Test
    fun `the live half is read at snapshot time, not stored`() {
        val ledger = VisitLedger()
        ledger.visited("wss://a.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = sec)

        val idle = ledger.snapshot(nowMs = sec).relays.single()
        assertFalse(idle.tailed)
        assertNull(idle.nextVisitInSec)
        assertNull(idle.heldForSec, "no worker holds it, and a 0 there would read as \"being synced right now\"")

        val live =
            ledger
                .snapshot(
                    nowMs = sec,
                    tailed = { true },
                    nextVisitInSec = { 240 },
                    heldForSec = { 12 },
                ).relays
                .single()
        assertTrue(live.tailed)
        assertEquals(240L, live.nextVisitInSec)
        assertEquals(12L, live.heldForSec)
    }

    @Test
    fun `the worst rows are the ones that survive the publish cap`() {
        val ledger = VisitLedger()
        ledger.roster((1..4).associate { "wss://r$it.example/" to listOf("content") })
        // Synced long ago, recently, and never — plus one the roster dropped.
        ledger.visited("wss://r1.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = 10 * sec)
        ledger.visited("wss://r2.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = 900 * sec)
        ledger.visited("wss://r3.example/", startedMs = sec, VisitLedger.Ending.REFUSED, events = 0, nowMs = 900 * sec)
        ledger.roster((1..3).associate { "wss://r$it.example/" to listOf("content") })

        val all = ledger.snapshot(nowMs = 1_000 * sec).relays.map { it.relay }
        assertEquals(
            listOf("wss://r3.example/", "wss://r1.example/", "wss://r2.example/", "wss://r4.example/"),
            all,
            "never-synced first, then oldest, then newest — and the relay nobody is dialling last",
        )

        val cut = ledger.snapshot(nowMs = 1_000 * sec, limit = 2)
        assertEquals(listOf("wss://r3.example/", "wss://r1.example/"), cut.relays.map { it.relay })
        assertEquals(2, cut.omitted, "a list that does not say it was cut reads as the whole answer")
    }

    @Test
    fun `the ledger forgets dropped relays before it forgets wanted ones`() {
        val ledger = VisitLedger(cap = 2)
        ledger.roster(mapOf("wss://gone1.example/" to listOf("content"), "wss://gone2.example/" to listOf("content")))
        ledger.visited("wss://gone1.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = sec)
        ledger.visited("wss://gone2.example/", startedMs = sec, VisitLedger.Ending.SYNCED, events = 1, nowMs = 2 * sec)
        // Both leave the roster; three relays join it. The cap is two, and what
        // must survive is the three the roster still wants — a ledger that
        // forgets the relay being looked up is worth more heap than it saves.
        ledger.roster((1..3).associate { "wss://keep$it.example/" to listOf("content") })

        // Asked with room to spare, so what this reads is what the ledger
        // still HOLDS rather than what the publish cap would show.
        val kept = ledger.snapshot(nowMs = 3 * sec, limit = 10).relays.map { it.relay }
        assertTrue(kept.none { it.startsWith("wss://gone") }, "the off-roster rows went first")
        assertEquals(3, kept.size, "…and an over-cap roster is kept whole rather than half-forgotten")
    }
}
