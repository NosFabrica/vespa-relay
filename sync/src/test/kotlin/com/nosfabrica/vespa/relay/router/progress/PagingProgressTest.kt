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
package com.nosfabrica.vespa.relay.router.progress

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PagingProgressTest {
    @Test
    fun `progress is the walked share of the time window`() {
        val p = PagingProgress()
        p.begin("a", top = 1_000L, bottom = 0L)

        assertEquals(0.0, p.fraction()!!, 0.001, "nothing walked yet")

        p.mark("a", 750L)
        assertEquals(0.25, p.fraction()!!, 0.001)

        p.mark("a", 100L)
        assertEquals(0.90, p.fraction()!!, 0.001)
    }

    @Test
    fun `a page that jumps backwards cannot un-advance the walk`() {
        // Pages arrive from one relay in order, but nothing in the protocol
        // guarantees it, and a percentage that goes DOWN is worse than one that
        // is slightly wrong — it reads as the sync having lost ground.
        val p = PagingProgress()
        p.begin("a", top = 1_000L, bottom = 0L)

        p.mark("a", 200L)
        p.mark("a", 900L)

        assertEquals(0.80, p.fraction()!!, 0.001, "the later, higher `until` is ignored")
    }

    @Test
    fun `relays average rather than sum`() {
        // Two relays each walking their own window: one done, one untouched, is
        // half way — not 100%, which summing would give.
        val p = PagingProgress()
        p.begin("a", top = 1_000L, bottom = 0L)
        p.begin("b", top = 500L, bottom = 0L)

        p.mark("a", 0L)

        assertEquals(0.5, p.fraction()!!, 0.001)
    }

    @Test
    fun `a finished walk stays in the average, so the fraction cannot go backwards`() {
        // The bug this replaces: `finish` used to REMOVE the walk, so a relay
        // that completed left the numerator and the denominator together and a
        // stream whose fast relays drained first fell from 60% to 20% while
        // strictly gaining ground.
        val p = PagingProgress()
        p.begin("s|a", top = 1_000L, bottom = 0L)
        p.begin("s|b", top = 1_000L, bottom = 0L)
        p.mark("s|b", 500L)

        val before = p.fraction("s")!!
        p.finish("s|a", covered = true)

        assertEquals(0.75, p.fraction("s")!!, 0.001, "a is done (1.0) and b is half way")
        assertTrue(p.fraction("s")!! >= before, "finishing a walk may never lower the fraction")

        p.finish("s|b", covered = true)
        assertEquals(1.0, p.fraction("s")!!, 0.001, "every walk settled")
    }

    @Test
    fun `a walk that ended without draining counts only what it reached`() {
        // The other half of the same rule. A leg that was capped, closed on, or
        // threw did not settle its window, and rounding it to 1.0 would make a
        // failed cycle read as a complete one — which is exactly how success and
        // failure came to render identically.
        val p = PagingProgress()
        p.begin("s|a", top = 1_000L, bottom = 0L)
        p.mark("s|a", 800L)

        p.finish("s|a", covered = false)

        assertEquals(0.2, p.fraction("s")!!, 0.001, "20% walked is 20%, finished or not")
    }

    @Test
    fun `a drained walk is a full share even though its last page stopped short`() {
        // `mark` is fed the cursor of each page RECEIVED, so the last thing a
        // drained walk reports is the oldest event the relay actually held —
        // routinely well above the filter's floor. Measured against the floor a
        // fully exhausted relay would sit near 70% forever.
        val p = PagingProgress()
        p.begin("s|a", top = 1_000L, bottom = 0L)
        p.mark("s|a", 700L)

        p.finish("s|a", covered = true)

        assertEquals(1.0, p.fraction("s")!!, 0.001)
    }

    @Test
    fun `the next cycle clears what the last one finished, and only that`() {
        // A stream name can carry both `urls` and `relaySource`, so a static
        // backfill's LIVE walk sits under the same prefix as the dynamic
        // cycle's. Clearing it would make the walk stop existing while it was
        // still running — every later mark and finish on a removed key is
        // silently a no-op.
        val p = PagingProgress()
        p.begin("s|done", top = 1_000L, bottom = 0L)
        p.begin("s|live", top = 1_000L, bottom = 0L)
        p.finish("s|done", covered = true)

        p.reset("s")

        assertEquals(0.0, p.fraction("s")!!, 0.001, "only the live walk is left, and it has walked nothing")
        p.mark("s|live", 500L)
        assertEquals(0.5, p.fraction("s")!!, 0.001, "the live walk still advances after the reset")
    }

    @Test
    fun `reached names where the walk IS, not the deepest point it ever touched`() {
        // The fraction counts finished walks; this one must not. The line prints
        // it as "back to <date>", and a finished walk's floor would pin that
        // date at the deepest thing the cycle touched while the relays still
        // walking are nowhere near it.
        val p = PagingProgress()
        p.begin("s|deep", top = 1_000L, bottom = 0L)
        p.begin("s|shallow", top = 1_000L, bottom = 0L)
        p.mark("s|deep", 10L)
        p.mark("s|shallow", 900L)
        p.finish("s|deep", covered = true)

        assertEquals(900L, p.reached("s"))
    }

    @Test
    fun `an inverted window is not a walk`() {
        // A leg whose since is above its until asks for a range nothing can be
        // in. Dividing by that span would produce infinities on the status line.
        val p = PagingProgress()

        p.begin("a", top = 100L, bottom = 900L)

        assertNull(p.fraction())
    }

    @Test
    fun `a single-second window is a walk`() {
        // A band's re-read edge leg is exactly this shape: since == until is
        // a real, one-second range, not an inverted one.
        val p = PagingProgress()

        p.begin("s|a", top = 500L, bottom = 500L)

        assertEquals(0.0, p.fraction()!!, 0.001)
        // `finish` keeps it — see the monotonicity test above — so it takes a
        // cycle boundary to make the number go away.
        p.finish("s|a")
        p.reset("s")
        assertNull(p.fraction())
    }

    @Test
    fun `no ETA before the estimate means anything`() {
        val p = PagingProgress()
        p.begin("a", top = 1_000_000L, bottom = 0L)

        p.mark("a", 999_000L)

        // 0.1% in: extrapolating here yields "ETA ~9 days" from connect latency
        // alone, which is worse than printing nothing.
        assertNull(p.etaMs(), "too early to extrapolate")
    }

    @Test
    fun `ETA extrapolates from the rate achieved so far`() {
        val p = PagingProgress()
        p.begin("a", top = 1_000L, bottom = 0L)
        p.mark("a", 500L)

        // Half way, so whatever has elapsed is also what remains. The clock is
        // real here, so assert the relationship rather than a wall-clock value.
        Thread.sleep(5_100)
        val eta = p.etaMs()

        assertTrue(eta != null && eta in 4_000..7_000, "half done => about the elapsed time again, got $eta")
    }
}
