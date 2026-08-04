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
    fun `a finished walk leaves the average`() {
        val p = PagingProgress()
        p.begin("a", top = 1_000L, bottom = 0L)
        p.begin("b", top = 1_000L, bottom = 0L)
        p.mark("b", 500L)

        p.finish("a")

        assertEquals(0.5, p.fraction()!!, 0.001, "only b is still walking")
        p.finish("b")
        assertNull(p.fraction(), "nothing walking means no number to report")
    }

    @Test
    fun `an inverted or empty window is not a walk`() {
        // A leg whose since is above its until asks for a range nothing can be
        // in. Dividing by that span would produce infinities on the status line.
        val p = PagingProgress()

        p.begin("a", top = 100L, bottom = 900L)

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
