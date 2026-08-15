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
    private fun w(
        stream: String,
        url: String,
    ) = PagingProgress.Walked(stream, url)

    @Test
    fun `progress is the walked share of the time window`() {
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!

        assertEquals(0.0, p.fraction()!!, 0.001, "nothing walked yet")

        a.reached(750L)
        assertEquals(0.25, p.fraction()!!, 0.001)

        a.reached(100L)
        assertEquals(0.90, p.fraction()!!, 0.001)
    }

    @Test
    fun `a page that jumps backwards cannot un-advance the walk`() {
        // Pages arrive from one relay in order, but nothing in the protocol
        // guarantees it, and a percentage that goes DOWN is worse than one that
        // is slightly wrong — it reads as the sync having lost ground.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!

        a.reached(200L)
        a.reached(900L)

        assertEquals(0.80, p.fraction()!!, 0.001, "the later, higher `until` is ignored")
    }

    @Test
    fun `relays average rather than sum`() {
        // Two relays each walking their own window: one done, one untouched, is
        // half way — not 100%, which summing would give.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        val b = p.begin(w("s", "b"), top = 500L, bottom = 0L)!!

        a.reached(0L)

        assertEquals(0.5, p.fraction()!!, 0.001)
    }

    @Test
    fun `a finished walk stays in the average, so the fraction cannot go backwards`() {
        // The bug this replaces: `finish` used to REMOVE the walk, so a relay
        // that completed left the numerator and the denominator together and a
        // stream whose fast relays drained first fell from 60% to 20% while
        // strictly gaining ground.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        val b = p.begin(w("s", "b"), top = 1_000L, bottom = 0L)!!
        b.reached(500L)

        val before = p.fraction("s")!!
        p.finish(w("s", "a"), covered = true)

        assertEquals(0.75, p.fraction("s")!!, 0.001, "a is done (1.0) and b is half way")
        assertTrue(p.fraction("s")!! >= before, "finishing a walk may never lower the fraction")

        p.finish(w("s", "b"), covered = true)
        assertEquals(1.0, p.fraction("s")!!, 0.001, "every walk settled")
    }

    @Test
    fun `a walk that ended without draining counts only what it reached`() {
        // The other half of the same rule. A leg that was capped, closed on, or
        // threw did not settle its window, and rounding it to 1.0 would make a
        // failed cycle read as a complete one — which is exactly how success and
        // failure came to render identically.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        a.reached(800L)

        p.finish(w("s", "a"), covered = false)

        assertEquals(0.2, p.fraction("s")!!, 0.001, "20% walked is 20%, finished or not")
    }

    @Test
    fun `a drained walk is a full share even though its last page stopped short`() {
        // `mark` is fed the cursor of each page RECEIVED, so the last thing a
        // drained walk reports is the oldest event the relay actually held —
        // routinely well above the filter's floor. Measured against the floor a
        // fully exhausted relay would sit near 70% forever.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        a.reached(700L)

        p.finish(w("s", "a"), covered = true)

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
        val done = p.begin(w("s", "done"), top = 1_000L, bottom = 0L)!!
        val live = p.begin(w("s", "live"), top = 1_000L, bottom = 0L)!!
        p.finish(w("s", "done"), covered = true)

        p.reset("s")

        assertEquals(0.0, p.fraction("s")!!, 0.001, "only the live walk is left, and it has walked nothing")
        live.reached(500L)
        assertEquals(0.5, p.fraction("s")!!, 0.001, "the live walk still advances after the reset")
    }

    @Test
    fun `reached names where the walk IS, not the deepest point it ever touched`() {
        // The fraction counts finished walks; this one must not. The line prints
        // it as "back to <date>", and a finished walk's floor would pin that
        // date at the deepest thing the cycle touched while the relays still
        // walking are nowhere near it.
        val p = PagingProgress()
        val deep = p.begin(w("s", "deep"), top = 1_000L, bottom = 0L)!!
        val shallow = p.begin(w("s", "shallow"), top = 1_000L, bottom = 0L)!!
        deep.reached(10L)
        shallow.reached(900L)
        p.finish(w("s", "deep"), covered = true)

        assertEquals(900L, p.reached("s"))
    }

    @Test
    fun `a walk reports where its events reached, before any page boundary`() {
        // THE FIRST PAGE IS THE WHOLE PROBLEM. `onNewPage` fires at page
        // BOUNDARIES, so a leg still inside its first page had never marked and
        // sat at `top` — the card then read `back to <the day the walk opened>`
        // on a leg streaming a backlog and on one receiving nothing at all, which
        // are the two states the cursor exists to separate. Marking per event
        // makes the position what has actually arrived.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!

        assertEquals(1_000L, p.cursorOf(w("s", "a")), "nothing received yet is the second it opened at")

        a.reached(900L)
        a.reached(880L)

        assertEquals(880L, p.cursorOf(w("s", "a")), "the oldest event received, not the last boundary crossed")
        assertEquals(0.12, p.fraction("s")!!, 0.001, "and the share moves with it")
    }

    @Test
    fun `a finished walk cannot be moved by a later leg on the same relay`() {
        // The per-event feed is wired into a callback SHARED with the negentropy
        // branch, and `finish` retains the walk for the rest of the cycle — so
        // without this guard a reconciling leg on the same `stream|url` would
        // drag the finished walk's cursor down with events that belong to no
        // walk at all, inflating the share it really achieved and the ETA drawn
        // from it. `cursorOf` and `reached` already filter the same way.
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        a.reached(800L)
        p.finish(w("s", "a"), covered = false)

        a.reached(100L)

        assertEquals(0.2, p.fraction("s")!!, 0.001, "20% is what it walked, and it is done walking")
        assertNull(p.cursorOf(w("s", "a")), "a finished walk is not a position")
    }

    @Test
    fun `a stream cannot claim another stream's walks by sharing a name prefix`() {
        // The key was `"$stream|$url"` matched with `startsWith("$stream|")`, so
        // stream `a` matched every key stream `a|b` wrote — clearing its walks on
        // `reset` and averaging them into its `fraction`. Nothing rejects a `|` in
        // a stream name; it comes from the operator's config file. Two fields
        // cannot collide whatever either half contains.
        val p = PagingProgress()
        val relay = p.begin(w("a|b", "relay"), top = 1_000L, bottom = 0L)!!
        relay.reached(500L)

        assertNull(p.fraction("a"), "stream `a` has no walks of its own")
        assertEquals(0.5, p.fraction("a|b")!!, 0.001)

        p.finish(w("a|b", "relay"), covered = true)
        p.reset("a")

        assertEquals(1.0, p.fraction("a|b")!!, 0.001, "another stream's reset may not clear these")
    }

    @Test
    fun `a leg whose window is inverted cannot inherit the previous leg's result`() {
        // One relay is walked leg after leg under the same key. While `finish`
        // DELETED the entry a skipped `begin` was harmless — the later marks and
        // `finish` found nothing. With the walk retained, leaving the old one in
        // place let the skipped leg's `finish` land on the finished one.
        val p = PagingProgress()
        p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        p.finish(w("s", "a"), covered = true)
        assertEquals(1.0, p.fraction("s")!!, 0.001)

        // The next leg is inverted, so nothing is walked for it at all — and the
        // null handle is how the caller learns that, rather than marking onto
        // whatever the key held before.
        val skipped = p.begin(w("s", "a"), top = 100L, bottom = 900L)
        assertNull(skipped, "an inverted window hands back no walk to report on")
        skipped?.reached(950L)
        p.finish(w("s", "a"), covered = false)

        assertNull(p.fraction("s"), "the drained walk is gone, and the inverted leg never became one")
    }

    @Test
    fun `an inverted window is not a walk`() {
        // A leg whose since is above its until asks for a range nothing can be
        // in. Dividing by that span would produce infinities on the status line.
        val p = PagingProgress()

        assertNull(p.begin(w("s", "a"), top = 100L, bottom = 900L))

        assertNull(p.fraction())
    }

    @Test
    fun `a single-second window is a walk`() {
        // A band's re-read edge leg is exactly this shape: since == until is
        // a real, one-second range, not an inverted one.
        val p = PagingProgress()

        p.begin(w("s", "a"), top = 500L, bottom = 500L)!!

        assertEquals(0.0, p.fraction()!!, 0.001)
        // `finish` keeps it — see the monotonicity test above — so it takes a
        // cycle boundary to make the number go away.
        p.finish(w("s", "a"))
        p.reset("s")
        assertNull(p.fraction())
    }

    @Test
    fun `no ETA before the estimate means anything`() {
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000_000L, bottom = 0L)!!

        a.reached(999_000L)

        // 0.1% in: extrapolating here yields "ETA ~9 days" from connect latency
        // alone, which is worse than printing nothing.
        assertNull(p.etaMs(), "too early to extrapolate")
    }

    @Test
    fun `ETA extrapolates from the rate achieved so far`() {
        val p = PagingProgress()
        val a = p.begin(w("s", "a"), top = 1_000L, bottom = 0L)!!
        a.reached(500L)

        // Half way, so whatever has elapsed is also what remains. The clock is
        // real here, so assert the relationship rather than a wall-clock value.
        Thread.sleep(5_100)
        val eta = p.etaMs()

        assertTrue(eta != null && eta in 4_000..7_000, "half done => about the elapsed time again, got $eta")
    }
}
