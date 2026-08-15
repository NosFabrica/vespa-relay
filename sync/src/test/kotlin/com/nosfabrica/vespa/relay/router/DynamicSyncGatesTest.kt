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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The two gates a dynamic stream's workers pass through, and why they are two.
 *
 * This is arithmetic, but it is the arithmetic a live run caught: with one gate
 * doing both jobs, a `concurrency = 8` stream returned 109 of 2,692 relays in
 * five minutes — a two-hour pass — because a discovered relay list is mostly
 * dead hosts and every one of them was spending a TRANSFER slot to be declared
 * dead. The same list at `concurrency = 30` reached 2,349 in the same five
 * minutes. The pass was tracking the pool size and not the network.
 */
class DynamicSyncGatesTest {
    @Test
    fun `a leg is given up on for SILENCE, never for elapsed time`() {
        // The reported failure: `wss://fiatjaf.com/xenon-lima` held for 5h00m
        // having delivered 85 events, quiet for the last 4h56m — one ask per
        // author chunk, each costing a full NEG_IDLE_MS window and nothing
        // bounding the sequence of them. The url is skipped by every pass in the
        // meantime, because the rotation claim is still ours.
        assertTrue(DynamicSync.givesUp(askIndex = 1, quietForMs = LEG_QUIET_GIVE_UP_MS))
        assertTrue(DynamicSync.givesUp(askIndex = 600, quietForMs = 4 * 60 * 60 * 1000L))

        // A relay that is still delivering resets the clock on every event, so
        // it cannot trip this however long it runs. That is the difference from
        // the wall-clock deadline this replaces — one that fired on elapsed time
        // and so could only ever cut a leg that was WORKING, and did: four
        // healthy upstreams truncated at its 4h mark.
        assertFalse(DynamicSync.givesUp(askIndex = 10_000, quietForMs = 0L))
        assertFalse(DynamicSync.givesUp(askIndex = 600, quietForMs = LEG_QUIET_GIVE_UP_MS - 1))
    }

    @Test
    fun `silence is measured from the transfer, not from the rotation claim`() {
        // The bug the audit found in the first cut. The quiet clock starts at
        // the CLAIM, which is taken before the strike check, the Tor probe, the
        // TCP pre-probe and the queue for a transfer slot — and a saturated pool
        // holds that queue for minutes. So a worker could reach ask 1 with the
        // clock already past the window and abandon a perfectly healthy relay,
        // every pass, having asked it once. `askIndex > 0` does not cover it,
        // because ask 0 legitimately returns nothing for most author chunks.
        //
        // syncRelay now caps the reading at how long the leg itself has been
        // running, so a leg that just started is never "silent for 5 minutes"
        // however long its claim waited. This is that arithmetic.
        val quietSinceClaim = 20 * 60 * 1000L
        val legJustStarted = 1_000L
        assertFalse(
            DynamicSync.givesUp(askIndex = 1, quietForMs = minOf(quietSinceClaim, legJustStarted)),
            "a leg that queued 20min for a slot was cut off on its second ask",
        )
        // …and once the leg itself has been silent that long, it still fires.
        assertTrue(DynamicSync.givesUp(askIndex = 1, quietForMs = minOf(quietSinceClaim, LEG_QUIET_GIVE_UP_MS)))
    }

    @Test
    fun `the first ask is always made, however long the claim waited`() {
        // The quiet clock runs from the CLAIM, which is taken before the guards
        // and before the queue for a transfer slot. A leg that waited out a
        // saturated pool therefore arrives already "quiet" for minutes — giving
        // up there would abandon the relay without asking it anything, which is
        // the opposite of the fault this fixes.
        assertFalse(DynamicSync.givesUp(askIndex = 0, quietForMs = 6 * 60 * 60 * 1000L))
    }

    @Test
    fun `no leg reporter is not evidence of silence`() {
        // Nothing is measuring this leg, so there is nothing to conclude from.
        // A guess in this direction costs a relay its whole pass.
        assertFalse(DynamicSync.givesUp(askIndex = 99, quietForMs = null))
    }

    @Test
    fun `the give-up window is well clear of a single ask's idle window`() {
        // An ask that returns nothing legitimately costs one idle window, so a
        // threshold near NEG_IDLE_MS would cut a relay for answering one empty
        // chunk slowly. Ten of them means a relay has to be genuinely silent for
        // five minutes.
        assertTrue(LEG_QUIET_GIVE_UP_MS >= 5 * NEG_IDLE_MS, "one slow empty ask must never be enough to abandon a relay")
        assertEquals(300_000L, LEG_QUIET_GIVE_UP_MS)
    }

    @Test
    fun `a walk that was refused with nothing delivered ends the relay's remaining legs`() {
        // The wedge class the give-up above cannot see: with `authorsPerLeg`
        // unset there is exactly one ask, so `givesUp` never runs, and the leg
        // loop inside the ask re-opened the same refused conversation once per
        // leg — an idle window of silence apiece, hours on a multi-leg filter.
        // quartz already names why each walk ended; this is believing it.
        for (end in listOf(
            PagedFetchResult.End.IDLE,
            PagedFetchResult.End.CLOSED,
            PagedFetchResult.End.AUTH_REQUIRED,
            PagedFetchResult.End.CANNOT_CONNECT,
            PagedFetchResult.End.UNPAGEABLE,
        )) {
            assertTrue(DynamicSync.refusedOutright(PagedFetchResult(0, end)), "$end with nothing delivered is a refusal")
        }
    }

    @Test
    fun `a drained or self-limited walk is not a refusal, and neither is one that delivered`() {
        // DRAINED is the relay honestly EOSEing an empty page — the one ending
        // that proves absence — and LIMIT_REACHED stopped on our own
        // instruction. Neither says the next leg is futile.
        assertFalse(DynamicSync.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.DRAINED)))
        assertFalse(DynamicSync.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.LIMIT_REACHED)))
        // A walk that carried events did real work whatever ended it: a CLOSED
        // after 4,000 events is a rate limit, not a dead relay, and the later
        // legs may fare better.
        assertFalse(DynamicSync.refusedOutright(PagedFetchResult(4_000, PagedFetchResult.End.CLOSED)))
        assertFalse(DynamicSync.refusedOutright(PagedFetchResult(1, PagedFetchResult.End.IDLE)))
    }

    @Test
    fun `admission is far wider than the transfer pool`() {
        // The guards are a TCP connect and the sync is a whole transfer; the
        // ratio is what stops the corpses in a relay list from crowding out the
        // relays that answer.
        assertEquals(128, DynamicSync.admissionWidth(8))
        assertEquals(480, DynamicSync.admissionWidth(30))
        assertTrue(DynamicSync.admissionWidth(8) > 8 * 8, "a small stream inherits the problem this exists to fix")
    }

    @Test
    fun `it has a floor, so a small stream is not throttled to its pool`() {
        // concurrency 1 x 16 is 16, which would put 16 dead hosts in front of
        // every live one. The floor is what makes the guard phase cheap
        // regardless of how narrow the transfer pool is.
        assertEquals(128, DynamicSync.admissionWidth(1))
        assertEquals(128, DynamicSync.admissionWidth(4))
    }

    @Test
    fun `the next pass waits for HALF the transfer pool, not for one slot`() {
        // A pass started against a committed pool is not extra parallelism: the
        // walk hands out its whole list regardless, and every url then queues
        // for a slot that does not exist. At `recycleSeconds = 1` against
        // `concurrency = 100` that is a pass a second producing log lines and a
        // `taken` count nobody can act on.
        assertEquals(50, DynamicSync.poolHeadroom(100))
        assertEquals(4, DynamicSync.poolHeadroom(8))
        assertEquals(15, DynamicSync.poolHeadroom(30))
    }

    @Test
    fun `rounded up, so a one-slot stream waits for its one leg`() {
        // `ceil` doing the general rule's job at the smallest size rather than a
        // special case: a stream configured at 1 cannot dial anything until its
        // single leg returns, so starting a pass before then is the pure form of
        // the waste above.
        assertEquals(1, DynamicSync.poolHeadroom(1))
        assertEquals(2, DynamicSync.poolHeadroom(3))
        assertTrue(DynamicSync.poolHeadroom(0) >= 1, "never zero — a gate that always opens is not a gate")
    }

    @Test
    fun `and a ceiling, because unbounded probing is a file-descriptor limit`() {
        // The shape before this gate existed: every url in the list probed at
        // once. 18,687 concurrent connects is not a concurrency setting, it is
        // an ulimit waiting to be found in production.
        assertEquals(512, DynamicSync.admissionWidth(100))
        assertEquals(512, DynamicSync.admissionWidth(10_000))
    }
}
