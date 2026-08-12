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

import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The bound, which is the whole reason this class exists: overlapping passes may
 * never put a third gigabyte-scale id list on the heap.
 *
 * Everything here is about a straggler — a relay still reconciling against the
 * set its own pass built while the next pass wants to install a fresh one.
 */
class SharedIdSetTest {
    private fun ids(n: Int) = List(n) { IdAndTime(it.toLong(), "%064x".format(it)) }

    @Test
    fun `a pass whose relays all finished installs freely`() {
        val set = SharedIdSet()
        assertTrue(set.mayInstall())
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 0)

        val lease = set.lease()
        assertEquals(3, lease.ids.size)
        lease.release()

        // Nothing is reading the outgoing generation, so it is garbage the
        // moment it is replaced and never occupies the retirement slot.
        assertTrue(set.mayInstall())
        set.install(ids(5), nowMs = 0, forSince = null, buildMs = 0)
        assertEquals(1, set.generationsAlive())
        assertEquals(5, set.size())
    }

    @Test
    fun `a straggler holds its own generation, and the next install is refused`() {
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 0)
        val straggler = set.lease()

        // The pass ends, a new one builds a set. The straggler is still
        // reconciling against the old one, so it stays alive for it — and
        // nothing may be built on top of that.
        set.install(ids(5), nowMs = 0, forSince = null, buildMs = 0)
        assertEquals(2, set.generationsAlive())
        assertEquals(3, straggler.ids.size, "a straggler must keep reading the set it started against")
        assertEquals(5, set.lease().ids.size, "new asks get the new set")

        assertFalse(set.mayInstall(), "a third generation would be unbounded — a hung leg holds one for hours")

        straggler.release()
        assertTrue(set.mayInstall())
        assertEquals(1, set.generationsAlive())
    }

    @Test
    fun `several stragglers all have to finish`() {
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 0)
        val a = set.lease()
        val b = set.lease()
        set.install(ids(5), nowMs = 0, forSince = null, buildMs = 0)

        a.release()
        assertFalse(set.mayInstall(), "one reader left is still a reader")
        b.release()
        assertTrue(set.mayInstall())
    }

    @Test
    fun `releasing twice does not free a generation nobody has`() {
        // The failure this prevents is silent and permanent: a double release
        // drives the holder count below zero, the retirement slot is cleared by
        // a reader that had already gone, and a later straggler's set is then
        // installed over.
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 0)
        val a = set.lease()
        val b = set.lease()
        set.install(ids(5), nowMs = 0, forSince = null, buildMs = 0)

        a.release()
        a.release()
        assertFalse(set.mayInstall(), "the second release freed a generation b is still reading")
        b.release()
        assertTrue(set.mayInstall())
    }

    @Test
    fun `a rebuild has to earn its cost against the last one`() {
        // The arithmetic a rotation broke. One build per pass meant one per
        // refreshSeconds while the fan-out ended in a join — six hours by
        // default. Passes are now as frequent as recycleSeconds, so without a
        // pace a negentropy stream with a short list rebuilds every few seconds
        // and spends all of its time walking the store instead of mirroring.
        val set = SharedIdSet()
        assertTrue(set.worthRebuilding(0, null), "nothing built yet")

        // A 90s walk buys 15 minutes of reuse — about a tenth of the stream's
        // time spent building, whatever the corpus turns out to cost.
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 90_000)
        assertFalse(set.worthRebuilding(5_000, null), "five seconds later, against a 90-second walk")
        assertFalse(set.worthRebuilding(899_000, null))
        assertTrue(set.worthRebuilding(900_000, null))
    }

    @Test
    fun `a walk fast enough to be free still gets the floor`() {
        // Ten seconds of reuse for a one-second walk is still a rebuild every
        // other pass at recycleSeconds = 5 — the shape this prevents, not a
        // small version of it.
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 1_000)
        assertFalse(set.worthRebuilding(59_000, null))
        assertTrue(set.worthRebuilding(60_000, null))
    }

    @Test
    fun `a WIDER window rebuilds whatever the clock says`() {
        // The one direction staleness is not free. The window is narrowed to
        // what the hungriest relay still needs, so a set built for a narrow one
        // and reused against a wider one is a SUBSET of what the diff needs —
        // the reconcile then believes we lack events we hold and downloads them
        // again, which is the opposite of what the set is for. A widened window
        // means a relay with no band appeared: a new url, or a fold that expired.
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = 1_000, buildMs = 90_000)

        assertTrue(set.worthRebuilding(1_000, neededSince = 500), "reaching further back than the set covers")
        assertTrue(set.worthRebuilding(1_000, neededSince = null), "unbounded is wider than every bounded window")
        assertFalse(set.worthRebuilding(1_000, neededSince = 1_000), "the same window")
        assertFalse(set.worthRebuilding(1_000, neededSince = 5_000), "narrower is free — a superset only means fewer downloads")
    }

    @Test
    fun `an unbounded set is never too narrow for anything`() {
        val set = SharedIdSet()
        set.install(ids(3), nowMs = 0, forSince = null, buildMs = 90_000)
        assertFalse(set.worthRebuilding(1_000, neededSince = null))
        assertFalse(set.worthRebuilding(1_000, neededSince = 0))
    }

    @Test
    fun `a leg that runs for hours buys everyone else ONE more snapshot, then freezes it`() {
        // The consequence of the two-generation bound, and it is not obvious
        // from the bound itself. A relay with a long history is one worker
        // running for hours while the walk that handed it out finished long
        // ago. It holds the generation it started against; the next pass
        // installs a fresh one, which RETIRES the held one — and nothing may be
        // built over an occupied retirement slot. So every pass after that
        // reuses the same set until the straggler finishes.
        //
        // Stale, not wrong: the diff then asks for events already stored, they
        // arrive, and ingest drops them as duplicates. The alternative is a
        // third and fourth gigabyte-scale list on the heap.
        val set = SharedIdSet()
        val hour = 3_600_000L
        set.install(ids(100), nowMs = 0, forSince = null, buildMs = 60_000)
        val tenHourLeg = set.lease()

        // Hour 1: a rebuild lands. Everyone else moves to the new set.
        assertTrue(set.worthRebuilding(hour, null))
        assertTrue(set.mayInstall())
        set.install(ids(101), nowMs = hour, forSince = null, buildMs = 60_000)
        assertEquals(101, set.size())

        // Hours 2-10: worth rebuilding every time, and refused every time.
        for (h in 2..10) {
            assertTrue(set.worthRebuilding(h * hour, null), "the clock says yes at hour $h")
            assertFalse(set.mayInstall(), "but the straggler still holds the retired generation at hour $h")
            assertEquals(2, set.generationsAlive(), "never a third, which is the whole bound")
        }
        assertEquals(100, tenHourLeg.ids.size, "and it keeps reading the set it started against")

        tenHourLeg.release()
        assertTrue(set.mayInstall(), "rebuilds resume the moment it finishes")
    }

    @Test
    fun `a stream that never builds one leases nothing rather than failing`() {
        // Fetch-only and deleteMissing streams never install. Their asks still
        // take a lease, and it has to be an empty read rather than a null the
        // call site has to remember to check.
        val set = SharedIdSet()
        assertTrue(set.isEmpty())
        val lease = set.lease()
        assertEquals(emptyList(), lease.ids)
        lease.release()
        assertEquals(0, set.generationsAlive())
    }
}
