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
        set.install(ids(3))

        val lease = set.lease()
        assertEquals(3, lease.ids.size)
        lease.release()

        // Nothing is reading the outgoing generation, so it is garbage the
        // moment it is replaced and never occupies the retirement slot.
        assertTrue(set.mayInstall())
        set.install(ids(5))
        assertEquals(1, set.generationsAlive())
        assertEquals(5, set.size())
    }

    @Test
    fun `a straggler holds its own generation, and the next install is refused`() {
        val set = SharedIdSet()
        set.install(ids(3))
        val straggler = set.lease()

        // The pass ends, a new one builds a set. The straggler is still
        // reconciling against the old one, so it stays alive for it — and
        // nothing may be built on top of that.
        set.install(ids(5))
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
        set.install(ids(3))
        val a = set.lease()
        val b = set.lease()
        set.install(ids(5))

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
        set.install(ids(3))
        val a = set.lease()
        val b = set.lease()
        set.install(ids(5))

        a.release()
        a.release()
        assertFalse(set.mayInstall(), "the second release freed a generation b is still reading")
        b.release()
        assertTrue(set.mayInstall())
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
