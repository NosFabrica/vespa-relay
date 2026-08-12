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

import kotlin.test.Test
import kotlin.test.assertEquals
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
