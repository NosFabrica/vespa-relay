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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * When a NIP-85 service's scores are gone, its kind 0 and 10002 go with them —
 * and at no other time. Every case below is a shape a real reconcile returns;
 * a `true` in the wrong row deletes a working provider's profile.
 */
class DeleteMissingCascadeTest {
    @Test
    fun `only a wholesale retraction cascades`() {
        // We held 40 scores, the provider's own relay now serves none of them
        // and offers nothing in their place. That is the case the cascade
        // exists for.
        assertTrue(DeleteMissingSync.retracts(mine = 40, need = 0, have = 40, windows = 3))

        // The ordinary republish, and the reason `need` is in the test at all:
        // an addressable score REPLACED gets a new id, so the old ids come
        // back as ours-only. Same `have == mine`, entirely different event.
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 40, have = 40, windows = 3))
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 1, have = 40, windows = 3))

        // Retracting some subjects is routine — a scammer scored and dropped.
        // The service is alive and keeps its profile.
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 0, have = 39, windows = 3))
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 0, have = 1, windows = 3))
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 0, have = 0, windows = 3))
    }

    @Test
    fun `a service we never held scores for has retracted nothing`() {
        // A newly announced service, or one whose relay is a wrong pointer:
        // we hold nothing, it serves nothing, and the two agree. Agreement on
        // emptiness is not a withdrawal, and this is the shape that would
        // otherwise delete the profile of every service on its first cycle.
        assertFalse(DeleteMissingSync.retracts(mine = 0, need = 0, have = 0, windows = 3))
        assertFalse(DeleteMissingSync.retracts(mine = 0, need = 0, have = 0, windows = 1))
    }

    @Test
    fun `a reconcile that compared no range decides nothing`() {
        // Zero windows means nothing was compared, so "the relay has none of
        // ours" is an artefact of the reconcile, not an answer from the relay.
        assertFalse(DeleteMissingSync.retracts(mine = 40, need = 0, have = 40, windows = 0))
        assertTrue(DeleteMissingSync.retracts(mine = 40, need = 0, have = 40, windows = 1))
    }
}
