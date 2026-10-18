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
package com.nosfabrica.vespa.relay.ingest

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A probe the store keeps refusing stops costing every batch a round trip, and comes back once it answers. */
class ProbeTroubleTest {
    @Test
    fun `a failing probe pauses for longer each time, up to a ceiling, and a success ends the pauses`() {
        var clock = 0L
        val trouble = ProbeTrouble("id", firstPauseMs = 1_000, maxPauseMs = 4_000, now = { clock })
        val boom = IllegalStateException("store down")

        assertTrue(trouble.ready())
        trouble.failed(boom)
        assertFalse(trouble.ready(), "the batch right after a failure goes unprobed")
        clock = 1_000
        assertTrue(trouble.ready())

        trouble.failed(boom)
        clock = 2_999
        assertFalse(trouble.ready(), "the second pause is twice the first")
        clock = 3_000
        trouble.failed(boom)
        trouble.failed(boom)
        clock += 4_000
        assertTrue(trouble.ready(), "the pause stops growing at its ceiling")

        trouble.succeeded()
        trouble.failed(boom)
        clock += 1_000
        assertTrue(trouble.ready(), "an answered probe resets the pause to its first length")
        assertEquals(5, trouble.failures())
    }
}
