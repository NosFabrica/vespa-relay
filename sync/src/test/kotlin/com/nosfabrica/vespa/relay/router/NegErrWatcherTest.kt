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
import kotlin.test.assertNull

/**
 * Reading a peer's cap out of its rejection is free when it works and harmful
 * when it misfires — a number taken from a rate-limit would shrink every window
 * against a relay that has no size limit at all. So these pin both directions:
 * the phrasings that carry a real cap, and the ones that must be ignored.
 */
class NegErrWatcherTest {
    private fun frame(
        reason: String,
        extra: String = "",
    ) = """["NEG-ERR","sub1",${'"'}$reason${'"'}$extra]"""

    @Test
    fun `strfry's comparison carries the cap on the right-hand side`() {
        val reason = "blocked: query matches too many records (2431002 > 1000000)"
        assertEquals(1_000_000, NegErrWatcher.capOf(frame(reason), reason))
    }

    @Test
    fun `a fourth element carries the cap when the prose does not`() {
        val reason = "blocked: too many query results"
        assertEquals(500_000, NegErrWatcher.capOf(frame(reason, ",500000"), reason))
    }

    @Test
    fun `an overflow with no number teaches nothing`() {
        val reason = "blocked: too many query results"
        assertNull(NegErrWatcher.capOf(frame(reason), reason))
    }

    @Test
    fun `a rate limit is not a cap however many numbers it carries`() {
        // The dangerous case: this does not shrink when the window shrinks, so
        // believing it would size every future window against a limit that has
        // nothing to do with result-set size.
        val reason = "rate-limited: too many requests (30 > 10)"
        assertNull(NegErrWatcher.capOf(frame(reason, ",10"), reason))
    }

    @Test
    fun `a refusal that is not about size teaches nothing`() {
        listOf(
            "auth-required: we only serve negentropy to authenticated users",
            "blocked: pubkey is banned",
            "error: negentropy disabled",
        ).forEach { assertNull(NegErrWatcher.capOf(frame(it), it), "learned a cap from: $it") }
    }

    @Test
    fun `a nonsensical cap is refused`() {
        val reason = "blocked: query matches too many records (5 > 0)"
        // A zero cap would wedge the pager at a window that can never fit.
        assertNull(NegErrWatcher.capOf(frame(reason, ",0"), reason))
    }

    @Test
    fun `an unparseable frame is not an error`() {
        val reason = "blocked: too many query results"
        assertNull(NegErrWatcher.capOf("not json at all", reason))
    }
}
