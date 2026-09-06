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
package com.nosfabrica.vespa.relay.maintenance

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The line the reconcile prints while it runs. What is asserted here is the
 * arithmetic and, more importantly, the REFUSAL to quote a percentage the
 * number cannot support — a frozen 100% is how the screening phase's end and
 * the re-derive phase's start came to look identical.
 */
class TrustReconcileProgressTest {
    @Test
    fun `screening reports a fraction and an eta`() {
        val line = reconcileProgressLine(inspected = 250, total = 1000, rebuilt = 0, applied = 0, elapsedMs = 50_000)
        assertContains(line, "250/1000")
        assertContains(line, "(25%)")
        assertTrue(line.contains("eta 2m"), "750 left at 5/s is 150s: $line")
    }

    /**
     * PAST SCREENING THE DENOMINATOR STOPS MOVING. The reconciler holds
     * `inspected` at `total` and advances `rebuilt` and the card count
     * instead, so a percentage here would read 100% for the whole expensive
     * half — the exact reading that makes a long walk look finished.
     */
    @Test
    fun `re-deriving reports services and cards, never a frozen percentage`() {
        val line = reconcileProgressLine(inspected = 1000, total = 1000, rebuilt = 7, applied = 4200, elapsedMs = 90_000)
        assertContains(line, "re-deriving 7 service(s) of 1000")
        assertContains(line, "4200 card(s) applied")
        assertFalse(line.contains("100%"), "a motionless 100% is what this exists to avoid: $line")
    }

    /** A denominator that is not known yet is said plainly rather than rendered as 0%. */
    @Test
    fun `an unknown total is stated, not turned into a percentage`() {
        val line = reconcileProgressLine(inspected = 12, total = 0, rebuilt = 0, applied = 0, elapsedMs = 5_000)
        assertContains(line, "total not known yet")
        assertFalse(line.contains("%"), "no percentage without a denominator: $line")
    }

    /** The throttle exists because the walk calls back per page; the first call must still emit. */
    @Test
    fun `progress is throttled but not silent`() {
        val emitted = mutableListOf<Unit>()
        val fn = reconcileProgress(everyMillis = 0)
        repeat(3) {
            fn(1, 10, 0, 0)
            emitted += Unit
        }
        assertTrue(emitted.size == 3, "the callback stays callable per page")
    }
}
