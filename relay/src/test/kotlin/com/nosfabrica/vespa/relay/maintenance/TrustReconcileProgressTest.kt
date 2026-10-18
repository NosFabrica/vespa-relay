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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TestTimeSource

/** The reconcile's progress line, what it retries, and its refusal to quote a percentage that cannot move. */
class TrustReconcileProgressTest {
    @Test
    fun `screening reports a fraction and an eta`() {
        val line = reconcileProgressLine(inspected = 250, total = 1000, rebuilt = 0, applied = 0, elapsedMs = 50_000)
        assertContains(line, "250/1000")
        assertContains(line, "(25%)")
        assertTrue(line.contains("eta 2m"), "750 left at 5/s is 150s: $line")
    }

    /** Past screening the denominator stops moving, so a percentage would sit at 100 for the expensive half. */
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

    /** One throttle for the whole walk: a reporter rebuilt per attempt restarts its window and never speaks. */
    @Test
    fun `one progress instance emits across the whole walk, not once per attempt`() {
        val out = mutableListOf<String>()
        var clock = 0L
        val fn = reconcileProgress(everyMillis = 30_000, now = { clock }, emit = { out += it })

        fn(1, 100, 0, 0)
        assertTrue(out.isEmpty(), "nothing at t=0")
        clock = 29_000
        fn(2, 100, 0, 0)
        assertTrue(out.isEmpty(), "still inside the window")
        clock = 31_000
        fn(30, 100, 0, 0)
        assertEquals(1, out.size, "past the window it speaks")
        clock = 95_000
        fn(60, 100, 0, 0)
        assertEquals(2, out.size, "and keeps speaking as the walk goes on")
        assertContains(out[1], "60/100")
    }

    /** A loop that prints only its first failure cannot be told from one that stopped failing. */
    @Test
    fun `retries stay audible after the first failure`() {
        val out = mutableListOf<String>()
        var clock = 1_000L
        val report = reconcileFailures(everyMillis = 30_000, now = { clock }, emit = { out += it })

        assertTrue(report(1, IllegalStateException("cold engine")), "the first failure earns the stack")
        assertEquals(1, out.size)
        assertContains(out[0], "engine not answering yet")

        clock = 10_000
        assertFalse(report(2, IllegalStateException("cold engine")), "only the first is the first")
        assertEquals(1, out.size, "throttled inside the window")

        clock = 40_000
        report(47, IllegalStateException("cold engine"))
        assertEquals(2, out.size, "a loop still retrying says so")
        assertContains(out[1], "attempt 47")
    }

    /** A JVM error that leaves the process sound must not end the retry loop silently. */
    @Test
    fun `every failure but cancellation and a failing JVM is retried`() {
        for (thrown in listOf(IllegalStateException("cold"), NoClassDefFoundError("x"), StackOverflowError(), AssertionError("x"))) {
            assertTrue(retryable<Unit> { throw thrown }.isFailure, "$thrown ended the retry loop")
        }
        assertFailsWith<CancellationException> { retryable<Unit> { throw CancellationException("stop") } }
        assertFailsWith<OutOfMemoryError> { retryable<Unit> { throw OutOfMemoryError() } }
        assertFailsWith<InternalError> { retryable<Unit> { throw InternalError() } }
        assertEquals(3, retryable { 3 }.getOrNull())
    }

    /** An attempt that hangs for minutes before failing must spend the budget, not only the sleeps between. */
    @Test
    fun `the wait budget counts the time each attempt takes`(): Unit =
        runBlocking {
            val clock = TestTimeSource()
            var attempts = 0
            reconcileWithRetry(time = clock, retryMs = 1, maxWaitMs = 10 * 60_000L, log = {}) {
                attempts++
                clock += 4.minutes
                throw IllegalStateException("engine timed out")
            }
            assertEquals(3, attempts, "four minutes per attempt spends a ten-minute budget on the third")
        }

    @Test
    fun `zero providers is retried within the same deadline, then accepted`(): Unit =
        runBlocking {
            val clock = TestTimeSource()
            val out = mutableListOf<String>()
            var attempts = 0
            reconcileWithRetry(time = clock, retryMs = 1, maxWaitMs = 10 * 60_000L, log = { out += it }) {
                attempts++
                clock += 3.minutes
                ReconcileOutcome(services = 0, rebuilt = 0, clean = true)
            }
            assertEquals(4, attempts)
            assertContains(out.last(), "still no provider lists after 720s")
        }

    @Test
    fun `an answer with providers ends the loop on the attempt that found them`(): Unit =
        runBlocking {
            var attempts = 0
            val out = mutableListOf<String>()
            reconcileWithRetry(time = TestTimeSource(), retryMs = 1, log = { out += it }) {
                attempts++
                if (attempts < 3) ReconcileOutcome(0, 0, true) else ReconcileOutcome(5, 2, false)
            }
            assertEquals(3, attempts)
            assertContains(out.last(), "re-derived 2 of 5")
        }
}
