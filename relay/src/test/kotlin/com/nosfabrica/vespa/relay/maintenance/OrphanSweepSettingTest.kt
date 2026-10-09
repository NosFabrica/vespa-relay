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
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** SWEEP_ORPHAN_SCORES_ON_START deletes data, so only the exact word `true` may delete. */
class OrphanSweepSettingTest {
    @Test
    fun `unset or blank runs no sweep`() {
        assertNull(orphanSweepDryRun(null))
        assertNull(orphanSweepDryRun(""))
        assertNull(orphanSweepDryRun("  "))
    }

    @Test
    fun `exactly true deletes`() {
        assertEquals(false, orphanSweepDryRun("true"))
        assertEquals(false, orphanSweepDryRun(" true "))
    }

    @Test
    fun `any other value is a dry run and never stops the boot`() {
        for (value in listOf("1", "yes", "on", "TRUE", "True", "dryRun", "false", "0", "no")) {
            assertEquals(true, orphanSweepDryRun(value), value)
        }
    }
}
