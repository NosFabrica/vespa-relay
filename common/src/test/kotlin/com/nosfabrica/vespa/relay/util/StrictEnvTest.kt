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
package com.nosfabrica.vespa.relay.util

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A setting is unset, or exactly what it says; never quietly the default. */
class StrictEnvTest {
    @Test
    fun `unset and blank leave the default to the caller`() {
        val env = mapOf("BLANK" to "  ")
        assertNull(env.strictInt("UNSET"))
        assertNull(env.strictLong("BLANK"))
        assertNull(env.strictFlag("BLANK"))
        assertNull(env.strictChoice("BLANK", setOf("a")))
    }

    @Test
    fun `a value that parses inside its range is read as written`() {
        val env = mapOf("N" to " 42 ", "F" to "Off", "T" to "yes", "C" to "Full")
        assertEquals(42, env.strictInt("N", 1..100))
        assertEquals(42L, env.strictLong("N", 1L..100L))
        assertEquals(false, env.strictFlag("F"))
        assertEquals(true, env.strictFlag("T"))
        assertEquals("full", env.strictChoice("C", setOf("sent", "full")))
    }

    @Test
    fun `a typo or an out-of-range value stops the boot and names the variable`() {
        val env = mapOf("N" to "80a", "LOW" to "3", "F" to "ture", "C" to "ful")
        assertTrue("N='80a'" in assertFailsWith<IllegalStateException> { env.strictInt("N") }.message.orEmpty())
        assertTrue("at least 5" in assertFailsWith<IllegalStateException> { env.strictLong("LOW", 5L..Long.MAX_VALUE) }.message.orEmpty())
        assertTrue("in 5..9" in assertFailsWith<IllegalStateException> { env.strictInt("LOW", 5..9) }.message.orEmpty())
        assertTrue("F='ture'" in assertFailsWith<IllegalStateException> { env.strictFlag("F") }.message.orEmpty())
        assertTrue("C='ful'" in assertFailsWith<IllegalStateException> { env.strictChoice("C", setOf("sent", "full")) }.message.orEmpty())
    }
}
