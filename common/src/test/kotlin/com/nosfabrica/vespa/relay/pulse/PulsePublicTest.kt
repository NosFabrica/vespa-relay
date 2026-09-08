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
package com.nosfabrica.vespa.relay.pulse

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A public pulse is the OPERATIONAL half only. "Public" and "publishes what
 * people searched for" must never be one forgotten variable apart, so the pair
 * is refused at boot rather than resolved silently in either direction.
 */
class PulsePublicTest {
    @Test
    fun `public with client detail is refused, and the message says which to turn off`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                pulsePublic(mapOf("PULSE_PUBLIC" to "true"), clientDerived = true)
            }
        assertTrue("PULSE_CLIENT_DETAIL" in e.message.orEmpty(), "the operator must be told which half is the problem: ${e.message}")
        assertTrue("searched for" in e.message.orEmpty(), "and why it matters: ${e.message}")
    }

    @Test
    fun `public with the operational half alone is allowed`() {
        assertTrue(pulsePublic(mapOf("PULSE_PUBLIC" to "true"), clientDerived = false))
    }

    @Test
    fun `client detail alone stays allowed — it is gated, not public`() {
        assertEquals(false, pulsePublic(mapOf("PULSE_CLIENT_DETAIL" to "true"), clientDerived = true))
    }

    @Test
    fun `unset is not public`() {
        assertEquals(false, pulsePublic(emptyMap(), clientDerived = false))
    }

    /**
     * The gate's own rule is unchanged for everyone else: no administrators and
     * not public is a boot that stops, never a document served to anyone.
     */
    @Test
    fun `an empty admin list still stops the boot unless public was declared`() {
        assertFailsWith<IllegalStateException> { pulseAdmins(emptySet(), "PULSE_PORT") }
        assertEquals(emptySet(), pulseAdmins(emptySet(), "PULSE_PORT", public = true))
    }
}
