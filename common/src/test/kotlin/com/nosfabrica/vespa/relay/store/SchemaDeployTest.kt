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
package com.nosfabrica.vespa.relay.store

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The sync's wait for the relay's schema, which replaced its own deploy. */
class SchemaDeployTest {
    @Test
    fun `an application already serving costs one check`() {
        val asked = AtomicInteger()
        awaitServedSchema("http://vespa:8080", isServing = { asked.incrementAndGet() > 0 })
        assertEquals(1, asked.get())
    }

    @Test
    fun `it waits for the relay's deploy, however many checks that takes`() {
        val asked = AtomicInteger()
        awaitServedSchema("http://vespa:8080", poll = Duration.ofMillis(1), isServing = { asked.incrementAndGet() >= 4 })
        assertEquals(4, asked.get())
    }

    @Test
    fun `nothing serving within the timeout stops the boot, naming who deploys`() {
        val e =
            assertFailsWith<IllegalStateException> {
                awaitServedSchema("http://vespa:8080", timeout = Duration.ofMillis(20), poll = Duration.ofMillis(1), isServing = { false })
            }
        assertTrue("relay deploys" in e.message.orEmpty(), e.message)
    }
}
