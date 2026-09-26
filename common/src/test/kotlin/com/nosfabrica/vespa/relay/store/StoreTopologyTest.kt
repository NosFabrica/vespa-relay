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

import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class StoreTopologyTest {
    @Test
    fun `neither process may skip a deletion probe`() {
        // The relay and the sync process write one index and each feeds the other, so the faster
        // modes are wrong here for a reason no profiler will show.
        assertEquals(
            WriterTopology.SHARED_STRICT,
            STORE_WRITERS,
            "SINGLE_WRITER is false with the sync process running, and SHARED only bounds the window — " +
                "a covered event admitted inside it is stored and served, and nothing repairs it afterwards",
        )
    }

    @Test
    fun `the provider pass refreshes every minute unless told otherwise`() {
        assertEquals(60L, providerRefreshSeconds(emptyMap()), "the window docs/configuration.md promises")
        assertEquals(60L, providerRefreshSeconds(mapOf(PROVIDER_REFRESH_ENV to " ")))
        assertEquals(15L, providerRefreshSeconds(mapOf(PROVIDER_REFRESH_ENV to "15")))
        assertEquals(0L, providerRefreshSeconds(mapOf(PROVIDER_REFRESH_ENV to "0")), "0 is off, not an error")
    }

    @Test
    fun `a provider refresh that does not parse stops the boot`() {
        assertFailsWith<IllegalStateException> { providerRefreshSeconds(mapOf(PROVIDER_REFRESH_ENV to "1m")) }
        assertFailsWith<IllegalStateException> { providerRefreshSeconds(mapOf(PROVIDER_REFRESH_ENV to "-5")) }
    }
}
