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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Reading a relay's sentence after a walk: quartz dispatches a `CLOSED` to subscription
 * listeners before connection listeners, so the plain read can be a hop too early.
 */
class RelayComplaintsTest {
    private val url = RelayUrlNormalizer.normalize("wss://purplerelay.com")

    /** A relay whose sentence lands on the [lateBy]-th read. */
    private class LateComplaint(
        private val text: String,
        private val lateBy: Int,
    ) : RelayComplaints {
        val reads = AtomicInteger()

        override fun since(
            url: NormalizedRelayUrl,
            sinceMs: Long,
        ): String? = if (reads.incrementAndGet() > lateBy) text else null
    }

    @Test
    fun `a sentence that has not landed yet is waited for, not missed`() =
        runBlocking {
            val said = "ERROR: bad req: filter validation failed: too many kinds in filter: 69"
            val relay = LateComplaint(said, lateBy = 3)
            // The plain read is null here, which `FilterWidths.learn` would take as no width refusal.
            assertNull(relay.since(url, 0))
            assertEquals(said, relay.awaitSince(url, 0), "the grace has to cover a listener that has not run yet")
        }

    @Test
    fun `a relay that genuinely said nothing costs the grace and no more`() =
        runBlocking {
            val relay = LateComplaint("never", lateBy = Int.MAX_VALUE)
            val startedMs = System.currentTimeMillis()
            assertNull(relay.awaitSince(url, 0, graceMs = 100))
            val took = System.currentTimeMillis() - startedMs
            kotlin.test.assertTrue(took in 100..2_000, "waited ${took}ms for a grace of 100ms")
        }

    @Test
    fun `a sentence already in hand costs nothing`() =
        runBlocking {
            val relay = LateComplaint("said at once", lateBy = 0)
            val startedMs = System.currentTimeMillis()
            assertEquals("said at once", relay.awaitSince(url, 0))
            kotlin.test.assertTrue(System.currentTimeMillis() - startedMs < 100, "the fast path must not sleep")
            assertEquals(1, relay.reads.get(), "…and must not poll either")
        }

    @Test
    fun `the deaf one hears nothing however long it is given`() =
        runBlocking {
            assertNull(RelayComplaints.DEAF.since(url, 0))
            assertNull(RelayComplaints.DEAF.awaitSince(url, 0, graceMs = 20))
        }
}
