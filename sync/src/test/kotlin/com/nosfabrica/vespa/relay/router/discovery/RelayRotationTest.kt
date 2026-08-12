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
package com.nosfabrica.vespa.relay.router.discovery

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What overlapping passes must never do: hand the same relay to two workers.
 *
 * A pass ends when its last url is handed out rather than when its last worker
 * returns, so a relay slower than a pass is still being synced when the next one
 * reaches it. Dialling it again would share a socket, race on one cursor band,
 * and spend two of the pool's slots on one relay.
 */
class RelayRotationTest {
    private val a = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://a.example"))
    private val b = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://b.example"))
    private val c = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://c.example"))

    @Test
    fun `a pass hands out everything nothing is holding`() {
        val rotation = RelayRotation()
        val pass = rotation.beginPass(listOf(a, b, c))

        assertEquals(listOf(a, b, c), pass.relays)
        assertEquals(0, pass.busy)
        assertEquals(1L, rotation.pass(), "the first pass is 1, so a log line can name it")
    }

    @Test
    fun `a relay still syncing is passed over, and counted`() {
        // Counted, not merely skipped: "still going from last time" and "never
        // reached" are the same silence otherwise, and one of them is the
        // rotation working while the other is a fan-out that stopped.
        val rotation = RelayRotation()
        assertTrue(rotation.take(b.url))

        val pass = rotation.beginPass(listOf(a, b, c))

        assertEquals(listOf(a, c), pass.relays)
        assertEquals(1, pass.busy)
    }

    @Test
    fun `it comes back on the pass after it finishes`() {
        val rotation = RelayRotation()
        rotation.take(b.url)
        rotation.beginPass(listOf(a, b, c))
        rotation.release(b.url)

        assertEquals(listOf(a, b, c), rotation.beginPass(listOf(a, b, c)).relays)
    }

    @Test
    fun `taking is the claim, and only one taker wins`() {
        val rotation = RelayRotation()

        assertTrue(rotation.take(a.url))
        assertFalse(rotation.take(a.url), "two workers would share a socket and race on one band")
        assertEquals(1, rotation.busyCount())

        rotation.release(a.url)
        assertTrue(rotation.take(a.url))
    }

    @Test
    fun `a relay freed mid-pass waits for the next one`() {
        // The pass list is a snapshot on purpose. Re-testing as the walk goes
        // would let a fast relay be dialled repeatedly within one pass while the
        // list behind it queues, which is the opposite of a rotation.
        val rotation = RelayRotation()
        rotation.take(a.url)
        val pass = rotation.beginPass(listOf(a, b, c))
        rotation.release(a.url)

        assertEquals(listOf(b, c), pass.relays, "the pass already decided what it was handing out")
    }

    @Test
    fun `busy count is what tells a working rotation from a stopped one`() {
        val rotation = RelayRotation()
        assertEquals(0, rotation.busyCount())

        rotation.take(a.url)
        rotation.take(b.url)
        assertEquals(2, rotation.busyCount())
        assertEquals(setOf(a.url, b.url), rotation.busyUrls().toSet())

        rotation.release(a.url)
        assertEquals(1, rotation.busyCount())
    }
}
