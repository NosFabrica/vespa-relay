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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** What the shrink guard believes, per stream and peer, and that it forgets what the roster dropped. */
class ShrinkGuardTest {
    private val kept = ShrinkGuard.Key("scores", RelayUrlNormalizer.normalize("wss://kept.example"), Filter(kinds = listOf(30382)))
    private val gone = ShrinkGuard.Key("scores", RelayUrlNormalizer.normalize("wss://gone.example"), Filter(kinds = listOf(30382)))

    @Test
    fun `a set that fell past a tenth of the last believed one is not acted on`() {
        val guard = ShrinkGuard()
        assertTrue(guard.mayDelete(kept, mine = 100, peerHeld = 100))
        assertFalse(guard.mayDelete(kept, mine = 100, peerHeld = 9))
        assertTrue(guard.mayDelete(kept, mine = 100, peerHeld = 95), "the old baseline stood through the suspect answer")
    }

    @Test
    fun `retain forgets the baselines of units off the roster`() {
        val guard = ShrinkGuard()
        guard.mayPush(kept, mine = 100, peerHeld = 100, mayBeLensed = false)
        guard.mayPush(gone, mine = 100, peerHeld = 100, mayBeLensed = false)
        assertEquals(2, guard.baselines)
        guard.retain { it.url == kept.url }
        assertEquals(1, guard.baselines)
        assertFalse(guard.mayPush(kept, mine = 100, peerHeld = 5, mayBeLensed = false), "a kept unit still judges by its baseline")
        assertTrue(guard.mayPush(gone, mine = 100, peerHeld = 5, mayBeLensed = false), "a forgotten one starts over")
    }
}
