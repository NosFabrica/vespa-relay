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
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/** State for a unit off the roster is forgotten, but only once it has stayed off for the grace period. */
class RetentionTest {
    private val kept = RelayUrlNormalizer.normalize("wss://kept.example")
    private val left = RelayUrlNormalizer.normalize("wss://left.example")
    private val notes = Filter(kinds = listOf(1))
    private val ttl = UnownedClock.UNOWNED_TTL_SECONDS

    private fun now() = System.currentTimeMillis() / 1000

    private fun owned(vararg urls: String) = urls.mapTo(HashSet()) { SyncBands.Held("s", notes.toJson(), it) }

    /** Both relays walked and audited under stream `s`. */
    private fun held(file: File? = null): SyncBands =
        SyncBands(file).apply {
            for (url in listOf(kept, left)) {
                record("s", url, notes, now() - 600, now() - 60, paged = true)
                record("s", url, notes, null, null, paged = false, reconciledThrough = now() - 60)
            }
        }

    @Test
    fun `a unit off the roster keeps its state through the grace period and loses it after`() {
        val bands = held()
        val t0 = now()
        assertEquals(0, bands.retain(owned(kept.url), t0), "the first rebuild without it only starts the clock")
        assertEquals(0, bands.retain(owned(kept.url), t0 + ttl - 1))
        assertNotNull(bands.band("s", left, notes))

        bands.retain(owned(kept.url), t0 + ttl)
        assertNull(bands.band("s", left, notes), "its bands went")
        assertNull(bands.verifiedAt("s", left, notes), "and its audit clock")
        assertNotNull(bands.band("s", kept, notes), "the owned unit kept everything")
        assertNotNull(bands.verifiedAt("s", kept, notes))
    }

    @Test
    fun `a unit that comes back restarts its clock`() {
        val bands = held()
        val t0 = now()
        bands.retain(owned(kept.url), t0)
        bands.retain(owned(kept.url, left.url), t0 + ttl / 2)
        bands.retain(owned(kept.url), t0 + ttl / 2 + 1)
        bands.retain(owned(kept.url), t0 + ttl)
        assertNotNull(bands.band("s", left, notes), "it was away for half the period twice, never the whole of it")
    }

    @Test
    fun `an empty roster forgets nothing however long it lasts`() {
        val bands = held()
        val t0 = now()
        bands.retain(emptySet(), t0)
        bands.retain(emptySet(), t0 + 2 * ttl)
        assertNotNull(bands.band("s", kept, notes))
        assertNotNull(bands.band("s", left, notes))
    }

    @Test
    fun `a roster that suddenly shrinks is not believed for a rebuild`() {
        val bands = held()
        val t0 = now()
        val wide = owned(kept.url, left.url, *Array(8) { "wss://other$it.example/" })
        bands.retain(wide, t0)
        // A fifth of the last roster: read as a failed rebuild, so no clock starts here.
        bands.retain(owned(kept.url, "wss://other0.example/"), t0 + 1)
        bands.retain(owned(kept.url, "wss://other0.example/"), t0 + ttl)
        assertNotNull(bands.band("s", left, notes), "its clock started only once the shrink was believed")
        bands.retain(owned(kept.url, "wss://other0.example/"), t0 + 2 * ttl)
        assertNull(bands.band("s", left, notes), "and runs out like any other")
    }

    @Test
    fun `the clock survives a restart`() {
        val f = File.createTempFile("bands", ".json").also { it.delete() }
        try {
            val t0 = now()
            held(f).apply {
                retain(owned(kept.url), t0)
                flush()
            }
            val reopened = SyncBands(f)
            reopened.retain(owned(kept.url), t0 + ttl)
            assertNull(reopened.band("s", left, notes), "a restart does not hand an orphan a fresh grace period")
            assertNotNull(reopened.band("s", kept, notes))
        } finally {
            f.delete()
        }
    }

    @Test
    fun `sweep state forgets unusable cursors and long-gone peers`() {
        val sweeps = SweepState(null, staleAfterSeconds = 3600)
        sweeps.advance(SweepState.keyFor("s", left, notes), 100, 200)
        sweeps.setTarget(kept, 5_000)
        sweeps.setTarget(left, 5_000)

        val t0 = now()
        sweeps.retain(setOf(kept.url), t0 + 3601)
        assertEquals(0, sweeps.size(), "a cursor past its resume age is gone")
        assertNotNull(sweeps.peer(left), "a peer off the roster keeps its learned size through the grace period")

        sweeps.retain(setOf(kept.url), t0 + 3601 + ttl)
        assertNull(sweeps.peer(left))
        assertNotNull(sweeps.peer(kept))
    }
}
