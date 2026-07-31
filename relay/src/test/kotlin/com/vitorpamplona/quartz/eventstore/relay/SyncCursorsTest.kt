/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A paged relay has no memory of what it already sent, so without a cursor every
 * restart re-downloads its whole corpus. These pin the band arithmetic and, more
 * importantly, the cases where a cursor must NOT be used — a stale band silently
 * skips events, which is a worse failure than re-reading them.
 */
class SyncCursorsTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val profiles = Filter(kinds = listOf(0))

    private fun tempFile(): File {
        val f = File.createTempFile("sync-cursors", ".json")
        f.delete()
        return f
    }

    // ---- the band arithmetic ----------------------------------------------

    @Test
    fun `with nothing recorded the whole filter is fetched`() {
        val c = SyncCursors(null)
        assertEquals(listOf(profiles), c.legs(relay, profiles))
    }

    @Test
    fun `a recorded band is fetched around, not through`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, observedMin = 1_000, observedMax = 2_000, paged = true)

        val legs = c.legs(relay, profiles)
        assertEquals(2, legs.size, "one leg older than the band, one newer")
        assertEquals(999L, legs[0].until, "older leg stops just before the band")
        assertNull(legs[0].since, "and reaches as far back as the filter allows")
        assertEquals(2_001L, legs[1].since, "newer leg starts just after it")
        assertNull(legs[1].until)
    }

    @Test
    fun `successive runs widen the band rather than replacing it`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_000, 2_000, paged = true)
        // A later run reaches further back and picks up newer events.
        c.record(relay, profiles, 500, 2_500, paged = true)

        val band = c.band(relay, profiles)!!
        assertEquals(500L, band.minCreatedAt)
        assertEquals(2_500L, band.maxCreatedAt)
    }

    @Test
    fun `a capped relay walks further back on each run`() {
        // The case that makes this worth having: a relay that only ever answers
        // with its newest N events. Each run starts below the last one's floor.
        val c = SyncCursors(null)
        c.record(relay, profiles, 9_000, 10_000, paged = true)
        assertEquals(8_999L, c.legs(relay, profiles)[0].until)

        c.record(relay, profiles, 8_000, 8_999, paged = true)
        assertEquals(7_999L, c.legs(relay, profiles)[0].until)
    }

    // ---- when a cursor must not be used ------------------------------------

    @Test
    fun `a negentropy sync records nothing`() {
        // Reconciliation already downloads only the diff; a band could only
        // narrow a future reconciliation for no gain.
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_000, 2_000, paged = false)
        assertNull(c.band(relay, profiles))
        assertEquals(listOf(profiles), c.legs(relay, profiles))
    }

    @Test
    fun `an empty fetch records nothing`() {
        // No events says nothing about what the relay holds, only that this
        // window was empty — recording it would fabricate coverage.
        val c = SyncCursors(null)
        c.record(relay, profiles, null, null, paged = true)
        assertNull(c.band(relay, profiles))
    }

    @Test
    fun `changing the filter starts over`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_000, 2_000, paged = true)

        // Widening the kinds means the old band skipped events it never fetched.
        val wider = Filter(kinds = listOf(0, 10002))
        assertEquals(listOf(wider), c.legs(relay, wider), "a new filter has no band")
        assertNull(c.band(relay, wider))
        // ...and the original is untouched, so reverting resumes where it was.
        assertEquals(1_000L, c.band(relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `each relay keeps its own band`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_000, 2_000, paged = true)
        assertEquals(listOf(profiles), c.legs(other, profiles))
    }

    // ---- the filter's own bounds still win ---------------------------------

    @Test
    fun `a bounded filter never widens past its own since and until`() {
        val bounded = Filter(kinds = listOf(0), since = 1_000, until = 5_000)
        val c = SyncCursors(null)
        c.record(relay, bounded, 2_000, 3_000, paged = true)

        val legs = c.legs(relay, bounded)
        assertEquals(2, legs.size)
        assertEquals(1_000L, legs[0].since, "the older leg keeps the configured floor")
        assertEquals(1_999L, legs[0].until)
        assertEquals(3_001L, legs[1].since)
        assertEquals(5_000L, legs[1].until, "the newer leg keeps the configured ceiling")
    }

    @Test
    fun `a fully covered bounded filter asks for nothing`() {
        val bounded = Filter(kinds = listOf(0), since = 1_000, until = 5_000)
        val c = SyncCursors(null)
        c.record(relay, bounded, 1_000, 5_000, paged = true)
        assertTrue(c.legs(relay, bounded).isEmpty(), "nothing outside the band is inside the filter")
    }

    // ---- persistence -------------------------------------------------------

    @Test
    fun `bands survive a restart`() {
        val f = tempFile()
        SyncCursors(f).record(relay, profiles, 1_000, 2_000, paged = true)

        // A fresh instance, as a restart would build.
        val reopened = SyncCursors(f)
        assertEquals(1_000L, reopened.band(relay, profiles)!!.minCreatedAt)
        assertEquals(2_000L, reopened.band(relay, profiles)!!.maxCreatedAt)
        assertEquals(999L, reopened.legs(relay, profiles)[0].until)
        f.delete()
    }

    @Test
    fun `a corrupt file starts fresh instead of refusing to start`() {
        val f = tempFile()
        f.writeText("{ this is not json")
        val c = SyncCursors(f)
        assertEquals(0, c.size())
        assertEquals(listOf(profiles), c.legs(relay, profiles), "no band, so fetch everything")
        f.delete()
    }

    @Test
    fun `with no file configured it still works, just not across restarts`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_000, 2_000, paged = true)
        assertEquals(1_000L, c.band(relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `fromEnv is off unless a path is given`() {
        assertEquals(0, SyncCursors.fromEnv(emptyMap()).size())
        assertEquals(0, SyncCursors.fromEnv(mapOf("ROUTER_SYNC_STATE_FILE" to "  ")).size())
    }
}
