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
        c.record(relay, profiles, observedMin = 1_700_001_000L, observedMax = 1_700_002_000L, paged = true)

        val legs = c.legs(relay, profiles)
        assertEquals(2, legs.size, "one leg older than the band, one newer")
        assertEquals(1_700_001_000L, legs[0].until, "older leg stops AT the band's floor")
        assertNull(legs[0].since, "and reaches as far back as the filter allows")
        assertEquals(1_700_002_000L, legs[1].since, "newer leg starts AT its ceiling")
        assertNull(legs[1].until)
    }

    @Test
    fun `an event sharing the band's boundary second is still reachable`() {
        // A paged relay cuts pages by count, so a boundary can fall inside a run
        // of events sharing one created_at. Excluding the edge would strand the
        // rest of that second in no leg at all, while the band called it covered.
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        val legs = c.legs(relay, profiles)

        fun reachable(t: Long) = legs.any { (it.since ?: Long.MIN_VALUE) <= t && t <= (it.until ?: Long.MAX_VALUE) }

        assertTrue(reachable(1_700_001_000L), "the band's own floor second must be re-read")
        assertTrue(reachable(1_700_002_000L), "and its ceiling second")
        assertTrue(reachable(1_700_000_999L), "below the band")
        assertTrue(reachable(1_700_002_001L), "above it")
        // Only the interior is skipped, which is the entire point.
        assertTrue(!reachable(1_700_001_500L), "the covered interior is not re-read")
    }

    @Test
    fun `successive runs widen the band rather than replacing it`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        // A later run reaches further back and picks up newer events.
        c.record(relay, profiles, 1_700_000_500L, 1_700_002_500L, paged = true)

        val band = c.band(relay, profiles)!!
        assertEquals(1_700_000_500L, band.minCreatedAt)
        assertEquals(1_700_002_500L, band.maxCreatedAt)
    }

    @Test
    fun `a capped relay walks further back on each run`() {
        // The case that makes this worth having: a relay that only ever answers
        // with its newest N events. Each run starts below the last one's floor.
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_700_009_000L, 1_700_010_000L, paged = true)
        assertEquals(1_700_009_000L, c.legs(relay, profiles)[0].until)

        c.record(relay, profiles, 1_700_008_000L, 1_700_008_999L, paged = true)
        assertEquals(1_700_008_000L, c.legs(relay, profiles)[0].until)
    }

    // ---- when a cursor must not be used ------------------------------------

    @Test
    fun `a negentropy sync records nothing`() {
        // Reconciliation already downloads only the diff; a band could only
        // narrow a future reconciliation for no gain.
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false)
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
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        // Widening the kinds means the old band skipped events it never fetched.
        val wider = Filter(kinds = listOf(0, 10002))
        assertEquals(listOf(wider), c.legs(relay, wider), "a new filter has no band")
        assertNull(c.band(relay, wider))
        // ...and the original is untouched, so reverting resumes where it was.
        assertEquals(1_700_001_000L, c.band(relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `each relay keeps its own band`() {
        val c = SyncCursors(null)
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(listOf(profiles), c.legs(other, profiles))
    }

    // ---- the filter's own bounds still win ---------------------------------

    @Test
    fun `a bounded filter never widens past its own since and until`() {
        val bounded = Filter(kinds = listOf(0), since = 1_700_001_000L, until = 1_700_005_000L)
        val c = SyncCursors(null)
        c.record(relay, bounded, 1_700_002_000L, 1_700_003_000L, paged = true)

        val legs = c.legs(relay, bounded)
        assertEquals(2, legs.size)
        assertEquals(1_700_001_000L, legs[0].since, "the older leg keeps the configured floor")
        assertEquals(1_700_002_000L, legs[0].until)
        assertEquals(1_700_003_000L, legs[1].since)
        assertEquals(1_700_005_000L, legs[1].until, "the newer leg keeps the configured ceiling")
    }

    @Test
    fun `a fully covered bounded filter re-reads only its two edge seconds`() {
        // Inclusive edges mean "covered" can never quite mean "ask for nothing":
        // the two boundary seconds are always re-read, because that is the only
        // way to catch a run of same-second events a page boundary cut in half.
        // Two seconds per cycle is the price of not stranding them.
        val bounded = Filter(kinds = listOf(0), since = 1_700_001_000L, until = 1_700_005_000L)
        val c = SyncCursors(null)
        c.record(relay, bounded, 1_700_001_000L, 1_700_005_000L, paged = true)

        val legs = c.legs(relay, bounded)
        assertEquals(2, legs.size)
        assertEquals(1_700_001_000L to 1_700_001_000L, legs[0].since to legs[0].until, "the floor second only")
        assertEquals(1_700_005_000L to 1_700_005_000L, legs[1].since to legs[1].until, "the ceiling second only")
    }

    @Test
    fun `bands survive a restart`() {
        val f = tempFile()
        SyncCursors(f).apply {
            record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            flush()
        }

        // A fresh instance, as a restart would build.
        val reopened = SyncCursors(f)
        assertEquals(1_700_001_000L, reopened.band(relay, profiles)!!.minCreatedAt)
        assertEquals(1_700_002_000L, reopened.band(relay, profiles)!!.maxCreatedAt)
        assertEquals(1_700_001_000L, reopened.legs(relay, profiles)[0].until)
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
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(1_700_001_000L, c.band(relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `a periodic flush persists progress a hard kill would otherwise lose`() {
        // The milestone flushes are minutes to hours apart; a SIGKILL between
        // them loses every band the run earned, and the next start re-downloads
        // the corpus. That is the cost this class exists to avoid.
        val f = tempFile()
        val c = SyncCursors(f).startPeriodicFlush(intervalSec = 1)
        c.record(relay, profiles, 1_700_000_000, 1_785_000_000, paged = true)

        val deadline = System.currentTimeMillis() + 15_000
        while (!f.isFile && System.currentTimeMillis() < deadline) Thread.sleep(100)
        assertTrue(f.isFile, "the periodic flush should have written it with no milestone reached")

        c.close()
        assertEquals(1_700_000_000L, SyncCursors(f).band(relay, profiles)?.minCreatedAt)
        f.delete()
    }

    @Test
    fun `recording does not write, flushing does`() {
        // A dynamic cycle records once per leg per relay. Writing there would
        // serialize the whole map thousands of times per cycle.
        val f = tempFile()
        val c = SyncCursors(f)
        c.record(relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertTrue(!f.exists(), "record() must not touch the file")

        c.flush()
        assertTrue(f.isFile, "flush() writes it")

        // And a second flush with nothing new does not rewrite.
        val stamp = f.lastModified()
        c.flush()
        assertEquals(stamp, f.lastModified(), "a clean flush is a no-op")
        f.delete()
    }

    @Test
    fun `the same filter instance is fingerprinted once`() {
        // Filter.toJson() runs to tens of thousands of characters for an
        // author-scoped filter, and the fan-out keys once per relay per cycle.
        val big = Filter(kinds = listOf(30382), authors = (1..500).map { "%064x".format(it) })
        val c = SyncCursors(null)
        c.record(relay, big, 1_700_001_000L, 1_700_002_000L, paged = true)

        // Same instance, many lookups: still one band, and cheap.
        repeat(50) { c.legs(relay, big) }
        assertEquals(1_700_001_000L, c.band(relay, big)!!.minCreatedAt)

        // An equal-but-distinct instance keys the same way; it just misses the cache.
        val copy = Filter(kinds = listOf(30382), authors = (1..500).map { "%064x".format(it) })
        assertEquals(1_700_001_000L, c.band(relay, copy)?.minCreatedAt, "identity caching must not change the key")
    }

    @Test
    fun `fromEnv is off unless a path is given`() {
        assertEquals(0, SyncCursors.fromEnv(emptyMap()).size())
        assertEquals(0, SyncCursors.fromEnv(mapOf("ROUTER_SYNC_STATE_FILE" to "  ")).size())
    }
}
