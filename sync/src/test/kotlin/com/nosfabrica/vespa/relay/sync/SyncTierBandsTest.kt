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

import com.nosfabrica.vespa.relay.config.SyncTier
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Age-banded schedules: each band on its own clock, so the tail of the corpus is not re-walked
 * at the cadence the last month needs.
 */
class SyncTierBandsTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val profiles = Filter(kinds = listOf(0))
    private val mirror = "profiles"

    private val month = 30L * 86400
    private val year = 365L * 86400
    private val week = 7L * 86400

    /** The schedule this whole structure was asked for. */
    private val tiers =
        listOf(
            SyncTier(thePastSeconds = month, everySeconds = week),
            SyncTier(thePastSeconds = year, everySeconds = month),
            SyncTier(thePastSeconds = null, everySeconds = year),
        )

    private fun tempFile(): File {
        val f = File.createTempFile("sync-tiers", ".json")
        f.delete()
        return f
    }

    // ---- tiling ------------------------------------------------------------

    @Test
    fun `bands tile the past without overlapping or leaving a hole`() {
        val now = 1_800_000_000L
        val windows =
            SyncTier.tile(tiers).map { (_, older, newer) ->
                SyncTier.windowedFilter(profiles, now, older, newer)
            }

        assertNull(windows[0].until, "the youngest band stays open at the top, so arrivals land inside it")
        assertEquals(windows[0].since, windows[1].until, "the month edge is one edge, shared")
        assertEquals(windows[1].since, windows[2].until, "and so is the year edge")
        assertNull(windows[2].since, "the oldest band reaches the corpus floor")
    }

    @Test
    fun `every record falls in exactly one band`() {
        val now = 1_800_000_000L
        val windows = SyncTier.tile(tiers).map { (_, o, n) -> SyncTier.windowedFilter(profiles, now, o, n) }

        // A day inside each band, plus one either side of both interior edges.
        for (age in listOf(0L, 3600L, month - 1, month + 1, year - 1, year + 1, 5 * year)) {
            val at = now - age
            val holders = windows.count { (it.since ?: Long.MIN_VALUE) <= at && at < (it.until ?: Long.MAX_VALUE) }
            assertEquals(1, holders, "a record ${age}s old belongs to exactly one band, not $holders")
        }
    }

    @Test
    fun `a band narrows the ask and never widens it`() {
        val now = 1_800_000_000L
        val bounded = Filter(kinds = listOf(0), since = now - 2 * month, until = now - 3600)
        val (_, older, newer) = SyncTier.tile(tiers)[2] // the 1y+ tail
        val windowed = SyncTier.windowedFilter(bounded, now, older, newer)

        assertEquals(bounded.since, windowed.since, "the tail cannot reach past what the stream asked for")
        assertTrue(windowed.until!! <= bounded.until!!, "nor above its ceiling")
    }

    // ---- per-band clocks ---------------------------------------------------

    @Test
    fun `verifying one band leaves the others due`() {
        val c = SyncBands(null)
        val now = 1_800_000_000L
        val ids = tiers.map { SyncTier.bandIdOf(tiers, it) }

        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now, band = ids[0])

        assertEquals(now + week, c.auditDueAt(mirror, relay, profiles, week, ids[0]), "the walked band waits its own period")
        assertNull(c.auditDueAt(mirror, relay, profiles, month, ids[1]), "the year band is untouched and still due")
        assertNull(c.auditDueAt(mirror, relay, profiles, year, ids[2]), "so is the tail")
    }

    @Test
    fun `the weekly band comes due while the yearly one is still waiting`() {
        val c = SyncBands(null)
        val now = 1_800_000_000L
        val ids = tiers.map { SyncTier.bandIdOf(tiers, it) }
        tiers.forEachIndexed { i, _ ->
            c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now, band = ids[i])
        }

        val aWeekOn = now + week + 1
        assertTrue(c.auditDueAt(mirror, relay, profiles, week, ids[0])!! <= aWeekOn, "the last month is re-checked weekly")
        assertTrue(c.auditDueAt(mirror, relay, profiles, month, ids[1])!! > aWeekOn, "the year band is not")
        assertTrue(c.auditDueAt(mirror, relay, profiles, year, ids[2])!! > aWeekOn, "and the tail is left alone")
    }

    @Test
    fun `a band claimed is a band the next visit will not re-claim`() {
        val c = SyncBands(null)
        val now = 1_800_000_000L
        val band = SyncTier.bandIdOf(tiers, tiers[2])

        assertTrue(c.claimAudit(mirror, relay, profiles, year, now, band), "never walked, so due")
        assertFalse(c.claimAudit(mirror, relay, profiles, year, now + 60, band), "and not re-claimed inside the attempt spacing")
        assertTrue(
            c.claimAudit(mirror, relay, profiles, week, now, SyncTier.bandIdOf(tiers, tiers[0])),
            "a different band's attempt clock is its own",
        )
    }

    // ---- persistence -------------------------------------------------------

    @Test
    fun `band clocks survive a restart`() {
        // Without this the oldest, most expensive band would re-walk on every restart: its
        // coverage is the stream's whole filter, so there is no band object to ride in.
        val f = tempFile()
        val now = 1_800_000_000L
        val band = SyncTier.bandIdOf(tiers, tiers[2])
        SyncBands(f).apply {
            record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now, band = band)
            flush()
        }

        val reopened = SyncBands(f)
        assertEquals(now, reopened.verifiedAt(mirror, relay, profiles, band), "the tail's clock came back")
        assertEquals(now + year, reopened.auditDueAt(mirror, relay, profiles, year, band))
        assertNull(reopened.verifiedAt(mirror, relay, profiles, SyncTier.bandIdOf(tiers, tiers[0])), "and only that band's")
        f.delete()
    }

    @Test
    fun `an unbanded stream keeps the key it has always used`() {
        // A bare `negentropySyncThePastSeconds` resolves to one unbounded band. Turning tiers on
        // for another stream must not orphan this one's state.
        val single = listOf(SyncTier(thePastSeconds = null, everySeconds = week))
        assertEquals("", SyncTier.bandIdOf(single, single[0]), "no discriminator")
        assertEquals(mirror, SyncTier.keyFor(mirror, single, single[0]), "and no qualified coverage key")

        val f = tempFile()
        val now = 1_800_000_000L
        SyncBands(f).apply {
            record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now)
            flush()
        }
        assertEquals(now, SyncBands(f).verifiedAt(mirror, relay, profiles), "read back on the unqualified key")
        f.delete()
    }

    @Test
    fun `a banded re-fetch keeps a coverage per band`() {
        assertEquals("$mirror#tier:$month", SyncTier.keyFor(mirror, tiers, tiers[0]))
        assertEquals("$mirror#tier:all", SyncTier.keyFor(mirror, tiers, tiers[2]))

        val c = SyncBands(null, perStream = tiers.associate { SyncTier.keyFor(mirror, tiers, it) to it.everySeconds })
        assertEquals(week, c.refetchThePastSecondsFor(SyncTier.keyFor(mirror, tiers, tiers[0])))
        assertEquals(year, c.refetchThePastSecondsFor(SyncTier.keyFor(mirror, tiers, tiers[2])))

        // Paging one band leaves the others outstanding in full. Real clock: quartz refuses to
        // band a timestamp it considers implausible, and a fixed future `now` is one.
        val now = System.currentTimeMillis() / 1000
        val youngest = SyncTier.windowedFilter(profiles, now, tiers[0].thePastSeconds, null)
        c.record(SyncTier.keyFor(mirror, tiers, tiers[0]), relay, youngest, now - week, now, paged = true)

        assertNotNull(c.band(SyncTier.keyFor(mirror, tiers, tiers[0]), relay, youngest), "the walked band is covered")
        assertNull(c.band(SyncTier.keyFor(mirror, tiers, tiers[2]), relay, profiles), "the tail is not")
    }
}
