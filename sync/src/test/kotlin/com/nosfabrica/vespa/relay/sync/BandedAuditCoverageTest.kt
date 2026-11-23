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

import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.config.SyncTier
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A banded audit whose window is a catch-up band's reconciles that band to its own floor, so it
 * claims it there. Filed under the bare stream name, as it was, it could never settle the band.
 */
class BandedAuditCoverageTest {
    private val url = RelayUrlNormalizer.normalize("wss://audited.example")
    private val day = 86_400L

    private val stream =
        RouterConfigLoader
            .parse(
                """
                streams {
                  notes {
                    dir = "down"
                    filter = { "kinds": [1] }
                    urls = [ "wss://audited.example" ]
                    negentropy = [
                      { maxAge = 2592000,  every = 604800 }
                      { maxAge = 31536000, every = 2592000 }
                      { every = 31536000 }
                    ]
                    refetch = [
                      { maxAge = 2592000,  every = 2592000 }
                      { maxAge = 31536000, every = 7776000 }
                      { every = 31536000 }
                    ]
                  }
                }
                """.trimIndent(),
            ).streams
            .single()

    private val filter: Filter = stream.filter
    private val tiles = SyncTier.tile(stream.negentropySchedule)
    private val middle = SyncBands.coverageKeys(stream)[1]

    private fun now() = System.currentTimeMillis() / 1000

    private fun bands() = SyncBands(null, perStream = SyncBands.catchUpBands(stream).associate { SyncTier.keyFor(stream.name, SyncBands.catchUpBands(stream), it) to it.everySeconds })

    /** The audit of one band as the pool records it on completion. */
    private fun SyncBands.audited(
        index: Int,
        now: Long,
    ) {
        val (tier, olderEdge, newerEdge) = tiles[index]
        recordStreamAudit(stream, url, filter, SyncTier.windowedFilter(filter, now, olderEdge, newerEdge), olderEdge, newerEdge, now - 60, SyncTier.bandIdOf(stream.negentropySchedule, tier), now)
    }

    @Test
    fun `every audit band finds the catch-up band with its window`() {
        assertEquals(SyncBands.coverageKeys(stream), tiles.map { (_, older, newer) -> SyncBands.coverageKeyFor(stream, older, newer) })
    }

    @Test
    fun `an audit of the middle band settles a walk cut short above its floor`() {
        val bands = bands()
        val now = now()
        val (since, until) = SyncTier.windowAt(now, 31_536_000, 2_592_000)
        // A walk cut a hundred days down: the rest of the band is still owed.
        val cut = SyncCoverage.Span(until!! - 100 * day, until)
        bands.record(middle, url, filter, cut.min, cut.max, paged = true, observedByKind = mapOf(1 to cut))
        assertTrue(bands.legs(middle, url, filter).any { it.until == cut.min }, "the state before the audit")

        bands.audited(1, now)

        val band = bands.band(middle, url, filter)!!
        assertTrue(band.complete, "the audit compared the band's whole window")
        assertEquals(since, band.minCreatedAt, "claimed from the window's floor, not from where the reconcile ended")
        assertFalse(bands.legs(middle, url, filter).any { (it.since ?: Long.MIN_VALUE) < cut.min }, "and nothing below the cut is owed")
        assertNull(bands.band(stream.name, url, filter), "nothing is filed under the bare name")
        assertNotNull(bands.verifiedAt(stream.name, url, filter, tiles[1].first.id), "the clock stays where the schedule reads it")
    }

    @Test
    fun `an audit never restarts the clock of a band owed its re-walk`() {
        val bands = bands()
        val (_, until) = SyncTier.windowAt(now(), 31_536_000, 2_592_000)
        val cut = SyncCoverage.Span(until!! - 100 * day, until)
        bands.record(middle, url, filter, cut.min, cut.max, paged = true, observedByKind = mapOf(1 to cut))
        val fullAt = bands.band(middle, url, filter)!!.fullAt
        // A hundred days on, past the band's ninety-day re-fetch period.
        val later = now() + 100 * day

        bands.audited(1, later)

        assertEquals(fullAt, bands.band(middle, url, filter)!!.fullAt, "the re-walk stays due")
        assertNotNull(bands.verifiedAt(stream.name, url, filter, tiles[1].first.id), "the audit's own clock still moves")
    }

    @Test
    fun `an audit with no band of its window files under the bare name only at the floor`() {
        assertNull(SyncBands.auditKey(stream), "every audit band here has a catch-up band")
    }

    @Test
    fun `an audit of the middle band never bridges a gap above its newer edge`() {
        val bands = bands()
        val now = now()
        val (_, until) = SyncTier.windowAt(now, 31_536_000, 2_592_000)
        // Only ground above the band's newer edge was walked under this key.
        val above = SyncCoverage.Span(until!! + day, now - 3_600)
        bands.record(middle, url, filter, above.min, above.max, paged = true, observedByKind = mapOf(1 to above))

        bands.audited(1, now)

        assertFalse(bands.band(middle, url, filter)!!.complete, "the day between the window and the held span was never compared")
    }
}
