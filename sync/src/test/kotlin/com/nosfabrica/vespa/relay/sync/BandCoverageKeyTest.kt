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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A BAND'S COVERAGE KEY MUST STAND STILL WHILE ITS WINDOW MOVES.
 *
 * Quartz keys coverage by `(relay, filter)`. A filter carrying the band's window
 * therefore takes a new key every time that window moves — and the window moves
 * with `now`. Shipped that way, every band was re-keyed daily, its coverage
 * orphaned, and its whole span re-paged from scratch for every relay, three
 * bands over, against a re-fetch that used to walk the same ground monthly.
 *
 * Caught on staging in the state file itself: one relay carrying three identical
 * bands (same min, same max, both complete) under three keys whose `until`s were
 * exactly 86400 apart, with three different `fullAt` stamps — three full passes
 * for one band's worth of data.
 */
class BandCoverageKeyTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val content = Filter(kinds = listOf(1, 30023))
    private val stream = "contentViaOutbox"

    private val month = 30L * 86400
    private val tiers =
        listOf(
            SyncTier(maxAgeSeconds = month, everySeconds = 7L * 86400),
            SyncTier(maxAgeSeconds = null, everySeconds = 365L * 86400),
        )

    /** What the pool files a band's coverage under. */
    private fun keyFor(tier: SyncTier) = SyncTier.keyFor(stream, tiers, tier)

    /** Quartz records no band for a multi-kind paged filter without its per-kind spans. */
    private fun spans(
        min: Long,
        max: Long,
    ) = content.kinds!!.associateWith { SyncCoverage.Span(min, max) }

    @Test
    fun `a band walked today is still covered tomorrow`() {
        val bands = SyncBands(null)
        val day = 86_400L
        val today = 1_780_000_000L

        // Today: the youngest band is walked and completes.
        bands.record(keyFor(tiers[0]), relay, content, today - month, today, paged = true, observedByKind = spans(today - month, today))
        assertTrue(bands.legs(keyFor(tiers[0]), relay, content).none { it.since == null && it.until == null }, "walked today")

        // Tomorrow the window has moved a day. The KEY must not have moved with it.
        val band = bands.band(keyFor(tiers[0]), relay, content)
        assertEquals(true, band != null, "the band a day later must be the one walked today, not a fresh key")
        assertEquals(today - month, band!!.minCreatedAt, "and must carry the span it was walked with")

        // The legs left are the edges, never the whole band again.
        val legs = bands.legs(keyFor(tiers[0]), relay, content)
        assertTrue(
            legs.none { it.since == null && it.until == null },
            "a re-keyed band re-pages its whole span; got an unbounded leg: $legs",
        )
        // Tomorrow's window, and what is left outstanding inside it. An open `until`
        // means "up to now", so it is scored at tomorrow rather than as unbounded.
        val tomorrow = today + day
        val (since, until) = SyncTier.windowAt(tomorrow, tiers[0].maxAgeSeconds, null)
        val outstanding =
            legs.mapNotNull { leg ->
                val lo = listOfNotNull(leg.since, since).maxOrNull() ?: 0L
                val hi = listOfNotNull(leg.until, until).minOrNull() ?: tomorrow
                if (lo > hi) null else (lo to hi)
            }
        assertTrue(
            outstanding.all { (lo, hi) -> hi - lo <= 2 * day },
            "a day later, only the day the window gained may be outstanding — got " +
                outstanding.joinToString { (lo, hi) -> "[$lo,$hi] = ${hi - lo}s" },
        )
    }

    /** Each band keeps its own coverage: one completing must not silence another. */
    @Test
    fun `bands do not share a coverage key`() {
        val bands = SyncBands(null)
        val now = 1_780_000_000L
        bands.record(keyFor(tiers[0]), relay, content, now - month, now, paged = true, observedByKind = spans(now - month, now))

        assertEquals(null, bands.band(keyFor(tiers[1]), relay, content), "the tail must not inherit the recent band's coverage")
        assertTrue(
            bands.legs(keyFor(tiers[1]), relay, content).any { it.since == null && it.until == null },
            "the tail has never been walked, so its whole span is outstanding",
        )
    }
}
