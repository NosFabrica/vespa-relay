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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A banded audit verifies one window. With an unbanded re-fetch it files under the catch-up's
 * own coverage key, so what it records there must claim that window and nothing older.
 */
class AuditCoverageTest {
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
                    refetchThePastSeconds = 2592000
                    negentropy = [
                      { maxAge = 604800, every = 86400 }
                      { every = 2592000 }
                    ]
                  }
                }
                """.trimIndent(),
            ).streams
            .single()

    private val filter: Filter = stream.filter
    private val tiles = SyncTier.tile(stream.negentropySchedule)

    private fun now() = System.currentTimeMillis() / 1000

    private fun bands() = SyncBands(null, perStream = mapOf(stream.name to 2_592_000L))

    private fun SyncBands.reaches(t: Long) = legs(stream.name, url, filter).any { (it.since ?: Long.MIN_VALUE) <= t && t <= (it.until ?: Long.MAX_VALUE) }

    /** The audit of one band as the pool records it on completion. */
    private fun SyncBands.audited(
        index: Int,
        now: Long,
    ) {
        val (tier, olderEdge, newerEdge) = tiles[index]
        recordAudit(
            stream.name,
            url,
            filter,
            SyncTier.windowedFilter(filter, now, olderEdge, newerEdge),
            reachesFloor = olderEdge == null,
            verifiedAt = now - 60,
            band = SyncTier.bandIdOf(stream.negentropySchedule, tier),
        )
    }

    @Test
    fun `the collision this guards is real`() {
        assertEquals(listOf(stream.name), SyncBands.coverageKeys(stream), "the catch-up files under the audit's own key")
    }

    @Test
    fun `a completed weekly audit leaves the catch-up's older leg outstanding`() {
        val bands = bands()
        val now = now()
        val (a, b) = now - 20 * day to now - 3600
        bands.record(stream.name, url, filter, a, b, paged = true)

        bands.audited(0, now)

        assertTrue(bands.reaches(a - day), "a week verified says nothing about what lies before the catch-up's floor")
        assertNotNull(bands.verifiedAt(stream.name, url, filter, SyncTier.bandIdOf(stream.negentropySchedule, tiles[0].first)), "the band's clock still moves")
    }

    @Test
    fun `the band that reaches the floor settles the past below the catch-up`() {
        val bands = bands()
        val now = now()
        val (a, b) = now - 20 * day to now - 3600
        bands.record(stream.name, url, filter, a, b, paged = true)

        bands.audited(1, now)

        assertFalse(bands.reaches(a - day), "everything up to the band's newer edge was reconciled, and the catch-up meets it")
    }

    @Test
    fun `the band that reaches the floor never bridges a gap above its newer edge`() {
        val bands = bands()
        val now = now()
        val (a, b) = now - 3 * day to now - 3600
        bands.record(stream.name, url, filter, a, b, paged = true)

        bands.audited(1, now)

        assertTrue(bands.reaches(now - 5 * day), "between the reconciled week edge and the catch-up's floor nothing was walked")
    }
}
