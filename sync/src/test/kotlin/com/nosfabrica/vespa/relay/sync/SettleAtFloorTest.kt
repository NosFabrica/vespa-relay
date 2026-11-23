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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A band's older edge rises a day at a time. A walk that passed yesterday's edge and stopped short
 * of a drain sits below today's, where quartz owes that kind no older leg and so nothing could
 * settle it: the pair read as paging until the band's whole re-fetch period ran out.
 */
class SettleAtFloorTest {
    private val url = RelayUrlNormalizer.normalize("wss://settle.example")
    private val notes = Filter(kinds = listOf(1, 7))
    private val key = "notes#tier:864000"
    private val day = 86_400L

    private fun now() = System.currentTimeMillis() / 1000

    private fun bands(period: Long = 30 * day) = SyncBands(null, perStream = mapOf(key to period))

    private fun SyncBands.held(vararg spans: Pair<Int, SyncCoverage.Span>) {
        val byKind = spans.toMap()
        record(key, url, notes, byKind.values.minOf { it.min }, byKind.values.maxOf { it.max }, paged = true, observedByKind = byKind)
    }

    /** Whether a visit would still ask this band for anything below [edge]. */
    private fun SyncBands.owesBelow(edge: Long) = legs(key, url, notes, floor = edge).any { (it.since ?: Long.MIN_VALUE) < edge || it.until?.let { u -> u <= edge } == true }

    @Test
    fun `the stuck state owes nothing and settles nothing without it`() {
        val bands = bands()
        val edge = SyncTier.windowAt(now(), 10 * day, null).first!!
        bands.held(1 to SyncCoverage.Span(edge - 3_600, now() - day), 7 to SyncCoverage.Span(edge - 60, now() - day))

        assertFalse(bands.owesBelow(edge), "quartz owes no older leg past the floor")
        assertFalse(bands.band(key, url, notes)!!.complete, "and nothing else ever marks it complete")
    }

    @Test
    fun `a kind walked past the edge is settled`() {
        val bands = bands()
        val edge = SyncTier.windowAt(now(), 10 * day, null).first!!
        bands.held(1 to SyncCoverage.Span(edge - 3_600, now() - day), 7 to SyncCoverage.Span(edge - 60, now() - day))

        assertTrue(bands.settleAtFloor(key, url, notes, edge))

        assertTrue(bands.band(key, url, notes)!!.complete)
        assertFalse(bands.owesBelow(edge))
    }

    @Test
    fun `a kind still above the edge keeps its older leg`() {
        val bands = bands()
        val edge = SyncTier.windowAt(now(), 10 * day, null).first!!
        bands.held(1 to SyncCoverage.Span(edge - 3_600, now() - day), 7 to SyncCoverage.Span(edge + day, now() - day))

        assertTrue(bands.settleAtFloor(key, url, notes, edge), "kind 1 settles")

        val band = bands.band(key, url, notes)!!
        assertTrue(band.spans.getValue(1).complete)
        assertFalse(band.spans.getValue(7).complete, "kind 7 stopped a day above the edge; that day is still owed")
        assertTrue(bands.legs(key, url, notes, floor = edge).any { 7 in it.kinds.orEmpty() && it.until == edge + day }, "and asked for")
    }

    @Test
    fun `a stale band is left for the re-walk that replaces it`() {
        val bands = bands(period = 100)
        val edge = SyncTier.windowAt(now(), 10 * day, null).first!!
        bands.held(1 to SyncCoverage.Span(edge - 3_600, now() - day))

        assertFalse(bands.settleAtFloor(key, url, notes, edge, now = now() + 200))

        assertFalse(bands.band(key, url, notes)!!.complete)
    }
}
