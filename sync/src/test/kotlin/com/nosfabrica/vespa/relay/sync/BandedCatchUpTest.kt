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

import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.config.SyncTier
import com.nosfabrica.vespa.relay.status.RelayStatusReport
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A banded re-fetch walked to its floor by real visits must read as settled, however many bands it keeps. */
class BandedCatchUpTest {
    private val url = RelayUrlNormalizer.normalize("wss://banded.example")
    private val notes = Filter(kinds = listOf(1))

    /** The schedule that showed every row paging: six hours hourly, the rest daily. */
    private val tiers = listOf(SyncTier(maxAgeSeconds = 21_600, everySeconds = 3_600), SyncTier(maxAgeSeconds = null, everySeconds = 86_400))

    private val stream: SyncStream = PoolFixture.stream("banded", url, notes).copy(refetchTiers = tiers)

    private fun now() = System.currentTimeMillis() / 1000

    private fun bands() = SyncBands(null, perStream = tiers.associate { SyncTier.keyFor(stream.name, tiers, it) to it.everySeconds })

    /** Events [ages] seconds old. */
    private fun corpus(vararg ages: Long) = ages.mapIndexed { i, age -> PoolFixture.event(i, now() - age) }

    private fun row(
        bands: SyncBands,
        pool: VisitPool,
    ): JsonObject =
        RelayStatusReport
            .build(bands.snapshot(), pool.primeUnits(), now())!!["rows"]!!
            .jsonArray
            .single()
            .jsonObject

    @Test
    fun `a banded stream walked to its floor reads complete`(): Unit =
        runBlocking {
            val bands = bands()
            val pool = PoolFixture.visitOnce(listOf(stream), bands, PoolFixture.holding(corpus(60, 3_000, 20_000, 40_000, 200_000, 5_000_000)))

            val row = row(bands, pool)
            assertEquals(RelayStatusReport.COMPLETE, row["syncStatus"]!!.jsonPrimitive.content, "every band was walked to its own floor")
            assertEquals(1, row["settled"]!!.jsonPrimitive.content.toInt())
        }

    @Test
    fun `a younger band walked to its floor owes nothing below what it holds`(): Unit =
        runBlocking {
            val bands = bands()
            PoolFixture.visitOnce(listOf(stream), bands, PoolFixture.holding(corpus(60, 3_000, 20_000, 40_000, 200_000)))

            val young = SyncBands.coverageKeys(stream).first()
            val held = bands.band(young, url, notes)!!
            assertTrue(
                bands.legs(young, url, notes).all { (it.since ?: Long.MIN_VALUE) >= held.maxCreatedAt },
                "only the present above the band is still owed, so the next visit does not re-walk the band's bottom",
            )
        }

    @Test
    fun `a younger band with nothing in its window does not hold the pair at paging`(): Unit =
        runBlocking {
            val bands = bands()
            // Nothing within the youngest band's window, which reaches at most thirty hours back.
            val pool = PoolFixture.visitOnce(listOf(stream), bands, PoolFixture.holding(corpus(400_000, 5_000_000)))

            assertEquals(RelayStatusReport.COMPLETE, row(bands, pool)["syncStatus"]!!.jsonPrimitive.content)
        }

    @Test
    fun `an unbanded stream walked to its floor still reads complete`(): Unit =
        runBlocking {
            val bands = SyncBands(null)
            val plain = PoolFixture.stream("plain", url, notes)
            val pool = PoolFixture.visitOnce(listOf(plain), bands, PoolFixture.holding(corpus(60, 40_000, 5_000_000)))

            assertEquals(RelayStatusReport.COMPLETE, row(bands, pool)["syncStatus"]!!.jsonPrimitive.content)
        }
}
