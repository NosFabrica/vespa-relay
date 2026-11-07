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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A relay that keeps the socket up and sends nothing must not hold a live tail for hours: past
 * [VisitPool.TAIL_SILENCE_CLOSE_MS] without an event the tail closes, and the unit is visited again.
 */
class TailSilenceTest {
    private val url = RelayUrlNormalizer.normalize("wss://live.example")

    private fun withTail(test: suspend (PoolFixture, VisitPool) -> Unit): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, listOf(PoolFixture.stream("live", url, Filter(kinds = listOf(1)))))
                val pool = fixture.pool()
                pool.start()
                fixture.awaitTail()
                test(fixture, pool)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a tail silent past the window is closed and its unit visited again on a fresh tail`() =
        withTail { fixture, pool ->
            val silentSub = fixture.tails.keys.single()
            val walksBefore = fixture.asked.size

            pool.closeSilentTails(System.currentTimeMillis() + VisitPool.TAIL_SILENCE_CLOSE_MS)

            assertTrue(!fixture.tails.containsKey(silentSub), "the silent subscription is closed")
            withTimeout(10_000) {
                while (fixture.tails.keys.none { it != silentSub }) delay(10)
            }
            assertTrue(fixture.asked.size > walksBefore, "the revisit paged over the silence before tailing again")
            assertEquals(1, fixture.tails.size, "one tail for the unit, not the old one beside a new one")
            assertEquals(1L, fixture.counts()["liveSilenced"])
        }

    @Test
    fun `a tail that delivered within the window stays open`() =
        withTail { fixture, pool ->
            val start = System.currentTimeMillis()
            val sub = fixture.tails.keys.single()
            delay(1_100)
            fixture.deliverers.values
                .single()
                .invoke(PoolFixture.event(1, System.currentTimeMillis() / 1000))

            // Past the window since the open, inside it since the delivery.
            pool.closeSilentTails(start + VisitPool.TAIL_SILENCE_CLOSE_MS + 500)

            assertEquals(setOf(sub), fixture.tails.keys, "the delivering tail is untouched")
            assertEquals(0L, fixture.counts()["liveSilenced"] ?: 0L)
        }
}
