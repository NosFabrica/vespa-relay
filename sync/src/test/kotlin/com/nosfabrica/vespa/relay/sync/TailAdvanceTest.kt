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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A tail's reconnect asks again from the `since` quartz holds for it, so that `since` follows what
 * the tail delivered, and nothing else: a tail the relay went quiet on keeps asking from before it.
 */
class TailAdvanceTest {
    private val url = RelayUrlNormalizer.normalize("wss://live.example")

    private fun sinceOf(fixture: PoolFixture) =
        fixture.tails.values
            .single()
            .map { it.since }
            .distinct()
            .single()

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
    fun `a tail that delivered asks a reconnect from its last event less the overlap`() =
        withTail { fixture, pool ->
            val opened = sinceOf(fixture)!!
            // A delivery a second or more after the open is what moves the edge.
            delay(1_100)
            val deliveredAt = System.currentTimeMillis() / 1000
            fixture.deliverers.values
                .single()
                .invoke(PoolFixture.event(1, deliveredAt))
            pool.advanceTails()

            val since = sinceOf(fixture)!!
            assertTrue(since > opened, "the stored since moved past the open's ($since vs $opened)")
            assertTrue(since >= deliveredAt - VisitPool.TAIL_OVERLAP_SECONDS, "up to the delivery, less the overlap")
            assertTrue(since <= deliveredAt - VisitPool.TAIL_OVERLAP_SECONDS + 1, "and never past it")
        }

    @Test
    fun `a tail that delivered nothing keeps asking from its open`() =
        withTail { fixture, pool ->
            val opened = sinceOf(fixture)
            delay(1_100)
            pool.advanceTails()

            assertEquals(opened, sinceOf(fixture), "silence proves nothing about the gap, so the reconnect still asks for it")
        }
}
