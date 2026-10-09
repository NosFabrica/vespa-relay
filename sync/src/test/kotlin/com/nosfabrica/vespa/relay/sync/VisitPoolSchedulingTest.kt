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

import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

/** When the real [VisitPool] gets back to a unit it could not serve. */
class VisitPoolSchedulingTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")
    private val b = RelayUrlNormalizer.normalize("wss://b.example")

    private fun stream(
        visitConcurrency: Int? = null,
        maxLiveConcurrency: Int? = null,
    ) = SyncStream(
        name = "content",
        dir = SyncDirection.DOWN,
        filter = Filter(kinds = listOf(1)),
        urls = listOf(a, b),
        trusted = false,
        visitConcurrency = visitConcurrency,
        maxLiveConcurrency = maxLiveConcurrency,
    )

    @Test
    fun `a unit refused a visit permit is retried within seconds, not after a revisit's wait`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val streams = listOf(stream(visitConcurrency = 1))
            val limits = PoolLimits.of(streams)
            val gate = CompletableDeferred<Unit>()
            val first = AtomicInteger()
            val paged = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
            val fixture =
                PoolFixture(scope, streams, answerAt = { url, _, _ ->
                    // The first walk holds the stream's only permit until the other unit has been turned away.
                    if (first.getAndIncrement() == 0) gate.await()
                    paged += url
                    PagedFetchResult(0, PagedFetchResult.End.DRAINED)
                })
            try {
                fixture.pool(limits = limits, workers = 2).start()
                withTimeout(10_000) {
                    while (limits.deferred("content", VisitPool.JOB_VISITING) == 0L) delay(10)
                }
                gate.complete(Unit)
                withTimeout(VisitPool.TURNED_AWAY_RETRY_MS + 10_000) {
                    while (paged.size < 2) delay(20)
                }
                assertEquals(setOf(a, b), paged.toSet())
            } finally {
                scope.cancel()
            }
        }
}
