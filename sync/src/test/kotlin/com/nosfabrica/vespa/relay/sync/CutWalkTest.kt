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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A walk the relay cut short read only from its top down to its oldest event. Recorded as a
 * span, it must never let the real band arithmetic call the unread ground below it covered.
 */
class CutWalkTest {
    private val url = RelayUrlNormalizer.normalize("wss://cut.example")
    private val notes = Filter(kinds = listOf(1))
    private val day = 86_400L

    private fun now() = System.currentTimeMillis() / 1000

    private fun SyncBands.reaches(
        key: String,
        filter: Filter,
        t: Long,
    ) = legs(key, url, filter).any { (it.since ?: Long.MIN_VALUE) <= t && t <= (it.until ?: Long.MAX_VALUE) }

    private fun span(
        min: Long,
        max: Long,
    ) = SyncCoverage.Span(min, max, false)

    @Test
    fun `a cut walk of the newer leg leaves the gap below it outstanding`() {
        val bands = SyncBands(null)
        val (a, b, t) = Triple(now() - 10 * day, now() - 5 * day, now() - day)
        bands.record("s", url, notes, a, b, paged = true, observedByKind = mapOf(1 to span(a, b)))

        bands.recordCut("s", url, notes, mapOf(1 to span(t, now() - 60)), walkedTop = now())

        assertTrue(bands.reaches("s", notes, (b + t) / 2), "the unread ground between the band and the cut stays a leg")
        assertFalse(bands.reaches("s", notes, (a + b) / 2), "and the band itself stays covered")
    }

    @Test
    fun `a cut walk of the older leg still extends the band downward`() {
        // Started at the band's floor, so what it read meets the band with no gap.
        val bands = SyncBands(null)
        val (a, b) = now() - 10 * day to now() - 5 * day
        bands.record("s", url, notes, a, b, paged = true, observedByKind = mapOf(1 to span(a, b)))

        bands.recordCut("s", url, notes, mapOf(1 to span(a - 3 * day, a - day)), walkedTop = a)

        assertFalse(bands.reaches("s", notes, a - 2 * day), "the walked ground is covered")
        assertTrue(bands.reaches("s", notes, a - 4 * day), "and the walk resumes below its oldest event")
    }

    @Test
    fun `a first walk cut short is still progress`() {
        val bands = SyncBands(null)
        val t = now() - day
        bands.recordCut("s", url, notes, mapOf(1 to span(t, now() - 60)), walkedTop = now())
        assertFalse(bands.reaches("s", notes, t + 600), "nothing held, so nothing can be bridged")
        assertTrue(bands.reaches("s", notes, t - 600))
    }

    @Test
    fun `a kindless filter is judged on the one span quartz files it under`() {
        val any = Filter(authors = listOf("a1".repeat(32)))
        val bands = SyncBands(null)
        val (a, b, t) = Triple(now() - 10 * day, now() - 5 * day, now() - day)
        bands.record("s", url, any, a, b, paged = true, observedByKind = mapOf(1 to span(a, b)))

        bands.recordCut("s", url, any, mapOf(1 to span(t, now() - 60), 7 to span(t + 60, now() - 120)), walkedTop = now())

        assertTrue(bands.reaches("s", any, (b + t) / 2))
    }

    @Test
    fun `a visit whose newer leg is cut short never claims the ground it did not read`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val (a, b, t) = Triple(now() - 10 * day, now() - 5 * day, now() - day)
            val stream = PoolFixture.stream("cut", url, notes)
            val key = SyncBands.coverageKeys(stream).single()
            val bands = SyncBands(null)
            bands.record(key, url, notes, a, b, paged = true, observedByKind = mapOf(1 to span(a, b)))
            val fixture =
                PoolFixture(scope, listOf(stream), bands = bands, answer = { filter, onEvent ->
                    if (filter.since == b) {
                        // Newest-first down to `t`, then the relay goes quiet.
                        for (i in 0 until 10) onEvent(PoolFixture.event(i, now() - 60 - i * (now() - 60 - t) / 9))
                        PagedFetchResult(10, PagedFetchResult.End.IDLE)
                    } else {
                        PagedFetchResult(0, PagedFetchResult.End.DRAINED)
                    }
                })
            try {
                fixture.pool().start()
                fixture.awaitTail()
                assertTrue(fixture.asked.any { it.since == b }, "the newer leg was walked")
                assertTrue(bands.reaches(key, notes, (b + t) / 2), "the ground between the band and the cut is still owed")
                assertFalse(bands.reaches(key, notes, (a + b) / 2), "and the band it already held is not re-walked")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a negentropy fallback page cut short is not a read window`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            var end = PagedFetchResult.End.IDLE
            val reads =
                object : RelayReads {
                    override suspend fun page(
                        url: NormalizedRelayUrl,
                        filter: Filter,
                        idleTimeoutMs: Long,
                        onEvent: suspend (Event) -> Unit,
                    ): PagedFetchResult {
                        repeat(3) { onEvent(PoolFixture.event(it, now() - 3600)) }
                        return PagedFetchResult(3, end)
                    }

                    override suspend fun tail(
                        subId: String,
                        url: NormalizedRelayUrl,
                        filters: List<Filter>,
                        onEvent: suspend (Event) -> Unit,
                    ) = Unit

                    override fun untail(subId: String) = Unit
                }
            try {
                val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
                val sync = ClientWindowSync(client, FilterWidths(), reads = reads)
                val window = notes.copy(since = now() - day, until = now() - 60)

                val cut = sync.page(url, window) {}
                assertEquals(3, cut.downloaded, "what arrived is kept")
                assertTrue(cut.refused, "but a window the relay stopped answering mid-walk was not read")

                end = PagedFetchResult.End.DRAINED
                assertFalse(sync.page(url, window) {}.refused, "a drained window was")
            } finally {
                scope.cancel()
            }
        }
}
