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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.sync.heal.WriteCapability
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The real [VisitPool] over a relay that refuses over-wide filters: the
 * re-ask in chunks reaches the wire, and the visit finishes.
 */
class VisitPoolWidthTest {
    private val url = RelayUrlNormalizer.normalize("wss://purplerelay.com")

    /** Small enough to assert on, wide enough to chunk. */
    private val kinds = listOf(1, 6, 7, 16, 1111)

    /**
     * A relay that refuses any filter naming more than [cap] kinds with a `CLOSED` and a
     * sentence. Its own [RelayComplaints], so the sentence is dated when the refusal is produced.
     */
    private class WidthCappedRelay(
        private val cap: Int,
        private val says: String = "invalid: too many kinds (max $cap)",
        private val end: PagedFetchResult.End = PagedFetchResult.End.CLOSED,
    ) : RelayReads,
        RelayComplaints {
        val asked = CopyOnWriteArrayList<List<Int>>()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        @Volatile
        private var saidAtMs = 0L

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            asked += filter.kinds.orEmpty()
            if ((filter.kinds?.size ?: 0) > cap) {
                saidAtMs = System.currentTimeMillis()
                return PagedFetchResult(0, end)
            }
            // DRAINED with nothing delivered is an honest empty relay, not a refusal.
            return PagedFetchResult(0, PagedFetchResult.End.DRAINED)
        }

        override fun since(
            url: NormalizedRelayUrl,
            sinceMs: Long,
        ): String? = says.takeIf { saidAtMs >= sinceMs }

        override suspend fun tail(
            subId: String,
            url: NormalizedRelayUrl,
            filters: List<Filter>,
            onEvent: suspend (Event) -> Unit,
        ) {
            tails[subId] = filters
        }

        override fun untail(subId: String) {
            tails.remove(subId)
        }
    }

    /** Claims nothing and counts nothing. */
    private object NoSockets : Sockets {
        override fun claim(url: NormalizedRelayUrl) = Unit

        override fun release(url: NormalizedRelayUrl) = Unit
    }

    /** The real [VisitPool] over one fake relay, so the two cases differ only in the relay they meet. */
    private fun poolOver(
        relay: WidthCappedRelay,
        store: NostrSemanticsStore,
        scope: CoroutineScope,
        processors: Processors,
    ): VisitPool {
        // Never dialled; it only satisfies the constructors.
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
        val bands = SyncBands(null)
        val streams =
            listOf(
                SyncStream(
                    name = "content",
                    dir = SyncDirection.DOWN,
                    filter = Filter(kinds = kinds),
                    urls = listOf(url),
                    trusted = false,
                ),
            )
        val ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null)
        ingest.start()
        return VisitPool(
            reads = relay,
            complaints = relay,
            bands = bands,
            ingest = ingest,
            pager =
                NegentropyPager(
                    StoreWindowIndex(store),
                    ClientWindowSync(client, FilterWidths(), refused = RefusedIds.disabled()),
                    SweepState(null),
                    NegPageTuning(target = 5_000, minTarget = 500, maxTarget = 50_000, slackSeconds = 60),
                ),
            healer = Healer(client, store, HealQueue(), WriteCapability(), RefusedIds.disabled(), null),
            sockets = NoSockets,
            scope = scope,
            rosterBuilder = RosterBuilder(store = store, streams = streams, bands = bands),
            streams = streams,
            progress = processors.of("visits"),
            workers = 1,
        )
    }

    @Test
    fun `a relay that rejects an over-wide filter is re-asked in chunks, and finishes`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val relay = WidthCappedRelay(cap = 2)
            val processors = Processors()
            val pool = poolOver(relay, store, scope, processors)
            try {
                pool.start()
                // The tail is the last thing a clean visit does, so its arrival means the ask completed.
                withTimeout(10_000) {
                    while (relay.tails.isEmpty()) delay(20)
                }

                // A cap is the relay's to state, so every relay is asked at full width once.
                assertEquals(kinds, relay.asked.first(), "the first ask is the stream's own filter, unsplit")

                // Three chunks at a cap of two: the kinds themselves, not a ceiling rounded up.
                val chunks = relay.asked.drop(1).take(3)
                assertTrue(chunks.all { it.size <= 2 }, "every chunk is inside the relay's stated limit: $chunks")
                assertEquals(kinds, chunks.flatten(), "every kind asked for, in order, exactly once")

                // The tail pays the same width; a subscription carrying the refused filter never delivers.
                val tail = relay.tails.values.first()
                assertTrue(tail.all { (it.kinds?.size ?: 0) <= 2 }, "the tail is chunked too: ${tail.map { it.kinds }}")
                assertEquals(kinds, tail.flatMap { it.kinds.orEmpty() })

                // Re-opened at the learned width: a cap is learned from a refusal, not a roster change.
                assertEquals(
                    1,
                    relay.tails.size,
                    "one live subscription, re-opened at the learned width rather than left at the refused one",
                )

                // A refusal the narrowing rescued is not an abort; it is a `narrowedRelays`.
                val counts =
                    processors
                        .snapshot()
                        .single { it.name == "visits" }
                        .counts
                        .associate { it.name to it.value }
                assertEquals(0L, counts["abortedVisits"], "the retry rescued the visit, so nothing aborted")
                assertEquals(1L, counts["narrowedRelays"], "one relay has told us what it will take")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a relay refusing for any other reason is not narrowed, it is named`() =
        runBlocking {
            // Narrowing is driven by the sentence alone; any other refusal is counted under its own name.
            val scope = CoroutineScope(SupervisorJob())
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val relay = WidthCappedRelay(cap = 2, says = "auth-required: we only serve authenticated users", end = PagedFetchResult.End.AUTH_REQUIRED)
            val processors = Processors()
            val pool = poolOver(relay, store, scope, processors)
            try {
                pool.start()
                val counts =
                    withTimeout(10_000) {
                        var seen: Map<String, Long>
                        while (true) {
                            seen =
                                processors
                                    .snapshot()
                                    .single { it.name == "visits" }
                                    .counts
                                    .associate { it.name to it.value }
                            if ((seen["abortedVisits"] ?: 0L) > 0) break
                            delay(20)
                        }
                        seen
                    }
                assertEquals(0L, counts["narrowedRelays"], "an auth wall is not a width to learn")
                assertTrue((counts["abortedAuthRequired"] ?: 0L) > 0, "and it is counted as the wall it is")
                assertEquals(counts["abortedVisits"], counts["abortedAuthRequired"], "with nothing else in the total")
                assertTrue(relay.tails.isEmpty(), "a refused visit never reaches its tail")
                // One ask: the relay is asked at full width and the visit ends there.
                assertTrue(relay.asked.all { it == kinds }, "no chunking was attempted: ${relay.asked}")
            } finally {
                scope.cancel()
            }
        }
}
