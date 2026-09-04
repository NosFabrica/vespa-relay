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
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
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
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A WALK THE MIRROR'S OWN INGEST QUEUE STALLED IS NOT A RELAY REFUSING US.
 *
 * quartz drains each socket through one consumer coroutine that awaits every
 * listener, and this pool's hooks hand each event to `IngestPipeline.submit`,
 * which suspends when the queue is full. So under backpressure the pager sees
 * silence, or a first page received and never counted as delivered, and ends
 * `IDLE` or `UNPAGEABLE` with nothing downloaded — which `refusedOutright`
 * reads as the relay's refusal. On staging that was 90% of all aborts, filed
 * as `abortedQuiet` and `abortedUnpageable` against relays the monitor had
 * correctly graded `prime`.
 *
 * Staged here the way it happens there: a pipeline never started and filled to
 * its capacity, a relay whose page hands the pool one event and returns the
 * ending quartz would while the hook is still parked. The same relay against
 * a pipeline with room keeps its own ending — the reclassification is about
 * the instant, not the relay.
 */
class VisitPoolBackpressureTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")

    /**
     * Hands the walk ONE matching event on a coroutine of its own — quartz's
     * socket consumer, in effect — waits for it to park, and returns the
     * ending the pager would report having never seen the hook come back.
     */
    private class StalledRelay(
        private val scope: CoroutineScope,
        private val end: PagedFetchResult.End,
        private val handsAnEvent: Boolean,
        /** What the queue does WHILE the page is out — fills to the brim, or nothing. */
        private val meanwhile: suspend () -> Unit,
    ) : RelayReads {
        val pages = AtomicInteger()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            pages.incrementAndGet()
            meanwhile()
            if (handsAnEvent) {
                scope.launch { onEvent(event(pages.get())) }
                // Long enough for the hook to reach the queue and park in it.
                delay(HOOK_SETTLE_MS)
            }
            return PagedFetchResult(0, end)
        }

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

    private object NoSockets : Sockets {
        override fun claim(url: NormalizedRelayUrl) = Unit

        override fun release(url: NormalizedRelayUrl) = Unit
    }

    private class Harness(
        end: PagedFetchResult.End,
        handsAnEvent: Boolean = true,
        /** The queue fills DURING the page — room at the dial, none by the time the hook runs. */
        fillsDuringThePage: Boolean = true,
        /** …and a producer that is not the pool parks on this relay's events while the page is out. */
        parksAForeignProducer: Boolean = false,
    ) {
        val scope = CoroutineScope(SupervisorJob())
        val store = NostrSemanticsStore(InMemoryEventIndex())
        val processors = Processors()

        /** NEVER STARTED: nothing drains it, so filling it parks every submit after. */
        val ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null)

        val relay =
            StalledRelay(scope, end, handsAnEvent) {
                if (fillsDuringThePage) fillIngest()
                if (parksAForeignProducer) {
                    scope.launch { ingest.submit(event(FOREIGN), skipVerify = true, origin = IngestOrigin(streams.single().urls.single())) }
                    delay(HOOK_SETTLE_MS)
                }
            }

        val streams =
            listOf(
                SyncStream(
                    name = "content",
                    dir = SyncDirection.DOWN,
                    filter = Filter(kinds = listOf(KIND)),
                    urls = listOf(RelayUrlNormalizer.normalize("wss://relay.example")),
                    trusted = false,
                ),
            )

        val pool: VisitPool by lazy {
            val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
            val bands = SyncBands(null)
            VisitPool(
                reads = relay,
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

        /** Fill the queue to the brim, so the pool's next hand-off suspends. */
        suspend fun fillIngest() {
            repeat(ingest.capacity) { ingest.submit(event(FILL_BASE + it), skipVerify = true) }
        }

        suspend fun countsAfterTheVisit(): Map<String, Long> = countsOnce { (it["visitsRun"] ?: 0L) >= 1L && (it["abortedVisits"] ?: 0L) >= 1L }

        suspend fun countsOnce(settled: (Map<String, Long>) -> Boolean): Map<String, Long> =
            withTimeout(15_000) {
                while (true) {
                    val counts =
                        processors
                            .snapshot()
                            .single { it.name == "visits" }
                            .counts
                            .associate { it.name to it.value }
                    if (settled(counts)) return@withTimeout counts
                    delay(50)
                }
                @Suppress("UNREACHABLE_CODE")
                emptyMap()
            }

        fun close() {
            scope.cancel()
        }
    }

    @Test
    fun `a first page parked in a full ingest queue is our stall, not the relay's cursor`() =
        runBlocking {
            val h = Harness(PagedFetchResult.End.UNPAGEABLE)
            try {
                h.pool.start()
                val counts = h.countsAfterTheVisit()
                assertEquals(1L, counts["abortedVisits"])
                assertEquals(1L, counts["abortedBackpressured"], "the hook was parked when the walk gave up")
                assertEquals(0L, counts["abortedUnpageable"], "which is not the relay ignoring a cursor")
                assertTrue(h.relay.tails.isEmpty(), "still an abort: no band, no tail, the leg stays outstanding")
            } finally {
                h.close()
            }
        }

    @Test
    fun `silence while another hook of ours is parked is our stall too`() =
        runBlocking {
            // The hook that stalls a walk is as often a tail's or an audit's on
            // the same socket, and the walk then sees nothing at all. Staged
            // with the relay handing the event and reporting IDLE: what matters
            // is a hook of ours parked on that relay at the instant, whichever
            // subscription it came in on.
            val h = Harness(PagedFetchResult.End.IDLE)
            try {
                h.pool.start()
                val counts = h.countsAfterTheVisit()
                assertEquals(1L, counts["abortedBackpressured"])
                assertEquals(0L, counts["abortedQuiet"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `with room in the queue the same ending is the relay's own`() =
        runBlocking {
            // The classification is about the instant, not the relay: the hook
            // returns at once, nothing of ours is parked, and UNPAGEABLE with
            // nothing downloaded means what quartz says it means.
            val h = Harness(PagedFetchResult.End.UNPAGEABLE, fillsDuringThePage = false)
            try {
                h.pool.start()
                val counts = h.countsAfterTheVisit()
                assertEquals(1L, counts["abortedUnpageable"])
                assertEquals(0L, counts["abortedBackpressured"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `a CLOSED is the relay's word even under backpressure`() =
        runBlocking {
            // The relay's sentence came through the same consumer, so the
            // consumer was not parked when it was said. Only the endings a
            // parked consumer can manufacture are ever re-read.
            val h = Harness(PagedFetchResult.End.CLOSED)
            try {
                h.pool.start()
                val counts = h.countsAfterTheVisit()
                assertEquals(1L, counts["abortedClosed"])
                assertEquals(0L, counts["abortedBackpressured"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `a queue already full at the claim is not dialled into at all`() =
        runBlocking {
            // The cheaper of the two: a download into a queue that cannot take
            // it would park its first event, stall the socket, and come back
            // `abortedBackpressured` an idle window later, having cost the
            // relay a handshake and a REQ for nothing. Skipped like a refused
            // dial permit — nothing recorded, the revisit brings it back.
            val h = Harness(PagedFetchResult.End.UNPAGEABLE, fillsDuringThePage = false)
            try {
                h.fillIngest()
                h.pool.start()
                val counts = h.countsOnce { (it["visitsHeldByIngest"] ?: 0L) >= 1L }
                assertEquals(0, h.relay.pages.get(), "no REQ went out")
                assertEquals(0L, counts["visitsRun"])
                assertEquals(0L, counts["abortedVisits"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `a producer outside the pool parks the socket just the same`() =
        runBlocking {
            // The count lives in the pipeline, so the retraction audit and the
            // monitor's passes — which hand events to the same queue on the
            // same sockets — are covered without knowing it. Staged as a
            // foreign submit tagged with the relay, parked while the page is
            // out and the walk's own hook never fires.
            val h = Harness(PagedFetchResult.End.IDLE, handsAnEvent = false, parksAForeignProducer = true)
            try {
                h.pool.start()
                val counts = h.countsAfterTheVisit()
                assertEquals(
                    1,
                    h.ingest.parkedOn(
                        h.streams
                            .single()
                            .urls
                            .single(),
                    ),
                    "the foreign producer is still parked on the relay",
                )
                assertEquals(1L, counts["abortedBackpressured"])
                assertEquals(0L, counts["abortedQuiet"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `refusedOutright and stalledByUs agree on which endings are refusals at all`() {
        // Every ending a stall can be mistaken for is one that aborts; the
        // converse is deliberately false, and both are exhaustive over quartz's
        // enum so a new ending is a compile error here and a decision there.
        for (end in PagedFetchResult.End.entries) {
            if (VisitPool.stalledByUs(end)) {
                assertTrue(VisitPool.refusedOutright(PagedFetchResult(0, end)), "$end")
            }
        }
        assertEquals(
            setOf(PagedFetchResult.End.IDLE, PagedFetchResult.End.UNPAGEABLE),
            PagedFetchResult.End.entries
                .filter { VisitPool.stalledByUs(it) }
                .toSet(),
        )
    }

    companion object {
        private const val KIND = 1
        private const val HOOK_SETTLE_MS = 300L
        private const val FILL_BASE = 1_000_000
        private const val FOREIGN = 2_000_000

        private fun idOf(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("backpressure-$n".toByteArray()))

        fun event(n: Int) =
            Event(
                id = idOf(n),
                pubKey = "a1".repeat(32),
                createdAt = 1_700_000_000L + n,
                kind = KIND,
                tags = emptyArray(),
                content = "",
                sig = "b2".repeat(32),
            )
    }
}
