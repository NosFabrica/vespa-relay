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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
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
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A walk stalled by the mirror's own full ingest queue pauses and resumes from its cursor; one
 * whose queue does not drain is filed as `abortedBackpressured`, not as the relay's refusal, and a
 * queue already full at the claim is not dialled into at all.
 */
class VisitPoolBackpressureTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")

    /** Hands the walk one event on its own coroutine, lets it park, and returns the pager's ending. */
    private class StalledRelay(
        private val scope: CoroutineScope,
        private val end: PagedFetchResult.End,
        private val handsAnEvent: Boolean,
        /** What the queue does while the page is out: fills to the brim, or nothing. */
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

    /**
     * A burst: the first page hands [delivered] newest-first, then its next event parks in a queue
     * filled to the brim and the page goes quiet; the queue drains a moment later. Later pages drain.
     */
    private class BurstRelay(
        private val h: Harness,
        private val delivered: List<Int>,
        private val parked: Int,
        /** The first page also delivers a second event in [parked]'s second, and the resume serves both again, as a real relay does. */
        private val reServes: Boolean = false,
    ) : RelayReads {
        val asked = CopyOnWriteArrayList<Filter>()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            asked += filter
            if (asked.size > 1) {
                if (reServes) {
                    onEvent(event(parked + SAME_SECOND, event(parked).createdAt))
                    onEvent(event(parked))
                    return PagedFetchResult(2, PagedFetchResult.End.DRAINED)
                }
                return PagedFetchResult(0, PagedFetchResult.End.DRAINED)
            }
            delivered.forEach { onEvent(event(it)) }
            if (reServes) onEvent(event(parked + SAME_SECOND, event(parked).createdAt))
            h.fillIngest()
            h.scope.launch { onEvent(event(parked)) }
            delay(HOOK_SETTLE_MS)
            // The store catches up while the walk waits, as it does after a burst.
            h.scope.launch {
                delay(HOOK_SETTLE_MS)
                h.ingest.start()
            }
            val end = if (delivered.isEmpty()) PagedFetchResult.End.UNPAGEABLE else PagedFetchResult.End.IDLE
            return PagedFetchResult(delivered.size + (if (reServes) 1 else 0), end)
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
        /** The queue fills during the page: room at the dial, none by the time the hook runs. */
        fillsDuringThePage: Boolean = true,
        /** A producer that is not the pool parks on this relay's events while the page is out. */
        parksAForeignProducer: Boolean = false,
        /** How long a stalled walk waits for the queue; the never-draining cases keep it short. */
        pauseLimitMs: Long = 500,
        /** The relay the pool reads, when it is not [StalledRelay]. */
        readsOf: ((Harness) -> RelayReads)? = null,
        /**
         * Started at once over a store that writes only while [open]: the test decides when the
         * queue drains and how far ([drainTo]), and can make it stall again afterwards.
         */
        gated: Boolean = false,
    ) {
        val scope = CoroutineScope(SupervisorJob())
        val store = NostrSemanticsStore(InMemoryEventIndex())
        val processors = Processors()

        /** Whether the gated store is writing. */
        val open = MutableStateFlow(false)

        /** Where a [drainTo] stops: the store closes its own gate once the queue is down to it. */
        @Volatile
        private var drainLevel = 0

        private lateinit var pipeline: IngestPipeline

        private val gatedStore =
            object : IEventStore by store {
                override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
                    open.first { it }
                    val written = store.batchInsert(events)
                    // Closed from in here, after each batch: a test polling the depth from outside
                    // overshoots by however many batches a poll interval lets through.
                    if (pipeline.queued.get() <= drainLevel) open.value = false
                    return written
                }
            }

        /** Never started unless [gated], so nothing drains it and filling it parks every submit after. */
        // Gated: a small queue, since the gated tests fill and drain it on every page.
        val ingest =
            IngestPipeline(if (gated) gatedStore else store, IngestTuning(concurrency = 1, batch = WORKER_BATCH, queue = if (gated) GATED_QUEUE else null), null, null, scope, null, null)
                .also {
                    pipeline = it
                    if (gated) it.start()
                }

        /** Let the gated store write until the queue holds at most [level] events (one worker batch fewer at worst), then stall it again. */
        suspend fun drainTo(level: Int) {
            if (ingest.queued.get() <= level) return
            drainLevel = level
            open.value = true
            open.first { !it }
        }

        val relay =
            StalledRelay(scope, end, handsAnEvent) {
                if (fillsDuringThePage) fillIngest()
                if (parksAForeignProducer) {
                    scope.launch { ingest.submit(event(FOREIGN), skipVerify = true, origin = IngestOrigin(streams.single().urls.single())) }
                    delay(HOOK_SETTLE_MS)
                }
            }

        val reads: RelayReads = readsOf?.invoke(this) ?: relay

        val streams =
            listOf(
                SyncStream(
                    name = "content",
                    dir = SyncDirection.DOWN,
                    filter = Filter(kinds = listOf(KIND)),
                    urls = listOf(RelayUrlNormalizer.normalize("wss://relay.example")),
                    // Trusted when gated: a started pipeline verifies an untrusted stream's events
                    // and drops these unsigned fixtures before they ever wait on the store.
                    trusted = gated,
                ),
            )

        val pool: VisitPool by lazy {
            val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
            val bands = SyncBands(null)
            VisitPool(
                reads = reads,
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
                pauseLimitMs = pauseLimitMs,
            )
        }

        /** Fill the queue to the brim, so the pool's next hand-off suspends. */
        suspend fun fillIngest() {
            repeat(ingest.capacity - ingest.queued.get()) { ingest.submit(event(FILL_BASE + it), skipVerify = true) }
        }

        /**
         * Hand [make]'s events to the walk's [onEvent] on their own coroutine until the walk's own
         * hook parks in the full queue — the burst itself filling it, as in production — and
         * return then. The rest of the flood goes in as the queue drains.
         */
        suspend fun floodUntilParked(
            onEvent: suspend (Event) -> Unit,
            make: (Int) -> Event,
        ) {
            val url = streams.single().urls.single()
            scope.launch { repeat(ingest.capacity + FLOOD_SPARE) { onEvent(make(it)) } }
            withTimeout(5_000) { while (ingest.parkedOn(url) < 1) delay(5) }
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
            // The parked hook is as often a tail's or an audit's; the walk then sees nothing at all.
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
            // Nothing of ours is parked, so UNPAGEABLE means what quartz says it means.
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
            // The relay's sentence came through the same consumer, so it was not parked when said.
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
            // Skipped like a refused dial permit: nothing recorded, the revisit brings it back.
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
            // The count lives in the pipeline, so a producer the pool does not own is covered too.
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
    fun `a walk our queue cut mid-page pauses and resumes from the oldest event it reached`() =
        runBlocking {
            // A cut walk re-asked from the top next visit would download the burst again.
            var burst: BurstRelay? = null
            val h = Harness(PagedFetchResult.End.IDLE, pauseLimitMs = 10_000, readsOf = { BurstRelay(it, listOf(30, 20), parked = 10).also { r -> burst = r } })
            try {
                h.pool.start()
                val counts = h.countsOnce { burst!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                val asked = burst!!.asked
                assertEquals(2, asked.size, "one REQ for the burst, one to resume it")
                assertEquals(event(10).createdAt, asked[1].until, "resumed at the second the walk stopped in, inclusive")
                assertEquals(0L, counts["abortedVisits"])
                assertEquals(1L, counts["walksPausedByIngest"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `a first page parked in a queue that drains is resumed instead of aborted`() =
        runBlocking {
            var burst: BurstRelay? = null
            val h = Harness(PagedFetchResult.End.UNPAGEABLE, pauseLimitMs = 10_000, readsOf = { BurstRelay(it, emptyList(), parked = 10).also { r -> burst = r } })
            try {
                h.pool.start()
                val counts = h.countsOnce { burst!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                assertEquals(2, burst!!.asked.size, "the parked page is asked again, not abandoned")
                assertEquals(event(10).createdAt, burst!!.asked[1].until)
                assertEquals(0L, counts["abortedVisits"])
                assertEquals(0L, counts["abortedBackpressured"])
            } finally {
                h.close()
            }
        }

    /**
     * Floods every page at ONE second until the walk's hook parks, the queue draining a moment
     * after each: a burst denser in one `created_at` second than half the queue. Each re-ask is inclusive at that second, so a
     * resume that does not require the cursor to move re-asks the same ground forever.
     */
    private class DenseRelay(
        private val h: Harness,
        /** What every event carries: one second, or a future one the cursor ignores. */
        private val createdAt: Long,
    ) : RelayReads {
        val asked = CopyOnWriteArrayList<Filter>()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            asked += filter
            // Bounds the test, not the pool: a livelock reads as six asks and no abort.
            if (asked.size > 5) return PagedFetchResult(0, PagedFetchResult.End.DRAINED)
            val n = asked.size
            h.floodUntilParked(onEvent) { event(PAGE_BASE * n + it, createdAt) }
            h.scope.launch {
                delay(HOOK_SETTLE_MS)
                h.drainTo(0)
            }
            return PagedFetchResult(2, PagedFetchResult.End.IDLE)
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

    /** One page: [delivered] events, then a hook parked in a full queue; [afterTheStall] runs meanwhile. Later pages drain. */
    private class OneStallRelay(
        private val h: Harness,
        private val delivered: Int,
        private val afterTheStall: suspend () -> Unit,
    ) : RelayReads {
        val asked = CopyOnWriteArrayList<Filter>()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            asked += filter
            if (asked.size > 1) return PagedFetchResult(0, PagedFetchResult.End.DRAINED)
            repeat(delivered) { onEvent(event(40 - it)) }
            h.floodUntilParked(onEvent) { event(PAGE_BASE + it, event(10).createdAt) }
            h.scope.launch { afterTheStall() }
            return PagedFetchResult(delivered, if (delivered == 0) PagedFetchResult.End.UNPAGEABLE else PagedFetchResult.End.IDLE)
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

    @Test
    fun `a resume that cannot move the cursor ends the walk instead of re-asking forever`() =
        runBlocking {
            var relay: DenseRelay? = null
            val h = Harness(PagedFetchResult.End.IDLE, pauseLimitMs = 10_000, gated = true, readsOf = { DenseRelay(it, event(10).createdAt).also { r -> relay = r } })
            try {
                h.pool.start()
                val counts = h.countsOnce { relay!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                assertEquals(2, relay!!.asked.size, "one resume into the dense second, then the walk ends: the cursor did not move")
                assertEquals(1L, counts["abortedBackpressured"], "ended on our queue, not the relay")
            } finally {
                h.close()
            }
        }

    @Test
    fun `a stalled page with no plausible cursor is not resumed from the top`() =
        runBlocking {
            // A page of future-dated events never moves the cursor, so a resume would re-ask the whole ask.
            var relay: DenseRelay? = null
            val h = Harness(PagedFetchResult.End.IDLE, pauseLimitMs = 10_000, gated = true, readsOf = { DenseRelay(it, FUTURE).also { r -> relay = r } })
            try {
                h.pool.start()
                val counts = h.countsOnce { relay!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                assertEquals(1, relay!!.asked.size, "nothing to resume from")
                assertEquals(1L, counts["abortedBackpressured"])
            } finally {
                h.close()
            }
        }

    @Test
    fun `a pause that times out after the hook cleared is still our stall`() =
        runBlocking {
            // The hook gets in, but the queue stays above half for the whole pause: by the time the
            // walk gives up nothing of ours is parked, and the ending must not be read then.
            var h: Harness? = null
            val harness =
                Harness(
                    PagedFetchResult.End.UNPAGEABLE,
                    pauseLimitMs = 1_500,
                    gated = true,
                    readsOf = {
                        OneStallRelay(it, delivered = 0) {
                            delay(HOOK_SETTLE_MS)
                            h!!.drainTo(h!!.ingest.capacity * 7 / 10)
                        }
                    },
                )
            h = harness
            try {
                harness.pool.start()
                val counts = harness.countsAfterTheVisit()
                assertEquals(1L, counts["abortedBackpressured"])
                assertEquals(0L, counts["abortedUnpageable"], "the relay answered; our queue did not drain")
            } finally {
                harness.close()
            }
        }

    @Test
    fun `a walk whose pause ran out ends the visit while the queue is still backed up`() =
        runBlocking {
            // Delivered events, so the walk is a cut, not a refusal: it used to go on to the next
            // leg and band, each parking again against the same queue while holding the permit.
            var h: Harness? = null
            var relay: OneStallRelay? = null
            val harness =
                Harness(
                    PagedFetchResult.End.IDLE,
                    pauseLimitMs = 500,
                    gated = true,
                    readsOf = {
                        OneStallRelay(it, delivered = 2) {
                            // After the pause ran out: enough for the walk's own events to be stored, not for headroom.
                            delay(1_500)
                            h!!.drainTo(h!!.ingest.capacity * 9 / 10)
                        }.also { r -> relay = r }
                    },
                )
            h = harness
            try {
                harness.pool.start()
                val counts = harness.countsOnce { relay!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                assertEquals(1, relay!!.asked.size, "no further ask against a queue with no room")
                assertEquals(1L, counts["abortedBackpressured"])
                assertEquals(0L, counts["abortedGaveUp"], "the wait was ours, not the relay's silence")
            } finally {
                harness.close()
            }
        }

    @Test
    fun `a resume does not deliver the boundary second's events twice`() =
        runBlocking {
            // The re-ask is inclusive at the second the walk stopped in, so the relay serves that
            // second's events again; handed on twice, they are counted (and submitted) twice.
            var burst: BurstRelay? = null
            val h = Harness(PagedFetchResult.End.IDLE, pauseLimitMs = 10_000, readsOf = { BurstRelay(it, listOf(30, 20), parked = 10, reServes = true).also { r -> burst = r } })
            try {
                h.pool.start()
                val counts = h.countsOnce { burst!!.tails.isNotEmpty() || (it["abortedVisits"] ?: 0L) >= 1L }
                assertEquals(2, burst!!.asked.size)
                assertEquals(4L, counts["poolReceived"], "30, 20, 10 and the second's other event, each once")
            } finally {
                h.close()
            }
        }

    @Test
    fun `refusedOutright and stalledByUs agree on which endings are refusals at all`() {
        // Every ending a stall can be mistaken for is one that aborts; the converse is false.
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
        private const val PAGE_BASE = 3_000_000

        /** Events a flood hands on past the queue's capacity, so it outlasts one worker batch. */
        private const val FLOOD_SPARE = 32

        private const val WORKER_BATCH = 16

        private const val GATED_QUEUE = 256
        private const val SAME_SECOND = 4_000_000

        /** A `created_at` in 2100: the walk's cursor ignores it as implausible. */
        private const val FUTURE = 4_102_444_800L

        private fun idOf(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("backpressure-$n".toByteArray()))

        fun event(
            n: Int,
            createdAt: Long = 1_700_000_000L + n,
        ) = Event(
            id = idOf(n),
            pubKey = "a1".repeat(32),
            createdAt = createdAt,
            kind = KIND,
            tags = emptyArray(),
            content = "",
            sig = "b2".repeat(32),
        )
    }
}
