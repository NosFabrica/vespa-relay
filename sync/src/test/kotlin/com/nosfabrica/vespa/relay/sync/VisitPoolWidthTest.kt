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
 * THE CONVERGENCE FIX, END TO END: a relay that rejects an over-wide filter
 * outright is asked again in chunks, and completes.
 *
 * Nine relays on `vespa-eventstore-staging` refuse this router's 139-kind
 * `contentViaOutbox` ask rather than trimming it. A refused walk ends the visit
 * ([VisitPool.refusedOutright]), and the next visit re-asks the identical
 * filter — so those relays could never complete a single ask, however many
 * times the pool visited them. That is a livelock, not a slow path, and the
 * unit tests beside this one ([FilterWidthsTest]) can only show the arithmetic;
 * only driving the real pool can show that the arithmetic is reached, that the
 * chunks actually go out, and that the visit finishes.
 */
class VisitPoolWidthTest {
    private val url = RelayUrlNormalizer.normalize("wss://purplerelay.com")

    /** Every kind the stream asks for — small enough to assert on, wide enough to chunk. */
    private val kinds = listOf(1, 6, 7, 16, 1111)

    /**
     * A relay that refuses any filter naming more than [cap] kinds, the way the
     * measured ones do: a `CLOSED` with nothing delivered, and a sentence
     * naming the limit.
     *
     * It is its own [RelayComplaints] so the DATING contract is exercised
     * rather than stubbed out — the sentence is stamped when the refusal is
     * produced, so a walk that was never refused reads back nothing, which is
     * what stops an ordinary quiet relay from being narrowed.
     */
    private class WidthCappedRelay(
        private val cap: Int,
        /** What it says when it refuses — a width complaint, or something else entirely. */
        private val says: String = "invalid: too many kinds (max $cap)",
        /** …and how the walk ends. A width refusal is a `CLOSED`; an auth wall is its own ending. */
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
            // DRAINED with nothing downloaded: an honest empty relay, which is
            // not a refusal, so the visit carries on to its tail.
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

    /** Claims nothing and counts nothing — this test is about what goes on the wire. */
    private object NoSockets : Sockets {
        override fun claim(url: NormalizedRelayUrl) = Unit

        override fun release(url: NormalizedRelayUrl) = Unit
    }

    /**
     * The real [VisitPool] over one fake relay — everything else is the plain
     * wiring the concurrency test already stands up, and it is here rather than
     * inline so the two cases below differ only in the relay they meet.
     */
    private fun poolOver(
        relay: WidthCappedRelay,
        store: NostrSemanticsStore,
        scope: CoroutineScope,
        processors: Processors,
    ): VisitPool {
        // Never dialled: nothing here queues a heal or sets
        // `negentropySyncThePastSeconds`, so neither the healer nor the pager
        // reaches a socket. It exists to satisfy their constructors.
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
                // The tail is the last thing a clean visit does, so its arrival
                // is the signal that the whole ask completed — which is the
                // claim under test: before the narrowing, this visit ended on
                // the first refusal and no tail was ever opened.
                withTimeout(10_000) {
                    while (relay.tails.isEmpty()) delay(20)
                }

                // THE FIRST ASK IS THE WHOLE FILTER, and it is refused. The
                // pool does not pre-split: a cap can only ever be the relay's,
                // so every relay on a roster is asked at full width once.
                assertEquals(kinds, relay.asked.first(), "the first ask is the stream's own filter, unsplit")

                // …AND THE RE-WALK COVERS EVERY KIND, in chunks the relay
                // accepts. Three of them at a cap of two — not the ceiling
                // rounded up, the kinds themselves.
                val chunks = relay.asked.drop(1).take(3)
                assertTrue(chunks.all { it.size <= 2 }, "every chunk is inside the relay's stated limit: $chunks")
                assertEquals(kinds, chunks.flatten(), "every kind asked for, in order, exactly once")

                // …AND THE TAIL PAYS THE SAME WIDTH. A live subscription
                // carrying the filter the relay just refused is one that
                // silently never delivers.
                val tail = relay.tails.values.first()
                assertTrue(tail.all { (it.kinds?.size ?: 0) <= 2 }, "the tail is chunked too: ${tail.map { it.kinds }}")
                assertEquals(kinds, tail.flatMap { it.kinds.orEmpty() })

                // ONE ABORT, AND IT IS NAMED. The first refusal is counted
                // whether or not the retry rescues the visit — a relay we had
                // to narrow to is a fact worth having on the card — and it is
                // counted under the ending quartz reported rather than lumped
                // into a total.
                // THE TAIL IS AT THE WIDTH THE RELAY TAKES, and it has to be
                // re-opened to get there: the cap is learned from a REFUSAL,
                // which is the roster changing nothing, so a tail whose
                // identity was the want set alone would keep the very filter
                // this relay refuses — on a subscription it had already closed
                // — while the pair went on reporting `tailed`.
                assertEquals(
                    1,
                    relay.tails.size,
                    "one live subscription, re-opened at the learned width rather than left at the refused one",
                )

                // AND THE VISIT DID NOT ABORT. `abortedVisits` counts visits
                // that ENDED early, so a refusal the pool took down itself must
                // not appear there: on this deployment that number is the
                // convergence measure, and inflating it with refusals that were
                // rescued would make the fix look like the fault. What the
                // narrowing leaves behind instead is `narrowedRelays`, which is
                // not a fault at all.
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
            // THE OTHER HALF OF THE GATE, and the more important one: 50 relays
            // on the same deployment refuse with `auth-required:` and 21 are
            // outright blocked. Chunking THOSE asks would spend three extra
            // round trips per leg on relays that will never serve us, forever.
            // So the narrowing is driven by the sentence and nothing else, and
            // what a refusal outside it earns is the thing that was missing:
            // a counter with its own name, and a line.
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
                // ONE ASK, NOT FOUR. The relay is asked at full width and the
                // visit ends there — the chunks that a width refusal earns are
                // exactly what this refusal must not.
                assertTrue(relay.asked.all { it == kinds }, "no chunking was attempted: ${relay.asked}")
            } finally {
                scope.cancel()
            }
        }
}
