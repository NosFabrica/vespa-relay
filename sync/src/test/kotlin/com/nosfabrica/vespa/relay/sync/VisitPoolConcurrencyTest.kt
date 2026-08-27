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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * THE POOL'S OWN SCHEDULING, driven without a network.
 *
 * Everything the pool decides — which units run together, which exclude each
 * other, whose budget a tail costs, what a socket is shared by — had no test
 * before [RelayReads] existed, because `fetchAllPages` is a quartz extension
 * and a pool holding a `NostrClient` can only be tested by being one. Two
 * defects shipped into this branch behind that gap in one afternoon, both
 * found by reading rather than by running.
 *
 * So this drives the real [VisitPool] over a fake relay and asserts the
 * invariant the unit of work exists for: **many streams may work one relay at
 * once; one stream sees that relay in one state at a time.**
 */
class VisitPoolConcurrencyTest {
    private val url = RelayUrlNormalizer.normalize("wss://a.example")

    /**
     * A relay that answers pages when told to, and remembers what it was asked.
     *
     * `page` parks on a channel so a test can hold several walks open at once
     * and look at what is in flight — which is the only way to assert
     * concurrency rather than merely completion.
     */
    private class FakeRelay : RelayReads {
        val paging = Channel<Filter>(Channel.UNLIMITED)
        val release = Channel<Unit>(Channel.UNLIMITED)
        val inFlight = AtomicInteger()
        val peak = AtomicInteger()
        val tails = ConcurrentHashMap<String, List<Filter>>()

        override suspend fun page(
            url: NormalizedRelayUrl,
            filter: Filter,
            idleTimeoutMs: Long,
            onEvent: suspend (Event) -> Unit,
        ): PagedFetchResult {
            // COUNT FIRST, THEN RECORD THE PEAK. `updateAndGet` re-runs its
            // lambda whenever the CAS loses, so an increment INSIDE it is
            // applied once per attempt — two workers entering together left
            // `inFlight` permanently one too high, and this fake exists to
            // count exactly that. The same slip was fixed in `VisitQueueTest`
            // and missed here, which is why the note is on both.
            val now = inFlight.incrementAndGet()
            peak.updateAndGet { was -> maxOf(was, now) }
            paging.send(filter)
            release.receive()
            inFlight.decrementAndGet()
            // DRAINED with nothing downloaded: an honest empty relay, which is
            // not a refusal — see `VisitPool.refusedOutright` — so the visit
            // carries on to its tail.
            return PagedFetchResult(0, PagedFetchResult.End.DRAINED)
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

    /** Counts what is claimed, so "one socket, shared" is an assertion and not a comment. */
    private class CountingSockets : Sockets {
        val held = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
        val peak = AtomicInteger()

        override fun claim(url: NormalizedRelayUrl) {
            val n = held.computeIfAbsent(url) { AtomicInteger() }.incrementAndGet()
            peak.updateAndGet { was -> maxOf(was, n) }
        }

        override fun release(url: NormalizedRelayUrl) {
            held[url]?.decrementAndGet()
        }
    }

    private fun streamNamed(
        name: String,
        kind: Int,
    ) = SyncStream(
        name = name,
        dir = SyncDirection.DOWN,
        filter = Filter(kinds = listOf(kind), limit = 10),
        urls = listOf(url),
        trusted = false,
    )

    @Test
    fun `two streams work one relay at once, over one socket, each with its own tail`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val relay = FakeRelay()
            val sockets = CountingSockets()
            // Never dialled: the healer's queue is empty and no stream sets
            // `negentropySyncThePastSeconds`, so neither the healer nor the
            // pager reaches a socket. It exists to satisfy their constructors.
            val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
            val bands = SyncBands(null)
            val streams = listOf(streamNamed("content", 1), streamNamed("indexers", 7))
            val ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null)
            ingest.start()
            val pool =
                VisitPool(
                    reads = relay,
                    bands = bands,
                    ingest = ingest,
                    pager =
                        NegentropyPager(
                            StoreWindowIndex(store),
                            ClientWindowSync(client, refused = RefusedIds.disabled()),
                            SweepState(null),
                            NegPageTuning(target = 5_000, minTarget = 500, maxTarget = 50_000, slackSeconds = 60),
                        ),
                    healer = Healer(client, store, HealQueue(), WriteCapability(), RefusedIds.disabled(), null),
                    sockets = sockets,
                    scope = scope,
                    rosterBuilder = RosterBuilder(store = store, streams = streams, bands = bands),
                    streams = streams,
                    progress = Processors().of("visits"),
                    workers = 4,
                )
            try {
                pool.start()
                // BOTH STREAMS ON THE ONE RELAY AT THE SAME TIME. Each parks
                // inside its own page, so seeing two means two units are live
                // on this url — the thing the (relay, stream) unit exists for.
                val asked =
                    withTimeout(10_000) {
                        setOf(relay.paging.receive().kinds, relay.paging.receive().kinds)
                    }
                assertEquals(setOf(listOf(1), listOf(7)), asked, "one relay, both streams' filters, at once")
                assertEquals(2, relay.inFlight.get(), "both parked inside their walk")

                // ONE SOCKET, SHARED. `RelaySockets` refcounts, so two
                // concurrent units on a url are two claims and one connection
                // — the property that makes the split cost no extra dials.
                assertEquals(2, sockets.peak.get(), "two claims…")
                assertEquals(1, sockets.held.size, "…on one url")

                relay.release.send(Unit)
                relay.release.send(Unit)

                // ONE TAIL PER UNIT, carrying only that stream's filter. It
                // used to be one subscription per relay carrying every wanting
                // stream's filters, which is why `maxLiveConcurrency` could
                // only ever be an upper bound on the sockets held.
                withTimeout(10_000) {
                    while (relay.tails.size < 2) kotlinx.coroutines.delay(20)
                }
                assertEquals(2, relay.tails.size, "a subscription each, not one shared")
                assertEquals(
                    setOf(listOf(1), listOf(7)),
                    relay.tails.values
                        .map { it.single().kinds }
                        .toSet(),
                    "and each carries its own stream's filter alone",
                )
                assertTrue(relay.peak.get() >= 2, "the walks overlapped rather than queueing behind one another")
            } finally {
                scope.cancel()
            }
        }
}
