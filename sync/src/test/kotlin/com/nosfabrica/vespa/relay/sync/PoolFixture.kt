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
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/** The real [VisitPool] over a scripted relay, for the tests that need a whole visit to run. */
internal class PoolFixture(
    val scope: CoroutineScope,
    val streams: List<SyncStream>,
    /** Answers each walk; the default is an honest empty relay. */
    val answer: suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult = { _, _ -> PagedFetchResult(0, PagedFetchResult.End.DRAINED) },
    val bands: SyncBands = SyncBands(null),
) : RelayReads {
    val store = NostrSemanticsStore(InMemoryEventIndex())
    val processors = Processors()
    val tails = ConcurrentHashMap<String, List<Filter>>()
    val asked = java.util.concurrent.CopyOnWriteArrayList<Filter>()

    override suspend fun page(
        url: NormalizedRelayUrl,
        filter: Filter,
        idleTimeoutMs: Long,
        onEvent: suspend (Event) -> Unit,
    ): PagedFetchResult {
        asked += filter
        return answer(filter, onEvent)
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

    /** The pool's visit counters by name. */
    fun counts(): Map<String, Long> =
        processors
            .snapshot()
            .single { it.name == "visits" }
            .counts
            .associate { it.name to it.value }

    /** Waits for the first tail, the last thing a clean visit does. */
    suspend fun awaitTail(timeoutMs: Long = 10_000) {
        withTimeout(timeoutMs) {
            while (tails.isEmpty()) delay(10)
        }
    }

    fun pool(
        limits: PoolLimits = PoolLimits(emptyMap()),
        quietGiveUpMs: Long = LEG_QUIET_GIVE_UP_MS,
        roster: RosterSource = RosterBuilder(store = store, streams = streams, bands = bands),
    ): VisitPool {
        // Never dialled; it only satisfies the constructors.
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
        val ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null)
        ingest.start()
        return VisitPool(
            reads = this,
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
            rosterBuilder = roster,
            streams = streams,
            progress = processors.of("visits"),
            workers = 1,
            limits = limits,
            quietGiveUpMs = quietGiveUpMs,
        )
    }

    /** Claims nothing and counts nothing. */
    private object NoSockets : Sockets {
        override fun claim(url: NormalizedRelayUrl) = Unit

        override fun release(url: NormalizedRelayUrl) = Unit
    }

    companion object {
        /** A relay holding [corpus] that answers every walk in full, newest first. */
        fun holding(corpus: List<Event>): suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult =
            { filter, onEvent ->
                val hits = corpus.filter { filter.match(it) }.sortedByDescending { it.createdAt }
                hits.forEach { onEvent(it) }
                PagedFetchResult(hits.size, PagedFetchResult.End.DRAINED)
            }

        /** One whole visit of [streams] against [answer], recording into [bands]; the pool stays readable. */
        suspend fun visitOnce(
            streams: List<SyncStream>,
            bands: SyncBands,
            answer: suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult,
        ): VisitPool {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, streams, answer, bands)
                val pool = fixture.pool()
                pool.start()
                fixture.awaitTail()
                return pool
            } finally {
                scope.cancel()
            }
        }

        fun stream(
            name: String,
            url: NormalizedRelayUrl,
            filter: Filter,
        ) = SyncStream(name = name, dir = SyncDirection.DOWN, filter = filter, urls = listOf(url), trusted = false)

        /** An event with a unique id; never verified, since nothing here reads it back. */
        fun event(
            n: Int,
            createdAt: Long,
            kind: Int = 1,
            pubKey: String = "a1".repeat(32),
        ) = Event(
            id = Hex.encode(MessageDigest.getInstance("SHA-256").digest("fixture-$n".toByteArray())),
            pubKey = pubKey,
            createdAt = createdAt,
            kind = kind,
            tags = emptyArray(),
            content = "",
            sig = "b2".repeat(32),
        )
    }
}
