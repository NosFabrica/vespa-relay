/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The router: a strfry-style down-mirror. It keeps a live REQ subscription
 * open against each configured upstream and writes every matching event into
 * the same store the relay serves, so what the network publishes shows up in
 * our search. With a backfill window it first negentropy-reconciles history,
 * then the live tail keeps it current.
 *
 * Structure follows geode's MirrorWorker:
 *  - one [NostrClient] over OkHttp owns every upstream connection (reconnect,
 *    backoff and re-subscribe on drop are quartz's job, not ours);
 *  - [SubscriptionListener.onEvent] cannot suspend, so events land on an
 *    unbounded [inbound] channel and a single consumer coroutine does the
 *    suspending write through an [IngestQueue] — the same group-commit +
 *    fanout path a client publish takes;
 *  - each event is re-checked against its upstream's filter before ingest
 *    (the trust boundary), and events arriving on the wrong relay's id are
 *    dropped;
 *  - untrusted upstreams (the default) have every event's id + signature
 *    verified in the queue's parallel stage; `trusted = true` skips that.
 *
 * Only the down direction is implemented; [start] logs any `up`/`both` streams
 * it is skipping. [close] stops touching the store before the composition root
 * closes it.
 */
class MirrorRouter(
    private val store: IEventStore,
    private val config: RouterConfig,
    parentContext: CoroutineContext = SupervisorJob(),
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
    )

    private val scope = CoroutineScope(Dispatchers.IO + parentContext)

    // One OkHttp client for every upstream. A 120s ping surfaces half-open
    // connections (peer vanished without a FIN) as a failed pong, which routes
    // into quartz's reconnect path instead of a silently dead subscription.
    private val okhttp =
        OkHttpClient
            .Builder()
            .pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSec))
            .build()

    private val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

    // Events are verified in the queue's parallel stage (off the socket thread),
    // exactly as NostrRelayServer does for client publishes.
    private val ingest = IngestQueue(store = store, parentContext = scope.coroutineContext, verify = { it.verify() })

    private val inbound = Channel<Inbound>(Channel.UNLIMITED)
    private val accepted = AtomicLong()
    private val rejected = AtomicLong()

    private val upstreams = config.downUpstreams()

    fun start(): MirrorRouter {
        val skipped = config.skippedUpDirections()
        if (skipped.isNotEmpty()) {
            System.err.println("router: skipping unimplemented up/both streams: ${skipped.joinToString()} (only dir=down is mirrored)")
        }
        if (upstreams.isEmpty()) {
            System.err.println("router: no down upstreams configured; nothing to mirror")
            return this
        }

        // Single consumer: the only writer to the store from the router.
        scope.launch {
            for (msg in inbound) {
                ingest.submit(msg.event, msg.skipVerify) { outcome ->
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> accepted.incrementAndGet()
                        is IEventStore.InsertOutcome.Rejected -> rejected.incrementAndGet()
                    }
                }
            }
        }

        // Live tail: subscribe on each upstream from now forward. History, when
        // asked for, is the backfill's job — so the tail never floods on connect.
        val liveSince = nowSeconds()
        upstreams.forEachIndexed { i, up ->
            client.subscribe(
                subId = "vespa-mirror-$i",
                filters = mapOf(up.url to listOf(up.filter.copy(since = liveSince))),
                listener = downListener(up),
            )
        }
        client.connect()

        val backfillers = upstreams.filter { it.backfillSeconds > 0 }
        if (backfillers.isNotEmpty()) scope.launch { backfill(backfillers) }

        scope.launch { statsLoop() }

        System.err.println(
            "router: mirroring ${upstreams.size} upstream(s) across ${config.streams.count { it.dir == MirrorDirection.DOWN || it.dir == MirrorDirection.BOTH }} down stream(s)" +
                if (backfillers.isNotEmpty()) "; backfilling ${backfillers.size}" else "",
        )
        return this
    }

    private fun downListener(up: MirrorUpstream): SubscriptionListener =
        object : SubscriptionListener {
            override fun onEvent(
                event: Event,
                isLive: Boolean,
                relay: NormalizedRelayUrl,
                forFilters: List<Filter>?,
            ) {
                // Bind trust to the relay we dialed, not the subscription id, and
                // re-check scope so a broken upstream can't widen what we ingest.
                if (relay != up.url) return
                if (!up.filter.match(event)) return
                inbound.trySend(Inbound(event, up.trusted))
            }

            override fun onCannotConnect(
                relay: NormalizedRelayUrl,
                message: String,
                forFilters: List<Filter>?,
            ) {
                System.err.println("router: cannot connect ${up.url.url}: $message")
            }
        }

    /**
     * One-shot historical catch-up per upstream: negentropy-reconcile the
     * `[now - backfillSeconds, now]` window against what we already hold and
     * download only the diff. quartz falls back to paged REQ automatically for
     * upstreams without NIP-77. Downloaded events funnel through the same
     * [inbound] channel, so ingest, verification, and dedup are identical to
     * the live path. Failures are logged, never fatal — the live tail carries on.
     */
    private suspend fun backfill(ups: List<MirrorUpstream>) {
        for (up in ups) {
            if (!scope.isActive) return
            val until = nowSeconds()
            val since = until - up.backfillSeconds
            val window = up.filter.copy(since = since, until = until)
            try {
                val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(window))
                val result =
                    client.negentropySyncOrFetch(
                        relay = up.url,
                        filter = window,
                        localEntries = local,
                        onEvent = { event ->
                            if (up.filter.match(event)) inbound.trySend(Inbound(event, up.trusted))
                        },
                    )
                System.err.println(
                    "router: backfill ${up.url.url} downloaded ${result.downloaded}" +
                        if (result.pagedFallback) " (paged REQ fallback — no NIP-77)" else " (negentropy)",
                )
            } catch (e: Exception) {
                System.err.println("router: backfill ${up.url.url} failed: ${e.message}")
            }
        }
    }

    private suspend fun statsLoop() {
        while (scope.isActive) {
            delay(60_000)
            System.err.println("router: ingested ${accepted.get()} accepted, ${rejected.get()} rejected; ${client.connectedRelaysFlow().value.size}/${upstreams.size} upstreams connected")
        }
    }

    /** Accepted/rejected counters, for tests and a final log line. */
    fun stats(): Pair<Long, Long> = accepted.get() to rejected.get()

    /** Number of live down-upstreams being mirrored. */
    fun upstreamCount(): Int = upstreams.size

    override fun close() {
        upstreams.indices.forEach { runCatching { client.unsubscribe("vespa-mirror-$it") } }
        runCatching { client.close() }
        inbound.close()
        runCatching { ingest.close() }
        scope.cancel()
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        System.err.println("router: stopped (${accepted.get()} accepted, ${rejected.get()} rejected)")
    }

    private fun nowSeconds(): Long = System.currentTimeMillis() / 1000
}
