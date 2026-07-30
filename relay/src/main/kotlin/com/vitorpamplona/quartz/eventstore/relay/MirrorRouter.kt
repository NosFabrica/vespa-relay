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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The router: a strfry-style mirror. For each configured upstream it moves
 * events between that relay and the store the relay serves, so what the
 * network holds shows up in our search and (optionally) what we hold shows up
 * on the upstream.
 *
 * Down (`dir = down`/`both`): a live REQ subscription per upstream streams new
 * matching events into the store; with a backfill window it first
 * negentropy-reconciles history, then the tail keeps it current. Structure
 * follows geode's MirrorWorker — [SubscriptionListener.onEvent] can't suspend,
 * so events land on a bounded [inbound] channel (a blocking send backpressures
 * the download when ingest falls behind) and a pool of ingest workers drains
 * it in batches through [IEventStore.batchInsert] — the store's bulk feed
 * (the store serializes writes on one mutex, so throughput comes from the batch
 * size, not the worker count). Every event is re-checked against its filter;
 * untrusted upstreams have every signature verified off the download threads.
 *
 * Up (`dir = up`/`both`): periodically negentropy-reconciles the store against
 * the upstream and publishes the events the upstream is missing. Reconciliation
 * gives echo-suppression for free — an event we just pulled down from a relay
 * is one that relay already has, so it is never pushed back.
 *
 * While backfilling, a progress line reports overall percent and an ETA to
 * "useful" (backfill complete), so an operator can tell how long the initial
 * fill will take. [close] stops touching the store before the store closes.
 */
class MirrorRouter(
    private val store: IEventStore,
    private val config: RouterConfig,
    parentContext: CoroutineContext = SupervisorJob(),
    // When set (PARSE_AUDIT_FILE), every mirrored event is also run through
    // quartz's search-indexing parse to collect what quartz cannot read. Off by
    // default: it costs one extra parse per event. See [ParseAudit].
    private val audit: ParseAudit? = null,
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

    // Bounded, so a fast download (negentropy/paged can deliver >10k/s) can't
    // outrun Vespa ingest and pile millions of events onto the heap. When it
    // fills, the producing thread blocks in [offer] and the upstream download
    // throttles to the ingest rate — flat memory instead of an OOM.
    // Workers and batch come from config; the buffer is sized to a few batches
    // so producers block (backpressure) rather than pile events onto the heap.
    private val ingestWorkers = config.ingestConcurrency
    private val ingestBatch = config.ingestBatch
    private val inbound = Channel<Inbound>((ingestBatch * 4).coerceAtLeast(4096))
    private val accepted = AtomicLong()
    private val rejected = AtomicLong()
    private val pushed = AtomicLong()

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val progress = BackfillProgress()

    // Hard cap per negentropy session, from ROUTER_NEG_TIMEOUT_SECONDS.
    private val negTimeoutMs = config.negTimeoutSec * 1000

    fun start(): MirrorRouter {
        if (downUpstreams.isEmpty() && upUpstreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            return this
        }

        // A pool of consumers drains the channel in batches and writes each batch
        // through the store's bulk path (batchInsert -> parallel Vespa feed), which
        // is what actually exploits the store's ingest parallelism. Feeding it one
        // event at a time — the old path — left that parallelism unused.
        repeat(ingestWorkers) { scope.launch { ingestLoop() } }

        // Down live tail: subscribe on each upstream from now forward. History,
        // when asked for, is the backfill's job — so the tail never floods on connect.
        val liveSince = nowSeconds()
        downUpstreams.forEachIndexed { i, up ->
            client.subscribe(
                subId = "vespa-mirror-down-$i",
                filters = mapOf(up.url to listOf(up.filter.copy(since = liveSince))),
                listener = downListener(up),
            )
        }
        if (downUpstreams.isNotEmpty() || upUpstreams.isNotEmpty()) client.connect()

        val backfillers = downUpstreams.filter { it.backfillSeconds > 0 }
        if (backfillers.isNotEmpty()) {
            progress.begin(backfillers.size)
            scope.launch { backfill(backfillers) }
            scope.launch { progressLoop() }
        }

        // Up: one reconcile loop per up-upstream.
        upUpstreams.forEach { up -> scope.launch { upLoop(up) } }

        scope.launch { statsLoop() }

        System.err.println(
            "router: ${downUpstreams.size} down + ${upUpstreams.size} up upstream(s)" +
                (if (backfillers.isNotEmpty()) "; backfilling ${backfillers.size}" else "; live-tail only") +
                (if (upUpstreams.isNotEmpty()) "; up every ${config.upIntervalSec}s" else ""),
        )
        return this
    }

    // ---- ingest ------------------------------------------------------------

    /**
     * Hand an event to the ingest pool, blocking the caller if the buffer is
     * full. The negentropy/subscription callbacks that call this are not
     * suspending, so a blocking send is how backpressure reaches the download:
     * when ingest can't keep up, the producing thread parks here and the
     * upstream stops being drained until there's room.
     */
    private fun offer(
        event: Event,
        skipVerify: Boolean,
    ) {
        inbound.trySendBlocking(Inbound(event, skipVerify))
    }

    /**
     * Drain the channel in batches and write each batch through [IEventStore.batchInsert]
     * (the store's bulk path: replaceable-dedup preload + a parallel Vespa feed).
     * Signatures are verified here, off the download threads, and skipped for
     * trusted upstreams. [ingestWorkers] of these run at once.
     */
    private suspend fun ingestLoop() {
        val batch = ArrayList<Inbound>(ingestBatch)
        while (scope.isActive) {
            val first = inbound.receiveCatching().getOrNull() ?: break
            batch.clear()
            batch.add(first)
            while (batch.size < ingestBatch) {
                val next = inbound.tryReceive().getOrNull() ?: break
                batch.add(next)
            }
            val valid = ArrayList<Event>(batch.size)
            var verifyRejected = 0
            for (msg in batch) {
                if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                    valid.add(msg.event)
                } else {
                    verifyRejected++
                }
            }
            if (verifyRejected > 0) rejected.addAndGet(verifyRejected.toLong())
            if (valid.isEmpty()) continue
            // Before the batch write: the store feeds Vespa in parallel, so a parse
            // report raised inside batchInsert cannot be attributed to one event.
            // Inspecting here keeps each parse on this worker thread, where the
            // audit's ThreadLocal makes the attribution exact.
            audit?.let { for (event in valid) it.inspect(event) }
            runCatching { store.batchInsert(valid) }
                .onSuccess { outcomes ->
                    for (outcome in outcomes) {
                        when (outcome) {
                            is IEventStore.InsertOutcome.Accepted -> accepted.incrementAndGet()
                            is IEventStore.InsertOutcome.Rejected -> rejected.incrementAndGet()
                        }
                    }
                }.onFailure { rejected.addAndGet(valid.size.toLong()) }
        }
    }

    // ---- down --------------------------------------------------------------

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
                offer(event, up.trusted)
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
     * [inbound] channel, so ingest, verification, and dedup match the live path.
     * Progress is reported through [progress]. Failures are logged, never fatal.
     */
    private suspend fun backfill(ups: List<MirrorUpstream>) {
        // Concurrently: a fast upstream (ditto reconciles in seconds) shouldn't
        // wait on a slow or stuck one. Each backfill is time-boxed (below), so a
        // negentropy session that never converges gives up instead of blocking.
        coroutineScope {
            ups.forEachIndexed { idx, up -> launch { backfillOne(idx, up) } }
        }
    }

    private suspend fun backfillOne(
        idx: Int,
        up: MirrorUpstream,
    ) {
        val until = nowSeconds()
        val window = up.filter.copy(since = until - up.backfillSeconds, until = until)
        try {
            // Hard cap: some relays advertise NIP-77 but their negentropy never
            // converges (no download, no idle-timeout either). Bound each session
            // so it fails cleanly and the live tail carries that upstream instead.
            val result =
                withTimeoutOrNull(negTimeoutMs) {
                    val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(window))
                    client.negentropySyncOrFetch(
                        relay = up.url,
                        filter = window,
                        idleTimeoutMs = NEG_IDLE_MS,
                        localEntries = local,
                        onProgress = { needSoFar, downloaded -> progress.update(idx, needSoFar, downloaded) },
                        onEvent = { event -> if (up.filter.match(event)) offer(event, up.trusted) },
                    )
                }
            if (result == null) {
                progress.done(idx, 0)
                System.err.println("router: backfill ${up.url.url} timed out after ${negTimeoutMs / 1000}s — live tail continues")
            } else {
                progress.done(idx, result.downloaded)
                System.err.println(
                    "router: backfill ${up.url.url} downloaded ${result.downloaded}" +
                        if (result.pagedFallback) " (paged REQ fallback — no NIP-77)" else " (negentropy)",
                )
            }
        } catch (e: Exception) {
            progress.done(idx, 0)
            System.err.println("router: backfill ${up.url.url} failed: ${e.message}")
        }
    }

    // ---- up ----------------------------------------------------------------

    /**
     * Push our matching events to an upstream that lacks them, then repeat every
     * [RouterConfig.upIntervalSec] to carry newly-arrived local events. Each pass
     * reconciles a few rounds until the upstream reports nothing more is missing.
     * `negentropyReconcile.onHaveIds` yields the ids the upstream needs; we load
     * those events from the store and publish them (paced, fire-and-forget).
     */
    private suspend fun upLoop(up: MirrorUpstream) {
        while (scope.isActive) {
            try {
                var rounds = 0
                var pushedThisPass: Long
                var pushedThisWindow = 0L
                var timedOut = false
                do {
                    pushedThisPass = 0
                    val completed =
                        withTimeoutOrNull(negTimeoutMs) {
                            val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(up.filter))
                            client.negentropyReconcile(
                                relay = up.url,
                                filter = up.filter,
                                localEntries = local,
                                idleTimeoutMs = NEG_IDLE_MS,
                                onHaveIds = { ids ->
                                    val events: List<Event> = store.query(Filter(ids = ids))
                                    for (event in events) {
                                        client.publish(event, setOf(up.url))
                                        pushed.incrementAndGet()
                                        pushedThisPass++
                                        delay(UP_PUBLISH_PACE_MS)
                                    }
                                },
                                onNeedIds = { /* up-only: we don't pull here, the down tail does */ },
                            )
                            true
                        }
                    timedOut = completed == null
                    pushedThisWindow += pushedThisPass
                    rounds++
                } while (pushedThisPass > 0 && !timedOut && rounds < UP_MAX_ROUNDS && scope.isActive)
                System.err.println(
                    "router: up ${up.url.url} pushed $pushedThisWindow event(s) upstream ($rounds round(s))" +
                        if (timedOut) " [reconcile timed out]" else "",
                )
            } catch (e: Exception) {
                System.err.println("router: up ${up.url.url} failed: ${e.message}")
            }
            delay(config.upIntervalSec * 1000)
        }
    }

    // ---- reporting ---------------------------------------------------------

    private suspend fun progressLoop() {
        while (scope.isActive) {
            delay(PROGRESS_INTERVAL_MS)
            val s = progress.snapshot()
            if (s.allDone) {
                System.err.println("router: backfill complete — ${s.downloaded} events from ${s.total} upstream(s) in ${fmtDuration(s.elapsedMs)}; live tail now streaming")
                return
            }
            // ETA from the average download rate since start — steadier than an
            // instantaneous window, which flickers to zero between negentropy pages.
            val elapsedSec = s.elapsedMs / 1000.0
            val avgRate = if (elapsedSec > 0) s.downloaded / elapsedSec else 0.0
            val remaining = (s.need - s.downloaded).coerceAtLeast(0)
            val etaSec = if (avgRate > 1) (remaining / avgRate).toLong() else -1
            System.err.println(
                "router: backfill ${s.done}/${s.total} upstream(s), ${s.downloaded}/${s.need} events (${s.percent()}%)" +
                    ", ${"%.0f".format(avgRate)}/s avg" +
                    (if (etaSec >= 0) ", ETA ~${fmtDuration(etaSec * 1000)} to useful" else ", ETA —"),
            )
        }
    }

    private suspend fun statsLoop() {
        while (scope.isActive) {
            delay(60_000)
            System.err.println(
                "router: ingested ${accepted.get()} accepted, ${rejected.get()} rejected" +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${pushed.get()} up" else "") +
                    "; ${client.connectedRelaysFlow().value.size}/${(downUpstreams + upUpstreams).map { it.url }.toSet().size} upstreams connected",
            )
        }
    }

    /** Accepted/rejected/pushed counters, for tests and a final log line. */
    fun stats(): Triple<Long, Long, Long> = Triple(accepted.get(), rejected.get(), pushed.get())

    /** Number of distinct upstreams (down + up) being mirrored. */
    fun upstreamCount(): Int = (downUpstreams + upUpstreams).map { it.url }.toSet().size

    override fun close() {
        downUpstreams.indices.forEach { runCatching { client.unsubscribe("vespa-mirror-down-$it") } }
        runCatching { client.close() }
        inbound.close()
        scope.cancel()
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        System.err.println("router: stopped (${accepted.get()} accepted, ${rejected.get()} rejected, ${pushed.get()} pushed)")
    }

    private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

    /**
     * Aggregate backfill progress across upstreams. Each upstream reports
     * `(needSoFar, downloaded)` as its negentropy reconciliation proceeds;
     * summing them gives an overall picture and a rate-based ETA. `needSoFar`
     * grows as reconciliation discovers ids, so early percentages are estimates
     * that firm up as the run proceeds.
     */
    private class BackfillProgress {
        private val need = java.util.concurrent.ConcurrentHashMap<Int, Long>()
        private val got = java.util.concurrent.ConcurrentHashMap<Int, Long>()
        private val finished = java.util.concurrent.ConcurrentHashMap<Int, Long>()

        @Volatile private var totalUpstreams = 0

        @Volatile private var startMs = 0L

        fun begin(n: Int) {
            totalUpstreams = n
            startMs = System.currentTimeMillis()
        }

        fun update(
            idx: Int,
            needSoFar: Int,
            downloaded: Int,
        ) {
            need[idx] = needSoFar.toLong()
            got[idx] = downloaded.toLong()
        }

        fun done(
            idx: Int,
            downloaded: Int,
        ) {
            finished[idx] = downloaded.toLong()
            got[idx] = downloaded.toLong()
            // A finished upstream's need is exactly what it downloaded.
            need[idx] = maxOf(need[idx] ?: 0L, downloaded.toLong())
        }

        fun snapshot(): Snapshot {
            val need = need.values.sum()
            val got = got.values.sum()
            return Snapshot(
                need = need,
                downloaded = got,
                done = finished.size,
                total = totalUpstreams,
                allDone = finished.size >= totalUpstreams && totalUpstreams > 0,
                elapsedMs = System.currentTimeMillis() - startMs,
            )
        }
    }

    private data class Snapshot(
        val need: Long,
        val downloaded: Long,
        val done: Int,
        val total: Int,
        val allDone: Boolean,
        val elapsedMs: Long,
    ) {
        fun percent(): Int = if (need <= 0) 0 else ((downloaded * 100) / need).coerceIn(0, 100).toInt()
    }

    companion object {
        private const val PROGRESS_INTERVAL_MS = 15_000L
        private const val UP_PUBLISH_PACE_MS = 40L
        private const val UP_MAX_ROUNDS = 8

        // Idle (no protocol frames for this long) aborts a negentropy session,
        // below the hard [negTimeoutMs] cap.
        private const val NEG_IDLE_MS = 30_000L

        private fun fmtDuration(ms: Long): String {
            val s = ms / 1000
            val h = s / 3600
            val m = (s % 3600) / 60
            val sec = s % 60
            return if (h > 0) "%d:%02d:%02d".format(h, m, sec) else "%d:%02d".format(m, sec)
        }
    }
}
