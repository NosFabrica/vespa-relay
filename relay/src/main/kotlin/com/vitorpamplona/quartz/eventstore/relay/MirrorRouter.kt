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

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.IngestStats
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.RelayLogger
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.count
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.TcpProber
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.Dispatcher
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
 * Dynamic (`relaySource = [ ... ]`): the stream has no configured relays. Every
 * refresh it reads each configured source out of our store ([RelayDiscovery]) —
 * relay lists, provider lists, relay hints on ordinary tags — and negentropy-syncs
 * the stream filter against every relay they name, all of them, with `concurrency`
 * pacing the fan-out rather than capping it. A set that size is synced on a period
 * rather than held open, so these streams have no live tail — the refresh *is* the
 * tail — and each relay's socket is dropped again once its sync returns.
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
    // How much of each filter's history we have already pulled from each relay,
    // so a paged relay is not re-read from scratch every restart. See [SyncCursors].
    private val cursors: SyncCursors = SyncCursors(null),
    // Answers NIP-42 challenges from upstreams that gate reads behind AUTH.
    // Null (the default) leaves challenges unanswered. See [RelayIdentity].
    private val signer: NostrSigner? = null,
    // ROUTER_WIRE_LOG: "" (errors only) / "sent" / "full". See [wireLog].
    private val wireLogMode: String = "",
    // Shared with the relay server: how slow client reads have become. Ingest
    // yields to them — see [ServingPressure].
    private val servingPressure: ServingPressure? = null,
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
            // The single most important number in the fan-out, and the least
            // obvious. OkHttp opens a websocket by ENQUEUING a call whose callback
            // runs loopReader() inline, and the Dispatcher counts a call as
            // running until its callback returns — so an open websocket holds one
            // of these slots for its entire life and never gives it back.
            //
            // At the stock 64 that is the real concurrency ceiling for the whole
            // router, whatever a stream's `concurrency` says: the permits are
            // handed out, the sockets queue in readyAsyncCalls, and the semaphore
            // stops throttling anything. Measured on a 20,340-relay outbox list:
            // 52 relays done and an ETA of 330 hours. Every static upstream also
            // holds one permanently, so they come straight off this budget.
            //
            // Must exceed (static upstreams + the sum of every stream's
            // `concurrency`) or the config silently does not mean what it says.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    // Per HOST, and every relay is a different host — this only
                    // ever bites when one host serves several of a list's urls.
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSec))
            .build()

    private val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

    // NIP-66. Watches EVERY connection this client makes — static live tails,
    // backfills, up-reconciles, dynamic fan-outs — so what we learn about a relay
    // does not depend on which code path dialled it, measures the round trips,
    // signs them as kind 30166 into this same store, and hands back a cheap
    // dead-relay set for the fan-out to skip.
    //
    // Built from what the router already holds, and only when there is an
    // identity to sign with: publishing relay quality is the whole point of
    // NIP-66, so a monitor that cannot sign would be a component configured,
    // silent, and doing nothing.
    private val monitor =
        signer?.let {
            RelayMonitor(
                client = client,
                store = store,
                scope = scope,
                signer = it,
                onError = { message -> System.err.println("router: $message") },
            )
        }

    // NIP-42. Relays that gate reads behind AUTH serve nothing until we answer
    // their challenge — and an unanswered challenge looks exactly like an
    // ordinary empty relay from here, so this is invisible when it is missing.
    // Attaching the authenticator is enough: it listens for AUTH on every
    // upstream, signs the 22242 back, and re-authenticates when a relay CLOSEs a
    // subscription with `auth-required`. Null when no key is configured, in
    // which case challenges are ignored exactly as before.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    // Bounded, so a fast download (negentropy/paged can deliver >10k/s) can't
    // outrun Vespa ingest and pile millions of events onto the heap. When it
    // fills, the producing thread blocks in [offer] and the upstream download
    // throttles to the ingest rate — flat memory instead of an OOM.
    // Workers and batch come from config; the buffer is sized to a few batches
    // so producers block (backpressure) rather than pile events onto the heap.
    private val ingestWorkers = config.ingestConcurrency

    /**
     * How many downloaded events may wait for ingest.
     *
     * Bounded at both ends, and the ceiling is the one that matters. This was
     * `ingestBatch * 4` with only a floor, so raising ROUTER_INGEST_BATCH to
     * 20000 — to cut per-batch round trips, which it did — silently sized the
     * queue at 80,000 events. With three streams and 50 relays transferring at
     * once that filled, and the heap went to 93% and then over:
     *
     *     health heap 8043/8608MB (93%) !! AT THE CEILING,
     *     ingest queue 80000/80000 FULL, 0 ev/s, 50 relay(s) transferring
     *     FATAL OutOfMemoryError killed thread DefaultDispatcher-worker-67
     *
     * Batch size and queue depth are separate concerns that happened to share a
     * knob: the batch decides how much work each mutex hold amortises, the queue
     * decides how much memory sits between download and write. Tying them meant
     * tuning throughput moved the memory ceiling, which is not a trade an
     * operator agreed to.
     *
     * The cap is deliberately below what the heap can hold, because this queue
     * is not the only claimant — the in-flight batches, the relay sockets and
     * the negentropy id sets all want the same heap.
     */
    private val inboundCapacity = (config.ingestBatch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

    /**
     * How many events one worker takes per pass — capped so the workers can
     * actually share the queue.
     *
     * [ingestLoop] drains up to this many from [inbound] per pass. If it exceeds
     * what the channel can hold, the first worker takes EVERYTHING and the rest
     * find an empty channel and idle: ingest concurrency collapses to one, and
     * that single worker then grinds the whole queue through the store's dedup
     * in one serial stall.
     *
     * Which is exactly what happened. ROUTER_INGEST_BATCH=20000 against a 16,384
     * capacity produced minutes of `queue 16384/16384 FULL … 0 ev/s` broken by
     * short bursts — an ingest that looked saturated and was mostly one thread
     * waiting on a very long batch.
     *
     * A worker may take at most its fair share of the channel, so every worker
     * can fill a batch from a full queue.
     */
    private val ingestBatch = config.ingestBatch.coerceAtMost((inboundCapacity / ingestWorkers).coerceAtLeast(1))
    private val negMinEvents = config.negMinEvents
    private val countTimeoutMs = config.countTimeoutMs

    /**
     * Relays that did not answer a NIP-45 COUNT, so we stop asking this run.
     *
     * COUNT is optional and widely unimplemented, and a relay that does not
     * support it is indistinguishable from one that is slow — both return null
     * after the timeout. Asking 20,000 relays once is a diagnostic; asking them
     * every cycle is [countTimeoutMs] of dead wait per relay per cycle.
     */
    private val countUnanswered =
        java.util.concurrent.ConcurrentHashMap
            .newKeySet<NormalizedRelayUrl>()

    private val inbound = Channel<Inbound>(inboundCapacity)

    /**
     * Threads the ingest workers own outright, which no producer can occupy.
     *
     * [offer] parks its caller when the channel is full — deliberate
     * backpressure — but it parks a thread from the SAME pool [ingestLoop] runs
     * on ([scope] is Dispatchers.IO, whose workers are the shared
     * `DefaultDispatcher` scheduler). Backpressure only works if the parked
     * thread is not the one that has to make room. It is not, until enough
     * producers park at once, and then it is a deadlock: every thread is waiting
     * for space that only a thread can create.
     *
     * That is not hypothetical. With 12-15 relays delivering concurrently:
     *
     *     ingest queue 8000/8000 FULL, 0 ev/s   (for minutes, permanently)
     *     ingested 1 accepted, 4 rejected       (five events, total)
     *
     * and a thread dump showing DefaultDispatcher workers parked in
     * BlockingCoroutine.joinBlocking — trySendBlocking's runBlocking, waiting on
     * a drain that could never be scheduled.
     *
     * A dedicated pool sized to the worker count is the smallest fix that makes
     * the invariant true rather than probable: however many producers park,
     * these threads are still free to drain.
     */
    private val ingestPool =
        java.util.concurrent.Executors
            .newFixedThreadPool(ingestWorkers) { r ->
                Thread(r, "vespa-relay-ingest").apply { isDaemon = true }
            }.asCoroutineDispatcher()

    // How full [inbound] is. Channel does not expose its depth, and this one
    // number decides whether the pipeline is starved or backpressured — the
    // question every stall this router has had came down to, and the one thing
    // nothing could answer without guessing from Vespa's access log.
    private val queued =
        java.util.concurrent.atomic
            .AtomicInteger()

    // OutOfMemoryError kills the thread that happens to allocate next and is
    // caught by nobody: four of them passed unnoticed while the router reported
    // healthy phases and a frozen counter. Counted here so the health line can
    // say the process is damaged rather than merely quiet.
    private val fatals = AtomicLong()
    private val accepted = AtomicLong()
    private val rejected = AtomicLong()
    private val pushed = AtomicLong()

    // Why events were rejected. Worth separating, because on a wide fan-out the
    // two are wildly different news: a bad signature means an upstream is
    // serving junk, while "the store already has this" is the expected result of
    // asking a thousand relays for the same replaceable profile — and the second
    // routinely outnumbers accepts. One `rejected` number for both reads like an
    // emergency when it is the system working.
    private val unverified = AtomicLong()
    private val rejectReasons = java.util.concurrent.ConcurrentHashMap<String, Long>()

    // Store failures already reported in full, so the raw-event dump stays one
    // per distinct defect however many events trip it. Ingest workers share it.
    private val poisonSeen =
        java.util.concurrent.ConcurrentHashMap
            .newKeySet<String>()

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val dynamicStreams = config.dynamicStreams()
    private val progress = BackfillProgress()

    // What every stream is doing right now. The old output only ever reported
    // things that had FINISHED, so the two longest phases — walking the local id
    // set and discovering relays — printed nothing at all. See [StreamPhases].
    private val phases = StreamPhases()

    // One stream reconciles at a time, across the static and dynamic paths both.
    //
    // A stream holds its whole local id set from the moment the snapshot starts
    // until its last relay finishes — not just for the walk — so concurrent
    // streams hold their sets SIMULTANEOUSLY. Measured: three of them at 48.9M
    // ids and 5.97 GiB with two still only ~57% walked, heading for ~73.5M ids
    // and roughly 9 GiB. Serialising makes the peak one stream's set instead of
    // every stream's sum.
    //
    // It costs little in wall clock. The streams were contending for the same
    // engine and the same heap, so running them together did not finish them
    // sooner — it made all three slow at once and risked the ceiling.
    private val streamGate = Semaphore(1)

    // The relays we hold a live subscription on. A dynamic sync drops its socket
    // when it finishes, and must not drop one of these out from under its tail.
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    // How many dynamic syncs are currently using each relay. Streams discover
    // from the same store, so two of them (an outbox and a NIP-85 one, say)
    // routinely land on the same relay at the same time — and whichever finished
    // first used to close the socket out from under the other, failing a sync
    // that was working. Only the last one out disconnects.
    private val inFlight = java.util.concurrent.ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /**
     * Relays with a transfer actually running, across every path.
     *
     * Not [inFlight], which is the dynamic path's socket refcount and is only
     * touched by [dynamicSyncOne]. Reusing it for the health line made that line
     * report `0 relay(s) transferring` while eleven static relays delivered
     * 16,000 events a second — the precise kind of confident wrong number the
     * health line exists to replace.
     */
    private val transferring =
        java.util.concurrent.atomic
            .AtomicInteger()

    /** Time-axis progress for every paged walk in flight, across both paths. */
    private val paging = PagingProgress()

    /**
     * Good events the store refused for structural reasons, and which nothing
     * will re-offer. Distinct from [rejected], most of which is the protocol
     * working: duplicates we already hold and signatures that were never valid.
     */
    private val lostToStore = AtomicLong()

    /**
     * Records dropped because an upstream that owns them stopped serving them.
     *
     * Counted separately and reported on the health line, because this is the
     * only number in the router that goes DOWN. Everything else it prints is
     * work done; this is data gone, and it should never be legible as a rounding
     * detail of a sync.
     */
    private val deleted = AtomicLong()

    /**
     * What actually goes down the wire, when the counters stop making sense.
     *
     * Tonight `purplepag.es` returned 3,137,680 events on one build and 601 on
     * the next, from the same code path and the same filter. Hand-walking the
     * relay with a throwaway script showed twelve full pages and no sign of
     * stopping — so the relay was willing and the client stopped, and there was
     * no way to see which REQ we sent last or what came back with it.
     *
     * [RelayLogger] already knew how to answer that and was simply never
     * constructed. Its error half — NOTICE, CLOSED, failed sends — is
     * unconditional and worth having on always: those are the relay telling us
     * why it stopped, and we have been discarding them. `full` adds every
     * command sent and message received, which is a line per event and belongs
     * only under a specific investigation.
     *
     * `LimitsMessage` is the one to watch: it carries `maxLimit` and
     * `maxSubscriptions`, the page cap we have been inferring from probes.
     */
    private val wireLog =
        when (wireLogMode) {
            "full", "sent" -> {
                // Lower the floor to match, or the switch does nothing. The sent
                // and received lines are DEBUG, and QUARTZ_LOG_LEVEL is WARN in
                // every deployment we run (quartz defaults to DEBUG, which logs a
                // line per malformed upstream profile). So ROUTER_WIRE_LOG=sent
                // was accepted, constructed its logger, and printed nothing —
                // a component configured, silent, and doing its job invisibly,
                // which is the failure this codebase keeps trying to design out.
                // Announced, because raising quartz's verbosity is not something
                // to do to an operator quietly.
                if (Log.minLevel > LogLevel.DEBUG) {
                    Log.minLevel = LogLevel.DEBUG
                    System.err.println(
                        "router: ROUTER_WIRE_LOG=$wireLogMode lowered the quartz log floor to DEBUG (was ${'$'}{LogLevel.WARN}) — this is verbose",
                    )
                }
                RelayLogger(client, debugSending = true, debugReceiving = wireLogMode == "full")
            }

            // Errors only, which need no floor change: NOTICE, CLOSED and failed
            // sends are logged at WARN and ERROR by RelayLogger regardless.
            else -> {
                RelayLogger(client, debugSending = false, debugReceiving = false)
            }
        }

    fun start(): MirrorRouter {
        if (downUpstreams.isEmpty() && upUpstreams.isEmpty() && dynamicStreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            return this
        }

        // A pool of consumers drains the channel in batches and writes each batch
        // through the store's bulk path (batchInsert -> parallel Vespa feed), which
        // is what actually exploits the store's ingest parallelism. Feeding it one
        // event at a time — the old path — left that parallelism unused.
        // Said out loud when it bites: an operator who set ROUTER_INGEST_BATCH
        // and silently got a different number would be tuning a knob that is not
        // connected, which is the failure this repo keeps producing.
        if (ingestBatch < config.ingestBatch) {
            System.err.println(
                "router: ROUTER_INGEST_BATCH=${config.ingestBatch} capped to $ingestBatch — " +
                    "$ingestWorkers worker(s) share a $inboundCapacity-event queue, and a batch bigger than " +
                    "one worker's share collapses ingest to a single thread",
            )
        }
        repeat(ingestWorkers) { scope.launch(ingestPool) { ingestLoop() } }

        // An OutOfMemoryError kills the thread that happens to allocate next and
        // is caught by nobody — not by `catch (e: Exception)`, which is what most
        // of this file uses. Four of them passed unnoticed in one run while the
        // phases still read healthy and the counters simply stopped moving. This
        // does not recover anything; it makes the damage visible instead of
        // leaving a silent process that looks merely quiet.
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, error ->
            if (error is VirtualMachineError) {
                fatals.incrementAndGet()
                System.err.println("router: FATAL ${error.javaClass.simpleName} killed thread ${thread.name} — the router is now degraded")
            }
            previous?.uncaughtException(thread, error)
        }
        scope.launch { healthLoop() }

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
        client.connect()

        // Every down upstream backfills: the stream's filter says how far, and a
        // filter naming no `since` is unbounded, exactly as NIP-01 reads it.
        val backfillers = downUpstreams
        if (backfillers.isNotEmpty()) {
            progress.begin(backfillers.size)
            scope.launch { backfill(backfillers) }
            scope.launch { progressLoop() }
        }

        // Up: one reconcile loop per up-upstream.
        upUpstreams.forEach { up -> scope.launch { upLoop(up) } }

        // Registered BEFORE anything runs: a configured stream must appear in the
        // report from the first tick, so silence can never be read as "not
        // configured" — which is exactly how two dynamic streams went unnoticed.
        downUpstreams.map { it.streamName }.distinct().forEach { phases.register(it) }
        dynamicStreams.forEach { phases.register(it.name) }

        // Dynamic: one refresh loop per stream, each discovering its own relays.
        dynamicStreams.forEach { stream -> scope.launch { dynamicLoop(stream) } }

        scope.launch { statsLoop() }

        System.err.println(
            "router: ${downUpstreams.size} down + ${upUpstreams.size} up relay(s)" +
                (if (backfillers.isNotEmpty()) "; backfilling ${backfillers.size}" else "; live-tail only") +
                (if (upUpstreams.isNotEmpty()) "; up every ${config.upIntervalSec}s" else "") +
                (
                    if (dynamicStreams.isNotEmpty()) {
                        "; ${dynamicStreams.size} dynamic stream(s): " +
                            dynamicStreams.joinToString { "${it.name} (${it.dynamic?.sources?.size} source(s))" }
                    } else {
                        ""
                    }
                ),
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
        queued.incrementAndGet()
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
            // Clients first. A batch's dedup and projection queries land in the
            // same engine a REQ does, and there is no way to reorder that queue
            // from here — only to stop adding to it. Zero while reads are
            // healthy, so the common case costs nothing.
            servingPressure?.backoffMs()?.takeIf { it > 0 }?.let { delay(it) }
            val first = inbound.receiveCatching().getOrNull() ?: break
            queued.decrementAndGet()
            batch.clear()
            batch.add(first)
            while (batch.size < ingestBatch) {
                val next = inbound.tryReceive().getOrNull() ?: break
                queued.decrementAndGet()
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
            if (verifyRejected > 0) {
                rejected.addAndGet(verifyRejected.toLong())
                unverified.addAndGet(verifyRejected.toLong())
            }
            if (valid.isEmpty()) continue
            // Before the batch write: the store feeds Vespa in parallel, so a parse
            // report raised inside batchInsert cannot be attributed to one event.
            // Inspecting here keeps each parse on this worker thread, where the
            // audit's ThreadLocal makes the attribution exact.
            audit?.let { for (event in valid) it.inspect(event) }
            insertIsolating(valid)
        }
    }

    /**
     * Write a batch, and if it throws, bisect it and write the halves.
     *
     * [IEventStore.batchInsert] fails as a unit: one event the store cannot handle
     * takes the whole batch with it. At the default batch size that is 999 good
     * events lost per bad one, permanently and with no retry — the loss is silent
     * except for a count, and the count is a multiple of the batch size rather
     * than a number of malformed events, which is itself misleading.
     *
     * Halving turns that into ~2·log2(n) extra writes on the rare failing batch
     * and isolates the offender to a single event. Re-writing the good halves is
     * safe: a batch that threw may have applied some of its events already, and
     * re-inserting those is just a duplicate the store rejects.
     *
     * The isolated event is then reported in full — that is the diagnostic this
     * exists for. A store-level throw has no other trace: it never reaches the
     * parse audit (which only covers the search-indexing path) and the exception
     * is caught here, so without this the raw event is unrecoverable.
     */
    private suspend fun insertIsolating(events: List<Event>) =
        insertBisecting(
            events = events,
            write = { store.batchInsert(it) },
            onOutcomes = { outcomes ->
                for (outcome in outcomes) {
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> {
                            accepted.incrementAndGet()
                        }

                        is IEventStore.InsertOutcome.Rejected -> {
                            rejected.incrementAndGet()
                            rejectReasons.merge(outcome.reason.take(48), 1L, Long::plus)
                        }
                    }
                }
            },
            onPoison = { event, e ->
                // Name the exception type too — a batch can fail with a
                // message-less throwable, and "store: null" tells nobody
                // anything about the event it just cost us.
                rejected.incrementAndGet()
                rejectReasons.merge("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L, Long::plus)
                reportPoison(event, e)
            },
            onGaveUp = { batch, e ->
                // Isolation ran out of budget, so these are counted but unnamed.
                // Tallied apart from the isolated ones on purpose: "we could not
                // say which" is a different fact from "this event is bad", and
                // reading them as one number hides a store-wide outage.
                rejected.addAndGet(batch.size.toLong())
                rejectReasons.merge("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong(), Long::plus)
                // These are LOST, not merely rejected. A bad signature is the
                // event's fault and dropping it is correct; a whole batch failing
                // structurally is the store's or the schema's fault, the events
                // were perfectly good, and nothing will ever offer them again.
                //
                // A schema drift dropped 2,336,288 events this way in one run
                // while every phase line read healthy — the total sat inside a
                // reason string in a stats line nobody was reading. Surfaced on
                // the health line so it cannot accumulate quietly again.
                lostToStore.addAndGet(batch.size.toLong())
            },
        )

    /**
     * Log an event the store threw on, once per distinct failure, with the raw
     * JSON. One line per occurrence would be a flood at mirror rates and the
     * hundredth copy of a defect teaches nothing the first did not, so the
     * signature is what is deduplicated — and [POISON_SAMPLE_LIMIT] caps even
     * that, because a genuinely novel corpus could otherwise print all day.
     */
    private fun reportPoison(
        event: Event,
        error: Throwable,
    ) {
        val signature = "${error.javaClass.name}: ${error.message}"
        if (!poisonSeen.add(signature) || poisonSeen.size > POISON_SAMPLE_LIMIT) return
        System.err.println(
            "router: store rejected event ${event.id} (kind ${event.kind}, pubkey ${event.pubKey}) — " +
                "${error.javaClass.simpleName}: ${error.message}\n" +
                "router: the event, verbatim: ${event.toJson().take(POISON_JSON_CHARS)}",
        )
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
     * stream's filter against what we already hold and
     * download only the diff. quartz falls back to paged REQ automatically for
     * upstreams without NIP-77. Downloaded events funnel through the same
     * [inbound] channel, so ingest, verification, and dedup match the live path.
     * Progress is reported through [progress]. Failures are logged, never fatal.
     */
    private suspend fun backfill(ups: List<MirrorUpstream>) {
        // ONE snapshot per STREAM, not one per relay.
        //
        // Every url in a stream shares that stream's filter instance, so a
        // per-relay snapshot walked the identical range once per url and threw
        // away N-1 byte-identical answers. Measured here: 7,683 visit pages
        // against a single `kind==0 or kind==10002` selection, ~6 minutes of
        // engine time, before one event had been downloaded — and none of it
        // visible, because `need` is unknown until the reconcile starts, so the
        // progress line reads "0/0 events (0%)" the whole time.
        //
        // [dynamicCycle] was fixed this way already; this is the same fix on the
        // path it missed. The trade is identical too: a relay reconciles against
        // the store as it was when its stream started, so an event a sibling
        // relay delivered in the meantime can be fetched twice. That was always
        // true — ingest is asynchronous, so a per-relay snapshot would not have
        // seen it either — and the store dedups on insert.
        coroutineScope {
            ups
                .withIndex()
                .groupBy { it.value.filter }
                .forEach { (filter, group) ->
                    launch {
                        val name = group.first().value.streamName
                        // Relays that will PAGE do not read the id set, so they do
                        // not wait for it. fetchAllPages takes a relay, a filter
                        // and a timeout — localEntries belongs to negentropySync
                        // alone — yet every paging relay used to sit through the
                        // whole walk for a set it never touched. That walk is 3:46
                        // for 14.9M ids and 10:36 for 43.7M, and a stream behind
                        // the gate waits for both.
                        //
                        // Who pages is decided by OVERLAP, not by cursor history
                        // — see [worthReconciling] for why that distinction cost
                        // 14.4M downloaded events to keep 9,878.
                        //
                        // Our own count is taken once for the stream, since every
                        // url here shares its filter, and it is the cheap half:
                        // one engine query against a per-relay round trip.
                        val ours = runCatching { store.count(filter) }.getOrNull() ?: 0
                        val (reconcilers, pagers) =
                            group.partitionSuspend { worthReconciling(it.value, filter, ours) }
                        System.err.println(
                            "router: $name ${reconcilers.size} relay(s) will reconcile, ${pagers.size} will fetch" +
                                " [sync=${group.first().value.sync.wire}]" +
                                " (we hold ${StreamPhases.fmtCount(ours)} matching event(s), floor $negMinEvents)",
                        )
                        val eventsEarly = AtomicLong()
                        // The pagers own the phase line only when there is nobody
                        // else to own it. When relays also reconcile, that path is
                        // the long pole and reports the id walk, which is the part
                        // an operator cannot otherwise see; the pagers' progress is
                        // on the `static backfill` line either way.
                        val pagersReport = reconcilers.isEmpty()
                        val pagedDone =
                            java.util.concurrent.atomic
                                .AtomicInteger()
                        val early =
                            if (pagers.isEmpty()) {
                                null
                            } else {
                                if (pagersReport) phases.set(name, StreamPhases.Phase.Fetching(0, pagers.size, 0))
                                scope.launch {
                                    // Refreshed on a tick, not only as relays
                                    // finish: twelve relays paging for an hour
                                    // complete almost never, and a window
                                    // percentage that only moves on completion is
                                    // the stale-phase bug this line was added to
                                    // end. Cancelled by the join below.
                                    val tick =
                                        if (!pagersReport) {
                                            null
                                        } else {
                                            launch {
                                                while (true) {
                                                    delay(PROGRESS_INTERVAL_MS)
                                                    phases.set(
                                                        name,
                                                        StreamPhases.Phase.Fetching(
                                                            pagedDone.get(),
                                                            pagers.size,
                                                            eventsEarly.get(),
                                                            paging.fraction(name),
                                                            paging.etaMs(name),
                                                            paging.reached(name),
                                                        ),
                                                    )
                                                }
                                            }
                                        }
                                    // coroutineScope, because `forEach { launch }`
                                    // inside scope.launch returns the instant the
                                    // children START. Cancelling the ticker after
                                    // it killed the ticker microseconds in — the
                                    // same mistake [dynamicCycle] already carries
                                    // a comment about. This awaits them.
                                    try {
                                        coroutineScope {
                                            pagers.forEach { (idx, up) ->
                                                launch {
                                                    // pageOne feeds [eventsEarly] as events land, so the
                                                    // ticker below has a number that moves. Adding its
                                                    // return value here instead left the line reading
                                                    // `0 event(s)` until the walk ENDED — for a walk that
                                                    // ran 17 minutes and took 7.5M events off one relay.
                                                    pageOne(idx, up, eventsEarly)
                                                    if (pagersReport) {
                                                        phases.set(
                                                            name,
                                                            StreamPhases.Phase.Fetching(
                                                                pagedDone.incrementAndGet(),
                                                                pagers.size,
                                                                eventsEarly.get(),
                                                                paging.fraction(name),
                                                                paging.etaMs(name),
                                                                paging.reached(name),
                                                            ),
                                                        )
                                                    }
                                                }
                                            }
                                        }
                                    } finally {
                                        tick?.cancel()
                                    }
                                }
                            }
                        if (reconcilers.isEmpty()) {
                            early?.join()
                            phases.set(name, StreamPhases.Phase.Idle(eventsEarly.get(), 0))
                            return@launch
                        }
                        phases.set(name, StreamPhases.Phase.Queued(reconcilers.size))
                        streamGate.withPermit {
                            val local = snapshotForStream(reconcilers.map { it.value }, filter)
                            // Awaited inside the permit: the id set stays live
                            // until the last relay is done with it, so releasing
                            // at the fan-out would let the next stream allocate
                            // its own on top of this one.
                            val done =
                                java.util.concurrent.atomic
                                    .AtomicInteger()
                            val events = AtomicLong()
                            phases.set(name, StreamPhases.Phase.Syncing(0, reconcilers.size, 0, 0, 0))
                            coroutineScope {
                                reconcilers.forEach { (idx, up) ->
                                    launch {
                                        val got = backfillOne(idx, up, local)
                                        events.addAndGet(got.toLong())
                                        // Reported per relay, because the snapshot
                                        // phase used to be the LAST thing this path
                                        // said: a stream that had finished still
                                        // read "snapshotting 100%" hours later, and
                                        // twice sent a diagnosis down the wrong path.
                                        phases.set(
                                            name,
                                            StreamPhases.Phase.Syncing(
                                                done.incrementAndGet(),
                                                reconcilers.size,
                                                events.get(),
                                                0,
                                                0,
                                            ),
                                        )
                                    }
                                }
                            }
                            early?.join()
                            phases.set(name, StreamPhases.Phase.Idle(events.get() + eventsEarly.get(), 0))
                        }
                    }
                }
        }
    }

    /**
     * The id walk, reporting its running count when the store can.
     *
     * The progress overload is a [VespaEventStore] capability, not part of
     * quartz's [IEventStore] contract — no other implementation needs a hook
     * only a mirror uses. A store without it still works; the phase simply
     * reports elapsed time and no count.
     */
    private suspend fun snapshotReporting(
        window: Filter,
        onProgress: (Int) -> Unit,
    ): List<IdAndTime> =
        when (store) {
            is VespaEventStore -> store.snapshotIdsForNegentropy(listOf(window), null, onProgress)
            else -> store.snapshotIdsForNegentropy(listOf(window))
        }

    /**
     * The local id set every relay in one stream reconciles against.
     *
     * Narrowed to what the hungriest of them still needs: once they all carry a
     * complete band, this is the sliver since the oldest ceiling rather than the
     * whole corpus. One relay that has never synced correctly widens it back —
     * that relay genuinely needs everything.
     */
    private class StreamSnapshot(
        val ids: List<IdAndTime>,
        /**
         * When the ids were read, in seconds. The coverage a reconcile earns is
         * measured from HERE, not from when a given relay's leg happened to
         * start: the comparison is against the store as it was at this instant,
         * and every relay in the stream shares it. Stamping the later leg start
         * would claim we had compared a window we never looked at.
         */
        val takenAt: Long,
    )

    /**
     * Page a relay's whole window, with no local id set involved.
     *
     * Same leg walk and same cursor bookkeeping as the reconcile path — it just
     * never touches the snapshot, so it runs while one is still being built.
     */
    private suspend fun pageOne(
        idx: Int,
        up: MirrorUpstream,
        live: AtomicLong,
    ): Int {
        val legs = cursors.legs(up.url, up.filter)
        if (legs.isEmpty()) {
            progress.done(idx, 0)
            return 0
        }
        var downloaded = 0
        transferring.incrementAndGet()
        return try {
            for (window in legs) {
                var seenMin: Long? = null
                var seenMax: Long? = null
                val walk = "${up.streamName}|${up.url.url}"
                // Counted HERE, as events arrive — not from [downloaded], which
                // fetchAllPages only assigns on return. Reporting that one from
                // inside the callback printed `0 event(s)` for the whole of a
                // walk that took 7,503,018 events off one relay, which is the
                // status line lying about the only thing it exists to say.
                var seenSoFar = 0
                paging.begin(walk, window.until ?: nowSeconds(), window.since ?: SyncCursors.PLAUSIBLE_FLOOR)
                downloaded +=
                    client.fetchAllPages(
                        up.url,
                        listOf(window),
                        NEG_IDLE_MS,
                        onNewPage = { until -> paging.mark(walk, until) },
                    ) { event ->
                        if (up.filter.match(event)) {
                            if (SyncCursors.isPlausible(event.createdAt)) {
                                seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                            }
                            offer(event, up.trusted)
                        }
                        seenSoFar++
                        live.incrementAndGet()
                        progress.update(idx, downloaded + seenSoFar, downloaded + seenSoFar)
                    }
                // paged = true: this walked a span, it did not reconcile a range,
                // so the band it earns is the span it saw and nothing more.
                paging.finish(walk)
                cursors.record(up.url, up.filter, seenMin, seenMax, paged = true)
            }
            progress.done(idx, downloaded)
            System.err.println("router: static backfill ${up.url.url} paged $downloaded (no snapshot needed)")
            downloaded
        } catch (e: Exception) {
            progress.done(idx, 0)
            System.err.println("router: static backfill ${up.url.url} paged fetch failed: ${e.message}")
            0
        } finally {
            transferring.decrementAndGet()
        }
    }

    /**
     * Reconcile against our id set, or just page the relay?
     *
     * Negentropy is worth its id exchange exactly to the extent that the two
     * sides already share data, and NOTHING ELSE decides it — not whether we
     * have met the relay before, which is what an earlier version of this asked.
     * A relay whose events we mostly hold is nearly free to reconcile and
     * ruinous to fetch; a relay whose events we lack transfers the same bytes
     * either way, and pays the id exchange on top.
     *
     * A stream that KNOWS which it is says so ([SyncMode]) and skips all of the
     * below. Sharing is a property of the kind, not of the volume: NIP-85
     * assertions put millions on both sides and share essentially none of them,
     * because each provider authors its own. Counts cannot see that, so a stream
     * whose relays do not mirror each other must declare `sync = "fetch"` rather
     * than be measured into the wrong answer.
     *
     * For `auto`, the measurement is two counts, ours and theirs, on the same
     * filter — a heuristic that assumes overlap tracks volume:
     *
     *  - **Ours below the floor** — we have nothing to reconcile against, so
     *    reconciling would transfer the relay's whole set anyway and build a
     *    snapshot to do it. Fetch. This is bootstrap, and it is why the indexer
     *    relays SHOULD fetch on a fresh store: they are what creates the overlap
     *    everything after them benefits from.
     *  - **Theirs below the floor** — a small relay is cheap to fetch outright,
     *    and cheaper than making it walk a set of ours it barely intersects.
     *  - **Both large** — reconcile. This is the 20,000-relay sweep, where the
     *    indexers have already given us the bulk of what each relay holds. It
     *    is also the case that cost 14.4M downloaded events to keep 9,878:
     *    `wss://profiles.nostr1.com` sent 5,099,996 profiles into a store that
     *    already had 12.28M.
     *
     * A relay that does not answer COUNT is reconciled, not fetched. Our side is
     * already known large by then, so overlap is the likely case; and if the
     * relay turns out to lack NIP-77 too, `negentropySyncOrFetch` falls back to
     * paging on its own. The failure mode of guessing wrong here is one
     * redundant id exchange, against a re-download of everything for guessing
     * wrong the other way.
     */
    private suspend fun worthReconciling(
        up: MirrorUpstream,
        filter: Filter,
        ours: Int,
    ): Boolean {
        // Declared beats measured: the operator knows whether this stream's
        // relays mirror each other, and no count can tell us.
        when (up.sync) {
            SyncMode.NEGENTROPY -> return true
            SyncMode.FETCH -> return false
            SyncMode.AUTO -> Unit
        }
        val url = up.url
        if (ours < negMinEvents) return false
        if (url in countUnanswered) return true
        val theirs = runCatching { client.count(url, filter, countTimeoutMs)?.count }.getOrNull()
        if (theirs == null) {
            countUnanswered.add(url)
            return true
        }
        return theirs >= negMinEvents
    }

    private suspend fun snapshotForStream(
        group: List<MirrorUpstream>,
        filter: Filter,
    ): StreamSnapshot {
        val window = cursors.coveringWindow(group.map { it.url }, filter)
        val startedMs = System.currentTimeMillis()
        val takenAt = startedMs / 1000
        val name = group.first().streamName
        // The denominator, asked for once. Seconds against a walk that takes
        // minutes, and it turns "4.2M ids so far" into "4.2M/14.9M (28%)".
        // Null rather than a guess if it fails: an unknown denominator is
        // better than a wrong one.
        val expected = runCatching { store.count(window) }.getOrNull()
        phases.set(name, StreamPhases.Phase.Snapshotting(0, expected, group.size))
        val local =
            snapshotReporting(window) { collected ->
                phases.set(name, StreamPhases.Phase.Snapshotting(collected, expected, group.size))
            }
        System.err.println(
            "router: static backfill $name local snapshot ${local.size} id(s) in ${fmtDuration(System.currentTimeMillis() - startedMs)}" +
                (window.since?.let { ", since $it" } ?: ", full filter (no relay is caught up yet)") +
                " — shared by ${group.size} relay(s)",
        )
        return StreamSnapshot(local, takenAt)
    }

    private suspend fun backfillOne(
        idx: Int,
        up: MirrorUpstream,
        snapshot: StreamSnapshot,
    ): Int {
        // The filter as the operator wrote it. `since`/`until` are NIP-01's own,
        // so absent means unbounded and this reaches the upstream's whole history
        // — minus whatever a previous run already walked, when this relay paged.
        val legs = cursors.legs(up.url, up.filter)
        if (legs.isEmpty()) {
            // Only reachable if a future change makes the legs exclusive again;
            // today the boundary seconds always leave something to ask for.
            progress.done(idx, 0)
            System.err.println("router: static backfill ${up.url.url} already covers its filter — nothing outside the synced band")
            return 0
        }
        transferring.incrementAndGet()
        try {
            var downloaded = 0
            var paged = false
            for (window in legs) {
                // Track the span this leg actually saw. The client reports how
                // many events came back, not when they were from, and the band
                // is the whole point of the exercise.
                var seenMin: Long? = null
                var seenMax: Long? = null
                // No deadline. Every timeout in the client is measured from the last
                // message, so a relay that stops answering is dropped in seconds by
                // [NEG_IDLE_MS] — and one still sending is doing the work we asked
                // for, however long its history takes. A wall clock could only fire
                // on the healthy case, which is how a 4h cap came to truncate four
                // working upstreams at exactly 14400s.

                // Coverage is stamped from when the SNAPSHOT was read, not from
                // now: that is the state the relay is being compared against, and
                // it is shared by the whole stream. Using the later leg start
                // would claim we had compared a window we never looked at — and
                // erring early only costs a small re-fetch, never a gap.
                val syncStartedAt = snapshot.takenAt
                val result =
                    client.negentropySyncOrFetch(
                        relay = up.url,
                        filter = window,
                        idleTimeoutMs = NEG_IDLE_MS,
                        localEntries = snapshot.ids,
                        onProgress = { needSoFar, done -> progress.update(idx, needSoFar, downloaded + done) },
                        onEvent = { event ->
                            if (up.filter.match(event)) {
                                // Only PLAUSIBLE stamps widen the band. Filtering
                                // here rather than on the aggregate matters: one
                                // future-dated event among 700k used to discard
                                // the whole upstream's band, so purplepag.es
                                // recorded nothing at all after a run that
                                // downloaded 700,767 events.
                                if (SyncCursors.isPlausible(event.createdAt)) {
                                    seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                    seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                }
                                offer(event, up.trusted)
                            }
                        },
                    )
                downloaded += result.downloaded
                paged = paged || result.pagedFallback
                // Per leg, not once at the end: a crash between legs then keeps
                // the ground the first one gained.
                cursors.record(
                    up.url,
                    up.filter,
                    seenMin,
                    seenMax,
                    result.pagedFallback,
                    // A reconcile that did not fall back covered the whole leg.
                    reconciledThrough = syncStartedAt.takeUnless { result.pagedFallback },
                )
            }
            run {
                progress.done(idx, downloaded)
                val band = cursors.band(up.url, up.filter)
                System.err.println(
                    "router: static backfill ${up.url.url} downloaded $downloaded" +
                        (if (paged) " (paged REQ fallback — no NIP-77)" else " (negentropy)") +
                        (if (legs.size > 1) " [resumed: ${legs.size} leg(s) outside the synced band]" else "") +
                        (if (band != null) " [synced ${band.minCreatedAt}..${band.maxCreatedAt}]" else ""),
                )
            }
            return downloaded
        } catch (e: Exception) {
            progress.done(idx, 0)
            System.err.println("router: static backfill ${up.url.url} failed: ${e.message}")
            return 0
        } finally {
            transferring.decrementAndGet()
        }
    }

    // ---- dynamic (relaySource) ---------------------------------------------

    /**
     * One dynamic stream, forever: read the relay lists our store already holds,
     * sync the stream's filter against every relay they name, sleep, repeat.
     *
     * The discovery is deliberately inside the loop — the store keeps filling
     * with relay lists while we run, so each cycle syncs against a wider (and
     * better-ranked) set than the last. A cycle that finds nothing is normal on
     * a cold store: it means no 10002/10040 has been mirrored in yet, and the
     * next refresh will find some.
     */
    private suspend fun dynamicLoop(stream: MirrorStream) {
        val dynamic = stream.dynamic ?: return
        val sourceNames =
            dynamic.sources.joinToString { s ->
                "kinds ${s.filter.kinds?.joinToString("/") ?: "?"} x${s.selects.size} select(s)"
            }
        // How long to wait before trying again when a cycle did NOT run. The full
        // refresh interval is the wrong answer there: a store that is still
        // filling has no relays to find yet, and a store that failed one query
        // (a degraded Vespa response aborts discovery by design, rather than
        // syncing against a half-read relay list) is usually fine moments later.
        // Waiting hours to notice either would be self-inflicted downtime, so
        // back off from short and climb, capped at the refresh interval.
        var retrySec = DYNAMIC_RETRY_BASE_SECONDS
        while (scope.isActive) {
            var ran = false
            try {
                // Never fan out onto ourselves: our own url is in plenty of lists.
                phases.set(stream.name, StreamPhases.Phase.Discovering(sourceNames))
                val relays = RelayDiscovery.discover(store, dynamic, skip = setOfNotNull(store.relay))
                if (relays.isEmpty()) {
                    phases.set(stream.name, StreamPhases.Phase.Waiting(sourceNames, retrySec))
                } else {
                    // Serialised with every other stream: this holds its id set
                    // for the whole fan-out, and two large sets resident at once
                    // is what pushed the heap to its ceiling.
                    phases.set(stream.name, StreamPhases.Phase.Queued(relays.size))
                    streamGate.withPermit { dynamicCycle(stream, dynamic, sourceNames, relays) }
                    ran = true
                }
            } catch (e: CancellationException) {
                // Shutdown, not a failure — a cycle can be mid-fan-out for a long
                // time, so close() almost always lands inside one. Let it end the
                // loop quietly instead of logging a scary line on every stop.
                throw e
            } catch (e: Exception) {
                phases.set(stream.name, StreamPhases.Phase.Failed(e.message?.take(80) ?: e.javaClass.simpleName, retrySec))
            }

            if (ran) {
                retrySec = DYNAMIC_RETRY_BASE_SECONDS
                delay(dynamic.refreshSeconds * 1000)
            } else {
                delay(retrySec * 1000)
                retrySec = (retrySec * 2).coerceAtMost(dynamic.refreshSeconds)
            }
        }
    }

    /** Sync every discovered relay, [DynamicRelayList.concurrency] of them at a time. */
    private suspend fun dynamicCycle(
        stream: MirrorStream,
        dynamic: DynamicRelayList,
        sourceNames: String,
        relays: List<DiscoveredRelay>,
    ) {
        val startedMs = System.currentTimeMillis()
        val gate = Semaphore(dynamic.concurrency)
        val downloaded = AtomicLong()
        val failed = AtomicLong()

        // What earlier runs — and any other NIP-66 monitor whose 30166s we mirror
        // — already proved unreachable. Loaded once for the fan-out, exactly as
        // the cursor bands are: this is a policy input, not a per-dial lookup.
        val knownDead = monitor?.deadSet().orEmpty()
        val health = RelayHealth(knownDead = knownDead)

        // ONE window and ONE local snapshot for the whole cycle, not one per
        // relay. Every relay in this stream reconciles the same filter, so a
        // per-relay snapshot re-scanned the identical range once per relay —
        // hundreds of full store scans per cycle, all returning the same thing.
        //
        // The cost of sharing it is that a relay synced late in the cycle
        // reconciles against the store as it was at the start, so an event two
        // relays both have can be downloaded twice. That was already true:
        // ingest is asynchronous (events queue through [inbound] and land in
        // batches), so a snapshot taken mid-cycle wouldn't have seen the earlier
        // relay's events either. The store dedups on insert; the scans were pure
        // waste.
        val window = stream.filter
        // ONE snapshot for the fan-out, but NOT over the whole filter. Taken over
        // the full stream filter this walked every kind-0 and kind-30382 we hold
        // — 24.8M ids on a real store, ~15 minutes of visit requests and gigabytes
        // of IdAndTime, all of it BEFORE the first log line, so a stream that was
        // busy was indistinguishable from one that had hung. Once every relay in
        // the list carries a complete band, this is the sliver since the oldest
        // of their ceilings instead. See [SyncCursors.coveringWindow].
        val snapshotWindow = cursors.coveringWindow(relays.map { it.url }, window)
        val snapStartedMs = System.currentTimeMillis()

        // A fetch-only stream never reconciles, so it never reads the id set —
        // and building one is the most expensive thing this router does. On the
        // `assertions` stream that is 24.8M ids and gigabytes of IdAndTime held
        // live for the whole fan-out, for relays that share essentially nothing
        // with us and would compare against it for nothing. Skipped outright.
        val local: List<IdAndTime> =
            if (stream.sync == SyncMode.FETCH) {
                System.err.println("router: ${stream.name} sync=fetch — no local id set needed, skipping the snapshot")
                emptyList()
            } else if (stream.deleteMissing != DeleteMissing.OFF) {
                // [reconcileAndDelete] reads its OWN ids, per ask, and must: the
                // shared snapshot spans every service on the stream, and handing
                // it to a one-service reconcile would report every other
                // service's cards as retracted. So this stream never touches it —
                // and building it anyway cost a full walk of the corpus per
                // cycle, 18.8M ids on this deployment, materialized and dropped.
                System.err.println("router: ${stream.name} deleteMissing — ids are read per ask, skipping the shared snapshot")
                emptyList()
            } else {
                val expected = runCatching { store.count(snapshotWindow) }.getOrNull()
                phases.set(stream.name, StreamPhases.Phase.Snapshotting(0, expected, relays.size))
                snapshotReporting(snapshotWindow) { collected ->
                    phases.set(stream.name, StreamPhases.Phase.Snapshotting(collected, expected, relays.size))
                }.also {
                    System.err.println(
                        "router: ${stream.name} local snapshot ${it.size} id(s) in ${fmtDuration(System.currentTimeMillis() - snapStartedMs)}" +
                            (snapshotWindow.since?.let { s -> ", since $s" } ?: ", full filter (no relay is caught up yet)"),
                    )
                }
            }
        // Why the unreachable ones were unreachable, tallied — a relay list is
        // full of dead hosts, and the shape of the failures is what tells an
        // operator whether that is normal or whether the whole cycle is broken.
        val reasons = java.util.concurrent.ConcurrentHashMap<String, Long>()
        System.err.println(
            "router: ${stream.name} syncing ${relays.size} relay(s) from [$sourceNames]" +
                " against ${local.size} local id(s)" +
                " (e.g. ${relays.take(3).joinToString { it.url.url }})",
        )
        val done = AtomicLong()
        val skipped = AtomicLong()
        coroutineScope {
            // A cycle over a relay list runs for HOURS with nothing but its
            // opening line, so a stalled fan-out and a working one look the same
            // from outside. The static backfill has a progress line for exactly
            // this reason; the bigger job had none.
            val ticker =
                launch {
                    while (true) {
                        delay(PROGRESS_INTERVAL_MS)
                        val finished = done.get()
                        val elapsed = System.currentTimeMillis() - startedMs
                        val rate = if (elapsed > 0) finished * 1000.0 / elapsed else 0.0
                        val etaSec = if (rate > 0) ((relays.size - finished) / rate).toLong() else -1
                        // A fetch-only stream has a real denominator — the time
                        // window each relay is walking — where a relay COUNT only
                        // ever says how many are finished, not how far the ones
                        // still running have got.
                        if (stream.sync == SyncMode.FETCH) {
                            phases.set(
                                stream.name,
                                StreamPhases.Phase.Fetching(
                                    done = finished.toInt(),
                                    total = relays.size,
                                    events = downloaded.get(),
                                    fraction = paging.fraction(stream.name),
                                    etaMs = paging.etaMs(stream.name),
                                    reachedSeconds = paging.reached(stream.name),
                                ),
                            )
                            continue
                        }
                        phases.set(
                            stream.name,
                            StreamPhases.Phase.Syncing(
                                done = finished.toInt(),
                                total = relays.size,
                                events = downloaded.get(),
                                skipped = skipped.get(),
                                unreachable = failed.get(),
                            ),
                        )
                    }
                }
            try {
                // The inner scope is what makes the ticker work. `forEach { launch }`
                // returns the moment the jobs are *issued*, so cancelling on the way
                // out of it killed the ticker microseconds after it started — before
                // its first delay ever elapsed, which is why a cycle over thousands
                // of relays printed its opening line and then nothing for hours.
                // Awaiting the jobs here keeps the ticker alive for the whole fan-out.
                coroutineScope {
                    relays.forEach { relay ->
                        launch {
                            // A TCP connect before the websocket handshake. The
                            // reachability records only help from the SECOND cycle
                            // on — the first one has nothing written down yet and
                            // would pay a full connect timeout for each of ~20k
                            // corpses. A refused connection or an unresolvable host
                            // comes back in milliseconds, and this is the cheapest
                            // possible way to learn it. Only a NEGATIVE result is
                            // acted on: a TCP handshake proves a socket, never a
                            // relay, so success still goes the long way round.
                            if (!tcpReachable(relay.url)) {
                                // Counted AND named. This path produced most of the
                                // "N unreachable" totals and never wrote a reason,
                                // so the cycle line printed the count with the
                                // explanation list empty — a number with no cause,
                                // which is what sent this investigation down two
                                // wrong paths.
                                reasons.merge("tcp: no route or refused", 1L, Long::plus)
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                publishStrike(health, relay.url)
                                return@launch
                            }
                            // Checked INSIDE the coroutine but OUTSIDE the permit,
                            // and re-checked rather than filtered up front: an
                            // authority struck out while this one waited for a slot
                            // should not still be dialled. Skipping costs nothing
                            // and frees the permit for a relay that might answer.
                            if (health.isDead(relay.url)) {
                                reasons.merge("skipped: authority already struck out", 1L, Long::plus)
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                return@launch
                            }
                            gate.withPermit {
                                val got =
                                    // The relay's OWN filter, narrowed by what the
                                    // tags that named it paired it with. Identical to
                                    // `window` for a select that binds only the url,
                                    // which is every config written before bindings
                                    // existed — so nothing changes for those.
                                    dynamicSyncOne(stream, relay.url, relay.narrowed(window), local) { reason ->
                                        reasons.merge(reason, 1L, Long::plus)
                                    }
                                when {
                                    // Could not reach it. Strike the authority and
                                    // publish, which is the finding NIP-66 exists for.
                                    got == UNREACHABLE -> {
                                        failed.incrementAndGet()
                                        publishStrike(health, relay.url)
                                    }

                                    // Reached it; the transfer broke. Counted as a
                                    // failure for this cycle, but NOT struck and NOT
                                    // published: the relay answered our handshake, so
                                    // telling the network it is unreachable would be
                                    // a false statement about someone else's server.
                                    got == TRANSFER_FAILED -> {
                                        failed.incrementAndGet()
                                    }

                                    // Delivered. Its whole authority is alive, which
                                    // beats any strike a sibling url earned racing it.
                                    got > 0 -> {
                                        downloaded.addAndGet(got.toLong())
                                        health.produced(relay.url)
                                    }

                                    // Answered cleanly with nothing new — a working
                                    // relay we are simply already in sync with. Not a
                                    // strike, and with a recorded band the next cycle
                                    // asks it for far less.
                                    else -> {
                                        health.produced(relay.url)
                                    }
                                }
                                done.incrementAndGet()
                            }
                        }
                    }
                }
            } finally {
                ticker.cancel()
            }
        }
        val topReasons =
            reasons.entries
                .sortedByDescending { it.value }
                .take(3)
                .joinToString { "${it.key} x${it.value}" }
        // One write for the whole fan-out, not one per relay.
        cursors.flush()
        val elapsedMs = System.currentTimeMillis() - startedMs
        // Liveness is flushed by the monitor for the whole router, not per cycle:
        // it observes the static upstreams too, and they never pass through here.
        System.err.println(
            "router: ${stream.name} cycle done — ${downloaded.get()} event(s) from ${relays.size - failed.get() - skipped.get()}/${relays.size} relay(s)" +
                " in ${fmtDuration(elapsedMs)}" +
                // The one number that makes two cycles comparable. Total and
                // duration were both here and the division was left to the reader.
                (if (elapsedMs >= 1_000 && downloaded.get() > 0) " (${downloaded.get() * 1000 / elapsedMs}/s)" else "") +
                "; ${health.summary(relays.size)}" +
                (if (topReasons.isNotEmpty()) "; unreachable: $topReasons" else "") +
                "; next in ${dynamic.refreshSeconds}s",
        )
        phases.set(stream.name, StreamPhases.Phase.Idle(downloaded.get(), dynamic.refreshSeconds))
    }

    /**
     * Reconcile one discovered relay against our store and pull what it has that
     * we don't — negentropy when the relay speaks NIP-77, paged REQ when it
     * doesn't. Returns the download count, or -1 when the relay never delivered.
     *
     * [window] and [local] are the cycle's, shared by every relay in it — see
     * [dynamicCycle] for why they are not recomputed here.
     */
    private suspend fun dynamicSyncOne(
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        onFailure: (String) -> Unit,
    ): Int {
        inFlight.merge(url, 1, Int::plus)
        transferring.incrementAndGet()
        return try {
            // No wall clock here either: these relays are strangers off a list,
            // but a slot held by one that is delivering is a slot doing its job.
            // Silence is what costs us, and [NEG_IDLE_MS] already answers that.
            var downloaded = 0
            for (ask in splitByAuthors(window, stream.dynamic?.authorsPerLeg)) {
                downloaded += syncOneFilter(stream, url, ask, local)
            }
            downloaded
        } catch (e: Exception) {
            // A dead host in a relay list is the common case, not an incident:
            // tally it and move on — one line per cycle carries the totals.
            onFailure("${e.javaClass.simpleName}: ${e.message?.take(50) ?: ""}".trim(':', ' '))
            // -1 says "this relay is unreachable" and costs it a signed NIP-66
            // record. Only say that when it is true. See [provesUnreachable].
            if (Unreachability.proves(e)) UNREACHABLE else TRANSFER_FAILED
        } finally {
            transferring.decrementAndGet()
            releaseSocket(url)
        }
    }

    /**
     * One relay, one filter: walk what the cursor says is outside its band.
     *
     * Split out of [dynamicSyncOne] because a narrowed stream asks the same
     * relay several times — once per author chunk — and each of those is its own
     * band. The socket is acquired once around all of them.
     */
    private suspend fun syncOneFilter(
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
    ): Int {
        if (stream.deleteMissing != DeleteMissing.OFF) return reconcileAndDelete(stream, url, window)
        var downloaded = 0
        for (leg in cursors.legs(url, window)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val syncStartedAt = System.currentTimeMillis() / 1000
            val onEvent: (Event) -> Unit = { event ->
                if (stream.filter.match(event)) {
                    if (SyncCursors.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    offer(event, stream.trusted)
                }
            }
            // Fetch-only: the leg came off the cursor band, so this asks for
            // what is outside what we already walked and nothing else. That
            // band IS the mechanism here — there is no id set to fall back on.
            val fetched = stream.sync == SyncMode.FETCH
            val result =
                if (fetched) {
                    null.also {
                        val walk = "${stream.name}|${url.url}"
                        paging.begin(walk, leg.until ?: nowSeconds(), leg.since ?: SyncCursors.PLAUSIBLE_FLOOR)
                        downloaded +=
                            client.fetchAllPages(
                                url,
                                listOf(leg),
                                NEG_IDLE_MS,
                                onNewPage = { until -> paging.mark(walk, until) },
                                onEvent = onEvent,
                            )
                        paging.finish(walk)
                    }
                } else {
                    client
                        .negentropySyncOrFetch(
                            relay = url,
                            filter = leg,
                            idleTimeoutMs = NEG_IDLE_MS,
                            localEntries = local,
                            onEvent = onEvent,
                        ).also { downloaded += it.downloaded }
                }
            cursors.record(
                url,
                window,
                seenMin,
                seenMax,
                paged = fetched || result?.pagedFallback == true,
                reconciledThrough = syncStartedAt.takeIf { result != null && !result.pagedFallback },
            )
        }
        return downloaded
    }

    /**
     * Reconcile one narrow ask BOTH ways: download what the upstream has and we
     * lack, delete what we have and it no longer serves.
     *
     * The upstream is the source of truth for these records — a provider's own
     * relay for its own scores — so its set is the answer, not a contribution to
     * one. Nothing is published: the diff is read and acted on locally, never
     * pushed. (quartz's `NegentropyStoreSync` can propagate real NIP-09
     * retractions instead, which is strictly safer, but arming it means
     * uploading our events to somebody else's relay and reading the rejections.)
     *
     * Absence has innocent causes, so this refuses far more often than it acts.
     */
    private suspend fun reconcileAndDelete(
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        ask: Filter,
    ): Int {
        // Read for THIS ask, never the cycle's shared snapshot. quartz says it
        // plainly: "entries outside the filter would show up as false have ids".
        // The shared snapshot spans every service on the stream, so handing it to
        // a one-service reconcile would report every OTHER service's cards as
        // missing upstream — and delete almost the whole corpus. This is the one
        // place in the router where getting a filter wrong destroys data rather
        // than wasting time, so the ids are derived from the ask itself and there
        // is no parameter to pass the wrong thing in.
        val mine = store.snapshotIdsForNegentropy(listOf(ask))
        // NOT an early return when we hold nothing. That was a bug: an ask we
        // have no records for is exactly a service we have never fetched — a new
        // 10040, or one the orphan sweep emptied — and returning here meant it
        // was never fetched again either. Reconciling against an empty local set
        // is well defined and is precisely "give me everything you have"; the
        // delete side then no-ops on its own, because haveIds is a subset of a
        // set that is empty.

        // A COMPLETED reconcile is the whole licence to delete. quartz never
        // silently falls back: it throws when a window cannot be reconciled over
        // NIP-77 at all — including "this relay does not speak it" — so a normal
        // return means every `created_at` window was compared end to end, and an
        // empty answer is the relay's real answer rather than its silence.
        //
        // Failing that, page the ask anyway so the mirror still fills, and delete
        // nothing: the set was never compared, so nothing about it is known.
        val diff =
            try {
                client.negentropyReconcileIds(url, ask, mine, idleTimeoutMs = NEG_IDLE_MS)
            } catch (e: NegentropySyncException) {
                System.err.println(
                    "router: ${stream.name} ${url.url} could not reconcile (${e.reason}) — paging instead, deleting nothing",
                )
                return pageAsk(stream, url, ask)
            }

        var downloaded = 0
        for (chunk in diff.needIds.chunked(ID_FETCH_CHUNK)) {
            downloaded +=
                client.fetchAllPages(url, listOf(Filter(ids = chunk)), NEG_IDLE_MS) { event ->
                    if (stream.filter.match(event)) offer(event, stream.trusted)
                }
        }
        if (diff.haveIds.isEmpty()) return downloaded

        // A reconcile that split into no windows compared no range. It cannot
        // have returned a meaningful diff, whatever it says.
        if (diff.windows < 1) {
            System.err.println(
                "router: ${stream.name} ${url.url} reconciled 0 window(s) — nothing was compared, deleting nothing",
            )
            return downloaded
        }

        // NO SIZE GUARD, deliberately, and this is the sharp end of the feature.
        //
        // An earlier version refused when the relay served nothing, and again
        // when a cycle would drop over half of an ask. Both fired constantly and
        // both were WRONG about the risk they were managing: they protect stored
        // records from a bad answer, when the thing actually worth protecting is
        // a reader from a stale score. A provider that retracts a subject usually
        // does it because the subject turned out to be a scammer — exactly the
        // score that must not survive — and a mass retraction is precisely when
        // the whole set goes. Guarding on volume blocks the case that matters
        // most while the harmless cases sail through.
        //
        // If a 10040 names a relay that never carried these scores, the relay
        // reconciles empty and we drop them. That is a misconfigured provider
        // list costing us a re-download, against the alternative of serving a
        // retracted score forever. The completed reconcile above is what makes
        // "empty" trustworthy enough to act on.
        val share = diff.haveIds.size.toDouble() / mine.size
        if (stream.deleteMissing == DeleteMissing.DRY_RUN) {
            System.err.println(
                "router: ${stream.name} would delete ${diff.haveIds.size}/${mine.size} record(s) (${(share * 100).toInt()}%)" +
                    " for ${url.url} after a clean ${diff.windows}-window reconcile — set deleteMissing = true to apply",
            )
            return downloaded
        }
        // Deleted BY ID and inside the ask: the filter that found them is the
        // filter that removes them, so a delete can never reach past the records
        // this reconcile actually compared.
        for (chunk in diff.haveIds.chunked(ID_FETCH_CHUNK)) {
            store.delete(ask.copy(ids = chunk, since = null, until = null, limit = null))
        }
        deleted.addAndGet(diff.haveIds.size.toLong())
        System.err.println(
            "router: ${stream.name} deleted ${diff.haveIds.size}/${mine.size} record(s) (${(share * 100).toInt()}%)" +
                " ${url.url} no longer serves, after a clean ${diff.windows}-window reconcile",
        )
        return downloaded
    }

    /**
     * Page one ask, for a relay that could not reconcile it.
     *
     * The mirror still wants these events; only the DELETE side needs a
     * reconcile, because only a reconcile compares whole sets.
     */
    private suspend fun pageAsk(
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        ask: Filter,
    ): Int {
        // Walk only what the band leaves, and record what this saw — the same
        // bookkeeping every other paged path does. Without it a relay that
        // cannot reconcile re-walked its whole history on every cycle, forever,
        // which is the one cost cursor bands exist to remove.
        var downloaded = 0
        for (leg in cursors.legs(url, ask)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            downloaded +=
                client.fetchAllPages(url, listOf(leg), NEG_IDLE_MS) { event ->
                    if (stream.filter.match(event)) {
                        if (SyncCursors.isPlausible(event.createdAt)) {
                            seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                            seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                        }
                        offer(event, stream.trusted)
                    }
                }
            cursors.record(url, ask, seenMin, seenMax, paged = true)
        }
        return downloaded
    }

    /**
     * [window] as one ask, or as several with at most [per] authors each.
     *
     * A cursor band is keyed on its filter, so the size of these chunks decides
     * how often a band survives. See [DynamicRelayList.authorsPerLeg] — this
     * only reshapes what that knob asked for. A filter with no bound authors is
     * returned untouched, which is every stream that does not narrow.
     */
    private fun splitByAuthors(
        window: Filter,
        per: Int?,
    ): List<Filter> {
        val authors = window.authors
        if (per == null || authors == null || authors.size <= per) return listOf(window)
        return authors.chunked(per).map { window.copy(authors = it) }
    }

    /**
     * Strike a relay, and publish the verdict if it takes its whole host down.
     *
     * The relay itself is recorded by whatever observed it. What is NOT recorded
     * anywhere else is the eviction: once an authority is struck out, every
     * sibling url under it is skipped without being dialled, so nothing will
     * ever observe them again. Publishing at the moment of eviction is the only
     * point where evidence exists — three observed failures on that host — and
     * it is what turns thousands of silent skips into a finding others can read.
     */
    private fun publishStrike(
        health: RelayHealth,
        url: NormalizedRelayUrl,
    ) {
        val evicted = health.strike(url) ?: return
        monitor?.observer?.record(
            url,
            reachable = false,
            error = "host ${evicted.authority} silent after ${evicted.strikes} attempts",
        )
    }

    /**
     * Can we open a TCP connection to this relay at all?
     *
     * Deliberately fail-OPEN: any error deciding this returns true, so a probe
     * that is itself broken can never silently amputate the fan-out. The cost of
     * a false positive is the connect timeout we were going to pay anyway.
     */
    private suspend fun tcpReachable(url: NormalizedRelayUrl): Boolean {
        val ok = runCatching { TcpProber.tcpReachable(url) }.getOrDefault(true)
        // PUBLISHED, not just acted on. This probe is the only thing that will
        // ever look at most of these relays — a fan-out skips the rest before
        // the websocket client sees them — so a verdict kept private means the
        // monitor has nothing to say about the relays it just judged. Measured
        // before this: 104 records for a 16,507-relay list.
        //
        // Only a NEGATIVE result is published. A completed TCP handshake proves
        // a socket, not a relay, and reporting that as reachable would assert
        // something this probe never tested; the connection that follows says it
        // properly.
        // No rtt either way: a TCP open is not a NIP-01 open, and publishing one
        // as the other misreports the field aggregators rank by.
        if (!ok) monitor?.observer?.record(url, reachable = false, error = "tcp: unreachable")
        return ok
    }

    /**
     * Drop a dynamic relay's socket once nothing is using it. Hundreds of relays
     * a cycle would otherwise leave hundreds of idle connections open until the
     * next one; relays carrying a live subscription ([pinnedUrls]) and relays
     * another stream is still syncing are left alone.
     */
    private fun releaseSocket(url: NormalizedRelayUrl) {
        val stillInUse = inFlight.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } } != null
        if (!stillInUse && url !in pinnedUrls) {
            runCatching { client.getOrCreateRelay(url).disconnect() }
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
                do {
                    pushedThisPass = 0
                    run {
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
                    }
                    pushedThisWindow += pushedThisPass
                    rounds++
                } while (pushedThisPass > 0 && rounds < UP_MAX_ROUNDS && scope.isActive)
                System.err.println(
                    "router: up ${up.url.url} pushed $pushedThisWindow event(s) upstream ($rounds round(s))",
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
                cursors.flush()
                // "backfill complete" used to read as "the relay is caught up",
                // while the dynamic streams — the larger half of the fill, by an
                // order of magnitude on a real relay list — were still going.
                val stillSyncing = dynamicStreams.size
                System.err.println(
                    "router: static backfill complete — ${s.downloaded} events from ${s.total} relay(s)" +
                        " in ${fmtDuration(s.elapsedMs)}; live tail now streaming" +
                        if (stillSyncing > 0) "; $stillSyncing dynamic stream(s) still syncing" else "",
                )
                return
            }
            // ETA from the average download rate since start — steadier than an
            // instantaneous window, which flickers to zero between negentropy pages.
            phases.report().forEach { System.err.println(it) }
            val elapsedSec = s.elapsedMs / 1000.0
            val avgRate = if (elapsedSec > 0) s.downloaded / elapsedSec else 0.0
            val remaining = (s.need - s.downloaded).coerceAtLeast(0)
            val etaSec = if (avgRate > 1) (remaining / avgRate).toLong() else -1
            System.err.println(
                // Named, because "backfill 5/12" says nothing about WHICH 12 —
                // and the dynamic streams, which do the larger share of the work,
                // are not in this count at all. "done" because all 12 run at
                // once: this counts the finished ones, not a position in a queue.
                "router: static backfill ${s.done}/${s.total} relay(s) done, ${s.downloaded}/${s.need} events (${s.percent()}%)" +
                    ", ${"%.0f".format(avgRate)}/s avg" +
                    (if (etaSec >= 0) ", ETA ~${fmtDuration(etaSec * 1000)} to useful" else ", ETA —"),
            )
        }
    }

    /**
     * Why the machine is idle, once a minute.
     *
     * Every stall this router has had was diagnosed from `docker stats`, Vespa's
     * access log and /proc — never from the relay, which reported healthy phases
     * throughout. This is the line that answers it directly: a full heap, a full
     * or empty queue, and a rate of zero each mean something different, and
     * together they name the bottleneck without guessing.
     *
     * The heap matters most. Four OutOfMemoryErrors passed unnoticed while the
     * counters simply stopped, because an OOM kills whichever thread allocates
     * next and nothing here was watching for it.
     */
    private suspend fun healthLoop() {
        var lastEvents = 0L
        var lastAt = System.currentTimeMillis()
        while (scope.isActive) {
            delay(60_000)
            val rt = Runtime.getRuntime()
            val usedMb = (rt.totalMemory() - rt.freeMemory()) / 1_048_576
            val maxMb = rt.maxMemory() / 1_048_576
            val heapPct = if (maxMb > 0) usedMb * 100 / maxMb else 0
            val events = accepted.get() + rejected.get()
            val now = System.currentTimeMillis()
            val rate = ((events - lastEvents) * 1000.0 / (now - lastAt).coerceAtLeast(1)).toInt()
            lastEvents = events
            lastAt = now
            val depth = queued.get()
            System.err.println(
                "router: health heap $usedMb/${maxMb}MB ($heapPct%)" +
                    (if (heapPct >= 90) " !! AT THE CEILING" else "") +
                    ", ingest queue $depth/$inboundCapacity" +
                    // Full and empty are opposite diagnoses and look identical in
                    // every other line the router prints.
                    //
                    // The depth is an instant, the rate a 60s average, so an empty
                    // queue means two different things depending on the rate and
                    // only the pair can tell them apart. Reading the depth alone
                    // had this line print "nothing is arriving to ingest" during a
                    // minute that ingested 15,443 events a second.
                    (
                        when {
                            depth >= inboundCapacity -> " FULL (ingest is the limit — downloads are backpressured)"
                            depth == 0 && rate == 0 -> " empty (nothing is arriving — the limit is upstream of ingest)"
                            depth == 0 -> " drained (ingest is keeping up; downloads are the limit)"
                            else -> ""
                        }
                    ) +
                    ", $rate ev/s" +
                    ", ${transferring.get()} relay(s) transferring" +
                    ", ${client.connectedRelaysFlow().value.size} connected" +
                    (if (fatals.get() > 0) ", ${fatals.get()} FATAL error(s) — threads were killed" else "") +
                    (deleted.get().takeIf { it > 0 }?.let { ", $it record(s) DELETED as retracted upstream" } ?: "") +
                    (servingPressure?.describe()?.let { ", $it" } ?: "") +
                    (
                        if (lostToStore.get() > 0) {
                            ", ${lostToStore.get()} event(s) LOST to store errors (good events, gone — check the schema)"
                        } else {
                            ""
                        }
                    ),
            )
            // Named, because "16,248 skipped" says nothing about which corner of
            // the network we stopped looking at, or whether the reason still
            // holds. These are relays a PREVIOUS run recorded unreachable; the
            // record expires on its own, and until it does they are invisible.
            monitor?.deadSet()?.takeIf { it.isNotEmpty() }?.let { dead ->
                System.err.println(
                    "router: health ${dead.size} relay(s) skipped on earlier NIP-66 records" +
                        " (top: ${dead.take(3).joinToString { it.url }})",
                )
            }
        }
    }

    private suspend fun statsLoop() {
        while (scope.isActive) {
            delay(60_000)
            System.err.println(
                "router: ingested ${accepted.get()} accepted, ${rejected.get()} rejected${rejectionBreakdown()}" +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${pushed.get()} up" else "") +
                    // A dynamic cycle connects relays that are in no upstream list,
                    // so the connected count is reported against the pinned ones
                    // rather than as a fraction of them.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (dynamicStreams.isNotEmpty()) " + dynamic" else ""),
            )
            // Where the minute actually went. The store already times every
            // ingest stage — dedup, guards, preload, versions, write and the two
            // projection halves — and nothing printed it, so "ingest is slow" had
            // to be inferred from access-log shapes instead of read off. A batch
            // of 1000 was taking ~2.2 minutes; this says which stage owns it.
            IngestStats.gauge().takeIf { it.isNotEmpty() }?.let { System.err.println("router: ingest $it") }
        }
    }

    /** Accepted/rejected/pushed counters, for tests and a final log line. */
    fun stats(): Triple<Long, Long, Long> = Triple(accepted.get(), rejected.get(), pushed.get())

    /**
     * What the rejections actually were. A wide fan-out asks a thousand relays
     * for the same replaceable events, so "already have it" dwarfing the accept
     * count is the system working — while a bad signature or a failing store is
     * not, and the bare total hides which one you are looking at.
     */
    private fun rejectionBreakdown(): String {
        if (rejected.get() == 0L) return ""
        val why =
            rejectReasons.entries
                .sortedByDescending { it.value }
                .take(2)
                .joinToString { "${it.key} x${it.value}" }
        val bad = if (unverified.get() > 0) "bad signature x${unverified.get()}" else ""
        val parts = listOf(bad, why).filter { it.isNotEmpty() }
        return if (parts.isEmpty()) "" else " [${parts.joinToString("; ")}]"
    }

    /** Number of distinct configured upstreams (down + up) being mirrored. */
    fun upstreamCount(): Int = pinnedUrls.size

    /** Number of streams whose relays are discovered from the store, not configured. */
    fun dynamicStreamCount(): Int = dynamicStreams.size

    override fun close() {
        // First: a backfill killed mid-flight still keeps the ground it gained.
        runCatching { cursors.flush() }
        // A last flush: a run that ends between intervals still knows things about
        // relays that the next run would otherwise pay to rediscover. The monitor
        // deliberately leaves this to us, because only the caller knows how long
        // a shutdown may block — and the engine being unreachable is a normal way
        // for a relay to be going down, with no read deadline on that client, so
        // unbounded here would hang exactly when it is most likely to.
        runCatching {
            kotlinx.coroutines.runBlocking {
                kotlinx.coroutines.withTimeoutOrNull(SHUTDOWN_FLUSH_MS) { monitor?.flush() }
            }
        }
        runCatching { monitor?.close() }
        runCatching { authenticator?.destroy() }
        downUpstreams.indices.forEach { runCatching { client.unsubscribe("vespa-mirror-down-$it") } }
        runCatching { client.close() }
        inbound.close()
        scope.cancel()
        // After the scope, so a worker mid-batch is cancelled rather than
        // stranded on a pool that has stopped accepting work.
        runCatching { ingestPool.close() }
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        System.err.println(
            "router: stopped (${accepted.get()} accepted, ${rejected.get()} rejected${rejectionBreakdown()}, ${pushed.get()} pushed)",
        )
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

        // First wait after a dynamic cycle could not run; doubles up to the
        // stream's own refresh interval.
        private const val DYNAMIC_RETRY_BASE_SECONDS = 30L

        // Idle (no protocol frames for this long) aborts a negentropy session.
        // This is the ONLY bound on a session: there is no wall-clock deadline,
        // because a relay that is still answering is one we still want.
        // Each running slot is a thread parked on a socket read, so this is a
        // real resource — but a modest one next to what it buys, and the fan-out
        // is bounded by the streams' own `concurrency` rather than by this.
        private const val MAX_CONCURRENT_SOCKETS = 1024
        private const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20

        /** How long a shutdown will wait on that last write before giving up. */
        private const val SHUTDOWN_FLUSH_MS = 5_000L

        /**
         * Idle time a transfer may sit silent before it is abandoned.
         *
         * IDLE, not a deadline — the clock resets on every message, so a relay
         * that is still delivering is never cut off however long its history
         * takes. That is a property of quartz's accessory APIs, and briefly it
         * was not: `fetchAllPages` treated this as a hard budget for the whole
         * page, and a page only finishes once EOSE has been PROCESSED — behind
         * every event ahead of it in the socket buffer, each going through
         * [offer], which blocks while the ingest queue is full. Measured against
         * nip85.nosfabrica.com, which answers a page with 100,000 events and an
         * EOSE in 4.3s:
         *
         * ```
         * a full page reaches back   23.8h
         * the router advanced only    4.4h   (~18% of the page)
         * ```
         *
         * The other 82% was cut off, re-requested and cut off again, so a walk's
         * depth depended on how congested ingest happened to be — 3,284 events
         * on one run and 38,530 on the next, same relay, same filter. This file
         * carried a 5-minute PAGE_BUDGET_MS to work around it; quartz 1622bd7109
         * made every accessory timeout an idle window, which fixes it properly
         * and for every caller, so the workaround is gone. Under idle semantics
         * a large value would only mean "hold a silent socket for longer".
         */
        private const val NEG_IDLE_MS = 30_000L

        /** Ids per by-id REQ, and per delete. The store's own bulk chunk. */
        private const val ID_FETCH_CHUNK = 500

        // Distinct store failures to dump a raw event for. A handful names every
        // defect a real corpus carries; past that it is a stuck loop, not news.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it. Kind 0 content runs long, and a
        // truncated tail still leaves the id, pubkey, kind and tags readable.
        private const val POISON_JSON_CHARS = 4_000
    }
}

/**
 * Write [events] through [write]; if that throws, split the batch and write the
 * halves, down to the single event the writer cannot take.
 *
 * A bulk write fails as a unit, so one event the store chokes on costs the whole
 * batch — at a 1000-event batch that is 999 good events lost per bad one, with no
 * retry and no way to tell which one did it. The failure count is then a multiple
 * of the batch size rather than a number of bad events, which reads as far worse
 * damage than it is.
 *
 * Bisecting costs ~2·log2(n) extra writes on a failing batch and nothing at all on
 * a healthy one, and it ends holding the offender by itself. Re-writing the good
 * halves is safe: a batch that threw may already have applied some of its events,
 * and re-inserting those is a duplicate the store rejects.
 *
 * Free-standing and injectable so the isolation can be tested without a store.
 */
internal suspend fun insertBisecting(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit = { _, _ -> },
) = bisect(events, write, onOutcomes, onPoison, onGaveUp, intArrayOf(ISOLATION_WRITE_BUDGET))

private suspend fun bisect(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit,
    budget: IntArray,
) {
    if (events.isEmpty()) return
    try {
        onOutcomes(write(events))
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        if (events.size == 1) {
            onPoison(events.single(), e)
            return
        }
        // Splitting assumes ONE event is at fault and the rest are fine. When the
        // store itself is refusing — a full disk, a dead engine — that assumption
        // inverts: every half fails, all the way down, and isolation turns one
        // failed write into ~2n. Precisely the wrong moment to multiply the load.
        //
        // Rather than reading the exception to guess which case this is (engine
        // error strings are not an API), spend a fixed budget of writes trying to
        // isolate, and give up on the remainder when it runs out. A batch with a
        // handful of bad events finishes well inside it; a store-wide failure
        // stops after a bounded probe instead of hammering the store 2n times.
        if (budget[0] <= 0) {
            onGaveUp(events, e)
            return
        }
        budget[0] -= 2
        val mid = events.size / 2
        bisect(events.subList(0, mid), write, onOutcomes, onPoison, onGaveUp, budget)
        bisect(events.subList(mid, events.size), write, onOutcomes, onPoison, onGaveUp, budget)
    }
}

/**
 * Writes one batch may spend isolating its bad events before giving up on the
 * rest.
 *
 * Isolating k bad events out of n costs about `2·k·log2(n)` writes, so 64 covers
 * three of them in a 1000-event batch — well past the one-per-batch rate seen in
 * practice. What it really bounds is the store-wide case, where every write fails
 * and the alternative is ~2000.
 */
private const val ISOLATION_WRITE_BUDGET = 64

/**
 * Ceiling on queued-but-not-yet-ingested events, independent of batch size.
 * 16k events is a few hundred MB at Nostr's event sizes — enough to keep ingest
 * fed across a stall, far short of what killed the process at 80,000.
 */
private const val MAX_INBOUND_QUEUE = 16_384

/**
 * [MirrorRouter.dynamicSyncOne]'s two failure returns, distinct because only one
 * of them is publishable — see [Unreachability]. Both are negative so `got > 0`
 * (delivered) and `got == 0` (nothing new) keep meaning what they did.
 */
private const val UNREACHABLE = -1

private const val TRANSFER_FAILED = -2

/**
 * [List.partition] where the predicate suspends, evaluated concurrently.
 *
 * Concurrency is the point, not a bonus: the predicate this exists for is a
 * NIP-45 COUNT round trip with its own timeout, and a relay that never answers
 * costs the whole timeout. Serially, twelve silent relays would be a minute of
 * dead wait before a byte was fetched, and twenty thousand of them would end the
 * cycle.
 */
private suspend fun <T> List<T>.partitionSuspend(predicate: suspend (T) -> Boolean): Pair<List<T>, List<T>> =
    coroutineScope {
        val marked = map { item -> async { item to predicate(item) } }.awaitAll()
        marked.filter { it.second }.map { it.first } to marked.filterNot { it.second }.map { it.first }
    }

/** Wall-clock seconds, the unit every `created_at` in the protocol is in. */
private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

/**
 * Whether a failure may be published as "this relay is unreachable".
 *
 * The distinction matters because the answer is PUBLISHED. A negative NIP-66
 * record is a signed, public statement about someone else's server, and this
 * router was making it for every failure of any kind.
 *
 * Two things were being libelled. A relay that completes a websocket handshake
 * — `nip85.nosfabrica.com` answered in 50ms — and then sends EOFException
 * part-way through a large page is emphatically reachable; it hung up on a
 * query it did not want to finish, which is a different fact and arguably ours
 * to fix. And an exception thrown by OUR code inside the fan-out (a
 * ConcurrentModificationException cost a relay a record in a real cycle) says
 * nothing whatever about the relay.
 *
 * So this asks only about the connection itself: name resolution, routing,
 * refusal, TLS. Anything after a socket is open is a transfer failure, and
 * anything that looks like our own bug is never the relay's fault.
 *
 * Unknown failures stay quiet — the conservative direction, because the cost of
 * silence is one retry next cycle and the cost of being wrong is a false record
 * carrying our signature.
 *
 * Top-level and pure so the rule can be tested without a live client. The rule
 * is the part worth testing.
 */
object Unreachability {
    fun proves(e: Exception): Boolean =
        when (e) {
            is java.net.UnknownHostException,
            is java.net.ConnectException,
            is java.net.NoRouteToHostException,
            is java.net.PortUnreachableException,
            is javax.net.ssl.SSLHandshakeException,
            -> true

            else -> false
        }
}
