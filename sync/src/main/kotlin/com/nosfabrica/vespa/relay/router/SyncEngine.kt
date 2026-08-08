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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.relay.maintenance.ParseAudit
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.SyncUpstream
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.RelayLogger
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The router: a strfry-style mirror. For each configured upstream it moves
 * events between that relay and the served relay's store.
 *
 * Down (`dir = down`/`both`): a live REQ subscription streams new events into
 * the store through [IngestPipeline]; [StaticBackfill] catches up on history
 * first. Up (`dir = up`/`both`): [UpstreamPush] periodically reconciles the
 * store against the upstream and publishes what it is missing. Dynamic
 * (`relaySource = [...]`): [DynamicSync] discovers relays from the store's
 * own relay-list events and syncs them on a period.
 *
 * This class owns the shared plumbing — the websocket client, the NIP-66
 * monitor, NIP-42 auth, the health and stats lines — and hands the work to
 * those collaborators. [close] stops touching the store before the store
 * closes.
 */
class SyncEngine(
    private val store: IEventStore,
    private val config: RouterConfig,
    parentContext: CoroutineContext = SupervisorJob(),
    // PARSE_AUDIT_FILE: run every mirrored event through quartz's
    // search-indexing parse. Off by default — it costs one parse per event.
    audit: ParseAudit? = null,
    // Resume state for paged relays, so a restart is not a re-download.
    private val bands: SyncBands = SyncBands(null),
    // Per-peer negentropy window sizes and the in-progress sweep cursor. In
    // memory by default: correct, but a restart re-learns both.
    private val sweepState: SweepState = SweepState(null),
    // Answers NIP-42 challenges from upstreams that gate reads behind AUTH.
    signer: NostrSigner? = null,
    // SYNC_WIRE_LOG: "" (errors only) / "sent" / "full".
    wireLogMode: String = "",
    // Fed by PressurePoller from the relay's GET /pressure: ingest yields
    // when client reads slow down. Null — no feed configured — is the
    // mirror-at-full-speed mode, and SyncMain says so at boot.
    servingPressure: ServingPressure? = null,
    // SYNC_TOR_SOCKS: the proxy .onion upstreams are dialled through. Null is
    // the clearnet-only deployment, where discovery drops .onion urls and a
    // configured one is a boot error — never a silent timeout.
    torSettings: TorSettings? = null,
    // Which of these ids the store already holds, so ingest can drop a
    // duplicate BEFORE paying to verify it (see IngestPipeline.dropDuplicates).
    // Not on IEventStore — SyncMain hands over the engine index's own
    // existence check. Null just means every copy is verified, as before.
    knownIds: (suspend (List<String>) -> Set<String>)? = null,
) : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.IO + parentContext)

    // One OkHttp client for every upstream. The 120s ping surfaces half-open
    // connections as a failed pong, which routes into quartz's reconnect path.
    private val okhttp =
        OkHttpClient
            .Builder()
            // The dispatcher budget is the real concurrency ceiling for the
            // whole router: an open websocket holds a dispatcher slot for its
            // entire life, so at the stock 64 every stream's `concurrency`
            // silently stopped meaning anything (measured: a 20,340-relay
            // cycle with an ETA of 330 hours). Must exceed static upstreams
            // plus the sum of every stream's `concurrency`.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    // Per HOST; only bites when one host serves several urls.
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSec))
            .build()

    // The Tor client, when there is one, and which urls it takes. See
    // [TorTransport] for why resolution has to happen inside the proxy.
    private val tor = torSettings?.let { TorTransport(it, okhttp) }

    // Per URL, not one client for the process: quartz's builder takes
    // (NormalizedRelayUrl) -> OkHttpClient precisely so a relay can be dialled
    // over the transport that can reach it.
    private val client = NostrClient(BasicOkHttpWebSocket.Builder { url -> tor?.clientFor(url) ?: okhttp }, scope)

    // NIP-66: watches every connection this client makes, measures round
    // trips, signs them as kind 30166 into this same store, and hands back a
    // cheap dead-relay set for the fan-out to skip. Only built when there is
    // an identity to sign with — publishing is the whole point.
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

    // NIP-42: relays that gate reads behind AUTH serve nothing until we answer
    // their challenge — and an unanswered challenge looks exactly like an
    // ordinary empty relay. Attaching the authenticator is enough.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    /**
     * What actually goes down the wire, for when the counters stop making
     * sense. The error half — NOTICE, CLOSED, failed sends — is on always:
     * those are the relay explaining itself. `sent`/`full` add outgoing
     * commands / every message.
     */
    private val wireLog =
        when (wireLogMode) {
            "full", "sent" -> {
                // The sent/received lines are DEBUG and quartz's floor is WARN
                // in every deployment we run — without lowering it the switch
                // would be accepted, construct its logger, and print nothing.
                if (Log.minLevel > LogLevel.DEBUG) {
                    Log.minLevel = LogLevel.DEBUG
                    System.err.println(
                        "router: SYNC_WIRE_LOG=$wireLogMode lowered the quartz log floor to DEBUG — this is verbose",
                    )
                }
                RelayLogger(client, debugSending = true, debugReceiving = wireLogMode == "full")
            }

            else -> {
                RelayLogger(client, debugSending = false, debugReceiving = false)
            }
        }

    // OutOfMemoryError kills whichever thread allocates next and is caught by
    // nobody; counted so the health line can say the process is damaged
    // rather than merely quiet.
    private val fatals = AtomicLong()

    /** Relays with a transfer actually running, across every path. */
    private val transferring = AtomicInteger()

    // One stream reconciles at a time (static and dynamic both): each holds
    // its whole id set for its whole run, and concurrent sets sum on the heap.
    private val streamGate = Semaphore(1)

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val dynamicStreams = config.dynamicStreams()

    // The relays we hold a live subscription on; a dynamic sync must not drop
    // one of these sockets out from under its tail.
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    private val phases = StreamPhases()
    private val paging = PagingProgress()
    private val ingest = IngestPipeline(store, config, audit, servingPressure, scope, knownIds)

    /**
     * The automatic window chunker. A peer's cap arrives through quartz —
     * `NegentropySyncResult.peerCap`, parsed off the relay's own refusal — so
     * nothing here has to watch the wire for it.
     */
    private val pager =
        NegentropyPager(
            StoreWindowIndex(store),
            ClientWindowSync(client),
            sweepState,
            NegPageTuning(
                target = config.negPageTarget,
                minTarget = config.negPageMin,
                maxTarget = config.negPageMax,
                slackSeconds = config.negPageSlackSec,
            ),
        )
    private val backfill = StaticBackfill(client, store, config, bands, ingest, phases, paging, pager, streamGate, transferring, scope)
    private val dynamic = DynamicSync(client, store, bands, ingest, phases, paging, streamGate, transferring, monitor, pinnedUrls, tor, scope)
    private val upPush = UpstreamPush(client, store, config.upIntervalSec, streamGate, scope)
    private val pressure = servingPressure

    fun start(): SyncEngine {
        if (downUpstreams.isEmpty() && upUpstreams.isEmpty() && dynamicStreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            return this
        }

        ingest.start()

        // Said at boot, both ways: a transport that is configured but not
        // answering must not be discovered later, one silent onion relay at a
        // time. The probe asks our own SOCKS port, so a false answer here is
        // a statement about this container and nobody else's server.
        tor?.let {
            val reach = if (it.socksAnswers()) "answering" else "NOT answering — .onion relays will be skipped until it does"
            System.err.println(
                "router: tor SOCKS ${it.settings.socksAddress} $reach" +
                    (if (it.settings.everything) "; SYNC_TOR_ALL is on — EVERY upstream goes through it" else " (.onion upstreams only)"),
            )
        }

        // Make a fatal error visible instead of leaving a silent process that
        // looks merely quiet — four OOMs once passed unnoticed while the
        // phases still read healthy.
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, error ->
            if (error is VirtualMachineError) {
                fatals.incrementAndGet()
                System.err.println("router: FATAL ${error.javaClass.simpleName} killed thread ${thread.name} — the router is now degraded")
            }
            previous?.uncaughtException(thread, error)
        }
        scope.launch { healthLoop() }

        // Down live tail: subscribe on each upstream from now forward.
        // History is the backfill's job, so the tail never floods on connect.
        val liveSince = nowSeconds()
        downUpstreams.forEachIndexed { i, up ->
            client.subscribe(
                subId = "vespa-mirror-down-$i",
                filters = mapOf(up.url to listOf(up.filter.copy(since = liveSince))),
                listener = downListener(up),
            )
        }
        client.connect()

        // Registered BEFORE anything is launched: a configured stream must
        // appear in the report from the first tick, so silence can never be
        // read as "not configured".
        downUpstreams.map { it.streamName }.distinct().forEach { phases.register(it) }
        dynamicStreams.forEach { phases.register(it.name) }

        if (downUpstreams.isNotEmpty()) {
            backfill.begin(downUpstreams.size)
            scope.launch { backfill.run(downUpstreams) }
            scope.launch { backfill.progressLoop(dynamicStreams.size) }
        }

        upUpstreams.forEach { up -> scope.launch { upPush.loop(up) } }

        dynamicStreams.forEach { stream -> scope.launch { dynamic.loop(stream) } }

        // The phase report runs for the life of the engine, not inside the
        // static backfill's progress loop: a dynamic-only config has no
        // backfill loop at all, and everyone else's dynamic streams — the
        // larger half of the fill — outlive it.
        if (downUpstreams.isNotEmpty() || dynamicStreams.isNotEmpty()) {
            scope.launch {
                while (scope.isActive) {
                    delay(PROGRESS_INTERVAL_MS)
                    phases.report().forEach { System.err.println(it) }
                }
            }
        }

        scope.launch { statsLoop() }

        System.err.println(
            "router: ${downUpstreams.size} down + ${upUpstreams.size} up relay(s)" +
                (if (downUpstreams.isNotEmpty()) "; backfilling ${downUpstreams.size}" else "; live-tail only") +
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

    private fun downListener(up: SyncUpstream): SubscriptionListener =
        object : SubscriptionListener {
            override suspend fun onEvent(
                event: Event,
                isLive: Boolean,
                relay: NormalizedRelayUrl,
                forFilters: List<Filter>?,
            ) {
                // Bind trust to the relay we dialed, and re-check scope so a
                // broken upstream can't widen what we ingest.
                if (relay != up.url) return
                if (!up.filter.match(event)) return
                ingest.submit(event, up.trusted)
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
     * Why the machine is idle, once a minute. A full heap, a full queue and
     * an empty queue each mean something different, and together they name
     * the bottleneck without guessing — every stall this router has had was
     * diagnosed from outside it until this line existed.
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
            val events = ingest.accepted.get() + ingest.rejected.get()
            val now = System.currentTimeMillis()
            val rate = ((events - lastEvents) * 1000.0 / (now - lastAt).coerceAtLeast(1)).toInt()
            lastEvents = events
            lastAt = now
            val depth = ingest.queued.get()
            System.err.println(
                "router: health heap $usedMb/${maxMb}MB ($heapPct%)" +
                    (if (heapPct >= 90) " !! AT THE CEILING" else "") +
                    ", ingest queue $depth/${ingest.capacity}" +
                    // Full and empty are opposite diagnoses that look
                    // identical everywhere else; the depth is an instant and
                    // the rate a 60s average, so only the pair tells them
                    // apart.
                    (
                        when {
                            depth >= ingest.capacity -> " FULL (ingest is the limit — downloads are backpressured)"
                            depth == 0 && rate == 0 -> " empty (nothing is arriving — the limit is upstream of ingest)"
                            depth == 0 -> " drained (ingest is keeping up; downloads are the limit)"
                            else -> ""
                        }
                    ) +
                    ", $rate ev/s" +
                    ", ${transferring.get()} relay(s) transferring" +
                    ", ${client.connectedRelaysFlow().value.size} connected" +
                    (if (fatals.get() > 0) ", ${fatals.get()} FATAL error(s) — threads were killed" else "") +
                    (
                        dynamic.deleted
                            .get()
                            .takeIf { it > 0 }
                            ?.let { ", $it record(s) DELETED as retracted upstream" } ?: ""
                    ) +
                    (pressure?.describe()?.let { ", $it" } ?: "") +
                    (
                        if (ingest.lostToStore.get() > 0) {
                            ", ${ingest.lostToStore.get()} event(s) LOST to store errors (good events, gone — check the schema)"
                        } else {
                            ""
                        }
                    ),
            )
            // Named, because "16,248 skipped" says nothing about which corner
            // of the network we stopped looking at.
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
                "router: ingested ${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected${ingest.rejectionBreakdown()}" +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${upPush.pushed.get()} up" else "") +
                    // A dynamic cycle connects relays that are in no upstream
                    // list, so the connected count is reported against the
                    // pinned ones rather than as a fraction of them.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (dynamicStreams.isNotEmpty()) " + dynamic" else ""),
            )
            // Where the minute actually went, per ingest stage — this is what
            // identified a projection read-back as 90% of ingest.
            IngestStats.statusLine().takeIf { it.isNotEmpty() }?.let { System.err.println("router: ingest $it") }
        }
    }

    /** Accepted/rejected/pushed counters, for tests and a final log line. */
    fun stats(): Triple<Long, Long, Long> = Triple(ingest.accepted.get(), ingest.rejected.get(), upPush.pushed.get())

    /** Number of distinct configured upstreams (down + up) being mirrored. */
    fun upstreamCount(): Int = pinnedUrls.size

    /** Number of streams whose relays are discovered from the store, not configured. */
    fun dynamicStreamCount(): Int = dynamicStreams.size

    override fun close() {
        // First: a backfill killed mid-flight still keeps the ground it gained.
        runCatching { bands.flush() }
        // The same reasoning one level finer — a sweep killed between windows
        // resumes at the window it reached, not at the top of the range.
        runCatching { sweepState.flush() }
        // Bounded flush of the monitor's liveness records: the engine being
        // unreachable is a normal way for a relay to be going down, and that
        // client has no read deadline — unbounded would hang exactly when it
        // is most likely to.
        runCatching {
            runBlocking {
                withTimeoutOrNull(SHUTDOWN_FLUSH_MS) { monitor?.flush() }
            }
        }
        runCatching { monitor?.close() }
        runCatching { authenticator?.destroy() }
        downUpstreams.indices.forEach { runCatching { client.unsubscribe("vespa-mirror-down-$it") } }
        runCatching { client.close() }
        ingest.closeIntake()
        scope.cancel()
        // After the scope, so a worker mid-batch is cancelled rather than
        // stranded on a pool that has stopped accepting work.
        ingest.close()
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        System.err.println(
            "router: stopped (${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected${ingest.rejectionBreakdown()}, ${upPush.pushed.get()} pushed)",
        )
    }

    companion object {
        private const val MAX_CONCURRENT_SOCKETS = 1024
        private const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20

        /** How long a shutdown will wait on the monitor's last write before giving up. */
        private const val SHUTDOWN_FLUSH_MS = 5_000L
    }
}
