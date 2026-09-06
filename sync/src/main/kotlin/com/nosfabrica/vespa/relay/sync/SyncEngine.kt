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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.ingest.AddressVersion
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.monitor.MonitorEngine
import com.nosfabrica.vespa.relay.monitor.MonitorStatus
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.status.RelayStatusReport
import com.nosfabrica.vespa.relay.status.StreamPhases
import com.nosfabrica.vespa.relay.status.SyncProgress
import com.nosfabrica.vespa.relay.sync.ClientRelayComplaints
import com.nosfabrica.vespa.relay.sync.ClientRelayPages
import com.nosfabrica.vespa.relay.sync.ClientRelayReads
import com.nosfabrica.vespa.relay.sync.ClientWindowSync
import com.nosfabrica.vespa.relay.sync.FilterWidths
import com.nosfabrica.vespa.relay.sync.NegPageTuning
import com.nosfabrica.vespa.relay.sync.NegentropyPager
import com.nosfabrica.vespa.relay.sync.PROGRESS_INTERVAL_MS
import com.nosfabrica.vespa.relay.sync.PoolLimits
import com.nosfabrica.vespa.relay.sync.RetractionAudit
import com.nosfabrica.vespa.relay.sync.RosterBuilder
import com.nosfabrica.vespa.relay.sync.StoreWindowIndex
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.sync.UpstreamPush
import com.nosfabrica.vespa.relay.sync.VisitPool
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.sync.heal.WriteCapability
import com.nosfabrica.vespa.relay.sync.refused.RouterRefusalSink
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The router: a strfry-style mirror that moves events between each configured upstream and
 * the served relay's store. Down streams ride [VisitPool], up streams are [UpstreamPush]; this
 * class owns the shared plumbing, and [close] stops touching the store before the store closes.
 */
class SyncEngine(
    private val store: IEventStore,
    private val config: RouterConfig,
    parentContext: CoroutineContext = SupervisorJob(),
    /** Runs every mirrored event through quartz's search-indexing parse; off by default. */
    audit: ParseAudit? = null,
    private val bands: SyncBands = SyncBands(null),
    private val sweepState: SweepState = SweepState(null),
    /** Ids twice refused by the store, and the repairs queued for the relays serving them. Disabled by default. */
    private val refusedIds: RefusedIds = RefusedIds.disabled(),
    /** Answers NIP-42 challenges and signs the monitor's verdicts. Without one every relay reads as unmeasured. */
    private val signer: NostrSigner? = null,
    /** "" (errors only), "sent" or "full". */
    wireLogMode: String = "",
    /** Ingest yields when client reads slow down; null mirrors at full speed. */
    servingPressure: ServingPressure? = null,
    /** The proxy .onion upstreams are dialled through; null drops .onion urls at discovery. */
    torSettings: TorSettings? = null,
    /** Which of these ids the store already holds, so a duplicate is dropped before it is verified. */
    knownIds: (suspend (List<String>) -> Set<String>)? = null,
    /** The newest stored version of each (kind, author) address, so a stale replaceable is dropped before it is verified. */
    newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)? = null,
    private val progress: SyncProgress = SyncProgress(),
    /** Installed on this engine's scope, so every store call made from a coroutine of ours books itself. */
    private val storeCalls: StoreCalls = StoreCalls(),
) : AutoCloseable {
    /** The scope every subsystem runs on. [storeCalls] rides it as a context element. */
    private val scope = CoroutineScope(Dispatchers.IO + parentContext + storeCalls)

    /** The websocket client, socket budget, Tor and NIP-42, shared with the monitor plane. */
    private val peers = PeerClient(scope, signer, torSettings, wireLogMode, config.connectionTimeoutSec)
    private val client = peers.client
    private val tor = peers.tor

    /** VirtualMachineErrors seen by the uncaught handler; the health line reports the process as damaged. */
    private val fatals = AtomicLong()

    /** One stream builds an id set at a time. Bounds the build, never a run. */
    private val streamGate = Semaphore(1)

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val discoveryStreams = config.discoveryStreams()

    /** The streams on the pool, retracting ones included. */
    private val visitStreams = config.streams.filter { VisitPool.ridesThePool(it) }

    /** Relays we hold a live subscription on; a discovery sync must not drop their sockets. */
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    private val phases = StreamPhases()

    /** The jobs that are not streams. */
    private val processors = Processors()

    /** Repairs discovered by ingest, drained per relay at the end of its visit. Drops rather than backpressures. */
    private val healQueue = HealQueue()
    private val writeCaps = WriteCapability()

    /** Whether ingest has to carry per-event origins at all. */
    private val healingPossible = config.streams.any { it.healContent || it.healRetractions }
    private val refusals = RouterRefusalSink(refusedIds, healQueue, refusedIds.enabled, healingPossible)
    private val ingest = IngestPipeline(store, IngestTuning(config.ingestConcurrency, config.ingestBatch), audit, servingPressure, scope, knownIds, newestVersions, refusals)
    private val healer = Healer(client, store, healQueue, writeCaps, refusedIds, servingPressure)

    /** What the upstreams say when they refuse. Attached to the client this engine owns, so released in [close]. */
    private val complaints = ClientRelayComplaints(client)

    /** What a refused ask carried anyway. Its own listener, because it needs arming to stay off the hot path. */
    private val pages = ClientRelayPages(client)

    /** Per-relay kind caps, one instance for the pool and the pager alike. */
    private val widths = FilterWidths()

    /** The window chunker. */
    private val pager =
        NegentropyPager(
            StoreWindowIndex(store),
            ClientWindowSync(client, widths, refused = refusedIds),
            sweepState,
            NegPageTuning(
                target = config.negPageTarget,
                minTarget = config.negPageMin,
                maxTarget = config.negPageMax,
                slackSeconds = config.negPageSlackSec,
            ),
            complaints,
        )

    /** One socket refcount across every stream and probe pass. */
    private val sockets = RelaySockets(client, pinnedUrls)

    /** The monitor plane, on its own clock, writing the verdicts the roster selects on. */
    private val monitor =
        MonitorEngine(
            store = store,
            settings = config.monitor,
            // The monitor's own declaration, handed over whole. Nothing here derives it from the
            // mirror's streams: the two planes name their relays independently.
            sources = config.monitorSources(),
            connectionTimeoutMs = config.connectionTimeoutSec * 1000L,
            peers = peers,
            signer = signer,
            sockets = sockets,
            // Submitted once, not once per wanting stream. Verified unless every wanting stream
            // trusts its source. Every stream, not the discovering ones: which streams the monitor
            // derives from is no longer a relationship, so who wanted an event is asked of them all.
            onProbeEvent = { event ->
                val wanted = config.streams.filter { it.filter.match(event) }
                if (wanted.isNotEmpty()) ingest.submit(event, wanted.all { it.trusted })
            },
            pinnedUrls = pinnedUrls,
            scope = scope,
        )

    /** The deleteMissing comparison for the pool's retracting asks. */
    private val retraction = RetractionAudit(client, store, bands, ingest, refusedIds)

    /** The monitor's own verdicts, read back. Null without a signer: nothing could have written them. */
    private val verdicts = signer?.let { RelayVerdictRecord(store, it) }

    /** The rotating pool, inert when no stream rides it. */
    private val visitPool =
        VisitPool(
            reads = ClientRelayReads(client),
            complaints = complaints,
            pages = pages,
            bands = bands,
            ingest = ingest,
            pager = pager,
            healer = healer,
            retraction = retraction,
            sockets = sockets,
            scope = scope,
            rosterBuilder =
                RosterBuilder(
                    store = store,
                    streams = visitStreams,
                    bands = bands,
                    foldedAway = monitor::foldedAway,
                    keepBands = pinnedUrls,
                    tor = tor,
                    verdicts = { urls -> verdicts?.load(urls) ?: RelayVerdictRecord.Verdicts() },
                    // Nothing signs without an identity, and nothing is measured without a source.
                    watching = signer != null && config.monitorSources() != null,
                ),
            streams = visitStreams,
            progress = processors.of(VISITS_PROCESSOR),
            phases = phases,
            workers = VisitPool.workersFor(visitStreams),
            limits = PoolLimits.of(visitStreams),
            widths = widths,
        )

    private val upPush = UpstreamPush(client, store, config.upIntervalSec, streamGate, scope)
    private val pressure = servingPressure

    fun start(): SyncEngine {
        if (visitStreams.isEmpty() && upUpstreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            // A monitor-only node still needs ingest draining, a connected client and the
            // progress document; each of those fails silently when skipped.
            ingest.start()
            registerProcessors()
            peers.announceTor()
            watchForFatals()
            scope.launch { healthLoop() }
            peers.connect()
            monitor.start()
            publishProgressLoop()
            scope.launch { statsLoop() }
            return this
        }

        ingest.start()
        registerProcessors()

        peers.announceTor()

        watchForFatals()
        scope.launch { healthLoop() }

        peers.connect()

        // Registered before anything is launched, so silence never reads as unconfigured.
        visitStreams.forEach { phases.register(it.name) }

        upUpstreams.forEach { up -> scope.launch { upPush.loop(up) } }

        visitPool.start()

        monitor.start()

        // Its own loop: the phase report below is skipped for a push-only router.
        publishProgressLoop()
        if (visitStreams.isNotEmpty()) {
            scope.launch {
                while (scope.isActive) {
                    delay(PROGRESS_INTERVAL_MS)
                    phases.report().forEach { System.err.println(it) }
                }
            }
        }

        scope.launch { statsLoop() }

        System.err.println(
            "router: ${visitStreams.size} down stream(s) on the pool (${downUpstreams.size} declared relay(s))" +
                " + ${upUpstreams.size} up relay(s)" +
                (if (upUpstreams.isNotEmpty()) "; up every ${config.upIntervalSec}s" else "") +
                (
                    if (discoveryStreams.isNotEmpty()) {
                        "; ${discoveryStreams.size} dynamic stream(s): " +
                            discoveryStreams.joinToString { "${it.name} (${it.discovery?.sources?.size} source(s))" }
                    } else {
                        ""
                    }
                ),
        )
        return this
    }

    /**
     * Wires the non-stream jobs to the counters they already keep. A job that is not running
     * is not registered at all: a zeroed row would claim it exists.
     */
    private fun registerProcessors() {
        processors.of(INGEST_PROCESSOR).let { p ->
            p.phase(Processors.RUNNING)
            p.reasons { ingest.rejectionReasons().map { (reason, n) -> Processors.Breakdown(reason, n) } }
            p.counts {
                listOf(
                    Processors.Count("queued", ingest.queued.get().toLong()),
                    Processors.Count("capacity", ingest.capacity.toLong()),
                    Processors.Count("accepted", ingest.accepted.get()),
                    Processors.Count("rejected", ingest.rejected.get()),
                    // The one counter that means data loss: verified events the store could not write.
                    Processors.Count("lostToStore", ingest.lostToStore.get()),
                    // inBatch against workers separates a backpressured queue from a stopped one.
                    Processors.Count("inBatch", ingest.inBatch().toLong()),
                    Processors.Count("workers", ingest.workerCount.toLong()),
                    Processors.Count("oldestBatchSec", ingest.oldestBatchMs() / 1000),
                    Processors.Count("workersRunning", ingest.workersRunning().toLong()),
                )
            }
        }
        if (healingPossible) {
            processors.of(HEAL_PROCESSOR).let { p ->
                p.phase(Processors.RUNNING)
                p.counts {
                    listOf(
                        // The live depth, the same meaning `queued` has on the ingest row.
                        Processors.Count("queued", healQueue.size().toLong()),
                        Processors.Count("dropped", healQueue.dropped.get()),
                        Processors.Count("pushed", healer.pushed.get()),
                    )
                }
            }
        }
        if (upUpstreams.isNotEmpty()) {
            processors.of(PUSH_PROCESSOR).let { p ->
                p.phase(Processors.RUNNING)
                p.counts { listOf(Processors.Count("pushed", upPush.pushed.get())) }
            }
        }
    }

    /**
     * Where the constraint is, read once for both the health line and the document. A full
     * queue is `ingest` only while it drains; a worker held inside one batch for minutes is `wedged`.
     */
    private fun bottleneckOf(
        depth: Int,
        rate: Int,
    ): String =
        when {
            ingest.wedged() -> "wedged"
            depth >= ingest.capacity -> "ingest"
            depth == 0 && rate == 0 -> "upstream"
            depth == 0 -> "downloads"
            else -> "mixed"
        }

    /**
     * Every ingest stage the store has booked, busiest first, with the shape of each one's
     * time beside its total. One `snapshot()` read, so a row's members describe one instant.
     */
    private fun stageSplit(): List<SyncProgress.StageDetail> =
        IngestStats
            .snapshot()
            .map { (name, st) ->
                SyncProgress.StageDetail(
                    stage = name,
                    ms = st.totalNanos / 1_000_000,
                    calls = st.calls,
                    meanMs = st.meanNanos / 1_000_000,
                    maxMs = st.maxNanos / 1_000_000,
                )
            }.sortedByDescending { it.ms }

    /** The latest health, for the progress tick to publish. */
    @Volatile
    private var health: SyncProgress.Health? = null

    /** Counts VirtualMachineErrors so a damaged process does not read as merely quiet. Both boot paths need it. */
    private fun watchForFatals() {
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, error ->
            if (error is VirtualMachineError) {
                fatals.incrementAndGet()
                System.err.println("router: FATAL ${error.javaClass.simpleName} killed thread ${thread.name} — the router is now degraded")
            }
            previous?.uncaughtException(thread, error)
        }
    }

    /** The status document on its own clock. It describes the process, so it runs with no streams at all. */
    private fun publishProgressLoop() {
        scope.launch {
            while (scope.isActive) {
                delay(PROGRESS_INTERVAL_MS)
                progress.publish(
                    phases.snapshot(),
                    processors.snapshot(),
                    health,
                    visitPool.livePool(),
                    fatals.get(),
                    // Same tick as the rest, so the batch row and the store-call row describe one instant.
                    store = storeCalls.snapshot(),
                )
            }
        }
    }

    /** Why the machine is idle, once a minute: heap, queue depth and both queue rates name the bottleneck. */
    private suspend fun healthLoop() {
        var lastEvents = 0L
        var lastSubmitted = 0L
        var lastAt = System.currentTimeMillis()
        while (scope.isActive) {
            delay(60_000)
            val rt = Runtime.getRuntime()
            val usedMb = (rt.totalMemory() - rt.freeMemory()) / 1_048_576
            val maxMb = rt.maxMemory() / 1_048_576
            val heapPct = if (maxMb > 0) usedMb * 100 / maxMb else 0
            val events = ingest.accepted.get() + ingest.rejected.get()
            // `rate` is what left a batch and `arriving` what went in; only the pair tells a
            // full queue at 0 ev/s from a fan-out gone quiet.
            val submitted = ingest.submitted.get()
            val now = System.currentTimeMillis()
            val windowMs = (now - lastAt).coerceAtLeast(1)
            val rate = ((events - lastEvents) * 1000.0 / windowMs).toInt()
            val arriving = ((submitted - lastSubmitted) * 1000.0 / windowMs).toInt()
            lastEvents = events
            lastSubmitted = submitted
            lastAt = now
            val depth = ingest.queued.get()
            // Read once, shared by the document and the line, so the two cannot disagree.
            val constraint = bottleneckOf(depth, rate)
            val open = client.connectedRelaysFlow().value.size
            val load = peers.socketLoad()
            val stages = stageSplit()
            health =
                SyncProgress.Health(
                    bottleneck = constraint,
                    eventsPerSec = rate,
                    arrivingPerSec = arriving,
                    heapUsedMb = usedMb,
                    heapMaxMb = maxMb,
                    sockets = open,
                    socketCeiling = PeerClient.MAX_CONCURRENT_SOCKETS,
                    socketsRunning = load.running,
                    socketsQueued = load.queued,
                    servingMs = pressure?.meanMs(),
                    // On this clock rather than the progress tick: the stage split explains `bottleneck`.
                    stageDetail = stages,
                    // What holds the store's write lock now; null means nothing does.
                    lockHeld =
                        IngestStats.heldNow()?.let { held ->
                            SyncProgress.LockHeld(
                                stage = held.stage,
                                heldMs = held.heldForMillis(),
                                detail = held.detail,
                            )
                        },
                    feed = (store as? VespaEventStore)?.runCatching { feedStatus() }?.getOrNull(),
                )
            System.err.println(
                "router: health heap $usedMb/${maxMb}MB ($heapPct%)" +
                    (if (heapPct >= 90) " !! AT THE CEILING" else "") +
                    ", ingest queue $depth/${ingest.capacity}" +
                    (
                        when (constraint) {
                            "wedged" -> {
                                " FULL and NOT DRAINING — ingest is wedged, not backpressured: " +
                                    "${ingest.inBatch()}/${ingest.workerCount} worker(s) in a batch, the oldest " +
                                    "for ${ingest.oldestBatchMs() / 1000}s. The store stopped answering; look " +
                                    "there, not at the relays" +
                                    (
                                        storeCalls.describeOldest()?.let { ". Longest store call: $it" }
                                            ?: ". No store call is outstanding — the workers are held somewhere other than the store"
                                    )
                            }

                            "ingest" -> {
                                " FULL (ingest is the limit — downloads are backpressured)"
                            }

                            "upstream" -> {
                                " empty (nothing is arriving — the limit is upstream of ingest)"
                            }

                            "downloads" -> {
                                " drained (ingest is keeping up; downloads are the limit)"
                            }

                            else -> {
                                ""
                            }
                        }
                    ) +
                    ", $arriving ev/s in, $rate ev/s out" +
                    ", $open connected" +
                    (if (fatals.get() > 0) ", ${fatals.get()} FATAL error(s) — threads were killed" else "") +
                    (
                        retraction.deleted
                            .get()
                            .takeIf { it > 0 }
                            ?.let { ", $it record(s) DELETED as retracted upstream" } ?: ""
                    ) +
                    (pressure?.describe()?.let { ", $it" } ?: "") +
                    (
                        ingest.workerCount
                            .minus(ingest.workersRunning())
                            .takeIf { it > 0 }
                            ?.let { ", $it of ${ingest.workerCount} ingest worker(s) HAVE STOPPED — that share of the queue has no drain" }
                            ?: ""
                    ) +
                    (
                        if (ingest.lostToStore.get() > 0) {
                            ", ${ingest.lostToStore.get()} event(s) LOST to store errors (good events, gone — check the schema)"
                        } else {
                            ""
                        }
                    ),
            )
            // On the health clock so `wedged` above and the call it is stuck in read together.
            storeCalls.warnSlow().forEach { System.err.println(it) }
            monitor.heldOutDead().takeIf { it > 0 }?.let { dead ->
                System.err.println(
                    "router: health $dead relay(s) carry a current `dead` verdict of ours and are held out of the probe passes",
                )
            }
        }
    }

    private suspend fun statsLoop() {
        while (scope.isActive) {
            delay(60_000)
            System.err.println(
                "router: ingested ${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                    ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${upPush.pushed.get()} up" else "") +
                    // Discovery connects relays outside every upstream list, so connected sits beside pinned.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (discoveryStreams.isNotEmpty()) " + discovery" else "") +
                    (if (refusedIds.enabled) "; ${refusedIds.summary()}" else "") +
                    (if (healQueue.enqueued.get() > 0 || healer.pushed.get() > 0) "; ${healer.summary()}" else ""),
            )
            IngestStats.statusLine().takeIf { it.isNotEmpty() }?.let { System.err.println("router: ingest $it") }
            ingest.probeStatus().takeIf { it.isNotEmpty() }?.let { System.err.println(it) }
        }
    }

    /** Accepted/rejected/pushed counters, for tests and a final log line. */
    fun stats(): Triple<Long, Long, Long> = Triple(ingest.accepted.get(), ingest.rejected.get(), upPush.pushed.get())

    /** Number of distinct configured upstreams (down + up) being mirrored. */
    fun upstreamCount(): Int = pinnedUrls.size

    /** Number of streams whose relays are discovered from the store, not configured. */
    fun dynamicStreamCount(): Int = discoveryStreams.size

    override fun close() {
        // Cursors first: a visit or sweep killed mid-flight keeps the ground it gained.
        runCatching { bands.flush() }
        runCatching { sweepState.flush() }
        // Workers before transport, so cancelled visits do not count their own deaths as aborts.
        scope.cancel()
        // Listeners off the client before the client's pool closes.
        runCatching { complaints.close() }
        runCatching { pages.close() }
        peers.close()
        ingest.closeIntake()
        // After the scope, so a worker mid-batch is cancelled rather than stranded.
        ingest.close()
        System.err.println(
            "router: stopped (${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                ", ${upPush.pushed.get()} pushed)",
        )
    }

    /** The pool's current roster, as the status page's per-relay table needs it. */
    fun primeUnits(): List<RelayStatusReport.PrimeUnit> = visitPool.primeUnits()

    /** The monitor plane's status document. */
    fun monitorStatus(
        everySeconds: Long,
        relayUrl: String?,
    ): MonitorStatus = monitor.status(everySeconds, relayUrl)

    companion object {
        // Published names in the progress document; a rename breaks readers charting them.
        const val VISITS_PROCESSOR = "visits"
        const val INGEST_PROCESSOR = "ingest"
        const val HEAL_PROCESSOR = "heal"
        const val PUSH_PROCESSOR = "upstreamPush"
    }
}
