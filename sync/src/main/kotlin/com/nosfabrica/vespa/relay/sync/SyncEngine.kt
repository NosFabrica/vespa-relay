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
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.fold.StoredFold
import com.nosfabrica.vespa.relay.ingest.AddressVersion
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.RelayWriter
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
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
import com.nosfabrica.vespa.relay.util.repeatEvery
import com.nosfabrica.vespa.relay.util.shutdownStep
import com.nosfabrica.vespa.relay.util.watchForFatals
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MessageDecoder
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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
    /** Answers upstream NIP-42 challenges, and is whose verdicts the roster follows when `verdictsFrom` is unset. */
    private val signer: NostrSigner? = null,
    /** "" (errors only), "sent" or "full". */
    wireLogMode: String = "",
    /**
     * The relay's client-read latency: the healer yields to it, and the status page reports it.
     * Ingest does not, since the relay's mirror lane already pauses when client reads slow down,
     * and pausing here too paid the same back-off twice per batch.
     */
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
    /**
     * Where the mirror's writes go: ingest's batches and the retraction audit's deletes. [store]
     * stays the read side in either mode. Closed with the engine.
     */
    private val writer: StoreWriter = StoreWriter.local(store),
) : AutoCloseable {
    /** The scope every subsystem runs on. [storeCalls] rides it as a context element. */
    private val scope = CoroutineScope(Dispatchers.IO + parentContext + storeCalls)

    /** The websocket client, socket budget, Tor and NIP-42. */
    private val peers = PeerClient(scope, signer, torSettings, wireLogMode, config.connectionTimeoutSec, MessageDecoder.Default)
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
    private val refusals = RouterRefusalSink(refusedIds, healQueue, refusedIds.enabled, healingPossible, forwarded = writer is RelayWriter)
    private val ingest = IngestPipeline(store, config.ingestTuning(), audit, servingPressure = null, scope, knownIds, newestVersions, refusals, writer = writer, bootName = "sync: ingest")
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

    /** One socket refcount across every stream. */
    private val sockets = RelaySockets(client, pinnedUrls)

    /** The deleteMissing comparison for the pool's retracting asks. */
    private val retraction = RetractionAudit(ClientRetractionReads(client), store, bands, ingest, refusedIds, writer)

    /** Whose 30166 records the roster reads: `verdictsFrom`, or this router's own key when that is unset. */
    private val verdictSigners = config.verdictSigners(signer?.pubKey)

    /** Those signers' verdicts, read back. Empty when nobody is followed. */
    private val verdicts = RelayVerdictRecord(store, signer = null, readFrom = verdictSigners)

    /** Says, on the health clock, which verdict-sourced streams no stored verdict feeds. */
    private val verdictSupply = VerdictSupply(store, visitStreams)

    /** The duplicate-url fold, read from those verdicts alone. */
    private val fold = StoredFold.over(store, verdictSigners, allowOnion = tor != null)

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
                    foldedAway = fold::foldedAway,
                    keepBands = pinnedUrls,
                    tor = tor,
                    verdicts = verdicts::load,
                    // Drift is counted against the signers followed; following nobody owes no verdict.
                    watching = verdictSigners.isNotEmpty(),
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

    /** Starts ingest, the client, the pool and the progress document; a push-only router still needs each. */
    fun start(): SyncEngine {
        val mirrors = visitStreams.isNotEmpty() || upUpstreams.isNotEmpty()
        if (!mirrors) System.err.println("router: no upstreams configured; nothing to mirror")

        ingest.start()
        registerProcessors()
        peers.announceTor()
        watchForFatals("router", fatals)
        scope.launch { healthLoop() }
        peers.connect()

        // Registered before anything is launched, so silence never reads as unconfigured.
        visitStreams.forEach { phases.register(it.name) }
        upUpstreams.groupBy { it.streamName }.values.forEach { urls -> scope.launch { upPush.loop(urls) } }
        visitPool.start()
        System.err.println(verdictsLine())

        // Its own loop: the phase report below is skipped for a push-only router.
        publishProgressLoop()
        if (visitStreams.isNotEmpty()) {
            scope.launch { repeatEvery(PROGRESS_INTERVAL_MS, "phase report") { phases.report().forEach { System.err.println(it) } } }
        }
        scope.launch { statsLoop() }

        if (!mirrors) return this
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

    /** Whose verdicts the roster follows, said once at boot so `verdictsFrom = []` never reads as a broken fold. */
    private fun verdictsLine(): String =
        when {
            verdictSigners.isEmpty() -> "router: the roster follows nobody's verdicts (verdictsFrom = [] or no RELAY_NSEC): no fold, no NIP-77 pacing"
            config.verdictsFrom == null -> "router: the roster follows our own verdicts (${verdictSigners.single().take(12)}…); set verdictsFrom to follow another monitor"
            else -> "router: the roster follows the verdicts of ${verdictSigners.size} signer(s) from verdictsFrom: ${verdictSigners.joinToString { it.take(12) + "…" }}"
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

    /** The status document on its own clock. It describes the process, so it runs with no streams at all. */
    private fun publishProgressLoop() {
        scope.launch {
            repeatEvery(PROGRESS_INTERVAL_MS, "progress publish") {
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
        val rates = IngestRates(System.currentTimeMillis())
        repeatEvery(60_000, "health line") {
            val reading = readHealth(rates)
            health = healthDocument(reading)
            System.err.println(healthLine(reading))
            // On the health clock so `wedged` above and the call it is stuck in read together.
            storeCalls.warnSlow().forEach { System.err.println(it) }
            verdictSupply.starved().forEach { System.err.println(it) }
        }
    }

    /** One reading of every counter the health line and the document share. */
    private fun readHealth(rates: IngestRates): HealthReading {
        val rt = Runtime.getRuntime()
        val (rate, arriving) = rates.next(ingest.accepted.get() + ingest.rejected.get(), ingest.submitted.get(), System.currentTimeMillis())
        val depth = ingest.queued.get()
        return HealthReading(
            heapUsedMb = (rt.totalMemory() - rt.freeMemory()) / 1_048_576,
            heapMaxMb = rt.maxMemory() / 1_048_576,
            queueDepth = depth,
            queueCapacity = ingest.capacity,
            bottleneck = bottleneckOf(ingest.wedged(), depth, ingest.capacity, rate),
            arrivingPerSec = arriving,
            eventsPerSec = rate,
            connected = client.connectedRelaysFlow().value.size,
            fatals = fatals.get(),
            retracted = retraction.deleted.get(),
            pressure = pressure?.describe(),
            ingestWorkers = ingest.workerCount,
            ingestWorkersRunning = ingest.workersRunning(),
            lostToStore = ingest.lostToStore.get(),
            inBatch = ingest.inBatch(),
            oldestBatchSec = ingest.oldestBatchMs() / 1000,
            oldestStoreCall = storeCalls.describeOldest(),
        )
    }

    /** The document's health block, on this clock rather than the progress tick: the stage split explains `bottleneck`. */
    private fun healthDocument(r: HealthReading): SyncProgress.Health {
        val load = peers.socketLoad()
        return SyncProgress.Health(
            bottleneck = r.bottleneck,
            eventsPerSec = r.eventsPerSec,
            arrivingPerSec = r.arrivingPerSec,
            heapUsedMb = r.heapUsedMb,
            heapMaxMb = r.heapMaxMb,
            sockets = r.connected,
            socketCeiling = PeerClient.MAX_CONCURRENT_SOCKETS,
            socketsRunning = load.running,
            socketsQueued = load.queued,
            servingMs = pressure?.meanMs(),
            stageDetail = stageSplit(),
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
    }

    private suspend fun statsLoop() {
        repeatEvery(60_000, "stats line") {
            val pool = visitPool.workerHealth()
            System.err.println(
                "router: ingested ${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                    ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${upPush.pushed.get()} up" else "") +
                    // Discovery connects relays outside every upstream list, so connected sits beside pinned.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (discoveryStreams.isNotEmpty()) " + discovery" else "") +
                    (if (refusedIds.enabled) "; ${refusedIds.summary()}" else "") +
                    (if (healQueue.enqueued.get() > 0 || healer.pushed.get() > 0) "; ${healer.summary()}" else "") +
                    (if (visitStreams.isNotEmpty()) "; visit workers ${pool.alive}/${pool.configured} alive" else "") +
                    (if (pool.respawned > 0) ", ${pool.respawned} replaced after dying" else ""),
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

    /** Each step guarded, so one that throws never keeps the next from running. */
    override fun close() {
        // Cursors first: a visit or sweep killed mid-flight keeps the ground it gained.
        shutdownStep("flush the bands") { bands.flush() }
        shutdownStep("flush the sweep state") { sweepState.flush() }
        // Workers before transport, so cancelled visits do not count their own deaths as aborts.
        shutdownStep("cancel the workers") { scope.cancel() }
        // Listeners off the client before the client's pool closes.
        shutdownStep("detach the complaints listener") { complaints.close() }
        shutdownStep("detach the pages listener") { pages.close() }
        shutdownStep("close the peer client") { peers.close() }
        shutdownStep("close ingest's intake") { ingest.closeIntake() }
        // After the scope, so a worker mid-batch is cancelled rather than stranded.
        shutdownStep("close ingest") { ingest.close() }
        // Last: ingest's final batch may still be on its way to the relay.
        shutdownStep("close the writer") { writer.close() }
        System.err.println(
            "router: stopped (${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                ", ${upPush.pushed.get()} pushed)",
        )
    }

    /** The pool's current roster, as the status page's per-relay table needs it. */
    fun primeUnits(): List<RelayStatusReport.PrimeUnit> = visitPool.primeUnits()

    companion object {
        // Published names in the progress document; a rename breaks readers charting them.
        const val VISITS_PROCESSOR = "visits"
        const val INGEST_PROCESSOR = "ingest"
        const val HEAL_PROCESSOR = "heal"
        const val PUSH_PROCESSOR = "upstreamPush"
    }
}
