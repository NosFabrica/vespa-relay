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
package com.nosfabrica.vespa.relay

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
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.progress.SyncProgress
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.nosfabrica.vespa.relay.sync.ClientWindowSync
import com.nosfabrica.vespa.relay.sync.NegPageTuning
import com.nosfabrica.vespa.relay.sync.NegentropyPager
import com.nosfabrica.vespa.relay.sync.PROGRESS_INTERVAL_MS
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
 * The router: a strfry-style mirror. For each configured upstream it moves
 * events between that relay and the served relay's store.
 *
 * Down (`dir = down`/`both`): a live REQ subscription streams new events into
 * the store through [IngestPipeline]; [VisitPool] catches up on history
 * first. Up (`dir = up`/`both`): [UpstreamPush] periodically reconciles the
 * store against the upstream and publishes what it is missing. Dynamic
 * (`relaySource = [...]`): [VisitPool] builds its roster from the monitor's
 * kind-30166 verdicts and rides it — constantly connected, audited weekly.
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
    // The ids twice refused by the store, so a reconcile stops asking for
    // them, and the queue of repairs to hand back to the relays serving them.
    // Disabled by default: the filter answers no to everything and records
    // nothing until SYNC_REFUSED_DIR is set.
    private val refusedIds: RefusedIds = RefusedIds.disabled(),
    // Answers NIP-42 challenges from upstreams that gate reads behind AUTH,
    // and signs every verdict the monitor passes publish.
    private val signer: NostrSigner? = null,
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
    // The newest stored version of each (kind, author) address, so a stale
    // replaceable is dropped before it is verified. Same reason as knownIds:
    // the query is the store's, the pipeline takes a function.
    newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)? = null,
    // What each stream is doing, and the disposition of every url its current
    // cycle took on — see [SyncProgress]. Republished on the progress tick and
    // read by this process's own status site off the same heap.
    private val progress: SyncProgress = SyncProgress(),
) : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.IO + parentContext)

    /**
     * HOW THIS PROCESS TALKS TO OTHER RELAYS — the websocket client, the socket
     * budget, Tor and NIP-42, shared with the monitor plane rather than owned
     * by either. See [PeerClient] for why one pool is the point.
     */
    private val peers = PeerClient(scope, signer, torSettings, wireLogMode, config.connectionTimeoutSec)
    private val client = peers.client
    private val tor = peers.tor

    // OutOfMemoryError kills whichever thread allocates next and is caught by
    // nobody; counted so the health line can say the process is damaged
    // rather than merely quiet.
    private val fatals = AtomicLong()

    /** Relays with a transfer actually running, across every path. */

    // One stream BUILDS an id set at a time: the set is a store walk and
    // concurrent ones sum on the heap. It bounds the BUILD, never a run — the
    // pool is a rotation with no join, so holding it for a run would mean
    // holding it for the life of the process while every other stream queued.
    //
    // The sets themselves are per reconcile now and die with it. The shared
    // generational set the fan-out passed between its stragglers (`SharedIdSet`)
    // went with the fan-out.
    private val streamGate = Semaphore(1)

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val discoveryStreams = config.discoveryStreams()

    /**
     * The fork's arithmetic — see [VisitPool.ridesThePool]: every relaySource
     * entry answers to the monitor, or the stream keeps the legacy pass
     * machinery (the union path for the deployment mid-crossing). A
     * retracting stream rides the pool too, its comparison running as its
     * audit ([RetractionAudit]).
     */
    private val visitStreams = config.streams.filter { VisitPool.ridesThePool(it) }

    // The relays we hold a live subscription on; a discovery sync must not drop
    // one of these sockets out from under its tail.
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    private val phases = StreamPhases()

    /**
     * The work that is NOT a stream — the two probe passes, the NIP-66 monitor,
     * ingest, the healer, the push. See [Processors] for why they needed a
     * report of their own; [registerProcessors] is where each one is wired to
     * the counters it already keeps.
     */
    private val processors = Processors()

    // Repairs discovered by ingest, drained per relay at the end of its own
    // sync. Bounded and coalescing: it drops rather than backpressure the
    // sweep, because a dropped heal is a retry and a stalled sweep is not.
    private val healQueue = HealQueue()
    private val writeCaps = WriteCapability()

    // Whether any stream can heal at all decides whether ingest has to carry
    // per-event origins; see RefusalSink.tracksOrigins. Both switches default
    // off, so a deployment that has not opted in pays nothing.
    private val healingPossible = config.streams.any { it.healContent || it.healRetractions }
    private val refusals = RouterRefusalSink(refusedIds, healQueue, refusedIds.enabled, healingPossible)
    private val ingest = IngestPipeline(store, IngestTuning(config.ingestConcurrency, config.ingestBatch), audit, servingPressure, scope, knownIds, newestVersions, refusals)
    private val healer = Healer(client, store, healQueue, writeCaps, refusedIds, servingPressure)

    /**
     * The automatic window chunker. A peer's cap arrives through quartz —
     * `NegentropySyncResult.peerCap`, parsed off the relay's own refusal — so
     * nothing here has to watch the wire for it.
     */
    private val pager =
        NegentropyPager(
            StoreWindowIndex(store),
            ClientWindowSync(client, refused = refusedIds),
            sweepState,
            NegPageTuning(
                target = config.negPageTarget,
                minTarget = config.negPageMin,
                maxTarget = config.negPageMax,
                slackSeconds = config.negPageSlackSec,
            ),
        )

    /**
     * WHO IS STILL USING THIS SOCKET — one refcount across every stream and
     * every probe pass, which is why it is built here and handed to the monitor
     * rather than owned by either. See [RelaySockets].
     */
    private val sockets = RelaySockets(client, pinnedUrls)

    /**
     * THE OTHER PLANE. What is out there and how much of it can we use —
     * the fold, the consistency gate and the fitness grades, on their own
     * clock, writing the signed kind-30166 records this plane's roster then
     * selects on. See [MonitorEngine] for what it still takes from this side.
     */
    private val monitor =
        MonitorEngine(
            store = store,
            config = config,
            peers = peers,
            signer = signer,
            sockets = sockets,
            ingest = ingest,
            pinnedUrls = pinnedUrls,
            scope = scope,
        )

    /** The deleteMissing comparison for the pool's retracting asks — see [RetractionAudit]. */
    private val retraction = RetractionAudit(client, store, bands, ingest, refusedIds)

    /**
     * The monitor's own verdicts, read back — built on the same terms as the
     * fold and the passes: a signer, or nothing. Without one this router has
     * no identity to have written verdicts under, so every relay reads as
     * unmeasured and every ask keeps trying, which is what the pool did before
     * it could read one at all.
     */
    private val verdicts = signer?.let { RelayVerdictRecord(store, it) }

    /** The rotating pool — the visit-mode streams' whole engine. Inert when none are configured. */
    private val visitPool =
        VisitPool(
            client = client,
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
                    // The fitness pass has always measured and signed this and
                    // nothing has ever read it — see [RosterBuilder.Roster].
                    speaksNegentropy = { urls -> verdicts?.load(urls)?.speaksNegentropy ?: emptyMap() },
                ),
            streams = visitStreams,
            progress = processors.of(VISITS_PROCESSOR),
            phases = phases,
            visitConcurrency = config.visitConcurrency,
            tailBudget = config.tailBudget,
        )

    private val upPush = UpstreamPush(client, store, config.upIntervalSec, streamGate, scope)
    private val pressure = servingPressure

    fun start(): SyncEngine {
        if (visitStreams.isEmpty() && upUpstreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            // …AND THE MONITOR PLANE IS NOT THE MIRROR'S PASSENGER. It was
            // split into its own engine because measuring relays is not
            // mirroring them, and a `monitor { sources }` block with no stream
            // beside it is a whole deployment on its own: a node that publishes
            // verdicts for other routers to read. Behind this return it started
            // nothing and said nothing — the boot log went straight from
            // "nothing to mirror" to serving pages, and the monitor's rows sat
            // at their registered phase for the life of the process, which is
            // indistinguishable from a monitor that is running and finding
            // nothing.
            //
            // Ingest starts with it, and that pairing is load-bearing rather
            // than tidy: a probe hands everything it downloaded to
            // [IngestPipeline.submit], whose channel is bounded, so a monitor
            // running beside a pipeline nobody drains parks its first pass on a
            // full queue — the exact wedge [AliasProbe.deadlineMs] exists for,
            // arrived at by configuration instead of by a relay.
            ingest.start()
            registerProcessors()
            // …AND THE CLIENT HAS TO BE CONNECTED, which this path did not do.
            //
            // `NostrClient.subscribe` registers the request locally and then
            // guards BOTH `sendToRelayIfChanged` and `reconnect` behind
            // `isActive()` — a flag only `connect()` sets. Verified against the
            // pinned jar: the bytecode branches straight to `return` when it is
            // false. So on this path every probe REQ was recorded and none was
            // ever sent, every ladder rung lapsed on its idle window, and a
            // deployment whose whole job is to publish verdicts published
            // almost none: `dialVerdict` answers null when nothing came back at
            // all (the 3,945-relay rule), so the passes correctly wrote nothing
            // — and the one grade that could still be reached was `dead`, off
            // the TCP pre-probe, which is a raw socket and never needed the
            // client. A monitor-only node graded corpses and nothing else.
            //
            // Silent in both directions, which is what made it survive: the
            // rows moved, the passes ran, the counts were honest about a
            // measurement nobody could take.
            peers.announceTor()
            watchForFatals()
            scope.launch { healthLoop() }
            peers.connect()
            monitor.start()
            // …AND THE DOCUMENT, which this path also returned above. The block
            // that publishes it reasons about exactly this case in its own
            // comment — "there are no streams to report" is where a process
            // description matters most — and then never ran here, so a
            // monitor-only node moved its rows in memory and published none of
            // them. The per-stream report loop is not wanted: it is already
            // guarded on `visitStreams`, and there are none.
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

        // Registered BEFORE anything is launched: a configured stream must
        // appear in the report from the first tick, so silence can never be
        // read as "not configured".
        visitStreams.forEach { phases.register(it.name) }

        upUpstreams.forEach { up -> scope.launch { upPush.loop(up) } }

        // ONE ENGINE. Every down stream rides the rotating pool — declared
        // `urls` and discovered relays alike — for one queue, socket-owning
        // workers, tails, and the two clocks that re-check the past. The
        // legacy pass machinery is gone with the last stream that needed it:
        // it walked a static relay ONCE per process and then live-tailed, so
        // `negentropySyncThePastSeconds` and `refetchThePastSeconds` could
        // never mean anything there, and the tail it opened here on boot is
        // now the pool's, opened after that relay's catch-up rather than
        // before it.
        visitPool.start()

        // Only where there is something for it to work on — see
        // [MonitorEngine.hasSources], which is the whole of that question and
        // NOT `discoveryStreams` alone.
        monitor.start()

        // Its own loop, NOT a passenger on the phase report. That report is
        // skipped when a config has neither a down upstream nor a dynamic
        // stream — a push-only router — and with the publish inside it the
        // status page had nothing to draw at all, so a perfectly healthy mirror
        // rendered as a stopped one. This document describes the PROCESS as
        // much as its streams, and "there are no streams to report" is exactly
        // the case where that distinction matters.
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
     * THE JOBS THAT ARE NOT STREAMS, wired to the counters they already keep.
     *
     * Each one is a supplier over live atomics rather than a copy pushed on a
     * tick — see [Processors] — so nothing here can drift from the thing it
     * describes, and a processor that is not running at all is simply never
     * registered. That absence is the honest report: a router with no signer
     * publishes no fold and no NIP-66 monitor because it HAS none, and a zeroed
     * row would claim the opposite.
     *
     * The two probe passes are registered elsewhere — they are constructed with
     * their handles, because the pass writes its own work numbers and only
     * the alias monitor knows its clock.
     */
    private fun registerProcessors() {
        // The monitor's own rows are registered by [MonitorEngine], including
        // the `off` phases for a deployment with nothing to fold — the gate and
        // the rows have to answer one question, and the damaging way for them
        // to disagree is a row marked `off` under a monitor that is running.
        // WHERE EVERY MIRRORED EVENT ACTUALLY LANDS, and the first thing to look
        // at when the streams read busy and the store is not growing. The queue
        // depth against its capacity is the whole diagnosis: full means ingest
        // is the limit and the downloads are backpressured behind it, empty
        // means the limit is upstream of here. That pair was in a stderr line
        // once a minute and nowhere else.
        processors.of(INGEST_PROCESSOR).let { p ->
            p.phase(Processors.RUNNING)
            // Why the rejections were rejected. Reported as `undecided` rows are
            // — a reason, a count — because a mirror rejecting most of what it
            // is offered is the pipeline working, and the total alone cannot
            // say that.
            p.reasons { ingest.rejectionReasons().map { (reason, n) -> Processors.Breakdown(reason, n) } }
            p.counts {
                listOf(
                    Processors.Count("queued", ingest.queued.get().toLong()),
                    Processors.Count("capacity", ingest.capacity.toLong()),
                    Processors.Count("accepted", ingest.accepted.get()),
                    Processors.Count("rejected", ingest.rejected.get()),
                    // THE ONLY COUNTER HERE THAT MEANS DATA LOSS: events that
                    // passed every check and then could not be written. Good
                    // events, gone. It reached a stderr line and nothing else.
                    Processors.Count("lostToStore", ingest.lostToStore.get()),
                )
            }
        }
        // There is no `reachability` processor any more. It reported a passive
        // NIP-66 watcher that no longer exists, and its two numbers already
        // have homes that mean more: the fitness pass publishes a count per
        // verdict (`dead` among them), and the urls a `dead` verdict holds out
        // of a pass are `heldOutDead` on the alias source's own row.
        // Repairs discovered by ingest and handed back to the relays serving
        // them. Registered only where a stream opted in: with neither switch on,
        // the queue refuses everything and a row of zeros would look like a
        // healer that is failing rather than one nobody asked for.
        if (healingPossible) {
            processors.of(HEAL_PROCESSOR).let { p ->
                p.phase(Processors.RUNNING)
                p.counts {
                    listOf(
                        // The DEPTH, from `size()`. This published
                        // `enqueued` — a lifetime counter — under the same name
                        // ingest uses for a live queue depth, on the row right
                        // below it: one word, two quantities, adjacent.
                        Processors.Count("queued", healQueue.size().toLong()),
                        // The queue coalesces and DROPS rather than
                        // backpressuring the sweep, so what it threw away is a
                        // fact about this router that nothing else records.
                        Processors.Count("dropped", healQueue.dropped.get()),
                        Processors.Count("pushed", healer.pushed.get()),
                    )
                }
            }
        }
        // The only half of the router that WRITES to other people's relays.
        if (upUpstreams.isNotEmpty()) {
            processors.of(PUSH_PROCESSOR).let { p ->
                p.phase(Processors.RUNNING)
                p.counts { listOf(Processors.Count("pushed", upPush.pushed.get())) }
            }
        }
    }

    /**
     * WHERE THE CONSTRAINT IS, decided once and read twice.
     *
     * A full ingest queue and an empty one are opposite diagnoses that look
     * identical from every other number this router publishes, and the pair
     * (depth against capacity, and whether anything is arriving at all) is what
     * separates them. The health line has said this in prose for a while; the
     * document says the same word, from the same function, so the log and the
     * dashboard cannot drift into disagreeing about the one thing an operator
     * asks first.
     */
    private fun bottleneckOf(
        depth: Int,
        rate: Int,
    ): String =
        when {
            depth >= ingest.capacity -> "ingest"
            depth == 0 && rate == 0 -> "upstream"
            depth == 0 -> "downloads"
            else -> "mixed"
        }

    /** The latest health, for the progress tick to publish — see [bottleneckOf]. */
    @Volatile
    private var health: SyncProgress.Health? = null

    /**
     * Make a fatal error visible instead of leaving a silent process that looks
     * merely quiet — four OOMs once passed unnoticed while the phases still
     * read healthy.
     *
     * A named function rather than an inline block because BOTH boot paths need
     * it and only one had it: a monitor-only node is a whole deployment, and it
     * was the one running without this.
     */
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

    /**
     * The status document, on its own clock — hoisted for the same reason
     * [watchForFatals] is, and it is the one that matters most: this document
     * describes the PROCESS as much as its streams, so a deployment with no
     * streams at all is precisely where it must still be written.
     */
    private fun publishProgressLoop() {
        scope.launch {
            while (scope.isActive) {
                delay(PROGRESS_INTERVAL_MS)
                progress.publish(phases.snapshot(), processors.snapshot(), health, visitPool.livePool(), fatals.get())
            }
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
            // Read ONCE and shared by the document and the line below. Both
            // wanted the same two readings and each took its own, so a socket
            // opening between them let the published count and the logged one
            // disagree about the same instant — for a pair whose whole purpose
            // is that they cannot drift.
            val constraint = bottleneckOf(depth, rate)
            val open = client.connectedRelaysFlow().value.size
            // Published before it is printed, so the document carries the same
            // verdict the log does even if the line below is ever reworded.
            health =
                SyncProgress.Health(
                    bottleneck = constraint,
                    eventsPerSec = rate,
                    heapUsedMb = usedMb,
                    heapMaxMb = maxMb,
                    sockets = open,
                    socketCeiling = PeerClient.MAX_CONCURRENT_SOCKETS,
                    servingMs = pressure?.meanMs(),
                )
            System.err.println(
                "router: health heap $usedMb/${maxMb}MB ($heapPct%)" +
                    (if (heapPct >= 90) " !! AT THE CEILING" else "") +
                    ", ingest queue $depth/${ingest.capacity}" +
                    // Full and empty are opposite diagnoses that look
                    // identical everywhere else; the depth is an instant and
                    // the rate a 60s average, so only the pair tells them
                    // apart.
                    (
                        when (constraint) {
                            "ingest" -> " FULL (ingest is the limit — downloads are backpressured)"
                            "upstream" -> " empty (nothing is arriving — the limit is upstream of ingest)"
                            "downloads" -> " drained (ingest is keeping up; downloads are the limit)"
                            else -> ""
                        }
                    ) +
                    ", $rate ev/s" +
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
                        if (ingest.lostToStore.get() > 0) {
                            ", ${ingest.lostToStore.get()} event(s) LOST to store errors (good events, gone — check the schema)"
                        } else {
                            ""
                        }
                    ),
            )
            // Named, because "16,248 skipped" says nothing about which corner
            // of the network we stopped looking at.
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
                    // A discovery cycle connects relays that are in no upstream
                    // list, so the connected count is reported against the
                    // pinned ones rather than as a fraction of them.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (discoveryStreams.isNotEmpty()) " + discovery" else "") +
                    (if (refusedIds.enabled) "; ${refusedIds.summary()}" else "") +
                    (if (healQueue.enqueued.get() > 0 || healer.pushed.get() > 0) "; ${healer.summary()}" else ""),
            )
            // Where the minute actually went, per ingest stage — this is what
            // identified a projection read-back as 90% of ingest.
            IngestStats.statusLine().takeIf { it.isNotEmpty() }?.let { System.err.println("router: ingest $it") }
            // Beside the stages, because a probe that gated itself off shows up
            // there only as a stage that stopped appearing.
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
        // First: a visit killed mid-flight still keeps the ground it gained.
        runCatching { bands.flush() }
        // The same reasoning one level finer — a sweep killed between windows
        // resumes at the window it reached, not at the top of the range.
        runCatching { sweepState.flush() }
        // No monitor flush here any more: the passes write their verdicts
        // synchronously as they measure, so there is no buffered liveness to
        // lose on the way down.
        // Workers before transport: cancelled visits and tails stop touching
        // the client before it closes, instead of racing it and counting
        // their own deaths into `aborted`.
        scope.cancel()
        peers.close()
        ingest.closeIntake()
        // After the scope, so a worker mid-batch is cancelled rather than
        // stranded on a pool that has stopped accepting work.
        ingest.close()
        System.err.println(
            "router: stopped (${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                ", ${upPush.pushed.get()} pushed)",
        )
    }

    /**
     * The monitor plane's status document — see [MonitorEngine.status].
     *
     * Reached through this engine because this process composes the two; the
     * document itself is the monitor's, built over its own rows.
     */
    fun monitorStatus(
        everySeconds: Long,
        relayUrl: String?,
    ): MonitorStatus = monitor.status(everySeconds, relayUrl)

    companion object {
        // The names the progress document calls this router's non-stream jobs.
        // Spelled out as constants for the reason `StreamPhases.word` gives:
        // they are PUBLISHED, and a reader charting them must not have a row
        // renamed by a Kotlin refactor. The monitor's four live on
        // [MonitorEngine], beside the passes that fill them.

        /** The rotating pool — roster, tails, audits, visits. See [VisitPool]. */
        const val VISITS_PROCESSOR = "visits"
        const val INGEST_PROCESSOR = "ingest"
        const val HEAL_PROCESSOR = "heal"
        const val PUSH_PROCESSOR = "upstreamPush"
    }
}
