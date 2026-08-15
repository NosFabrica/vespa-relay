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
import com.nosfabrica.vespa.relay.router.config.MonitorConfig
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.SyncUpstream
import com.nosfabrica.vespa.relay.router.discovery.AliasFolding
import com.nosfabrica.vespa.relay.router.discovery.AliasMonitor
import com.nosfabrica.vespa.relay.router.discovery.AliasProbe
import com.nosfabrica.vespa.relay.router.discovery.ConsistencyPass
import com.nosfabrica.vespa.relay.router.discovery.FitnessPass
import com.nosfabrica.vespa.relay.router.discovery.ReachabilityProbe
import com.nosfabrica.vespa.relay.router.discovery.RelayAliasRecord
import com.nosfabrica.vespa.relay.router.discovery.RelayAliases
import com.nosfabrica.vespa.relay.router.discovery.RelayConsistency
import com.nosfabrica.vespa.relay.router.discovery.RelaySockets
import com.nosfabrica.vespa.relay.router.discovery.StreamWorld
import com.nosfabrica.vespa.relay.router.heal.HealQueue
import com.nosfabrica.vespa.relay.router.heal.Healer
import com.nosfabrica.vespa.relay.router.heal.WriteCapability
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.Processors
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.router.progress.SyncProgress
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.router.refused.RouterRefusalSink
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
    // The ids twice refused by the store, so a reconcile stops asking for
    // them, and the queue of repairs to hand back to the relays serving them.
    // Disabled by default: the filter answers no to everything and records
    // nothing until SYNC_REFUSED_DIR is set.
    private val refusedIds: RefusedIds = RefusedIds.disabled(),
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
    // The newest stored version of each (kind, author) address, so a stale
    // replaceable is dropped before it is verified. Same reason as knownIds:
    // the query is the store's, the pipeline takes a function.
    newestVersions: (suspend (Int, List<String>) -> Map<String, Version>)? = null,
    // SYNC_PROGRESS_FILE: what each stream is doing, and the disposition of
    // every url its current cycle took on, written where the relay can publish
    // it. Unset writes nothing — see [SyncProgress].
    private val progressFile: SyncProgress = SyncProgress(null),
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

    // One stream BUILDS its id set at a time, static and dynamic both: the set
    // is a full store walk and concurrent ones sum on the heap.
    //
    // It used to be held for a whole run, which was the same thing while a
    // dynamic fan-out ended in a join. It cannot be now: `DynamicSync` is a
    // rotation with no join, so "the whole run" is forever and every other
    // id-set stream would queue behind it for the life of the process. What
    // bounds RESIDENCY on that side is `SharedIdSet`, which never lets a stream
    // hold more than the set in use plus one still being read by a straggler.
    private val streamGate = Semaphore(1)

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val dynamicStreams = config.dynamicStreams()

    /**
     * The fork's arithmetic: a dynamic stream whose relay list comes ENTIRELY
     * from the monitor's verdicts (a syncable source, no parsed sources) is a
     * visit-mode stream; one still parsing relay lists keeps the legacy pass
     * machinery. Both at once — `syncableRelays` beside a `relaySource` — is
     * the union path through the legacy engine, for the deployment mid-crossing.
     */
    private val visitStreams = dynamicStreams.filter { it.dynamic?.syncable != null && it.dynamic.sources.isEmpty() }
    private val legacyStreams = dynamicStreams - visitStreams.toSet()

    // The relays we hold a live subscription on; a dynamic sync must not drop
    // one of these sockets out from under its tail.
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    private val phases = StreamPhases()
    private val paging = PagingProgress()

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
    private val ingest = IngestPipeline(store, config, audit, servingPressure, scope, knownIds, newestVersions, refusals)
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
    private val backfill = StaticBackfill(client, store, config, bands, ingest, phases, paging, pager, streamGate, transferring, scope, healer, refusedIds)

    /** The `monitor { concurrency }` knob, applied to every pass that dials — see [MonitorConfig.concurrency]. */
    private val monitorConcurrency = config.monitor?.concurrency ?: MonitorConfig.DEFAULT_CONCURRENCY

    /**
     * The duplicate-url fold, built only when there is a signer — the verdict
     * it produces is a signed NIP-66 record, so a router with no identity has
     * nowhere to put one and dials every url as its own relay, exactly as
     * before this existed.
     *
     * ONE instance, shared by both halves, because [RelayAliases] is the
     * in-memory cache of what has been decided: give the reader and the prober
     * one each and the reader never sees this boot's verdicts without a store
     * round trip, and the prober re-probes what the reader already resolved.
     */
    private val folding =
        signer?.let {
            AliasFolding(
                aliases = RelayAliases(),
                record = RelayAliasRecord(store, it),
                // Per url, not per process: a `.onion` fingerprint that is only
                // given the clearnet handshake budget times out while its
                // circuit is still being built, and comes back as an empty
                // window — which folds nothing, clears nothing, and leaves every
                // url on that host in the fan-out forever. See [probeIdleMs].
                probe =
                    AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { url ->
                        probeIdleMs(url, tor, config.connectionTimeoutSec * 1000L)
                    },
                concurrency = monitorConcurrency,
                progress = processors.of(FOLD_PROCESSOR),
            )
        }

    /**
     * The stability gate: does a relay answer one filter the same way twice?
     *
     * Built on the same terms as the fold — signer or nothing, since the verdict
     * is a signed NIP-66 record — and sharing its [RelayConsistency] between the
     * reader and the prober for the same reason [RelayAliases] is shared.
     *
     * A separate probe instance from the fold's, because they walk to different
     * depths for different reasons and one is not a cache of the other.
     */
    private val consistency = RelayConsistency()
    private val stability =
        signer?.let {
            ConsistencyPass(
                consistency = consistency,
                record = RelayAliasRecord(store, it),
                probe =
                    AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { url ->
                        probeIdleMs(url, tor, config.connectionTimeoutSec * 1000L)
                    },
                concurrency = monitorConcurrency,
                progress = processors.of(STABILITY_PROCESSOR),
            )
        }

    /**
     * The dialling half of both, on their own schedule so a probe pass never
     * stands between a stream finishing discovery and starting its download.
     *
     * Order matters and is not alphabetical: the fold runs first so that a
     * stability pass measures survivors rather than urls about to be folded away
     * — measuring a duplicate twice and then deleting it is the one ordering
     * that pays for work it throws away.
     */
    private val sockets = RelaySockets(client, pinnedUrls)
    private val probe = ReachabilityProbe(tor, monitor)

    /**
     * What the probe passes measure. Built here rather than reached for through
     * [dynamic]: the monitor is one of that object's constructor arguments, so
     * asking it for the world is a cycle Kotlin can only be talked out of.
     */
    private val world = StreamWorld(store, dynamicStreams, probe, ingest, monitor, tor, sockets, monitorSources = config.monitor)

    /**
     * The verdict the sync plane selects on — `"#s": ["syncable"]` — plus the
     * measured facts a visit reads back. Third in the pass order on purpose:
     * fitness turns the fold's and the consistency pass's standing verdicts
     * into refusals without re-dialling, so both must have run first or a
     * to-be-folded url earns a dial the fold is about to make pointless.
     */
    private val fitness =
        signer?.let { s ->
            FitnessPass(
                record = RelayAliasRecord(store, s),
                probe =
                    AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { url ->
                        probeIdleMs(url, tor, config.connectionTimeoutSec * 1000L)
                    },
                client = client,
                foldedAway = { urls -> folding?.apply(urls)?.aliases ?: emptyMap() },
                unstable = { urls -> stability?.apply(urls)?.toSet() ?: emptySet() },
                progress = processors.of(FITNESS_PROCESSOR),
                concurrency = monitorConcurrency,
            )
        }

    private val aliasMonitor: AliasMonitor? =
        listOfNotNull<AliasMonitor.Pass>(
            // Each wrapper carries the same handle its pass writes into, so the
            // monitor's clock — when the pass ran, how long it took, when the
            // next one is due — lands on the row the pass is filling in. See
            // [AliasMonitor.Pass.progress].
            folding?.let { f ->
                object : AliasMonitor.Pass {
                    override val progress = f.progress

                    override suspend fun measure(
                        label: String,
                        candidates: List<NormalizedRelayUrl>,
                        canDial: suspend (NormalizedRelayUrl) -> Boolean,
                        onEvent: suspend (Event) -> Unit,
                        sockets: AliasFolding.Sockets,
                    ): Int = f.measure(label, candidates, canDial, onEvent, sockets)
                }
            },
            stability?.let { g ->
                object : AliasMonitor.Pass {
                    override val progress = g.progress

                    override suspend fun measure(
                        label: String,
                        candidates: List<NormalizedRelayUrl>,
                        canDial: suspend (NormalizedRelayUrl) -> Boolean,
                        onEvent: suspend (Event) -> Unit,
                        sockets: AliasFolding.Sockets,
                    ): Int = g.measure(label, candidates, canDial, onEvent, sockets)
                }
            },
            fitness?.let { f ->
                object : AliasMonitor.Pass {
                    override val progress = f.progress

                    override suspend fun measure(
                        label: String,
                        candidates: List<NormalizedRelayUrl>,
                        canDial: suspend (NormalizedRelayUrl) -> Boolean,
                        onEvent: suspend (Event) -> Unit,
                        sockets: AliasFolding.Sockets,
                    ): Int = f.measure(label, candidates, canDial, onEvent, sockets)
                }
            },
        ).takeIf { it.isNotEmpty() }
            ?.let { passes ->
                AliasMonitor(
                    passes,
                    scope,
                    // The monitor block's clock where one is configured; the
                    // historical six hours otherwise.
                    intervalMs = (config.monitor?.sweepSeconds ?: MonitorConfig.DEFAULT_SWEEP_SECONDS) * 1000L,
                    source = world,
                    // The fast lane runs FITNESS alone: a first `syncable` is
                    // what a new relay waits on; fold and consistency verdicts
                    // ride the next sweep.
                    newUrlEveryMs = config.monitor?.newUrlSeconds?.times(1000L),
                    newUrlPass =
                        fitness?.let { f ->
                            object : AliasMonitor.Pass {
                                override val progress = f.progress

                                override suspend fun measure(
                                    label: String,
                                    candidates: List<NormalizedRelayUrl>,
                                    canDial: suspend (NormalizedRelayUrl) -> Boolean,
                                    onEvent: suspend (Event) -> Unit,
                                    sockets: AliasFolding.Sockets,
                                ): Int = f.measure(label, candidates, canDial, onEvent, sockets)
                            }
                        },
                )
            }
            // WHERE THE CANDIDATE SET CAME FROM, on both passes' rows.
            //
            // Every number those passes publish is a share of `candidates`, and
            // `candidates` is itself already filtered — a url a signed NIP-66
            // record calls dead is dropped by [StreamWorld] before either pass
            // sees it. Without these two, a reader has the whole funnel except
            // its mouth, and no way to tell a corpus that shrank from one that
            // was never that large. Both rows carry them because both passes
            // measure the same derived set; a supplier rather than a copy, for
            // the reason [Processors] gives.
            ?.also {
                for (pass in listOfNotNull(folding?.progress, stability?.progress)) {
                    pass.counts {
                        listOf(
                            Processors.Count("sourced", world.lastDerivation.sourced.toLong()),
                            Processors.Count("excluded", world.lastDerivation.excluded.toLong()),
                            Processors.Count("heldOutDead", world.lastDerivation.heldOutDead.toLong()),
                        )
                    }
                }
            }

    private val dynamic: DynamicSync =
        DynamicSync(
            client,
            store,
            bands,
            ingest,
            phases,
            paging,
            streamGate,
            transferring,
            monitor,
            pinnedUrls,
            folding,
            stability,
            aliasMonitor,
            tor,
            scope,
            healer,
            refusedIds,
            sockets,
            probe,
            monitorAuthor = signer?.pubKey,
        )

    /** The rotating pool — the visit-mode streams' whole engine. Inert when none are configured. */
    private val visitPool =
        VisitPool(
            client = client,
            store = store,
            bands = bands,
            ingest = ingest,
            pager = pager,
            healer = healer,
            sockets = sockets,
            tor = tor,
            scope = scope,
            monitorAuthor = signer?.pubKey,
            streams = visitStreams,
            progress = processors.of(VISITS_PROCESSOR),
        )

    private val upPush = UpstreamPush(client, store, config.upIntervalSec, streamGate, scope)
    private val pressure = servingPressure

    fun start(): SyncEngine {
        if (downUpstreams.isEmpty() && upUpstreams.isEmpty() && dynamicStreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            return this
        }

        ingest.start()
        registerProcessors()

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

        // THE FORK IN THE ROAD. A stream running purely on the monitor's
        // verdicts rides the rotating pool — one queue, socket-owning workers,
        // tails, the audit clock. Everything else keeps the legacy pass
        // machinery, which is the migration posture: both engines run side by
        // side until every stream has crossed.
        legacyStreams.forEach { stream -> scope.launch { dynamic.loop(stream) } }
        visitPool.start()

        // Only where there is something to fold for. A dynamic stream is what
        // discovers urls off other people's relay lists; a static config names
        // its upstreams by hand and has no duplicates to find.
        if (dynamicStreams.isNotEmpty()) aliasMonitor?.start()

        // The phase report runs for the life of the engine, not inside the
        // static backfill's progress loop: a dynamic-only config has no
        // backfill loop at all, and everyone else's dynamic streams — the
        // larger half of the fill — outlive it.
        // The heartbeat is its own loop, NOT a passenger on the phase report.
        // That report is skipped when a config has neither a down upstream nor a
        // dynamic stream — a push-only router — and with the write inside it
        // `writtenAt` never advanced, so the relay reported a perfectly healthy
        // mirror as "probably not running". The whole point of this file is that
        // it ticks whatever the streams are doing, and "there are no streams to
        // report" is exactly that case.
        scope.launch {
            while (scope.isActive) {
                delay(PROGRESS_INTERVAL_MS)
                progressFile.write(phases.snapshot(), processors.snapshot(), health, fatals.get())
            }
        }
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
     * [AliasMonitor] knows its clock.
     */
    private fun registerProcessors() {
        // The two probe passes exist whenever there is a signer, and RUN only
        // where there is something to fold: a static config names its upstreams
        // by hand and has no duplicate urls to find, so `aliasMonitor.start()`
        // is never called. Said out loud, because a row left at `starting` for
        // the life of the process reads as a pass that is about to run.
        if (dynamicStreams.isEmpty()) {
            folding?.progress?.phase(Processors.OFF)
            stability?.progress?.phase(Processors.OFF)
        }
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
        // NIP-66, and the answer to "is that the same thing as the alias fold":
        // the records are, the processors are not. This one watches every socket
        // the client opens and signs what it learns about REACHABILITY; the fold
        // and the stability gate dial deliberately and write IDENTITY and
        // USABILITY tags. All three land on the same addressable kind-30166
        // record per url, which is why a verdict panel shows them together and
        // why they must not write one at the same moment.
        monitor?.let { m ->
            processors.of(REACHABILITY_PROCESSOR).let { p ->
                p.phase(Processors.WATCHING)
                p.counts {
                    listOf(
                        // Relays it has an observation for at all — the set it
                        // could publish about.
                        Processors.Count(
                            "observed",
                            m.observer
                                .all()
                                .size
                                .toLong(),
                        ),
                        // …and the ones a current unreachability record takes
                        // out of every fan-out until the TTL lapses. This is the
                        // number that makes a stream's `knownDead` outcome
                        // explicable rather than mysterious.
                        Processors.Count("knownDead", m.deadSet().size.toLong()),
                    )
                }
            }
        }
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
                ingest.submit(event, up.trusted, IngestOrigin(up.url, up.healContent, up.healRetractions))
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
                    socketCeiling = MAX_CONCURRENT_SOCKETS,
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
                    ", ${transferring.get()} relay(s) transferring" +
                    ", $open connected" +
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
                "router: ingested ${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                    ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${upPush.pushed.get()} up" else "") +
                    // A dynamic cycle connects relays that are in no upstream
                    // list, so the connected count is reported against the
                    // pinned ones rather than as a fraction of them.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (dynamicStreams.isNotEmpty()) " + dynamic" else "") +
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
            "router: stopped (${ingest.accepted.get()} accepted, ${ingest.rejected.get()} rejected" +
                ingest.rejectionBreakdown() + ingest.suppressionBreakdown() +
                ", ${upPush.pushed.get()} pushed)",
        )
    }

    companion object {
        /**
         * The names the progress document calls this router's non-stream jobs.
         *
         * Spelled out as constants for the reason `StreamPhases.word` gives:
         * they are PUBLISHED, and a reader charting them must not have a row
         * renamed by a Kotlin refactor.
         */
        const val FOLD_PROCESSOR = "aliasFold"

        // `consistency`, not `stability`: the class is `ConsistencyPass`, the
        // state is `RelayConsistency` and the published tag is
        // `self-consistent`. A fourth word for the same measurement is a word
        // nobody can grep from the document back to the code.
        const val STABILITY_PROCESSOR = "consistency"

        /**
         * The fitness pass — the verdict funnel the sync plane selects on.
         * Its counts are the verdicts themselves, not the candidate funnel the
         * fold and consistency rows carry: those two share a derivation, this
         * one reports what it decided.
         */
        const val FITNESS_PROCESSOR = "fitness"

        /** The rotating pool — roster, tails, audits, visits. See [VisitPool]. */
        const val VISITS_PROCESSOR = "visits"
        const val REACHABILITY_PROCESSOR = "reachability"
        const val INGEST_PROCESSOR = "ingest"
        const val HEAL_PROCESSOR = "heal"
        const val PUSH_PROCESSOR = "upstreamPush"

        private const val MAX_CONCURRENT_SOCKETS = 1024
        private const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20

        /** How long a shutdown will wait on the monitor's last write before giving up. */
        private const val SHUTDOWN_FLUSH_MS = 5_000L
    }
}
