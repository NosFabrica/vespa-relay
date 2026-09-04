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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.config.MonitorConfig
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.probeIdleMs
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * The monitor plane: what is out there, and how much of it can be used.
 *
 * Three passes over one candidate set, on their own clock. The fold decides
 * which urls are one server wearing several addresses; the consistency pass
 * decides which cannot answer the same question twice; the fitness pass
 * grades what survives and signs a NIP-66 kind-30166 record for each. Those
 * records outlive this process and are what the mirror's roster selects on.
 *
 * The mirror reads verdicts from the store, a plain NIP-01 read. What it
 * writes here is [ingest] (a probe dial's events feed the fast lane's relay
 * lists), [sockets] (one refcount over both planes) and [pinnedUrls]; cut
 * those three and this is a separate process.
 */
class MonitorEngine(
    private val store: IEventStore,
    private val config: RouterConfig,
    private val peers: PeerClient,
    /** The identity every verdict is signed under. Null is a deployment with no monitor: every pass is absent and its row says `off`. */
    private val signer: NostrSigner?,
    /** This plane's own report; a row belongs to the object that registered it. */
    private val processors: Processors = Processors(),
    /** The socket refcount shared with the mirror, so a pass releasing a url never closes a socket a stream is on. */
    private val sockets: RelaySockets,
    /** Where an event a probe dial happened to see goes. */
    private val ingest: IngestPipeline,
    /** The relays the mirror holds a live subscription on; never dialled twice, never closed by a pass. */
    private val pinnedUrls: Set<NormalizedRelayUrl>,
    private val scope: CoroutineScope,
) {
    private val client = peers.client
    private val tor = peers.tor
    private val discoveryStreams = config.discoveryStreams()

    /** Decided once for the start gate and the `off` rows, so they cannot disagree. See [hasMonitorSources]. */
    private val hasSources = hasMonitorSources(config)

    private val monitorConcurrency = config.monitor?.dialConcurrency ?: MonitorConfig.DEFAULT_DIAL_CONCURRENCY

    /** How often the fast lane looks, or null for a lane that is off. See [fastLaneSecondsFor]. */
    private val fastLaneSeconds = fastLaneSecondsFor(config)

    /**
     * The derivation's row. Declared above the passes it feeds: [Processors.of]
     * registers in call order and the document draws in that order.
     */
    private val sourceProgress = signer?.let { processors.of(SOURCE_PROCESSOR) }

    /**
     * The duplicate-url fold. One instance for the reader and the prober:
     * [RelayAliases] is the cache of what has been decided this boot.
     */
    private val folding =
        signer?.let {
            AliasFolding(
                aliases = RelayAliases(),
                record = RelayVerdictRecord(store, it),
                // The idle budget is per url: a `.onion` on the clearnet budget times out mid-circuit.
                probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency = monitorConcurrency,
                progress = processors.of(FOLD_PROCESSOR),
                // Hidden services gate on Tor's own socket budget, not the clearnet permits. See [DialGate].
                tor = tor,
            )
        }

    /**
     * The stability gate: does a relay answer one filter the same way twice?
     * Its probe is separate from the fold's; they walk to different depths.
     */
    private val consistency = RelayConsistency()
    private val consistencyPass =
        signer?.let {
            ConsistencyPass(
                consistency = consistency,
                record = RelayVerdictRecord(store, it),
                probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency = monitorConcurrency,
                progress = processors.of(STABILITY_PROCESSOR),
                tor = tor,
            )
        }

    /** The three passes' probe: same transport and timeout budget, only the target depth differs. */
    private fun probeOver(target: Int) =
        AliasProbe.over(client, target) { url ->
            probeIdleMs(url, tor, config.connectionTimeoutSec * 1000L)
        }

    private val probe = ReachabilityProbe(tor)

    /** What the passes measure. Built here, not reached through discovery, which takes this engine as an argument. */
    private val world =
        StreamWorld(
            store,
            discoveryStreams,
            probe,
            ingest,
            // Whose `dead` verdicts may hold a candidate out: our signer plus
            // every monitor the config's verdict sources and gates name. A
            // source with no `authors` contributes nothing; an unscoped `dead`
            // would starve a relay out for good. See ForeignMonitorTest.
            monitorAuthors =
                (
                    listOfNotNull(signer?.pubKey) +
                        discoveryStreams
                            .flatMap { it.discovery?.let { d -> d.sources + d.gatedBy }.orEmpty() }
                            .flatMap { it.filter.authors.orEmpty() }
                ).distinct(),
            self = signer?.pubKey,
            tor = tor,
            sockets = sockets,
            monitorConfig = config.monitor,
            progress = sourceProgress,
        )

    /**
     * The grade the sync plane selects on (`"#l": ["prime"]`) plus the facts a
     * visit reads back. Third in pass order: it turns the fold's and the
     * consistency pass's standing verdicts into refusals without re-dialling.
     */
    private val fitness =
        signer?.let { s ->
            FitnessPass(
                record = RelayVerdictRecord(store, s),
                probe = probeOver(FitnessPass.FITNESS_TARGET),
                client = client,
                // `aliases` only: this pass signs `l=alias` for every entry it is
                // handed, and a stand-in was never measured. See [AliasFolding.Collapsed.standIns].
                foldedAway = { urls -> folding?.applyVerdicts(urls)?.aliases ?: emptyMap() },
                inconsistent = { urls -> consistencyPass?.applyVerdicts(urls)?.toSet() ?: emptySet() },
                progress = processors.of(FITNESS_PROCESSOR),
                // The per-url transport the dial uses: a `.onion` document is fetched inside the circuit. See [TorTransport].
                document = RelayDocument(peers::httpFor),
                tor = tor,
                concurrency = monitorConcurrency,
            )
        }

    /** One pass as [AliasMonitor] sees it: the work, plus the row it reports on. */
    private fun entry(
        handle: Processors.Handle?,
        run: suspend (String, List<NormalizedRelayUrl>, suspend (NormalizedRelayUrl) -> Boolean, suspend (Event) -> Unit, Sockets) -> Int,
    ) = object : AliasMonitor.Pass {
        // The handle the pass writes into, so the monitor's clock lands on the row the pass fills.
        override val progress = handle

        override suspend fun measure(
            label: String,
            candidates: List<NormalizedRelayUrl>,
            canDial: suspend (NormalizedRelayUrl) -> Boolean,
            onEvent: suspend (Event) -> Unit,
            sockets: Sockets,
        ): Int = run(label, candidates, canDial, onEvent, sockets)
    }

    private val foldEntry = folding?.let { f -> entry(f.progress, f::measure) }

    private val stabilityEntry = consistencyPass?.let { g -> entry(g.progress, g::measure) }

    private val fitnessEntry = fitness?.let { f -> entry(f.progress, f::measure) }

    private val aliasMonitor: AliasMonitor? =
        listOfNotNull(foldEntry, stabilityEntry, fitnessEntry)
            .takeIf { it.isNotEmpty() }
            ?.let { passes ->
                AliasMonitor(
                    passes,
                    scope,
                    intervalMs = (config.monitor?.sweepSeconds ?: MonitorConfig.DEFAULT_SWEEP_SECONDS) * 1000L,
                    source = world,
                    // The fast lane runs stability then fitness: a first `prime`
                    // waits on the stability answer. The fold rides the sweep;
                    // it needs a host's whole group. See [AliasMonitor.fastLanePasses].
                    fastLaneEveryMs = fastLaneSeconds?.times(1000L),
                    fastLanePasses = listOfNotNull(stabilityEntry, fitnessEntry),
                )
            }
            // Where the candidate set came from, on every row that shares the
            // derivation. Suppliers rather than copies, for the reason [Processors] gives.
            ?.also {
                sourceProgress?.counts {
                    // Nothing until a walk has run: zeros would read as a measurement. See [StreamWorld.derived].
                    if (!world.derived) {
                        emptyList()
                    } else {
                        listOf(
                            Processors.Count("sourced", world.lastDerivation.sourced.toLong()),
                            Processors.Count("excluded", world.lastDerivation.excluded.toLong()),
                            Processors.Count("heldOutDead", world.lastDerivation.heldOutDead.toLong()),
                            Processors.Count("candidates", world.lastDerivation.candidates.toLong()),
                            Processors.Count("recordedOnly", world.lastDerivation.recordedOnly.toLong()),
                        )
                    }
                }
                for (pass in listOfNotNull(folding?.progress, consistencyPass?.progress)) {
                    pass.counts {
                        listOf(
                            Processors.Count("sourced", world.lastDerivation.sourced.toLong()),
                            Processors.Count("excluded", world.lastDerivation.excluded.toLong()),
                            Processors.Count("heldOutDead", world.lastDerivation.heldOutDead.toLong()),
                            // Urls we hold records about that no relay list named this round.
                            Processors.Count("recordedOnly", world.lastDerivation.recordedOnly.toLong()),
                        )
                    }
                }
            }

    /**
     * Which urls this router has folded away, and onto what; the roster asks so
     * it never builds a leg for a url that is another url's relay. Stand-ins
     * are included: this caller only decides which socket to open and publishes nothing.
     */
    suspend fun foldedAway(urls: List<NormalizedRelayUrl>): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folding?.applyVerdicts(urls)?.let { it.aliases + it.standIns } ?: emptyMap()

    /** How many candidate urls a current `dead` verdict of ours holds out of the passes. */
    fun heldOutDead(): Int = world.lastDerivation.heldOutDead

    /** Runs the passes if there is anything for them to work on; returns whether they were started. */
    fun start(): Boolean {
        // Runs regardless of the gate: verdicts already signed are still selected on.
        // Off the boot path; the passes wait for it and the mirror does not.
        val retired = scope.async { retireOwnStaleVerdicts() }
        if (!hasSources) {
            // Every row, fitness included: one left at `starting` reads as a pass about to run.
            sourceProgress?.phase(Processors.OFF)
            folding?.progress?.phase(Processors.OFF)
            consistencyPass?.progress?.phase(Processors.OFF)
            fitness?.progress?.phase(Processors.OFF)
            return false
        }
        // The clearnet number; the Tor half is capped by the Tor dispatcher's width. See [DialGate].
        System.err.println("router: monitor passes gated at ${DialGate.over(monitorConcurrency, tor).describe()}")
        // The passes sign on what they read, so they start after the retraction.
        scope.launch {
            retired.await()
            aliasMonitor?.start()
        }
        return true
    }

    /**
     * Retires the verdicts this router signed that it would no longer sign: the
     * stale epochs and the grades in the legacy `s` tag. A paged walk of the
     * whole graded corpus, so it runs as a coroutine on [scope]; [start]
     * orders the passes after it and the mirror's first roster rebuild is not held.
     */
    private suspend fun retireOwnStaleVerdicts() {
        signer?.let { s ->
            val record = RelayVerdictRecord(store, s)
            // Two guards: a throw in the first must not skip the second.
            runCatching { withContext(booked) { FitnessPass.retireStaleEpochs(store, record, s.pubKey) } }
                .onFailure { System.err.println("router: could not retire stale-epoch verdicts: ${it.message}") }
            runCatching { withContext(booked) { FitnessPass.retireLegacyGrades(store, record, s.pubKey) } }
                .onFailure { System.err.println("router: could not retire legacy `s` grades: ${it.message}") }
        }
    }

    /**
     * The [StoreCalls] element of [scope], for the two retractions; only the
     * element, since the scope's `Job` and dispatcher decide other things.
     */
    private val booked: CoroutineContext
        get() = scope.coroutineContext[StoreCalls] ?: EmptyCoroutineContext

    companion object {
        /**
         * Is there anything for the monitor to work on: a stream's own
         * `relaySource`, or the `monitor { sources }` block? A function over
         * the config so a test can hand it the pure-monitor deployment.
         */
        internal fun hasMonitorSources(config: RouterConfig): Boolean = config.discoveryStreams().isNotEmpty() || config.monitor?.sources?.isNotEmpty() == true

        /**
         * How often the fast lane looks, or null for a lane that is off. The two
         * nulls mean opposite things: no `monitor` block takes the default,
         * while `fastLaneSeconds = 0` inside a block is the off switch and survives.
         */
        internal fun fastLaneSecondsFor(config: RouterConfig): Long? = config.monitor?.let { it.fastLaneSeconds } ?: MonitorConfig.DEFAULT_FAST_LANE_SECONDS.takeIf { config.monitor == null }

        /** The names the progress document calls the monitor's jobs. Published; a rename changes a chart. */
        const val FOLD_PROCESSOR = "aliasFold"

        /** The candidate derivation, named for the log line ("router: alias source derived ...") rather than the class. */
        const val SOURCE_PROCESSOR = "aliasSource"

        // `consistency`, the word the class, the state and the published tag share.
        const val STABILITY_PROCESSOR = "consistency"

        /** Its counts are the verdicts themselves, not the candidate funnel the fold and consistency rows carry. */
        const val FITNESS_PROCESSOR = "fitness"
    }

    /** This plane's own status document, over its own [Processors]. */
    fun status(
        everySeconds: Long,
        relayUrl: String?,
    ): MonitorStatus = MonitorStatus(processors, everySeconds, relayUrl)
}
