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
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
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
 * The monitor plane: three passes over one candidate set, on their own clock. The fold decides
 * which urls are one server, consistency which cannot answer twice, and fitness grades what
 * survives and signs the NIP-66 record per url that the mirror's roster selects on.
 */
class MonitorEngine(
    private val store: IEventStore,
    /** The `monitor { }` block. Null is a deployment with no monitor. */
    private val settings: MonitorConfig?,
    /**
     * The relay lists this plane scans, from the monitor's own config. Every url they name becomes
     * a signed public claim, so the set is declared here and never inferred from a stream.
     */
    private val sources: RelayDiscoveryConfig?,
    /** The dial budget a probe starts with, before Tor's own slack. */
    private val connectionTimeoutMs: Long,
    private val peers: PeerClient,
    /** The identity every verdict is signed under. Null is a deployment with no monitor. */
    private val signer: NostrSigner?,
    /** This plane's own report. */
    private val processors: Processors = Processors(),
    /** The socket refcount shared with the mirror, so a pass never closes a socket a stream is on. */
    private val sockets: RelaySockets,
    /** Where an event a probe dial happened to see goes; the mirror decides who wanted it. */
    private val onProbeEvent: suspend (Event) -> Unit,
    /** The relays the mirror holds live; never dialled twice, never closed by a pass. */
    private val pinnedUrls: Set<NormalizedRelayUrl>,
    private val scope: CoroutineScope,
) {
    private val client = peers.client
    private val tor = peers.tor

    /** Decided once for the start gate and the `off` rows, so they cannot disagree. */
    private val hasSources = sources != null

    private val monitorConcurrency = settings?.dialConcurrency ?: MonitorConfig.DEFAULT_DIAL_CONCURRENCY

    /** How often the fast lane looks, or null for a lane that is off. */
    private val fastLaneSeconds = fastLaneSecondsFor(settings)

    /**
     * The derivation's row. Declared above the passes it feeds: [Processors.of] registers in
     * call order and the document draws in that order.
     */
    private val sourceProgress = signer?.let { processors.of(SOURCE_PROCESSOR) }

    /** The duplicate-url fold. One instance for the reader and the prober. */
    private val folding =
        signer?.let {
            AliasFolding(
                aliases = RelayAliases(),
                record = RelayVerdictRecord(store, it),
                // The idle budget is per url: a `.onion` on the clearnet budget times out mid-circuit.
                probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency = monitorConcurrency,
                progress = processors.of(FOLD_PROCESSOR),
                // Hidden services gate on Tor's own socket budget, not the clearnet permits.
                tor = tor,
            )
        }

    /** The stability gate. Its probe is separate from the fold's; they walk to different depths. */
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
            probeIdleMs(url, tor, connectionTimeoutMs)
        }

    private val probe = ReachabilityProbe(tor)

    /** What the passes measure. */
    private val world =
        StreamWorld(
            store,
            sources,
            probe,
            // Our signer plus every monitor the verdict sources and gates name. A source with no
            // `authors` contributes nothing: an unscoped `dead` would starve a relay out for good.
            monitorAuthors =
                (
                    listOfNotNull(signer?.pubKey) +
                        (sources?.let { it.sources + it.gatedBy }.orEmpty())
                            .flatMap { it.filter.authors.orEmpty() }
                ).distinct(),
            self = signer?.pubKey,
            tor = tor,
            sockets = sockets,
            onProbeEvent = onProbeEvent,
            progress = sourceProgress,
        )

    /**
     * The grade the sync plane selects on plus the facts a visit reads back. Third in pass order:
     * it turns the fold's and the consistency pass's standing verdicts into refusals without re-dialling.
     */
    private val fitness =
        signer?.let { s ->
            FitnessPass(
                record = RelayVerdictRecord(store, s),
                probe = probeOver(FitnessPass.FITNESS_TARGET),
                client = client,
                // `aliases` only: this pass signs `l=alias` for every entry, and a stand-in was never measured.
                foldedAway = { urls -> folding?.applyVerdicts(urls)?.aliases ?: emptyMap() },
                inconsistent = { urls -> consistencyPass?.applyVerdicts(urls)?.toSet() ?: emptySet() },
                progress = processors.of(FITNESS_PROCESSOR),
                // The per-url transport, so a `.onion` document is fetched inside the circuit.
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
                    intervalMs = (settings?.sweepSeconds ?: MonitorConfig.DEFAULT_SWEEP_SECONDS) * 1000L,
                    source = world,
                    // Stability then fitness, so a first `prime` waits on the stability answer; the fold
                    // needs a host's whole group and rides the sweep.
                    fastLaneEveryMs = fastLaneSeconds?.times(1000L),
                    fastLanePasses = listOfNotNull(stabilityEntry, fitnessEntry),
                )
            }
            // The derivation's numbers on every row that shares it, as suppliers rather than copies.
            ?.also {
                sourceProgress?.counts {
                    // Nothing until a walk has run: zeros would read as a measurement.
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
                            Processors.Count("recordedOnly", world.lastDerivation.recordedOnly.toLong()),
                        )
                    }
                }
            }

    /**
     * Which urls this router has folded away, and onto what, so the roster never builds a leg
     * for another url's relay. Stand-ins are included: this caller only decides which socket to open.
     */
    suspend fun foldedAway(urls: List<NormalizedRelayUrl>): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folding?.applyVerdicts(urls)?.let { it.aliases + it.standIns } ?: emptyMap()

    /** How many candidate urls a current `dead` verdict of ours holds out of the passes. */
    fun heldOutDead(): Int = world.lastDerivation.heldOutDead

    /** Runs the passes if there is anything for them to work on; returns whether they were started. */
    fun start(): Boolean {
        // Runs regardless of the gate, off the boot path: the passes wait for it and the mirror does not.
        val retired = scope.async { retireOwnStaleVerdicts() }
        if (!hasSources) {
            // Every row, fitness included: one left at `starting` reads as a pass about to run.
            sourceProgress?.phase(Processors.OFF)
            folding?.progress?.phase(Processors.OFF)
            consistencyPass?.progress?.phase(Processors.OFF)
            fitness?.progress?.phase(Processors.OFF)
            return false
        }
        System.err.println("router: monitor passes gated at ${DialGate.over(monitorConcurrency, tor).describe()}")
        // The passes sign on what they read, so they start after the retraction.
        scope.launch {
            retired.await()
            aliasMonitor?.start()
        }
        return true
    }

    /**
     * Retires the verdicts this router signed that it would no longer sign: stale epochs and
     * legacy `s` grades. A paged walk of the whole graded corpus, so it runs off the boot path.
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

    /** The [StoreCalls] element of [scope] alone; its `Job` and dispatcher decide other things. */
    private val booked: CoroutineContext
        get() = scope.coroutineContext[StoreCalls] ?: EmptyCoroutineContext

    companion object {
        /**
         * How often the fast lane looks, or null for a lane that is off. No `monitor` block takes
         * the default; `fastLaneSeconds = 0` inside a block is the off switch and survives.
         */
        internal fun fastLaneSecondsFor(settings: MonitorConfig?): Long? = settings?.let { it.fastLaneSeconds } ?: MonitorConfig.DEFAULT_FAST_LANE_SECONDS.takeIf { settings == null }

        /** The published names of the monitor's jobs; a rename changes a chart. */
        const val FOLD_PROCESSOR = "aliasFold"

        const val SOURCE_PROCESSOR = "aliasSource"

        // `consistency`, the word the class, the state and the published tag share.
        const val STABILITY_PROCESSOR = "consistency"

        const val FITNESS_PROCESSOR = "fitness"
    }

    /** This plane's own status document, over its own [Processors]. */
    fun status(
        everySeconds: Long,
        relayUrl: String?,
    ): MonitorStatus = MonitorStatus(processors, everySeconds, relayUrl)
}
