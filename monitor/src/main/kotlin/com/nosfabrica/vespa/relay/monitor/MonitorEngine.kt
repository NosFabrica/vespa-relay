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
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.probeIdleMs
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.util.BootClock
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch

/**
 * The monitor plane: three passes over one candidate set, on their own clock. The fold decides
 * which urls are one server, consistency which cannot answer twice, and fitness grades what
 * survives and signs the NIP-66 record per url that the mirror's roster selects on.
 */
class MonitorEngine(
    private val store: IEventStore,
    /** `monitor.conf`. */
    private val settings: MonitorConfig,
    /**
     * The relay lists this plane scans, from the monitor's own config. Every url they name becomes
     * a signed public claim, so the set is declared here and never inferred from a stream.
     */
    private val sources: RelayDiscoveryConfig?,
    /** The dial budget a probe starts with, before Tor's own slack. */
    private val connectionTimeoutMs: Long,
    private val peers: PeerClient,
    /** The identity every verdict is signed under. */
    private val signer: NostrSigner,
    /** This plane's own report. */
    private val processors: Processors = Processors(),
    /** The socket refcount the passes share, so one pass never closes a socket another is on. */
    private val sockets: Sockets,
    private val scope: CoroutineScope,
    /** Where a verdict is written; [store] is opened read-only, and is only where it is read back from. */
    private val writer: StoreWriter,
) {
    private val client = peers.client
    private val tor = peers.tor

    /** Decided once for the start gate and the `off` rows, so they cannot disagree. */
    private val hasSources = sources != null

    private val sweepSeconds = settings.sweepSeconds

    /** Every pass dials through this one gate, so `dialConcurrency` bounds the plane rather than each pass. */
    private val gate = DialGate.over(settings.dialConcurrency, tor)

    /** The one record every pass and the retirement edit: only one instance can see two edits of a url overlap. */
    private val record = RelayVerdictRecord(store, signer, writer = writer)

    // The rows, in the order the document draws them: [Processors.of] registers in call order.
    private val sourceRow = processors.of(SOURCE_PROCESSOR)
    private val foldRow = processors.of(FOLD_PROCESSOR)
    private val stabilityRow = processors.of(STABILITY_PROCESSOR)
    private val fitnessRow = processors.of(FITNESS_PROCESSOR)

    /** Of [urls], those carrying a fitness grade of ours taken within the last sweep period. */
    private suspend fun recentlyGraded(urls: Collection<NormalizedRelayUrl>): Set<NormalizedRelayUrl> = StreamWorld.gradedSince(record, urls, nowSeconds() - sweepSeconds)

    /** Of [urls], those fitness is still backing off from after a deferral. */
    private fun backingOff(urls: Collection<NormalizedRelayUrl>): Set<NormalizedRelayUrl> = fitness.backingOff(urls)

    /** The three passes' probe: same transport and timeout budget, only the target depth differs. */
    private fun probeOver(target: Int) =
        AliasProbe.over(client, target, retriesTimeouts = { tor?.routes(it) == true }) { url ->
            probeIdleMs(url, tor, connectionTimeoutMs)
        }

    private val probe = ReachabilityProbe(tor, threads = settings.dialConcurrency, reuseMs = minOf(ReachabilityProbe.REUSE_MS, sweepSeconds * 1000L))

    /** What the passes measure. */
    private val world =
        StreamWorld(
            store,
            sources,
            probe,
            monitorAuthors = deadVerdictSigners(signer.pubKey, sources),
            self = signer.pubKey,
            tor = tor,
            progress = sourceRow,
            recentlyGraded = ::recentlyGraded,
            backingOff = ::backingOff,
        )

    /** The duplicate-url fold. One instance for the reader and the prober. */
    private val folding =
        AliasFolding(
            record = record,
            // The idle budget is per url: a `.onion` on the clearnet budget times out mid-circuit.
            probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
            gate = gate,
            progress = foldRow,
            world = { world.known },
            canDial = { foldDials(world.reach(it)) },
            sockets = sockets,
        )

    /** The stability gate. Its probe is separate from the fold's; they walk to different depths. */
    private val consistencyPass =
        ConsistencyPass(
            consistency = RelayConsistency(),
            record = record,
            probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
            gate = gate,
            progress = stabilityRow,
            world = { world.known },
            canDial = { gateDials(world.reach(it)) },
            sockets = sockets,
        )

    /**
     * The grade the sync plane selects on plus the facts a visit reads back. Third in pass order:
     * it turns the fold's and the consistency pass's standing verdicts into refusals without re-dialling.
     */
    private val fitness =
        FitnessPass(
            record = record,
            probe = probeOver(FitnessPass.FITNESS_TARGET),
            client = client,
            progress = fitnessRow,
            // The per-url transport, so a `.onion` document is fetched inside the circuit; one per dial the gate admits.
            document = RelayDocument(peers::httpFor, concurrency = gate.permits),
            tor = tor,
            gate = gate,
            tlsCheck = probe::tls,
            world = { world.known },
            sweepMs = sweepSeconds * 1000L,
            reach = world::reach,
            sockets = sockets,
        )

    private val aliasMonitor =
        AliasMonitor(
            listOf(folding, consistencyPass, fitness),
            scope,
            intervalMs = sweepSeconds * 1000L,
            source = world,
            // Stability then fitness, so a first `prime` waits on the stability answer; the fold
            // needs a host's whole group and rides the sweep.
            fastLaneEveryMs = settings.fastLaneSeconds?.times(1000L),
            fastLanePasses = listOf(consistencyPass, fitness),
        )

    init {
        // The derivation's numbers on every row that shares it, as suppliers rather than copies.
        sourceRow.counts { derivationCounts(withCandidates = true) }
        foldRow.counts { derivationCounts(withCandidates = false) }
        stabilityRow.counts { derivationCounts(withCandidates = false) }
    }

    /**
     * One row's share of the last derivation, read once so a sweep publishing meanwhile cannot tear
     * `sourced = excluded + heldOutDead + candidates`. Nothing before the first: zeros would read as a measurement.
     */
    private fun derivationCounts(withCandidates: Boolean): List<Processors.Count> {
        val d = world.lastDerivation ?: return emptyList()
        return listOfNotNull(
            Processors.Count("sourced", d.sourced.toLong()),
            Processors.Count("excluded", d.excluded.toLong()),
            Processors.Count("heldOutDead", d.heldOutDead.toLong()),
            Processors.Count("candidates", d.candidates.toLong()).takeIf { withCandidates },
            Processors.Count("recordedOnly", d.recordedOnly.toLong()),
        )
    }

    /** How many candidate urls a current `dead` verdict of ours holds out of the passes. */
    fun heldOutDead(): Int = world.lastDerivation?.heldOutDead ?: 0

    /** Runs the passes if there is anything for them to work on; returns whether they were started. */
    fun start(): Boolean {
        // Runs with or without sources, off the boot path; the passes wait for it.
        val retired = scope.async { retireOwnStaleVerdicts() }
        if (!hasSources) {
            // Every row, fitness included: one left at `starting` reads as a pass about to run.
            for (row in listOf(sourceRow, foldRow, stabilityRow, fitnessRow)) row.phase(Processors.OFF)
            BootClock.mark("monitor: off (no sources)")
            return false
        }
        System.err.println("router: monitor passes gated at ${gate.describe()}")
        // The passes sign on what they read, so they start after the retraction.
        BootClock.mark("monitor: retiring its own stale verdicts before the passes")
        scope.launch {
            retired.await()
            aliasMonitor.start()
            BootClock.mark("monitor: passes started")
        }
        return true
    }

    /**
     * Retires the verdicts this monitor signed that it would no longer sign: stale epochs and
     * legacy `s` grades. A paged walk of the whole graded corpus, so it runs off the boot path.
     */
    private suspend fun retireOwnStaleVerdicts() {
        // On the fitness row, since every pass waits on it and a long one otherwise reads as `starting`;
        // never on a row that is `off`, which this runs beside.
        val row = fitnessRow.takeIf { hasSources }
        row?.phase(Processors.RETIRING)
        // Two guards: a throw in the first must not skip the second.
        guarded("stale-epoch verdicts") { FitnessPass.retireStaleEpochs(store, record, signer.pubKey, row) }
        guarded("legacy `s` grades") { FitnessPass.retireLegacyGrades(store, record, signer.pubKey, row) }
        row?.clearMeasuring()
        row?.phase(Processors.STARTING)
    }

    /** One boot retraction, its failure logged and contained; a cancellation still ends the boot. */
    private suspend fun guarded(
        what: String,
        retire: suspend () -> Unit,
    ) {
        try {
            retire()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("router: could not retire $what: ${e.message}")
        }
    }

    companion object {
        /**
         * Whose `dead` verdicts hold a url out: our key plus every monitor our sources name. A source
         * with no `authors` adds nothing, since an unscoped `dead` would starve a relay out for good.
         */
        fun deadVerdictSigners(
            self: String?,
            sources: RelayDiscoveryConfig?,
        ): List<String> = (listOfNotNull(self) + sources?.sources.orEmpty().flatMap { it.filter.authors.orEmpty() }).distinct()

        /** What the fold dials: a host that took a connect, since it says nothing about one that did not. */
        internal fun foldDials(reach: Reach): Boolean = reach == Reach.REACHABLE

        /** What the stability gate walks: an unanswered host too, since fitness's longer dial may grade it `prime`. */
        internal fun gateDials(reach: Reach): Boolean = reach == Reach.REACHABLE || reach == Reach.UNANSWERED

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
        storeCalls: StoreCalls? = null,
    ): MonitorStatus = MonitorStatus(processors, everySeconds, relayUrl, storeCalls)
}
