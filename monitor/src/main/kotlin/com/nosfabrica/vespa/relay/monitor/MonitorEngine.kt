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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking

/**
 * THE MONITOR PLANE: what is out there, and how much of it can we use.
 *
 * Three passes over one candidate set, on their own clock. The fold decides
 * which urls are one server wearing several addresses; the consistency pass
 * decides which cannot answer the same question twice; the fitness pass grades
 * what survives and signs a NIP-66 kind-30166 record for each. Those records
 * outlive this process and are what the mirror's roster selects on.
 *
 * ## Why it is its own class
 *
 * It was 278 lines inside `SyncEngine`, which is how one class came to start
 * two planes and belong to neither. The two ask different questions — "is the
 * mirror keeping up" against "what is out there" — run on different clocks,
 * count in different units (an event against a relay url), and produce
 * different artifacts (rows in a store against signed records). Nothing about
 * that was visible while they shared a constructor.
 *
 * ## What it still takes from the mirror, and why that is the interesting part
 *
 * Reading the monitor is already clean: the mirror asks the STORE for verdicts,
 * a plain NIP-01 read of records this plane signed. Writing to it is not, and
 * these three arguments are the whole of it:
 *
 *  - [ingest] — a probe dial sees events, and they go into the mirror's own
 *    pipeline rather than being dropped. That is a feature: the fast lane
 *    verdicts a new relay in minutes by reading relay lists back OUT of the
 *    store, so the probe feeding it is what closes that loop.
 *  - [sockets] — the refcount that spans both planes, so a probe fingerprints a
 *    relay a stream is already transferring on without a second dial.
 *  - [pinnedUrls] and the streams' `discovery.sources` — the candidate set is
 *    derived from the mirror's configuration.
 *
 * Cut those three and this becomes a separate PROCESS. Whether that trade is
 * worth making — two containers, at the cost of duplicate dials and a second
 * ingest path — is a decision nobody has taken yet, and it does not have to be
 * taken to have the boundary.
 */
class MonitorEngine(
    private val store: IEventStore,
    private val config: RouterConfig,
    private val peers: PeerClient,
    /**
     * The identity every verdict is signed under. Null is a deployment with no
     * monitor at all: without a key there is nothing to sign a kind-30166 as,
     * so every pass here is absent rather than idle, and the rows say `off`.
     */
    private val signer: NostrSigner?,
    /**
     * THIS PLANE'S OWN report, not the mirror's.
     *
     * It used to be one shared `Processors`, so both planes' rows landed in one
     * document and the page had to split them apart again by name
     * (`splitProcessors` in the JS, which is gone with it). Each plane
     * publishes what it runs; a row belongs to the object that registered it.
     */
    private val processors: Processors = Processors(),
    /**
     * WHO IS STILL USING THIS SOCKET — shared with the mirror, not owned here.
     * A probe pass releasing a url must not close a socket a stream is still
     * transferring on, and one refcount over one pool is what makes that
     * decidable. Built on the mirror's side because the pinned set is its.
     */
    private val sockets: RelaySockets,
    /**
     * Where an event a probe dial happened to see goes — the mirror's own
     * queue, deduped and verified on its terms. See the class note.
     */
    private val ingest: IngestPipeline,
    /** The relays the mirror holds a live subscription on; never dialled twice, never closed by a pass. */
    private val pinnedUrls: Set<NormalizedRelayUrl>,
    /**
     * The scope the passes run on.
     *
     * The mirror's today, and named as an argument rather than built here for
     * exactly that reason: the day this plane becomes its own process it brings
     * its own, and the seam is already where it has to be. A pass sharing
     * `Dispatchers.IO` with ingest is one that queues behind whatever is
     * saturating it — which is the argument for the split, not against the
     * current arrangement.
     */
    private val scope: CoroutineScope,
) {
    private val client = peers.client
    private val tor = peers.tor
    private val discoveryStreams = config.discoveryStreams()

    /**
     * IS THERE ANYTHING FOR THE MONITOR TO WORK ON — the one question both the
     * start gate and the `off` rows are decided by.
     *
     * It was `discoveryStreams.isNotEmpty()` alone, written when a stream's own
     * `relaySource` was the only way a url could enter the system. The
     * `monitor { sources }` block is the other way, and [MonitorConfig]
     * documents the posture it exists for: a deployment moves every ounce of
     * relay-list parsing off the streams, which then run on verdict queries
     * alone. Take that all the way — streams with static `urls` and one monitor
     * block, the pure-monitor deployment — and `discoveryStreams` is EMPTY
     * while the block names three sources. `aliasMonitor.start()` was never
     * called: no fold, no stability gate, no fitness, no `prime` ever signed,
     * and four rows on the monitor card reading `off` for the life of the
     * process. The urls were derived correctly by [StreamWorld], which unions
     * both, and then nothing ran over them.
     *
     * Decided ONCE and read from both places, because the failure mode of them
     * disagreeing is silent in one direction: rows marked `off` under a monitor
     * that is running. The rule itself is [hasMonitorSources], out where a test
     * can put a config in front of it — building a whole engine to ask a
     * question about a config file is how a gate goes untested.
     */
    private val hasSources = hasMonitorSources(config)

    /** The `monitor { concurrency }` knob, applied to every pass that dials — see [MonitorConfig.concurrency]. */
    private val monitorConcurrency = config.monitor?.dialConcurrency ?: MonitorConfig.DEFAULT_DIAL_CONCURRENCY

    /** How often the fast lane looks, or null for a lane that is off — see [fastLaneSecondsFor]. */
    private val fastLaneSeconds = fastLaneSecondsFor(config)

    /**
     * THE ROW THE DERIVATION REPORTS ON, and it is declared HERE, above the
     * passes it feeds, on purpose: [Processors.of] registers in call order and
     * the document is drawn in registration order, so a handle taken after the
     * fold's would draw the collection step under the pass that waits on it.
     *
     * On the same terms as the passes — a signer or nothing. It is not that the
     * derivation needs an identity, but that without one there is no
     * [aliasMonitor] to run it, and a row for work this deployment never does
     * would draw a `Relay monitor` card on a router that has no monitor.
     */
    private val sourceProgress = signer?.let { processors.of(SOURCE_PROCESSOR) }

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
                record = RelayVerdictRecord(store, it),
                // Per url, not per process: a `.onion` fingerprint that is only
                // given the clearnet handshake budget times out while its
                // circuit is still being built, and comes back as an empty
                // window — which folds nothing, clears nothing, and leaves every
                // url on that host in the fan-out forever. See [probeIdleMs].
                probe = probeOver(RelayAliases.DEFAULT_PROBE_TARGET),
                concurrency = monitorConcurrency,
                progress = processors.of(FOLD_PROCESSOR),
                // The gate, not the dial: a hidden service waits on Tor's own
                // socket budget instead of on the clearnet fan-out's permits.
                // See [DialGate] for what the shared one cost.
                tor = tor,
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

    /**
     * One spelling of the probe constructor for the three passes — the fold,
     * the stability gate, the fitness pass. Same transport and the same
     * timeout budget; only the target depth differs.
     */
    private fun probeOver(target: Int) =
        AliasProbe.over(client, target) { url ->
            probeIdleMs(url, tor, config.connectionTimeoutSec * 1000L)
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
    private val probe = ReachabilityProbe(tor)

    /**
     * What the probe passes measure. Built here rather than reached for through
     * [discovery]: the monitor is one of that object's constructor arguments, so
     * asking it for the world is a cycle Kotlin can only be talked out of.
     */
    private val world =
        StreamWorld(
            store,
            discoveryStreams,
            probe,
            ingest,
            // Whose `dead` verdicts may hold a candidate out: our own
            // signer, plus every monitor npub the config's verdict sources
            // and certified gates name — the operator's trust statements.
            //
            // DELIBERATELY NOT the roster's rule. A source that names no
            // `authors` reads verdicts unscoped, because admitting is a
            // positive claim that still has to survive a dial. Holding out is
            // the opposite: unscoped, one `dead` grade from anybody starves
            // a relay out of the candidate set for good — never dialled, never
            // re-measured, so the mark never clears. So an unscoped source
            // contributes nothing here, and the set stays the identities the
            // operator actually vouched for. See ForeignMonitorTest.
            monitorAuthors =
                (
                    listOfNotNull(signer?.pubKey) +
                        discoveryStreams
                            .flatMap { it.discovery?.let { d -> d.sources + d.gatedBy }.orEmpty() }
                            .flatMap { it.filter.authors.orEmpty() }
                ).distinct(),
            // …and OURS alone, for the one count that is about the size of this
            // router's own corpus rather than about whose word it takes.
            self = signer?.pubKey,
            tor = tor,
            sockets = sockets,
            monitorConfig = config.monitor,
            progress = sourceProgress,
        )

    /**
     * The grade the sync plane selects on — `"#l": ["prime"]` — plus the
     * measured facts a visit reads back. Third in the pass order on purpose:
     * fitness turns the fold's and the consistency pass's standing verdicts
     * into refusals without re-dialling, so both must have run first or a
     * to-be-folded url earns a dial the fold is about to make pointless.
     */
    private val fitness =
        signer?.let { s ->
            FitnessPass(
                record = RelayVerdictRecord(store, s),
                probe = probeOver(FitnessPass.FITNESS_TARGET),
                client = client,
                // `aliases` ALONE, deliberately: this pass SIGNS a record for
                // every entry it is handed — `l=alias`, `folds onto <url>`, on
                // a tag `publishFitness` owns and therefore replaces. Only the
                // half a probe actually measured may be published that way, so
                // a stand-in elected for an absent survivor must not arrive
                // here. See [AliasFolding.Collapsed.standIns].
                foldedAway = { urls -> folding?.applyVerdicts(urls)?.aliases ?: emptyMap() },
                inconsistent = { urls -> consistencyPass?.applyVerdicts(urls)?.toSet() ?: emptySet() },
                progress = processors.of(FITNESS_PROCESSOR),
                // THE SAME PER-URL TRANSPORT THE DIAL USES. A `.onion`
                // document has to be fetched inside the circuit, and handing
                // this the direct client would both fail and put a hidden
                // service through the local resolver — see [TorTransport].
                document = RelayDocument(peers::httpFor),
                // What the `n` tag names AND what the gate is sized from.
                tor = tor,
                concurrency = monitorConcurrency,
            )
        }

    /**
     * One pass as [AliasMonitor] sees it: the work, plus the row it reports on.
     *
     * A named builder rather than an anonymous object per use, because the
     * stability gate and fitness each appear in TWO lists now — the sweep and
     * the fast lane — and two wrappers over one pass are two places for the
     * handle to drift from the object writing to it.
     */
    private fun entry(
        handle: Processors.Handle?,
        run: suspend (String, List<NormalizedRelayUrl>, suspend (NormalizedRelayUrl) -> Boolean, suspend (Event) -> Unit, Sockets) -> Int,
    ) = object : AliasMonitor.Pass {
        // The same handle the pass itself writes into, so the monitor's clock —
        // when the pass ran, how long it took, when the next one is due — lands
        // on the row the pass is filling in. See [AliasMonitor.Pass.progress].
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
                    // The monitor block's clock where one is configured; the
                    // historical six hours otherwise.
                    intervalMs = (config.monitor?.sweepSeconds ?: MonitorConfig.DEFAULT_SWEEP_SECONDS) * 1000L,
                    source = world,
                    // The fast lane runs THE STABILITY GATE AND THEN FITNESS:
                    // a first `prime` is what a new relay waits on, and it must
                    // not be handed one before anything has asked whether the
                    // relay answers the same question twice. The FOLD still
                    // rides the sweep — it needs a host's whole group, and a
                    // since-bound set holds only what was named in the last
                    // tick. See [AliasMonitor.fastLanePasses].
                    // DEFAULTED LIKE ITS NEIGHBOURS, and it was the only one
                    // that was not — but the two nulls here mean OPPOSITE
                    // things and collapsing them with `?:` would be worse than
                    // the bug.
                    //
                    // No `monitor` block at all is a deployment discovering
                    // through stream `relaySource` blocks alone — a shape
                    // [MonitorGateTest] exists to keep working — and there this
                    // read carried the null all the way out and turned the lane
                    // OFF, so a new relay waited a full `sweepSeconds` for its
                    // first `prime` on exactly the configs least likely to
                    // notice one missing. `sweepSeconds` and `dialConcurrency`
                    // both take their default on that path.
                    //
                    // A null INSIDE a block is the operator writing
                    // `fastLaneSeconds = 0`, the documented off switch. That
                    // one has to survive: a fallback that read it as "unset"
                    // would restart a lane somebody turned off by hand.
                    fastLaneEveryMs = fastLaneSeconds?.times(1000L),
                    fastLanePasses = listOfNotNull(stabilityEntry, fitnessEntry),
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
                // THE DERIVATION'S OWN ROW, which is the same four numbers plus
                // the one the passes below cannot state: what it handed them.
                // On the row that produced them rather than only on the rows
                // that consume them — a reader watching the collection step run
                // is asking how big the corpus turned out to be, and every
                // other number on this card is a share of that answer.
                sourceProgress?.counts {
                    // NOTHING UNTIL A WALK HAS RUN. These five are the row's
                    // whole fact line, and a boot that published them as zeros
                    // would say `0 url(s) named` for the two minutes before the
                    // first sweep — a measurement nobody has taken, and one a
                    // reader cannot tell from a store with no relay lists in
                    // it. See [StreamWorld.derived].
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
                            // The corpus BESIDE the derivation: urls we hold
                            // records about that no relay list named this round.
                            // Without it the card's mouth is one walk's yield
                            // and calls itself everything this router knows of.
                            Processors.Count("recordedOnly", world.lastDerivation.recordedOnly.toLong()),
                        )
                    }
                }
            }

    /**
     * WHICH URLS THIS ROUTER HAS FOLDED AWAY, and onto what — the mirror's
     * roster asks so it never builds a leg for a url that is another url's
     * relay.
     *
     * The one thing the sync plane reads from this object rather than from the
     * store. Everything else it takes — the grades, `speaksNegentropy` — is a
     * plain NIP-01 read of records signed here, which is why that direction
     * would survive a process boundary untouched and this one would not.
     *
     * BOTH maps, where the fitness pass above is handed `aliases` alone, and
     * the difference is the whole of `Collapsed.standIns`: this caller only
     * decides which socket to open, and nothing here publishes anything. A
     * stand-in is not evidence — but it does keep a group whose survivor went
     * missing from being dialled once per member.
     */
    suspend fun foldedAway(urls: List<NormalizedRelayUrl>): Map<NormalizedRelayUrl, NormalizedRelayUrl> = folding?.applyVerdicts(urls)?.let { it.aliases + it.standIns } ?: emptyMap()

    /** How many candidate urls a current `dead` verdict of ours holds out of the passes — for the health line. */
    fun heldOutDead(): Int = world.lastDerivation.heldOutDead

    /**
     * Run the passes, if there is anything for them to work on.
     *
     * Returns whether they were started, so the caller does not have to ask
     * [hasSources] a second time and cannot get a different answer.
     */
    fun start(): Boolean {
        // Runs whatever the sources say, and BEFORE the gate below: a
        // deployment that has stopped discovering still holds verdicts it
        // signed, and the mirror still selects on them.
        retireOwnStaleVerdicts()
        if (!hasSources) {
            // Said out loud, in the document, because a row left at `starting`
            // for the life of the process reads as a pass that is about to run.
            // The derivation with them: `aliasMonitor.start()` is what runs it.
            sourceProgress?.phase(Processors.OFF)
            folding?.progress?.phase(Processors.OFF)
            consistencyPass?.progress?.phase(Processors.OFF)
            // …AND FITNESS, which was missing from a list whose whole job is
            // that no row is left reading as a pass about to run. It is the one
            // that only ever sets its phase from INSIDE `measure`, so on a
            // deployment with no sources it never set one at all — the row sat
            // at `starting` for the life of the process, which is the exact
            // state these three lines exist to prevent, on the row an operator
            // checks first because `prime` is what the streams select on.
            fitness?.progress?.phase(Processors.OFF)
            return false
        }
        // WHAT THE PASSES ARE BOUNDED BY, said once and only when they really
        // run. `dialConcurrency` is the CLEARNET number: the Tor half is capped
        // at the Tor dispatcher's own width, so an operator who raises the knob
        // to buy onion throughput can see from this line that it did not move,
        // and reach for `SYNC_TOR_MAX_SOCKETS` instead. See [DialGate].
        System.err.println("router: monitor passes gated at ${DialGate.over(monitorConcurrency, tor).describe()}")
        aliasMonitor?.start()
        return true
    }

    // BEFORE any pass reads a verdict and before the roster's first
    // rebuild, which is why it blocks: the reads downstream ask only
    // whether a url holds a verdict, so a record standing under rules this
    // build no longer applies would be acted on as current. See
    // [FitnessPass.retireStaleEpochs] for why the retraction belongs here
    // rather than in every reader.
    //
    // Costs a PAGED walk of our own graded records on every boot — the
    // epoch and the legacy tag are both decided per record rather than in
    // the filter, so neither retraction can ask the store to return only
    // what it wants — and on the boot after an epoch bump it costs a signed
    // edit per standing verdict too. Paid once, at a deploy the operator
    // chose, in exchange for never serving on a verdict we would not
    // re-take. (This said "one indexed query returning nothing"; the query
    // is indexed and it returns the whole graded corpus.)
    //
    // TWO GUARDS, NOT ONE. Sharing a `runCatching` meant a throw in the
    // first retraction silently skipped the second — and reported it under
    // the first one's name, so a store that could not answer the epoch walk
    // left every legacy `s` grade standing with nothing said about it.

    /**
     * Retire the verdicts THIS ROUTER signed that it would no longer sign.
     *
     * On the monitor's side of the split because they are the monitor's own
     * records: it wrote them, its epoch decides which are stale, and its grade
     * vocabulary decides which are in a tag that now means something else. It
     * ran in `SyncEngine.start()` when there was one engine, which put a
     * boot-blocking store walk on the mirror's critical path for the monitor's
     * bookkeeping.
     */
    private fun retireOwnStaleVerdicts() {
        signer?.let { s ->
            val record = RelayVerdictRecord(store, s)
            runCatching { runBlocking { FitnessPass.retireStaleEpochs(store, record, s.pubKey) } }
                .onFailure { System.err.println("router: could not retire stale-epoch verdicts: ${it.message}") }
            // …and the grades written before the move off `s`, which are not
            // stale readings but readings in a tag that now means something
            // else entirely. Same boot, same reason it cannot be left to the
            // readers, and now its own failure to report.
            runCatching { runBlocking { FitnessPass.retireLegacyGrades(store, record, s.pubKey) } }
                .onFailure { System.err.println("router: could not retire legacy `s` grades: ${it.message}") }
        }
    }

    companion object {
        /**
         * Is there anything for the monitor to work on — a stream's own
         * `relaySource`, or the `monitor { sources }` block?
         *
         * A function over the config rather than a property of the engine so a
         * test can hand it the deployment that broke: streams on static `urls`
         * with every url entering through the monitor block, which is the
         * posture [MonitorConfig] documents and the one the old rule
         * (`discoveryStreams.isNotEmpty()`) answered `false` for.
         */
        internal fun hasMonitorSources(config: RouterConfig): Boolean = config.discoveryStreams().isNotEmpty() || config.monitor?.sources?.isNotEmpty() == true

        /**
         * How often the fast lane looks, or null for a lane that is off.
         *
         * **The two ways of reading null here mean opposite things**, which is
         * why this is a named function rather than one `?.` in the constructor
         * — where it was, and where it was wrong.
         *
         * NO `monitor` BLOCK is a deployment discovering through stream
         * `relaySource` blocks alone, a shape [hasMonitorSources] exists to
         * keep working. `config.monitor?.fastLaneSeconds` is null there, and
         * carrying that null out turned the lane OFF — so a new relay waited a
         * full `sweepSeconds` for its first `prime` on exactly the configs
         * least likely to notice the lane was missing. `sweepSeconds` and
         * `dialConcurrency` both take their documented default on that path;
         * this now does too.
         *
         * A null INSIDE a block is the operator writing `fastLaneSeconds = 0`,
         * the documented off switch — see [MonitorConfig.fastLaneSeconds] and
         * the loader that maps 0 to null. That one has to survive, so a plain
         * `?: DEFAULT_FAST_LANE_SECONDS` would be a worse bug than the one it
         * fixes: it would restart a lane somebody turned off by hand.
         */
        internal fun fastLaneSecondsFor(config: RouterConfig): Long? = config.monitor?.let { it.fastLaneSeconds } ?: MonitorConfig.DEFAULT_FAST_LANE_SECONDS.takeIf { config.monitor == null }

        /**
         * The names the progress document calls the monitor's jobs.
         *
         * Spelled out as constants for the reason `StreamPhases.word` gives:
         * they are PUBLISHED, and a reader charting them must not have a row
         * renamed by a Kotlin refactor. Here rather than on `SyncEngine`
         * because these rows are this plane's — the mirror never writes one.
         */
        const val FOLD_PROCESSOR = "aliasFold"

        /**
         * The candidate derivation — `StreamWorld`, which the router's own log
         * line has always called the alias source ("router: alias source
         * derived 16,752 url(s)"). Named for that line rather than for the
         * class, so the document, the log and the code are one word.
         */
        const val SOURCE_PROCESSOR = "aliasSource"

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
    }

    /**
     * This plane's own status document, over its own [Processors].
     *
     * Handed out rather than built by the caller because the rows are private
     * to this object — which is the property that makes the page survive the
     * process split unchanged: whatever composes the two engines today, the
     * monitor's page reads the monitor's own report.
     */
    fun status(
        everySeconds: Long,
        relayUrl: String?,
    ): MonitorStatus = MonitorStatus(processors, everySeconds, relayUrl)
}
