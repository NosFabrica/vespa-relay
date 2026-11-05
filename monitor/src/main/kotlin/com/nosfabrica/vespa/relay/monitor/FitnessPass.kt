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

import com.nosfabrica.vespa.relay.fold.FoldReading
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.fold.ServerIndex
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.coroutineScope

/**
 * The fitness certificate: one measured verdict per url, written as a NIP-32 label a stream can
 * select on. `prime` is a composite: reachable, answering, canonical, consistent, pageable,
 * compliant and readable by us. NIP-11 never decides anything; it is published beside the verdict.
 */
class FitnessPass(
    private val record: RelayVerdictRecord,
    /** The small-target ladder; [FITNESS_TARGET] events say "answers and pages" well enough. */
    private val probe: AliasProbe,
    private val client: INostrClient,
    /** The fold's standing verdicts over these candidates, off the batch's one read; never earned here. */
    private val foldedAway: (RelayVerdictRecord.Verdicts, List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl> =
        { held, urls -> FoldReading.foldsAmong(held, urls) },
    /** The consistency pass's standing refusals; same bargain. */
    private val inconsistent: (RelayVerdictRecord.Verdicts, List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl> =
        { held, urls -> urls.filterTo(HashSet()) { it in held.inconsistent } },
    /** The urls the monitor knows, so a fast-lane batch sees the other urls on each candidate's server. */
    private val world: () -> ServerIndex = { ServerIndex.EMPTY },
    /** The bars a relay's answer is held to. */
    private val compliance: RelayCompliance = RelayCompliance(),
    override val progress: Processors.Handle,
    /** The relay's NIP-11 document. Null publishes the same verdicts with fewer facts. */
    private val document: RelayDocument? = null,
    /** The proxy, where there is one; the `n` tag must name what carried the measurement. */
    private val tor: TorTransport? = null,
    /** The monitor's one dial gate, shared with every other pass. */
    private val gate: DialGate = DialGate(AliasFolding.DEFAULT_DIAL_CONCURRENCY),
    /** The wall clock on one verdict write. */
    private val publishDeadlineMs: Long = WriteLimits.PUBLISH_DEADLINE_MS,
    /** The wall clock on the NEG-OPEN. */
    private val nip77DeadlineMs: Long = NIP77_DEADLINE_MS,
    /** How much wall time one batch may lose to a store that is not answering. */
    private val publishWedgeBudgetMs: Long = WriteLimits.PUBLISH_WEDGE_BUDGET_MS,
    /** Verdict writes in flight at once. */
    private val writeConcurrency: Int = WriteLimits.WRITE_CONCURRENCY,
    /** A direct handshake to a url whose dial failed TLS, on the pre-probe's route; the default checks nothing. */
    private val tlsCheck: suspend (NormalizedRelayUrl) -> TlsAnswer = { TlsAnswer.UNCHECKED },
    /** The NEG-OPEN, the only thing this pass asks [client] for. */
    private val reconcile: suspend (NormalizedRelayUrl, Filter) -> Unit = { url, sliver ->
        client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
    },
    /** One sweep's period: how long a deferred url backs off, and the unit a deferring `prime` ages in. */
    private val sweepMs: Long = AliasMonitor.DEFAULT_INTERVAL_MS,
    /** How often the scheduler replays [flushPending], which the log lines promise. */
    private val retryMs: Long = AliasMonitor.DEFAULT_RETRY_MS,
    /** The pre-probe: whether this process can reach a url at all, and whose side a refusal is on. */
    private val reach: suspend (NormalizedRelayUrl) -> Reach = { Reach.REACHABLE },
    /** The socket refcount; without one every dial leaves a websocket behind. */
    private val sockets: Sockets = Sockets.NONE,
) : AliasMonitor.Pass {
    private val dials = FitnessDial(probe, compliance, progress, document, tor, tlsCheck, reconcile, nip77DeadlineMs, reach, sockets)

    private val writes = FitnessWrites(record, progress, tor, WriteLimits(writeConcurrency, publishDeadlineMs, wedgeBudgetMs = publishWedgeBudgetMs))

    private val backoff = DeferralBackoff(sweepMs)

    private val reports = FitnessReport(progress, writes, nip77DeadlineMs, retryMs)

    /** Of [urls], those still backing off from a deferral; the fast lane leaves them out before any pass dials. */
    fun backingOff(urls: Collection<NormalizedRelayUrl>): Set<NormalizedRelayUrl> = backoff.backingOff(urls)

    /**
     * Replay the writes the store did not take, under the same limits as a batch's own, dropping any
     * the record has since overtaken. Returns how many landed. Never runs beside [measure].
     */
    override suspend fun flushPending(): Int = writes.flush()

    /** Measure [candidates] and write a verdict for each. Returns how many events the dials were served. */
    override suspend fun measure(
        kind: RunKind,
        candidates: List<NormalizedRelayUrl>,
    ): Int {
        if (candidates.isEmpty()) return 0
        val label = kind.label
        val sweep = kind == RunKind.SWEEP
        if (sweep) {
            // A url the sweep no longer names is not ours to grade; its queued write would land as a claim about it.
            val named = candidates.toHashSet()
            writes.retainOnly(named)
            backoff.retainOnly(named)
        }
        progress.begin("measuring fitness")
        val startedMs = System.currentTimeMillis()
        try {
            // The other urls on each candidate's server, read with it: a fast-lane batch holds only what is new.
            val mates = world().matesOf(candidates)
            // After the passes before this one wrote; without the standing folds an alias would be graded as its own relay.
            val held = readStanding("fitness", label) { record.load(candidates + mates) } ?: return 0
            val free = freeRefusals(held, candidates, foldedAway, inconsistent)
            val graded = candidates.filter { it !in free }
            // A url the fold has not decided waits behind its server's stand-in: grading it too would
            // put one server on the roster once per path.
            val heldBack = FoldReading.heldAmong(held, graded, mates)
            val backingOff = backingOff(graded.filter { it !in heldBack })
            val toDial = graded.filter { it !in heldBack && it !in backingOff }
            // Only on a server some url of which answered HTTP last time: one down there is down at every path.
            val answering = answeringServers(held)
            val toCheck = heldBack.keys.filterTo(HashSet()) { RelayAliases.authorityOf(it.url) in answering }
            // The free refusals are outside this count: they cost no socket.
            progress.measuring(toDial.size + toCheck.size, Processors.UNIT_URL)
            // A week back, so "events above the anchor" can only mean "ignored the cursor".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            val slots = (toDial + toCheck).map { DialSlot(it, held = it in toCheck) }
            // The documents are joined here, after the dials gave their permits back.
            coroutineScope {
                val documents = this
                dialEach(
                    slots.map { it.url },
                    gate,
                    probe::deadlineMs,
                    progress,
                    // A held url writes nothing when cut, so it is outside the batch guard's count.
                    cut = { i, _ -> if (!slots[i].held) slots[i].cut() },
                    before = { i, url -> slots[i].held || dials.preProbe(url, slots[i]) },
                ) { i, url ->
                    val slot = slots[i]
                    if (slot.held) dials.checkHeld(url, anchor, slot) else dials.measureOne(url, anchor, slot, documents)
                }
            }
            val batch = FitnessBatch(free, slots, backingOff)
            // Before the guard: a refused batch still heard each relay ask us to come back later.
            backoff.backOff(batch)

            if (refusesToPublish(label, toDial, batch)) {
                reports.report(label, candidates.size, startedMs, batch, emptyMap(), written = null)
                return batch.downloaded
            }
            val stale = if (sweep) backoff.staleWhileDeferring(held, batch.deferring()) else emptySet()
            val trusted = batch.earned - withheldTlsDead(label, toDial, batch.earned, tor)
            val spoken = trusted - withheldUnspoken(label, toDial, trusted, batch)
            // After the guards, so a held url inherits only the proof that survived them.
            val outcomes = spoken + inheritedSocketProof(heldBack, spoken)
            // A held path graded here no longer waits; every other one still does.
            val waiting = heldBack.filterKeys { it !in outcomes }
            val written = writes.write(label, held, waiting, stale, outcomes, batch.readings)
            if (sweep) {
                // Over a sweep only: a fast-lane batch is a handful of urls, not the roster.
                val prime =
                    candidates.filterTo(HashSet()) { url ->
                        outcomes[url]?.let { it.verdict == Verdict.PRIME } ?: (url !in written.withdrawn && held.fitness[url]?.value == Verdict.PRIME.value)
                    }
                progress.sharedHosts(PrimeCensus.of(prime, held))
            }
            reports.report(label, candidates.size, startedMs, batch, outcomes, written)
            return batch.downloaded
        } finally {
            progress.finish()
        }
    }

    /**
     * The servers, by [RelayAliases.authorityOf], with a url whose standing grade says the server
     * answered HTTP: it spoke NIP-01, or refused the websocket upgrade in so many words.
     */
    private fun answeringServers(held: RelayVerdictRecord.Verdicts): Set<String> =
        held.fitness.entries
            .filter { (_, grade) -> grade.value in ANSWERED_GRADES || (grade.value == Verdict.DEAD.value && grade.evidence?.startsWith(Silence.UPGRADE.reason) == true) }
            .mapTo(HashSet()) { RelayAliases.authorityOf(it.key.url) }

    companion object {
        /** NIP-66's network values this monitor can honestly write. */
        const val NETWORK_CLEARNET = "clearnet"

        const val NETWORK_TOR = "tor"

        /** The `noncompliant` grade's evidence, one sentence whichever page decided it. */
        const val NONCOMPLIANT_EVIDENCE = "answered with events the filter did not ask for"

        /** Events per fitness ask; the pass dials the whole corpus, so the target is its cost. */
        const val FITNESS_TARGET = 20

        /** The share of a url's budget the second page may spend, scaling with the transport. */
        const val COMPLIANCE_BUDGET_DIVISOR = 4

        /** The NEG-OPEN sliver; enough to prove the verb. */
        const val NIP77_WINDOW_SECONDS = 3600L

        /** The NEG-OPEN's idle window, shorter than a transfer's. */
        const val NIP77_IDLE_MS = 10_000L

        /**
         * The wall clock over the NEG-OPEN. Each round re-arms the idle window, so a relay that
         * keeps answering slowly never trips [NIP77_IDLE_MS].
         */
        const val NIP77_DEADLINE_MS = 3 * NIP77_IDLE_MS

        /** A held leg's `stage`, in the pass's own words. */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_LADDER = "ask ladder"

        /** The one ask a url waiting on the fold gets. */
        const val STAGE_HELD = "held for the fold"

        /** Grades a server earns only by answering a REQ or refusing one, whatever else they say. */
        private val ANSWERED_GRADES =
            setOf(Verdict.PRIME, Verdict.RESTRICTED, Verdict.AUTH_REFUSED, Verdict.NONCOMPLIANT, Verdict.UNPAGEABLE).mapTo(HashSet()) { it.value }

        const val STAGE_NIP77 = "neg-open"

        /** Our own handshake, checking a dial's TLS failure. */
        const val STAGE_TLS_CHECK = "tls check"

        /** The one ask below the ladder's window, a REQ and not a rung of it. */
        const val STAGE_COMPLIANCE = "second page"

        /** The record edit that takes a waiting url's `prime` back. */
        const val STAGE_WITHDRAW = "prime withdrawn"

        /** The count of urls a batch held back for the fold, on the fitness row. */
        const val WAITING_ON_FOLD = "waitingOnFold"

        /** Earned writes the store has not taken yet, on the fitness row. */
        const val PENDING_WRITES = "pendingWrites"

        /** The row's phase while it replays them. */
        const val RETRYING_WRITES = "retrying writes"

        const val MAX_ABANDONED_NAMED = 32

        /** Sweeps a url may defer in a row, or periods its `prime` may age while deferring, before it is withdrawn. */
        const val DEFERRED_SWEEPS = 2

        /** The longest wait a relay's own `Retry-After` may buy it, past the sweep's own backoff. */
        const val MAX_RETRY_AFTER_MS = 24L * 60 * 60 * 1000

        /** The share of a batch's dials that may come back with nothing before the whole pass is refused. */
        const val GUARD_SHARE = 0.25

        /** The batch size below which the guard does not apply. */
        const val GUARD_FLOOR = 50

        /**
         * The share of a batch's direct wss dials whose TLS failure our own handshake may confirm before
         * those `dead`s are withheld; real certificate faults are a small minority of relays.
         */
        const val TLS_GUARD_SHARE = 0.25

        /** The share of a batch's dials the pre-probe may prove gone before that proof is doubted. */
        const val DEAD_GUARD_SHARE = 0.75
    }
}
