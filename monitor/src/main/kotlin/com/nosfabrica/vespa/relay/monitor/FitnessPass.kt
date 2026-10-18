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

import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

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
        { held, urls -> AliasFolding.foldsAmong(held, urls) },
    /** The consistency pass's standing refusals; same bargain. */
    private val inconsistent: (RelayVerdictRecord.Verdicts, List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl> =
        { held, urls -> urls.filterTo(HashSet()) { it in held.inconsistent } },
    /** The urls the monitor knows, so a fast-lane batch sees the other urls on each candidate's server. */
    private val world: () -> ServerIndex = { ServerIndex.EMPTY },
    /** The bars a relay's answer is held to. */
    private val compliance: RelayCompliance = RelayCompliance(),
    val progress: Processors.Handle,
    /** The relay's NIP-11 document. Null publishes the same verdicts with fewer facts. */
    private val document: RelayDocument? = null,
    /** The proxy, where there is one; the `n` tag must name what carried the measurement. */
    private val tor: TorTransport? = null,
    private val concurrency: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
    /** The wall clock on one verdict write. */
    private val publishDeadlineMs: Long = VerdictWrites.PUBLISH_DEADLINE_MS,
    /** The wall clock on the NEG-OPEN. */
    private val nip77DeadlineMs: Long = NIP77_DEADLINE_MS,
    /** How much wall time one batch may lose to a store that is not answering. */
    private val publishWedgeBudgetMs: Long = VerdictWrites.PUBLISH_WEDGE_BUDGET_MS,
    /** Verdict writes in flight at once. */
    private val writeConcurrency: Int = VerdictWrites.WRITE_CONCURRENCY,
    /** A direct handshake to a url whose dial failed TLS, on the pre-probe's route; the default checks nothing. */
    private val tlsCheck: suspend (NormalizedRelayUrl) -> TlsAnswer = { TlsAnswer.UNCHECKED },
    /** The NEG-OPEN, the only thing this pass asks [client] for. */
    private val reconcile: suspend (NormalizedRelayUrl, Filter) -> Unit = { url, sliver ->
        client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
    },
) {
    /** One gate for every pass this component runs. */
    private val gate = DialGate.over(concurrency, tor)

    /**
     * The url each batch's write loop stopped on, or no entry when it wrote everything. Keyed by
     * label because the sweep and the fast lane share this object.
     */
    private val writeCursors = ConcurrentHashMap<String, String>()

    /** One write the store did not take: when it was earned, the grade it signs (null takes one back), and the write. */
    private class Pending(
        val earnedAt: Long,
        val grade: String?,
        val write: suspend () -> Boolean,
    )

    /**
     * Writes a batch earned and the store did not take, per url, replayed by [flushPending] rather
     * than dropped until the next sweep. A later batch's decision about the url replaces it.
     */
    private val pending = ConcurrentHashMap<NormalizedRelayUrl, Pending>()

    /**
     * Replay the writes the store did not take, under the same limits as a batch's own, dropping any
     * the record has since overtaken. Returns how many landed. The caller serialises this with [measure].
     */
    suspend fun flushPending(): Int {
        if (pending.isEmpty()) return 0
        val queued = HashMap(pending)
        progress.phase(RETRYING_WRITES)
        try {
            val held = readBeforeReplay(queued.keys) ?: return 0
            val overtaken = queued.filter { (url, write) -> overtaken(url, write, held) }
            for ((url, write) in overtaken) pending.remove(url, write)
            val urls = (queued.keys - overtaken.keys).sortedBy { it.url }
            progress.measuring(urls.size, Processors.UNIT_VERDICT)
            val tally =
                writeEach(urls, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, VerdictWrites.STAGE_PUBLISH) { url ->
                    queued.getValue(url).write()
                }
            // Removed only if still the write that landed: a batch may have queued a newer one meanwhile.
            for (url in tally.stored) pending.remove(url, queued.getValue(url))
            System.err.println(
                "router: fitness — ${tally.stored.size} of ${urls.size} pending write(s) landed" +
                    (if (overtaken.isEmpty()) "" else ", ${overtaken.size} dropped as overtaken by the record") +
                    (tally.stoppedBy?.let { "; the rest wait again because $it" }.orEmpty()),
            )
            return tally.stored.size
        } finally {
            progress.clearMeasuring()
            progress.phase(Processors.IDLE)
        }
    }

    /**
     * The record over the queued urls, under the write deadline: the caller holds the pass gate, so a
     * read that never answers would stop every sweep and lane behind it. Null leaves the queue as it is.
     */
    private suspend fun readBeforeReplay(urls: Set<NormalizedRelayUrl>): RelayVerdictRecord.Verdicts? {
        val why =
            try {
                withTimeoutOrNull(publishDeadlineMs) { record.load(urls) }?.let { return it }
                "the read outlived the ${fmtDuration(publishDeadlineMs)} write deadline"
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                "the read failed (${e.javaClass.simpleName})"
            }
        System.err.println("router: fitness [$RETRYING_WRITES] — could not read the record before replaying ${urls.size} pending write(s): $why; they stay queued for the next retry")
        return null
    }

    /**
     * Has the record moved past this queued write: a grade measured after it, or, for a `prime`, a
     * stability refusal or a fold the roster would otherwise be overruled on.
     */
    private fun overtaken(
        url: NormalizedRelayUrl,
        write: Pending,
        held: RelayVerdictRecord.Verdicts,
    ): Boolean {
        val standing = held.fitness[url]?.measuredAt
        if (standing != null && standing > write.earnedAt) return true
        return write.grade == Verdict.PRIME.value && (url in held.inconsistent || url in held.aliases)
    }

    /** The NEG-OPEN's answer, boxed so `withTimeoutOrNull` can tell null from the clock firing. */
    private class Reconciled(
        val fact: Pair<Boolean, String>?,
    )

    /** The `compliant` fact for one reading, or null where the bars do not support one. */
    private fun factOf(reading: AliasProbe.Compliance): Pair<Boolean, String>? =
        when (compliance.decide(reading)) {
            RelayCompliance.Verdict.UNMEASURABLE -> null
            RelayCompliance.Verdict.NONCOMPLIANT -> false to compliance.evidence(reading)
            RelayCompliance.Verdict.COMPLIANT -> true to compliance.evidence(reading)
        }

    /** Why a url was set aside with no verdict. */
    private enum class Deferral(
        val why: String,
        /** A server answered, which is evidence our own network works. */
        val serverAnswered: Boolean,
        /** As readily our own side failing as the relay's, so it counts as a blind dial, once per host. */
        val blind: Boolean = false,
    ) {
        SERVER_ERROR("the server answered the upgrade with a 5xx", serverAnswered = true),
        TOR_DOWN("our Tor proxy is not answering", serverAnswered = false),
        LOOKUP_UNEXPLAINED("the lookup failed with no reason left to read", serverAnswered = false, blind = true),
        OUR_TLS("the dial failed TLS where our direct handshake succeeded, so our transport did", serverAnswered = false),
        TLS_UNCHECKED("the dial failed TLS and no direct handshake could check it", serverAnswered = false),
        RATE_LIMITED("the relay rate limited us", serverAnswered = true),
        UNRECOGNISED("the dial gave up in words we do not recognise", serverAnswered = false, blind = true),
        PARTLY_REFUSED("it refused some shapes and went quiet on the rest", serverAnswered = true),
    }

    /** What the second page came back with, boxed for [Reconciled]'s reason. */
    private class Paged(
        val window: AliasProbe.Compliance?,
    )

    /** One url's outcome, carrying the measured facts that ride the same record edit. */
    private class Outcome(
        val verdict: Verdict,
        val evidence: String,
        val pageable: Pair<Boolean, String>? = null,
        val nip77: Pair<Boolean, String>? = null,
        /** Did the events match the filter that asked for them. */
        val compliant: Pair<Boolean, String>? = null,
        /** NIP-66's `rtt-read`: the first page of the rung that answered, never the whole walk. */
        val rttReadMs: Long? = null,
        /** The relay refused our NIP-42 key. Only ever true: a clean read is no evidence of `!auth`. */
        val authRequired: Boolean? = null,
        /**
         * Did this pass dial for this outcome. False for the two free refusals, which must not be
         * re-stamped.
         */
        val tested: Boolean = true,
        /** Earned at the pre-probe from our side of the network, before any server said a word. */
        val preProbe: Boolean = false,
        /** A failed TLS handshake, confirmed by our own; a stale truststore or clock fails both. */
        val tls: Boolean = false,
    ) {
        /** A server on the far side answered something: the evidence our own network was working. */
        val reachedServer: Boolean get() = tested && !preProbe && !tls && verdict != Verdict.SILENT
    }

    /** What one batch learned, url by url, while its dials run concurrently. */
    private class Batch {
        val outcomes = ConcurrentHashMap<NormalizedRelayUrl, Outcome>()

        /** Beside the outcomes, not on them: the document and the REQ fail independently. */
        val readings = ConcurrentHashMap<NormalizedRelayUrl, RelayDocument.Reading>()

        /** Events the dials handed to ingest. */
        val downloaded = AtomicInteger()

        /** Urls this pass gave up on rather than measured; [abandoned] names a bounded few. */
        val abandonedCount = AtomicInteger()

        val abandoned: MutableSet<String> = ConcurrentHashMap.newKeySet()

        /** Urls the per-url deadline cut after the dial had earned a verdict. These publish. */
        val cutLate = AtomicInteger()

        /** Urls whose NEG-OPEN was cut by its own wall clock; the grade is already earned. */
        val negOpenCut = AtomicInteger()

        /** Urls whose second page was cut by its own; these are graded on one page. */
        val secondPageCut = AtomicInteger()

        /** Urls that end with no `pageable` claim at all. */
        val pageUnproven = AtomicInteger()

        /** Urls this pass asked and got no answer of any kind about. Never published. */
        val unmeasured = ConcurrentHashMap<NormalizedRelayUrl, String>()

        /** Urls set aside for a reason that is neither a verdict nor our dialling failing. Never published. */
        val deferred = ConcurrentHashMap<NormalizedRelayUrl, Deferral>()

        /** Urls set aside for [why]. */
        fun deferredFor(why: Deferral): Set<NormalizedRelayUrl> = deferred.filterValues { it == why }.keys

        /** The hosts of [urls]: one host's failure repeats across all of its paths, so it counts once. */
        fun hostsOf(urls: Collection<NormalizedRelayUrl>): Int = urls.mapTo(HashSet()) { runCatching { URI(it.url).host }.getOrNull() ?: it.url }.size

        /** The hosts of every blind deferral, the share of the batch guard they count for. */
        fun blindHosts(): Int = hostsOf(deferred.filterValues { it.blind }.keys)

        /** Every deferral that is not blind, by why. */
        fun deferredCounts(): Map<String, Int> =
            deferred.values
                .filter { !it.blind }
                .groupingBy { it.why }
                .eachCount()

        /** The per-url deadline fired on [url]. */
        fun cut(url: NormalizedRelayUrl) {
            if (outcomes.containsKey(url)) {
                // Cut late, and the verdict stands: our clock firing one step later does not un-tell it.
                cutLate.incrementAndGet()
            } else if (!deferred.containsKey(url)) {
                // No verdict is written: our timeout is not a fact about the relay.
                if (abandoned.size < MAX_ABANDONED_NAMED) abandoned += url.url
                abandonedCount.incrementAndGet()
            }
        }
    }

    /** How a batch's writes went, for its report. */
    private class Written(
        val tally: VerdictWrites,
        /** Inherited verdicts left standing because the record already carries them. */
        val skipped: Int,
        val waiting: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        val withdrawn: Set<NormalizedRelayUrl>,
        val withdrawalsPending: Int,
    )

    /**
     * Measure [candidates] and write a verdict for each. Returns how many events the dials
     * delivered to [onEvent].
     */
    suspend fun measure(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        reach: suspend (NormalizedRelayUrl) -> Reach,
        onEvent: suspend (Event) -> Unit,
        sockets: Sockets,
    ): Int {
        if (candidates.isEmpty()) return 0
        // A url the sweep no longer names is not ours to grade; its queued write would land as a claim about it.
        if (label != AliasMonitor.FAST_LANE) pending.keys.retainAll(candidates.toHashSet())
        progress.begin("measuring fitness")
        val startedMs = System.currentTimeMillis()
        val batch = Batch()
        try {
            // The other urls on each candidate's server, read with it: a fast-lane batch holds only what is new.
            val mates = world().matesOf(candidates)
            // After the passes before this one wrote; without the standing folds an alias would be graded as its own relay.
            val held = readStanding("fitness", label) { record.load(candidates + mates) } ?: return 0
            val graded = gradeFreeRefusals(held, candidates, batch.outcomes)
            // A url the fold has not decided waits behind its server's stand-in: grading it too would
            // put one server on the roster once per path.
            val waiting = AliasFolding.heldAmong(held, graded, mates)
            val toDial = graded.filter { it !in waiting }
            // The free refusals are outside this count: they cost no socket.
            progress.measuring(toDial.size, Processors.UNIT_URL)
            // A week back, so "events above the anchor" can only mean "ignored the cursor".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            dialEach(toDial, gate, probe::deadlineMs, progress, cut = batch::cut) { url ->
                measureOne(url, anchor, reach, sockets, batch, onEvent)
            }

            if (refusesToPublish(label, toDial, batch)) {
                report(label, candidates.size, startedMs, batch, written = null)
                return batch.downloaded.get()
            }
            withholdTlsDead(label, toDial, batch)
            withholdUnspoken(label, toDial, batch)
            val written = write(label, held, waiting, batch)
            if (label != AliasMonitor.FAST_LANE) {
                // Over a sweep only: a fast-lane batch is a handful of urls, not the roster.
                val prime =
                    candidates.filterTo(HashSet()) { url ->
                        batch.outcomes[url]?.let { it.verdict == Verdict.PRIME } ?: (url !in written.withdrawn && held.fitness[url]?.value == Verdict.PRIME.value)
                    }
                progress.sharedHosts(PrimeCensus.of(prime, held))
            }
            report(label, candidates.size, startedMs, batch, written)
        } finally {
            progress.finish()
        }
        return batch.downloaded.get()
    }

    /** Grades, at no dial, the urls other passes' standing verdicts already refuse, and returns the rest. */
    private fun gradeFreeRefusals(
        held: RelayVerdictRecord.Verdicts,
        candidates: List<NormalizedRelayUrl>,
        outcomes: MutableMap<NormalizedRelayUrl, Outcome>,
    ): List<NormalizedRelayUrl> {
        val folded = foldedAway(held, candidates)
        for ((alias, canonical) in folded) {
            outcomes[alias] = Outcome(Verdict.ALIAS, "folds onto ${canonical.url}", tested = false)
        }
        val remaining = candidates.filter { it !in folded }
        val shaky = inconsistent(held, remaining)
        for (url in shaky) {
            outcomes[url] = Outcome(Verdict.INCONSISTENT, "failed the reproducibility bar; see the consistency tag", tested = false)
        }
        return remaining.filter { it !in shaky }
    }

    /**
     * The batch guard: when our own dialling breaks it breaks for every url at once, so nothing is
     * published, the clean-looking verdicts included. True when it refused, having said so.
     */
    private fun refusesToPublish(
        label: String,
        toDial: List<NormalizedRelayUrl>,
        batch: Batch,
    ): Boolean {
        // The share is over the dials that went out: a url set aside before any server could answer is outside it.
        val blindHosts = batch.blindHosts()
        val wentOut = toDial.size - batch.deferred.values.count { !it.serverAnswered } + blindHosts
        val blind = batch.unmeasured.size + batch.abandonedCount.get() + blindHosts
        if (wentOut < GUARD_FLOOR || blind <= wentOut * GUARD_SHARE) return false
        System.err.println(
            "router: fitness [$label] — REFUSING TO PUBLISH: $blind of $wentOut dial(s) came back with no " +
                "answer at all (${(100.0 * blind / wentOut).toInt()}%, over the ${(100 * GUARD_SHARE).toInt()}% " +
                "guard). A network does not go dark in one pass — this router could not dial. " +
                "${batch.outcomes.size} verdict(s) dropped unwritten; every url is measured again next pass.",
        )
        return true
    }

    /** The TLS guard: certificates do not fail together, but our truststore or clock fails them all. */
    private fun withholdTlsDead(
        label: String,
        toDial: List<NormalizedRelayUrl>,
        batch: Batch,
    ) {
        val outcomes = batch.outcomes
        val tlsDead = toDial.filter { outcomes[it]?.tls == true }
        val tlsDialled = toDial.count { it.url.startsWith("wss://", ignoreCase = true) && tor?.routes(it) != true && outcomes[it]?.preProbe != true }
        if (tlsDead.size <= tlsDialled * TLS_GUARD_SHARE) return
        System.err.println(
            "router: fitness [$label] — WITHHOLDING ${tlsDead.size} TLS `dead` verdict(s): our own direct handshake " +
                "failed too on ${tlsDead.size} of $tlsDialled wss dial(s), over the ${(100 * TLS_GUARD_SHARE).toInt()}% guard. " +
                "That many certificates do not fail at once — this is more likely our truststore or clock. " +
                "They are measured again next pass.",
        )
        for (url in tlsDead) outcomes.remove(url)
    }

    /**
     * The dark-network guard: a failure is believed only beside proof that our network reaches
     * others, so a batch where too few did withholds every verdict no server spoke for.
     */
    private fun withholdUnspoken(
        label: String,
        toDial: List<NormalizedRelayUrl>,
        batch: Batch,
    ) {
        val outcomes = batch.outcomes
        val provedGone = toDial.count { outcomes[it]?.preProbe == true }
        val reached = toDial.count { outcomes[it]?.reachedServer == true } + batch.deferred.values.count { it.serverAnswered }
        if (!looksDark(toDial.size, provedGone, reached)) return
        val unspoken = toDial.filter { url -> outcomes[url]?.let { it.tested && !it.reachedServer } == true }
        if (unspoken.isEmpty()) return
        System.err.println(
            "router: fitness [$label] — WITHHOLDING ${unspoken.size} `dead`/`silent` verdict(s): $provedGone of " +
                "${toDial.size} dial(s) failed the pre-probe and $reached reached a server. A network does not go dark " +
                "at once — this is more likely our resolver or egress. They are measured again next pass.",
        )
        for (url in unspoken) outcomes.remove(url)
    }

    /**
     * The batch's grades, several at a time and each under its own wall clock, then the withdrawals
     * its waiting urls owe. What the store does not take waits in [pending].
     */
    private suspend fun write(
        label: String,
        held: RelayVerdictRecord.Verdicts,
        waiting: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        batch: Batch,
    ): Written {
        val outcomes = batch.outcomes
        val order = writeOrder(label, outcomes.keys)
        // The position moves to the writes, which outnumber the dials: a free refusal costs no socket.
        progress.measuring(order.size, Processors.UNIT_VERDICT)
        val toWrite =
            order.filter { url ->
                val standing = inherited(url, outcomes.getValue(url), held)
                if (standing) progress.attempted()
                !standing
            }
        val measuredAt = nowSeconds()
        val writes = toWrite.associateWith { url -> gradeWrite(url, outcomes.getValue(url), batch.readings[url], measuredAt) }
        val tally =
            writeEach(toWrite, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, VerdictWrites.STAGE_PUBLISH) { url ->
                writes.getValue(url)()
            }
        tally.resumeAt?.let { writeCursors[label] = it.url }
        // A waiting url's standing `prime` is withdrawn, not replaced: whether it is its own relay is the fold's to say.
        val unprimed = waiting.keys.filter { held.fitness[it]?.value == Verdict.PRIME.value }.sortedBy { it.url }
        val withdrawals = unprimed.associateWith { url -> suspend { record.retireFitness(url) != null } }
        // A stopped batch means the store is not answering; the withdrawals wait with the unwritten grades.
        val withdrawn =
            if (unprimed.isEmpty() || tally.stoppedBy != null) {
                emptySet()
            } else {
                progress.measuring(unprimed.size, Processors.UNIT_VERDICT)
                writeEach(unprimed, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, STAGE_WITHDRAW) { url ->
                    withdrawals.getValue(url)()
                }.stored
            }
        // This batch's decision replaces whatever waited for these urls; a url it decided nothing about keeps its write.
        for (url in outcomes.keys + waiting.keys) pending.remove(url)
        for ((url, write) in writes) if (url !in tally.stored) pending[url] = Pending(measuredAt, outcomes.getValue(url).verdict.value, write)
        for ((url, withdraw) in withdrawals) if (url !in withdrawn) pending[url] = Pending(measuredAt, null, withdraw)
        return Written(tally, order.size - toWrite.size, waiting, withdrawn, unprimed.size - withdrawn.size)
    }

    /** [urls] by url, resumed where [label]'s last batch stopped, so the wedge limits do not drop the same tail every pass. */
    private fun writeOrder(
        label: String,
        urls: Collection<NormalizedRelayUrl>,
    ): List<NormalizedRelayUrl> {
        val order = urls.sortedBy { it.url }
        val resumeFrom = writeCursors[label]?.let { c -> order.indexOfFirst { it.url >= c } } ?: 0
        // Cleared up front, so a throw mid-write cannot leave the next batch resuming at a url never reached.
        writeCursors.remove(label)
        // -1 is a cursor sorting past everything this batch holds; it wraps to the top as index 0 does.
        return if (resumeFrom <= 0) order else order.subList(resumeFrom, order.size) + order.subList(0, resumeFrom)
    }

    /**
     * An untested verdict the record already carries word for word, evidence included. Re-signing it
     * would stamp a fresh `measured-at` on what this pass did not test, which would make it immortal.
     */
    private fun inherited(
        url: NormalizedRelayUrl,
        outcome: Outcome,
        held: RelayVerdictRecord.Verdicts,
    ): Boolean = !outcome.tested && held.fitness[url]?.let { it.value == outcome.verdict.value && it.evidence == outcome.evidence } == true

    /** One url's grade as a write, stamped with when it was measured so a replay keeps that time. */
    private fun gradeWrite(
        url: NormalizedRelayUrl,
        outcome: Outcome,
        reading: RelayDocument.Reading?,
        measuredAt: Long,
    ): suspend () -> Boolean {
        val facts = factsOf(url, outcome, reading)
        return {
            record.publishFitness(
                url = url,
                status = outcome.verdict.value,
                evidence = outcome.evidence,
                pageable = outcome.pageable,
                nip77 = outcome.nip77,
                compliant = outcome.compliant,
                facts = facts,
                measuredAt = measuredAt,
            ) != null
        }
    }

    /**
     * One url's whole job: the pre-probe, then the document, then the dial. Only the dial claims
     * a socket. [Processors.Handle.holding] is called at each boundary to name the step.
     */
    private suspend fun measureOne(
        url: NormalizedRelayUrl,
        anchor: Long,
        reach: suspend (NormalizedRelayUrl) -> Reach,
        sockets: Sockets,
        batch: Batch,
        onEvent: suspend (Event) -> Unit,
    ) {
        progress.holding(url.url, STAGE_REACHABILITY)
        val reached =
            try {
                reach(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // The pre-probe throwing is this box failing to ask, not evidence about the server.
                batch.unmeasured[url] = "the reachability probe itself threw ${e.javaClass.simpleName}"
                return
            }
        when (reached) {
            Reach.REACHABLE -> {
                Unit
            }

            Reach.PROVED_UNREACHABLE -> {
                batch.outcomes[url] = Outcome(Verdict.DEAD, "no TCP answer at the pre-probe", preProbe = true)
                return
            }

            // Our proxy not answering says nothing about the relay behind it, nor about our clearnet dials.
            Reach.TRANSPORT_DOWN -> {
                if (tor?.routes(url) == true) batch.deferred[url] = Deferral.TOR_DOWN else batch.unmeasured[url] = "our own transport is not answering"
                return
            }

            Reach.UNEXPLAINED -> {
                batch.deferred[url] = Deferral.LOOKUP_UNEXPLAINED
                return
            }
        }
        progress.holding(url.url, STAGE_DOCUMENT)
        document?.read(url)?.let { batch.readings[url] = it }
        progress.holding(url.url, STAGE_LADDER)
        sockets.claim(url)
        try {
            val outcome =
                dialVerdict(url, anchor, batch) { event ->
                    batch.downloaded.incrementAndGet()
                    onEvent(event)
                }
            if (outcome != null) {
                batch.outcomes[url] = outcome
            } else if (!batch.deferred.containsKey(url)) {
                batch.unmeasured.putIfAbsent(url, "no EOSE, no CLOSED and no transport reason on any rung")
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Our instrument giving up, unless the ladder already settled a verdict: a url in both
            // maps would feed the batch guard's blind share on a dial that answered.
            if (!batch.outcomes.containsKey(url) && !batch.deferred.containsKey(url)) {
                batch.unmeasured[url] = "the dial threw ${e.javaClass.simpleName} before the relay said anything"
            }
        } finally {
            sockets.release(url)
        }
    }

    /**
     * A refused websocket upgrade, graded by the status the server answered with: a server error
     * or a CDN's is a moment, not a claim; an auth or payment wall is a live relay that will not
     * serve us; anything else is no relay at that address.
     */
    private fun upgradeRefused(
        raw: String?,
        defer: (Deferral) -> Unit,
    ): Outcome? {
        val status = Silence.upgradeStatus(raw)
        val said = status?.let { "the websocket upgrade was refused with HTTP $it" } ?: Silence.UPGRADE.reason
        return when (status) {
            in 500..599 -> {
                defer(Deferral.SERVER_ERROR)
                null
            }

            401, 402, 403 -> {
                Outcome(Verdict.RESTRICTED, said)
            }

            else -> {
                Outcome(Verdict.DEAD, said)
            }
        }
    }

    /**
     * A dial's TLS failure, believed only when our own direct handshake fails on the server too:
     * anything between us and the relay can fail a dial's handshake, and that is not the relay's.
     */
    private suspend fun tlsFailed(
        url: NormalizedRelayUrl,
        defer: (Deferral) -> Unit,
    ): Outcome? {
        progress.holding(url.url, STAGE_TLS_CHECK)
        val answer =
            try {
                tlsCheck(url)
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                TlsAnswer.UNCHECKED
            }
        return when (answer) {
            TlsAnswer.REJECTED -> {
                Outcome(Verdict.DEAD, Silence.TLS.reason, tls = true)
            }

            TlsAnswer.ACCEPTED -> {
                defer(Deferral.OUR_TLS)
                null
            }

            TlsAnswer.UNCHECKED -> {
                defer(Deferral.TLS_UNCHECKED)
                null
            }
        }
    }

    /**
     * The dial itself: the ask ladder for "answers", the anchored events for "honours `until`",
     * one NEG-OPEN for "reconciles". Each rung's transport reason is kept so the verdict can say why.
     */
    private suspend fun dialVerdict(
        url: NormalizedRelayUrl,
        anchor: Long,
        batch: Batch,
        onEvent: suspend (Event) -> Unit,
    ): Outcome? {
        // Handed over the moment the ladder has earned it, so the per-url deadline cannot cost it.
        val settled: (Outcome) -> Unit = { batch.outcomes[url] = it }
        val unmeasured: (String) -> Unit = { batch.unmeasured[url] = it }
        val defer: (Deferral) -> Unit = { batch.deferred[url] = it }
        var lastReason: String? = null
        // The first shape that served events, else the first answered with an empty EOSE.
        var served: Pair<AliasProbe.Window, List<Int>?>? = null
        var empty: Pair<AliasProbe.Window, List<Int>?>? = null
        // Every shape the relay heard and CLOSED, with its words: a refusal is not an empty answer.
        val refusals = ArrayList<String>()
        var silent = 0
        // A shape refused for our key: chorus.bonsai.com gates DMs this way and serves profiles openly.
        var authRefused = false
        for (rung in AliasProbe.SHAPES) {
            val window = probe.window(url, anchor, rung, onEvent)
            if (window.authRefused && window.ids.isNullOrEmpty()) {
                authRefused = true
                refusals += AUTH_REQUIRED + " " + (window.refusal ?: "rejected our key")
                continue
            }
            val ids = window.ids
            if (ids == null) {
                lastReason = window.reason ?: lastReason
                // filter.nostr.wine answers nothing on that connection once it has refused us, and each
                // further shape would wait out the whole idle window.
                if (authRefused) break
                silent++
                // Two silent rungs in a row is a url that is not there.
                if (rung == AliasProbe.FALLBACK_KINDS && silent == 2) break
                continue
            }
            if (ids.isNotEmpty()) {
                served = window to rung
                break
            }
            // An indexer refuses a bare filter and serves kind 0; a group host serves 39000 alone.
            val refusal = window.refusal
            if (refusal != null) {
                refusals += refusal
            } else if (empty == null) {
                empty = window to rung
            }
        }
        // Refused our key on a shape and served nothing anywhere: what it holds is behind the key.
        if (served == null && authRefused) {
            return Outcome(Verdict.AUTH_REFUSED, "asked for NIP-42 and rejected our key", authRequired = true)
        }
        val chosen = served ?: empty

        // It heard every shape and turned each down: nothing we can generally send gets an answer. A
        // shape that went silent instead leaves it unproven, and the silence path below decides.
        if (chosen == null && refusals.size == AliasProbe.SHAPES.size) {
            val words = refusals.first().take(REFUSAL_QUOTE)
            return if (refusals.all { it.startsWith(AUTH_REQUIRED, ignoreCase = true) }) {
                // After our AUTH: a members-only relay that does not count us as one.
                Outcome(Verdict.AUTH_REFUSED, "refused every shape it was asked as auth-required: $words", authRequired = true)
            } else {
                Outcome(Verdict.RESTRICTED, "refused every shape it was asked (${AliasProbe.SHAPES.joinToString { AliasProbe.shapeOf(it) }}): $words")
            }
        }
        val answered = chosen?.first
        val shape = chosen?.second
        val readMs = answered?.firstPageMs
        if (answered == null) {
            // A relay that CLOSED one shape spoke, so it is not silent, and the quiet shapes keep it unproven.
            if (refusals.isNotEmpty()) {
                defer(Deferral.PARTLY_REFUSED)
                return null
            }
            return silenced(url, lastReason, unmeasured, defer)
        }

        val walked = answered.compliance
        val seen = walked.seen

        // All-or-nothing: a relay that puts only some events above the cursor still advances it.
        if (walked.offWindow > 0 && walked.offWindow == seen) {
            return Outcome(
                Verdict.UNPAGEABLE,
                "every event answered above the `until` it was asked for",
                pageable = false to "${walked.offWindow} of $seen events came back above the anchor — the cursor was ignored",
                rttReadMs = readMs,
                // Asked of the judge, not asserted: one event above the anchor is below the compliance floors.
                compliant = factOf(walked),
            )
        }
        val evidence =
            if (seen == 0) {
                "answered every shape it was asked empty, or refused it, at a settled anchor"
            } else {
                "answered $seen events at a settled anchor" + (shape?.let { ", through ${AliasProbe.shapeOf(it)}" }.orEmpty())
            }

        // Handed over before any further dial, so the per-url deadline cannot leave the url with no
        // verdict, and already graded on page one so a cut cannot publish a grade page one refutes.
        settled(
            if (compliance.decide(walked) == RelayCompliance.Verdict.NONCOMPLIANT) {
                Outcome(Verdict.NONCOMPLIANT, NONCOMPLIANT_EVIDENCE, rttReadMs = readMs, compliant = factOf(walked))
            } else {
                Outcome(Verdict.PRIME, evidence, rttReadMs = readMs, compliant = factOf(walked))
            },
        )

        // The second page, through the rung that answered and under its own clock; a cut claims nothing.
        // A group host's metadata is its whole list of groups, whatever the cursor: there is no page two.
        val floor = answered.oldestAt.takeIf { shape != RelayAliases.GROUP_METADATA_KINDS }
        val asked =
            if (floor == null) {
                null
            } else {
                progress.holding(url.url, STAGE_COMPLIANCE)
                withTimeoutOrNull(probe.deadlineMs(url) / COMPLIANCE_BUDGET_DIVISOR) {
                    Paged(probe.pageBelow(url, floor - 1, shape, onEvent))
                }
            }
        // Boxed for [Reconciled]'s reason: only the clock firing belongs in the count.
        if (floor != null && asked == null) batch.secondPageCut.incrementAndGet()
        val second = asked?.window

        // An empty first page proves the relay answers, not that it can be walked; no claim is published.
        if (second == null) batch.pageUnproven.incrementAndGet()

        val pageable =
            when {
                second == null -> {
                    null
                }

                // A drain is the strongest answer: a relay ignoring the cursor would have served
                // its newest again.
                second.seen == 0 -> {
                    true to "page two below $floor drained — the walk terminates"
                }

                // The fault the mirror aborts on: the walk cannot advance.
                second.offWindow == second.seen -> {
                    return Outcome(
                        Verdict.UNPAGEABLE,
                        "honoured the anchor and then ignored the cursor: page two came back entirely above it",
                        pageable = false to "page one walked to $floor; page two asked below it and answered ${second.seen} events, all above it",
                        rttReadMs = readMs,
                        compliant = factOf(walked + second),
                    )
                }

                else -> {
                    true to
                        "walked two pages, cursor advanced past $floor " +
                        "(${second.seen - second.offWindow} of ${second.seen} event(s) below it)"
                }
            }

        val checked = walked + (second ?: AliasProbe.Compliance())
        if (compliance.decide(checked) == RelayCompliance.Verdict.NONCOMPLIANT) {
            return Outcome(
                Verdict.NONCOMPLIANT,
                NONCOMPLIANT_EVIDENCE,
                pageable = pageable,
                rttReadMs = readMs,
                compliant = factOf(checked),
            )
        }
        val compliantFact = factOf(checked)

        // Re-handed with the facts earned so far, in case the NEG-OPEN outlives the url's clock.
        settled(Outcome(Verdict.PRIME, evidence, pageable = pageable, compliant = compliantFact, rttReadMs = readMs))

        // One NEG-OPEN against a sliver of the window: a normal return is NIP-77 spoken, the
        // dedicated exception is it declined, and anything else writes nothing.
        val sliver = Filter(kinds = shape, since = anchor - NIP77_WINDOW_SECONDS, until = anchor)
        progress.holding(url.url, STAGE_NIP77)
        val reconciled =
            withTimeoutOrNull(nip77DeadlineMs) {
                Reconciled(
                    try {
                        reconcile(url, sliver)
                        true to "answered a NEG-OPEN over a ${NIP77_WINDOW_SECONDS / 3600}h window"
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: NegentropySyncException) {
                        false to "declined the NEG-OPEN: ${e.reason}"
                    } catch (_: Exception) {
                        // No fact: the failure was ours or the moment's, not the relay's.
                        null
                    },
                )
            }
        if (reconciled == null) batch.negOpenCut.incrementAndGet()
        val nip77 = reconciled?.fact

        return Outcome(
            Verdict.PRIME,
            evidence,
            pageable = pageable,
            nip77 = nip77,
            compliant = compliantFact,
            rttReadMs = readMs,
        )
    }

    /**
     * The grade for a url no shape heard from, read off the last transport word. Only a word that is
     * evidence earns `silent`; a rate limit, or words [Silence] does not know, earn nothing.
     */
    private suspend fun silenced(
        url: NormalizedRelayUrl,
        lastReason: String?,
        unmeasured: (String) -> Unit,
        defer: (Deferral) -> Unit,
    ): Outcome? =
        when (val cause = Silence.of(lastReason)) {
            Silence.TLS -> {
                tlsFailed(url, defer)
            }

            Silence.UPGRADE -> {
                upgradeRefused(lastReason, defer)
            }

            // The pre-probe resolved this name moments ago, so a dial that could not is our resolver.
            Silence.NAME -> {
                unmeasured("the dial could not resolve a name the pre-probe had resolved")
                null
            }

            // A server answered and asked us to come back later, which says nothing about the relay.
            Silence.RATE_LIMITED -> {
                defer(Deferral.RATE_LIMITED)
                null
            }

            Silence.UNKNOWN -> {
                // No words at all is our own socket layer as often as the relay, and is left unmeasured.
                if (lastReason != null) defer(Deferral.UNRECOGNISED)
                null
            }

            // A refusal or route word is text, not proof: the typed pre-probe owns `dead`, and it let this url through.
            Silence.REFUSED, Silence.NO_ROUTE, Silence.TIMEOUT -> {
                Outcome(Verdict.SILENT, cause.reason)
            }
        }

    /**
     * The NIP-66 payload for one url: what the dial measured, over what the document claimed.
     * Measured beats advertised tag by tag; only `auth` is measured, and only in the positive direction.
     */
    private fun factsOf(
        url: NormalizedRelayUrl,
        outcome: Outcome,
        reading: RelayDocument.Reading?,
    ): RelayFacts {
        val doc = reading?.doc
        val measured =
            outcome.authRequired?.let { listOf(RelayFacts.requirement(RelayFacts.REQUIREMENT_AUTH, it)) }.orEmpty()
        return RelayFacts(
            network = network(url),
            rttOpenMs = reading?.openMs,
            rttReadMs = outcome.rttReadMs,
            requirements = RelayFacts.merge(measured = measured, advertised = doc?.requirements.orEmpty()),
            software = doc?.software,
            version = doc?.version,
            supportedNips = doc?.supportedNips.orEmpty(),
        )
    }

    /** NIP-66's `n`, from the same predicate the dial uses. */
    private fun network(url: NormalizedRelayUrl): String = if (tor?.routes(url) == true) NETWORK_TOR else NETWORK_CLEARNET

    /** The batch's log lines and its row's counts. [written] is null on the guard's refuse-to-publish path. */
    private fun report(
        label: String,
        candidates: Int,
        startedMs: Long,
        batch: Batch,
        written: Written?,
    ) {
        val byVerdict =
            if (written == null) {
                emptyMap()
            } else {
                batch.outcomes.values
                    .groupingBy { it.verdict }
                    .eachCount()
            }
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts" +
                "; ${batch.downloaded.get()} event(s) downloaded",
        )
        reportSetAside(label, batch)
        if (written != null) reportWrites(label, batch, written)
        val waiting = written?.waiting?.size ?: 0
        progress.counts {
            Verdict.entries.mapNotNull { v -> byVerdict[v]?.let { Processors.Count(v.value, it.toLong()) } } +
                Processors.Count(WAITING_ON_FOLD, waiting.toLong()) +
                // Live, not the batch's: it falls as the retries land.
                Processors.Count(PENDING_WRITES, pending.size.toLong())
        }
    }

    /** One line per way a url left this batch with no verdict, or with less of one; only when there were any. */
    private fun reportSetAside(
        label: String,
        batch: Batch,
    ) {
        val unmeasured = batch.unmeasured.size
        if (unmeasured > 0) {
            System.err.println(
                "router: fitness [$label] — $unmeasured url(s) answered nothing at all, no verdict written: " +
                    "no EOSE, no CLOSED and no transport reason on any rung, or our own resolver, route or socket layer failing",
            )
        }
        val lookups = batch.deferredFor(Deferral.LOOKUP_UNEXPLAINED)
        if (lookups.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${lookups.size} url(s) on ${batch.hostsOf(lookups)} host(s) failed the lookup with no reason left " +
                    "to read, no verdict written; counted once per host as this router failing to dial, since our resolver " +
                    "fails that way as readily as the name",
            )
        }
        val unrecognised = batch.deferredFor(Deferral.UNRECOGNISED)
        if (unrecognised.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${unrecognised.size} url(s) on ${batch.hostsOf(unrecognised)} host(s) gave up in words " +
                    "we do not recognise, no verdict written; counted once per host as this router failing to dial, since a reset, " +
                    "an EOF or a cancellation is our own socket layer as readily as the relay",
            )
        }
        val deferredCounts = batch.deferredCounts()
        if (deferredCounts.isNotEmpty()) {
            val why = deferredCounts.entries.sortedByDescending { it.value }.joinToString { "${it.value} x ${it.key}" }
            System.err.println(
                "router: fitness [$label] — ${deferredCounts.values.sum()} url(s) set aside, no verdict written and NOT " +
                    "counted as this router failing to dial: $why",
            )
        }
        val abandonedCount = batch.abandonedCount.get()
        if (abandonedCount > 0) {
            val named = batch.abandoned.sorted().joinToString()
            val more = if (abandonedCount > batch.abandoned.size) " (+${abandonedCount - batch.abandoned.size} more)" else ""
            System.err.println(
                "router: fitness [$label] — gave up on $abandonedCount url(s) at the per-url deadline, no verdict written: $named$more",
            )
        }
        val negOpenCut = batch.negOpenCut.get()
        if (negOpenCut > 0) {
            System.err.println(
                "router: fitness [$label] — $negOpenCut NEG-OPEN(s) cut at the ${NIP77_DEADLINE_MS / 1000}s wall clock; " +
                    "no nip77 fact published for them, which is our clock and NOT the relay declining",
            )
        }
        val secondPageCut = batch.secondPageCut.get()
        if (secondPageCut > 0) {
            System.err.println(
                "router: fitness [$label] — $secondPageCut second page(s) cut at our own wall clock; those urls " +
                    "were graded on ONE page — our clock, and NOT the relay ignoring a cursor",
            )
        }
        val pageUnproven = batch.pageUnproven.get()
        if (pageUnproven > 0) {
            System.err.println(
                "router: fitness [$label] — $pageUnproven url(s) carry NO `pageable` claim: nothing came back to " +
                    "page from, or page two never answered. Their grade stands; the claim is simply not one this " +
                    "pass earned",
            )
        }
        val cutLate = batch.cutLate.get()
        if (cutLate > 0) {
            System.err.println(
                "router: fitness [$label] — $cutLate url(s) ran past the per-url deadline AFTER earning a verdict; " +
                    "the verdict stands and was written, but the dial did not finish",
            )
        }
    }

    /** What the store took of the batch's writes, and what it is still owed. */
    private fun reportWrites(
        label: String,
        batch: Batch,
        written: Written,
    ) {
        val tally = written.tally
        if (written.waiting.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${written.waiting.size} url(s) on ${written.waiting.values.toSet().size} server(s) wait on " +
                    "the fold behind another url of the same server, not dialled; ${written.withdrawn.size} standing `prime` grade(s) " +
                    "taken back. The fold has not said whether those paths are their own relays, so only the server's stand-in is graded",
            )
        }
        if (written.withdrawalsPending > 0) {
            System.err.println(
                "router: fitness [$label] — ${written.withdrawalsPending} standing `prime` grade(s) NOT taken back yet: the store " +
                    "did not take the batch's writes" + (tally.stoppedBy?.let { " ($it)" }.orEmpty()) +
                    ". They wait with the unwritten grades and are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)}, not next sweep",
            )
        }
        if (written.skipped > 0) {
            System.err.println(
                "router: fitness [$label] — ${written.skipped} inherited verdict(s) left standing: the record already carries " +
                    "them and this pass dialled nothing for them, so re-signing would refresh a measurement it did not take",
            )
        }
        // The pass ended abnormally: the verdicts were earned and the store would not take them.
        val unwritten = batch.outcomes.size - tally.published - written.skipped
        if (unwritten <= 0) return
        val dropped = unwritten - tally.wedged - tally.declined
        val parts =
            buildList {
                if (tally.wedged > 0) add("${tally.wedged} write(s) hit the per-write store deadline")
                if (tally.declined > 0) add("${tally.declined} write(s) failed outright with the store answering")
                if (dropped > 0) add("the remaining $dropped were dropped rather than paying the deadline each")
                // Never nested under `dropped`, which can be zero or negative when a batch stops on its last urls.
                tally.stoppedBy?.let { add("the batch stopped because $it") }
            }
        System.err.println(
            "router: fitness [$label] — $unwritten earned verdict(s) NOT written: ${parts.joinToString(", ")}; " +
                "they are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)} until the store takes them" +
                (writeCursors[label]?.let { ", and the next batch's writes START at $it so the same tail is not dropped twice" }.orEmpty()),
        )
    }

    companion object {
        /**
         * Take back every verdict signed under an older [RelayVerdictRecord.FITNESS_EPOCH]. Runs at
         * boot, the only moment the epoch can have changed, as a store walk with no dials.
         */
        suspend fun retireStaleEpochs(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
            progress: Processors.Handle? = null,
        ): Int {
            // The index answers on the label's value alone, so this walks every graded record and decides here.
            val stale = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to Verdict.entries.map { it.value }),
                ),
                SCAN_PAGE,
                caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            ) { event ->
                val record = event as? RelayDiscoveryEvent ?: return@scan
                // Somebody else's label carrying one of our words can come back; a null here is not
                // a stale grade.
                val grade = ourGrade(record) ?: return@scan
                if (grade.getOrNull(RelayVerdictRecord.LABEL_EPOCH_INDEX) != RelayVerdictRecord.FITNESS_EPOCH) {
                    record.relay()?.let(stale::add)
                }
            }
            retire(record, stale, progress)
            if (stale.isNotEmpty()) {
                System.err.println(
                    "router: fitness — retired ${stale.size} verdict(s) taken under older rules; " +
                        "they read as unmeasured until the next sweep re-takes them",
                )
            }
            return stale.size
        }

        /**
         * Take back every grade still written on `s`, the software tag, where `["s", "dead"]` reads
         * as a relay running software called `dead`. Runs at boot beside [retireStaleEpochs].
         */
        suspend fun retireLegacyGrades(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
            progress: Processors.Handle? = null,
        ): Int {
            val legacy = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    tags = mapOf(RelayVerdictRecord.LEGACY_STATUS_TAG to LEGACY_GRADES),
                ),
                SCAN_PAGE,
                caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            ) { event -> (event as? RelayDiscoveryEvent)?.relay()?.let(legacy::add) }
            retire(record, legacy, progress)
            if (legacy.isNotEmpty()) {
                System.err.println(
                    "router: fitness — retired ${legacy.size} grade(s) still written on the `s` tag; " +
                        "they re-grade onto NIP-32 labels on the next sweep",
                )
            }
            return legacy.size
        }

        /**
         * Withdraw a verdict from each of [urls], several at a time. Every url is a distinct
         * addressable record, so this keeps the single-writer rule as long as no pass runs beside it.
         */
        private suspend fun retire(
            record: RelayVerdictRecord,
            urls: List<NormalizedRelayUrl>,
            progress: Processors.Handle?,
        ) {
            if (urls.isEmpty()) return
            progress?.measuring(urls.size, Processors.UNIT_VERDICT)
            val gate = Semaphore(RETIRE_CONCURRENCY)
            coroutineScope {
                for (url in urls) {
                    launch {
                        gate.withPermit { record.retireFitness(url) }
                        progress?.attempted()
                    }
                }
            }
        }

        private const val RETIRE_CONCURRENCY = 16

        /** This monitor's grade on a record, told apart by namespace: `l` is shared vocabulary. */
        private fun ourGrade(event: RelayDiscoveryEvent): Array<String>? =
            event.tags.firstOrNull {
                it.size > RelayVerdictRecord.LABEL_NAMESPACE_INDEX &&
                    it[0] == RelayVerdictRecord.LABEL_TAG &&
                    it[RelayVerdictRecord.LABEL_NAMESPACE_INDEX] == RelayVerdictRecord.FITNESS_NAMESPACE
            }

        /** What the old build could have written on `s`. May only grow; must not follow a later rename. */
        val LEGACY_GRADES = Verdict.entries.map { it.value } + "syncable"

        /** NIP-66's network values this router can honestly write. */
        const val NETWORK_CLEARNET = "clearnet"

        const val NETWORK_TOR = "tor"

        /** NIP-42's machine-readable prefix on a CLOSED. */
        private const val AUTH_REQUIRED = "auth-required:"

        /** How much of a relay's own refusal a verdict quotes. */
        private const val REFUSAL_QUOTE = 120

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

        /** Records per page when the boot retractions walk our own corpus. */
        const val SCAN_PAGE = 2_000

        /** A held leg's `stage`, in the pass's own words. */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_DOCUMENT = "nip-11 document"

        const val STAGE_LADDER = "ask ladder"

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

        /**
         * Does a batch look like our own network went dark: no dial reached a server, or the pre-probe
         * failed more than [DEAD_GUARD_SHARE] of it, or, below [GUARD_FLOOR], more urls than reached one.
         */
        internal fun looksDark(
            dialled: Int,
            provedGone: Int,
            reached: Int,
        ): Boolean =
            reached == 0 ||
                if (dialled >= GUARD_FLOOR) provedGone > dialled * DEAD_GUARD_SHARE else provedGone > reached
    }
}
