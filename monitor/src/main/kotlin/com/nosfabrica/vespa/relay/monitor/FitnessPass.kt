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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The fitness certificate: one measured verdict per url, written where a
 * stream can select on it.
 *
 * A stream's relay list is one filter over kind-30166 records, `"#l": ["prime"]`,
 * so `prime` is a composite: reachable, answering, canonical, consistent,
 * pageable, answering the filter asked, and readable by us. Slow, empty and a
 * small message cap are facts, not refusals. The grade is a NIP-32 label under
 * [RelayVerdictRecord.FITNESS_NAMESPACE] and names the relay, not our use of it.
 *
 * Every verdict comes from what the relay did: the ask ladder, a second page
 * below the first, one NEG-OPEN, quartz's NIP-42 flow. NIP-11 never decides
 * anything; its descriptive half is published beside the verdict through
 * [RelayFacts], and [factsOf] is the one place a measurement overrides a claim.
 *
 * Verdicts are earned on the monitor's clock and only applied on the stream's.
 * This pass writes its own namespace, `pageable`, `nip77`, `compliant` and
 * [RelayFacts.OWNED], nothing else; the fold's and the consistency pass's
 * standing verdicts are read into `alias` and `inconsistent` at no dial.
 */
class FitnessPass(
    private val record: RelayVerdictRecord,
    /** The small-target ladder; [FITNESS_TARGET] events say "answers and pages" as well as five hundred would. */
    private val probe: AliasProbe,
    private val client: INostrClient,
    /** The fold's standing verdicts over these candidates; read, never earned here. */
    private val foldedAway: suspend (List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    /** The consistency pass's standing refusals; same bargain. */
    private val inconsistent: suspend (List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl>,
    /** The bars a relay's answer is held to. A parameter so the probe that measures them can sweep them. */
    private val compliance: RelayCompliance = RelayCompliance(),
    val progress: Processors.Handle,
    /** The relay's NIP-11 document, for the descriptive fields. Null publishes the same verdicts with fewer facts. */
    private val document: RelayDocument? = null,
    /**
     * The proxy, where there is one. The transport itself, not a predicate off
     * it: the `n` tag must name what carried the measurement, and the gate is
     * sized from the dispatcher it dials on.
     */
    private val tor: TorTransport? = null,
    private val concurrency: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
    /** The wall clock on one verdict write. A parameter so a test can shrink it. */
    private val publishDeadlineMs: Long = PUBLISH_DEADLINE_MS,
    /** The wall clock on the NEG-OPEN; see [NIP77_DEADLINE_MS]. */
    private val nip77DeadlineMs: Long = NIP77_DEADLINE_MS,
    /** How much wall time one batch may lose to a store that is not answering; see [PUBLISH_WEDGE_BUDGET_MS]. */
    private val publishWedgeBudgetMs: Long = PUBLISH_WEDGE_BUDGET_MS,
    /** The NEG-OPEN, the only thing this pass asks [client] for. A seam so a test can make one not come back. */
    private val reconcile: suspend (NormalizedRelayUrl, Filter) -> Unit = { url, sliver ->
        client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
    },
) {
    /** One gate for every pass this component runs; see [DialGate]. */
    private val gate = DialGate.over(concurrency, tor)

    /**
     * The url each batch's write loop stopped on, or no entry when it wrote
     * everything; see the rotation in [measure]. Keyed by label because the
     * sweep and the fast lane share this object, and a lane tick that writes
     * its whole batch would otherwise clear the sweep's resume point.
     * Passes never overlap, so the map is for visibility, not arbitration.
     */
    private val writeCursors = ConcurrentHashMap<String, String>()

    /**
     * What the NEG-OPEN came back with, boxed so `withTimeoutOrNull` can tell
     * "the block returned null" from "the clock fired".
     */
    private class Reconciled(
        val fact: Pair<Boolean, String>?,
    )

    /**
     * The `compliant` fact for one reading, or nothing where the bars do not
     * support one. Every path asks [RelayCompliance] here rather than asserting,
     * since a refusal is where stating the fact instead of measuring it is tempting.
     */
    private fun factOf(reading: AliasProbe.Compliance): Pair<Boolean, String>? =
        when (compliance.decide(reading)) {
            RelayCompliance.Verdict.UNMEASURABLE -> null
            RelayCompliance.Verdict.NONCOMPLIANT -> false to compliance.evidence(reading)
            RelayCompliance.Verdict.COMPLIANT -> true to compliance.evidence(reading)
        }

    /** What the second page came back with, boxed for [Reconciled]'s reason. */
    private class Paged(
        val window: AliasProbe.Compliance?,
    )

    /**
     * One url's outcome, carrying the measured facts that ride the same record
     * edit. Fields rather than side maps keyed by url, so a dial that throws
     * after learning a fact cannot strand an entry.
     */
    private class Outcome(
        val verdict: Verdict,
        val evidence: String,
        val pageable: Pair<Boolean, String>? = null,
        val nip77: Pair<Boolean, String>? = null,
        /** Did the events match the filter that asked for them; see [RelayCompliance]. */
        val compliant: Pair<Boolean, String>? = null,
        /**
         * NIP-66's `rtt-read`: the first page of the rung that answered, never
         * the ladder as a whole or the walk. See [AliasProbe.Window.firstPageMs].
         */
        val rttReadMs: Long? = null,
        /**
         * The relay demanded NIP-42 and would not take our key; a measured
         * requirement that outranks the document. Only ever true: quartz reports
         * `authRefused` and nothing else, so a clean read is not evidence of `!auth`.
         */
        val authRequired: Boolean? = null,
        /**
         * Did this pass dial for this outcome. False only for the two free
         * refusals, which the write loop must not re-stamp; see [RelayVerdictRecord.fitnessGrades].
         */
        val tested: Boolean = true,
    )

    /**
     * Measure [candidates] and write a verdict for each. Returns how many
     * events the dials delivered to [onEvent]; a fitness walk is a sync that
     * also decides.
     */
    suspend fun measure(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit,
        sockets: Sockets,
    ): Int {
        if (candidates.isEmpty()) return 0
        progress.begin("measuring fitness")
        val startedMs = System.currentTimeMillis()
        val outcomes = ConcurrentHashMap<NormalizedRelayUrl, Outcome>()
        // Beside the outcomes, not on them: the document and the REQ fail independently.
        val readings = ConcurrentHashMap<NormalizedRelayUrl, RelayDocument.Reading>()
        val downloaded = AtomicInteger()
        // Urls this pass gave up on rather than measured; the names are bounded.
        val abandonedCount = AtomicInteger()
        val abandoned = ConcurrentHashMap.newKeySet<String>()
        // Urls the same clock cut after the dial had earned a verdict. These publish.
        val cutLate = AtomicInteger()
        // Urls whose NEG-OPEN was cut by its own wall clock; the grade is already earned.
        val negOpenCut = AtomicInteger()
        // Urls whose second page was cut by its own; these are graded on one page.
        val secondPageCut = AtomicInteger()
        // Urls that end with no `pageable` claim at all.
        val pageUnproven = AtomicInteger()
        // Urls this pass asked and got no answer of any kind about. Never
        // published; see the null branch in [dialVerdict].
        val unmeasured = ConcurrentHashMap<NormalizedRelayUrl, String>()
        try {
            // The free refusals first: standing verdicts other passes paid dials for.
            val folded = foldedAway(candidates)
            for ((alias, canonical) in folded) {
                outcomes[alias] = Outcome(Verdict.ALIAS, "folds onto ${canonical.url}", tested = false)
            }
            val remaining = candidates.filter { it !in folded }
            val shaky = inconsistent(remaining)
            for (url in shaky) {
                outcomes[url] = Outcome(Verdict.INCONSISTENT, "failed the reproducibility bar; see the consistency tag", tested = false)
            }

            val toDial = remaining.filter { it !in shaky }
            // The free refusals are outside this count: they cost no socket and
            // would open every pass at a position it did not earn.
            progress.measuring(toDial.size, Processors.UNIT_URL)
            // A week back, so "events above the anchor" can only mean "ignored the cursor".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            coroutineScope {
                for (url in toDial) {
                    launch {
                        gate.withPermit(url) {
                            // The deadline sits inside the permit, bounding the
                            // steps this job owns; around the launch it would time
                            // the wait for a permit. See [AliasProbe.deadlineMs].
                            val ran =
                                withTimeoutOrNull(probe.deadlineMs(url)) {
                                    try {
                                        measureOne(
                                            url,
                                            anchor,
                                            canDial,
                                            sockets,
                                            outcomes,
                                            unmeasured,
                                            readings,
                                            downloaded,
                                            negOpenCut,
                                            secondPageCut,
                                            pageUnproven,
                                            onEvent,
                                        )
                                    } finally {
                                        progress.released(url.url)
                                    }
                                }
                            if (ran == null) {
                                if (outcomes.containsKey(url)) {
                                    // Cut late, and the verdict stands: a relay that
                                    // answered the ladder has told us what it is, and
                                    // our clock firing one step later does not un-tell us.
                                    cutLate.incrementAndGet()
                                } else {
                                    // No verdict is written: our timeout is not a fact
                                    // about the relay. It arrives at the next pass as it did at this one.
                                    if (abandoned.size < MAX_ABANDONED_NAMED) abandoned += url.url
                                    abandonedCount.incrementAndGet()
                                }
                            }
                        }
                        // Counted on completion: the url is behind the pass however it ended.
                    }.invokeOnCompletion { progress.attempted() }
                }
            }

            // The batch guard. Every rule above is per url; when our own
            // dialling breaks it breaks for all of them, and a network does not
            // go dark in one pass. Nothing is published, the clean-looking
            // verdicts included, since the same socket layer produced those.
            val dialled = toDial.size
            val blind = unmeasured.size + abandonedCount.get()
            if (dialled >= GUARD_FLOOR && blind > dialled * GUARD_SHARE) {
                System.err.println(
                    "router: fitness [$label] — REFUSING TO PUBLISH: $blind of $dialled dial(s) came back with no " +
                        "answer at all (${(100.0 * blind / dialled).toInt()}%, over the ${(100 * GUARD_SHARE).toInt()}% " +
                        "guard). A network does not go dark in one pass — this router could not dial. " +
                        "${outcomes.size} verdict(s) dropped unwritten; every url is measured again next pass.",
                )
                report(
                    label,
                    candidates.size,
                    emptyMap(),
                    startedMs,
                    abandonedCount.get(),
                    abandoned,
                    unmeasured.size,
                    downloaded.get(),
                    cutLate = cutLate.get(),
                    negOpenCut = negOpenCut.get(),
                    secondPageCut = secondPageCut.get(),
                    pageUnproven = pageUnproven.get(),
                )
                return downloaded.get()
            }

            // The writes, serial and after the dials: the record edit is a
            // read-modify-write with no CAS, and passes never overlap.
            //
            // Each write under its own wall clock. The store's HTTP client
            // carries no read deadline, so a response that never comes would
            // suspend this loop for the life of the process and stop every
            // later pass behind the monitor's gate. A cut write is not retried:
            // the old record stands and the next sweep re-earns the verdict.
            var published = 0
            var declined = 0
            var skipped = 0
            var wedgedRun = 0
            var wedgedTotal = 0
            // Measured wall time, not a count times the deadline; see [PUBLISH_WEDGE_BUDGET_MS].
            var wedgedMs = 0L
            // Which limit ended the batch, for the report.
            var stoppedBy: String? = null
            // An inherited verdict is written only when it would change
            // something: `measured-at` is how a verdict ages, so re-stamping
            // what this pass did not test would make it immortal. A failed
            // read falls back to writing everything.
            val untested = outcomes.entries.filterNot { it.value.tested }.map { it.key }
            val standing =
                try {
                    record.fitnessGrades(untested)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    System.err.println(
                        "router: fitness [$label] — could not read the standing grades (${e.javaClass.simpleName}); " +
                            "re-signing every inherited verdict this pass rather than skipping one the record needs",
                    )
                    emptyMap()
                }
            // Ordered by url and resumed where the last batch stopped. The
            // wedge limits below end the loop and drop the tail, and hash
            // order is stable across passes, so an unrotated loop dropped the
            // same tail every time. In memory on purpose: a restart starts at
            // the top, which is the same guarantee from a different offset.
            val order = outcomes.keys.sortedBy { it.url }
            val resumeFrom = writeCursors[label]?.let { c -> order.indexOfFirst { it.url >= c } } ?: 0
            // -1 is a cursor sorting past everything this batch holds; it wraps to the top as index 0 does.
            val rotated = if (resumeFrom <= 0) order else order.subList(resumeFrom, order.size) + order.subList(0, resumeFrom)
            // Cleared up front so a throw before the loop's end cannot leave
            // the next batch resuming at a url this one never reached.
            writeCursors.remove(label)
            for (url in rotated) {
                val outcome = outcomes[url] ?: continue
                // The evidence has to match too: a url re-folded onto a
                // different canonical is `alias` both times and not the same statement.
                if (!outcome.tested && standing[url]?.let { it.value == outcome.verdict.value && it.evidence == outcome.evidence } == true) {
                    skipped++
                    continue
                }
                progress.holding(url.url, STAGE_PUBLISH)
                // Three-valued: `true` stored, `false` the store answering and
                // the write still failing, `null` the deadline.
                val writeStartedMs = System.currentTimeMillis()
                val wrote =
                    try {
                        withTimeoutOrNull(publishDeadlineMs) {
                            record.publishFitness(
                                url = url,
                                status = outcome.verdict.value,
                                evidence = outcome.evidence,
                                pageable = outcome.pageable,
                                nip77 = outcome.nip77,
                                compliant = outcome.compliant,
                                facts = factsOf(url, outcome, readings[url]),
                            ) != null
                        }
                    } finally {
                        progress.released(url.url)
                    }
                when (wrote) {
                    true -> {
                        published++
                        // A write going through is proof the wall is not there; see [PUBLISH_WEDGE_LIMIT].
                        wedgedRun = 0
                    }

                    // The store spoke and the write still failed. No reason to
                    // stop: a prompt failure costs nothing per verdict.
                    false -> {
                        declined++
                        // A decline is the store answering, so it ends a run
                        // too; [PUBLISH_WEDGE_BUDGET_MS] bounds the alternating case.
                        wedgedRun = 0
                    }

                    null -> {
                        // Each timed-out write costs the full deadline, so a
                        // store that has stopped answering ends the batch,
                        // loudly, rather than being paid a minute per verdict.
                        wedgedTotal++
                        wedgedRun++
                        wedgedMs += System.currentTimeMillis() - writeStartedMs
                        stoppedBy =
                            when {
                                wedgedRun >= PUBLISH_WEDGE_LIMIT -> "$wedgedRun write(s) in a row went unanswered"
                                wedgedMs >= publishWedgeBudgetMs -> "${wedgedMs / 1000}s of this batch was spent waiting on writes that never came back"
                                else -> null
                            }
                        if (stoppedBy != null) {
                            // At this url, not after it: the write that tripped the limit did not land.
                            writeCursors[label] = url.url
                            break
                        }
                    }
                }
            }

            report(
                label,
                candidates.size,
                outcomes.values.groupingBy { it.verdict }.eachCount(),
                startedMs,
                abandonedCount.get(),
                abandoned,
                unmeasured.size,
                downloaded.get(),
                unwrittenCount = outcomes.size - published - skipped,
                wedgedWrites = wedgedTotal,
                declinedWrites = declined,
                cutLate = cutLate.get(),
                resumeAt = writeCursors[label],
                skippedWrites = skipped,
                stoppedBy = stoppedBy,
                negOpenCut = negOpenCut.get(),
                secondPageCut = secondPageCut.get(),
                pageUnproven = pageUnproven.get(),
            )
        } finally {
            progress.finish()
        }
        return downloaded.get()
    }

    /**
     * One url's whole job: the pre-probe, then the document, then the dial.
     * Only the dial claims a socket; the NIP-11 ask is plain HTTP. [Processors.Handle.holding]
     * is called at each boundary because a suspended coroutine has no stack frame to name its step.
     */
    private suspend fun measureOne(
        url: NormalizedRelayUrl,
        anchor: Long,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        sockets: Sockets,
        outcomes: ConcurrentHashMap<NormalizedRelayUrl, Outcome>,
        unmeasured: ConcurrentHashMap<NormalizedRelayUrl, String>,
        readings: ConcurrentHashMap<NormalizedRelayUrl, RelayDocument.Reading>,
        downloaded: AtomicInteger,
        negOpenCut: AtomicInteger,
        secondPageCut: AtomicInteger,
        pageUnproven: AtomicInteger,
        onEvent: suspend (Event) -> Unit,
    ) {
        progress.holding(url.url, STAGE_REACHABILITY)
        val reachable =
            try {
                canDial(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // The pre-probe throwing is this box failing to ask, not evidence about the server.
                unmeasured[url] = "the reachability probe itself threw ${e.javaClass.simpleName}"
                return
            }
        if (!reachable) {
            outcomes[url] = Outcome(Verdict.DEAD, "no TCP answer at the pre-probe")
            return
        }
        progress.holding(url.url, STAGE_DOCUMENT)
        document?.read(url)?.let { readings[url] = it }
        progress.holding(url.url, STAGE_LADDER)
        sockets.claim(url)
        try {
            val outcome =
                dialVerdict(
                    url,
                    anchor,
                    settled = { outcomes[url] = it },
                    negOpenCut = negOpenCut,
                    secondPageCut = secondPageCut,
                    pageUnproven = pageUnproven,
                ) { event ->
                    downloaded.incrementAndGet()
                    onEvent(event)
                }
            if (outcome != null) {
                outcomes[url] = outcome
            } else {
                unmeasured[url] = "no EOSE, no CLOSED and no transport reason on any rung"
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // A throw on our side is our instrument giving up, unless the
            // ladder already settled a verdict: a url in both `outcomes` and
            // `unmeasured` would feed the batch guard's blind share on a dial that answered.
            if (!outcomes.containsKey(url)) {
                unmeasured[url] = "the dial threw ${e.javaClass.simpleName} before the relay said anything"
            }
        } finally {
            sockets.release(url)
        }
    }

    /**
     * The dial itself: the ask ladder for "answers", the anchored events for
     * "honours `until`", one NEG-OPEN for "reconciles". The ladder follows
     * [AliasProbe.leaderPrint] but keeps each rung's transport reason, because
     * a verdict has to say why.
     */
    private suspend fun dialVerdict(
        url: NormalizedRelayUrl,
        anchor: Long,
        /**
         * The verdict, handed over the moment the ladder has earned it and
         * before any step with no wall clock of its own, so the per-url deadline
         * cannot cost it. A later return replaces it. Only the `prime` path calls this.
         */
        settled: (Outcome) -> Unit,
        /** Bumped when the NEG-OPEN's own wall clock fires; see [NIP77_DEADLINE_MS]. */
        negOpenCut: AtomicInteger,
        /** Bumped when the second page's does; that url is graded on one page. See [COMPLIANCE_BUDGET_DIVISOR]. */
        secondPageCut: AtomicInteger,
        /** Bumped for urls that end with no `pageable` claim at all. Not a fault. */
        pageUnproven: AtomicInteger,
        onEvent: suspend (Event) -> Unit,
    ): Outcome? {
        var lastReason: String? = null
        var answered: AliasProbe.Window? = null
        var shape: List<Int>? = null
        var readMs: Long? = null
        for (rung in listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)) {
            val window = probe.window(url, anchor, rung, onEvent)
            if (window.authRefused) {
                return Outcome(
                    Verdict.AUTH_REFUSED,
                    "asked for NIP-42 and rejected our key",
                    authRequired = true,
                )
            }
            if (window.ids != null) {
                answered = window
                shape = rung
                readMs = window.firstPageMs
                break
            }
            lastReason = window.reason ?: lastReason
            // Two silent rungs in a row is a url that is not there; the same exit [AliasProbe.leaderPrint] makes.
            if (rung == AliasProbe.FALLBACK_KINDS && lastReason != null) break
        }

        if (answered == null) {
            // Never spoke, or refused every shape. [Silence] tells the two apart.
            return when (val cause = Silence.of(lastReason)) {
                Silence.TIMEOUT, Silence.RATE_LIMITED, Silence.UNKNOWN -> {
                    if (lastReason == null) {
                        // Nothing came back at all: no EOSE, no CLOSED, no
                        // transport word. That is what our own socket layer
                        // produces when it is the broken thing, so nothing is
                        // published and the url is measured again next pass.
                        // [Verdict.RESTRICTED] has no path here: an empty
                        // window is read as a drain, and telling it from a
                        // refusal needs a signal `AliasProbe.Page` does not carry.
                        null
                    } else {
                        // Only a transport word reaches here: anything the relay
                        // itself said made it "speak".
                        Outcome(Verdict.SILENT, cause.reason)
                    }
                }

                else -> {
                    Outcome(Verdict.DEAD, cause.reason)
                }
            }
        }

        // From the rung that answered, which is every event this dial saw;
        // checked against each page's own cursor. See [AliasProbe.Compliance].
        val walked = answered.compliance
        val seen = walked.seen

        // The anchor itself ignored: every event above the `until` asked for,
        // so there is nothing to walk from. All-or-nothing, since a relay that
        // puts some events above the cursor still advances it.
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
        val evidence = "answered ${if (seen == 0) "an empty anchored page" else "$seen events"} at a settled anchor"

        // Handed over before any further dial: the per-url deadline firing in
        // the second page must not leave the url with no verdict and feed the
        // batch guard's blind share.
        settled(Outcome(Verdict.PRIME, evidence, rttReadMs = readMs))

        // The second page, through the rung that answered: page two of a
        // different shape is not page two of page one. Its own clock; a cut
        // publishes no `pageable` claim and no refusal.
        val floor = answered.oldestAt
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
        if (floor != null && asked == null) secondPageCut.incrementAndGet()
        val second = asked?.window

        // A claim we could not earn is not published: an empty first page
        // proves the relay answers and cannot prove it can be walked. It does
        // not cost the relay its grade.
        if (second == null) pageUnproven.incrementAndGet()

        val pageable =
            when {
                second == null -> {
                    null
                }

                // A drain is the strongest answer: a relay ignoring the cursor
                // would have served its newest events again.
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
                "answered with events the filter did not ask for",
                pageable = pageable,
                rttReadMs = readMs,
                compliant = factOf(checked),
            )
        }
        val compliantFact = factOf(checked)

        // Re-handed with the facts the two steps above earned, so a NEG-OPEN
        // that outlives the url's clock leaves the fuller verdict down.
        settled(Outcome(Verdict.PRIME, evidence, pageable = pageable, compliant = compliantFact, rttReadMs = readMs))

        // One NEG-OPEN against a sliver of the window. A normal return is the
        // relay speaking NIP-77; the dedicated exception is it declining.
        // Anything else writes nothing, so a flaky moment cannot demote a reconciling relay.
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
        // A budget that fires says so; every other budget in this pass is named in the report.
        if (reconciled == null) negOpenCut.incrementAndGet()
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
     * The NIP-66 payload for one url: what the dial measured, over what the
     * document claimed. Measured beats advertised tag by tag, which NIP-66
     * names as the expected case. Only `auth` is measured, and only in the
     * positive direction; see [Outcome.authRequired]. The two free refusals
     * dialled nothing, so they carry only [network] and the rest is cleared.
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

    /** NIP-66's `n`, from the same predicate the dial uses, so a record never says `clearnet` about a Tor url. */
    private fun network(url: NormalizedRelayUrl): String = if (tor?.routes(url) == true) NETWORK_TOR else NETWORK_CLEARNET

    private fun report(
        label: String,
        candidates: Int,
        byVerdict: Map<Verdict, Int>,
        startedMs: Long,
        abandonedCount: Int,
        abandoned: Set<String>,
        /** Urls this pass asked and learned nothing from. They carry no verdict, so they are absent from the counts. */
        unmeasuredCount: Int,
        /** Events the dials handed to ingest; the monitor plane's largest effect on the store. */
        downloadedCount: Int,
        /** Earned verdicts that never reached the store. Zero on the guard's refuse-to-publish path. */
        unwrittenCount: Int = 0,
        /** Of which this many hit the per-write deadline. */
        wedgedWrites: Int = 0,
        /** And this many the store answered and still failed. */
        declinedWrites: Int = 0,
        /** Urls the per-url deadline cut after the dial had earned a verdict. These publish. */
        cutLate: Int = 0,
        /** The url the next batch's write loop resumes at; null when every earned verdict was written. */
        resumeAt: String? = null,
        /** Inherited verdicts the loop did not re-sign because the record already carries them. */
        skippedWrites: Int = 0,
        /** Which limit ended the batch, in words, or null when it ran to its end. */
        stoppedBy: String? = null,
        /** Urls whose NEG-OPEN was cut by its own wall clock; never a verdict. */
        negOpenCut: Int = 0,
        /** Urls whose second page was cut by its own; still graded, on one page. */
        secondPageCut: Int = 0,
        /** Urls that ended with no `pageable` claim at all. */
        pageUnproven: Int = 0,
    ) {
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts" +
                "; $downloadedCount event(s) downloaded",
        )
        // Own lines, only when there were any: these urls are absent from the
        // counts above, and a pass that lost a hundred would otherwise read as
        // a clean partition over the ones it reached.
        if (unmeasuredCount > 0) {
            System.err.println(
                "router: fitness [$label] — $unmeasuredCount url(s) answered nothing at all, no verdict written: " +
                    "no EOSE, no CLOSED and no transport reason on any rung, or a throw on our side of the socket",
            )
        }
        if (abandonedCount > 0) {
            val named = abandoned.sorted().joinToString()
            val more = if (abandonedCount > abandoned.size) " (+${abandonedCount - abandoned.size} more)" else ""
            System.err.println(
                "router: fitness [$label] — gave up on $abandonedCount url(s) at the per-url deadline, no verdict written: $named$more",
            )
        }
        if (negOpenCut > 0) {
            System.err.println(
                "router: fitness [$label] — $negOpenCut NEG-OPEN(s) cut at the ${NIP77_DEADLINE_MS / 1000}s wall clock; " +
                    "no nip77 fact published for them, which is our clock and NOT the relay declining",
            )
        }
        if (secondPageCut > 0) {
            System.err.println(
                "router: fitness [$label] — $secondPageCut second page(s) cut at our own wall clock; those urls " +
                    "were graded on ONE page, which is the state #187 is about — our clock, and NOT the relay " +
                    "ignoring a cursor",
            )
        }
        if (pageUnproven > 0) {
            System.err.println(
                "router: fitness [$label] — $pageUnproven url(s) carry NO `pageable` claim: nothing came back to " +
                    "page from, or page two never answered. Their grade stands; the claim is simply not one this " +
                    "pass earned",
            )
        }
        if (cutLate > 0) {
            System.err.println(
                "router: fitness [$label] — $cutLate url(s) ran past the per-url deadline AFTER earning a verdict; " +
                    "the verdict stands and was written, but the dial did not finish",
            )
        }
        if (skippedWrites > 0) {
            System.err.println(
                "router: fitness [$label] — $skippedWrites inherited verdict(s) left standing: the record already carries " +
                    "them and this pass dialled nothing for them, so re-signing would refresh a measurement it did not take",
            )
        }
        // The pass ended abnormally: the verdicts were earned and the store would not take them.
        if (unwrittenCount > 0) {
            val dropped = unwrittenCount - wedgedWrites - declinedWrites
            val parts =
                buildList {
                    if (wedgedWrites > 0) add("$wedgedWrites write(s) hit the per-write store deadline")
                    if (declinedWrites > 0) add("$declinedWrites write(s) failed outright with the store answering")
                    if (dropped > 0) add("the remaining $dropped were dropped rather than paying the deadline each")
                    // Never nested under `dropped`, which can come out zero or
                    // negative when a batch stops on its last few urls.
                    if (stoppedBy != null) add("the batch stopped because $stoppedBy")
                }
            System.err.println(
                "router: fitness [$label] — $unwrittenCount earned verdict(s) NOT written: ${parts.joinToString(", ")}; " +
                    "every url is measured again next pass" +
                    (resumeAt?.let { ", and the next batch's writes START at $it so the same tail is not dropped twice" }.orEmpty()),
            )
        }
        progress.counts {
            Verdict.entries.mapNotNull { v -> byVerdict[v]?.let { Processors.Count(v.value, it.toLong()) } }
        }
    }

    companion object {
        /**
         * Take back every verdict signed under an older [RelayVerdictRecord.FITNESS_EPOCH]:
         * a reading of a different rule that no amount of waiting reconciles.
         * Runs at boot, the only moment the epoch can have changed, as a store
         * walk with no dials. Candidates re-earn their verdict on the next sweep.
         */
        suspend fun retireStaleEpochs(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
        ): Int {
            // Paged, because the tag index answers on the label's value and
            // the epoch lives further along the same tag, so this returns every
            // graded record and the epoch is decided here.
            val stale = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to Verdict.entries.map { it.value }),
                ),
                SCAN_PAGE,
                // The monitor's own records, not the url round-up's sources; see [StoreCalls].
                caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            ) { event ->
                val record = event as? RelayDiscoveryEvent ?: return@scan
                // The index answers on the value alone, so somebody else's label
                // carrying one of our words can come back; a null here is not a stale grade.
                val grade = ourGrade(record) ?: return@scan
                if (grade.getOrNull(RelayVerdictRecord.LABEL_EPOCH_INDEX) != RelayVerdictRecord.FITNESS_EPOCH) {
                    record.relay()?.let(stale::add)
                }
            }
            retire(record, stale)
            if (stale.isNotEmpty()) {
                System.err.println(
                    "router: fitness — retired ${stale.size} verdict(s) taken under older rules; " +
                        "they read as unmeasured until the next sweep re-takes them",
                )
            }
            return stale.size
        }

        /**
         * Take back every grade still written on `s`, the software tag
         * ([RelayVerdictRecord.LEGACY_STATUS_TAG]), where `["s", "dead"]` reads
         * as a relay running software called `dead`. Runs at boot beside
         * [retireStaleEpochs]; once the store holds no such record it costs one
         * empty indexed query per boot.
         */
        suspend fun retireLegacyGrades(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
        ): Int {
            val legacy = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    // The old build's vocabulary, not this one's; see [LEGACY_GRADES].
                    tags = mapOf(RelayVerdictRecord.LEGACY_STATUS_TAG to LEGACY_GRADES),
                ),
                SCAN_PAGE,
                caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            ) { event -> (event as? RelayDiscoveryEvent)?.relay()?.let(legacy::add) }
            retire(record, legacy)
            if (legacy.isNotEmpty()) {
                System.err.println(
                    "router: fitness — retired ${legacy.size} grade(s) still written on the `s` tag; " +
                        "they re-grade onto NIP-32 labels on the next sweep",
                )
            }
            return legacy.size
        }

        /**
         * Withdraw a verdict from each of [urls], several at a time. Every url
         * is a distinct addressable record, so this keeps the single-writer
         * rule; what must not happen is running beside the fitness pass, which
         * is why both callers stay on the boot path.
         */
        private suspend fun retire(
            record: RelayVerdictRecord,
            urls: List<NormalizedRelayUrl>,
        ) {
            if (urls.isEmpty()) return
            val gate = Semaphore(RETIRE_CONCURRENCY)
            coroutineScope {
                for (url in urls) launch { gate.withPermit { record.retireFitness(url) } }
            }
        }

        /** Retractions in flight at once: a store round trip and a signature, on a store shared with a serving relay. */
        private const val RETIRE_CONCURRENCY = 16

        /**
         * This monitor's grade on a record, told apart by namespace: `l` is
         * shared vocabulary, and a foreign label's third element is not our epoch.
         */
        private fun ourGrade(event: RelayDiscoveryEvent): Array<String>? =
            event.tags.firstOrNull {
                it.size > RelayVerdictRecord.LABEL_NAMESPACE_INDEX &&
                    it[0] == RelayVerdictRecord.LABEL_TAG &&
                    it[RelayVerdictRecord.LABEL_NAMESPACE_INDEX] == RelayVerdictRecord.FITNESS_NAMESPACE
            }

        /**
         * What the old build could have written on `s`. Frozen in source
         * because it describes history: it may only grow, and must not follow
         * a rename made after the records were signed.
         */
        val LEGACY_GRADES = Verdict.entries.map { it.value } + "syncable"

        /** NIP-66's network values this router can honestly write; `i2p` and `loki` have no transport here. */
        const val NETWORK_CLEARNET = "clearnet"

        const val NETWORK_TOR = "tor"

        /** Events per fitness ask; this pass dials the whole corpus, so the target is the pass's cost. */
        const val FITNESS_TARGET = 20

        /**
         * What share of a url's whole budget the second page may spend. A
         * divisor rather than a duration so the bound scales with the transport
         * the way [AliasProbe.deadlineMs] does.
         */
        const val COMPLIANCE_BUDGET_DIVISOR = 4

        /** The NEG-OPEN sliver: one hour is enough to prove the verb. */
        const val NIP77_WINDOW_SECONDS = 3600L

        /** The NEG-OPEN's idle window, shorter than a transfer's: a yes/no about the protocol. */
        const val NIP77_IDLE_MS = 10_000L

        /**
         * The wall clock over the NEG-OPEN. A reconciliation is rounds, and the
         * idle window is re-armed by each, so a relay that keeps answering
         * slowly never trips [NIP77_IDLE_MS]; this is the one step able to
         * spend the whole per-url budget.
         */
        const val NIP77_DEADLINE_MS = 3 * NIP77_IDLE_MS

        /** Records per page when the boot retractions walk our own corpus; neither can ask its question in a filter. */
        const val SCAN_PAGE = 2_000

        /** A held leg's `stage`, in the pass's own words; see [Processors.Holding.Held.stage]. */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_DOCUMENT = "nip-11 document"

        const val STAGE_LADDER = "ask ladder"

        const val STAGE_NIP77 = "neg-open"

        /** The one ask below the ladder's window, a REQ and not a rung of it. */
        const val STAGE_COMPLIANCE = "second page"

        /** The one stage that is not a dial: the record edit that puts a verdict down. */
        const val STAGE_PUBLISH = "verdict write"

        /**
         * The wall clock on one verdict write. Sized against the store's worst
         * honest case (a write queued behind the mirror's bulk commits), since
         * the fault it bounds is not slowness but forever.
         */
        const val PUBLISH_DEADLINE_MS = 60_000L

        /**
         * How many writes in a row may hit [PUBLISH_DEADLINE_MS] before the
         * pass stops publishing the batch. Consecutive, resetting on every
         * write the store answers: a wedged store fails every write the same
         * way, while ordinary load lets a few straggle with thousands succeeding between.
         */
        const val PUBLISH_WEDGE_LIMIT = 3

        /**
         * How much wall time one batch may lose to writes that never came back.
         * The consecutive limit does not bound a store that alternates, and a
         * duration means the same thing on a 500-url lane tick and a 20,000-url sweep.
         */
        const val PUBLISH_WEDGE_BUDGET_MS = 20L * 60 * 1000

        /** How many abandoned urls a pass names in its log line. The count beside them is always whole. */
        const val MAX_ABANDONED_NAMED = 32

        /**
         * The share of a batch's dials that may come back with no answer before
         * the whole pass is refused. A dead url answers, with a refusal or an
         * NXDOMAIN; nothing at all is our socket layer.
         */
        const val GUARD_SHARE = 0.25

        /** The batch size below which the guard does not apply; it is a statement about a population. */
        const val GUARD_FLOOR = 50
    }
}
