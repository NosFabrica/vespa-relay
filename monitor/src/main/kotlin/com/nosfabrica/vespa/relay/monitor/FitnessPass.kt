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
 * THE FITNESS CERTIFICATE — one measured verdict per url, written where a
 * stream can select on it.
 *
 * ## What `prime` asserts, and what it deliberately does not
 *
 * A sync stream's relay list is built from one filter over kind-30166 records:
 * `"#l": ["prime"]`. For that filter to be the WHOLE admission decision, the
 * value has to be a composite: reachable AND answering AND canonical AND
 * consistent AND pageable AND ANSWERING THE FILTER ASKED AND readable by us.
 * Six of the refusals below describe relays that are perfectly alive — an alias
 * serves events, an inconsistent relay answers promptly, a cursor-ignoring
 * relay EOSEs every page, a noncompliant one serves real signed events all day
 * — which is why the value is not called "live". Slow is not a refusal (rtt is
 * a fact, not a fault), empty is not a refusal (a drain is the relay's honest
 * answer), and a small message cap is not a refusal (a shape the asks respect).
 *
 * **`noncompliant` is the newest of them and the one no other pass could ever
 * have earned.** The stability gate compares two answers to each other, so a
 * relay serving the same wrong events to every REQ passes it at 1.000; nothing
 * in that comparison opens an event. This pass reads them — see
 * [RelayCompliance], which owns the rules, and [AliasProbe.Compliance], which
 * owns the counters.
 *
 * The grade is a NIP-32 label under [RelayVerdictRecord.FITNESS_NAMESPACE] and
 * it names the RELAY, not our use of it — see [Verdict] for why that stopped
 * being `syncable`.
 *
 * ## No VERDICT is read off NIP-11; the descriptive fields are
 *
 * Every verdict here comes from what the relay DID: the ask ladder for whether
 * it answers, a SECOND PAGE below the first for whether it can be walked at
 * all and whether the events it serves are the ones asked for, one NEG-OPEN for
 * whether it reconciles, quartz's own NIP-42 flow for whether our auth sticks. NIP-11 documents routinely disagree with the relay's own practice —
 * measured on this corpus: a relay that served a REQ over its declared
 * `max_message_length`, a fleet that publishes no document at all — so nothing
 * that changes a decision is taken from one.
 *
 * That rule is about DECISIONS, and it used to be applied to the whole record.
 * The result was a 30166 carrying a verdict and nothing else — no software, no
 * supported nips, no rtt, no network — which is not a neutral record but a
 * misleading one: quartz's own convention reads a rtt-less 30166 inside the TTL
 * as "checked, could not open". So the pass also fetches the document and
 * publishes the descriptive half, clearly separated. [RelayFacts] is what goes
 * on the record and where each field came from; [factsOf] is the one place a
 * measurement overrides a claim.
 *
 * ## Why a separate pass, on the monitor's clock
 *
 * The checks a stream needs are exactly the checks that must never sit on its
 * critical path. Run inside the fan-out they delay every pass, so they stay
 * minimal; run here they can be as thorough as the verdict deserves, and the
 * stream's discovery collapses to one indexed query. Same bargain as the fold:
 * verdicts are earned on this clock and only APPLIED on the stream's.
 *
 * ## One writer per tag
 *
 * This pass owns its own NIP-32 namespace, `pageable`, `nip77`, `compliant` and
 * the descriptive tags in [RelayFacts.OWNED] — and nothing else. The fold's
 * `same-as` and the consistency pass's tag are read, never written; another
 * labeller's namespace on the same record rides through untouched, which is
 * exactly what moving off `s` bought. The alias and inconsistency REFUSALS
 * cost no dial at all — those passes already paid for the evidence, and this
 * one turns their standing verdicts into the one value a stream filters on.
 */
class FitnessPass(
    private val record: RelayVerdictRecord,
    /**
     * The small-target ladder — see [AliasProbe]. Sized for a verdict, not a
     * fingerprint: [FITNESS_TARGET] events say "answers and pages" as well as
     * five hundred would.
     */
    private val probe: AliasProbe,
    private val client: INostrClient,
    /** The fold's standing verdicts over these candidates — read, never earned here. */
    private val foldedAway: suspend (List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    /** The consistency pass's standing refusals — same bargain. */
    private val inconsistent: suspend (List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl>,
    /**
     * The bars a relay's answer is held to — see [RelayCompliance], whose own
     * header says how provisional they are. A parameter so the probe that has
     * to MEASURE them can sweep them without a second copy of the rules.
     */
    private val compliance: RelayCompliance = RelayCompliance(),
    val progress: Processors.Handle,
    /**
     * The relay's own NIP-11 document, for the descriptive fields no dial can
     * measure — see [RelayDocument], and [RelayFacts] for which those are and
     * which they deliberately are not.
     *
     * NULLABLE, and null is a monitor that publishes the same verdicts with
     * fewer facts beside them. A test that only cares what a relay was graded
     * should not have to stand up an HTTP client to find out, and a deployment
     * that would rather not ask strangers for their documents has a way to say
     * so that does not involve a second code path.
     */
    private val document: RelayDocument? = null,
    /**
     * The proxy, where this deployment has one. TWO things read it, and both
     * have to be the transport itself rather than a predicate copied off it:
     * the `n` tag this pass publishes must name the transport that actually
     * carried the measurement, and the gate below has to be sized from the
     * dispatcher that transport dials on. Handing in a `(url) -> Boolean` gave
     * the first and not the second, and a second argument for the second is a
     * second thing to keep in step with the first.
     */
    private val tor: TorTransport? = null,
    private val concurrency: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
    /**
     * The wall clock on ONE verdict write — see the write loop in [measure]
     * for why it exists. A parameter for [AliasProbe.idleMs]'s reason: the
     * production value is a minute, and a test that cannot shrink it can only
     * assert that the deadline compiles.
     */
    private val publishDeadlineMs: Long = PUBLISH_DEADLINE_MS,
    /**
     * The wall clock on the NEG-OPEN — see [NIP77_DEADLINE_MS], and
     * [publishDeadlineMs] for why a production-sized constant needs a
     * parameter beside it: a test that cannot shrink this can only assert that
     * the bound compiles.
     */
    private val nip77DeadlineMs: Long = NIP77_DEADLINE_MS,
    /**
     * How much wall time one batch may lose to a store that is not answering —
     * see [PUBLISH_WEDGE_BUDGET_MS], and [publishDeadlineMs] for why a
     * production-sized constant needs a parameter beside it.
     */
    private val publishWedgeBudgetMs: Long = PUBLISH_WEDGE_BUDGET_MS,
    /**
     * The NEG-OPEN itself, which is the ONLY thing this pass asks [client] for.
     *
     * A seam for the same reason [AliasProbe] takes its `fetch` as one: the
     * behaviour that matters here is a reconciliation that does not come back,
     * and there is no way to ask a real client for one. The default is the real
     * call and nothing the router builds passes anything else.
     */
    private val reconcile: suspend (NormalizedRelayUrl, Filter) -> Unit = { url, sliver ->
        client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
    },
) {
    /** One gate object for every pass this component runs — see [DialGate], and [AliasFolding]. */
    private val gate = DialGate.over(concurrency, tor)

    /**
     * The url each batch's write loop stopped ON, or no entry when it wrote
     * every verdict it earned — see the rotation in [measure].
     *
     * **KEYED BY LABEL, and that is the whole of it working at all.** One
     * [FitnessPass] serves two callers on two clocks: the sweep, over the whole
     * corpus every `monitor { interval }`, and the fast lane, over the handful
     * of urls named since its last look every `fastLaneSeconds` (120 by
     * default) — see [MonitorEngine]'s `fitnessEntry`, which is the SAME object
     * in both lists. A single cursor is therefore written ~180 times between
     * sweeps by batches of three urls, and a lane tick that writes its whole
     * batch clears it: the sweep's resume point would be gone every time,
     * every sweep, and the rotation would be dead code that looks alive.
     *
     * Two entries ever. Only the write loop reads or writes them, and passes
     * never overlap ([AliasMonitor] holds a mutex over the sweep and the lane
     * alike), so the map is for visibility across threads rather than for
     * arbitrating a race.
     */
    private val writeCursors = ConcurrentHashMap<String, String>()

    /**
     * What the NEG-OPEN came back with, boxed.
     *
     * `withTimeoutOrNull` collapses "the block returned null" and "the clock
     * fired" into one null, and those are different facts about different
     * things: the first is the relay's moment or ours, the second is THIS
     * PASS's budget being spent. A box makes the outer null mean exactly one
     * thing, so the budget can be counted and said out loud.
     */
    private class Reconciled(
        val fact: Pair<Boolean, String>?,
    )

    /**
     * The `compliant` fact for one reading, or NOTHING where the bars do not
     * support one — the single place every path asks [RelayCompliance] rather
     * than asserting.
     *
     * It exists because two of the three publishing paths are refusals, and a
     * refusal is exactly where it is tempting to state the fact rather than
     * measure it: a relay that put one event above the anchor is `unpageable`
     * by a rule that needs no share and no count, and a `compliant false`
     * written beside it on that evidence is the claim
     * [RelayCompliance.minOffFilterEvents] exists to refuse.
     */
    private fun factOf(reading: AliasProbe.Compliance): Pair<Boolean, String>? =
        when (compliance.decide(reading)) {
            RelayCompliance.Verdict.UNMEASURABLE -> null
            RelayCompliance.Verdict.NONCOMPLIANT -> false to compliance.evidence(reading)
            RelayCompliance.Verdict.COMPLIANT -> true to compliance.evidence(reading)
        }

    /**
     * What the second page came back with, boxed — see [Reconciled], whose
     * argument this is one for one.
     */
    private class Paged(
        val window: AliasProbe.Compliance?,
    )

    /**
     * One url's outcome, for the pass's own funnel — carrying the measured
     * facts that ride the same record edit. Fields on the return value
     * rather than side maps keyed by url: a dial that throws after learning
     * a fact cannot strand an entry a remove-on-read map would hold forever.
     */
    private class Outcome(
        val verdict: Verdict,
        val evidence: String,
        val pageable: Pair<Boolean, String>? = null,
        val nip77: Pair<Boolean, String>? = null,
        /** Did the events match the filter that asked for them — see [RelayCompliance]. */
        val compliant: Pair<Boolean, String>? = null,
        /**
         * NIP-66's `rtt-read`: how long the relay took to answer the ask that
         * settled the verdict.
         *
         * From the rung that ANSWERED, never from the ladder as a whole. A
         * relay that refuses a bare filter and serves the kinds one has told us
         * how fast it reads once asked properly; billing it for the rung it
         * declined would publish our ladder's shape as the relay's latency.
         *
         * And from that rung's FIRST PAGE, not its walk — see
         * [AliasProbe.Window.firstPageMs]. Timing the walk billed a relay
         * capping at ten events for the two round trips our twenty-event
         * target then costs, which is a measurement of our target against
         * their cap rather than of their latency.
         */
        val rttReadMs: Long? = null,
        /**
         * The relay demanded NIP-42 and would not take our key — a MEASURED
         * requirement, which outranks whatever its document claims.
         *
         * **ONLY EVER TRUE, and the asymmetry is the instrument's, not an
         * oversight.** A dial that read cleanly proves nothing about `!auth`:
         * quartz reports `authRefused` and nothing else, so a relay that
         * challenged us and accepted our signer is indistinguishable here from
         * one that never challenged at all — and the first genuinely DOES
         * require auth. Publishing `!auth` off a successful read would
         * therefore tell every reader without our key that a gated relay is
         * open to them.
         *
         * So the override is one-directional: we can contradict a document
         * that claims `!auth`, and we defer to one that claims `auth`. Making
         * it symmetric needs a "was challenged" signal from the client, not a
         * change here.
         */
        val authRequired: Boolean? = null,
        /**
         * Did THIS pass dial for this outcome, or did it inherit one?
         *
         * True for everything [measureOne] produces — including the `dead` a
         * refused pre-probe earns, which is a measurement like any other. False
         * only for the two free refusals, which reach the write loop having
         * cost no socket: the fold already proved the alias and the stability
         * gate already proved the inconsistency, and this pass is only
         * restating them in the one value a stream filters on.
         *
         * The write loop is what reads it — see the skip there, and
         * [RelayVerdictRecord.fitnessGrades] for why re-stamping an inherited
         * verdict is a claim we did not earn.
         */
        val tested: Boolean = true,
    )

    /**
     * Measure [candidates] and write a verdict for each. Returns how many
     * events the dials delivered to [onEvent] — a fitness walk is a sync that
     * also decides, same as the fold's.
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
        // What each url's NIP-11 ask returned. Kept beside the outcomes rather
        // than on them because the two are taken by different asks that fail
        // independently: a relay can serve a perfect document and refuse every
        // REQ, or answer every REQ and serve no document at all.
        val readings = ConcurrentHashMap<NormalizedRelayUrl, RelayDocument.Reading>()
        val downloaded = AtomicInteger()
        // Urls this pass gave up on rather than measured — see
        // [AliasProbe.deadlineMs]. The count is the fact; the names are what
        // made the 74-minute stall unnameable without them, and they are
        // bounded because a pass that abandons everything must not turn one
        // fault into 12,374 log lines.
        val abandonedCount = AtomicInteger()
        val abandoned = ConcurrentHashMap.newKeySet<String>()
        // …and the urls the same clock cut AFTER the dial had already earned a
        // verdict — see the cut-late branch below. Counted apart from both
        // [abandoned] and the verdicts: these DO publish, so they are not a
        // url the pass lost, but a job running past its budget is still a fact
        // about this instrument and the number is the only place it shows.
        val cutLate = AtomicInteger()
        // …and the urls whose NEG-OPEN was cut by its own wall clock — see
        // [NIP77_DEADLINE_MS]. Not a verdict and not a loss: the grade was
        // already earned and is published. It is counted because a budget that
        // spends itself in silence is exactly what made #172 hard.
        val negOpenCut = AtomicInteger()
        // …and the urls that answered every rung by REFUSING it — see the climb
        // in [dialVerdict]. Not a loss and not a refusal: they are graded, on
        // the relay having answered rather than on a window.
        val refusedShapes = AtomicInteger()
        // …and the urls whose SECOND PAGE was cut by its own — see
        // [COMPLIANCE_BUDGET_DIVISOR]. Kept apart from the count above because
        // this one costs a check the verdict rests on: the url is graded on ONE
        // page, which is the state #187 is about.
        val secondPageCut = AtomicInteger()
        // …and the urls that end with no `pageable` claim at all, for any
        // reason: nothing came back to page from, or page two never answered.
        val pageUnproven = AtomicInteger()
        // …and the urls this pass ASKED and got no answer of any kind about:
        // no EOSE, no CLOSED, no transport word, or a throw on our side of the
        // socket. Kept apart from [outcomes] because they are not verdicts and
        // must never be published — see the null branch in [dialVerdict] — and
        // apart from [abandoned] because a deadline and a silence are different
        // faults with different fixes.
        val unmeasured = ConcurrentHashMap<NormalizedRelayUrl, String>()
        try {
            // THE FREE REFUSALS FIRST. Both are standing verdicts other passes
            // already paid dials for; turning them into a status costs a store
            // read and a record edit, never a socket.
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
            // WHAT THIS PASS IS ABOUT TO WALK, published before the first dial
            // — see [Processors.Measuring]. The two free refusals above are
            // deliberately outside it: they cost no socket and are all decided
            // before this line, so counting them would open every pass at a
            // position it did not earn and mislead the rate the ETA is drawn
            // from.
            progress.measuring(toDial.size, Processors.UNIT_URL)
            // A week back, for the same reason the consistency pass anchors
            // there: an anchored ask against a settled window is the only way
            // "events above the anchor" can mean "ignored the cursor" rather
            // than "new events arrived mid-walk".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            coroutineScope {
                for (url in toDial) {
                    launch {
                        gate.withPermit(url) {
                            // THE DEADLINE, AND IT IS INSIDE THE PERMIT.
                            //
                            // Around the `launch` instead, it would be counting
                            // the wait for one of `concurrency` permits — which
                            // on 12,374 urls at 500 permits is most of a job's
                            // life, is the pass's own shape rather than any
                            // relay's, and would cut the urls at the back of the
                            // queue first. Here it bounds exactly the steps this
                            // job owns: the pre-probe, the document, the ladder,
                            // the NEG-OPEN. See [AliasProbe.deadlineMs] for what
                            // it is made of and why the job needed one at all.
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
                                            refusedShapes,
                                            secondPageCut,
                                            pageUnproven,
                                            onEvent,
                                        )
                                    } finally {
                                        // Whatever ended it — a verdict, a
                                        // throw, the deadline, a shutdown — the
                                        // url is no longer held.
                                        progress.released(url.url)
                                    }
                                }
                            if (ran == null) {
                                if (outcomes.containsKey(url)) {
                                    // CUT LATE, AND THE VERDICT STANDS.
                                    //
                                    // The deadline's rule is that OUR timeout is
                                    // not a fact about somebody's relay — and the
                                    // converse has to hold too: a relay that
                                    // answered the ladder has told us what it is,
                                    // and our clock firing one step later does not
                                    // un-tell us. This branch used to throw that
                                    // verdict away with everything else, and the
                                    // url then aged a whole sweep on a measurement
                                    // the pass had already taken.
                                    //
                                    // **NOT #172, and the measurement that says so
                                    // is worth keeping.** This was the first theory
                                    // for it — the NEG-OPEN is the last step and
                                    // had no wall clock, so a slow relay would lose
                                    // its verdict to the budget every sweep — and
                                    // [FitnessBudgetLiveProbe] refuted it against
                                    // the real relays: the whole job runs 1.1-11.9s
                                    // against a 240s budget, the NEG-OPEN's own
                                    // share never exceeding the 10s idle window it
                                    // already had. #172 was the write loop below.
                                    // This branch is the hole that theory found on
                                    // the way, which is real and is now shut.
                                    cutLate.incrementAndGet()
                                } else {
                                    // NO VERDICT IS WRITTEN. Our instrument gave up
                                    // before the dial proved anything; that is not a
                                    // fact about the relay, and publishing one would
                                    // sign our own timeout as its grade — see
                                    // [AliasProbe.deadlineMs]. It arrives at the next
                                    // pass exactly as it arrived at this one.
                                    if (abandoned.size < MAX_ABANDONED_NAMED) abandoned += url.url
                                    abandonedCount.incrementAndGet()
                                }
                            }
                        }
                        // FROM THE JOB'S COMPLETION, for the reason the other
                        // two passes carry: this url is behind the pass
                        // however it ended, and both of the early returns
                        // above are verdicts a reader is waiting on as much as
                        // a `prime` is.
                    }.invokeOnCompletion { progress.attempted() }
                }
            }

            // THE BATCH GUARD, AND IT IS THE LAST THING BETWEEN A BROKEN
            // INSTRUMENT AND THE NETWORK.
            //
            // Every rule above is per url, and the failure this exists for is
            // not: when our own dialling breaks, it breaks for ALL of them at
            // once, and a pass that judged each url honestly on its own still
            // signs one wrong verdict per url. Measured on staging — 3,945
            // relays graded `silent` in a single pass, of which a re-dial found
            // more than half answering in under two seconds, most with an
            // immediate CLOSED.
            //
            // A relay network does not go dark in one pass. So a batch where
            // that share of the dials came back with NOTHING is a fact about
            // this router, and the honest thing to do with it is to publish
            // none of it — including the verdicts that look fine, because the
            // same broken socket layer produced those too.
            //
            // Floored at [GUARD_FLOOR] urls: on a handful of candidates one
            // dead host is a large share and means nothing.
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
                    refusedShapes = refusedShapes.get(),
                )
                return downloaded.get()
            }

            // The writes, serial and after the dials: the record edit is a
            // read-modify-write with no CAS, and this pass is its only writer
            // for these tags — see [AliasMonitor] for why passes never overlap.
            //
            // EACH WRITE UNDER ITS OWN WALL CLOCK, because this loop is the one
            // stretch of the monitor plane that had none — the dials above are
            // bounded per url by [AliasProbe.deadlineMs], and these writes were
            // bounded by nothing but the store answering. The store's HTTP
            // client carries NO read deadline by design (an unlimited query may
            // take as long as it takes), so a store request whose response
            // never comes suspends its caller FOREVER — and a suspended
            // coroutine holds no thread, so nothing anywhere names the fault.
            // Measured on `vespa-eventstore-staging` (#165): a 13,560-url pass
            // finished every dial in 40 minutes, then sat in `measuring
            // fitness` for TEN HOURS with `attempted == toProbe`, no thread in
            // any monitor frame and the process nearly idle. And because the
            // sweep holds [AliasMonitor]'s pass gate while it runs, the one
            // suspended write also stopped every future sweep AND the fast
            // lane — `passesRun: 0` for the life of the process, no relay
            // re-graded again, masked by quartz's passive record refresh
            // keeping verdict ages looking healthy.
            //
            // A write the clock cuts is NOT retried and NOT mourned: the old
            // record stands, the url reads as it did before this pass, and the
            // next sweep re-earns the verdict — the same bargain an abandoned
            // dial gets. And it is held/released like a dial, so the ten
            // invisible hours become one nameable url on one nameable stage.
            var published = 0
            var declined = 0
            var skipped = 0
            var wedgedRun = 0
            var wedgedTotal = 0
            // …AND WHAT THEY ACTUALLY COST, which is the thing being bounded —
            // see [PUBLISH_WEDGE_BUDGET_MS]. Measured rather than assumed from
            // [publishDeadlineMs]: a write can be cut by a deadline that later
            // becomes adaptive, and a budget computed from a constant would
            // then be counting something other than the time it spent.
            var wedgedMs = 0L
            // Which limit ended the batch, for the report — the two say
            // different things about the store and want different answers.
            var stoppedBy: String? = null
            // WHAT THIS PASS DID NOT TEST, AND THEREFORE MAY NOT RE-SIGN.
            //
            // A measured verdict is always written: the point of dialling is to
            // put down what the relay did, and the stamp that rides with it is
            // true. An INHERITED one is different — the two free refusals reach
            // here having cost no socket, and stamping `measured-at = now` on
            // them claims a measurement this pass did not make. The stamp is
            // the whole mechanism by which a verdict ages
            // ([RelayVerdictRecord.current]), so refreshing what we did not
            // test is how an untested verdict becomes immortal.
            //
            // So an inherited verdict is written only when it would CHANGE
            // something: the record does not carry that grade, carries a
            // different one, or carries it under a superseded epoch or aged
            // past the TTL. Skipping the rest costs one chunked read in place
            // of thousands of read-modify-writes — measured on staging, 6,192
            // of 20,075 records (31%) are `alias` or `inconsistent`, so this is
            // also most of a third of the loop that gets cut.
            //
            // A read that FAILS is not "nothing is on the record": that would
            // skip writes the record needs. It falls back to writing
            // everything, which is exactly the behaviour this replaces.
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
            // WHERE THIS BATCH PICKS UP, AND WHY THE ORDER IS NOT THE MAP'S.
            // THIS IS #172.
            //
            // Both wedge limits below END the loop, and the verdicts after the
            // break are dropped. `outcomes` is a [ConcurrentHashMap], so
            // iterating it walks hash order — arbitrary, and STABLE across
            // passes for a stable set of urls. Every cut batch therefore
            // dropped the SAME tail, pass after pass, and "measured again next
            // pass" was true of the urls at the front and false forever of the
            // ones at the back: a healthy relay whose url happened to hash
            // late could never be re-graded, however often the pass ran.
            //
            // MEASURED, on the 20,075 graded records
            // `search-staging.brainstorm.world` was serving — see
            // [WriteOrderForensicProbe], which rebuilds this very map from them
            // and walks it. Replayed in bucket order the verdict ages are a
            // staircase, not a spread: positions 0-12,043 stamped 1.2-1.5h ago,
            // 12,043-15,555 stamped 29.1-29.9h ago, 15,555-20,072 stamped
            // 72.3h ago — each cohort a CONTIGUOUS slice, ages decreasing
            // across it exactly as a loop stamping `now` as it walks would
            // leave them. 735 crossings between written and not where a
            // per-url cause predicts ~9,700. Three sweeps, each starting at
            // position zero, each reaching less far than the last (20,072 ->
            // 15,555 -> 12,191), and the 7,881 urls past the newest cut had
            // gone un-regraded for three days.
            //
            // The share was the same in every grade — `prime` 55.8% fresh,
            // `dead` 57.7%, `alias` 61.8%, `silent` 57.7% — which is what
            // rules out the relay having anything to do with it.
            //
            // So the order is the url, and the batch resumes where the last one
            // stopped. A wedge that costs this pass the tail costs the NEXT
            // pass nothing — those urls are its head — and every url is reached
            // within a bounded number of passes rather than never. In-memory
            // and deliberately not persisted: a restart starts at the top,
            // which is the same guarantee from a different offset.
            val order = outcomes.keys.sortedBy { it.url }
            val resumeFrom = writeCursors[label]?.let { c -> order.indexOfFirst { it.url >= c } } ?: 0
            // -1 is a cursor sorting past everything this batch holds, which
            // wraps to the top exactly as index 0 does.
            val rotated = if (resumeFrom <= 0) order else order.subList(resumeFrom, order.size) + order.subList(0, resumeFrom)
            // Cleared up front so a pass that throws between here and the loop's
            // end cannot leave the next one resuming at a url this batch never
            // reached.
            writeCursors.remove(label)
            for (url in rotated) {
                val outcome = outcomes[url] ?: continue
                // NOT TESTED AND NOT CHANGED — see the block above. Counted, so
                // the funnel still divides and a reader can see the loop got
                // shorter rather than the corpus getting smaller.
                // …and the EVIDENCE has to match too, not just the grade. A
                // url that re-folds onto a different canonical is `alias` both
                // times and is not the same statement: skipping on the grade
                // alone would leave the record naming a url the fold no longer
                // elects.
                if (!outcome.tested && standing[url]?.let { it.value == outcome.verdict.value && it.evidence == outcome.evidence } == true) {
                    skipped++
                    continue
                }
                progress.holding(url.url, STAGE_PUBLISH)
                // Three-valued on purpose: `true` is a stored record, `false`
                // is the store ANSWERING and the write still failing (a throw
                // [RelayVerdictRecord.edit] caught, or no record coming back
                // signed), `null` is the deadline — no answer at all. The
                // first cut collapsed the middle case into "published", so a
                // store failing every write PROMPTLY reported a clean pass:
                // the fast-fail shape of the very outage this loop exists to
                // make loud.
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
                        // A success ends a run: the wedge this guards fails
                        // every write the same way, so a write going THROUGH
                        // is proof the wall is not there — see
                        // [PUBLISH_WEDGE_LIMIT] for why the limit is
                        // consecutive and not a tally.
                        wedgedRun = 0
                    }

                    // The store spoke and the write still failed. Counted and
                    // reported, but no reason to stop: a prompt failure costs
                    // nothing per verdict, and the urls after this one may
                    // write fine.
                    false -> {
                        declined++
                        // …AND IT ENDS A RUN, for the same reason a success
                        // does. The run counter is looking for a store that has
                        // stopped ANSWERING, and a decline is the store
                        // answering — it came back, promptly, and refused.
                        // Without this a store alternating timeout/decline
                        // reached "three consecutive timeouts" having never
                        // timed out twice in a row, and the counter did not
                        // mean what its name says. [PUBLISH_WEDGE_BUDGET_MS]
                        // is what bounds the alternating case, and it still
                        // does.
                        wedgedRun = 0
                    }

                    null -> {
                        // THE WEDGE LIMIT. One timed-out write could be one
                        // slow moment, but each costs the full deadline — so a
                        // store that has stopped answering must not be paid a
                        // minute per verdict, 13,560 times. A run of them on
                        // distinct urls is the store, not the writes: drop the
                        // rest of the batch and let the pass END, loudly, so
                        // the sweep clock keeps running and the fault is a log
                        // line instead of a phase that never moves.
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
                            // AT this url and not after it: the write that
                            // tripped the limit is one of the ones that did not
                            // land, and the limit is a statement about the
                            // store rather than about the url it stopped on.
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
                refusedShapes = refusedShapes.get(),
            )
        } finally {
            progress.finish()
        }
        return downloaded.get()
    }

    /**
     * ONE URL'S WHOLE JOB, extracted so the deadline above has something to
     * wrap and so each step can say which step it is.
     *
     * The steps are unchanged and so is their order — the pre-probe, then the
     * document, then the dial — and only the last of them claims a socket: the
     * NIP-11 ask is an ordinary HTTP call to the same host and has nothing to
     * do with the websocket refcount.
     *
     * [Processors.Handle.holding] is called at each boundary because the steps
     * fail for unrelated reasons and a held url that cannot say which one it is
     * on names half a fault. A suspended coroutine has no stack frame, so this
     * is the only place the answer can come from.
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
        refusedShapes: AtomicInteger,
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
                // OUR instrument, not the relay: the pre-probe throwing says
                // this box could not ask, which is not evidence about the
                // server. Was `dead`. See [measure]'s unmeasured branch.
                unmeasured[url] = "the reachability probe itself threw ${e.javaClass.simpleName}"
                return
            }
        if (!reachable) {
            outcomes[url] = Outcome(Verdict.DEAD, "no TCP answer at the pre-probe")
            return
        }
        // The relay's own account of itself, and the handshake that carries it
        // — asked BEFORE the socket is claimed, because it is an ordinary HTTP
        // call to the same host and has nothing to do with the websocket
        // refcount.
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
                    refusedShapes = refusedShapes,
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
            // Same rule: a throw on our side of the socket is our instrument
            // giving up. [ConsistencyPass.Unmeasured.FAILED] has always read it
            // that way; this published `silent` about the relay instead.
            //
            // …UNLESS THE LADDER HAD ALREADY SETTLED ONE. `settled` records the
            // verdict mid-job now, so a throw in the steps after it would put
            // this url in `outcomes` AND `unmeasured` at once — published, and
            // simultaneously counted as a dial that came back with nothing.
            // That is a contradiction in the report and, worse, a url added to
            // the batch guard's blind share on a dial that answered, which
            // pushes a healthy pass towards refusing to publish ANY of itself.
            if (!outcomes.containsKey(url)) {
                unmeasured[url] = "the dial threw ${e.javaClass.simpleName} before the relay said anything"
            }
        } finally {
            sockets.release(url)
        }
    }

    /**
     * The dial itself: the ask ladder for "answers", the anchored events for
     * "honours `until`", one NEG-OPEN for "reconciles". Ladder shape follows
     * [AliasProbe.leaderPrint] — bare filter, then the general kinds, then the
     * NIP-29 group rung — but keeps each rung's transport reason, because a
     * verdict has to say WHY where a fingerprint only has to say whether.
     */
    private suspend fun dialVerdict(
        url: NormalizedRelayUrl,
        anchor: Long,
        /**
         * The verdict, HANDED OVER THE MOMENT THE LADDER HAS EARNED IT and
         * before the one step of this job that has no wall clock of its own.
         *
         * See [measure]'s cut-late branch for what it buys. The caller records
         * it exactly as it would record the return value; a job that runs to
         * completion then hands the same verdict back with the NIP-77 fact on
         * it, and the second write replaces the first.
         *
         * Only the `prime` path calls this. Every other outcome is returned
         * from a line with no dial after it, so there is nothing left for a
         * clock to cut.
         */
        settled: (Outcome) -> Unit,
        /** Bumped when the NEG-OPEN's own wall clock fires — see [NIP77_DEADLINE_MS]. */
        negOpenCut: AtomicInteger,
        /**
         * …and when a url answered every rung by REFUSING it — see the climb in
         * this function.
         *
         * Counted because it is a population, not an oddity: 46 of 229 hosts on
         * one sweep. The grade those urls carry rests on the relay having
         * answered rather than on anything measured, and an operator watching
         * this land needs to see how much of the corpus that is.
         */
        refusedShapes: AtomicInteger,
        /**
         * …and when the SECOND PAGE's does — see [COMPLIANCE_BUDGET_DIVISOR].
         *
         * Counted for [negOpenCut]'s reason and one degree more sharply: this
         * clock CAN cost a check the verdict depends on, because a url whose
         * second page never came back is graded on the ladder's one page —
         * which is precisely the state #187 is about. A pass silently cutting
         * every one of them would publish `prime` across a corpus it had
         * stopped walking, and that looks from the outside exactly like a
         * corpus that passed.
         */
        secondPageCut: AtomicInteger,
        /**
         * …and the urls that end with NO `pageable` claim at all: nothing came
         * back to page from, or page two never answered.
         *
         * Not a refusal and not a fault — see the branch that bumps it. It is
         * counted because it is the number that used to be published as
         * `pageable: true`, and an operator watching this land needs to see how
         * much of the corpus was making a claim on no evidence.
         */
        pageUnproven: AtomicInteger,
        onEvent: suspend (Event) -> Unit,
    ): Outcome? {
        var lastReason: String? = null
        var answered: AliasProbe.Window? = null
        var shape: List<Int>? = null
        var readMs: Long? = null
        // A relay that answered every rung by refusing it — see the climb below.
        var refusedEveryShape: AliasProbe.Window? = null
        for (rung in listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)) {
            val window = probe.window(url, anchor, rung, onEvent)
            if (window.authRefused) {
                return Outcome(
                    Verdict.AUTH_REFUSED,
                    "asked for NIP-42 and rejected our key",
                    // The one requirement this router can state from
                    // MEASUREMENT rather than from the relay's own word.
                    authRequired = true,
                )
            }
            if (window.ids != null) {
                // AN EMPTY WINDOW IS NOT ALWAYS A DRAIN, and the ladder used to
                // treat it as one. `dialVerdict` stopped on ANY non-null
                // window, so a relay answering the bare rung with `CLOSED
                // blocked: can't handle empty filters` — 46 of 229 hosts, 892
                // urls, on the sweep [AliasProbe.leaderPrint] records — was read
                // as "holds nothing" and graded `prime` on no window at all.
                // Nothing about it was measured: not its cursor, not its
                // compliance, not its read latency.
                //
                // [AliasProbe.Window.drained] is the signal that was missing,
                // and it says the relay EOSEd. An empty window that did not is
                // this SHAPE being refused, so the ladder climbs — which is what
                // `leaderPrint` has always done and what this loop's own comment
                // claimed it followed.
                if (window.ids.isNotEmpty() || window.drained) {
                    answered = window
                    shape = rung
                    readMs = window.firstPageMs
                    break
                }
                // Kept so a relay that refuses EVERY shape is still graded on
                // having answered — see the branch below. It is the last rung's
                // refusal, which is as good as any: none of them is a window.
                refusedEveryShape = window
                continue
            }
            lastReason = window.reason ?: lastReason
            // Two silent rungs in a row is a url that is not there — the same
            // early exit [AliasProbe.leaderPrint] makes, for the same Tor-window
            // arithmetic.
            if (rung == AliasProbe.FALLBACK_KINDS && lastReason != null) break
        }

        // REFUSED EVERY SHAPE THIS PASS KNOWS, and that is still not a reason
        // to take the relay out of the fan-out.
        //
        // [Verdict.RESTRICTED] describes exactly this and is still left with no
        // path to it, deliberately: the mirror's own asks carry 141 kinds and 3,
        // neither of which is a rung here, so a relay declining `kinds=[1]` may
        // serve `contentViaOutbox` perfectly well. Refusing it on the strength
        // of three shapes we happen to probe with would cost real data to make a
        // grade tidier — the same trade the silence branch below refuses at
        // greater length.
        //
        // So it grades on what it DID do: it answered. The evidence says the
        // grade rests on that and not on a window, and no `pageable` or
        // `compliant` fact rides with it, because there was nothing to measure.
        if (answered == null && refusedEveryShape != null) {
            answered = refusedEveryShape
            refusedShapes.incrementAndGet()
        }

        if (answered == null) {
            // Never spoke, or refused every shape. The transport's own words
            // separate the two — [Silence] makes that call in one place.
            return when (val cause = Silence.of(lastReason)) {
                Silence.TIMEOUT, Silence.RATE_LIMITED, Silence.UNKNOWN -> {
                    if (lastReason == null) {
                        // NOTHING CAME BACK AT ALL. No EOSE, no CLOSED, no
                        // transport failure — every rung's window simply
                        // lapsed, which is [Verdict.SILENT] word for word.
                        //
                        // This branch published `restricted` for eleven
                        // months, and the two were swapped: `restricted` means
                        // the relay ANSWERED and none of the answers was a
                        // window, so it needs a terminal reason to be true and
                        // this is the one case that has none. Measured on
                        // `quietplace.xyz`, which accepts a socket, serves a
                        // NIP-11 document, and then answers no REQ and no
                        // NEG-OPEN ever: it was being published to the whole
                        // network as a relay with a narrow query policy.
                        //
                        // [Verdict.RESTRICTED] is left with NO PATH TO IT, and
                        // that is the honest state rather than a regression to
                        // hide: the case it describes — a relay that answers
                        // only shapes we cannot send — currently grades
                        // `prime`, because a CLOSED refusal makes the relay
                        // "speak" and an empty window is deliberately read as a
                        // drain rather than as a refusal. Telling those two
                        // empties apart needs a signal `AliasProbe.Page` does
                        // not carry, so it is a change to the probe and not to
                        // this line. Manufacturing a path here out of an
                        // unrecognised transport string would only publish
                        // OUR socket failing as the relay having a query
                        // policy, which is the same class of mistake this
                        // branch is being fixed for.
                        // NOT A VERDICT, AND THIS IS THE LINE THAT COST 3,945
                        // RELAYS. Every rung's window lapsed with no EOSE, no
                        // CLOSED and no transport word — which is precisely the
                        // state our own socket layer produces when it is the
                        // thing that is broken, and is indistinguishable from
                        // the relay's silence from in here. A pass whose dials
                        // were failing published `silent` about thousands of
                        // relays that answer a REQ in under two seconds, and
                        // each of those verdicts took its url off every roster.
                        //
                        // So this is [AliasProbe.deadlineMs]'s rule, one branch
                        // over: NOTHING IS PUBLISHED when the instrument
                        // returned nothing. The url is counted, named, and
                        // measured again next pass — see [measure]. A relay
                        // that genuinely never answers costs one re-dial a
                        // pass, which is the cheaper of the two mistakes by
                        // several orders of magnitude.
                        null
                    } else {
                        // A reason we could not place. Still silence — and note
                        // it can only be a TRANSPORT word: quartz reports a
                        // reason with no events exclusively for `cannot:`, so
                        // anything the relay itself said made it "speak" and
                        // never reaches here.
                        Outcome(Verdict.SILENT, cause.reason)
                    }
                }

                else -> {
                    Outcome(Verdict.DEAD, cause.reason)
                }
            }
        }

        // WHAT THE LADDER'S ANSWER DID WITH THE LADDER'S ASK — taken from the
        // rung that answered, which is every event this dial saw: a rung
        // returning null [AliasProbe.Window.ids] delivered no page at all, so
        // there is nothing on the rungs above to add. It is also checked
        // against each PAGE's own cursor rather than the anchor, which the
        // counter this replaced could not do — see [AliasProbe.Compliance].
        val walked = answered.compliance
        val seen = walked.seen

        // THE ANCHOR ITSELF IGNORED — the one cursor fault the first page can
        // prove on its own, and the cheapest to catch: every event came back
        // above the `until` the ask carried, so there is nothing to walk from
        // and no point paying for a second page to find that out.
        //
        // ALL-OR-NOTHING on purpose: a relay that puts SOME events above the
        // cursor still advances it, and it is the compliance check below that
        // has something to say about that.
        if (walked.offWindow > 0 && walked.offWindow == seen) {
            return Outcome(
                Verdict.UNPAGEABLE,
                "every event answered above the `until` it was asked for",
                pageable = false to "${walked.offWindow} of $seen events came back above the anchor — the cursor was ignored",
                // A relay that ignores the cursor still answered, and how fast
                // it did so is a fact about it either way.
                rttReadMs = readMs,
                // ASKED OF THE JUDGE, not asserted. A relay that answered ONE
                // event above the anchor is unpageable by the rule above and
                // below [RelayCompliance]'s own floors, and a hard-coded
                // `compliant false` beside it would publish a claim those bars
                // exist to refuse.
                compliant = factOf(walked),
            )
        }
        val evidence =
            when {
                seen > 0 -> "answered $seen events at a settled anchor"

                answered.drained -> "answered an empty anchored page at a settled anchor"

                // See the climb above: it answered, and none of the answers was
                // a window. The sentence has to say which, or the record reads
                // as a relay that simply holds nothing.
                else -> "answered, and refused every filter shape this pass knows — graded on the answer, not on a window"
            }

        // THE VERDICT THE LADDER EARNED, HANDED OVER BEFORE ANY FURTHER DIAL —
        // and it moved back up here for the reason [measure]'s cut-late branch
        // exists. The second page below can change it, so it was tempting to
        // wait; what waiting cost is that the per-url deadline firing during
        // that ask left the url with NO verdict at all, where it used to keep
        // the one the ladder had already proved. Those urls are counted
        // `abandoned`, `abandoned` feeds the batch guard's blind share, and a
        // slow enough batch could then refuse to publish any of its own
        // verdicts — a paging check costing a pass its output.
        //
        // A later refusal simply replaces this: both paths below return an
        // Outcome the write loop records over it, exactly as the NEG-OPEN's
        // fuller verdict replaces this same handover today.
        settled(Outcome(Verdict.PRIME, evidence, rttReadMs = readMs))

        // THE SECOND PAGE — the whole of #187, and the one ask that answers
        // both open questions at once. See [AliasProbe.pageBelow] for the
        // measurement that produced it: 137 relays the mirror aborted for
        // ignoring the cursor were all graded `prime` AND `pageable: true`
        // here, because the pass had only ever asked ONE page.
        //
        // Bounded, and its own clock. A clock that fires here publishes no
        // `pageable` claim and no refusal — our instrument giving up is not the
        // relay ignoring a cursor, the same rule [AliasProbe.deadlineMs] states
        // one level up — and the verdict above is already down, so it costs a
        // fact rather than a url.
        //
        // ASKED THROUGH THE RUNG THAT ANSWERED, unchanged: a page two of a
        // different shape is not page two of page one, and its drain would
        // prove nothing. See [AliasProbe.pageBelow].
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
        // BOXED, for [Reconciled]'s reason exactly: `withTimeoutOrNull`
        // collapses "the ask returned null" and "the clock fired" into one
        // null, and here those are a relay that went quiet on page two and a
        // budget of OURS being spent. Only the second is this pass's fault and
        // only the second belongs in the count.
        if (floor != null && asked == null) secondPageCut.incrementAndGet()
        val second = asked?.window

        // A CLAIM WE COULD NOT EARN IS NOT PUBLISHED, and this is where the
        // second half of #187 lands: `pageable: true` used to be granted on an
        // empty first page — "empty anchored page, honestly EOSEd", 26% of the
        // 137 — which proves the relay ANSWERS and cannot prove it can be
        // walked. There is nothing to page from, so there is nothing to say.
        //
        // It does not cost the relay its grade. Absence of evidence is not a
        // refusal here for the reason the silence branch above spells out at
        // length, and an empty anchored page is the honest answer of every
        // relay that holds nothing recent.
        if (second == null) pageUnproven.incrementAndGet()

        val pageable =
            when {
                second == null -> {
                    null
                }

                // A DRAIN, and the strongest of the three answers: a relay
                // ignoring the cursor would have served its newest events
                // again, and this one served nothing at all. The walk
                // terminates here, which is what pageable means.
                second.seen == 0 -> {
                    true to "page two below $floor drained — the walk terminates"
                }

                // THE FAULT THE MIRROR ABORTS ON, now caught before the roster
                // rather than after a visit. Every event of page two came back
                // above the cursor it was asked under: the walk cannot advance.
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

        // …AND RE-HANDED with the facts the two steps above earned, so a
        // NEG-OPEN that outlives the url's clock leaves the fuller verdict
        // down rather than the bare one. The write loop records the latest.
        settled(Outcome(Verdict.PRIME, evidence, pageable = pageable, compliant = compliantFact, rttReadMs = readMs))

        // One NEG-OPEN against a sliver of the window. A normal return —
        // however empty — is the relay speaking NIP-77; the dedicated
        // exception is it declining. Anything else proves nothing and writes
        // nothing, so a flaky moment cannot demote a reconciling relay.
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
        // A BUDGET THAT FIRES SAYS SO. Publishing nothing is right — our clock
        // is not the relay declining — but a bound that spends itself in
        // silence is the shape of fault #172 took three diagnoses to find, and
        // every other budget in this pass is already named in the report.
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
     * document claimed.
     *
     * **MEASURED BEATS ADVERTISED, tag by tag**, which NIP-66 names as the
     * expected case — *"Information corresponding to field in a relay's NIP 11
     * document MAY contradict actual values if monitors find that a different
     * policy is implemented than is advertised."* A relay that publishes
     * `auth_required: false` and then rejects our key is not a relay with an
     * open read policy, and a monitor that copied the document across would be
     * signing the relay's mistake under its own name.
     *
     * Only `auth` can currently be measured, and only in the POSITIVE
     * direction — see [Outcome.authRequired] for why a clean read is not
     * evidence of `!auth`. The rest of the limitation block rides through as
     * the claim it is.
     *
     * The two free refusals — a url the fold already called an alias, one the
     * stability gate already refused — reach here having dialled NOTHING this
     * pass, so they carry only [network], which is a property of the url rather
     * than a reading of the relay. Everything else stays absent and therefore
     * gets cleared, which is correct: those tags would otherwise be a rtt and a
     * software version standing as current for a url nothing measured.
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

    /**
     * NIP-66's `n`, from the transport that would actually carry this url.
     *
     * From the URL and not from the document, which cannot know how we reached
     * it — and from the same predicate the dial itself uses, so a record can
     * never say `clearnet` about a url the fan-out sends through Tor.
     */
    private fun network(url: NormalizedRelayUrl): String = if (tor?.routes(url) == true) NETWORK_TOR else NETWORK_CLEARNET

    private fun report(
        label: String,
        candidates: Int,
        byVerdict: Map<Verdict, Int>,
        startedMs: Long,
        abandonedCount: Int,
        abandoned: Set<String>,
        /**
         * How many urls this pass ASKED and learned nothing from — see the
         * unmeasured map in [measure]. On its own line for [abandonedCount]'s
         * reason: these carry no verdict either, so they are absent from the
         * counts by construction, and a pass that measured nothing must not
         * read as a pass that found a clean partition.
         */
        unmeasuredCount: Int,
        /**
         * How many events the dials pulled down on the way to these verdicts.
         *
         * REPORTED, because it was the monitor plane's largest effect on the
         * system and nothing said it out loud: the pass hands every event it
         * receives to ingest, so this number is what the sweep wrote into the
         * store to decide "does it answer" — and everything it writes makes the
         * next sweep's reads slower, the candidate derivation's projection
         * included. See [AliasProbe.over]'s `page`.
         */
        downloadedCount: Int,
        /**
         * How many EARNED verdicts never reached the store — writes the
         * per-write deadline cut, plus the rest of a batch dropped once
         * [PUBLISH_WEDGE_LIMIT] writes had wedged. Zero on the guard's
         * refuse-to-publish path, which has its own louder line: these are the
         * verdicts the pass stood behind and could not put down, and a pass
         * that ends that way must say so or the fault is invisible again.
         */
        unwrittenCount: Int = 0,
        /** …of which this many are writes that actually hit the deadline. */
        wedgedWrites: Int = 0,
        /**
         * …and this many the store answered and still failed — a throw the
         * record edit caught, or an edit that came back unsigned. Its own
         * count because it points the other way: a deadline says the store is
         * not answering, a decline says it is answering and refusing.
         */
        declinedWrites: Int = 0,
        /**
         * How many urls the per-url deadline cut AFTER the dial had earned a
         * verdict — see the cut-late branch in [measure].
         *
         * Apart from [abandonedCount] because the two end differently: an
         * abandoned url is one this pass lost, a cut-late one publishes. It is
         * still worth a number, and a rising one is the shape of #172 coming
         * back: the budget is being spent, and the urls spending it are the
         * slow relays the mirror depends on most.
         */
        cutLate: Int = 0,
        /**
         * The url the next batch's write loop will resume at, when this one
         * stopped early — see the rotation in [measure]. Null when every
         * earned verdict was written.
         */
        resumeAt: String? = null,
        /**
         * Inherited verdicts the loop did NOT re-sign because the record
         * already carries them — see the skip in [measure].
         *
         * Reported because it is a large number that looks like a loss and is
         * not one: on a discovered corpus roughly a third of the batch is a
         * standing alias or refusal, and a reader watching `published` drop by
         * that much has to be able to see where it went.
         */
        skippedWrites: Int = 0,
        /**
         * Which limit ended the batch, in words, or null when it ran to its
         * end — see the write loop in [measure].
         *
         * The two say different things about the store and want different
         * answers from an operator: a run of unanswered writes is a store that
         * has stopped, where a spent time budget is one that is answering
         * SOME of them, slowly, which is load rather than an outage.
         */
        stoppedBy: String? = null,
        /**
         * How many urls had their NEG-OPEN cut by its own wall clock — see
         * [NIP77_DEADLINE_MS].
         *
         * Never a verdict: the grade was earned before that step and is
         * published either way. Reported because every other bound in this pass
         * is, and one that spends itself in silence is the shape of the fault
         * #172 turned out to be.
         */
        negOpenCut: Int = 0,
        /**
         * How many urls had their SECOND PAGE cut by its own wall clock — see
         * [COMPLIANCE_BUDGET_DIVISOR].
         *
         * Reported SEPARATELY from [negOpenCut] and with a sharper line,
         * because the two budgets cost different things. That one costs a fact
         * beside a verdict already earned; this one costs a CHECK the verdict
         * depends on — the url is still graded, on one page.
         */
        secondPageCut: Int = 0,
        /** …and how many ended with no `pageable` claim at all — see the parameter on [dialVerdict]. */
        pageUnproven: Int = 0,
        /** …and how many answered every rung by refusing it — see [dialVerdict]'s climb. */
        refusedShapes: Int = 0,
    ) {
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts" +
                "; $downloadedCount event(s) downloaded",
        )
        // ON ITS OWN LINE, and only when there were any. These urls carry no
        // verdict at all, so they are absent from the counts above by
        // construction — a pass that abandoned a hundred urls would otherwise
        // report a clean partition over the ones it managed to reach. Named,
        // because the whole reason this line exists is that the held url was
        // not nameable from anywhere in the system.
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
        // NOT A LOSS, AND STILL A FACT. These urls ran past the per-url budget
        // with a verdict already in hand, so the verdict was published — the
        // number is here because it is the only warning that the budget is
        // being spent, and the urls spending it are the slowest-answering ones
        // in the corpus (#172).
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
        // NOT A REFUSAL, AND THE NUMBER #187 IS ABOUT. Every one of these used
        // to leave the pass carrying `pageable: true` on no evidence at all.
        // They are still graded — absence of evidence is not a refusal — so
        // this line is how an operator sees the size of what is now honestly
        // unclaimed.
        // A POPULATION, NOT AN ODDITY — 46 of 229 hosts on one sweep. These
        // urls are graded on having answered rather than on a window, which is
        // the honest reading and a thin one, and the number is how an operator
        // sees how much of the corpus it covers.
        if (refusedShapes > 0) {
            System.err.println(
                "router: fitness [$label] — $refusedShapes url(s) refused every filter shape this pass knows and were " +
                    "graded on having answered; no window, so no `pageable` and no `compliant` fact rides with them. " +
                    "They STAY in the fan-out: the mirror's own asks are shapes this ladder does not send",
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
        // NOT A LOSS. The record already says this, and this pass did not dial
        // to find out — so re-signing would have stamped a measurement it did
        // not make. See [RelayVerdictRecord.fitnessGrades].
        if (skippedWrites > 0) {
            System.err.println(
                "router: fitness [$label] — $skippedWrites inherited verdict(s) left standing: the record already carries " +
                    "them and this pass dialled nothing for them, so re-signing would refresh a measurement it did not take",
            )
        }
        // THE PASS ENDED ABNORMALLY, and this line is the difference between a
        // fault an operator can read and the ten silent hours of #165. The
        // verdicts were earned; the store would not take them.
        if (unwrittenCount > 0) {
            val dropped = unwrittenCount - wedgedWrites - declinedWrites
            val parts =
                buildList {
                    if (wedgedWrites > 0) add("$wedgedWrites write(s) hit the per-write store deadline")
                    if (declinedWrites > 0) add("$declinedWrites write(s) failed outright with the store answering")
                    if (dropped > 0) add("the remaining $dropped were dropped rather than paying the deadline each")
                    // ON ITS OWN, never nested under `dropped`. A batch that
                    // stops on its last few urls can have `dropped` come out
                    // zero or negative, and "it stopped" with no "because"
                    // is the half of the line an operator actually acts on.
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
         * TAKE BACK EVERY VERDICT THIS BUILD NO LONGER STANDS BEHIND — the
         * rules-epoch change, applied at the one moment it can happen.
         *
         * A verdict is a measurement, and a measurement means what the
         * procedure that took it meant. Bump [RelayVerdictRecord.FITNESS_EPOCH]
         * in the same commit as a rule change and every record signed under
         * the old one is not a stale reading of the current rule, it is a
         * reading of a different rule that no amount of waiting reconciles.
         *
         * This used to be a check on every READ — `s[4] == FITNESS_EPOCH`,
         * evaluated by every consumer of every record. That put our private
         * versioning scheme in the way of anyone else's records: a standard
         * NIP-66 monitor carries no such element, so no foreign verdict could
         * ever pass, whatever the config said about whose verdicts to trust.
         * The claim is ours to retract, so we retract it, and the read is left
         * to ask the one question it should: does this url hold a verdict.
         *
         * Runs at boot and nowhere else, because that is the only moment the
         * epoch can have changed — the constant is a source edit, and a source
         * edit is a restart. A store walk over our own records, no dials.
         * Candidates re-earn their verdict on the next sweep; a url that has
         * left every relay list does not, which is correct — nothing is
         * measuring it, so nothing should be admitting it either.
         */
        suspend fun retireStaleEpochs(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
        ): Int {
            // PAGED, because the epoch cannot be asked for in the filter.
            //
            // The tag index answers on the label's VALUE, and the epoch lives
            // further along the same tag — so this query returns every record
            // carrying any of our grades and the epoch is decided here, one
            // event at a time. That is the whole corpus of graded urls (12,374
            // on a staging deployment), materialized at boot, inside a
            // `runBlocking` that the roster's first rebuild waits on. Paging
            // bounds what is alive at once to [SCAN_PAGE] rather than to how
            // many relays this router has ever graded.
            val stale = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    tags = mapOf(RelayVerdictRecord.LABEL_TAG to Verdict.entries.map { it.value }),
                ),
                SCAN_PAGE,
                // The MONITOR's own records, not the url round-up's sources —
                // `scan`'s default is the round-up because that is what pages
                // through it most, and this walk of the whole graded corpus
                // filed under it would send an operator to the wrong pass. See
                // [StoreCalls] and `RelayDiscovery.scan`.
                caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            ) { event ->
                val record = event as? RelayDiscoveryEvent ?: return@scan
                // A record with NO grade of ours is not a stale grade, it is
                // somebody else's label that happened to carry one of our
                // values — the tag index answers on the value alone, so the
                // query can return those. Retiring on a null here would edit
                // records we have no verdict on at all.
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
         * TAKE BACK EVERY GRADE STILL WRITTEN ON `s` — the one-time migration
         * off the tag this monitor should never have taken.
         *
         * `s` is the software field everywhere else (see
         * [RelayVerdictRecord.LEGACY_STATUS_TAG]), so a record left carrying
         * `["s", "dead"]` does not merely fail to be read as a grade: it is read
         * as the relay running a piece of software called `dead`, by every
         * NIP-66 consumer including our own stats panel. Every record this
         * deployment has ever signed carries one.
         *
         * Retracting is the whole of it. [RelayVerdictRecord.retireFitness]
         * owns `s`, so the stale grade leaves; the url then reads as unmeasured,
         * which is the state that gets it re-graded — under `l` this time — on
         * the next sweep. Nothing has to be deleted and no operator has to
         * intervene.
         *
         * Runs at boot beside [retireStaleEpochs] and is a store walk with no
         * dials. It stops finding anything once the store holds no pre-migration
         * records, so it costs one empty indexed query per boot forever after —
         * which is the price of not having to remember to delete it.
         */
        suspend fun retireLegacyGrades(
            store: IEventStore,
            record: RelayVerdictRecord,
            author: String,
        ): Int {
            // Paged for [retireStaleEpochs]'s reason: a boot-time walk of our
            // own records must not be bounded by how many of them there are.
            val legacy = mutableListOf<NormalizedRelayUrl>()
            RelayDiscovery.scan(
                store,
                Filter(
                    kinds = listOf(RelayDiscoveryEvent.KIND),
                    authors = listOf(author),
                    // THE OLD BUILD'S VOCABULARY, which is not this one — see
                    // [LEGACY_GRADES]. Querying today's values here missed
                    // every `syncable` record in the store, i.e. the only
                    // admitting grade and the largest group of them.
                    tags = mapOf(RelayVerdictRecord.LEGACY_STATUS_TAG to LEGACY_GRADES),
                ),
                SCAN_PAGE,
                // The monitor's own, for [retireStaleEpochs]'s reason.
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
         * Withdraw a verdict from each of [urls], several at a time.
         *
         * **SERIAL, THIS COST A BLOCKED BOOT.** Both callers run before any
         * pass reads a verdict, and both do a read-modify-write per url — a
         * store query for the current record, a schnorr signature, an insert.
         * Measured against the live corpus that is 17,189 records on the first
         * boot after the grade move, and one round trip at a time it is minutes
         * of a router that has not started mirroring yet.
         *
         * Concurrent WITHIN one pass, and that does not weaken the
         * single-writer rule [AliasMonitor] keeps: every url here is a distinct
         * addressable record, so no two of these edits ever touch the same
         * address. What must not happen is this running BESIDE the fitness
         * pass, which is why both callers stay on the boot path rather than
         * moving to a background job — a retraction racing a re-grade is two
         * writers on one address, and the loser's tags are gone.
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

        /**
         * How many retractions are in flight at once.
         *
         * The work is a store round trip and a signature, not a dial, so this
         * is not [AliasFolding.DEFAULT_DIAL_CONCURRENCY]'s question — nobody
         * else's server is being asked for anything. Bounded all the same
         * because the store is shared with a relay that is serving reads.
         */
        private const val RETIRE_CONCURRENCY = 16

        /**
         * This monitor's grade on a record, told apart from everyone else's
         * labels by its NAMESPACE.
         *
         * `l` is shared vocabulary — the same record carries country and ASN
         * labels from other monitors — so matching on the tag name alone would
         * read a foreign label's third element as our epoch and retire a verdict
         * on the strength of it.
         */
        private fun ourGrade(event: RelayDiscoveryEvent): Array<String>? =
            event.tags.firstOrNull {
                it.size > RelayVerdictRecord.LABEL_NAMESPACE_INDEX &&
                    it[0] == RelayVerdictRecord.LABEL_TAG &&
                    it[RelayVerdictRecord.LABEL_NAMESPACE_INDEX] == RelayVerdictRecord.FITNESS_NAMESPACE
            }

        /**
         * WHAT THE OLD BUILD COULD ACTUALLY HAVE WRITTEN on `s` — every grade
         * in today's vocabulary plus the one word that changed.
         *
         * **Spelled out rather than derived from [Verdict], and that is the
         * whole point.** A migration query built from `Verdict.entries` asks
         * for the vocabulary of the build doing the asking, which is precisely
         * the build whose records do not need migrating. Written that way it
         * silently missed every `["s","syncable"]` record in the store — the
         * admitting grade, and the largest group of them: 1,716 of 4,000 on
         * this deployment. The refusals happened to survive because their
         * spellings did not change, which is what made the miss look like a
         * working migration.
         *
         * A list frozen in source is the correct shape for this: it describes
         * history, so it may only ever GROW, and it must not follow a rename
         * made after the records were signed.
         */
        val LEGACY_GRADES = Verdict.entries.map { it.value } + "syncable"

        /**
         * NIP-66's two network values this router can honestly write. `i2p` and
         * `loki` are in its vocabulary and not here — no transport, so no url on
         * one was ever dialled to write a record about.
         */
        const val NETWORK_CLEARNET = "clearnet"

        const val NETWORK_TOR = "tor"

        /**
         * Events per fitness ask. A verdict needs "answers and pages", which
         * twenty events prove as well as the fold's five hundred — and this
         * pass dials the whole corpus, so the difference is the pass's cost.
         */
        const val FITNESS_TARGET = 20

        /**
         * What share of a url's whole budget the second page may spend — a
         * QUARTER, which at [AliasProbe.WINDOWS_PER_URL] is three idle
         * windows.
         *
         * A divisor rather than a duration, so the bound scales with the
         * transport the way [AliasProbe.deadlineMs] does: a fixed number of
         * milliseconds here would cut every `.onion` the pass measures, which
         * is the exact failure that header exists to warn about. Three windows
         * is generous for ONE REQ — the ladder just established that this relay
         * answers — and a quarter of the job is what a step that can change the
         * verdict is worth paying.
         */
        const val COMPLIANCE_BUDGET_DIVISOR = 4

        /** The NEG-OPEN sliver: one hour is enough to prove the verb. */
        const val NIP77_WINDOW_SECONDS = 3600L

        /**
         * The NEG-OPEN's own idle window, shorter than a transfer's: this is a
         * yes/no about the protocol, and a relay that takes half a minute to
         * say NEG-MSG is answered by the transfer's own fallback anyway.
         */
        const val NIP77_IDLE_MS = 10_000L

        /**
         * …AND THE WALL CLOCK OVER IT, because the window above is not one.
         *
         * A negentropy reconciliation is ROUNDS — NEG-MSG in, NEG-MSG back,
         * until both sides agree — and an idle window is re-armed by every one
         * of them. A relay that keeps answering, slowly, therefore never trips
         * [NIP77_IDLE_MS] and the step has no bound of its own: the same
         * property quartz states for its fetch loop, one verb over, and the
         * reason [AliasProbe.deadlineMs] exists at all.
         *
         * That makes this the one step of a url's job able to spend the WHOLE
         * per-url budget, and it sits last — after the ladder has settled the
         * verdict, so what it spends is taken from a measurement already in
         * hand. Measured against real relays ([FitnessBudgetLiveProbe]) it does
         * not currently come close: 59ms to 10.1s across nine, the ceiling
         * being [NIP77_IDLE_MS] firing on a relay that declines. That is a
         * reading of one afternoon, not a bound — the window is re-armed by
         * traffic, so nothing in it says a busier relay cannot stream for
         * minutes. Three windows: a yes/no about a protocol that has already
         * been answered by the first NEG-MSG, with room for a slow one.
         */
        const val NIP77_DEADLINE_MS = 3 * NIP77_IDLE_MS

        /**
         * Records per page when the boot retractions walk our own corpus.
         *
         * Neither of them can ask its question in a filter — the epoch and the
         * legacy tag are both decided per record — so both read every record
         * carrying a grade and both used to do it in ONE unbounded query, at
         * boot, inside the `runBlocking` the roster's first rebuild waits on.
         * Paging bounds what is alive at once to this rather than to how many
         * relays this router has ever graded.
         */
        const val SCAN_PAGE = 2_000

        /**
         * The steps one url's job passes through, published as a held leg's
         * `stage` — see [Processors.Holding.Held.stage].
         *
         * The pass's own words for its own steps, so a reader can grep from the
         * document to the line that was running. Deliberately not four verdict
         * values: nothing here is ever published about a relay.
         */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_DOCUMENT = "nip-11 document"

        const val STAGE_LADDER = "ask ladder"

        const val STAGE_NIP77 = "neg-open"

        /** …and the one ask below the ladder's window, which is a REQ and not a rung of it. */
        const val STAGE_COMPLIANCE = "second page"

        /**
         * …and the one stage that is not a dial: the record edit that puts a
         * verdict down. Named so the held set can answer "what has this pass
         * been doing for nine hours" when the answer is a store write — the
         * question #165 could not answer from the position, the log, or a
         * thread dump.
         */
        const val STAGE_PUBLISH = "verdict write"

        /**
         * The wall clock on one verdict write — see the write loop in
         * [measure].
         *
         * A minute, sized against the store's own worst honest case rather
         * than its typical one: a write queues on the store's single ingest
         * mutex behind the mirror's bulk commits, and a 20k-event bulk at the
         * measured ~500µs/event holds it for ~10s, so tens of seconds of
         * queueing is legitimate under load and must not cost a verdict. The
         * fault this bounds is not slowness but FOREVER — the store's HTTP
         * client deliberately carries no read deadline, so a response that
         * never comes suspends the caller for the life of the process (#165:
         * ten hours and counting, sweep and fast lane both stopped behind it).
         * Any finite number ends that; a generous one never fires by accident.
         */
        const val PUBLISH_DEADLINE_MS = 60_000L

        /**
         * How many writes IN A ROW may hit [PUBLISH_DEADLINE_MS] before the
         * pass stops publishing the rest of the batch.
         *
         * Each timed-out write costs the full deadline, and the failure this
         * exists for is batch-shaped: a wedged store fails every write the
         * same way, and paying a minute apiece over 13,560 verdicts is nine
         * days of a pass that should have ended in three minutes. Three
         * distinct writes on distinct urls hitting the same wall is the store
         * and not the writes — the same population logic as [GUARD_SHARE], at
         * the other end of the pass.
         *
         * CONSECUTIVE, resetting on every write the store ANSWERS — one that
         * goes through and one it declines alike, since either proves the wall
         * is not there — because
         * that is what the wedge looks like and what ordinary load does not:
         * [PUBLISH_DEADLINE_MS]'s own note says tens of seconds behind the
         * mirror's bulk commits is legitimate, so on a long batch under a
         * load spike a handful of writes can straggle past the deadline with
         * thousands succeeding in between — and a straight tally of three
         * would have dropped every verdict after the third straggler while
         * the store was demonstrably taking writes the whole time.
         */
        const val PUBLISH_WEDGE_LIMIT = 3

        /**
         * …and the ceiling the consecutive rule cannot provide: how much WALL
         * TIME one batch may lose to writes that never came back, before the
         * pass stops publishing regardless of what succeeded in between.
         *
         * The consecutive limit bounds the wedge; it does not bound a store
         * that alternates — timing out every other write clears the run
         * counter each time and would let a 13,560-verdict batch spend up to
         * four and a half days in deadlines while technically making progress.
         *
         * **A DURATION AND NOT A COUNT, because the thing worth bounding is
         * the time and the batch is not a fixed size.** This was twenty
         * timed-out writes, justified in its own words as "twenty minutes
         * lost" — which is the right quantity named through the wrong
         * variable. A count says something different on every corpus: the
         * cost of tripping it is every verdict after the trip, so on the 500
         * urls a fast lane tick carries it is a generous allowance, and on a
         * 20,000-url sweep it lets 0.1% of writes straggling forfeit the other
         * 40% — which is how a limit written for a wedged store came to fire
         * on a merely busy one, every sweep (#172). Twenty minutes is twenty
         * minutes at any batch size, and the arithmetic that used to derive it
         * is now just the default's derivation rather than its definition.
         *
         * Measured, not assumed from [PUBLISH_DEADLINE_MS]: what is being
         * spent is wall time, and it should be counted as wall time.
         *
         * The urls a tripped budget drops are the next pass's HEAD, not its
         * tail — see the rotation in [measure] — so the cost of stopping is
         * bounded in a way it was not when this was written.
         */
        const val PUBLISH_WEDGE_BUDGET_MS = 20L * 60 * 1000

        /**
         * How many abandoned urls a pass names in its log line.
         *
         * A ceiling, not a sample: the point of the line is that the url was
         * unnameable, so it has to carry enough of them to act on while
         * refusing to turn one systemic fault into a page of stderr. The count
         * beside the names is always the whole truth.
         */
        const val MAX_ABANDONED_NAMED = 32

        /**
         * The share of a batch's dials that may come back with NO answer
         * before the whole pass is refused — see the batch guard in [measure].
         *
         * A quarter, and it is deliberately far below the share the incident
         * produced (about half) rather than tuned to just catch it: the number
         * has to be a statement about what a relay network does, not about one
         * outage. Real corpora are full of dead urls, but a dead url ANSWERS —
         * with a refused connection, an NXDOMAIN, a TLS failure — and lands in
         * a verdict. Coming back with nothing at all is our socket layer, and
         * one dial in four failing that way has never been normal here.
         */
        const val GUARD_SHARE = 0.25

        /**
         * …and the batch size below which the guard does not apply. On five
         * candidates, two silent hosts is 40% and means nothing; the guard is a
         * statement about a population.
         */
        const val GUARD_FLOOR = 50
    }
}
