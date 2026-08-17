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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.relay.router.progress.Processors
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
 * consistent AND pageable AND readable by us. Five of the refusals below
 * describe relays that are perfectly alive — an alias serves events, an
 * inconsistent relay answers promptly, a cursor-ignoring relay EOSEs every
 * page — which is why the value is not called "live". Slow is not a refusal
 * (rtt is a fact, not a fault), empty is not a refusal (a drain is the relay's
 * honest answer), and a small message cap is not a refusal (a shape the asks
 * respect).
 *
 * The grade is a NIP-32 label under [RelayVerdictRecord.FITNESS_NAMESPACE] and
 * it names the RELAY, not our use of it — see [Verdict] for why that stopped
 * being `syncable`.
 *
 * ## No VERDICT is read off NIP-11; the descriptive fields are
 *
 * Every verdict here comes from what the relay DID: the ask ladder for whether
 * it answers, the anchored walk for whether it honours `until`, one NEG-OPEN
 * for whether it reconciles, quartz's own NIP-42 flow for whether our auth
 * sticks. NIP-11 documents routinely disagree with the relay's own practice —
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
 * This pass owns its own NIP-32 namespace, `pageable`, `nip77` and the
 * descriptive tags in [RelayFacts.OWNED] — and nothing else. The fold's
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
     * Does this url go through Tor? Bound to the same predicate the dial
     * consults, so `n` cannot disagree with the transport that carried the
     * measurement.
     *
     * A function rather than the transport itself: `TorTransport` is internal
     * to the router module and this class is not, and the only thing the
     * question needs is the answer.
     */
    private val routesThroughTor: (NormalizedRelayUrl) -> Boolean = { false },
    private val concurrency: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
) {
    /**
     * The grade vocabulary. [PRIME] is the only admitting value; every refusal
     * is descriptive, so the record explains itself instead of a worker's log
     * line having to.
     *
     * **`prime`, and it used to be `syncable`.** The old word named OUR use of
     * the relay, on a record published for everyone — a crawler, an archiver, a
     * client choosing read relays all want this same composite, and none of them
     * are syncing. A grade names the relay; what the reader does with a prime
     * one is their business.
     */
    enum class Verdict(
        val value: String,
    ) {
        PRIME("prime"),

        /** No TCP, no TLS, no websocket — the transport itself said no. */
        DEAD("dead"),

        /** Connected, then nothing: no EOSE, no CLOSED, the window lapsed. */
        SILENT("silent"),

        /** Works fine, and is another record's relay — syncing it doubles every event. */
        ALIAS("alias"),

        /** Two answers to one question — would poison bands and coverage. */
        INCONSISTENT("inconsistent"),

        /** Ignores `until`: a paged walk against it cannot terminate. */
        UNPAGEABLE("unpageable"),

        /** Requires NIP-42 and turned OUR key down. */
        AUTH_REFUSED("auth-refused"),

        /** Answers only shaped queries this router cannot generally send. */
        RESTRICTED("restricted"),
    }

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
        sockets: AliasFolding.Sockets,
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
        try {
            // THE FREE REFUSALS FIRST. Both are standing verdicts other passes
            // already paid dials for; turning them into a status costs a store
            // read and a record edit, never a socket.
            val folded = foldedAway(candidates)
            for ((alias, canonical) in folded) {
                outcomes[alias] = Outcome(Verdict.ALIAS, "folds onto ${canonical.url}")
            }
            val remaining = candidates.filter { it !in folded }
            val shaky = inconsistent(remaining)
            for (url in shaky) {
                outcomes[url] = Outcome(Verdict.INCONSISTENT, "failed the reproducibility bar; see the consistency tag")
            }

            val toDial = remaining.filter { it !in shaky }
            // WHAT THIS PASS IS ABOUT TO WALK, published before the first dial
            // — see [Processors.Measuring]. The two free refusals above are
            // deliberately outside it: they cost no socket and are all decided
            // before this line, so counting them would open every pass at a
            // position it did not earn and mislead the rate the ETA is drawn
            // from.
            progress.measuring(toDial.size, Processors.UNIT_URL)
            val gate = Semaphore(concurrency)
            // A week back, for the same reason the consistency pass anchors
            // there: an anchored ask against a settled window is the only way
            // "events above the anchor" can mean "ignored the cursor" rather
            // than "new events arrived mid-walk".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            coroutineScope {
                for (url in toDial) {
                    launch {
                        gate.withPermit {
                            val reachable =
                                try {
                                    canDial(url)
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: Exception) {
                                    outcomes[url] = Outcome(Verdict.DEAD, "the reachability probe itself failed: ${e.javaClass.simpleName}")
                                    return@withPermit
                                }
                            if (!reachable) {
                                outcomes[url] = Outcome(Verdict.DEAD, "no TCP answer at the pre-probe")
                                return@withPermit
                            }
                            // The relay's own account of itself, and the
                            // handshake that carries it — asked BEFORE the
                            // socket is claimed, because it is an ordinary
                            // HTTP call to the same host and has nothing to do
                            // with the websocket refcount.
                            document?.read(url)?.let { readings[url] = it }
                            sockets.claim(url)
                            try {
                                outcomes[url] =
                                    dialVerdict(url, anchor) { event ->
                                        downloaded.incrementAndGet()
                                        onEvent(event)
                                    }
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                outcomes[url] = Outcome(Verdict.SILENT, "the dial threw ${e.javaClass.simpleName} before the relay said anything")
                            } finally {
                                sockets.release(url)
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

            // The writes, serial and after the dials: the record edit is a
            // read-modify-write with no CAS, and this pass is its only writer
            // for these tags — see [AliasMonitor] for why passes never overlap.
            for ((url, outcome) in outcomes) {
                record.publishFitness(
                    url = url,
                    status = outcome.verdict.value,
                    evidence = outcome.evidence,
                    pageable = outcome.pageable,
                    nip77 = outcome.nip77,
                    facts = factsOf(url, outcome, readings[url]),
                )
            }

            report(label, candidates.size, outcomes.values.groupingBy { it.verdict }.eachCount(), startedMs)
        } finally {
            progress.finish()
        }
        return downloaded.get()
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
        onEvent: suspend (Event) -> Unit,
    ): Outcome {
        // Events ABOVE the anchor are the relay answering a question it was not
        // asked: the walk's `until` is the anchor, so an honest relay never
        // sends one. Counted across every rung — the shape of the ask does not
        // change what the cursor means.
        var above = 0
        var seen = 0
        val counting: suspend (Event) -> Unit = { event ->
            seen++
            if (event.createdAt > anchor + ANCHOR_SLACK_SECONDS) above++
            onEvent(event)
        }

        var lastReason: String? = null
        var answered: AliasProbe.Window? = null
        var shape: List<Int>? = null
        var readMs: Long? = null
        for (rung in listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)) {
            val window = probe.window(url, anchor, rung, counting)
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
                answered = window
                shape = rung
                readMs = window.firstPageMs
                break
            }
            lastReason = window.reason ?: lastReason
            // Two silent rungs in a row is a url that is not there — the same
            // early exit [AliasProbe.leaderPrint] makes, for the same Tor-window
            // arithmetic.
            if (rung == AliasProbe.FALLBACK_KINDS && lastReason != null) break
        }

        if (answered == null) {
            // Never spoke, or refused every shape. The transport's own words
            // separate the two — [Silence] makes that call in one place.
            return when (val cause = Silence.of(lastReason)) {
                Silence.TIMEOUT, Silence.RATE_LIMITED, Silence.UNKNOWN -> {
                    if (lastReason == null) {
                        Outcome(Verdict.RESTRICTED, "answered no shape this router can send; see the ask ladder")
                    } else {
                        Outcome(Verdict.SILENT, cause.reason)
                    }
                }

                else -> {
                    Outcome(Verdict.DEAD, cause.reason)
                }
            }
        }

        // The pageable fact, from the same events the ladder already paid for.
        // Vacuously pageable when the anchored window is empty: an EOSE on an
        // empty page is a drain, which is exactly how a paged walk terminates.
        if (above > 0 && above == seen) {
            return Outcome(
                Verdict.UNPAGEABLE,
                "every event answered above the `until` it was asked for",
                pageable = false to "$above of $seen events came back above the anchor — the cursor was ignored",
                // A relay that ignores the cursor still answered, and how fast
                // it did so is a fact about it either way.
                rttReadMs = readMs,
            )
        }
        val pageable = true to (if (seen == 0) "empty anchored page, honestly EOSEd" else "$seen events, all at or below the anchor")

        // One NEG-OPEN against a sliver of the window. A normal return —
        // however empty — is the relay speaking NIP-77; the dedicated
        // exception is it declining. Anything else proves nothing and writes
        // nothing, so a flaky moment cannot demote a reconciling relay.
        val sliver = Filter(kinds = shape, since = anchor - NIP77_WINDOW_SECONDS, until = anchor)
        val nip77 =
            try {
                client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
                true to "answered a NEG-OPEN over a ${NIP77_WINDOW_SECONDS / 3600}h window"
            } catch (e: CancellationException) {
                throw e
            } catch (e: NegentropySyncException) {
                false to "declined the NEG-OPEN: ${e.reason}"
            } catch (_: Exception) {
                // No fact: the failure was ours or the moment's, not the relay's.
                null
            }

        return Outcome(
            Verdict.PRIME,
            "answered ${if (seen == 0) "an empty anchored page" else "$seen events"} at a settled anchor",
            pageable = pageable,
            nip77 = nip77,
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
    private fun network(url: NormalizedRelayUrl): String = if (routesThroughTor(url)) NETWORK_TOR else NETWORK_CLEARNET

    private fun report(
        label: String,
        candidates: Int,
        byVerdict: Map<Verdict, Int>,
        startedMs: Long,
    ) {
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts",
        )
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
            val stale =
                store
                    .query<RelayDiscoveryEvent>(
                        Filter(
                            kinds = listOf(RelayDiscoveryEvent.KIND),
                            authors = listOf(author),
                            tags = mapOf(RelayVerdictRecord.LABEL_TAG to Verdict.entries.map { it.value }),
                        ),
                    ).filter { event ->
                        // A record with NO grade of ours is not a stale grade,
                        // it is somebody else's label that happened to carry
                        // one of our values — the tag index answers on the
                        // value alone, so the query can return those. Retiring
                        // on a null here would edit records we have no verdict
                        // on at all.
                        val grade = ourGrade(event) ?: return@filter false
                        grade.getOrNull(RelayVerdictRecord.LABEL_EPOCH_INDEX) != RelayVerdictRecord.FITNESS_EPOCH
                    }.mapNotNull { it.relay() }
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
            val legacy =
                store
                    .query<RelayDiscoveryEvent>(
                        Filter(
                            kinds = listOf(RelayDiscoveryEvent.KIND),
                            authors = listOf(author),
                            // THE OLD BUILD'S VOCABULARY, which is not this
                            // one — see [LEGACY_GRADES]. Querying today's
                            // values here missed every `syncable` record in
                            // the store, i.e. the only admitting grade and the
                            // largest group of them.
                            tags = mapOf(RelayVerdictRecord.LEGACY_STATUS_TAG to LEGACY_GRADES),
                        ),
                    ).mapNotNull { it.relay() }
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
         * How far above the anchor an event may sit before it counts as the
         * relay ignoring the cursor. Publishers' clocks run fast by seconds,
         * not minutes; a relay that ignores `until` answers with its newest
         * events, which sit a WEEK above a settled anchor.
         */
        const val ANCHOR_SLACK_SECONDS = 300L

        /** The NEG-OPEN sliver: one hour is enough to prove the verb. */
        const val NIP77_WINDOW_SECONDS = 3600L

        /**
         * The NEG-OPEN's own idle window, shorter than a transfer's: this is a
         * yes/no about the protocol, and a relay that takes half a minute to
         * say NEG-MSG is answered by the transfer's own fallback anyway.
         */
        const val NIP77_IDLE_MS = 10_000L
    }
}
