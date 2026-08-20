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
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Ask every relay the same question twice and drop the ones that answer
 * differently.
 *
 * ## The shape, and why it is not the fold's
 *
 * [AliasFolding] compares two DIFFERENT urls and only ever looks at hosts
 * wearing more than one. This compares a url to ITSELF and looks at every url
 * there is — a relay does not need a sibling to be unusable. The two share the
 * probe, the record and the monitor's clock, and nothing else.
 *
 * The pair is asked CONCURRENTLY, over one connection — two REQs in flight at
 * the same instant, so a relay that shuffles per REQ shows it immediately and
 * one whose answers differ only because time passed between them cannot: there
 * is no between. The anchor is a week old on top of that
 * ([RelayConsistency.ANCHOR_LAG_SECONDS]), so neither answer can contain an
 * event the other could not have seen.
 *
 * Both walks ask the SAME filter, which is what the two-stage shape in
 * [walkPair] is for: the bare filter is tried as a pair first, and only a host
 * that refuses it pays a second pair through `kinds=[1]`. Two windows taken
 * through different filters are not evidence of anything.
 *
 * ## What is written down
 *
 * One `self-consistent` tag per measured url on its NIP-66 30166 record, good
 * for [RelayVerdictRecord.DEFAULT_TTL_SECONDS] — a month, after which the relay is
 * measured again and a server that has been fixed comes back into the fan-out on
 * its own. Nothing is written for a url that could not be measured: an
 * unreachable relay and one holding nine events have proved nothing, and a
 * negative verdict costs a relay its place in the mirror.
 *
 * ## Cost
 *
 * Two REQs per url, once a month per url, [concurrency] in flight and NO
 * per-pass total: the budget this used to carry was dropped so that a pass
 * measures its whole set. Everything downloaded goes to the caller's
 * ingest, exactly as [AliasProbe]'s own doc describes — on a stable relay the
 * window was worth having, and on an unstable one the store drops it as
 * already-held. The pass is not a tax on the mirror; it is a sync that also
 * decides.
 *
 * ## What it says about the urls it could NOT decide
 *
 * Everything. A pass over a discovered corpus decides a few hundred urls out of
 * several thousand, and for a long time the other several thousand were one
 * undifferentiated number — which reads as a gate that is not getting anywhere,
 * when most of it is a corpus of dead and unservable urls being re-asked every
 * six hours. [Unmeasured] names the seven ways a url reaches the end of a pass
 * with nothing written down, and [report] publishes them as counts of urls, so
 * `candidates` divides exactly once and a reader can see which fix each slice
 * needs. That partition is the whole point: three of the seven are about us,
 * four are about the far end, and they were indistinguishable.
 */
class ConsistencyPass(
    private val consistency: RelayConsistency,
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    private val concurrency: Int = DEFAULT_DIAL_CONCURRENCY,
    /**
     * Where each pass reports how far it got — see [AliasFolding.progress] for
     * the argument. Null says nothing, which is every caller but the router.
     */
    val progress: Processors.Handle? = null,
    /**
     * The proxy, where there is one — for the GATE alone; see
     * [AliasFolding]'s parameter of the same name and [DialGate].
     */
    tor: TorTransport? = null,
) {
    /** One gate object for every pass this component runs — see [DialGate], and [AliasFolding]. */
    private val gate = DialGate.over(concurrency, tor)

    /** Urls the last [adopt] saw a fold verdict for — never worth measuring. */
    @Volatile
    private var folded: Set<NormalizedRelayUrl> = emptySet()

    /**
     * EVERY WAY A URL SURVIVES A PASS WITH NOTHING WRITTEN DOWN.
     *
     * One of these is assigned to every url the pass attempted and did not
     * decide, so the seven counts sum to `unmeasured` exactly — see [report].
     * They are not interchangeable and that is the reason for having them:
     *
     *  - [TRANSPORT] and [FAILED] are about US. A `.onion` on a router with no
     *    Tor, a host the TCP pre-probe found nothing listening on, our own
     *    socket giving up. No relay has done anything.
     *  - [SILENT], [AUTH_REFUSED] and [FILTER_REFUSED] are about the far end
     *    REFUSING, in three ways that want three different responses: nothing
     *    came back at all, our credentials were turned down, or the server
     *    answered every filter we know with nothing.
     *  - [ONE_SIDED] and [TOO_THIN] are about the far end ANSWERING, just not
     *    enough to judge on. A relay holding nine events is not misbehaving and
     *    must never be refused for it — see [RelayConsistency].
     *
     * The wording is the log line's, and the fold's where the two passes can
     * reach the same finding, so a reader meeting both does not have to work out
     * that they are the same fact.
     */
    enum class Unmeasured(
        val reason: String,
    ) {
        /** Our own transport would not carry it — no Tor for a `.onion`, or nothing listening. */
        TRANSPORT("declined by our own transport"),

        /** Dialled, and nothing came back through any filter. */
        SILENT("never answered a REQ"),

        /**
         * One of the concurrent pair answered and the other did not.
         *
         * Its own bucket rather than [SILENT], because it is the one reason here
         * that is itself a finding: the relay was reachable enough to serve one
         * REQ and not the second one issued at the same instant. That is a
         * capacity or rate-limit story, not an availability one.
         */
        ONE_SIDED("answered one of the two asks, not both"),

        /** NIP-42 came back rejected, or the relay went on demanding auth we cannot satisfy. */
        AUTH_REFUSED("refused our auth"),

        /** It answered — with nothing, to both the bare filter and the kinds fallback. */
        FILTER_REFUSED("answered, but served no filter we know"),

        /** A real window, under [RelayAliases.DEFAULT_MIN_SAMPLE] events. No evidence either way. */
        TOO_THIN("too few events to judge on"),

        /** The probe threw. Ours to fix, and never a claim about the relay. */
        FAILED("the probe failed mid-walk"),

        /**
         * The job ran out its wall clock — see [AliasProbe.deadlineMs].
         *
         * Beside [FAILED] rather than folded into it, and both are ours rather
         * than the relay's. A probe that threw got an answer it could not use;
         * this one never got an answer at all, and the two want different
         * responses: the first is a bug in the walk, the second is a url that
         * would have held the whole pass open before there was a deadline to
         * end it.
         */
        ABANDONED("gave up at the per-url deadline"),
    }

    /**
     * One url's outcome: the reason, and — where the reason has one — the cause
     * underneath it.
     *
     * Only [Unmeasured.SILENT] carries a [Silence] today, because it is the only
     * reason with evidence underneath it: the transport's own message. The rest
     * are already as specific as what we know.
     */
    private data class Finding(
        val reason: Unmeasured,
        val cause: Silence? = null,
    ) {
        /** What the row is called, and what it is a REFINEMENT of — see [report]. */
        val label: String get() = cause?.reason ?: reason.reason

        val parent: String? get() = cause?.let { reason.reason }
    }

    /**
     * Read back what is already known about these urls, WITHOUT dialling.
     *
     * The cheap half, and the one a fan-out runs: one `#d` query per 500 urls,
     * no sockets. Returns the urls to refuse.
     *
     * A store that cannot answer is not a store saying "nothing is wrong" — but
     * it is also not grounds to narrow a fan-out, so a failed read leaves the
     * previous answer standing rather than emptying it. That is the opposite of
     * the fold's `adopt`, and deliberately: there, losing a verdict costs a
     * duplicate dial; here it would cost a relay.
     */
    suspend fun applyVerdicts(candidates: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        if (candidates.isEmpty()) return emptyList()
        adopt(candidates)
        return consistency.unusable(candidates)
    }

    /**
     * Measure the urls nothing is known about yet, and publish what that proves.
     *
     * Signature-compatible with [AliasMonitor.Pass], so the monitor runs this on
     * the same clock and in the same sequence as the fold — which is what keeps
     * two writers off one 30166 record inside this process.
     *
     * Returns how many NEW verdicts were reached, so a pass that decided nothing
     * logs differently from one that never ran.
     */
    suspend fun measure(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit = {},
        sockets: Sockets = Sockets.NONE,
    ): Int {
        if (candidates.isEmpty()) return 0
        val startedMs = System.currentTimeMillis()
        adopt(candidates)

        val wanted = consistency.toProbe(candidates).filter { it !in folded }
        if (wanted.isEmpty()) {
            // NOT SILENT, because "nothing left to measure" is the state this
            // gate is trying to reach and the one an operator most wants to
            // see. Returning without a word left it indistinguishable from a
            // pass that never ran, which is what a monthly TTL looks like for
            // twenty-nine days out of thirty.
            report(label, candidates, dialled = 0, decided = 0, unmeasurable = emptyMap())
            return 0
        }

        // WHAT THIS PASS IS ABOUT TO WALK, published before the first dial —
        // see [Processors.Measuring]. `wanted`, not `candidates`: everything
        // already carrying a verdict was dropped above, and on a settled corpus
        // that is most of it.
        progress?.measuring(wanted.size, Processors.UNIT_URL)
        val decided = AtomicInteger()
        val refused = AtomicInteger()
        // Urls a socket was actually opened for. NOT `wanted.size`, which this
        // used to publish as `dialled` — a url [canDial] held back costs no
        // connection, and counting it made the number a claim about work that
        // never happened.
        val walked = AtomicInteger()
        // The urls that proved nothing AND WHY. Bounded by the candidate set.
        val silent = ConcurrentHashMap<NormalizedRelayUrl, Finding>()
        // Raw terminal text [Silence] could not place, bounded and distinct.
        // Sampled to stderr rather than published: the table is extended from
        // real strings, and until it is, the count is the honest answer and the
        // sample is how the next person fixes it.
        val unplaced = ConcurrentHashMap.newKeySet<String>()
        // ONE anchor for the whole pass, a week behind the clock. Shared for the
        // same reason the fold shares one per group — two urls measured from
        // different anchors are not comparable — though here it matters less,
        // since each url is only ever compared to itself.
        val anchor = RelayConsistency.settledAnchor(nowSeconds())

        coroutineScope {
            for (url in wanted) {
                launch {
                    gate.withPermit(url) {
                        // THE DEADLINE, AND IT IS INSIDE THE PERMIT — see
                        // [AliasProbe.deadlineMs] for what it is made of, and
                        // [FitnessPass.measure] for why it cannot go around the
                        // `launch`: out there it would be timing the wait for one
                        // of `concurrency` permits, which is this pass's own shape
                        // and no relay's fault.
                        val ran =
                            withTimeoutOrNull(probe.deadlineMs(url)) {
                                try {
                                    measureOne(url, anchor, canDial, onEvent, sockets, walked, decided, refused, silent, unplaced)
                                } finally {
                                    // However it ended — a verdict, a throw, the
                                    // deadline, a shutdown — the url is no longer
                                    // held.
                                    progress?.released(url.url)
                                }
                            }
                        // NOTHING IS PUBLISHED about a url the deadline cut. The
                        // clock is ours and the verdict would be about the relay
                        // — see [Unmeasured.ABANDONED].
                        if (ran == null) silent[url] = Finding(Unmeasured.ABANDONED)
                    }
                    // FROM THE JOB'S COMPLETION, not from a counter inside the
                    // body: this url is behind the pass however it ended, and
                    // the body ends five ways — the transport guard throwing,
                    // the guard refusing, the probe failing, an answer that
                    // decided nothing, and a verdict. Four of those are early
                    // returns, so an increment at the bottom would count only
                    // the urls that worked and a position that stops counting
                    // failures reads as a pass slowing down.
                }.invokeOnCompletion { progress?.attempted() }
            }
        }

        if (decided.get() > 0 || silent.isNotEmpty()) {
            System.err.println(
                "router: $label stability walked ${walked.get()} of ${wanted.size} url(s) ? ${decided.get()} decided " +
                    "(${refused.get()} refused as inconsistent), ${silent.size} proved nothing" +
                    // WHY, widest first. One "said too little to judge" covered
                    // a dead corpus, an auth wall and a thin relay alike.
                    breakdown(silent).joinToString(prefix = " (", postfix = ")") { "${it.second} ${it.first.label}" } +
                    ", ${consistency.refusedCount()} url(s) now refused in total " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        // How [Silence]'s table gets extended: from text a real relay produced.
        if (unplaced.isNotEmpty()) {
            System.err.println("router: $label stability could not classify ${unplaced.size} terminal reason(s): " + unplaced.joinToString(" | "))
        }
        report(label, candidates, dialled = walked.get(), decided = decided.get(), unmeasurable = silent)
        return decided.get()
    }

    /** The undecided urls by finding, commonest first — the log's order and the report's. */
    private fun breakdown(silent: Map<NormalizedRelayUrl, Finding>): List<Pair<Finding, Int>> = order(silent.values.groupingBy { it }.eachCount())

    /** Findings widest first, ties broken by the enum order so a document is stable. */
    private fun order(counts: Map<Finding, Int>): List<Pair<Finding, Int>> =
        counts.entries
            .sortedWith(
                compareByDescending<Map.Entry<Finding, Int>> { it.value }
                    .thenBy { it.key.reason.ordinal }
                    .thenBy { it.key.cause?.ordinal ?: -1 },
            ).map { it.key to it.value }

    /**
     * What this pass reached, where it outlives the log line above.
     *
     * **The members are a PARTITION of the candidate set, and that is the whole
     * design of this method**: `candidates = foldedAway + consistent +
     * inconsistent + unmeasured`, and `unmeasured` in turn is the sum of the
     * [Unmeasured] rows. Every url the streams would dial lands in exactly one
     * leaf, so a reader can subdivide the fan-out without the arithmetic
     * silently failing to close — the same rule the pass tallies hold their own
     * numbers to, for the same reason: a breakdown that does not sum is one
     * nobody can act on.
     *
     * The precedence is FOLD FIRST, then a verdict, then nothing — matching what
     * the pass actually does, since a folded url is never measured. A url that
     * folded away after being measured therefore counts as folded here and its
     * stability verdict is not double-counted.
     *
     * `unmeasured` is re-derived AFTER the walk rather than taken as
     * `wanted.size - decided`: a url that was decided is gone from
     * [RelayConsistency.toProbe] and one that proved nothing is still in it, so
     * asking again is the only reading that counts a silent relay as still
     * outstanding — which it is. It is also what makes the number fall to zero
     * when every url carries a current verdict, which is the state the whole
     * gate is working towards and the one nothing could previously show.
     */
    private fun report(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        dialled: Int,
        decided: Int,
        unmeasurable: Map<NormalizedRelayUrl, Finding>,
    ) {
        val handle = progress ?: return
        // ONE PASS over the candidates for the whole partition, and one over the
        // undecided urls for every row's urls AND hosts. It was four passes and
        // then a filter of the whole map per row — seven times over five
        // thousand urls, parsing a host out of each — for numbers a single walk
        // produces.
        var foldedAway = 0
        var consistent = 0
        var inconsistent = 0
        var unmeasured = 0
        for (url in candidates) {
            when {
                url in folded -> foldedAway++
                consistency.isConsistent(url) -> consistent++
                consistency.isInconsistent(url) -> inconsistent++
                else -> unmeasured++
            }
        }
        val urls = HashMap<Finding, Int>()
        val hosts = HashMap<Finding, HashMap<String, Int>>()
        for ((url, finding) in unmeasurable) {
            urls.merge(finding, 1, Int::plus)
            hosts.getOrPut(finding) { HashMap() }.merge(RelayAliases.hostOf(url.url), 1, Int::plus)
        }
        val rows =
            order(urls).map { (finding, count) ->
                val byHost = hosts[finding].orEmpty()
                Processors.Undecided(
                    reason = finding.label,
                    parent = finding.parent,
                    urls = count,
                    // Beside the urls, never instead: the url count closes the
                    // partition, the host count says how many SERVERS those urls
                    // are, and the ranked head says whether they concentrate.
                    hosts = byHost.size,
                    // Ranked by host name within a count, so two passes over an
                    // unchanged network publish the same document.
                    top =
                        byHost.entries
                            .sortedWith(compareByDescending<Map.Entry<String, Int>> { it.value }.thenBy { it.key })
                            .take(Processors.MAX_UNDECIDED_HOSTS)
                            .map { Processors.HostCount(it.key, it.value) },
                )
            }
        handle.record(
            Processors.Work(
                stream = label,
                candidates = candidates.size,
                foldedAway = foldedAway,
                consistent = consistent,
                inconsistent = inconsistent,
                unmeasured = unmeasured,
                dialled = dialled,
                decided = decided,
                // WHOLE. A reason is an enum value in this source, so the
                // network cannot grow this list and there is nothing for a cap
                // to protect — see [Processors.Work.undecidedOmitted] for the
                // two times one was short of its own enumeration.
                undecided = rows,
            ),
        )
    }

    /**
     * One url's pair of answers through ONE filter, and how much they are worth.
     *
     * A pair rather than two loose windows because the two are only meaningful
     * together: [RelayConsistency.decide] compares them, and [depth] ranks one
     * attempt against another when neither could decide — which is what lets the
     * ladder keep the more informative of two failures instead of whichever ran
     * last.
     */
    private data class Answers(
        val first: AliasProbe.Window,
        val second: AliasProbe.Window,
    ) {
        val authRefused: Boolean get() = first.authRefused || second.authRefused

        /**
         * How much this attempt proved, as a single comparable number.
         *
         * Ordered so that MORE EVIDENCE always ranks higher: total silence is
         * the bottom, one side answering beats neither, and past that it is the
         * thinner of the two windows — the depth any verdict would rest on.
         * Without the two negative rungs, a bare filter that produced nothing at
         * all and a kinds filter that produced one real answer would tie, and
         * the reason reported for the url would depend on argument order.
         */
        val depth: Int get() =
            when {
                first.ids == null && second.ids == null -> -2
                first.ids == null || second.ids == null -> -1
                else -> minOf(first.ids.size, second.ids.size)
            }
    }

    /** Both rungs of the filter ladder, and which of them the verdict is read from. */
    private data class Ladder(
        val bare: Answers,
        val fallback: Answers?,
    ) {
        /** The attempt that got furthest — the one a verdict, or a reason, is taken from. */
        val best: Answers get() = if (fallback != null && fallback.depth > bare.depth) fallback else bare

        /**
         * What the TRANSPORT said when it gave up, across both rungs.
         *
         * The first non-null wins rather than the best attempt's, because a
         * window that never spoke is the only kind that carries one at all —
         * there is nothing to rank, and either rung's message describes the same
         * socket.
         */
        fun saidWhat(): String? =
            bare.first.reason
                ?: bare.second.reason
                ?: fallback?.first?.reason
                ?: fallback?.second?.reason

        /**
         * WHY this url ended the pass undecided, read off the evidence rather
         * than guessed at.
         *
         * Auth first and across BOTH rungs: a credential refusal explains every
         * thin window under it, and reading it off [best] alone would report a
         * relay that refused us as one that merely said little.
         */
        fun why(): Unmeasured =
            when {
                bare.authRefused || fallback?.authRefused == true -> Unmeasured.AUTH_REFUSED
                best.depth == -2 -> Unmeasured.SILENT
                best.depth == -1 -> Unmeasured.ONE_SIDED
                best.depth == 0 -> Unmeasured.FILTER_REFUSED
                else -> Unmeasured.TOO_THIN
            }
    }

    /**
     * ONE URL'S PAIRED WALK, extracted so the deadline above has something to
     * wrap and so each step can say which step it is.
     *
     * Unchanged from where it used to sit inline, but for the two
     * [Processors.Handle.holding] calls: a held url that cannot say whether it
     * is in the pre-probe or on the walk names half a fault, and a suspended
     * coroutine has no stack frame to answer from.
     */
    private suspend fun measureOne(
        url: NormalizedRelayUrl,
        anchor: Long,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit,
        sockets: Sockets,
        walked: AtomicInteger,
        decided: AtomicInteger,
        refused: AtomicInteger,
        silent: ConcurrentHashMap<NormalizedRelayUrl, Finding>,
        unplaced: MutableSet<String>,
    ) {
        progress?.holding(url.url, STAGE_REACHABILITY)
        // The pre-probe opens a socket of its own and can throw.
        // NOT `runCatching`, which swallows CancellationException:
        // a pass cancelled at shutdown would record every
        // remaining url as a probe failure on its way out.
        val reachable =
            try {
                canDial(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                silent[url] = Finding(Unmeasured.FAILED)
                return
            }
        if (!reachable) {
            silent[url] = Finding(Unmeasured.TRANSPORT)
            return
        }
        walked.incrementAndGet()
        progress?.holding(url.url, STAGE_LADDER)
        sockets.claim(url)
        val attempt =
            try {
                // The pair, genuinely concurrent over one
                // connection, so "the answer changed" cannot be
                // blamed on elapsed time. It was staged once,
                // with a comment claiming concurrency it did not
                // have. See [ladder] for the filter rungs.
                ladder(url, anchor, onEvent)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // A relay failing mid-walk is [HostStrikes]'
                // business. It is not evidence about how this one
                // answers, so it must not become a verdict.
                null
            } finally {
                sockets.release(url)
            }
        if (attempt == null) {
            silent[url] = Finding(Unmeasured.FAILED)
            return
        }
        val answer = consistency.decide(attempt.best.first.ids, attempt.best.second.ids)
        if (answer == RelayConsistency.Verdict.UNMEASURABLE) {
            val why = attempt.why()
            // The transport's own words, but only under the one
            // reason they explain: a relay that answered thinly
            // said nothing about its socket.
            val cause = if (why == Unmeasured.SILENT) Silence.of(attempt.saidWhat()) else null
            if (cause == Silence.UNKNOWN && unplaced.size < MAX_UNPLACED_SAMPLES) {
                attempt.saidWhat()?.let { unplaced += it }
            }
            silent[url] = Finding(why, cause)
            return
        }
        val first = attempt.best.first.ids!!
        val second = attempt.best.second.ids!!
        consistency.learn(url, answer)
        decided.incrementAndGet()
        if (answer == RelayConsistency.Verdict.INCONSISTENT) refused.incrementAndGet()
        // Written as each url is decided, not at the end of the
        // pass: a pass over a wide fan-out runs for a long time,
        // one url's answer is complete on its own, and a restart
        // in the middle must not throw away what was already
        // proved. Guarded, because these are signed public
        // statements and one failing to write must not take the
        // pass down.
        runCatching {
            record.publishConsistency(
                url,
                consistent = answer == RelayConsistency.Verdict.CONSISTENT,
                first = first.size,
                second = second.size,
                shared = consistency.shared(first, second),
                score = consistency.containment(first, second),
                // The pass's own tuning, handed to the record rather than read
                // out of it — see [RelayVerdictRecord.publishConsistency].
                anchorDays = RelayConsistency.ANCHOR_LAG_SECONDS / (24 * 60 * 60),
            )
        }
    }

    /**
     * The bare filter, then the kinds fallback — each asked as a concurrent
     * pair, and the second one only when the first proved nothing.
     *
     * **An auth refusal ends the ladder here.** See [AliasProbe.Page.authRefused]:
     * measured on `filter.nostr.wine`, the first ask is answered in 1.6s with a
     * refusal and every ask after it on that connection is answered with
     * nothing at all, so each one waits out the full idle window. This used to
     * fall through to the kinds pair regardless — two more REQs into a wall we
     * had already been shown, per url, per pass — because the refusal was
     * flattened into "proved nothing" before the caller could see it.
     */
    private suspend fun ladder(
        url: NormalizedRelayUrl,
        anchor: Long,
        onEvent: suspend (Event) -> Unit,
    ): Ladder {
        val bare = walkPair(url, anchor, null, onEvent)
        val decided = consistency.decide(bare.first.ids, bare.second.ids) != RelayConsistency.Verdict.UNMEASURABLE
        if (decided || bare.authRefused) return Ladder(bare, null)
        return Ladder(bare, walkPair(url, anchor, AliasProbe.FALLBACK_KINDS, onEvent))
    }

    /**
     * Two walks of [url] through the SAME filter, in flight at the same time.
     *
     * **Neither walk may throw out of here, and that is structural rather than
     * defensive.** `async` reports a failure by cancelling its parent, so an
     * exception from one walk would cancel the sibling, the enclosing `launch`
     * and the `coroutineScope` around the whole pass — one unlucky relay taking
     * down every other url's measurement, with the caller's `catch` running far
     * too late to stop it. Catching inside each child keeps the failure a value.
     *
     * A failed walk arrives as a window that never spoke, which is what it is
     * from here: [Unmeasured.FAILED] is reserved for the probe failing outside
     * the pair, where nothing was asked at all.
     */
    private suspend fun walkPair(
        url: NormalizedRelayUrl,
        anchor: Long,
        kinds: List<Int>?,
        onEvent: suspend (Event) -> Unit,
    ): Answers =
        coroutineScope {
            val walks =
                List(2) {
                    async {
                        runCatching { probe.window(url, anchor, kinds, onEvent) }
                            .getOrDefault(AliasProbe.Window(null))
                    }
                }.awaitAll()
            Answers(walks[0], walks[1])
        }

    /**
     * Pull the stored verdicts into memory, forgetting first so the record's TTL
     * means something.
     *
     * A url whose verdict has aged out comes back with no verdict, which is
     * "dial it" — that is how a relay gets re-measured every month and how one
     * that has been fixed rejoins the fan-out without anybody intervening.
     */
    private suspend fun adopt(candidates: List<NormalizedRelayUrl>) {
        val held =
            try {
                record.load(candidates)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Keep what we hold. Emptying the cache on a failed read would
                // put every refused relay back into the fan-out at once, which
                // is the failure this whole pass exists to prevent.
                return
            }
        consistency.replace(candidates, held.consistent, held.inconsistent)
        // Urls a FOLD has already taken out of the fan-out. They will never be
        // dialled, so measuring their stability is budget spent on a question
        // nobody asks: on a polluted store two thirds of a candidate set folds
        // away, and letting them through delayed the survivors' verdicts by
        // several six-hour passes. The monitor hands both passes the same raw
        // pre-fold list — it must, since the fold needs the duplicates to fold
        // them — so the filtering belongs here.
        folded = held.aliases.keys
    }

    companion object {
        /** Urls in flight. Matches the fold's — see [AliasFolding.DEFAULT_DIAL_CONCURRENCY] for the sizing. */
        const val DEFAULT_DIAL_CONCURRENCY = AliasFolding.DEFAULT_DIAL_CONCURRENCY

        /**
         * Distinct unclassified terminal reasons sampled per pass — see
         * [Silence.UNKNOWN]. Three is enough to recognise a pattern and extend
         * the table; the COUNT of urls under it is published in full either way,
         * so nothing is hidden by the cap.
         */
        const val MAX_UNPLACED_SAMPLES = 3

        /**
         * The steps one url's job passes through, published as a held leg's
         * `stage` — see [Processors.Holding.Held.stage]. The pass's own words
         * for its own steps, so a reader can grep from the document to the line
         * that was running.
         */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_LADDER = "paired walk"
    }
}
