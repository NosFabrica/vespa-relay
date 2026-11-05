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

import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.peers.Fact
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** Why a url was set aside with no verdict. */
internal enum class Deferral(
    val why: String,
    /** A server answered, which is evidence our own network works. */
    val serverAnswered: Boolean,
    /** As readily our own side failing as the relay's, so it counts as a blind dial, once per host. */
    val blind: Boolean = false,
    /** The relay's side of the socket ended it, so dialling again at once only adds to its load. */
    val backsOff: Boolean = false,
) {
    SERVER_ERROR("the server answered the upgrade with a 5xx", serverAnswered = true, backsOff = true),
    TOR_DOWN("our Tor proxy is not answering", serverAnswered = false),
    LOOKUP_UNEXPLAINED("the lookup failed or timed out with no reason left to read", serverAnswered = false, blind = true),
    OUR_TLS("the dial failed TLS where our direct handshake succeeded, so our transport did", serverAnswered = false),
    TLS_UNCHECKED("the dial failed TLS and no direct handshake could check it", serverAnswered = false),
    RATE_LIMITED("the relay rate limited us", serverAnswered = true, backsOff = true),
    UNRECOGNISED("the dial gave up in words we do not recognise", serverAnswered = false, blind = true, backsOff = true),
    PARTLY_REFUSED("it refused some shapes and went quiet on the rest", serverAnswered = true, backsOff = true),
}

/** One url's outcome, carrying the measured facts that ride the same record edit. */
internal class FitnessOutcome(
    val verdict: Verdict,
    val evidence: String,
    val pageable: Fact? = null,
    val nip77: Fact? = null,
    /** Did the events match the filter that asked for them. */
    val compliant: Fact? = null,
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

/** Where one dialled url ended: one result at most, and none for a held url its check did not grade. */
internal sealed interface DialResult {
    /** A verdict, written unless a batch guard withholds it. */
    class Graded(
        val outcome: FitnessOutcome,
    ) : DialResult

    /** Set aside, neither a verdict nor our dialling failing; [retryAfterMs] is the wait the relay named. */
    class Deferred(
        val why: Deferral,
        val retryAfterMs: Long? = null,
    ) : DialResult

    /** Asked, and no answer of any kind. Never published. */
    class Unmeasured(
        val why: String,
    ) : DialResult

    /** Cut by the per-url deadline before any verdict: our clock, not a fact about the relay. */
    data object Abandoned : DialResult
}

/**
 * One dialled url's slot, written only by that url's coroutine, except [reading], which only its
 * document child writes. Read once the dials and the documents have joined.
 */
internal class DialSlot(
    val url: NormalizedRelayUrl,
    /** A url waiting on the fold, asked once on a server that answered HTTP; its cut writes nothing. */
    val held: Boolean,
) {
    var result: DialResult? = null

    /** Beside the result, not on it: the document and the REQ fail independently. */
    var reading: RelayDocument.Reading? = null

    /** Events the dials were served. */
    var downloaded = 0

    /** The per-url deadline cut it after the dial had earned a verdict, which publishes. */
    var cutLate = false

    /** The NEG-OPEN was cut by its own wall clock; the grade is already earned. */
    var negOpenCut = false

    /** The second page was cut by its own clock; graded on one page. */
    var secondPageCut = false

    /** Ends with no `pageable` claim at all. */
    var pageUnproven = false

    /** The per-url deadline fired: an earned verdict stands, and whatever else was filed first wins. */
    fun cut() {
        when (result) {
            is DialResult.Graded -> cutLate = true
            null -> result = DialResult.Abandoned
            else -> Unit
        }
    }
}

/** What one batch learned, read off its slots after the joins. */
internal class FitnessBatch(
    /** Urls other passes' standing verdicts refuse, graded at no dial. */
    free: Map<NormalizedRelayUrl, FitnessOutcome>,
    slots: List<DialSlot>,
    /** Urls not dialled because an earlier deferral's backoff has not lapsed. */
    val backingOff: Set<NormalizedRelayUrl>,
) {
    /** Every verdict before the guards: the free refusals, then the dials'. */
    val earned: Map<NormalizedRelayUrl, FitnessOutcome> = free + slots.resultsOf<DialResult.Graded>().mapValues { it.value.outcome }

    /** Set aside for a reason that is neither a verdict nor our dialling failing. Never published. */
    val deferred: Map<NormalizedRelayUrl, Deferral> = slots.resultsOf<DialResult.Deferred>().mapValues { it.value.why }

    /** The wait a deferring relay named, where its words carried one. */
    val retryAfterMs: Map<NormalizedRelayUrl, Long> =
        slots.resultsOf<DialResult.Deferred>().mapNotNull { (url, d) -> d.retryAfterMs?.let { url to it } }.toMap()

    /** Asked, and no answer of any kind. Never published. */
    val unmeasured: Map<NormalizedRelayUrl, String> = slots.resultsOf<DialResult.Unmeasured>().mapValues { it.value.why }

    /** Given up on rather than measured. */
    val abandoned: List<NormalizedRelayUrl> = slots.filter { it.result == DialResult.Abandoned }.map { it.url }

    val readings: Map<NormalizedRelayUrl, RelayDocument.Reading> = slots.mapNotNull { s -> s.reading?.let { s.url to it } }.toMap()

    val downloaded: Int = slots.sumOf { it.downloaded }

    val cutLate: Int = slots.count { it.cutLate }

    val negOpenCut: Int = slots.count { it.negOpenCut }

    val secondPageCut: Int = slots.count { it.secondPageCut }

    val pageUnproven: Int = slots.count { it.pageUnproven }

    /** The backing-off deferrals, and the urls still waiting one out. */
    fun deferring(): Set<NormalizedRelayUrl> = backingOff + deferred.filterValues { it.backsOff }.keys

    /** Urls set aside for [why]. */
    fun deferredFor(why: Deferral): Set<NormalizedRelayUrl> = deferred.filterValues { it == why }.keys

    /** The hosts of [urls]: one host's failure repeats across all of its paths, so it counts once. */
    fun hostsOf(urls: Collection<NormalizedRelayUrl>): Int = urls.mapTo(HashSet()) { RelayAliases.hostOf(it.url) }.size

    /** The hosts of every blind deferral, the share of the batch guard they count for. */
    fun blindHosts(): Int = hostsOf(deferred.filterValues { it.blind }.keys)

    /** Every deferral that is not blind, by why. */
    fun deferredCounts(): Map<String, Int> =
        deferred.values
            .filter { !it.blind }
            .groupingBy { it.why }
            .eachCount()

    private companion object {
        inline fun <reified R : DialResult> List<DialSlot>.resultsOf(): Map<NormalizedRelayUrl, R> = mapNotNull { s -> (s.result as? R)?.let { s.url to it } }.toMap()
    }
}

/** The urls other passes' standing verdicts already refuse, graded at no dial. */
internal fun freeRefusals(
    held: RelayVerdictRecord.Verdicts,
    candidates: List<NormalizedRelayUrl>,
    foldedAway: (RelayVerdictRecord.Verdicts, List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    inconsistent: (RelayVerdictRecord.Verdicts, List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl>,
): Map<NormalizedRelayUrl, FitnessOutcome> {
    val outcomes = LinkedHashMap<NormalizedRelayUrl, FitnessOutcome>()
    val folded = foldedAway(held, candidates)
    for ((alias, canonical) in folded) {
        outcomes[alias] = FitnessOutcome(Verdict.ALIAS, "folds onto ${canonical.url}", tested = false)
    }
    for (url in inconsistent(held, candidates.filter { it !in folded })) {
        outcomes[url] = FitnessOutcome(Verdict.INCONSISTENT, "failed the reproducibility bar; see the consistency tag", tested = false)
    }
    return outcomes
}

/**
 * The batch guard: when our own dialling breaks it breaks for every url at once, so nothing is
 * published, the clean-looking verdicts included. True when it refused, having said so.
 */
internal fun refusesToPublish(
    label: String,
    toDial: List<NormalizedRelayUrl>,
    batch: FitnessBatch,
): Boolean {
    // The share is over the dials that went out: a url set aside before any server could answer is outside it.
    val blindHosts = batch.blindHosts()
    val wentOut = toDial.size - batch.deferred.values.count { !it.serverAnswered } + blindHosts
    val blind = batch.unmeasured.size + batch.abandoned.size + blindHosts
    if (wentOut < FitnessPass.GUARD_FLOOR || blind <= wentOut * FitnessPass.GUARD_SHARE) return false
    System.err.println(
        "monitor: fitness [$label] — REFUSING TO PUBLISH: $blind of $wentOut dial(s) came back with no " +
            "answer at all (${(100.0 * blind / wentOut).toInt()}%, over the ${(100 * FitnessPass.GUARD_SHARE).toInt()}% " +
            "guard). A network does not go dark in one pass — this monitor could not dial. " +
            "${batch.earned.size} verdict(s) dropped unwritten; every url is measured again next pass.",
    )
    return true
}

/** The TLS guard: certificates do not fail together, but our truststore or clock fails them all. Returns what it withholds. */
internal fun withheldTlsDead(
    label: String,
    toDial: List<NormalizedRelayUrl>,
    outcomes: Map<NormalizedRelayUrl, FitnessOutcome>,
    tor: TorTransport?,
): Set<NormalizedRelayUrl> {
    val tlsDead = toDial.filterTo(LinkedHashSet()) { outcomes[it]?.tls == true }
    val tlsDialled = toDial.count { it.url.startsWith("wss://", ignoreCase = true) && tor?.routes(it) != true && outcomes[it]?.preProbe != true }
    if (tlsDead.size <= tlsDialled * FitnessPass.TLS_GUARD_SHARE) return emptySet()
    System.err.println(
        "monitor: fitness [$label] — WITHHOLDING ${tlsDead.size} TLS `dead` verdict(s): our own direct handshake " +
            "failed too on ${tlsDead.size} of $tlsDialled wss dial(s), over the ${(100 * FitnessPass.TLS_GUARD_SHARE).toInt()}% guard. " +
            "That many certificates do not fail at once — this is more likely our truststore or clock. " +
            "They are measured again next pass.",
    )
    return tlsDead
}

/**
 * The dark-network guard: a failure is believed only beside proof that our network reaches
 * others, so a batch where too few did withholds every verdict no server spoke for. Returns what it withholds.
 */
internal fun withheldUnspoken(
    label: String,
    toDial: List<NormalizedRelayUrl>,
    outcomes: Map<NormalizedRelayUrl, FitnessOutcome>,
    batch: FitnessBatch,
): Set<NormalizedRelayUrl> {
    val provedGone = toDial.count { outcomes[it]?.preProbe == true }
    val reached = toDial.count { outcomes[it]?.reachedServer == true } + batch.deferred.values.count { it.serverAnswered }
    if (!looksDark(toDial.size, provedGone, reached)) return emptySet()
    val unspoken = toDial.filterTo(LinkedHashSet()) { url -> outcomes[url]?.let { it.tested && !it.reachedServer } == true }
    if (unspoken.isEmpty()) return emptySet()
    System.err.println(
        "monitor: fitness [$label] — WITHHOLDING ${unspoken.size} `dead`/`silent` verdict(s): $provedGone of " +
            "${toDial.size} dial(s) failed the pre-probe and $reached reached a server. A network does not go dark " +
            "at once — this is more likely our resolver or egress. They are measured again next pass.",
    )
    return unspoken
}

/**
 * Does a batch look like our own network went dark: no dial reached a server, or the pre-probe
 * failed more than [FitnessPass.DEAD_GUARD_SHARE] of it, or, below [FitnessPass.GUARD_FLOOR], more urls than reached one.
 */
internal fun looksDark(
    dialled: Int,
    provedGone: Int,
    reached: Int,
): Boolean =
    reached == 0 ||
        if (dialled >= FitnessPass.GUARD_FLOOR) provedGone > dialled * FitnessPass.DEAD_GUARD_SHARE else provedGone > reached

/**
 * Held urls on the socket a stand-in's pre-probe or handshake proof is about inherit it, since
 * both test the host and port, which every path on them shares.
 */
internal fun inheritedSocketProof(
    heldBack: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    outcomes: Map<NormalizedRelayUrl, FitnessOutcome>,
): Map<NormalizedRelayUrl, FitnessOutcome> {
    val inherited = LinkedHashMap<NormalizedRelayUrl, FitnessOutcome>()
    for ((url, standIn) in heldBack) {
        if (url in outcomes) continue
        val proof = outcomes[standIn]?.takeIf { it.verdict == Verdict.DEAD && (it.preProbe || it.tls) } ?: continue
        val socket = DialTarget.of(url) ?: continue
        if (socket == DialTarget.of(standIn)) inherited[url] = proof
    }
    return inherited
}
