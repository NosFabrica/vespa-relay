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
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

/**
 * Ask every relay the same question twice and drop the ones that answer differently. The pair
 * is asked concurrently over one connection at a week-old anchor, so neither elapsed time nor a
 * filter change can explain a difference. Every undecided url lands in exactly one [Unmeasured] row.
 */
class ConsistencyPass(
    private val consistency: RelayConsistency,
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    /** The monitor's one dial gate, shared with every other pass. */
    private val gate: DialGate = DialGate(DEFAULT_DIAL_CONCURRENCY),
    /** Where each pass reports how far it got. Null for every caller but the monitor. */
    override val progress: Processors.Handle? = null,
    /** The wall clock on one verdict write, which runs after the dial and outside its deadline. */
    private val publishDeadlineMs: Long = VerdictWrites.PUBLISH_DEADLINE_MS,
    private val writeConcurrency: Int = VerdictWrites.WRITE_CONCURRENCY,
    /** Every url the monitor knows, by server: a stand-in may be a url outside this batch. */
    private val world: () -> ServerIndex = { ServerIndex.EMPTY },
    /** Whether the pre-probe lets a url be walked. */
    private val canDial: suspend (NormalizedRelayUrl) -> Boolean = { true },
    /** The socket refcount; without one every walk leaves a websocket behind. */
    private val sockets: Sockets = Sockets.NONE,
) : AliasMonitor.Pass {
    /**
     * Every way a url survives a pass with nothing written down. Exactly one is assigned to every
     * url attempted and not decided, so the counts sum to `unmeasured`.
     */
    enum class Unmeasured(
        val reason: String,
    ) {
        /** No Tor for a `.onion`, or nothing listening. */
        TRANSPORT("declined by our own transport"),

        /** Dialled, and nothing came back through any filter. */
        SILENT("never answered a REQ"),

        /** One of the concurrent pair answered and the other did not; itself a finding. */
        ONE_SIDED("answered one of the two asks, not both"),

        /** NIP-42 came back rejected, or the relay went on demanding auth we cannot satisfy. */
        AUTH_REFUSED("refused our auth"),

        /** It answered with nothing, to both the bare filter and the kinds fallback. */
        FILTER_REFUSED("answered, but served no filter we know"),

        /** A real window, under [RelayAliases.DEFAULT_MIN_SAMPLE] events. */
        TOO_THIN("too few events to judge on"),

        /** The probe threw. Ours to fix, never a claim about the relay. */
        FAILED("the probe failed mid-walk"),

        /** The job ran out its wall clock before any answer. */
        ABANDONED("gave up at the per-url deadline"),

        /** Not dialled: another url of its server stands in until the fold decides, as in fitness. */
        WAITING("waits on the fold behind another url of its server"),
    }

    /** One url's outcome: the reason and, for [Unmeasured.SILENT] only, the cause underneath it. */
    private data class Finding(
        val reason: Unmeasured,
        val cause: Silence? = null,
    ) {
        /** What the row is called, and what it refines. */
        val label: String get() = cause?.reason ?: reason.reason

        val parent: String? get() = cause?.let { reason.reason }
    }

    /**
     * Measure the urls nothing is known about yet and publish what that proves. Returns how many
     * new verdicts were reached. Run in the monitor's sequence so two writers never share a record.
     */
    override suspend fun measure(
        kind: RunKind,
        candidates: List<NormalizedRelayUrl>,
    ): Int {
        if (candidates.isEmpty()) return 0
        val label = kind.label
        val startedMs = System.currentTimeMillis()
        // The other urls on each candidate's server, read with it: a fast-lane batch holds only what is new.
        val mates = world().matesOf(candidates)
        val held = readStanding("stability", label) { record.load(candidates + mates) } ?: return 0
        // A folded url is never walked; the monitor hands every pass the raw pre-fold list.
        val folded = held.aliases.keys

        val open = candidates.filter { it !in folded && verdictOf(it, held) == null }
        // A path the fold has not decided waits behind its server's stand-in, the rule fitness keeps:
        // otherwise every path of one server is paired-walked, over Tor a socket for minutes each.
        val waiting = FoldReading.heldAmong(held, open, mates)
        val wanted = open.filter { it !in waiting }
        val waitingFindings = waiting.keys.associateWith { Finding(Unmeasured.WAITING) }
        if (wanted.isEmpty()) {
            // Reported, not skipped: "nothing left to measure" is the state the gate works towards.
            report(label, candidates, held, emptyMap(), dialled = 0, decided = 0, unmeasurable = waitingFindings)
            return 0
        }

        progress?.measuring(wanted.size, Processors.UNIT_URL)
        // One anchor for the whole pass.
        val anchor = RelayConsistency.settledAnchor(nowSeconds())
        // One slot per url, written only by its own coroutine; null is a url the deadline cut.
        val outcomes = arrayOfNulls<Outcome>(wanted.size)
        dialEach(
            wanted,
            gate,
            probe::deadlineMs,
            progress,
            before = { i, url ->
                outcomes[i] = declined(url)
                outcomes[i] == null
            },
        ) { i, url ->
            outcomes[i] = measureOne(url, anchor)
        }
        val tally = Tally.of(wanted, outcomes)

        // Outside the dial's deadline and permit: a slow store must not cost a verdict the relay earned.
        progress?.measuring(tally.earned.size, Processors.UNIT_VERDICT)
        val writes =
            writeEach(
                tally.earned.keys.sortedBy { it.url },
                writeConcurrency,
                publishDeadlineMs,
                VerdictWrites.PUBLISH_WEDGE_LIMIT,
                VerdictWrites.PUBLISH_WEDGE_BUDGET_MS,
                progress,
                VerdictWrites.STAGE_PUBLISH,
            ) { url ->
                val e = tally.earned.getValue(url)
                record.publishConsistency(
                    url,
                    consistent = e.consistent,
                    first = e.first,
                    second = e.second,
                    shared = e.shared,
                    score = e.score,
                    anchorDays = RelayConsistency.ANCHOR_LAG_SECONDS / (24 * 60 * 60),
                ) != null
            }
        // Only a verdict the store took is held or counted: one it refused is measured again next pass.
        val landed = writes.stored.associateWith { tally.earned.getValue(it).consistent }
        val decided = landed.size
        val refused = landed.count { !it.value }
        if (writes.published < tally.earned.size) {
            System.err.println(
                "router: $label stability — ${tally.earned.size - writes.published} earned verdict(s) NOT written " +
                    "(${writes.wedged} hit the write deadline, ${writes.declined} failed outright" +
                    (writes.stoppedBy?.let { "; stopped because $it" }.orEmpty()) +
                    "); those urls are measured again next pass",
            )
        }

        val silent = tally.findings
        if (decided > 0 || silent.isNotEmpty()) {
            System.err.println(
                "router: $label stability walked ${tally.walked} of ${wanted.size} url(s) — $decided decided " +
                    "($refused refused as inconsistent), ${silent.size} proved nothing" +
                    breakdown(silent).joinToString(prefix = " (", postfix = ")") { "${it.second} ${it.first.label}" } +
                    ", ${candidates.count { verdictOf(it, held, landed) == false }} url(s) now refused in total " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        if (waiting.isNotEmpty()) {
            System.err.println(
                "router: $label stability — ${waiting.size} url(s) on ${waiting.values.toSet().size} server(s) wait on the fold " +
                    "behind another url of the same server, not walked",
            )
        }
        if (tally.unplaced.isNotEmpty()) {
            System.err.println("router: $label stability could not classify ${tally.unplaced.size} terminal reason(s): " + tally.unplaced.joinToString(" | "))
        }
        report(label, candidates, held, landed, dialled = tally.walked, decided = decided, unmeasurable = silent + waitingFindings)
        return decided
    }

    /** What one url's dial came to. None at all is a url the deadline cut, [Unmeasured.ABANDONED]. */
    private sealed interface Outcome {
        /** A verdict the relay earned, held from the dial to the write. */
        class Earned(
            val consistent: Boolean,
            val first: Int,
            val second: Int,
            val shared: Int,
            val score: Double,
        ) : Outcome

        /** No verdict, and why; [unplaced] is the transport's words where [Silence] could not place them. */
        class Unproven(
            val finding: Finding,
            /** A socket was opened for it; a url the pre-probe held back is not walked. */
            val walked: Boolean,
            val unplaced: String? = null,
        ) : Outcome
    }

    /** One pass's outcomes after the join, read on one coroutine. */
    private class Tally(
        val earned: Map<NormalizedRelayUrl, Outcome.Earned>,
        val findings: Map<NormalizedRelayUrl, Finding>,
        val walked: Int,
        /** Terminal text Silence could not place, sampled so the table grows from real strings. */
        val unplaced: Set<String>,
    ) {
        companion object {
            fun of(
                urls: List<NormalizedRelayUrl>,
                outcomes: Array<Outcome?>,
            ): Tally {
                val earned = HashMap<NormalizedRelayUrl, Outcome.Earned>()
                val findings = HashMap<NormalizedRelayUrl, Finding>()
                val unplaced = LinkedHashSet<String>()
                var walked = 0
                for ((i, url) in urls.withIndex()) {
                    when (val outcome = outcomes[i]) {
                        // Nothing is published about a url the deadline cut: the clock is ours.
                        null -> {
                            walked++
                            findings[url] = Finding(Unmeasured.ABANDONED)
                        }

                        is Outcome.Earned -> {
                            walked++
                            earned[url] = outcome
                        }

                        is Outcome.Unproven -> {
                            if (outcome.walked) walked++
                            findings[url] = outcome.finding
                            outcome.unplaced?.let { if (unplaced.size < MAX_UNPLACED_SAMPLES) unplaced += it }
                        }
                    }
                }
                return Tally(earned, findings, walked, unplaced)
            }
        }
    }

    /**
     * A url's verdict after this pass, true consistent and null none: what landed in [landed], else what
     * [held] read. A refusal read wins over a consistent one.
     */
    private fun verdictOf(
        url: NormalizedRelayUrl,
        held: RelayVerdictRecord.Verdicts,
        landed: Map<NormalizedRelayUrl, Boolean> = emptyMap(),
    ): Boolean? =
        landed[url] ?: when (url) {
            in held.inconsistent -> false
            in held.consistent -> true
            else -> null
        }

    /** The undecided urls by finding, commonest first. */
    private fun breakdown(silent: Map<NormalizedRelayUrl, Finding>): List<Pair<Finding, Int>> = order(silent.values.groupingBy { it }.eachCount())

    /** Findings widest first, ties broken by enum order so a document is stable. */
    private fun order(counts: Map<Finding, Int>): List<Pair<Finding, Int>> =
        counts.entries
            .sortedWith(
                compareByDescending<Map.Entry<Finding, Int>> { it.value }
                    .thenBy { it.key.reason.ordinal }
                    .thenBy { it.key.cause?.ordinal ?: -1 },
            ).map { it.key to it.value }

    /**
     * What this pass reached, where it outlives the log line. The members partition the candidate
     * set: `candidates = foldedAway + consistent + inconsistent + unmeasured`, and `unmeasured` is
     * re-derived after the walk so a silent relay still counts as outstanding.
     */
    private fun report(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        held: RelayVerdictRecord.Verdicts,
        landed: Map<NormalizedRelayUrl, Boolean>,
        dialled: Int,
        decided: Int,
        unmeasurable: Map<NormalizedRelayUrl, Finding>,
    ) {
        val handle = progress ?: return
        var foldedAway = 0
        var consistent = 0
        var inconsistent = 0
        var unmeasured = 0
        for (url in candidates) {
            if (url in held.aliases) {
                foldedAway++
                continue
            }
            when (verdictOf(url, held, landed)) {
                true -> consistent++
                false -> inconsistent++
                null -> unmeasured++
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
                    hosts = byHost.size,
                    // Ranked by host name within a count, so an unchanged network publishes the same document.
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
                // Whole: a reason is an enum value, so the network cannot grow this list.
                undecided = rows,
            ),
        )
    }

    /** One url's pair of answers through one filter. */
    private data class Answers(
        val first: AliasProbe.Window,
        val second: AliasProbe.Window,
    ) {
        val authRefused: Boolean get() = first.authRefused || second.authRefused

        /**
         * How much this attempt proved, more ranking higher: total silence, then one side
         * answering, then the thinner window.
         */
        val depth: Int get() =
            when {
                first.ids == null && second.ids == null -> -2
                first.ids == null || second.ids == null -> -1
                else -> minOf(first.ids.size, second.ids.size)
            }
    }

    /** Both rungs of the filter ladder, which of them the verdict is read from, and what it decided. */
    private data class Ladder(
        val bare: Answers,
        val fallback: Answers?,
        val decided: RelayConsistency.Decided?,
    ) {
        /** The attempt that got furthest. */
        val best: Answers get() = if (fallback != null && fallback.depth > bare.depth) fallback else bare

        /** What the transport said when it gave up; either rung's message describes the same socket. */
        fun saidWhat(): String? =
            bare.first.reason
                ?: bare.second.reason
                ?: fallback?.first?.reason
                ?: fallback?.second?.reason

        /**
         * Why this url ended undecided. Auth first and across both rungs: a refusal explains every
         * thin window under it.
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

    /** The pre-probe, before any dial permit: null when the url is walked, else what it ends as. */
    private suspend fun declined(url: NormalizedRelayUrl): Outcome? {
        progress?.holding(url.url, STAGE_REACHABILITY)
        // Not runCatching: it swallows CancellationException and would file a shutdown as probe failures.
        val reachable =
            try {
                canDial(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                return Outcome.Unproven(Finding(Unmeasured.FAILED), walked = false)
            }
        return if (reachable) null else Outcome.Unproven(Finding(Unmeasured.TRANSPORT), walked = false)
    }

    /** One url's paired walk, so the deadline has something to wrap. */
    private suspend fun measureOne(
        url: NormalizedRelayUrl,
        anchor: Long,
    ): Outcome {
        progress?.holding(url.url, STAGE_LADDER)
        sockets.claim(url)
        val attempt =
            try {
                ladder(url, anchor)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // A walk that threw is our instrument failing, never a verdict about the relay.
                null
            } finally {
                sockets.release(url)
            }
        if (attempt == null) return Outcome.Unproven(Finding(Unmeasured.FAILED), walked = true)
        val answer = attempt.decided
        if (answer == null) {
            val why = attempt.why()
            // The transport's words only under the one reason they explain.
            val cause = if (why == Unmeasured.SILENT) Silence.of(attempt.saidWhat()) else null
            return Outcome.Unproven(Finding(why, cause), walked = true, unplaced = attempt.saidWhat().takeIf { cause == Silence.UNKNOWN })
        }
        // Learned only once the write lands, in [measure].
        return Outcome.Earned(
            consistent = answer.verdict == RelayConsistency.Verdict.CONSISTENT,
            first = answer.first.size,
            second = answer.second.size,
            shared = answer.shared,
            score = answer.score,
        )
    }

    /**
     * The bare filter, then the kinds fallback only when the first proved nothing. An auth
     * refusal ends the ladder: every later ask on that connection waits out the idle window,
     * as a dial that ran out of time does on the next one ([AliasProbe.endsLadder]).
     */
    private suspend fun ladder(
        url: NormalizedRelayUrl,
        anchor: Long,
    ): Ladder {
        val bare = walkPair(url, anchor, null)
        consistency.judge(bare.first.ids, bare.second.ids)?.let { return Ladder(bare, null, it) }
        val timedOut = probe.endsLadder(url, bare.first) && probe.endsLadder(url, bare.second)
        if (bare.authRefused || timedOut) return Ladder(bare, null, null)
        val fallback = walkPair(url, anchor, AliasProbe.FALLBACK_KINDS)
        // The verdict is read from the deeper rung, and an undecided bare pair decides nothing.
        val decided = if (fallback.depth > bare.depth) consistency.judge(fallback.first.ids, fallback.second.ids) else null
        return Ladder(bare, fallback, decided)
    }

    /**
     * Two walks of [url] through the same filter, in flight at the same time. Neither may throw:
     * `async` reports a failure by cancelling its parent, taking every other url down with it.
     */
    private suspend fun walkPair(
        url: NormalizedRelayUrl,
        anchor: Long,
        kinds: List<Int>?,
    ): Answers =
        coroutineScope {
            val walks =
                List(2) {
                    async {
                        // Not runCatching: it would turn a cancellation into a silent walk.
                        try {
                            probe.window(url, anchor, kinds)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (_: Exception) {
                            AliasProbe.Window(null)
                        }
                    }
                }.awaitAll()
            Answers(walks[0], walks[1])
        }

    companion object {
        const val DEFAULT_DIAL_CONCURRENCY = AliasFolding.DEFAULT_DIAL_CONCURRENCY

        /** Distinct unclassified terminal reasons sampled per pass; the count is published in full. */
        const val MAX_UNPLACED_SAMPLES = 3

        /** A held leg's `stage`, in the pass's own words. */
        const val STAGE_REACHABILITY = "pre-probe"

        const val STAGE_LADDER = "paired walk"
    }
}
