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
 * Ask every relay the same question twice and drop the ones that answer differently. The pair
 * is asked concurrently over one connection at a week-old anchor, so neither elapsed time nor a
 * filter change can explain a difference. Every undecided url lands in exactly one [Unmeasured] row.
 */
class ConsistencyPass(
    private val consistency: RelayConsistency,
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    private val concurrency: Int = DEFAULT_DIAL_CONCURRENCY,
    /** Where each pass reports how far it got. Null for every caller but the router. */
    val progress: Processors.Handle? = null,
    /** The proxy, for the gate alone. */
    tor: TorTransport? = null,
) {
    /** One gate for every pass this component runs. */
    private val gate = DialGate.over(concurrency, tor)

    /** Urls the last [adopt] saw a fold verdict for; never worth measuring. */
    @Volatile
    private var folded: Set<NormalizedRelayUrl> = emptySet()

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
     * Read back what is already known about these urls without dialling, and return the urls to
     * refuse. A failed read leaves the previous answer standing.
     */
    suspend fun applyVerdicts(candidates: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        if (candidates.isEmpty()) return emptyList()
        adopt(candidates)
        return consistency.unusable(candidates)
    }

    /**
     * Measure the urls nothing is known about yet and publish what that proves. Returns how many
     * new verdicts were reached. Run in the monitor's sequence so two writers never share a record.
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
            // Reported, not skipped: "nothing left to measure" is the state the gate works towards.
            report(label, candidates, dialled = 0, decided = 0, unmeasurable = emptyMap())
            return 0
        }

        progress?.measuring(wanted.size, Processors.UNIT_URL)
        val decided = AtomicInteger()
        val refused = AtomicInteger()
        // Urls a socket was opened for; a url [canDial] held back is not one.
        val walked = AtomicInteger()
        val silent = ConcurrentHashMap<NormalizedRelayUrl, Finding>()
        // Terminal text Silence could not place, sampled so the table grows from real strings.
        val unplaced = ConcurrentHashMap.newKeySet<String>()
        // One anchor for the whole pass.
        val anchor = RelayConsistency.settledAnchor(nowSeconds())

        coroutineScope {
            for (url in wanted) {
                launch {
                    gate.withPermit(url) {
                        // The deadline sits inside the permit; around the launch it would time the
                        // wait for a permit.
                        val ran =
                            withTimeoutOrNull(probe.deadlineMs(url)) {
                                try {
                                    measureOne(url, anchor, canDial, onEvent, sockets, walked, decided, refused, silent, unplaced)
                                } finally {
                                    progress?.released(url.url)
                                }
                            }
                        // Nothing is published about a url the deadline cut: the clock is ours.
                        if (ran == null) silent[url] = Finding(Unmeasured.ABANDONED)
                    }
                    // Counted on completion, since the body has four early returns.
                }.invokeOnCompletion { progress?.attempted() }
            }
        }

        if (decided.get() > 0 || silent.isNotEmpty()) {
            System.err.println(
                "router: $label stability walked ${walked.get()} of ${wanted.size} url(s) ? ${decided.get()} decided " +
                    "(${refused.get()} refused as inconsistent), ${silent.size} proved nothing" +
                    breakdown(silent).joinToString(prefix = " (", postfix = ")") { "${it.second} ${it.first.label}" } +
                    ", ${consistency.refusedCount()} url(s) now refused in total " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        if (unplaced.isNotEmpty()) {
            System.err.println("router: $label stability could not classify ${unplaced.size} terminal reason(s): " + unplaced.joinToString(" | "))
        }
        report(label, candidates, dialled = walked.get(), decided = decided.get(), unmeasurable = silent)
        return decided.get()
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

    /** Both rungs of the filter ladder, and which of them the verdict is read from. */
    private data class Ladder(
        val bare: Answers,
        val fallback: Answers?,
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

    /** One url's paired walk, so the deadline has something to wrap. */
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
        // Not runCatching: it swallows CancellationException and would file a shutdown as probe failures.
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
                ladder(url, anchor, onEvent)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // A relay failing mid-walk is [HostStrikes]' business, not a verdict.
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
            // The transport's words only under the one reason they explain.
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
        // Written per url so a restart mid-pass keeps what was proved. Not runCatching: a swallowed
        // cancellation would file the url as ABANDONED after `decided` already counted it.
        try {
            record.publishConsistency(
                url,
                consistent = answer == RelayConsistency.Verdict.CONSISTENT,
                first = first.size,
                second = second.size,
                shared = consistency.shared(first, second),
                score = consistency.containment(first, second),
                anchorDays = RelayConsistency.ANCHOR_LAG_SECONDS / (24 * 60 * 60),
            )
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
        }
    }

    /**
     * The bare filter, then the kinds fallback only when the first proved nothing. An auth
     * refusal ends the ladder: every later ask on that connection waits out the idle window.
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
     * Two walks of [url] through the same filter, in flight at the same time. Neither may throw:
     * `async` reports a failure by cancelling its parent, taking every other url down with it.
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
     * Pull the stored verdicts into memory. A url whose verdict has aged out comes back with
     * none, which is "dial it".
     */
    private suspend fun adopt(candidates: List<NormalizedRelayUrl>) {
        val held =
            try {
                record.load(candidates)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Keep what we hold; emptying on a failed read would readmit every refused relay at once.
                return
            }
        consistency.replace(candidates, held.consistent, held.inconsistent)
        // A folded url is never dialled; the monitor hands both passes the raw pre-fold list.
        folded = held.aliases.keys
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
