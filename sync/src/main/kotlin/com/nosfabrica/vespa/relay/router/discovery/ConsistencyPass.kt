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
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
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
 * for [RelayAliasRecord.DEFAULT_TTL_SECONDS] — a month, after which the relay is
 * measured again and a server that has been fixed comes back into the fan-out on
 * its own. Nothing is written for a url that could not be measured: an
 * unreachable relay and one holding nine events have proved nothing, and a
 * negative verdict costs a relay its place in the mirror.
 *
 * ## Cost
 *
 * Two REQs per url, once a month per url, [concurrency] in flight and no
 * per-pass total — see [DEFAULT_PROBES_PER_CYCLE]. Everything downloaded goes to the caller's
 * ingest, exactly as [AliasProbe]'s own doc describes — on a stable relay the
 * window was worth having, and on an unstable one the store drops it as
 * already-held. The pass is not a tax on the mirror; it is a sync that also
 * decides.
 */
class ConsistencyPass(
    private val consistency: RelayConsistency,
    private val record: RelayAliasRecord,
    private val probe: AliasProbe,
    private val probesPerCycle: Int = DEFAULT_PROBES_PER_CYCLE,
    private val concurrency: Int = DEFAULT_CONCURRENCY,
    /**
     * Where each pass reports how far it got — see [AliasFolding.progress] for
     * the argument. Null says nothing, which is every caller but the router.
     */
    val progress: Processors.Handle? = null,
) {
    /** Urls the last [adopt] saw a fold verdict for — never worth measuring. */
    @Volatile
    private var folded: Set<NormalizedRelayUrl> = emptySet()

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
    suspend fun apply(candidates: List<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
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
        sockets: AliasFolding.Sockets = AliasFolding.Sockets.NONE,
    ): Int {
        if (candidates.isEmpty()) return 0
        val startedMs = System.currentTimeMillis()
        adopt(candidates)

        val wanted = consistency.toProbe(candidates).filter { it !in folded }.take(probesPerCycle)
        if (wanted.isEmpty()) {
            // NOT SILENT, because "nothing left to measure" is the state this
            // gate is trying to reach and the one an operator most wants to
            // see. Returning without a word left it indistinguishable from a
            // pass that never ran, which is what a monthly TTL looks like for
            // twenty-nine days out of thirty.
            report(label, candidates, dialled = 0, decided = 0, unmeasurable = emptySet())
            return 0
        }

        val gate = Semaphore(concurrency)
        val decided = AtomicInteger()
        val refused = AtomicInteger()
        val unmeasurable = AtomicInteger()
        // The urls that proved nothing, kept rather than only counted: grouped
        // by host they become the one `undecided` row this pass can publish, and
        // "which server would not answer twice" is what an operator chases.
        // Bounded by `probesPerCycle`, since that is what `wanted` is capped at.
        val silent = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
        // ONE anchor for the whole pass, a week behind the clock. Shared for the
        // same reason the fold shares one per group — two urls measured from
        // different anchors are not comparable — though here it matters less,
        // since each url is only ever compared to itself.
        val anchor = RelayConsistency.settledAnchor(nowSeconds())

        coroutineScope {
            for (url in wanted) {
                launch {
                    gate.withPermit {
                        if (!canDial(url)) return@withPermit
                        sockets.claim(url)
                        val verdict =
                            try {
                                // THE PAIR, GENUINELY CONCURRENT, over one
                                // connection — two REQs in flight at the same
                                // instant, which is what makes "the answer
                                // changed" impossible to blame on elapsed time.
                                //
                                // This was staged before, and the comment here
                                // claimed concurrency it did not have: the first
                                // walk ran to completion to discover the filter
                                // and only the second was wrapped in `async`, so
                                // the pair was sequential and one of the two
                                // `async`s was returning an already-computed set.
                                //
                                // The filter still has to be agreed on — two
                                // answers to different questions are not evidence
                                // — so the bare filter is tried as a concurrent
                                // PAIR first, and only if it comes back unusable
                                // is the kinds fallback tried, also as a pair.
                                // The common case is two REQs and no extra walk;
                                // the minority of hosts refusing a bare filter
                                // pay a second pair. Neither case compares two
                                // windows taken through different filters.
                                walkPair(url, anchor, null, onEvent)
                                    ?: walkPair(url, anchor, AliasProbe.FALLBACK_KINDS, onEvent)
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
                        val answer = consistency.decide(verdict?.first, verdict?.second)
                        if (answer == RelayConsistency.Verdict.UNMEASURABLE) {
                            unmeasurable.incrementAndGet()
                            silent += url
                            return@withPermit
                        }
                        val first = verdict!!.first!!
                        val second = verdict.second!!
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
                            )
                        }
                    }
                }
            }
        }

        if (decided.get() > 0 || unmeasurable.get() > 0) {
            System.err.println(
                "router: $label stability measured ${wanted.size} url(s) ? ${decided.get()} decided " +
                    "(${refused.get()} refused as inconsistent, ${unmeasurable.get()} said too little to judge), " +
                    "${consistency.refusedCount()} url(s) now refused in total " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        report(label, candidates, dialled = wanted.size, decided = decided.get(), unmeasurable = silent)
        return decided.get()
    }

    /**
     * What this pass reached, where it outlives the log line above.
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
        unmeasurable: Set<NormalizedRelayUrl>,
    ) {
        val handle = progress ?: return
        // By HOST, like the fold's rows: the urls are what was asked, but the
        // thing that would not answer twice is a server, and one row per url
        // would be a list of paths that says the same thing forty times.
        val hosts = unmeasurable.map { RelayAliases.hostOf(it.url) }.distinct().sorted()
        handle.record(
            Processors.Work(
                stream = label,
                candidates = candidates.size,
                unmeasured = consistency.toProbe(candidates).count { it !in folded },
                dialled = dialled,
                decided = decided,
                undecided =
                    if (hosts.isEmpty()) {
                        emptyList()
                    } else {
                        listOf(
                            Processors.Undecided(
                                // The words the log line uses, so a reader
                                // meeting both does not have to work out that
                                // they are the same finding.
                                reason = "said too little to judge",
                                hosts = hosts.size,
                                examples = hosts.take(Processors.MAX_UNDECIDED_EXAMPLES),
                            ),
                        )
                    },
            ),
        )
    }

    /**
     * Two walks of [url] through the SAME filter, in flight at the same time.
     *
     * Null when the pair proves nothing — either walk unanswered, or a window
     * too short for [RelayConsistency] to judge — so the caller can fall through
     * to the kinds filter without treating a refused bare filter as a verdict.
     *
     * **Neither walk may throw out of here, and that is structural rather than
     * defensive.** `async` reports a failure by cancelling its parent, so an
     * exception from one walk would cancel the sibling, the enclosing `launch`
     * and the `coroutineScope` around the whole pass — one unlucky relay taking
     * down every other url's measurement, with the caller's `catch` running far
     * too late to stop it. Catching inside each child keeps the failure a value.
     */
    private suspend fun walkPair(
        url: NormalizedRelayUrl,
        anchor: Long,
        kinds: List<Int>?,
        onEvent: suspend (Event) -> Unit,
    ): Pair<Set<String>?, Set<String>?>? =
        coroutineScope {
            val walks =
                List(2) {
                    async {
                        runCatching { probe.fingerprint(url, anchor, kinds, onEvent) }
                            .getOrNull()
                    }
                }.awaitAll()
            val pair = walks[0] to walks[1]
            // "Proves nothing" is the pass's own bar, asked here so a bare filter
            // the relay refused (empty, or a handful of events) falls through to
            // the fallback instead of being published as a verdict.
            if (consistency.decide(pair.first, pair.second) == RelayConsistency.Verdict.UNMEASURABLE) null else pair
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
        consistency.replace(candidates, held.stable, held.unstable)
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
        /**
         * Urls measured per pass. UNCAPPED, for the same reason the fold's is —
         * see [AliasFolding.DEFAULT_PROBES_PER_CYCLE].
         *
         * It was 3,000. [concurrency] is what bounds the pass; the cap only
         * ever decided how many six-hour intervals full coverage took, and this
         * gate measures one url at a time with no group to leave half-done, so
         * stopping at a cap bought nothing a shorter interval would not.
         */
        const val DEFAULT_PROBES_PER_CYCLE = Int.MAX_VALUE

        /** Urls in flight. Matches the fold: this is background work. */
        const val DEFAULT_CONCURRENCY = 16
    }
}
