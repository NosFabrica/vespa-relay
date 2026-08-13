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
 * The pair is asked CONCURRENTLY, over one connection, which is both cheaper and
 * strictly better evidence: a relay that shuffles per REQ shows it immediately,
 * and a relay whose two answers differ only because time passed between them
 * cannot — there is no between. The anchor is a week old on top of that
 * ([RelayConsistency.ANCHOR_LAG_SECONDS]), so neither answer can contain an
 * event the other could not have seen.
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
 * Two REQs per url, once a month per url, capped at [probesPerCycle] per pass
 * and [concurrency] in flight. Everything downloaded goes to the caller's
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
) {
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

        val wanted = consistency.toProbe(candidates).take(probesPerCycle)
        if (wanted.isEmpty()) return 0

        val gate = Semaphore(concurrency)
        val decided = AtomicInteger()
        val refused = AtomicInteger()
        val unmeasurable = AtomicInteger()
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
                                // The pair, CONCURRENTLY, over one connection.
                                // Two asks separated by a round trip would let a
                                // relay differ for a reason that is about time
                                // rather than about the relay; asked together
                                // there is no elapsed time to blame.
                                //
                                // The first ask also decides the FILTER — a bare
                                // one where the relay takes it, kinds=[1] where
                                // it does not — and the second is given the same
                                // one, because two answers to different questions
                                // are not evidence of anything. That costs the
                                // pair being staged rather than truly
                                // simultaneous for the minority of hosts that
                                // refuse a bare filter, which is the right trade:
                                // a comparable pair a moment apart beats an
                                // incomparable pair at once.
                                val lead = probe.leaderPrint(url, anchor, onEvent)
                                if (lead == null) {
                                    null
                                } else {
                                    val pair =
                                        listOf(
                                            async { lead.ids },
                                            async { probe.fingerprint(url, anchor, lead.kinds, onEvent) },
                                        ).awaitAll()
                                    pair[0] to pair[1]
                                }
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
        return decided.get()
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
        consistency.forget(candidates)
        consistency.adopt(held.stable, held.unstable)
    }

    companion object {
        /**
         * Urls measured per pass. Higher than the fold's, because this is two
         * asks against one url rather than a group of fingerprints that only
         * mean anything together — there is no reservation to make and no group
         * to leave half-done, so a pass can simply stop at the cap and continue
         * next time.
         */
        const val DEFAULT_PROBES_PER_CYCLE = 3_000

        /** Urls in flight. Matches the fold: this is background work. */
        const val DEFAULT_CONCURRENCY = 16
    }
}
