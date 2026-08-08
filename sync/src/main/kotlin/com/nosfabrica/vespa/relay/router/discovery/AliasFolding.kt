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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The fold, end to end: read back what the monitor already decided, probe the
 * urls nothing is known about, publish the new verdicts, and hand the fan-out
 * the urls actually worth dialling.
 *
 * Runs once per cycle, before the fan-out and after discovery, because that is
 * the only point where the whole candidate set exists — a duplicate is not a
 * property of a url, it is a property of a url NEXT TO another one.
 *
 * The cost is bounded on both axes and deliberately so. [probesPerCycle] caps
 * how many fingerprints one cycle will take, so the first cycle after this
 * ships does not turn into a five-figure probe run; the rest are learned over
 * the cycles that follow, worst case first, and every verdict is written down
 * for [RelayAliasRecord.DEFAULT_TTL_SECONDS]. A steady-state cycle probes only
 * the urls that appeared since the last one.
 */
class AliasFolding(
    private val aliases: RelayAliases,
    private val record: RelayAliasRecord,
    private val probe: AliasProbe,
    private val probesPerCycle: Int = DEFAULT_PROBES_PER_CYCLE,
    private val concurrency: Int = DEFAULT_CONCURRENCY,
) {
    /**
     * What a set of urls collapses to.
     *
     * [aliases] is handed back rather than applied, because only the caller
     * knows what else it keys by url — a discovery stream has authors bound to
     * each one, a monitor has records, a config has an exclude list. Applying
     * the map is one line ([RelayAliases.fold] does it for a discovery stream);
     * guessing at the caller's structures is not this component's business.
     *
     * [unmeasured] is not a failure and must not be dropped. It is "no verdict
     * yet" — never probed, unreachable this cycle, or a relay whose answers are
     * not reproducible — and the only safe reading of it is to dial the url as
     * it stands. Kept separate from [dial] so that policy is the caller's
     * explicit choice rather than a silent default inside here.
     */
    data class Cleaned(
        /** The urls worth dialling: canonical, plus everything still unmeasured. */
        val dial: List<NormalizedRelayUrl>,
        /** Folded url -> the url that stands in for it. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        /** Urls with no verdict either way. A subset of [dial]. */
        val unmeasured: List<NormalizedRelayUrl>,
    )

    /**
     * Urls in, deduplicated urls out — the whole component in one call.
     *
     * Reads back what was already measured, fingerprints what is still unknown
     * within this cycle's budget, publishes what it learned, and returns the
     * collapsed set. Safe to call with anything: a url whose host wears no
     * other url is never probed, and a set of one is returned untouched.
     *
     * [canDial] is the caller's own transport guard — the same one its fan-out
     * applies — so a probe never dials what the caller would refuse to.
     * [onEvent] receives everything the probes downloaded; hand it an ingest and
     * a fingerprint stops being wasted bandwidth. Discard it and the probe is
     * pure overhead, which is a choice a caller is allowed to make.
     */
    suspend fun clean(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit = {},
    ): Cleaned {
        if (candidates.size < 2) return Cleaned(candidates, emptyMap(), candidates)
        val startedMs = System.currentTimeMillis()

        // What a previous run — this boot or another — already measured.
        val fromStore = runCatching { record.load(candidates) }.getOrDefault(emptyMap())
        val heldBefore = aliases.verdicts().filterKeys { it in candidates }
        aliases.adopt(fromStore)
        // A verdict this process still holds but the store no longer does was
        // clobbered by the other writer on that address — see
        // [RelayAliasRecord.reassert]. Restoring it costs a signature, not a
        // probe, which is the whole point of noticing.
        val restored = runCatching { record.reassert(heldBefore, fromStore) }.getOrDefault(0)
        if (restored > 0) System.err.println("router: $label restored $restored alias record(s) overwritten by the monitor")

        val groups = aliases.unresolved(candidates)
        var learned = 0
        var probed = 0
        if (groups.isNotEmpty()) {
            val budget = AtomicInteger(probesPerCycle)
            val gate = Semaphore(concurrency)
            val newVerdicts = ConcurrentHashMap<NormalizedRelayUrl, Pair<NormalizedRelayUrl, Pair<Int, Int>>>()
            val taken = AtomicInteger()
            coroutineScope {
                // Widest group first: a host wearing 55 urls is 54 dials a
                // cycle, and a host wearing 2 is one. With a budget that runs
                // out, the order it runs out in is the whole design.
                for (group in groups.sortedByDescending { it.size }) {
                    launch {
                        val wanted = aliases.toProbe(group)
                        // Reserved as a block: half a group's fingerprints
                        // decides nothing, so a group that cannot be finished
                        // this cycle is left entirely for the next one.
                        if (budget.addAndGet(-wanted.size) < 0) {
                            budget.addAndGet(wanted.size)
                            return@launch
                        }
                        val prints = ConcurrentHashMap<NormalizedRelayUrl, Set<String>>()
                        val leader = wanted.first()
                        // ONE anchor for the whole group, taken before any of
                        // it is dialled, and held a minute behind the clock.
                        //
                        // Shared, because "the newest N" is a moving window and
                        // these walks are minutes apart behind a 16-permit
                        // gate; measured live, two urls of nos.lol scored 0.41
                        // unanchored — the same relay, missed.
                        //
                        // Behind the clock, because an event is not visible the
                        // instant its `created_at` passes: it still has to
                        // arrive and be indexed. Anchored at `now`, the newest
                        // second of the window is whatever each relay happened
                        // to have finished writing, which differs per dial and
                        // reintroduces the drift the anchor removes. See
                        // [AliasProbe.ANCHOR_LAG_SECONDS].
                        val anchor = AliasProbe.settledAnchor(nowSeconds())

                        // THE LEADER GOES FIRST, alone, for two reasons the
                        // full-corpus sweep made expensive to ignore.
                        //
                        // It decides the FILTER. 46 of 229 hosts refused a bare
                        // one outright (`CLOSED blocked: can't handle empty
                        // filters`) and answered a kinds filter perfectly well
                        // — but two urls fingerprinted through different
                        // filters are not comparable, so whatever the leader
                        // had to be asked is what its whole group is asked.
                        //
                        // And it decides whether to ask AT ALL. A group whose
                        // leader has no usable fingerprint can never fold
                        // ([RelayAliases.learn] returns nothing without one),
                        // so dialling its members is guaranteed waste: in that
                        // same sweep, 892 urls behind 46 leaders.
                        val lead =
                            gate.withPermit {
                                if (!canDial(leader)) return@withPermit null
                                taken.incrementAndGet()
                                probe.leaderPrint(leader, anchor, onEvent)
                            } ?: return@launch
                        prints[leader] = lead.ids

                        coroutineScope {
                            for (url in wanted.drop(1)) {
                                launch {
                                    gate.withPermit {
                                        if (!canDial(url)) return@withPermit
                                        taken.incrementAndGet()
                                        probe.fingerprint(url, anchor, lead.kinds, onEvent)?.let { prints[url] = it }
                                    }
                                }
                            }
                        }
                        val leaderPrint = lead.ids
                        for ((alias, canonical) in aliases.learn(group, prints)) {
                            val print = prints[alias].orEmpty()
                            val shared = leaderPrint.count { it in print }
                            newVerdicts[alias] = canonical to (print.size to shared)
                        }
                    }
                }
            }
            probed = taken.get()
            learned = newVerdicts.size
            // Published AFTER the probing, one at a time: these are signed
            // public statements about other people's servers, and a failure to
            // write one must not take a fan-out down with it.
            for ((alias, verdict) in newVerdicts) {
                val (canonical, evidence) = verdict
                runCatching { record.publish(alias, canonical, evidence.first, evidence.second) }
            }
        }

        val dial = candidates.map { aliases.canonicalOf(it) }.distinct()
        val map = candidates.filter { aliases.canonicalOf(it) != it }.associateWith { aliases.canonicalOf(it) }
        val unmeasured = dial.filter { !aliases.measured(it) }
        if (learned > 0 || dial.size < candidates.size) {
            System.err.println(
                "router: $label folded ${candidates.size} url(s) onto ${dial.size} relay(s) " +
                    "($learned new alias(es) from $probed fingerprint(s), ${aliases.size()} known, " +
                    "${unmeasured.size} unmeasured) in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        return Cleaned(dial, map, unmeasured)
    }

    companion object {
        /**
         * Fingerprints one cycle will take. Sized so a first run against a
         * fully polluted store spreads over a handful of cycles instead of
         * becoming one enormous probe pass, and so a steady-state cycle — which
         * only ever sees newly discovered urls — never comes near it.
         */
        const val DEFAULT_PROBES_PER_CYCLE = 2_000

        /** Probes in flight. Below the fan-out's own concurrency: this is a side quest. */
        const val DEFAULT_CONCURRENCY = 16
    }
}
