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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The fold, in two halves that run at different times and cost different
 * things.
 *
 * [apply] READS. It loads the verdicts already written down, collapses the
 * candidate set, and returns. One `#d` query per 500 urls, no sockets, no
 * dialling — cheap enough to sit on the fan-out's critical path, which is where
 * it has to be: a duplicate is not a property of a url, it is a property of a
 * url NEXT TO another one, so the only place the fold can be applied is the
 * point where the whole candidate set exists.
 *
 * [measure] DIALS. It fingerprints the urls nothing is known about and publishes
 * what it learns. This is the expensive half — 179 fingerprints took 1:19 in the
 * Docker run, and a first pass over a fully polluted store is five figures of
 * them — so it runs on [AliasMonitor]'s own schedule rather than in front of a
 * sync cycle that is only trying to start downloading.
 *
 * The two communicate through the store and nothing else. [measure] signs
 * kind-30166 records; [apply] reads them back on the next cycle. That is what
 * makes the split safe across a restart, and what lets a second router share the
 * work without either one knowing about the other.
 *
 * The cost of [measure] is bounded on both axes and deliberately so.
 * [probesPerCycle] caps how many fingerprints one pass will take, so the first
 * one after this ships does not turn into a single enormous probe run; the rest
 * are learned over the passes that follow, worst case first, and every verdict
 * is written down for [RelayAliasRecord.DEFAULT_TTL_SECONDS]. A steady-state
 * pass probes only the urls that appeared since the last one.
 *
 * **The cost of the split is that folding now lags discovery by one pass.** A
 * url discovered for the first time has no verdict when [apply] runs, so that
 * cycle dials it; the fold takes hold on the next one. Paying that once per new
 * url is the trade for never making a fan-out wait on a probe.
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
     * Urls in, deduplicated urls out, WITHOUT dialling anything.
     *
     * Reads back the verdicts already published — by an earlier pass of
     * [measure], an earlier boot, or another router signing with the same key —
     * and collapses the candidate set against them. Everything with no verdict
     * yet comes back in [Cleaned.dial] and [Cleaned.unmeasured], because the
     * only safe reading of "not measured" is "dial it as it stands".
     *
     * This is the half that runs in front of a fan-out. It must stay cheap: one
     * `#d` query per 500 urls and no network at all.
     */
    suspend fun apply(candidates: List<NormalizedRelayUrl>): Cleaned {
        if (candidates.size < 2) return Cleaned(candidates, emptyMap(), candidates)
        // What a previous pass — this boot or another — already measured.
        adopt(candidates)
        return collapse(candidates)
    }

    /**
     * The stream's socket bookkeeping, because a fingerprint opens a websocket
     * and NOTHING in quartz ever closes one.
     *
     * `fetchAll` unsubscribes when it returns — it sends a CLOSE — and leaves
     * the connection in the pool; the client's own keep-alive only ever
     * RECONNECTS. So a pass that fingerprints up to [DEFAULT_PROBES_PER_CYCLE]
     * urls used to leave up to that many sockets open behind it, against a
     * router whose whole dispatcher budget is 1024 and whose per-HOST budget is
     * 20 — and the fold probes widest group first, i.e. the hosts wearing 55
     * urls. Every one of those sockets is a slot the fan-out cannot have.
     *
     * It is the STREAM's, not this component's, for the reason
     * `DynamicSync.releaseSocket` exists at all: two streams routinely land on
     * one relay, so closing a socket is only safe behind a refcount, and this
     * pass runs alongside a fan-out that may be holding the same url. Claiming
     * before the dial is what puts the probe INTO that count instead of
     * decrementing somebody else's.
     */
    interface Sockets {
        /** Take a share of this url's socket before dialling it. */
        fun claim(url: NormalizedRelayUrl)

        /** Give it back — and close the socket if nothing else holds one. */
        fun release(url: NormalizedRelayUrl)

        companion object {
            /**
             * Leaves every socket where it is. The honest default for a caller
             * with no refcount to offer: leaking a connection is recoverable,
             * closing one out from under a live transfer is not.
             */
            val NONE =
                object : Sockets {
                    override fun claim(url: NormalizedRelayUrl) = Unit

                    override fun release(url: NormalizedRelayUrl) = Unit
                }
        }
    }

    /**
     * Fingerprint what [apply] could not answer, and publish what that proves.
     *
     * The dialling half, and the reason the two are separate: this walks up to
     * [probesPerCycle] fingerprints, each of which is a paged websocket
     * conversation with somebody else's relay. Returns how many new aliases it
     * learned, so a caller can log a pass that did nothing differently from one
     * that never ran.
     *
     * Safe to call with anything: a url whose host wears no other url is never
     * probed, and a set of one returns immediately.
     *
     * [canDial] is the caller's own transport guard — the same one its fan-out
     * applies — so a probe never dials what the caller would refuse to.
     * [onEvent] receives everything the probes downloaded; hand it an ingest and
     * a fingerprint stops being wasted bandwidth. Discard it and the probe is
     * pure overhead, which is a choice a caller is allowed to make. [sockets] is
     * the caller's connection refcount: without one every fingerprint leaves a
     * websocket behind — see [Sockets].
     */
    suspend fun measure(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit = {},
        sockets: Sockets = Sockets.NONE,
    ): Int {
        if (candidates.size < 2) return 0
        val startedMs = System.currentTimeMillis()

        // What a previous pass — this boot or another — already measured.
        adopt(candidates)

        val groups = aliases.unresolved(candidates)
        var learned = 0
        var probed = 0
        if (groups.isNotEmpty()) {
            val budget = AtomicInteger(probesPerCycle)
            val gate = Semaphore(concurrency)
            // The pass-wide count of folds, and nothing else: a second map of
            // the cleared urls was accumulated here and never read.
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
                        //
                        // "No usable fingerprint" means TOO SHORT as well as
                        // absent. A leader that hands over three ids is under
                        // [RelayAliases.DEFAULT_MIN_SAMPLE], so nothing can fold
                        // onto it and — since the thin-window guard — nothing
                        // can be cleared against it either. Accepting it as a
                        // yardstick dialled every member of its group to decide
                        // nothing, and did it again every pass, forever. The
                        // leader alone still costs one dial per pass, which is
                        // the right price for noticing it has recovered.
                        var dialled = false
                        val lead =
                            gate.withPermit {
                                if (!canDial(leader)) return@withPermit null
                                dialled = true
                                taken.incrementAndGet()
                                sockets.claim(leader)
                                try {
                                    probe.leaderPrint(leader, anchor, onEvent)
                                } finally {
                                    sockets.release(leader)
                                }
                            }
                        if (lead == null || !aliases.usableWindow(lead.ids)) {
                            // Hand back what this group reserved and did not
                            // spend, or the budget is consumed by intentions and
                            // a later group goes unprobed for it. A leader the
                            // transport guard DECLINED cost nothing at all, so
                            // the whole reservation goes back — refunding
                            // `size - 1` there paid a fingerprint that was never
                            // taken, out of the budget of a group that would
                            // have used it.
                            budget.addAndGet(if (dialled) wanted.size - 1 else wanted.size)
                            return@launch
                        }
                        prints[leader] = lead.ids

                        coroutineScope {
                            for (url in wanted.drop(1)) {
                                launch {
                                    gate.withPermit {
                                        if (!canDial(url)) return@withPermit
                                        taken.incrementAndGet()
                                        sockets.claim(url)
                                        try {
                                            probe.fingerprint(url, anchor, lead.kinds, onEvent)?.let { prints[url] = it }
                                        } finally {
                                            sockets.release(url)
                                        }
                                    }
                                }
                            }
                        }
                        val leaderPrint = lead.ids
                        val result = aliases.learn(group, leader, prints)
                        // This group's share of the pass, kept separately so it
                        // can be written the moment the group is decided. The
                        // pass-wide map is only a counter for the summary line.
                        val verdicts = LinkedHashMap<NormalizedRelayUrl, Pair<NormalizedRelayUrl, Pair<Int, Int>>>()
                        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
                        for ((alias, canonical) in result.folded) {
                            val print = prints[alias].orEmpty()
                            val shared = leaderPrint.count { it in print }
                            newVerdicts[alias] = canonical to (print.size to shared)
                            verdicts[alias] = canonical to (print.size to shared)
                        }
                        // The cleared half, and its evidence has to name what was
                        // actually held up against what. A member is compared to
                        // the LEADER and to nothing else — saying "of N peers on
                        // this host" claimed comparisons that never happened, in
                        // a signed month-long statement about someone else's
                        // server, which is the same over-claiming the thin-window
                        // guard exists to stop.
                        val compared = result.distinct.count { it != leader }
                        for (url in result.distinct) {
                            val print = prints[url].orEmpty()
                            if (url != leader) {
                                cleared[url] = Cleared(print.size, leader.url, leaderPrint.count { it in print })
                            } else {
                                // The leader's own separation is the best any
                                // member managed against it; a hardcoded 0 would
                                // claim a cleaner one than was measured.
                                val best =
                                    result.distinct
                                        .filter { it != leader }
                                        .maxOfOrNull { other -> prints[other].orEmpty().count { it in leaderPrint } }
                                        ?: 0
                                cleared[url] = Cleared(print.size, "$compared compared peer(s)", best)
                            }
                        }

                        // WRITTEN AS THIS GROUP FINISHES, not when the pass
                        // does. A pass is background work that yields to the
                        // fan-out, so on a cold store — where nothing is folded
                        // yet and the mirror is therefore at its widest — it can
                        // run for a quarter of an hour. Held to the end, every
                        // fingerprint in it is lost to a restart, and a cold
                        // store is exactly when a restart is most likely and the
                        // work most expensive to redo. One group's verdicts are
                        // a complete, self-contained answer; there is nothing to
                        // wait for.
                        //
                        // One at a time and each guarded: these are signed public
                        // statements about other people's servers, and a failure
                        // to write one must not take the pass down with it.
                        for ((alias, v) in verdicts) {
                            runCatching { record.publish(alias, v.first, v.second.first, v.second.second) }
                        }
                        for ((url, c) in cleared) {
                            runCatching { record.publishDistinct(url, c.sampled, c.comparedAgainst, c.bestShared) }
                        }
                    }
                }
            }
            probed = taken.get()
            learned = newVerdicts.size
        }

        if (probed > 0 || learned > 0) {
            val cleaned = collapse(candidates)
            System.err.println(
                "router: $label measured $probed fingerprint(s) ? $learned new alias(es), " +
                    "${candidates.size} url(s) now fold onto ${cleaned.dial.size} relay(s) " +
                    "(${aliases.size()} known, ${cleaned.unmeasured.size} unmeasured) " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        return learned
    }

    /** The evidence behind one cleared url, held until the group is decided. */
    private data class Cleared(
        val sampled: Int,
        val comparedAgainst: String,
        val bestShared: Int,
    )

    /**
     * Pull both halves of the stored verdict into memory: the folds, and the
     * urls a previous pass cleared as their own relay.
     *
     * A store that cannot answer is not an error — it means "nothing known",
     * which is already the safe state — so the query is allowed to fail into an
     * empty result rather than take a fan-out down with it.
     */
    private suspend fun adopt(candidates: List<NormalizedRelayUrl>) {
        val held =
            try {
                record.load(candidates)
            } catch (e: CancellationException) {
                // Not a store failure — the scope is shutting down, and
                // swallowing it here would let a cancelled cycle carry on
                // rewriting the verdict cache. `runCatching` catches it, which
                // is why this is spelled out.
                throw e
            } catch (e: Exception) {
                return
            }
        // FORGET FIRST, so the store is authoritative on every pass and its TTL
        // means something. `load` already refuses a record past its TTL, but a
        // verdict adopted while it was still fresh used to live in memory for
        // the rest of the process: `measured` kept answering true, so the url
        // was never re-probed and the expired verdict was never republished,
        // and the fan-out went on folding on evidence that had ceased to exist
        // anywhere. Skipped entirely when the query FAILS — a store that cannot
        // answer is not a store saying "no verdict", and dropping what we hold
        // on the strength of a failed read would unfold the whole fan-out.
        aliases.forget(candidates)
        aliases.adopt(held.aliases)
        aliases.adoptDistinct(held.distinct)
    }

    /**
     * The candidate set as the verdicts currently in memory see it.
     *
     * Pure — no store, no network — so both halves end the same way and the
     * numbers [measure] logs are the numbers the next [apply] will produce.
     */
    private fun collapse(candidates: List<NormalizedRelayUrl>): Cleaned {
        val dial = candidates.map { aliases.canonicalOf(it) }.distinct()
        val map = candidates.filter { aliases.canonicalOf(it) != it }.associateWith { aliases.canonicalOf(it) }
        return Cleaned(dial, map, dial.filter { !aliases.measured(it) })
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
