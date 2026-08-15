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
 * The cost of [measure] is bounded by [concurrency] alone — 16 permits, and
 * therefore 16 sockets, however large the candidate set. There is no per-pass
 * total: a pass measures its whole set, worst case first, and every verdict is
 * written down for [RelayAliasRecord.DEFAULT_TTL_SECONDS], so the run is paid
 * once per url per month rather than once per pass. A steady-state
 * pass probes only the urls that appeared since the last one.
 *
 * **The cost of the split is that folding now lags discovery by one pass.** A
 * url discovered for the first time has no verdict when [apply] runs, so that
 * cycle dials it; the fold takes hold on the next one. Paying that once per new
 * url is the trade for never making a fan-out wait on a probe.
 *
 * **A pass that leaves a host unfolded says which host and why** — see
 * [Undecided]. That is not decoration. Five different things end a group with
 * nothing written down, four of them recover on their own and one never will,
 * and from outside this process all five are the same silence: a url still being
 * dialled next to eleven siblings that fold.
 */
class AliasFolding(
    private val aliases: RelayAliases,
    private val record: RelayAliasRecord,
    private val probe: AliasProbe,
    private val concurrency: Int = DEFAULT_CONCURRENCY,
    /** How long a host that could not be decided is left alone — see [undecidable]. */
    private val undecidableCooldownMs: Long = DEFAULT_UNDECIDABLE_COOLDOWN_MS,
    /**
     * Fold a group that every url ANSWERED and none of them would serve anything
     * from — see [foldUnreadableGroups] for what it decides and what it risks.
     */
    private val foldUnreadableGroups: Boolean = DEFAULT_FOLD_UNREADABLE_GROUPS,
    /**
     * Where each pass reports how far it got, or null to say nothing — which is
     * every test and every caller that is not the router.
     *
     * The pass writes the WORK here and [AliasMonitor] writes the CLOCK to the
     * same handle, which is why it is handed in rather than created here: the
     * two halves of "how is the fold doing" are measured by two objects and have
     * to land in one row.
     */
    val progress: Processors.Handle? = null,
) {
    /**
     * Hosts a pass DIALLED and could not decide anything about, and the moment
     * each becomes worth trying again.
     *
     * **This is what stops the widest groups eating the budget forever.** THREE
     * exits below leave a DIALLED group with no verdict at all — no url on the
     * host could be a yardstick (silent, or a window too short to measure
     * against), the yardstick could not reproduce itself and the group was
     * forgotten, or it was a fine yardstick and every other url was silent or
     * too thin to compare, so nothing was held up against it. None of them write
     * anything down, by design: they are all cases where publishing would be a
     * claim the measurement does not support. [Undecided] is the same list,
     * reported rather than acted on, plus the two exits that never reach a dial.
     *
     * Nothing written down means [RelayAliases.unresolved] hands the same group
     * back on the next pass. Groups are probed WIDEST FIRST, and a host wearing
     * dozens of minted paths is exactly the shape that fails these tests, so an
     * undecidable host is re-dialled at the FRONT of every pass, learns nothing,
     * and does it again six hours later — permanently.
     *
     * Measured against the four hosts this was reported on: `relay.lightning.pub`
     * folds four urls in TWO SECONDS at containment 1.000, and
     * `multiplexer.huszonegy.world` folds four more in fourteen. Neither is hard;
     * they were simply queued behind hosts that can never be decided and can
     * never stop being asked. It no longer costs a foldable host its turn —
     * nothing is rationed — but it still costs the pass's WALL CLOCK, and
     * re-proving every cycle that `espelho.girino.org` still cannot reproduce
     * its own window is time the hosts that can be decided are waiting through.
     * (`groups.satsdisco.com` stood here until it turned out to be answerable —
     * it was refusing the filter rather than saying nothing, and it folds since
     * [RelayAliases.GROUP_METADATA_KINDS]. Worth remembering before reading the
     * next silent host as permanent.)
     *
     * In memory rather than signed, and that is the point. "I could not measure
     * this" is a fact about OUR pass, not about somebody's server — the
     * distinction the reproducibility guard exists to protect — so it must not
     * become a NIP-66 record. The cost of holding it here is that a restart pays
     * one pass to rediscover it, which is the correct price for never publishing
     * a claim we cannot support.
     */
    private val undecidable = ConcurrentHashMap<String, Long>()

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
     * RECONNECTS. So a pass used to leave one open socket per url it
     * fingerprinted, against a router whose whole dispatcher budget is 1024 and
     * whose per-HOST budget is 20 — and the fold probes widest group first,
     * i.e. the hosts wearing 55 urls. Every one of those sockets is a slot the
     * fan-out cannot have. That is what makes this refcount load-bearing rather
     * than tidy: with the per-pass cap gone, the number of urls a single pass
     * touches is the whole candidate set, so a leak here would be unbounded
     * where it used to be merely large.
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
     * The dialling half, and the reason the two are separate: this fingerprints
     * every group its candidates leave unresolved, [concurrency] at a time, and each fingerprint is a paged websocket conversation with
     * somebody else's relay. Returns how many new aliases it learned, so a
     * caller can log a pass that did nothing differently from one that never
     * ran.
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

        // Everything unresolved, minus the hosts a recent pass already dialled
        // and could not decide. Held back rather than dropped: the cooldown
        // lapses and they are tried again, just not at the front of every pass
        // between now and then. See [undecidable].
        val startedAtMs = System.currentTimeMillis()
        val all = aliases.unresolved(candidates)
        val groups = all.filter { group -> !onCooldown(group, startedAtMs) }
        var learned = 0
        var probed = 0
        // Why each host that ended the pass with nothing written down ended
        // that way — see [Undecided]. Host-keyed, because a group IS a host.
        val undecided = ConcurrentHashMap<String, Undecided>()
        for (group in all - groups.toSet()) {
            undecided[RelayAliases.hostOf(group.first().url)] = Undecided.COOLDOWN
        }
        if (groups.isNotEmpty()) {
            val gate = Semaphore(concurrency)
            // The pass-wide count of folds, and nothing else: a second map of
            // the cleared urls was accumulated here and never read, and the
            // verdicts themselves are held per group, where they are written.
            val newVerdicts = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
            val taken = AtomicInteger()
            coroutineScope {
                // Widest group first: a host wearing 55 urls is 54 dials and a
                // host wearing 2 is one, so the passes that clear the most
                // pollution start earliest and the pass's own wall clock is
                // spent worst-first.
                for (group in groups.sortedByDescending { it.size }) {
                    launch {
                        val wanted = aliases.toProbe(group)
                        val prints = ConcurrentHashMap<NormalizedRelayUrl, Set<String>>()
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

                        // THE YARDSTICK GOES FIRST, alone, for two reasons the
                        // full-corpus sweep made expensive to ignore.
                        //
                        // It decides the FILTER. 46 of 229 hosts refused a bare
                        // one outright (`CLOSED blocked: can't handle empty
                        // filters`) and answered a kinds filter perfectly well
                        // — but two urls fingerprinted through different
                        // filters are not comparable, so whatever the yardstick
                        // had to be asked is what its whole group is asked.
                        //
                        // And it decides whether to ask AT ALL. A group with no
                        // usable yardstick can never fold ([RelayAliases.learn]
                        // returns nothing without one), so dialling the rest is
                        // guaranteed waste: in that same sweep, 892 urls behind
                        // 46 leaders.
                        //
                        // "No usable fingerprint" means TOO SHORT as well as
                        // absent. A url that hands over three ids is under
                        // [RelayAliases.DEFAULT_MIN_SAMPLE], so nothing can fold
                        // onto it and — since the thin-window guard — nothing
                        // can be cleared against it either. Accepting it as a
                        // yardstick dialled every member of its group to decide
                        // nothing, and did it again every pass, forever.
                        //
                        // TOO SHORT FOR WHAT, THOUGH, IS A QUESTION ABOUT THE
                        // FILTER. The bar above is calibrated for a slice of a
                        // general feed; a window of
                        // [RelayAliases.GROUP_METADATA_KINDS] is a NIP-29
                        // relay's COMPLETE list of groups, so a host with seven
                        // groups hands over seven ids and has held nothing back.
                        // Judging that by the firehose floor threw away the
                        // hosts the third rung of the ladder exists to reach —
                        // measured, `groups.hzrd149.com` at 7 ids and
                        // `groups.fiatjaf.com` at 16, both folding at
                        // containment 1.000. So the window is held up against
                        // the floor for the filter that produced it, which is
                        // what [RelayAliases.usableWindow] takes `kinds` for.
                        //
                        // **BUT THE PREFERRED LEADER IS NOT THE ONLY URL THAT
                        // CAN BE THE YARDSTICK, AND TREATING IT AS SUCH LOST
                        // WHOLE HOSTS.** [RelayAliases.PREFERENCE] picks the
                        // pathless url, which is the right SURVIVOR — everyone
                        // else's relay lists name it — but it is only one of the
                        // group's urls and it can be the one that will not
                        // answer: a paid endpoint gating reads it does not gate
                        // on a path, a hidden service whose bare url is dead
                        // while its paths are not, or simply the url whose dial
                        // lost the race this pass. Every one of those abandoned
                        // a group that was perfectly foldable, wrote nothing
                        // down, and — because nothing was written — came back
                        // widest-first on the next pass to fail identically.
                        // Measured on `asia.azzamo.net`: 12 urls, every pair at
                        // containment 1.000, 11 folds in five seconds once
                        // ANYTHING on the host is allowed to hold the ruler.
                        //
                        // So the walk continues down the preference order while
                        // urls stay SILENT, capped at [YARDSTICK_ATTEMPTS]. It
                        // costs nothing in the common case (the first url is the
                        // yardstick and this is the old single dial) and nothing
                        // extra in the failing one either: a url tried and
                        // rejected here was going to be dialled as a member
                        // anyway, and having failed BOTH filters it cannot
                        // usefully be dialled again — so it is dropped from the
                        // member walk rather than asked twice.
                        var dialled = false
                        var spent = 0
                        var found: NormalizedRelayUrl? = null
                        var foundPrint: AliasProbe.Leader? = null
                        // The url that ANSWERED but too thinly to be a yardstick,
                        // held rather than discarded — see the scheme-twin exit
                        // below, which is the one thing such a window can settle.
                        var thin: NormalizedRelayUrl? = null
                        var thinPrint: AliasProbe.Leader? = null
                        // Urls ASKED to be the yardstick that answered nothing.
                        //
                        // Distinct from "urls the search looked at", which is
                        // what this used to drop from the member walk — and a
                        // url the transport guard DECLINED was never asked. On a
                        // deployment whose Tor is momentarily down, every
                        // candidate ahead of the yardstick is refused without a
                        // dial, and counting those as exhausted silently removed
                        // perfectly foldable urls from the group's measurement
                        // for the whole pass, on our outage rather than their
                        // behaviour.
                        val exhausted = HashSet<NormalizedRelayUrl>()
                        // Urls this pass ASKED, and the subset that ANSWERED —
                        // the two facts [foldUnreadableGroups] turns on. A url
                        // the transport declined is in neither.
                        val askedUrls = HashSet<NormalizedRelayUrl>()
                        val spoke = HashSet<NormalizedRelayUrl>()
                        for (candidate in wanted.take(YARDSTICK_ATTEMPTS)) {
                            var asked = false
                            val attempt =
                                gate.withPermit {
                                    if (!canDial(candidate)) return@withPermit null
                                    asked = true
                                    dialled = true
                                    spent++
                                    taken.incrementAndGet()
                                    sockets.claim(candidate)
                                    try {
                                        probe.leaderPrint(candidate, anchor, onEvent)
                                    } finally {
                                        sockets.release(candidate)
                                    }
                                }
                            if (asked) askedUrls += candidate
                            if (attempt?.spoke == true) spoke += candidate
                            val print = attempt?.leader
                            // Asked, and it said nothing. It failed EVERY filter,
                            // so asking it again as a member this pass buys the
                            // same silence at the price of a dial.
                            if (asked && print == null) exhausted += candidate
                            if (print != null) {
                                // IT ANSWERED, so the search stops here whether
                                // or not the window is usable — the two failures
                                // are facts about different things.
                                //
                                // A url that hands back a window under
                                // [RelayAliases.DEFAULT_MIN_SAMPLE] has told us
                                // about the HOST: it is a relay holding a
                                // handful of events, and its siblings serve the
                                // same handful, so walking down the preference
                                // order buys three thin windows instead of one.
                                // Silence is the opposite — it is about that url
                                // alone, and the url next to it may well be
                                // serving five hundred events happily. Only
                                // silence is worth another attempt.
                                // Judged against the floor for the filter that
                                // produced it. A NIP-29 host's whole list of
                                // groups is a handful of ids and is not "thin"
                                // in the sense this test means — see
                                // [RelayAliases.usableWindow].
                                if (aliases.usableWindow(print.ids, print.kinds)) {
                                    found = candidate
                                    foundPrint = print
                                } else {
                                    thin = candidate
                                    thinPrint = print
                                }
                                break
                            }
                        }
                        // Everything this group will be asked. Narrowed below when
                        // the only url that answered is too thin to measure
                        // against, because then only one member is worth a dial.
                        var members = wanted
                        // A WINDOW TOO THIN TO BE A YARDSTICK STILL DECIDES ONE
                        // THING: its own scheme twin.
                        //
                        // The rule above — a leader under
                        // [RelayAliases.DEFAULT_MIN_SAMPLE] does not drag its
                        // group onto the wire — is right about the group and was
                        // wrong about the pair. Nothing can fold ONTO a nine-event
                        // window on containment and nothing can be cleared against
                        // it, so the members are correctly left alone; but
                        // `wss://x` and `ws://x` are decided by naming one
                        // endpoint and both answering, and nine events are ample
                        // proof of "it answered". Measured on the shape this was
                        // reported for: a host serving a handful of events on both
                        // schemes was permanently undecidable — abandoned here
                        // every pass, at the FRONT of every pass, for a verdict
                        // that costs exactly one more dial.
                        //
                        // So the walk goes on, and no further than the twin: every
                        // other member still has nothing to be measured against.
                        val thinLeader = thin
                        val thinLead = thinPrint
                        if (found == null && thinLeader != null && thinLead != null) {
                            aliases.plainTwinIn(group, thinLeader)?.let { twin ->
                                found = thinLeader
                                foundPrint = thinLead
                                members = listOf(twin)
                            }
                        }
                        // FOLD UNLESS PROVEN DIFFERENT: the group nothing would
                        // read from collapses onto its preferred survivor.
                        //
                        // Everything above this line concludes nothing from
                        // silence. This concludes the opposite by default, and it
                        // is a POLICY rather than a measurement — see
                        // [RelayAliases.foldUnreadable] for the argument and for
                        // the host it gets wrong.
                        //
                        // The yardstick walk only asked three urls, so the rest
                        // are asked here before anything is concluded: "all of
                        // them answer, none of them serves" is a claim about the
                        // WHOLE group and cannot be made from a sample of it.
                        // Concurrently, because no filter has to be agreed —
                        // nothing is being compared, only counted.
                        // ONLY when nothing on the host served anything at all.
                        // A THIN leader is a url that answered with content, so
                        // its group is not "all alike" — and sweeping it here
                        // would also undo the cheap exit that keeps a thin
                        // yardstick from dragging its whole group onto the wire.
                        //
                        // And only while the evidence still points that way. If a
                        // url the yardstick walk already asked did not ANSWER, the
                        // group can never be "all of them answered" — so sweeping
                        // the rest buys nothing and would undo the bound that
                        // keeps a dead host from costing a dial per url. A silent
                        // host still stops at [YARDSTICK_ATTEMPTS], exactly as it
                        // did before this policy existed.
                        if (found == null &&
                            thinLeader == null &&
                            foldUnreadableGroups &&
                            askedUrls.isNotEmpty() &&
                            askedUrls.all { it in spoke }
                        ) {
                            val rest = wanted.filter { it !in askedUrls }
                            val swept = ConcurrentHashMap<NormalizedRelayUrl, AliasProbe.Attempt>()
                            coroutineScope {
                                for (url in rest) {
                                    launch {
                                        gate.withPermit {
                                            if (!canDial(url)) return@withPermit
                                            taken.incrementAndGet()
                                            sockets.claim(url)
                                            try {
                                                swept[url] = probe.leaderPrint(url, anchor, onEvent)
                                            } finally {
                                                sockets.release(url)
                                            }
                                        }
                                    }
                                }
                            }
                            for ((url, attempt) in swept) {
                                if (attempt.spoke) spoke += url
                                if (attempt.leader == null) exhausted += url
                            }
                            // A WINDOW TURNED UP BEYOND THE THIRD ATTEMPT. The
                            // sweep is a wider yardstick search that happens to
                            // have run, so take it rather than throw it away —
                            // and take it in [RelayAliases.PREFERENCE] order, so
                            // the leader does not depend on which dial finished
                            // first. The members are re-dialled below through
                            // this filter, which is what keeps the group
                            // comparable.
                            //
                            // **It must be a window that can be MEASURED against,
                            // which the walk above tests and this did not.** A
                            // sub-floor window still arrives as a [AliasProbe.Leader],
                            // so taking the first one let five ids beat forty
                            // fetched by the same sweep: the thin url led, nothing
                            // could fold onto it, and a host whose other paths
                            // served an identical window took a 24h cooldown.
                            val usable =
                                wanted.firstOrNull { url ->
                                    swept[url]?.leader?.let { aliases.usableWindow(it.ids, it.kinds) } == true
                                }
                            usable?.let { better ->
                                found = better
                                foundPrint = swept.getValue(better).leader
                                exhausted -= better
                            }
                            // ANYTHING served disqualifies the shared-name
                            // default, thin windows included. This is the trap in
                            // the gate above: once a sub-floor window no longer
                            // sets `found`, a host that DID serve would fall
                            // straight through to a rule whose whole premise is
                            // that nothing did.
                            val servedSomething = swept.values.any { it.leader != null }
                            if (found == null && !servedSomething) {
                                // EVERY url, not most of them. One url our own
                                // transport could not reach makes this "we do not
                                // know", and folding on that would turn our
                                // outage into a claim about their server.
                                val everyUrlAnswered = wanted.all { it in spoke }
                                if (everyUrlAnswered && wanted.size > 1) {
                                    val survivor = wanted.first()
                                    val folds = aliases.foldUnreadable(wanted, survivor)
                                    for (alias in folds.keys) {
                                        runCatching { record.publishUnreadable(alias, survivor, wanted.size) }
                                    }
                                    if (folds.isNotEmpty()) {
                                        newVerdicts += folds.keys
                                        clearUndecidable(survivor)
                                        System.err.println(
                                            "router: $label ${RelayAliases.hostOf(survivor.url)} served nothing at any of " +
                                                "${wanted.size} url(s) and every one answered — folded onto ${survivor.url} " +
                                                "on the shared name, WITHOUT a measurement",
                                        )
                                    }
                                    return@launch
                                }
                            }
                        }
                        if (found == null || foundPrint == null) {
                            // Only when something was actually ASKED. A group the
                            // transport guard declined outright was never
                            // measured — our Tor being down is not evidence about
                            // their server — and cooling it down would hold a
                            // foldable host back for a day on our own outage.
                            if (dialled) {
                                markUndecidable(group.first(), startedAtMs)
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.NO_YARDSTICK
                            } else {
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.TRANSPORT
                            }
                            return@launch
                        }
                        // Immutable from here down. The search above has to
                        // assign across an iteration, and Kotlin will not smart
                        // cast a captured `var` inside the lambdas below — so the
                        // null check is paid once, here, rather than at every use.
                        val leader = found
                        val lead = foundPrint
                        prints[leader] = lead.ids

                        coroutineScope {
                            // The yardstick itself, and every url the search
                            // ASKED and got nothing from. Not the ones it merely
                            // looked at: a candidate the transport declined was
                            // never measured and is still worth a dial here.
                            for (url in members.filter { it != leader && it !in exhausted }) {
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
                        val result = aliases.learn(group, leader, prints, lead.kinds)
                        // PROVE THE YARDSTICK BEFORE MAKING A NEGATIVE CLAIM.
                        //
                        // Some relays do not answer the same question the same
                        // way twice, and against one of those every containment
                        // in this group is noise — including the ones that
                        // cleared the 0.5 bar and the ones that missed it.
                        // Measured on `fiatjaf.com`: one url asked twice from
                        // ONE anchor, seconds apart, shared NONE of its ten
                        // events; over a paged walk it self-scored 0.694-0.720
                        // while its two sibling paths scored 0.592 and 0.775
                        // against each other — a cross-url score sitting inside
                        // the band the url scores against itself. Whichever side
                        // of 0.5 a pass happens to land on, it signs the answer
                        // for a month: land low and two urls of one relay are
                        // published as separate relays and never re-probed until
                        // the TTL lapses, which is a duplicate pinned in the
                        // fan-out for thirty days on evidence a re-run
                        // contradicts.
                        //
                        // So: a second walk of the leader, from the SAME anchor
                        // through the SAME filter, and nothing is published
                        // unless it comes back. Paid only where a negative claim
                        // is about to be made — a group that folded cleanly is
                        // making the safe claim and pays nothing — and the
                        // group is forgotten rather than half-kept, so the next
                        // pass starts from the store exactly as if this one had
                        // never run.
                        if (result.distinct.isNotEmpty()) {
                            val again =
                                gate.withPermit {
                                    if (!canDial(leader)) return@withPermit null
                                    taken.incrementAndGet()
                                    sockets.claim(leader)
                                    try {
                                        probe.fingerprint(leader, anchor, lead.kinds, onEvent)
                                    } finally {
                                        sockets.release(leader)
                                    }
                                }
                            if (again == null || !aliases.reproducible(leaderPrint, again)) {
                                val self = again?.let { s -> leaderPrint.count { it in s } } ?: 0
                                aliases.forget(group)
                                // Forgotten means nothing was written down, which
                                // means this group comes back on the next pass —
                                // widest first — and fails the same way. A host
                                // that cannot reproduce its own window today is
                                // very unlikely to manage it in six hours.
                                markUndecidable(leader, startedAtMs)
                                undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOT_REPRODUCIBLE
                                System.err.println(
                                    "router: $label ${RelayAliases.hostOf(leader.url)} cannot reproduce its own window " +
                                        "($self of ${leaderPrint.size} id(s) on a second walk from the same anchor) — " +
                                        "${group.size} url(s) left unmeasured rather than published as ${result.distinct.size} " +
                                        "separate relay(s)",
                                )
                                return@launch
                            }
                        }
                        // This group's share of the pass, kept separately so it
                        // can be written the moment the group is decided. The
                        // pass-wide map is only a counter for the summary line.
                        val verdicts = LinkedHashMap<NormalizedRelayUrl, Fold>()
                        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
                        for ((alias, canonical) in result.folded) {
                            val print = prints[alias].orEmpty()
                            // Against the url it folded ONTO, which since the
                            // cross-member pass is not always the leader — a
                            // minted path can fold onto another minted path when
                            // the leader is a different endpoint. Counting
                            // against the leader there published a number from a
                            // comparison the verdict was not based on.
                            val shared = prints[canonical].orEmpty().count { it in print }
                            newVerdicts += alias
                            verdicts[alias] = Fold(canonical, print.size, shared, alias in result.twins, lead.kinds == RelayAliases.GROUP_METADATA_KINDS)
                        }
                        // The cleared half, and its evidence has to name what was
                        // actually held up against what. This once said "of N
                        // peers on this host" while a member had only ever been
                        // compared to the leader — comparisons that never
                        // happened, in a signed month-long statement about
                        // someone else's server.
                        //
                        // Since the cross-member pass they HAVE all happened:
                        // every cleared url is a cluster head, held up against
                        // the leader and against every other head. So the count
                        // is true again, and it is the count that matters —
                        // "distinct from one endpoint" and "distinct from all
                        // five we found here" are different strengths of claim.
                        for (url in result.distinct) {
                            val print = prints[url].orEmpty()
                            val others = result.distinct.filter { it != url } + listOfNotNull(leader.takeIf { it != url })
                            val best = others.maxOfOrNull { other -> prints[other].orEmpty().count { it in print } } ?: 0
                            cleared[url] = Cleared(print.size, "${others.size} compared endpoint(s) on this host", best)
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
                        // Something was decided here, so an older "could not
                        // measure this host" no longer describes it — and when
                        // NOTHING was decided, this is the fourth way a group
                        // ends with no verdict and the one the first cut of the
                        // cooldown missed. A leader that answered fine while
                        // every member of its group was silent or under
                        // `minSample` compares nothing, so `learn` returns two
                        // empty halves, nothing is published, and the group comes
                        // back widest-first on the very next pass — the exact
                        // starvation this cooldown exists to stop, arriving
                        // through the one door it was not watching.
                        if (verdicts.isNotEmpty() || cleared.isNotEmpty()) {
                            clearUndecidable(leader)
                        } else {
                            markUndecidable(leader, startedAtMs)
                            undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOTHING_COMPARED
                        }
                        for ((alias, v) in verdicts) {
                            runCatching {
                                // Each verdict published with the argument it was
                                // actually made on — see [RelayAliasRecord.publishSecureTwin].
                                if (v.secureTwin) {
                                    // Both flags can be set at once — a `ws://`
                                    // pair on a NIP-29 host — and the twin form
                                    // wins because the pairing is the argument.
                                    // It still has to name what it counted.
                                    record.publishSecureTwin(alias, v.canonical, v.sampled, v.groupList)
                                } else if (v.groupList) {
                                    record.publishGroupList(alias, v.canonical, v.sampled, v.shared)
                                } else {
                                    record.publish(alias, v.canonical, v.sampled, v.shared)
                                }
                            }
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

        // Collapsed once, for the log line and the report both. It is a walk of
        // an in-memory map, and taking it twice would let the two disagree.
        val cleaned = if (probed > 0 || learned > 0 || progress != null) collapse(candidates) else null
        if (cleaned != null && (probed > 0 || learned > 0)) {
            System.err.println(
                "router: $label measured $probed fingerprint(s) ? $learned new alias(es), " +
                    "${candidates.size} url(s) now fold onto ${cleaned.dial.size} relay(s) " +
                    "(${aliases.size()} known, ${cleaned.unmeasured.size} unmeasured) " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        // THE SAME FACTS, WHERE THEY OUTLIVE THE LOG. Everything above reaches
        // stderr, which rotates inside the hour on this deployment — and "how
        // much of the fan-out has the fold not got to yet" is a question asked
        // days later, about a card showing forty urls of one server still being
        // dialled. `unmeasured` is the answer and it is the only number here
        // that says whether the fold is making progress at all.
        progress?.record(
            Processors.Work(
                stream = label,
                candidates = candidates.size,
                unmeasured = cleaned?.unmeasured?.size ?: candidates.size,
                dialled = probed,
                decided = learned,
                undecided = undecidedRows(undecided),
                undecidedOmitted = (undecided.values.distinct().size - Processors.MAX_UNDECIDED_REASONS).coerceAtLeast(0),
            ),
        )
        // WHICH HOSTS THIS PASS LEFT UNFOLDED, AND WHY.
        //
        // Everything above counts what the pass DID. Nothing counted what it
        // did not, and the difference was the whole diagnostic gap: a url still
        // being dialled beside eleven of its siblings is the symptom of five
        // completely different causes — never reached for budget, held on a
        // cooldown, no yardstick, nothing to compare against one, a host that
        // cannot repeat itself — and from outside the process they are one
        // silence. Every theory about a specific host started by guessing which,
        // and a guess is a bad place to start when the alternative is one line.
        //
        // Bounded per reason, because a first pass over a polluted store can
        // leave hundreds: this is a diagnostic, not an inventory. The count is
        // the fact, the examples are the lead.
        if (undecided.isNotEmpty()) {
            val byReason = undecided.entries.groupBy({ it.value }, { it.key })
            System.err.println(
                "router: $label alias pass left ${undecided.size} host(s) undecided — " +
                    Undecided.entries
                        .filter { byReason.containsKey(it) }
                        .joinToString("; ") { reason ->
                            val hosts = byReason.getValue(reason)
                            "${hosts.size} ${reason.reason} (e.g. ${hosts.sorted().take(NAMED_PER_REASON).joinToString()})"
                        },
            )
        }
        return learned
    }

    /**
     * The undecided map as publishable rows: one per reason, biggest first, a
     * few hosts named.
     *
     * In [Undecided]'s own declaration order rather than by size, deliberately —
     * that order runs from "waiting its turn" to "can never be decided", so a
     * reader scanning the rows meets the recoverable causes before the permanent
     * one. The same order the stderr line uses, so the two read alike.
     */
    private fun undecidedRows(undecided: Map<String, Undecided>): List<Processors.Undecided> {
        if (undecided.isEmpty()) return emptyList()
        val byReason = undecided.entries.groupBy({ it.value }, { it.key })
        return Undecided.entries
            .filter { byReason.containsKey(it) }
            .take(Processors.MAX_UNDECIDED_REASONS)
            .map { reason ->
                val hosts = byReason.getValue(reason)
                Processors.Undecided(
                    reason = reason.reason,
                    hosts = hosts.size,
                    // Sorted, so two rollups of one pass publish the same rows.
                    examples = hosts.sorted().take(Processors.MAX_UNDECIDED_EXAMPLES),
                )
            }
    }

    /**
     * Why a pass ended a group with nothing written down.
     *
     * Every one of these is a legitimate outcome — none of them is a failure to
     * be fixed by publishing something anyway, which is the trap the thin-window
     * guard was written for. What they are not is interchangeable: a host on a
     * cooldown will fold on a later pass with no intervention, and a host that
     * cannot reproduce its own window never will. Telling them apart from
     * outside the process is what this exists for.
     */
    private enum class Undecided(
        val reason: String,
    ) {
        /** Held back by [undecidable] from a pass that already failed on it. */
        COOLDOWN("cooling down from an earlier failed pass"),

        /** Our own transport declined every url — see the `canDial` note above. */
        TRANSPORT("declined by our own transport"),

        /** Nothing on the host answered enough to be a yardstick. */
        NO_YARDSTICK("no url that could be a yardstick"),

        /** A yardstick, but every other url was silent or too thin to compare. */
        NOTHING_COMPARED("nothing to hold up against the yardstick"),

        /** The yardstick would not give the same window twice — see [RelayAliases.reproducible]. */
        NOT_REPRODUCIBLE("a host that cannot repeat itself"),
    }

    /**
     * Is this group's host still inside the window a failed pass bought it?
     *
     * Keyed by host, because that is what a group IS — [RelayAliases.unresolved]
     * groups by hostname — and because the thing that could not be measured is
     * the server, not the individual url that happened to lead this time.
     */
    private fun onCooldown(
        group: List<NormalizedRelayUrl>,
        nowMs: Long,
    ): Boolean {
        val host = RelayAliases.hostOf(group.first().url)
        val until = undecidable[host] ?: return false
        // Lapsed: drop it so the map cannot grow without bound over a long run,
        // and let the group through.
        if (nowMs >= until) {
            undecidable.remove(host)
            return false
        }
        return true
    }

    /** This host was dialled and proved nothing. Leave it alone for a while. */
    private fun markUndecidable(
        leader: NormalizedRelayUrl,
        nowMs: Long,
    ) {
        undecidable[RelayAliases.hostOf(leader.url)] = nowMs + undecidableCooldownMs
    }

    /**
     * This host decided something, so whatever an earlier pass could not measure
     * about it no longer applies. Cheap to call on every decided group and it
     * keeps a recovered host from serving out a cooldown it has already
     * disproved.
     */
    private fun clearUndecidable(leader: NormalizedRelayUrl) {
        undecidable.remove(RelayAliases.hostOf(leader.url))
    }

    /** One fold and what it rests on, held until the group is decided. */
    private data class Fold(
        val canonical: NormalizedRelayUrl,
        val sampled: Int,
        val shared: Int,
        /**
         * Decided by the two urls naming one endpoint rather than by their
         * windows — see [RelayAliases.Learned.twins]. It changes nothing about
         * the fold and everything about the evidence published with it.
         */
        val secureTwin: Boolean,
        /**
         * Decided on the host's list of groups rather than on a slice of its
         * feed — see [RelayAliasRecord.publishGroupList]. Same reason
         * [secureTwin] is carried: it changes nothing about the fold and
         * everything about the sentence published with it.
         */
        val groupList: Boolean,
    )

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
        // THE STORE IS AUTHORITATIVE ON EVERY PASS, which is what gives its TTL
        // — and the rules epoch — their teeth. `load` already refuses a record
        // past either, but a verdict adopted while it was still current used to
        // live in memory for the rest of the process: `measured` kept answering
        // true, so the url was never re-probed and the retired verdict was never
        // republished, and the fan-out went on folding on evidence that had
        // ceased to exist anywhere. Skipped entirely when the query FAILS — a
        // store that cannot answer is not a store saying "no verdict", and
        // dropping what we hold on the strength of a failed read would unfold
        // the whole fan-out.
        //
        // ONE PASS, not a bulk forget followed by a bulk adopt: this map is
        // shared by every stream and by the monitor, and the gap between those
        // two walks was a window in which every fold in the store was missing.
        // See [RelayAliases.replace].
        aliases.replace(candidates, held.aliases, held.distinct)
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
         * Probes in flight, for every monitor pass that dials.
         *
         * This was 16, with the note "below the fan-out's own concurrency:
         * this is a side quest" — true when the fold shared its sockets with
         * the streams' fan-out, and a relic after the split. The monitor IS
         * the admission path now: nothing certifies until its passes finish,
         * and the corpus is mostly dead relays whose cost is a timeout, not
         * bandwidth — a 929-url sweep measured at 16 spent half an hour in
         * the fitness dials alone, nearly all of it waiting. The passes are
         * serialized on the monitor's clock, so only one holds this many at
         * a time, and the dispatcher ceiling minus the visit pool's budget
         * leaves room for it comfortably.
         */
        const val DEFAULT_CONCURRENCY = 128

        /**
         * How far down a group's preference order the search for a yardstick
         * goes before the host is given up on for this pass.
         *
         * Three, and the shape of the cost is why it can be small. The attempts
         * are SEQUENTIAL — each has to fail before the next is worth making —
         * so this is the only place in the pass where a dead url delays another
         * dial rather than merely occupying a permit. Against a host that
         * answers nothing, three is three windows of silence in a row on one of
         * the 16 permits; against the onion window ([probeIdleMs]) that is
         * minutes.
         *
         * What it buys is measured on the two shapes this was reported on:
         * every url of `asia.azzamo.net` and of one hidden service serves the
         * same events, so ANY of them is a perfect yardstick and the group folds
         * whole the moment one answers. A host where the first three all refuse
         * is a host where the fourth is not the likely difference.
         *
         * It costs nothing in the common case: the first url answers, the loop
         * runs once, and this is the single leader dial it replaced.
         */
        const val YARDSTICK_ATTEMPTS = 3

        /** Hosts named per reason in the undecided summary. A lead, not an inventory. */
        private const val NAMED_PER_REASON = 3

        /**
         * How long a host that was dialled and could not be decided is left
         * alone — a day, i.e. four passes at [AliasMonitor.DEFAULT_INTERVAL_MS].
         *
         * Long enough that such a host stops crowding out the ones that fold in
         * seconds, short enough that a relay which was merely having a bad
         * afternoon — mid-restart, briefly serving a shuffled window — is back
         * in the fold within a day rather than waiting out
         * [RelayAliasRecord.DEFAULT_TTL_SECONDS].
         *
         * Deliberately far shorter than that TTL: this is the weakest thing the
         * fold records, so it gets the shortest memory. A verdict is a
         * measurement of somebody's server; this is only a note that ours could
         * not take one.
         */
        const val DEFAULT_UNDECIDABLE_COOLDOWN_MS = 24L * 60 * 60 * 1000

        /**
         * Whether a group nothing can be read from folds onto its survivor.
         *
         * ON, which INVERTS this component's oldest default: silence used to
         * decide nothing and now decides sameness. See
         * [RelayAliases.foldUnreadable] — including `filter.nostr.wine`, the
         * measured host it gets wrong.
         */
        const val DEFAULT_FOLD_UNREADABLE_GROUPS = true
    }
}
