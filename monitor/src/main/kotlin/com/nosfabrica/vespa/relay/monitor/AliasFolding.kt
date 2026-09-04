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

import com.nosfabrica.vespa.relay.config.MonitorConfig
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The fold, in two halves that run at different times.
 *
 * [applyVerdicts] reads: it loads the verdicts already written down and
 * collapses the candidate set, one `#d` query per 500 urls and no sockets, so
 * it can sit on the fan-out's critical path. [measure] dials: it fingerprints
 * the groups nothing is known about and publishes what it learns, on
 * [AliasMonitor]'s own schedule. The two communicate through the store and
 * nothing else, which is what makes the split safe across a restart and lets
 * a second router share the work.
 *
 * The cost of [measure] is bounded by [concurrency] alone, and each verdict
 * stands for [RelayVerdictRecord.DEFAULT_TTL_SECONDS]. Folding lags discovery
 * by one pass: a url's first cycle dials it, the fold takes hold on the next.
 * A pass that leaves a host unfolded says which host and why ([Undecided]).
 */
class AliasFolding(
    private val aliases: RelayAliases,
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    private val concurrency: Int = DEFAULT_DIAL_CONCURRENCY,
    /** How long a host that could not be decided is left alone. See [undecidable]. */
    private val undecidableCooldownMs: Long = DEFAULT_UNDECIDABLE_COOLDOWN_MS,
    /** Fold a group every url answered and none would serve from. See [RelayAliases.foldUnreadable]. */
    private val foldUnreadableGroups: Boolean = DEFAULT_FOLD_UNREADABLE_GROUPS,
    /**
     * Where each pass reports how far it got, or null to say nothing. Handed
     * in because [AliasMonitor] writes the clock to the same handle.
     */
    val progress: Processors.Handle? = null,
    /** The proxy, for the gate only: a `.onion` waits on the Tor dispatcher's permits. See [DialGate]. */
    tor: TorTransport? = null,
) {
    /** One gate object, not one per pass: [AliasMonitor] serialises passes. */
    private val gate = DialGate.over(concurrency, tor)

    /**
     * Hosts a pass dialled and could not decide anything about, and the moment
     * each becomes worth trying again.
     *
     * Nothing is written down for such a host, by design, so
     * [RelayAliases.unresolved] hands it back every pass, and groups are probed
     * widest first, so the hosts that can never be decided would otherwise
     * lead every pass. In memory rather than signed: "we could not measure
     * this" is a fact about our pass, not about somebody's server.
     */
    private val undecidable = ConcurrentHashMap<String, Long>()

    /**
     * What a set of urls collapses to. [aliases] is handed back rather than
     * applied because only the caller knows what else it keys by url.
     *
     * [aliases] and [standIns] are separate because one of them gets signed:
     * `FitnessPass` publishes an `l=alias` record for every entry it is handed.
     * Take [aliases] to publish, take both to route.
     */
    data class Collapsed(
        /** The urls worth dialling: canonical, plus everything still unmeasured. */
        val dial: List<NormalizedRelayUrl>,
        /** Folded url -> the survivor a probe compared it against, present in this set. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        /** Urls with no verdict either way. A subset of [dial]; the only safe reading is to dial them. */
        val unmeasured: List<NormalizedRelayUrl>,
        /**
         * Folded url -> the member standing in for a survivor absent from this
         * set. Routing only: the two were each measured against the missing
         * canonical and never against each other, so no caller may publish it.
         */
        val standIns: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
    )

    /**
     * Urls in, deduplicated urls out, without dialling anything. A single url
     * is still worth asking about: the verdict may say it is somebody else's
     * second address.
     */
    suspend fun applyVerdicts(candidates: List<NormalizedRelayUrl>): Collapsed {
        if (candidates.isEmpty()) return Collapsed(candidates, emptyMap(), candidates)
        adopt(candidates)
        return collapse(candidates)
    }

    /**
     * Fingerprint every group left unresolved once [candidates] are grouped
     * against the whole recorded world ([adoptWorld]), [concurrency] at a
     * time, and publish what that proves. Returns how many new aliases it
     * learned.
     *
     * [canDial] is the caller's transport guard. [onEvent] receives everything
     * the probes download. [sockets] is the caller's connection refcount;
     * without one every fingerprint leaves a websocket behind.
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

        // Grouped over the whole recorded world, so a url arriving alone on a
        // host we have measured is held against what we know about that host.
        val grouped = adoptWorld(candidates)
        val world = grouped.urls
        if (world.size < 2) return 0
        // Read after the adopt and before the first dial, over the candidates
        // the caller is waiting on: with `unmeasured` after the pass it makes
        // the fraction the card shows.
        val fresh = candidates.count { !aliases.measured(it) }

        // Hosts on cooldown are held back, not dropped. See [undecidable].
        val startedAtMs = System.currentTimeMillis()
        val all = aliases.unresolved(world)
        val groups = all.filter { group -> !onCooldown(group, startedAtMs) }
        var learned = 0
        var probed = 0
        // Host-keyed, because a group is a host. See [Undecided].
        val undecided = ConcurrentHashMap<String, Undecided>()
        for (group in all - groups.toSet()) {
            undecided[RelayAliases.hostOf(group.first().url)] = Undecided.COOLDOWN
        }
        if (groups.isNotEmpty()) {
            // Counted in hosts, because a host is what this pass decides.
            progress?.measuring(groups.size, Processors.UNIT_HOST)
            val newVerdicts = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
            val taken = AtomicInteger()
            coroutineScope {
                // Widest group first, so the pass's wall clock clears the most
                // pollution earliest.
                for (group in groups.sortedByDescending { it.size }) {
                    launch {
                        val wanted = aliases.toProbe(group)
                        val prints = ConcurrentHashMap<NormalizedRelayUrl, Set<String>>()
                        // One anchor for the whole group, taken before any of
                        // it is dialled and held behind the clock. See
                        // [AliasProbe.ANCHOR_LAG_SECONDS].
                        val anchor = AliasProbe.settledAnchor(nowSeconds())

                        // The yardstick goes first, alone. It decides the
                        // filter the whole group is asked through, and whether
                        // to ask at all: a group with no usable yardstick can
                        // never fold. The search walks down the preference
                        // order while urls stay silent, capped at
                        // [YARDSTICK_ATTEMPTS], because the preferred url can
                        // be the one that will not answer.
                        var dialled = false
                        var spent = 0
                        var found: NormalizedRelayUrl? = null
                        var foundPrint: AliasProbe.Leader? = null
                        // A url that answered, but too thinly to be a
                        // yardstick. Held for the scheme-twin exit below.
                        var thin: NormalizedRelayUrl? = null
                        var thinPrint: AliasProbe.Leader? = null
                        // Urls asked to be the yardstick that answered nothing.
                        // A url the transport guard declined was never asked
                        // and stays in the member walk.
                        val exhausted = HashSet<NormalizedRelayUrl>()
                        // Urls this pass asked, and the subset that answered:
                        // the two facts [foldUnreadableGroups] turns on.
                        val askedUrls = HashSet<NormalizedRelayUrl>()
                        val spoke = HashSet<NormalizedRelayUrl>()
                        for (candidate in wanted.take(YARDSTICK_ATTEMPTS)) {
                            var asked = false
                            val attempt =
                                gate.withPermit(candidate) {
                                    if (!canDial(candidate)) return@withPermit null
                                    asked = true
                                    dialled = true
                                    spent++
                                    taken.incrementAndGet()
                                    dial(candidate, sockets) { probe.leaderPrint(candidate, anchor, onEvent) }
                                }
                            if (asked) askedUrls += candidate
                            if (attempt?.spoke == true) spoke += candidate
                            val print = attempt?.leader
                            // Asked and silent on every filter, or cut by the
                            // deadline: a second dial this pass buys the same
                            // silence. Pass-local, never published.
                            if (asked && print == null) exhausted += candidate
                            if (print != null) {
                                // It answered, so the search stops whether or
                                // not the window is usable: a thin window is a
                                // fact about the host, silence about the url.
                                // Judged against the floor for the filter that
                                // produced it. See [RelayAliases.usableWindow].
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
                        // Narrowed below when the only url that answered is
                        // too thin to measure against.
                        var members = wanted
                        // A window too thin to be a yardstick still decides
                        // its own scheme twin: that pair is settled by naming
                        // one endpoint and both answering. No further than the
                        // twin, since nothing else can be measured against it.
                        val thinLeader = thin
                        val thinLead = thinPrint
                        if (found == null && thinLeader != null && thinLead != null) {
                            aliases.plainTwinIn(group, thinLeader)?.let { twin ->
                                found = thinLeader
                                foundPrint = thinLead
                                members = listOf(twin)
                            }
                        }
                        // Fold unless proven different: a group nothing would
                        // read from collapses onto its preferred survivor. A
                        // policy, not a measurement; see
                        // [RelayAliases.foldUnreadable].
                        //
                        // Only when nothing on the host served anything, and
                        // only while every url asked so far answered: one
                        // silent url means the group can never be "all of
                        // them answered", so a dead host still stops at
                        // [YARDSTICK_ATTEMPTS].
                        if (found == null &&
                            thinLeader == null &&
                            foldUnreadableGroups &&
                            askedUrls.isNotEmpty() &&
                            askedUrls.all { it in spoke }
                        ) {
                            // The rest of the group, asked before anything is
                            // concluded about the whole of it. Concurrently,
                            // because nothing is being compared.
                            val rest = wanted.filter { it !in askedUrls }
                            val swept = ConcurrentHashMap<NormalizedRelayUrl, AliasProbe.Attempt>()
                            coroutineScope {
                                for (url in rest) {
                                    launch {
                                        gate.withPermit(url) {
                                            if (!canDial(url)) return@withPermit
                                            taken.incrementAndGet()
                                            dial(url, sockets) { probe.leaderPrint(url, anchor, onEvent) }?.let { swept[url] = it }
                                        }
                                    }
                                }
                            }
                            for ((url, attempt) in swept) {
                                if (attempt.spoke) spoke += url
                                if (attempt.leader == null) exhausted += url
                            }
                            // A usable window beyond the third attempt is a
                            // yardstick the sweep found; taken in preference
                            // order, so the leader does not depend on which
                            // dial finished first. It must be measurable, or a
                            // thin url would lead and nothing could fold onto it.
                            val usable =
                                wanted.firstOrNull { url ->
                                    swept[url]?.leader?.let { aliases.usableWindow(it.ids, it.kinds) } == true
                                }
                            usable?.let { better ->
                                found = better
                                foundPrint = swept.getValue(better).leader
                                exhausted -= better
                            }
                            // Anything served, thin windows included,
                            // disqualifies the shared-name default.
                            val servedSomething = swept.values.any { it.leader != null }
                            if (found == null && !servedSomething) {
                                // Every url, not most: one our transport could
                                // not reach makes this "we do not know".
                                val everyUrlAnswered = wanted.all { it in spoke }
                                if (everyUrlAnswered && wanted.size > 1) {
                                    val survivor = wanted.first()
                                    val folds = aliases.foldUnreadable(wanted, survivor)
                                    for (alias in folds.keys) {
                                        guarded { record.publishUnreadable(alias, survivor, wanted.size) }
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
                            // Cooled down only when something was asked: a
                            // group the transport declined was never measured.
                            if (dialled) {
                                markUndecidable(group.first(), startedAtMs)
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.NO_YARDSTICK
                            } else {
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.TRANSPORT
                            }
                            return@launch
                        }
                        // Kotlin will not smart cast a captured `var` inside
                        // the lambdas below.
                        val leader = found
                        val lead = foundPrint
                        prints[leader] = lead.ids

                        coroutineScope {
                            // Skipping the yardstick and the urls the search
                            // asked and got nothing from; a candidate the
                            // transport declined is still worth a dial.
                            for (url in members.filter { it != leader && it !in exhausted }) {
                                launch {
                                    gate.withPermit(url) {
                                        if (!canDial(url)) return@withPermit
                                        taken.incrementAndGet()
                                        dial(url, sockets) { probe.fingerprint(url, anchor, lead.kinds, onEvent) }?.let { prints[url] = it }
                                    }
                                }
                            }
                        }
                        val leaderPrint = lead.ids
                        val result = aliases.learn(group, leader, prints, lead.kinds)
                        // Prove the yardstick before making a negative claim:
                        // a second walk of the leader from the same anchor
                        // through the same filter. Paid only where a negative
                        // claim is about to be made, and the group is put back
                        // as the store had it rather than half-kept. See
                        // [RelayAliases.reproducible].
                        if (result.distinct.isNotEmpty()) {
                            val again =
                                gate.withPermit(leader) {
                                    if (!canDial(leader)) return@withPermit null
                                    taken.incrementAndGet()
                                    dial(leader, sockets) { probe.fingerprint(leader, anchor, lead.kinds, onEvent) }
                                }
                            if (again == null || !aliases.reproducible(leaderPrint, again)) {
                                val self = again?.let { s -> leaderPrint.count { it in s } } ?: 0
                                // Back to what the store says, which undoes
                                // exactly this pass's learnings about this
                                // group. `forget` would also drop the verdicts
                                // adopted moments ago.
                                grouped.held
                                    ?.let { aliases.replace(group, it.aliases, it.distinct) }
                                    ?: aliases.forget(group)
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
                        // This group's share of the pass, written the moment
                        // the group is decided.
                        val verdicts = LinkedHashMap<NormalizedRelayUrl, Fold>()
                        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
                        for ((alias, canonical) in result.folded) {
                            val print = prints[alias].orEmpty()
                            // Against the url it folded onto, which since the
                            // cross-member pass is not always the leader.
                            val shared = prints[canonical].orEmpty().count { it in print }
                            newVerdicts += alias
                            verdicts[alias] = Fold(canonical, print.size, shared, alias in result.twins, lead.kinds == RelayAliases.GROUP_METADATA_KINDS)
                        }
                        // Every cleared url is a cluster head, held up against
                        // the leader and every other head, so the count names
                        // comparisons that actually happened.
                        for (url in result.distinct) {
                            val print = prints[url].orEmpty()
                            val others = result.distinct.filter { it != url } + listOfNotNull(leader.takeIf { it != url })
                            val best = others.maxOfOrNull { other -> prints[other].orEmpty().count { it in print } } ?: 0
                            cleared[url] = Cleared(print.size, "${others.size} compared endpoint(s) on this host", best)
                        }

                        // Written as this group finishes, not when the pass
                        // does: a cold-store pass runs for a quarter of an
                        // hour and a restart would lose every fingerprint in
                        // it. A leader that compared nothing is the fourth way
                        // a group ends with no verdict, and takes the cooldown.
                        if (verdicts.isNotEmpty() || cleared.isNotEmpty()) {
                            clearUndecidable(leader)
                        } else {
                            markUndecidable(leader, startedAtMs)
                            undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOTHING_COMPARED
                        }
                        for ((alias, v) in verdicts) {
                            guarded {
                                // Each verdict published with the argument it
                                // was made on. Both flags can be set at once;
                                // the twin form wins because the pairing is
                                // the argument.
                                if (v.secureTwin) {
                                    record.publishSecureTwin(alias, v.canonical, v.sampled, v.groupList)
                                } else if (v.groupList) {
                                    record.publishGroupList(alias, v.canonical, v.sampled, v.shared)
                                } else {
                                    record.publish(alias, v.canonical, v.sampled, v.shared)
                                }
                            }
                        }
                        for ((url, c) in cleared) {
                            guarded { record.publishDistinct(url, c.sampled, c.comparedAgainst, c.bestShared) }
                        }
                        // From the job's completion, because three of the four
                        // exits above are a `return@launch`.
                    }.invokeOnCompletion { progress?.attempted() }
                }
            }
            probed = taken.get()
            learned = newVerdicts.size
        }

        // Collapsed once, for the log line and the report both.
        val cleaned = if (probed > 0 || learned > 0 || progress != null) collapse(candidates) else null
        if (cleaned != null && (probed > 0 || learned > 0)) {
            System.err.println(
                "router: $label measured $probed fingerprint(s) ? $learned new alias(es), " +
                    "${candidates.size} url(s) now fold onto ${cleaned.dial.size} relay(s) " +
                    "(${aliases.size()} known, ${cleaned.unmeasured.size} unmeasured) " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
        // The same facts where they outlive the log: `unmeasured` is the one
        // number that says whether the fold is making progress.
        progress?.record(
            Processors.Work(
                stream = label,
                candidates = candidates.size,
                newUrls = fresh,
                unmeasured = cleaned?.unmeasured?.size ?: candidates.size,
                dialled = probed,
                decided = learned,
                undecided = undecidedRows(undecided),
            ),
        )
        // Which hosts this pass left unfolded, and why. Bounded per reason:
        // the count is the fact, the examples are the lead.
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
     * The undecided map as publishable rows, in [Undecided]'s declaration
     * order: from "waiting its turn" to "can never be decided", the same order
     * the stderr line uses.
     */
    private fun undecidedRows(undecided: Map<String, Undecided>): List<Processors.Undecided> {
        if (undecided.isEmpty()) return emptyList()
        val byReason = undecided.entries.groupBy({ it.value }, { it.key })
        return Undecided.entries
            .filter { byReason.containsKey(it) }
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
     * Why a pass ended a group with nothing written down. Every one is a
     * legitimate outcome, and they are not interchangeable: a cooldown folds on
     * a later pass, a host that cannot reproduce its window never will.
     */
    private enum class Undecided(
        val reason: String,
    ) {
        /** Held back by [undecidable] from a pass that already failed on it. */
        COOLDOWN("cooling down from an earlier failed pass"),

        /** Our own transport declined every url. */
        TRANSPORT("declined by our own transport"),

        /** Nothing on the host answered enough to be a yardstick. */
        NO_YARDSTICK("no url that could be a yardstick"),

        /** A yardstick, but every other url was silent or too thin to compare. */
        NOTHING_COMPARED("nothing to hold up against the yardstick"),

        /** The yardstick would not give the same window twice. See [RelayAliases.reproducible]. */
        NOT_REPRODUCIBLE("a host that cannot repeat itself"),
    }

    /**
     * One dial, bounded by [AliasProbe.deadlineMs], refcounted and named:
     * every socket this pass opens goes through here. Called inside
     * `gate.withPermit`, never around it, so the wait for a permit is not
     * charged to the relay.
     *
     * Null is "we did not get an answer": the group goes undecided and no
     * verdict is ever published off this clock.
     */
    private suspend fun <T> dial(
        url: NormalizedRelayUrl,
        sockets: Sockets,
        walk: suspend () -> T,
    ): T? =
        withTimeoutOrNull(probe.deadlineMs(url)) {
            progress?.holding(url.url, STAGE_FINGERPRINT)
            sockets.claim(url)
            try {
                walk()
            } finally {
                sockets.release(url)
                progress?.released(url.url)
            }
        }

    /**
     * One verdict write, guarded: a failure to write must not take the pass
     * down, and the url re-earns its verdict next pass. Not `runCatching`,
     * which swallows CancellationException.
     */
    private suspend fun guarded(write: suspend () -> Unit) {
        try {
            write()
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
        }
    }

    /**
     * Is this group's host still inside the window a failed pass bought it?
     * Keyed by host, because the thing that could not be measured is the
     * server, not the url that happened to lead.
     */
    private fun onCooldown(
        group: List<NormalizedRelayUrl>,
        nowMs: Long,
    ): Boolean {
        val host = RelayAliases.hostOf(group.first().url)
        val until = undecidable[host] ?: return false
        // Lapsed: dropped so the map cannot grow without bound.
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

    /** This host decided something, so an earlier cooldown no longer applies. */
    private fun clearUndecidable(leader: NormalizedRelayUrl) {
        undecidable.remove(RelayAliases.hostOf(leader.url))
    }

    /** One fold and what it rests on, held until the group is decided. */
    private data class Fold(
        val canonical: NormalizedRelayUrl,
        val sampled: Int,
        val shared: Int,
        /** Decided by the two urls naming one endpoint rather than by their windows. See [RelayAliases.Learned.twins]. */
        val secureTwin: Boolean,
        /** Decided on the host's list of groups rather than a slice of its feed. See [RelayVerdictRecord.publishGroupList]. */
        val groupList: Boolean,
    )

    /** The evidence behind one cleared url, held until the group is decided. */
    private data class Cleared(
        val sampled: Int,
        val comparedAgainst: String,
        val bestShared: Int,
    )

    /**
     * Pull both halves of the stored verdict into memory. A store that cannot
     * answer is not "no verdict", so a failed query keeps what we hold rather
     * than unfolding the fan-out.
     */
    private suspend fun adopt(candidates: List<NormalizedRelayUrl>) {
        val held =
            try {
                record.load(candidates)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                return
            }
        // The store is authoritative on every pass, which is what gives its
        // TTL its teeth. One pass, not a bulk forget and a bulk adopt: this
        // map is shared by every stream. See [RelayAliases.replace].
        aliases.replace(candidates, held.aliases, held.distinct)
    }

    /**
     * Adopt every verdict the store holds and hand back the set to group: the
     * candidates, plus every survivor a verdict names. A url arriving alone on
     * a host whose siblings dropped out of the candidate set would otherwise
     * be a group of one and dial as its own relay while a record folding it
     * sits unread.
     *
     * Survivors only, never the urls that folded away: they can contribute
     * nothing, and a group whose survivor is absent would hand [RelayAliases.leaderOf]
     * a known duplicate to lead it. Falls back to [adopt] when the store
     * cannot answer.
     */
    private suspend fun adoptWorld(candidates: List<NormalizedRelayUrl>): World {
        val held =
            try {
                record.loadAll()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                adopt(candidates)
                return World(candidates, null)
            }
        // Candidates first, so the world keeps the caller's order.
        val world = LinkedHashSet(candidates)
        world += held.aliases.values
        world += held.distinct
        aliases.replace(world, held.aliases, held.distinct)
        return World(world.toList(), held)
    }

    /**
     * What a pass grouped, and the store's own answer about it. [held] is
     * carried because the reproducibility guard has to put the store's
     * verdicts back; null when the store could not answer, and then there is
     * nothing to restore from.
     */
    private class World(
        val urls: List<NormalizedRelayUrl>,
        val held: RelayVerdictRecord.Verdicts?,
    )

    /**
     * The candidate set as the verdicts currently in memory see it. Pure, so
     * the numbers [measure] logs are the numbers the next apply will produce.
     *
     * A fold is only applied where the set holds a survivor to apply it to.
     * Every consumer applies [Collapsed.aliases] by dropping the alias, and a
     * verdict can name a survivor this caller never asked about (held out as
     * dead, gone from the relay list). Dropping the alias then would take the
     * relay out of the fan-out with nothing put back.
     *
     * An absent survivor re-elects rather than unfolds: the best present
     * member by [RelayAliases.preferred] stands in and the rest fold onto it,
     * so the group stays one relay. The re-election leaves by
     * [Collapsed.standIns], never [Collapsed.aliases], because nothing has
     * measured the members against each other. A present but un-dialable
     * survivor, and an elected member the caller's own gate then drops, are
     * deliberately not handled here.
     */
    private fun collapse(candidates: List<NormalizedRelayUrl>): Collapsed {
        val present = candidates.toHashSet()
        val elected = reElected(candidates, present)
        // One pass and one `canonicalOf` per url; this set is re-collapsed in
        // front of every roster tick.
        val measured = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val inferred = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val dial = ArrayList<NormalizedRelayUrl>(candidates.size)
        val seen = HashSet<NormalizedRelayUrl>(candidates.size)
        for (url in candidates) {
            val canonical = aliases.canonicalOf(url)
            // The recorded survivor while present, the elected stand-in while
            // not, and the url itself when it is nobody's duplicate.
            val into = if (canonical in present) canonical else elected[canonical] ?: url
            if (seen.add(into)) dial += into
            if (into == url) continue
            if (into == canonical) measured[url] = into else inferred[url] = into
        }
        return Collapsed(dial, measured, dial.filter { !aliases.measured(it) }, inferred)
    }

    /**
     * Absent survivor -> the member of its group that stands in for it here.
     * Keyed by the missing canonical rather than by host: a host can carry
     * more than one group. A group of one elects itself, which is a no-op.
     */
    private fun reElected(
        candidates: List<NormalizedRelayUrl>,
        present: Set<NormalizedRelayUrl>,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val groups = HashMap<NormalizedRelayUrl, MutableList<NormalizedRelayUrl>>()
        for (url in candidates) {
            val canonical = aliases.canonicalOf(url)
            if (canonical == url || canonical in present) continue
            groups.getOrPut(canonical) { ArrayList() } += url
        }
        val elected = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>(groups.size)
        for ((canonical, members) in groups) {
            aliases.preferred(members)?.let { elected[canonical] = it }
        }
        return elected
    }

    companion object {
        /** Probes in flight. `monitor { concurrency }` is the operator's knob; this serves callers built without one. */
        const val DEFAULT_DIAL_CONCURRENCY = MonitorConfig.DEFAULT_DIAL_CONCURRENCY

        /** What a held url of this pass is doing. See [Processors.Holding.Held.stage]. */
        const val STAGE_FINGERPRINT = "fingerprint"

        /**
         * How far down a group's preference order the search for a yardstick
         * goes. The attempts are sequential, so this is the one place a dead
         * url delays another dial; a host where three urls refuse is a host
         * where the fourth is not the likely difference.
         */
        const val YARDSTICK_ATTEMPTS = 3

        /** Hosts named per reason in the undecided summary. A lead, not an inventory. */
        private const val NAMED_PER_REASON = 3

        /**
         * A day, four passes at [AliasMonitor.DEFAULT_INTERVAL_MS]. Far shorter
         * than the verdict TTL: this is only a note that our pass could not
         * take a measurement, so it gets the shortest memory.
         */
        const val DEFAULT_UNDECIDABLE_COOLDOWN_MS = 24L * 60 * 60 * 1000

        /** Whether a group nothing can be read from folds onto its survivor. See [RelayAliases.foldUnreadable]. */
        const val DEFAULT_FOLD_UNREADABLE_GROUPS = true
    }
}
