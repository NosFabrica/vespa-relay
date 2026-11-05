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
import com.nosfabrica.vespa.relay.fold.Fingerprint
import com.nosfabrica.vespa.relay.fold.FoldReading
import com.nosfabrica.vespa.relay.fold.FoldReading.collapse
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.fold.RelayAliases.Learned
import com.nosfabrica.vespa.relay.fold.ServerIndex
import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.random.Random

/**
 * Which urls are one server: fingerprints the groups nothing is known about on [AliasMonitor]'s
 * schedule and publishes what that proves. Any reader collapses a candidate set off those records,
 * without a socket, through [FoldReading.readBack].
 */
class AliasFolding(
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    /** The monitor's one dial gate, shared with every other pass. */
    private val gate: DialGate = DialGate(DEFAULT_DIAL_CONCURRENCY),
    /** How long a host that could not be decided is left alone. */
    private val undecidableCooldownMs: Long = DEFAULT_UNDECIDABLE_COOLDOWN_MS,
    /** Fold a group every url answered and none would serve from. */
    private val foldUnreadableGroups: Boolean = DEFAULT_FOLD_UNREADABLE_GROUPS,
    /** Where each pass reports, or null; [AliasMonitor] writes the clock to the same handle. */
    override val progress: Processors.Handle? = null,
    /** The wall clock on one verdict write. */
    private val publishDeadlineMs: Long = WriteLimits.PUBLISH_DEADLINE_MS,
    /** The urls the monitor knows, so a narrower read still takes in the other urls on each candidate's server. */
    private val world: () -> ServerIndex = { ServerIndex.EMPTY },
    /** Whether the pre-probe lets a url be fingerprinted. */
    private val canDial: suspend (NormalizedRelayUrl) -> Boolean = { true },
    /** The socket refcount; without one every fingerprint leaves a websocket behind. */
    private val sockets: Sockets = Sockets.NONE,
) : AliasMonitor.Pass {
    /**
     * Hosts a pass dialled and could not decide anything about, and when each is worth trying
     * again. In memory, not signed: "we could not measure this" is a fact about our pass. Read
     * before a pass's groups and written after they join.
     */
    private val undecidable = HashMap<String, Long>()

    /**
     * Fingerprint every group left unresolved once [candidates] are grouped against the whole
     * recorded world, and publish what that proves. Returns how many new aliases it learned.
     */
    override suspend fun measure(
        kind: RunKind,
        candidates: List<NormalizedRelayUrl>,
    ): Int {
        if (candidates.isEmpty()) return 0
        val label = kind.label
        val startedMs = System.currentTimeMillis()

        // The store is authoritative on every pass, which is what gives its TTL its teeth.
        val held = readStanding("fold", label) { readWorld(label, candidates) } ?: return 0
        // The candidates, first so the caller's order holds, and every survivor a verdict names; never the urls that folded away.
        val world = (LinkedHashSet(candidates) + held.aliases.values + held.distinct).toList()
        // A sweep's world is everything the fold can be asked about; a fast-lane batch is a handful of urls.
        if (kind == RunKind.SWEEP) forgetCooldownsBut(world)
        if (world.size < 2) return 0
        val view = FoldReading.view(held, world)
        // With `unmeasured` after the pass it makes the card's fraction.
        val fresh = candidates.count { !view.measured(it) }

        val run = Run(label, System.currentTimeMillis(), newWriter())
        // Hosts on cooldown are held back, not dropped.
        val all = view.unresolved(world)
        val groups = all.filter { group -> !onCooldown(group, run.startedAtMs) }
        val undecided = HashMap<String, Undecided>()
        for (group in all - groups.toSet()) {
            undecided[RelayAliases.hostOf(group.first().url)] = Undecided.COOLDOWN
        }
        val outcomes =
            if (groups.isEmpty()) {
                emptyList()
            } else {
                progress?.measuring(groups.size, Processors.UNIT_HOST)
                coroutineScope {
                    // Widest group first, so the pass's wall clock clears the most pollution earliest.
                    groups
                        .sortedByDescending { it.size }
                        .map { group ->
                            async {
                                try {
                                    measureGroup(run, view, group)
                                } finally {
                                    // In the coroutine, not from `invokeOnCompletion`, which can run after the scope resumes.
                                    progress?.attempted()
                                }
                            }
                        }.awaitAll()
                }
            }
        // The one writer of the pass's state, after the join: each group's delta holds only what the store took.
        var after = view
        var probed = 0
        for (outcome in outcomes) {
            after += outcome.learned
            probed += outcome.dials
            outcome.undecided?.let { undecided[outcome.host] = it }
            when (outcome.cooldown) {
                true -> undecidable[outcome.host] = run.startedAtMs + undecidableCooldownMs
                false -> undecidable.remove(outcome.host)
                null -> Unit
            }
        }
        val learned = outcomes.sumOf { it.learned.folded.size }
        reportUnwritten(label, run.writer)
        val cleaned = if (probed > 0 || learned > 0 || progress != null) collapse(candidates, after) else null
        if (cleaned != null && (probed > 0 || learned > 0)) {
            System.err.println(
                "router: $label measured $probed fingerprint(s) — $learned new alias(es), " +
                    "${candidates.size} url(s) now fold onto ${cleaned.dial.size} relay(s) " +
                    "(${after.size()} known, ${cleaned.unmeasured.size} unmeasured) " +
                    "in ${fmtDuration(System.currentTimeMillis() - startedMs)}",
            )
        }
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
        reportUndecided(label, undecided)
        return learned
    }

    /** What every group of one pass shares. */
    private class Run(
        val label: String,
        val startedAtMs: Long,
        /** One set of write limits for the whole pass, so a store that stops answering stops every group's writes. */
        val writer: VerdictWriter,
    )

    /** One host's group while it is measured, written only on its own coroutine. */
    private class Group(
        val run: Run,
        /** The standing verdicts, the same for every group of the pass. */
        val view: RelayAliases,
        val urls: List<NormalizedRelayUrl>,
    ) {
        val host: String = RelayAliases.hostOf(urls.first().url)

        /** A group is one host, so this is the host's share of the pass's dials. */
        val permits = Semaphore(DIALS_PER_HOST)

        /** Dials this group took. */
        var dials = 0

        /** Why the group ended with nothing written down, if it did. */
        var undecided: Undecided? = null

        /** True starts the host's cooldown, false lifts it, null leaves it. */
        var cooldown: Boolean? = null

        /** What the group proved and the store took. */
        var learned = Learned()

        fun count(attempt: Asked<*>) {
            if (attempt.asked) dials++
        }

        fun count(attempts: List<Pair<NormalizedRelayUrl, Asked<*>>>) {
            for ((_, attempt) in attempts) count(attempt)
        }
    }

    /** One group's outcome, merged into the pass's state after the join. */
    private class Measured(
        val host: String,
        val learned: Learned,
        val undecided: Undecided?,
        val cooldown: Boolean?,
        val dials: Int,
    )

    /** One url's answer, or [asked] false when our transport declined to dial it. */
    private class Asked<T>(
        val asked: Boolean,
        val answer: T?,
    )

    /** What the search for a group's yardstick found, and what it learned about the urls it asked. */
    private class Search {
        var found: NormalizedRelayUrl? = null
        var foundPrint: AliasProbe.Leader? = null

        /** Answered, but too thinly to be a yardstick; held for the scheme-twin exit. */
        var thin: NormalizedRelayUrl? = null
        var thinPrint: AliasProbe.Leader? = null

        /** Asked and answered nothing; a url the transport declined was never asked. */
        val exhausted = HashSet<NormalizedRelayUrl>()

        /** Urls the search asked, and the subset that answered other than by refusing our key. */
        val asked = HashSet<NormalizedRelayUrl>()
        val spoke = HashSet<NormalizedRelayUrl>()
    }

    /** The pass's writer, under the limits every pass's verdict writes share. */
    private fun newWriter() =
        VerdictWriter(
            WriteLimits(deadlineMs = publishDeadlineMs),
            progress,
            STAGE_PUBLISH,
            // The position counts hosts, not writes.
            tick = false,
        )

    /**
     * One answer from [url] through [walk], dialled under its gate permit and the pass's deadline. The
     * pre-probe runs before either permit: it needs no socket, and bounds itself. The caller counts the dial.
     */
    private suspend fun <T> ask(
        group: Group,
        url: NormalizedRelayUrl,
        walk: suspend () -> T,
    ): Asked<T> {
        val writer = group.run.writer
        // A stopped writer refuses whatever this dial could prove, so it is not taken.
        if (writer.stoppedBy != null || !canDial(url)) return Asked(false, null)
        // The host's permit before the pass's: a server's queue must not hold a permit another server could use.
        return group.permits.withPermit {
            gate.withPermit(url) {
                if (writer.stoppedBy != null) return@withPermit Asked(false, null)
                Asked(true, dial(url, walk))
            }
        }
    }

    /** [urls] asked at once through [walk], each answer beside its url, in [urls]' order. */
    private suspend fun <T> askEach(
        group: Group,
        urls: List<NormalizedRelayUrl>,
        walk: suspend (NormalizedRelayUrl) -> T,
    ): List<Pair<NormalizedRelayUrl, Asked<T>>> =
        coroutineScope { urls.map { url -> async { url to ask(group, url) { walk(url) } } }.awaitAll() }
            .also { group.count(it) }

    /** One host's group measured against [view]: what it proved and the store took, and why not where it did not. */
    private suspend fun measureGroup(
        run: Run,
        view: RelayAliases,
        urls: List<NormalizedRelayUrl>,
    ): Measured {
        val group = Group(run, view, urls)
        decide(group)
        return Measured(group.host, group.learned, group.undecided, group.cooldown, group.dials)
    }

    /** Find a yardstick, then hold the rest up against it and publish what that proves. */
    private suspend fun decide(group: Group) {
        if (writesStopped(group)) return
        val wanted = group.view.toProbe(group.urls)
        // One anchor for the whole group, taken before any of it is dialled.
        val anchor = AliasProbe.settledAnchor(nowSeconds())
        val search = searchYardstick(group, wanted, anchor)
        if (writesStopped(group)) return
        var members = wanted
        // A window too thin to be a yardstick still settles its own scheme twin, and no further:
        // nothing else can be measured against it.
        val thin = search.thin
        val thinPrint = search.thinPrint
        if (search.found == null && thin != null && thinPrint != null) {
            group.view.plainTwinIn(group.urls, thin)?.let { twin ->
                search.found = thin
                search.foundPrint = thinPrint
                members = listOf(twin)
            }
        }
        // Too small a store to sample is still a whole store, and a server serving it at any path is one relay.
        if (search.found == null && foldedOnWholeStore(group, wanted, search, anchor)) return
        // A group nothing would read from folds onto its preferred survivor: a policy, not a
        // measurement, and only while every url asked so far answered.
        val unreadable = search.found == null && thin == null && foldUnreadableGroups && search.asked.isNotEmpty() && search.asked.all { it in search.spoke }
        if (unreadable && foldedOnName(group, wanted, search, anchor)) return
        val leader = search.found
        val lead = search.foundPrint
        if (writesStopped(group)) return
        if (leader == null || lead == null) {
            // Cooled down only when something was asked: a group the transport declined was never measured.
            if (search.asked.isNotEmpty()) {
                group.cooldown = true
                group.undecided = Undecided.NO_YARDSTICK
            } else {
                group.undecided = Undecided.TRANSPORT
            }
            return
        }
        // Not the yardstick or the exhausted; a url the transport declined is still worth a dial.
        compareAgainst(group, members.filter { it != leader && it !in search.exhausted }, leader, lead, anchor)
    }

    /**
     * Has the pass's writer stopped: then nothing this group could prove would be written, so it is left
     * unresolved for the next pass, with no cooldown, since the store failed and not the host.
     */
    private fun writesStopped(group: Group): Boolean {
        if (group.run.writer.stoppedBy == null) return false
        group.undecided = Undecided.WRITES_STOPPED
        return true
    }

    /**
     * The yardstick goes first, alone: it decides the filter the whole group is asked through. The
     * search walks down the preference order while urls stay silent or thin.
     */
    private suspend fun searchYardstick(
        group: Group,
        wanted: List<NormalizedRelayUrl>,
        anchor: Long,
    ): Search {
        val search = Search()
        // Two budgets: silence costs a whole deadline, a thin answer one short page.
        var fruitless = 0
        var thinAnswers = 0
        for (candidate in wanted) {
            if (fruitless >= YARDSTICK_ATTEMPTS || thinAnswers >= THIN_ATTEMPTS) break
            val attempt = ask(group, candidate) { probe.leaderPrint(candidate, anchor) }
            group.count(attempt)
            if (attempt.asked) search.asked += candidate
            if (attempt.answer?.answeredForFold == true) search.spoke += candidate
            val print = attempt.answer?.leader
            if (print == null) {
                // Asked and silent, or cut by the deadline: a second dial this pass buys the same silence.
                if (attempt.asked) search.exhausted += candidate
                fruitless++
                continue
            }
            if (group.view.usableWindow(print.ids, print.kinds)) {
                search.found = candidate
                search.foundPrint = print
                break
            }
            // A thin window is a fact about the path, not the host: a path-routing host serves a quiet
            // path beside a busy one. The first is kept for the twin exit.
            thinAnswers++
            if (search.thin == null) {
                search.thin = candidate
                search.thinPrint = print
            }
        }
        return search
    }

    /**
     * Ask the rest of a group nothing has served from, before concluding anything about the whole of
     * it. A usable window found becomes the yardstick; true when the group was folded on its name instead.
     */
    private suspend fun foldedOnName(
        group: Group,
        wanted: List<NormalizedRelayUrl>,
        search: Search,
        anchor: Long,
    ): Boolean {
        val swept =
            askEach(group, wanted.filter { it !in search.asked }) { url -> probe.leaderPrint(url, anchor) }
                .mapNotNull { (url, attempt) -> attempt.answer?.let { url to it } }
                .toMap()
        for ((url, attempt) in swept) {
            if (attempt.answeredForFold) search.spoke += url
            if (attempt.leader == null) search.exhausted += url
        }
        // Taken in preference order, so the leader does not depend on which dial finished first.
        wanted.firstOrNull { url -> swept[url]?.leader?.let { group.view.usableWindow(it.ids, it.kinds) } == true }?.let { better ->
            search.found = better
            search.foundPrint = swept.getValue(better).leader
            search.exhausted -= better
        }
        // Anything served, thin windows included, disqualifies the shared-name default.
        if (search.found != null || swept.values.any { it.leader != null }) return false
        // Every url, not most: one our transport could not reach makes this "we do not know".
        if (wanted.size < 2 || !wanted.all { it in search.spoke }) return false
        val survivor = wanted.first()
        val folds = group.view.foldUnreadable(wanted, survivor)
        val landed = write(group, folds) { record.publishFold(it, survivor, FoldEvidence.unreadable(wanted.size)) != null }
        if (folds.folded.isNotEmpty()) group.cooldown = false
        if (landed.size < folds.folded.size) writesStopped(group)
        if (landed.isNotEmpty()) {
            System.err.println(
                "router: ${group.run.label} ${RelayAliases.hostOf(survivor.url)} served nothing at any of " +
                    "${wanted.size} url(s) and every one answered — ${landed.size} folded onto ${survivor.url} " +
                    "on the shared name, WITHOUT a measurement",
            )
        }
        return true
    }

    /**
     * A group whose first thin window is the whole of its relay's store: a path on the host that no
     * list names is asked through the same filter, and only if it serves that same complete store
     * does the server ignore the path, so every member serving it too folds onto the preferred of them.
     */
    private suspend fun foldedOnWholeStore(
        group: Group,
        wanted: List<NormalizedRelayUrl>,
        search: Search,
        anchor: Long,
    ): Boolean {
        val thin = search.thin ?: return false
        val store = search.thinPrint?.takeIf { it.drained && it.ids.isNotEmpty() } ?: return false
        val control = controlPathOf(thin) ?: return false
        val seen = ask(group, control) { probe.window(control, anchor, store.kinds) }.also { group.count(it) }.answer
        if (seen == null || !servesStore(seen, store)) return false
        val stores =
            askEach(group, wanted.filter { it != thin }) { url -> probe.window(url, anchor, store.kinds) }
                .mapNotNull { (url, attempt) -> attempt.answer?.let { url to it } }
                .toMap()
        val same = listOf(thin) + wanted.filter { url -> stores[url]?.let { servesStore(it, store) } == true }
        if (same.size < 2 || writesStopped(group)) return false
        val survivor = group.view.preferred(same) ?: return false
        val folds = group.view.foldOnto(same, survivor)
        val landed = write(group, folds) { record.publishFold(it, survivor, FoldEvidence.wholeStore(survivor, store.ids.size)) != null }
        if (folds.folded.isNotEmpty()) group.cooldown = false
        if (landed.size < folds.folded.size) writesStopped(group)
        if (landed.isNotEmpty()) {
            System.err.println(
                "router: ${group.run.label} ${RelayAliases.hostOf(survivor.url)} serves one complete store of ${store.ids.size} event(s) " +
                    "at every path, a made-up one included — ${landed.size} url(s) folded onto ${survivor.url}",
            )
        }
        return folds.folded.isNotEmpty()
    }

    /** The same complete store, id for id: a sample would not do, and nor would an empty one. */
    private fun servesStore(
        window: AliasProbe.Window,
        store: AliasProbe.Leader,
    ): Boolean = window.drained && window.ids == store.ids

    /** Fingerprint [members] through the yardstick's filter and publish what the group proved. */
    private suspend fun compareAgainst(
        group: Group,
        members: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        lead: AliasProbe.Leader,
        anchor: Long,
    ) {
        // Each window is held as a Fingerprint from the moment it lands, not as its ids.
        val prints =
            mapOf(leader to Fingerprint.of(lead.ids)) +
                askEach(group, members) { url -> probe.fingerprint(url, anchor, lead.kinds)?.let(Fingerprint::of) }
                    .mapNotNull { (url, attempt) -> attempt.answer?.let { url to it } }
        // Before anything is learned: a member the stopped writer kept us from asking is not a silent one.
        if (writesStopped(group)) return
        val result = group.view.learn(group.urls, leader, prints, lead.kinds)
        if (result.distinct.isNotEmpty() && !yardstickHolds(group, leader, lead, prints.getValue(leader), anchor, result.distinct.size)) return
        val verdicts = LinkedHashMap<NormalizedRelayUrl, Fold>()
        for ((alias, canonical) in result.folded) {
            val sampled = prints[alias]?.size ?: 0
            verdicts[alias] = Fold(canonical, sampled, result.shared[alias] ?: 0, alias in result.twins, lead.kinds == RelayAliases.GROUP_METADATA_KINDS)
        }
        // A cleared url was held up against the leader and every other head; the count names real comparisons.
        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
        for (url in result.distinct) {
            // A set: the leader is often among the distinct urls already.
            val others = (result.distinct + leader) - url
            cleared[url] = Cleared(prints[url]?.size ?: 0, "${others.size} compared endpoint(s) on this host", result.shared[url] ?: 0)
        }
        // A leader that compared nothing ends with no verdict and takes the cooldown.
        if (verdicts.isNotEmpty() || cleared.isNotEmpty()) {
            group.cooldown = false
        } else {
            group.cooldown = true
            group.undecided = Undecided.NOTHING_COMPARED
        }
        publish(group, result, verdicts, cleared)
    }

    /**
     * Prove the yardstick before making a negative claim: a second walk from the same anchor through
     * the same filter. False, with nothing learned and the host cooled down, when it cannot.
     */
    private suspend fun yardstickHolds(
        group: Group,
        leader: NormalizedRelayUrl,
        lead: AliasProbe.Leader,
        leaderPrint: Fingerprint,
        anchor: Long,
        distinct: Int,
    ): Boolean {
        val again = ask(group, leader) { probe.fingerprint(leader, anchor, lead.kinds)?.let(Fingerprint::of) }.also { group.count(it) }.answer
        if (again != null && group.view.reproducible(leaderPrint, again)) return true
        val self = again?.let(leaderPrint::shared) ?: 0
        if (writesStopped(group)) return false
        group.cooldown = true
        group.undecided = Undecided.NOT_REPRODUCIBLE
        System.err.println(
            "router: ${group.run.label} ${RelayAliases.hostOf(leader.url)} cannot reproduce its own window " +
                "($self of ${leaderPrint.size} id(s) on a second walk from the same anchor, " +
                // A short second walk is a relay or our transport cutting it; a full one, a window that moved.
                "${again?.let { "which returned ${it.size}" } ?: "which never answered"}) — " +
                "${group.urls.size} url(s) left unmeasured rather than published as $distinct separate relay(s)",
        )
        return false
    }

    /** A group's verdicts, written as it finishes so a restart mid-pass keeps them; only a fold that landed is learned. */
    private suspend fun publish(
        group: Group,
        result: Learned,
        verdicts: Map<NormalizedRelayUrl, Fold>,
        cleared: Map<NormalizedRelayUrl, Cleared>,
    ) {
        write(group, result) { url ->
            val v = verdicts[url]
            // Both flags can be set at once; the twin form wins because the pairing is the argument.
            when {
                v == null -> cleared[url]?.let { record.publishFold(url, url, FoldEvidence.distinct(it.sampled, it.comparedAgainst, it.bestShared)) }
                v.secureTwin -> record.publishFold(url, v.canonical, FoldEvidence.secureTwin(v.canonical, v.sampled, v.groupList))
                v.groupList -> record.publishFold(url, v.canonical, FoldEvidence.groupList(v.canonical, v.sampled, v.shared))
                else -> record.publishFold(url, v.canonical, FoldEvidence.contained(v.canonical, v.sampled, v.shared))
            } != null
        }
    }

    /**
     * [proved]'s folds and clearances through the pass's writer. The group learns only the ones the
     * store took; the rest stay as the standing verdicts have them. Returns the urls that landed.
     */
    private suspend fun write(
        group: Group,
        proved: Learned,
        publish: suspend (NormalizedRelayUrl) -> Boolean,
    ): Set<NormalizedRelayUrl> {
        val landed = group.run.writer.write((proved.folded.keys + proved.distinct).toList(), publish)
        group.learned =
            Learned(
                folded = proved.folded.filterKeys { it in landed },
                distinct = proved.distinct.filterTo(LinkedHashSet()) { it in landed },
                twins = proved.twins.filterTo(HashSet()) { it in landed },
            )
        return landed
    }

    /** The writes the store did not take this pass; those hosts are measured again next pass. */
    private fun reportUnwritten(
        label: String,
        writer: VerdictWriter,
    ) {
        if (writer.unwritten == 0) return
        System.err.println(
            "router: $label fold — ${writer.unwritten} verdict(s) NOT written (${writer.wedged} hit the write deadline, " +
                "${writer.declined} failed outright, ${writer.refused} never started" +
                (writer.stoppedBy?.let { "; stopped because $it" }.orEmpty()) +
                "); those hosts are measured again next pass",
        )
    }

    /** One line naming why hosts ended undecided, a few examples per reason. */
    private fun reportUndecided(
        label: String,
        undecided: Map<String, Undecided>,
    ) {
        if (undecided.isEmpty()) return
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

    /** The undecided map as publishable rows, in [Undecided]'s declaration order. */
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

    /** Why a pass ended a group with nothing written down. */
    private enum class Undecided(
        val reason: String,
    ) {
        /** Held back by the cooldown from a pass that already failed on it. */
        COOLDOWN("cooling down from an earlier failed pass"),

        /** Our own transport declined every url. */
        TRANSPORT("declined by our own transport"),

        /** Nothing on the host answered enough to be a yardstick. */
        NO_YARDSTICK("no url that could be a yardstick"),

        /** A yardstick, but every other url was silent or too thin to compare. */
        NOTHING_COMPARED("nothing to hold up against the yardstick"),

        /** The yardstick would not give the same window twice. */
        NOT_REPRODUCIBLE("a host that cannot repeat itself"),

        /** The store stopped taking this pass's writes, so nothing was dialled to prove. */
        WRITES_STOPPED("left for the next pass: the store stopped taking writes"),
    }

    /**
     * One dial, bounded by [AliasProbe.deadlineMs], refcounted and named. Called inside
     * `gate.withPermit`, never around it, so the wait for a permit is not charged to the relay.
     * Null is "we did not get an answer", and no verdict is published off it.
     */
    private suspend fun <T> dial(
        url: NormalizedRelayUrl,
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
     * Is this group's host still inside the window a failed pass bought it? Keyed by host: the
     * server is what could not be measured.
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

    /** Drop the cooldowns that have lapsed or whose host left [world]. */
    private fun forgetCooldownsBut(world: List<NormalizedRelayUrl>) {
        val hosts = world.mapTo(HashSet()) { RelayAliases.hostOf(it.url) }
        val nowMs = System.currentTimeMillis()
        undecidable.entries.removeIf { (host, until) -> host !in hosts || nowMs >= until }
    }

    /** One fold and what it rests on, held until the group is decided. */
    private data class Fold(
        val canonical: NormalizedRelayUrl,
        val sampled: Int,
        val shared: Int,
        /** Decided by the two urls naming one endpoint rather than by their windows. */
        val secureTwin: Boolean,
        /** Decided on the host's list of groups rather than a slice of its feed. */
        val groupList: Boolean,
    )

    /** The evidence behind one cleared url, held until the group is decided. */
    private data class Cleared(
        val sampled: Int,
        val comparedAgainst: String,
        val bestShared: Int,
    )

    /**
     * Every verdict we hold, or, when that paged walk fails, the verdicts over [candidates] and the other
     * urls on their servers: narrower, but still a read, so nothing is signed blind. Throws when both fail.
     */
    private suspend fun readWorld(
        label: String,
        candidates: List<NormalizedRelayUrl>,
    ): RelayVerdictRecord.Verdicts =
        try {
            record.loadAll()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("router: fold [$label] — the walk over every verdict failed (${whyUnread(e)}); reading the candidates' own instead")
            record.load(candidates + world().matesOf(candidates))
        }

    companion object {
        const val DEFAULT_DIAL_CONCURRENCY = MonitorConfig.DEFAULT_DIAL_CONCURRENCY

        /**
         * A url on [url]'s host and port that no relay list names: a path made up for this ask alone,
         * so a server answering it as it answers the listed ones is not routing by path.
         */
        internal fun controlPathOf(
            url: NormalizedRelayUrl,
            token: String = randomToken(),
        ): NormalizedRelayUrl? {
            val origin = Regex("^(wss?://[^/?#]+)").find(url.url)?.value ?: return null
            return RelayUrlNormalizer.normalizeOrNull("$origin/$token")
        }

        private fun randomToken(): String = (1..CONTROL_TOKEN_BYTES).joinToString("") { "%02x".format(Random.nextInt(256)) }

        private const val CONTROL_TOKEN_BYTES = 8

        /** What a held url of this pass is doing. */
        const val STAGE_FINGERPRINT = "fingerprint"

        /**
         * A host's dials in flight at once. A server asked for every path together answers the
         * overflow with 429s and 502s, and a member it refused is a member the fold cannot fold.
         */
        const val DIALS_PER_HOST = 4

        /** How many urls of a group the sequential search for a yardstick may find silent or declined. */
        const val YARDSTICK_ATTEMPTS = 3

        /** How many thin answers it may walk past; each costs one short page, not a deadline. */
        const val THIN_ATTEMPTS = 12

        private const val NAMED_PER_REASON = 3

        /** Far shorter than the verdict TTL: only a note that our pass could not take a measurement. */
        const val DEFAULT_UNDECIDABLE_COOLDOWN_MS = 24L * 60 * 60 * 1000

        const val DEFAULT_FOLD_UNREADABLE_GROUPS = true
    }
}

/** The evidence each kind of fold signs beside its `same-as`. The words are published: a change is a change to every record. */
internal object FoldEvidence {
    /** The alias's window found in the canonical's. */
    fun contained(
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): String = "$sampled newest events, $shared shared with ${canonical.url}"

    /** The one fold whose evidence is not a containment: a `ws://` url and the `wss://` url of the same endpoint, both answering. */
    fun secureTwin(
        canonical: NormalizedRelayUrl,
        sampled: Int,
        /** True when the pair's windows came through [RelayAliases.GROUP_METADATA_KINDS]. */
        groupList: Boolean,
    ): String =
        "same endpoint as ${canonical.url} over TLS, both answered; " +
            if (groupList) "$sampled group definitions here" else "$sampled newest events here"

    /** A fold decided on a relay's complete list of groups rather than a slice of its feed. */
    fun groupList(
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): String = "same group list as ${canonical.url}: $shared of $sampled group definitions shared"

    /** Both urls, and a path on their host no relay list names, served the same complete store: only a server ignoring the path does. */
    fun wholeStore(
        canonical: NormalizedRelayUrl,
        events: Int,
    ): String = "the same complete store of $events event(s) as ${canonical.url}, as a path no relay list names also serves"

    /** The weakest fold: these urls share a host, every one answered, none would serve anything. */
    fun unreadable(urls: Int): String = "nothing readable at any of $urls url(s) on this host; folded on the shared name, not on a measurement"

    /** Fingerprinted and matched nothing it was held up against, signed as `same-as` its own url; [comparedAgainst] names what that was. */
    fun distinct(
        sampled: Int,
        comparedAgainst: String,
        bestShared: Int,
    ): String = "$sampled newest events, best $bestShared shared with $comparedAgainst"
}
