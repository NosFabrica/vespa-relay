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
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * The fold, in two halves: [applyVerdicts] reads the verdicts already written and collapses a
 * candidate set without a socket, so it can sit on the fan-out's critical path; [measure]
 * fingerprints the groups nothing is known about on [AliasMonitor]'s schedule and publishes.
 */
class AliasFolding(
    private val aliases: RelayAliases,
    private val record: RelayVerdictRecord,
    private val probe: AliasProbe,
    private val concurrency: Int = DEFAULT_DIAL_CONCURRENCY,
    /** How long a host that could not be decided is left alone. */
    private val undecidableCooldownMs: Long = DEFAULT_UNDECIDABLE_COOLDOWN_MS,
    /** Fold a group every url answered and none would serve from. */
    private val foldUnreadableGroups: Boolean = DEFAULT_FOLD_UNREADABLE_GROUPS,
    /** Where each pass reports, or null; [AliasMonitor] writes the clock to the same handle. */
    val progress: Processors.Handle? = null,
    /** The proxy, for the gate only. */
    tor: TorTransport? = null,
    /** The wall clock on one verdict write. */
    private val publishDeadlineMs: Long = VerdictWrites.PUBLISH_DEADLINE_MS,
    /** The urls the monitor knows, so a narrower read still takes in the other urls on each candidate's server. */
    private val world: () -> ServerIndex = { ServerIndex.EMPTY },
) {
    /** One gate object, not one per pass. */
    private val gate = DialGate.over(concurrency, tor)

    /**
     * Hosts a pass dialled and could not decide anything about, and when each is worth trying
     * again. In memory, not signed: "we could not measure this" is a fact about our pass.
     */
    private val undecidable = ConcurrentHashMap<String, Long>()

    /**
     * What a set of urls collapses to. [aliases] and [standIns] are separate because
     * `FitnessPass` signs an `l=alias` record for every [aliases] entry: take [aliases] to
     * publish, both to route.
     */
    data class Collapsed(
        /** The urls worth dialling: canonical, plus everything still unmeasured. */
        val dial: List<NormalizedRelayUrl>,
        /** Folded url -> the survivor a probe compared it against, present in this set. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        /** Urls with no verdict either way. A subset of [dial]; the only safe reading is to dial them. */
        val unmeasured: List<NormalizedRelayUrl>,
        /** Folded url -> the member standing in for an absent survivor. Routing only, never published. */
        val standIns: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Undecided url -> the url of its server standing in until the fold decides. Routing only, never published. */
        val waiting: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
    )

    /**
     * Urls in, deduplicated urls out, without dialling anything or changing what the fold holds.
     * [world] is every url the monitor knows: a canonical there is folded onto even when the set
     * lacks it, and a waiting url's stand-in is the one fitness grades.
     */
    suspend fun applyVerdicts(
        candidates: List<NormalizedRelayUrl>,
        world: ServerIndex = ServerIndex.EMPTY,
    ): Collapsed {
        if (candidates.isEmpty()) return Collapsed(candidates, emptyMap(), candidates)
        val mates = world.matesOf(candidates)
        val held =
            try {
                record.load(candidates + mates)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // A store that cannot answer is not "no verdict": read what the fold holds, and hold
                // nothing back, since who stands in is a reading of the store.
                return collapse(candidates, aliases)
            }
        // A map of its own: a narrower set replaced into [aliases] would drop the canonicals of
        // aliases outside it mid-fold. Only [measure] replaces.
        val known = RelayAliases().also { it.replace(candidates + mates, held.aliases, held.distinct) }
        val collapsed = collapse(candidates, known, present = candidates.toHashSet() + mates)
        val electorate: (NormalizedRelayUrl) -> Boolean = { world.isEmpty() || it in world }
        return collapsed.copy(waiting = known.heldForFold(collapsed.dial, mates, eligibleIn(held, electorate)))
    }

    /**
     * Fingerprint every group left unresolved once [candidates] are grouped against the whole
     * recorded world, and publish what that proves. Returns how many new aliases it learned.
     * [sockets] is the caller's refcount; without one every fingerprint leaves a websocket behind.
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

        val grouped = adoptWorld(label, candidates) ?: return 0
        val world = grouped.urls
        // A sweep's world is everything the fold can be asked about; a fast-lane batch is a handful of urls.
        if (label != AliasMonitor.FAST_LANE) forgetAllBut(world)
        if (world.size < 2) return 0
        // Read after the adopt and before the first dial; with `unmeasured` after the pass it makes
        // the card's fraction.
        val fresh = candidates.count { !aliases.measured(it) }

        val run = Run(label, canDial, onEvent, sockets, grouped.held, System.currentTimeMillis(), newWriter())
        // Hosts on cooldown are held back, not dropped.
        val all = aliases.unresolved(world)
        val groups = all.filter { group -> !onCooldown(group, run.startedAtMs) }
        for (group in all - groups.toSet()) {
            run.undecided[RelayAliases.hostOf(group.first().url)] = Undecided.COOLDOWN
        }
        if (groups.isNotEmpty()) {
            progress?.measuring(groups.size, Processors.UNIT_HOST)
            coroutineScope {
                // Widest group first, so the pass's wall clock clears the most pollution earliest.
                for (group in groups.sortedByDescending { it.size }) {
                    launch {
                        try {
                            measureGroup(run, group)
                        } finally {
                            // In the launch, not from `invokeOnCompletion`, which can run after the scope resumes.
                            progress?.attempted()
                        }
                    }
                }
            }
        }
        val probed = run.taken.get()
        val learned = run.newVerdicts.size
        reportUnwritten(label, run.writer)
        val cleaned = if (probed > 0 || learned > 0 || progress != null) collapse(candidates, aliases) else null
        if (cleaned != null && (probed > 0 || learned > 0)) {
            System.err.println(
                "router: $label measured $probed fingerprint(s) — $learned new alias(es), " +
                    "${candidates.size} url(s) now fold onto ${cleaned.dial.size} relay(s) " +
                    "(${aliases.size()} known, ${cleaned.unmeasured.size} unmeasured) " +
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
                undecided = undecidedRows(run.undecided),
            ),
        )
        reportUndecided(label, run.undecided)
        return learned
    }

    /** One pass's state, shared by the groups it measures concurrently. */
    private class Run(
        val label: String,
        val canDial: suspend (NormalizedRelayUrl) -> Boolean,
        val onEvent: suspend (Event) -> Unit,
        val sockets: Sockets,
        /** The store's own answer at the start of the pass, which the reproducibility guard restores. */
        val held: RelayVerdictRecord.Verdicts,
        val startedAtMs: Long,
        /** One set of write limits for the whole pass, so a store that stops answering stops every group's writes. */
        val writer: VerdictWriter,
    ) {
        /** Aliases whose write the store took. */
        val newVerdicts: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

        /** Why each host ended with nothing written down; host-keyed, because a group is a host. */
        val undecided = ConcurrentHashMap<String, Undecided>()

        /** Dials this pass took. */
        val taken = AtomicInteger()

        /** Each host's share of the pass's dials, so a server is never asked for every path at once. */
        private val hosts = ConcurrentHashMap<String, Semaphore>()

        fun hostGate(url: NormalizedRelayUrl): Semaphore = hosts.computeIfAbsent(RelayAliases.hostOf(url.url)) { Semaphore(DIALS_PER_HOST) }
    }

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
            VerdictWrites.WRITE_CONCURRENCY,
            publishDeadlineMs,
            VerdictWrites.PUBLISH_WEDGE_LIMIT,
            VerdictWrites.PUBLISH_WEDGE_BUDGET_MS,
            progress,
            VerdictWrites.STAGE_PUBLISH,
            // The position counts hosts, not writes.
            tick = false,
        )

    /** One answer from [url] through [walk], dialled under its gate permit and the pass's deadline. */
    private suspend fun <T> ask(
        run: Run,
        url: NormalizedRelayUrl,
        walk: suspend () -> T,
    ): Asked<T> =
        // The host's permit before the pass's: a server's queue must not hold a permit another server could use.
        run.hostGate(url).withPermit {
            gate.withPermit(url) {
                // A stopped writer refuses whatever this dial could prove, so it is not taken.
                if (run.writer.stoppedBy != null || !run.canDial(url)) return@withPermit Asked(false, null)
                run.taken.incrementAndGet()
                Asked(true, dial(url, run.sockets, walk))
            }
        }

    /** One host's group: find a yardstick, then hold the rest up against it and publish what that proves. */
    private suspend fun measureGroup(
        run: Run,
        group: List<NormalizedRelayUrl>,
    ) {
        if (writesStopped(run, group)) return
        val wanted = aliases.toProbe(group)
        // One anchor for the whole group, taken before any of it is dialled.
        val anchor = AliasProbe.settledAnchor(nowSeconds())
        val search = searchYardstick(run, wanted, anchor)
        if (writesStopped(run, group)) return
        var members = wanted
        // A window too thin to be a yardstick still settles its own scheme twin, and no further:
        // nothing else can be measured against it.
        val thin = search.thin
        val thinPrint = search.thinPrint
        if (search.found == null && thin != null && thinPrint != null) {
            aliases.plainTwinIn(group, thin)?.let { twin ->
                search.found = thin
                search.foundPrint = thinPrint
                members = listOf(twin)
            }
        }
        // Too small a store to sample is still a whole store, and a server serving it at any path is one relay.
        if (search.found == null && foldedOnWholeStore(run, wanted, search, anchor)) return
        // A group nothing would read from folds onto its preferred survivor: a policy, not a
        // measurement, and only while every url asked so far answered.
        val unreadable = search.found == null && thin == null && foldUnreadableGroups && search.asked.isNotEmpty() && search.asked.all { it in search.spoke }
        if (unreadable && foldedOnName(run, wanted, search, anchor)) return
        val leader = search.found
        val lead = search.foundPrint
        if (writesStopped(run, group)) return
        if (leader == null || lead == null) {
            // Cooled down only when something was asked: a group the transport declined was never measured.
            val host = RelayAliases.hostOf(group.first().url)
            if (search.asked.isNotEmpty()) {
                markUndecidable(group.first(), run.startedAtMs)
                run.undecided[host] = Undecided.NO_YARDSTICK
            } else {
                run.undecided[host] = Undecided.TRANSPORT
            }
            return
        }
        // Not the yardstick or the exhausted; a url the transport declined is still worth a dial.
        compareAgainst(run, group, members.filter { it != leader && it !in search.exhausted }, leader, lead, anchor)
    }

    /**
     * Has the pass's writer stopped: then nothing this group could prove would be written, so it is left
     * unresolved for the next pass, with no cooldown, since the store failed and not the host.
     */
    private fun writesStopped(
        run: Run,
        group: List<NormalizedRelayUrl>,
    ): Boolean {
        if (run.writer.stoppedBy == null) return false
        run.undecided[RelayAliases.hostOf(group.first().url)] = Undecided.WRITES_STOPPED
        return true
    }

    /**
     * The yardstick goes first, alone: it decides the filter the whole group is asked through. The
     * search walks down the preference order while urls stay silent or thin.
     */
    private suspend fun searchYardstick(
        run: Run,
        wanted: List<NormalizedRelayUrl>,
        anchor: Long,
    ): Search {
        val search = Search()
        // Two budgets: silence costs a whole deadline, a thin answer one short page.
        var fruitless = 0
        var thinAnswers = 0
        for (candidate in wanted) {
            if (fruitless >= YARDSTICK_ATTEMPTS || thinAnswers >= THIN_ATTEMPTS) break
            val attempt = ask(run, candidate) { probe.leaderPrint(candidate, anchor, run.onEvent) }
            if (attempt.asked) search.asked += candidate
            if (attempt.answer?.answeredForFold == true) search.spoke += candidate
            val print = attempt.answer?.leader
            if (print == null) {
                // Asked and silent, or cut by the deadline: a second dial this pass buys the same silence.
                if (attempt.asked) search.exhausted += candidate
                fruitless++
                continue
            }
            if (aliases.usableWindow(print.ids, print.kinds)) {
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
        run: Run,
        wanted: List<NormalizedRelayUrl>,
        search: Search,
        anchor: Long,
    ): Boolean {
        val swept = ConcurrentHashMap<NormalizedRelayUrl, AliasProbe.Attempt>()
        coroutineScope {
            for (url in wanted.filter { it !in search.asked }) {
                launch { ask(run, url) { probe.leaderPrint(url, anchor, run.onEvent) }.answer?.let { swept[url] = it } }
            }
        }
        for ((url, attempt) in swept) {
            if (attempt.answeredForFold) search.spoke += url
            if (attempt.leader == null) search.exhausted += url
        }
        // Taken in preference order, so the leader does not depend on which dial finished first.
        wanted.firstOrNull { url -> swept[url]?.leader?.let { aliases.usableWindow(it.ids, it.kinds) } == true }?.let { better ->
            search.found = better
            search.foundPrint = swept.getValue(better).leader
            search.exhausted -= better
        }
        // Anything served, thin windows included, disqualifies the shared-name default.
        if (search.found != null || swept.values.any { it.leader != null }) return false
        // Every url, not most: one our transport could not reach makes this "we do not know".
        if (wanted.size < 2 || !wanted.all { it in search.spoke }) return false
        val survivor = wanted.first()
        val folds = aliases.foldUnreadable(wanted, survivor)
        val landed = run.writer.write(folds.keys.toList()) { record.publishUnreadable(it, survivor, wanted.size) != null }
        run.newVerdicts += landed
        if (folds.isNotEmpty()) clearUndecidable(survivor)
        if (landed.size < folds.size) writesStopped(run, wanted)
        if (landed.isNotEmpty()) {
            System.err.println(
                "router: ${run.label} ${RelayAliases.hostOf(survivor.url)} served nothing at any of " +
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
        run: Run,
        wanted: List<NormalizedRelayUrl>,
        search: Search,
        anchor: Long,
    ): Boolean {
        val thin = search.thin ?: return false
        val store = search.thinPrint?.takeIf { it.drained && it.ids.isNotEmpty() } ?: return false
        val control = controlPathOf(thin) ?: return false
        val seen = ask(run, control) { probe.window(control, anchor, store.kinds, run.onEvent) }.answer
        if (seen == null || !servesStore(seen, store)) return false
        val stores = ConcurrentHashMap<NormalizedRelayUrl, AliasProbe.Window>()
        coroutineScope {
            for (url in wanted.filter { it != thin }) {
                launch { ask(run, url) { probe.window(url, anchor, store.kinds, run.onEvent) }.answer?.let { stores[url] = it } }
            }
        }
        val same = listOf(thin) + wanted.filter { url -> stores[url]?.let { servesStore(it, store) } == true }
        if (same.size < 2 || writesStopped(run, same)) return false
        val survivor = aliases.preferred(same) ?: return false
        val folds = aliases.foldOnto(same, survivor)
        val landed = run.writer.write(folds.keys.toList()) { record.publishWholeStore(it, survivor, store.ids.size) != null }
        run.newVerdicts += landed
        if (folds.isNotEmpty()) clearUndecidable(survivor)
        if (landed.size < folds.size) writesStopped(run, same)
        if (landed.isNotEmpty()) {
            System.err.println(
                "router: ${run.label} ${RelayAliases.hostOf(survivor.url)} serves one complete store of ${store.ids.size} event(s) " +
                    "at every path, a made-up one included — ${landed.size} url(s) folded onto ${survivor.url}",
            )
        }
        return folds.isNotEmpty()
    }

    /** The same complete store, id for id: a sample would not do, and nor would an empty one. */
    private fun servesStore(
        window: AliasProbe.Window,
        store: AliasProbe.Leader,
    ): Boolean = window.drained && window.ids == store.ids

    /** Fingerprint [members] through the yardstick's filter and publish what the group proved. */
    private suspend fun compareAgainst(
        run: Run,
        group: List<NormalizedRelayUrl>,
        members: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        lead: AliasProbe.Leader,
        anchor: Long,
    ) {
        val prints = ConcurrentHashMap<NormalizedRelayUrl, Set<String>>()
        prints[leader] = lead.ids
        coroutineScope {
            for (url in members) {
                launch { ask(run, url) { probe.fingerprint(url, anchor, lead.kinds, run.onEvent) }.answer?.let { prints[url] = it } }
            }
        }
        // Before anything is learned: a member the stopped writer kept us from asking is not a silent one.
        if (writesStopped(run, group)) return
        val result = aliases.learn(group, leader, prints, lead.kinds)
        if (result.distinct.isNotEmpty() && !yardstickHolds(run, group, leader, lead, anchor, result.distinct.size)) return
        val verdicts = LinkedHashMap<NormalizedRelayUrl, Fold>()
        for ((alias, canonical) in result.folded) {
            val print = prints[alias].orEmpty()
            // Against the url it folded onto, which is not always the leader.
            val shared = prints[canonical].orEmpty().count { it in print }
            verdicts[alias] = Fold(canonical, print.size, shared, alias in result.twins, lead.kinds == RelayAliases.GROUP_METADATA_KINDS)
        }
        // A cleared url was held up against the leader and every other head; the count names real comparisons.
        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
        for (url in result.distinct) {
            val print = prints[url].orEmpty()
            val others = result.distinct.filter { it != url } + listOfNotNull(leader.takeIf { it != url })
            val best = others.maxOfOrNull { other -> prints[other].orEmpty().count { it in print } } ?: 0
            cleared[url] = Cleared(print.size, "${others.size} compared endpoint(s) on this host", best)
        }
        // A leader that compared nothing ends with no verdict and takes the cooldown.
        if (verdicts.isNotEmpty() || cleared.isNotEmpty()) {
            clearUndecidable(leader)
        } else {
            markUndecidable(leader, run.startedAtMs)
            run.undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOTHING_COMPARED
        }
        publish(run, verdicts, cleared)
    }

    /**
     * Prove the yardstick before making a negative claim: a second walk from the same anchor through
     * the same filter. False, with the store's answer restored and the host cooled down, when it cannot.
     */
    private suspend fun yardstickHolds(
        run: Run,
        group: List<NormalizedRelayUrl>,
        leader: NormalizedRelayUrl,
        lead: AliasProbe.Leader,
        anchor: Long,
        distinct: Int,
    ): Boolean {
        val leaderPrint = lead.ids
        val again = ask(run, leader) { probe.fingerprint(leader, anchor, lead.kinds, run.onEvent) }.answer
        if (again != null && aliases.reproducible(leaderPrint, again)) return true
        val self = again?.let { s -> leaderPrint.count { it in s } } ?: 0
        // Back to what the store says.
        aliases.replace(group, run.held.aliases, run.held.distinct)
        if (writesStopped(run, group)) return false
        markUndecidable(leader, run.startedAtMs)
        run.undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOT_REPRODUCIBLE
        System.err.println(
            "router: ${run.label} ${RelayAliases.hostOf(leader.url)} cannot reproduce its own window " +
                "($self of ${leaderPrint.size} id(s) on a second walk from the same anchor, " +
                // A short second walk is a relay or our transport cutting it; a full one, a window that moved.
                "${again?.let { "which returned ${it.size}" } ?: "which never answered"}) — " +
                "${group.size} url(s) left unmeasured rather than published as $distinct separate relay(s)",
        )
        return false
    }

    /** A group's verdicts, written as it finishes so a restart mid-pass keeps them; only a fold that landed is learned. */
    private suspend fun publish(
        run: Run,
        verdicts: Map<NormalizedRelayUrl, Fold>,
        cleared: Map<NormalizedRelayUrl, Cleared>,
    ) {
        val landed =
            run.writer.write((verdicts.keys + cleared.keys).toList()) { url ->
                val v = verdicts[url]
                // Both flags can be set at once; the twin form wins because the pairing is the argument.
                when {
                    v == null -> cleared[url]?.let { record.publishDistinct(url, it.sampled, it.comparedAgainst, it.bestShared) }
                    v.secureTwin -> record.publishSecureTwin(url, v.canonical, v.sampled, v.groupList)
                    v.groupList -> record.publishGroupList(url, v.canonical, v.sampled, v.shared)
                    else -> record.publish(url, v.canonical, v.sampled, v.shared)
                } != null
            }
        run.newVerdicts += verdicts.keys.filter { it in landed }
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
        /** Held back by [undecidable] from a pass that already failed on it. */
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

    /** Drop what is held about urls outside [world], and cooldowns that have lapsed or whose host left it. */
    private fun forgetAllBut(world: List<NormalizedRelayUrl>) {
        aliases.retainOnly(world)
        val hosts = world.mapTo(HashSet()) { RelayAliases.hostOf(it.url) }
        val nowMs = System.currentTimeMillis()
        undecidable.entries.removeIf { (host, until) -> host !in hosts || nowMs >= until }
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
     * Adopt every verdict the store holds and hand back the set to group: the candidates plus every
     * survivor a verdict names, never the urls that folded away. Null when the store cannot answer.
     */
    private suspend fun adoptWorld(
        label: String,
        candidates: List<NormalizedRelayUrl>,
    ): World? {
        // The store is authoritative on every pass, which is what gives its TTL its teeth.
        val held = readStanding("fold", label) { readWorld(label, candidates) } ?: return null
        // Candidates first, so the world keeps the caller's order.
        val world = LinkedHashSet(candidates)
        world += held.aliases.values
        world += held.distinct
        aliases.replace(world, held.aliases, held.distinct)
        return World(world.toList(), held)
    }

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
            System.err.println("router: fold [$label] — the walk over every verdict failed (${e.javaClass.simpleName}); reading the candidates' own instead")
            record.load(candidates + world().matesOf(candidates))
        }

    /** What a pass grouped, and the store's own answer about it, which the reproducibility guard restores. */
    private class World(
        val urls: List<NormalizedRelayUrl>,
        val held: RelayVerdictRecord.Verdicts,
    )

    /**
     * The candidate set as the verdicts in [known] see it. A fold is applied only where the set
     * holds a survivor; an absent survivor re-elects the best present member, through
     * [Collapsed.standIns] and never [Collapsed.aliases], so the group stays one relay.
     */
    private fun collapse(
        candidates: List<NormalizedRelayUrl>,
        known: RelayAliases,
        /** Where a survivor counts as present; the candidates, unless the caller can route elsewhere. */
        present: Set<NormalizedRelayUrl> = candidates.toHashSet(),
    ): Collapsed {
        val elected = reElected(candidates, present, known)
        val measured = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val inferred = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val dial = ArrayList<NormalizedRelayUrl>(candidates.size)
        val seen = HashSet<NormalizedRelayUrl>(candidates.size)
        for (url in candidates) {
            val canonical = known.canonicalOf(url)
            val into = if (canonical in present) canonical else elected[canonical] ?: url
            if (seen.add(into)) dial += into
            if (into == url) continue
            if (into == canonical) measured[url] = into else inferred[url] = into
        }
        return Collapsed(dial, measured, dial.filter { !known.measured(it) }, inferred)
    }

    /**
     * Absent survivor -> the member of its group that stands in for it. Keyed by canonical, not
     * host: a host can carry more than one group.
     */
    private fun reElected(
        candidates: List<NormalizedRelayUrl>,
        present: Set<NormalizedRelayUrl>,
        known: RelayAliases,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val groups = HashMap<NormalizedRelayUrl, MutableList<NormalizedRelayUrl>>()
        for (url in candidates) {
            val canonical = known.canonicalOf(url)
            if (canonical == url || canonical in present) continue
            groups.getOrPut(canonical) { ArrayList() } += url
        }
        val elected = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>(groups.size)
        for ((canonical, members) in groups) {
            known.preferred(members)?.let { elected[canonical] = it }
        }
        return elected
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

        /**
         * Every candidate a fold in [held] names, onto the url it folds onto, whether or not that
         * url is among them: a known alias is graded `alias` whatever else is in the batch.
         */
        fun foldsAmong(
            held: RelayVerdictRecord.Verdicts,
            candidates: List<NormalizedRelayUrl>,
        ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
            val known = RelayAliases().also { it.replace(candidates, held.aliases, held.distinct) }
            return candidates.mapNotNull { url -> known.canonicalOf(url).takeIf { it != url }?.let { url to it } }.toMap()
        }

        /**
         * Every candidate no fold in [held] decides that another url of its server, among
         * [candidates] and [world], stands in for: fitness grades the stand-in alone until the fold decides.
         */
        fun heldAmong(
            held: RelayVerdictRecord.Verdicts,
            candidates: List<NormalizedRelayUrl>,
            world: Collection<NormalizedRelayUrl> = emptyList(),
        ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
            val known = RelayAliases().also { it.replace((candidates + world).distinct(), held.aliases, held.distinct) }
            return known.heldForFold(candidates, world, eligibleIn(held) { true })
        }

        /**
         * Who may stand in for a server, one rule for fitness and the roster: a url [electorate]
         * admits that stands ungraded or `prime`. A refusal moves the stand-in on to the next url,
         * or one refusing path would keep the whole server off the roster.
         */
        private fun eligibleIn(
            held: RelayVerdictRecord.Verdicts,
            electorate: (NormalizedRelayUrl) -> Boolean,
        ): (NormalizedRelayUrl) -> Boolean =
            { url ->
                electorate(url) && url !in held.inconsistent && held.fitness[url]?.value.let { it == null || it == Verdict.PRIME.value }
            }

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
