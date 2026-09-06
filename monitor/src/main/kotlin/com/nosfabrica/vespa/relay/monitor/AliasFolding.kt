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
    )

    /** Urls in, deduplicated urls out, without dialling anything. */
    suspend fun applyVerdicts(candidates: List<NormalizedRelayUrl>): Collapsed {
        if (candidates.isEmpty()) return Collapsed(candidates, emptyMap(), candidates)
        adopt(candidates)
        return collapse(candidates)
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

        val grouped = adoptWorld(candidates)
        val world = grouped.urls
        if (world.size < 2) return 0
        // Read after the adopt and before the first dial; with `unmeasured` after the pass it makes
        // the card's fraction.
        val fresh = candidates.count { !aliases.measured(it) }

        // Hosts on cooldown are held back, not dropped.
        val startedAtMs = System.currentTimeMillis()
        val all = aliases.unresolved(world)
        val groups = all.filter { group -> !onCooldown(group, startedAtMs) }
        var learned = 0
        var probed = 0
        // Host-keyed, because a group is a host.
        val undecided = ConcurrentHashMap<String, Undecided>()
        for (group in all - groups.toSet()) {
            undecided[RelayAliases.hostOf(group.first().url)] = Undecided.COOLDOWN
        }
        if (groups.isNotEmpty()) {
            progress?.measuring(groups.size, Processors.UNIT_HOST)
            val newVerdicts = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
            val taken = AtomicInteger()
            coroutineScope {
                // Widest group first, so the pass's wall clock clears the most pollution earliest.
                for (group in groups.sortedByDescending { it.size }) {
                    launch {
                        val wanted = aliases.toProbe(group)
                        val prints = ConcurrentHashMap<NormalizedRelayUrl, Set<String>>()
                        // One anchor for the whole group, taken before any of it is dialled.
                        val anchor = AliasProbe.settledAnchor(nowSeconds())

                        // The yardstick goes first, alone: it decides the filter the whole group is asked
                        // through. The search walks down the preference order while urls stay silent.
                        var dialled = false
                        var spent = 0
                        var found: NormalizedRelayUrl? = null
                        var foundPrint: AliasProbe.Leader? = null
                        // Answered, but too thinly to be a yardstick; held for the scheme-twin exit.
                        var thin: NormalizedRelayUrl? = null
                        var thinPrint: AliasProbe.Leader? = null
                        // Asked to be the yardstick and answered nothing; a url the transport
                        // declined was never asked.
                        val exhausted = HashSet<NormalizedRelayUrl>()
                        // Urls this pass asked, and the subset that answered: what
                        // foldUnreadableGroups turns on.
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
                            // Asked and silent, or cut by the deadline: a second dial this pass
                            // buys the same silence.
                            if (asked && print == null) exhausted += candidate
                            if (print != null) {
                                // It answered, so the search stops whether or not the window is usable: a
                                // thin window is a fact about the host, silence about the url.
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
                        var members = wanted
                        // A window too thin to be a yardstick still settles its own scheme twin, and no
                        // further: nothing else can be measured against it.
                        val thinLeader = thin
                        val thinLead = thinPrint
                        if (found == null && thinLeader != null && thinLead != null) {
                            aliases.plainTwinIn(group, thinLeader)?.let { twin ->
                                found = thinLeader
                                foundPrint = thinLead
                                members = listOf(twin)
                            }
                        }
                        // A group nothing would read from folds onto its preferred survivor: a policy, not
                        // a measurement, and only while every url asked so far answered.
                        if (found == null &&
                            thinLeader == null &&
                            foldUnreadableGroups &&
                            askedUrls.isNotEmpty() &&
                            askedUrls.all { it in spoke }
                        ) {
                            // The rest of the group, asked before anything is concluded about the whole of it.
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
                            // A usable window the sweep found is a yardstick, taken in preference order so the
                            // leader does not depend on which dial finished first.
                            val usable =
                                wanted.firstOrNull { url ->
                                    swept[url]?.leader?.let { aliases.usableWindow(it.ids, it.kinds) } == true
                                }
                            usable?.let { better ->
                                found = better
                                foundPrint = swept.getValue(better).leader
                                exhausted -= better
                            }
                            // Anything served, thin windows included, disqualifies the shared-name default.
                            val servedSomething = swept.values.any { it.leader != null }
                            if (found == null && !servedSomething) {
                                // Every url, not most: one our transport could not reach makes this
                                // "we do not know".
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
                            // Cooled down only when something was asked: a group the transport
                            // declined was never measured.
                            if (dialled) {
                                markUndecidable(group.first(), startedAtMs)
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.NO_YARDSTICK
                            } else {
                                undecided[RelayAliases.hostOf(group.first().url)] = Undecided.TRANSPORT
                            }
                            return@launch
                        }
                        // Kotlin will not smart cast a captured `var` inside the lambdas below.
                        val leader = found
                        val lead = foundPrint
                        prints[leader] = lead.ids

                        coroutineScope {
                            // Not the yardstick or the exhausted; a url the transport declined is
                            // still worth a dial.
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
                        // Prove the yardstick before making a negative claim: a second walk from the same
                        // anchor through the same filter, paid only where a negative claim is about to be made.
                        if (result.distinct.isNotEmpty()) {
                            val again =
                                gate.withPermit(leader) {
                                    if (!canDial(leader)) return@withPermit null
                                    taken.incrementAndGet()
                                    dial(leader, sockets) { probe.fingerprint(leader, anchor, lead.kinds, onEvent) }
                                }
                            if (again == null || !aliases.reproducible(leaderPrint, again)) {
                                val self = again?.let { s -> leaderPrint.count { it in s } } ?: 0
                                // Back to what the store says; `forget` would also drop the
                                // verdicts adopted moments ago.
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
                        val verdicts = LinkedHashMap<NormalizedRelayUrl, Fold>()
                        val cleared = LinkedHashMap<NormalizedRelayUrl, Cleared>()
                        for ((alias, canonical) in result.folded) {
                            val print = prints[alias].orEmpty()
                            // Against the url it folded onto, which is not always the leader.
                            val shared = prints[canonical].orEmpty().count { it in print }
                            newVerdicts += alias
                            verdicts[alias] = Fold(canonical, print.size, shared, alias in result.twins, lead.kinds == RelayAliases.GROUP_METADATA_KINDS)
                        }
                        // A cleared url was held up against the leader and every other head; the
                        // count names real comparisons.
                        for (url in result.distinct) {
                            val print = prints[url].orEmpty()
                            val others = result.distinct.filter { it != url } + listOfNotNull(leader.takeIf { it != url })
                            val best = others.maxOfOrNull { other -> prints[other].orEmpty().count { it in print } } ?: 0
                            cleared[url] = Cleared(print.size, "${others.size} compared endpoint(s) on this host", best)
                        }

                        // Written as this group finishes, not when the pass does, so a restart mid-pass keeps
                        // it. A leader that compared nothing ends with no verdict and takes the cooldown.
                        if (verdicts.isNotEmpty() || cleared.isNotEmpty()) {
                            clearUndecidable(leader)
                        } else {
                            markUndecidable(leader, startedAtMs)
                            undecided[RelayAliases.hostOf(leader.url)] = Undecided.NOTHING_COMPARED
                        }
                        for ((alias, v) in verdicts) {
                            guarded {
                                // Both flags can be set at once; the twin form wins because the
                                // pairing is the argument.
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
                        // From the job's completion, because three of the four exits above are a
                        // `return@launch`.
                    }.invokeOnCompletion { progress?.attempted() }
                }
            }
            probed = taken.get()
            learned = newVerdicts.size
        }

        val cleaned = if (probed > 0 || learned > 0 || progress != null) collapse(candidates) else null
        if (cleaned != null && (probed > 0 || learned > 0)) {
            System.err.println(
                "router: $label measured $probed fingerprint(s) ? $learned new alias(es), " +
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
                undecided = undecidedRows(undecided),
            ),
        )
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
     * One verdict write, guarded so a failed write does not take the pass down. Not
     * `runCatching`, which swallows CancellationException.
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
     * Pull the stored verdicts into memory. A store that cannot answer is not "no verdict", so a
     * failed query keeps what we hold.
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
        // The store is authoritative on every pass, which is what gives its TTL its teeth.
        aliases.replace(candidates, held.aliases, held.distinct)
    }

    /**
     * Adopt every verdict the store holds and hand back the set to group: the candidates plus
     * every survivor a verdict names, never the urls that folded away. Falls back to [adopt]
     * when the store cannot answer.
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
     * What a pass grouped, and the store's own answer about it, which the reproducibility guard
     * restores. [held] is null when the store could not answer.
     */
    private class World(
        val urls: List<NormalizedRelayUrl>,
        val held: RelayVerdictRecord.Verdicts?,
    )

    /**
     * The candidate set as the verdicts in memory see it. A fold is applied only where the set
     * holds a survivor; an absent survivor re-elects the best present member, through
     * [Collapsed.standIns] and never [Collapsed.aliases], so the group stays one relay.
     */
    private fun collapse(candidates: List<NormalizedRelayUrl>): Collapsed {
        val present = candidates.toHashSet()
        val elected = reElected(candidates, present)
        val measured = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val inferred = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val dial = ArrayList<NormalizedRelayUrl>(candidates.size)
        val seen = HashSet<NormalizedRelayUrl>(candidates.size)
        for (url in candidates) {
            val canonical = aliases.canonicalOf(url)
            val into = if (canonical in present) canonical else elected[canonical] ?: url
            if (seen.add(into)) dial += into
            if (into == url) continue
            if (into == canonical) measured[url] = into else inferred[url] = into
        }
        return Collapsed(dial, measured, dial.filter { !aliases.measured(it) }, inferred)
    }

    /**
     * Absent survivor -> the member of its group that stands in for it. Keyed by canonical, not
     * host: a host can carry more than one group.
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
        const val DEFAULT_DIAL_CONCURRENCY = MonitorConfig.DEFAULT_DIAL_CONCURRENCY

        /** What a held url of this pass is doing. */
        const val STAGE_FINGERPRINT = "fingerprint"

        /** How far down a group's preference order the sequential search for a yardstick goes. */
        const val YARDSTICK_ATTEMPTS = 3

        private const val NAMED_PER_REASON = 3

        /** Far shorter than the verdict TTL: only a note that our pass could not take a measurement. */
        const val DEFAULT_UNDECIDABLE_COOLDOWN_MS = 24L * 60 * 60 * 1000

        const val DEFAULT_FOLD_UNREADABLE_GROUPS = true
    }
}
