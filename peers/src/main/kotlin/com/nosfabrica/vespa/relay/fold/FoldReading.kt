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
package com.nosfabrica.vespa.relay.fold

import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Verdict
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException

/** The duplicate-url fold as kind-30166 records state it: what any reader routes on, with no probe. */
object FoldReading {
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
     * Urls in, deduplicated urls out, read from the verdicts alone: what a fold's `applyVerdicts`
     * answers, for a caller with no fold of its own. Null when the store cannot answer.
     */
    suspend fun readBack(
        record: RelayVerdictRecord,
        candidates: List<NormalizedRelayUrl>,
        world: ServerIndex = ServerIndex.EMPTY,
    ): Collapsed? {
        if (candidates.isEmpty()) return Collapsed(candidates, emptyMap(), candidates)
        val mates = world.matesOf(candidates)
        val held =
            try {
                record.load(candidates + mates)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                return null
            }
        // A map of its own: a narrower set replaced into a fold's aliases would drop the canonicals
        // of aliases outside it mid-fold. Only [measure] replaces.
        val known = RelayAliases().also { it.replace(candidates + mates, held.aliases, held.distinct) }
        val collapsed = collapse(candidates, known, present = candidates.toHashSet() + mates)
        val electorate: (NormalizedRelayUrl) -> Boolean = { world.isEmpty() || it in world }
        return collapsed.copy(waiting = known.heldForFold(collapsed.dial, mates, eligibleIn(held, electorate)))
    }

    /**
     * The candidate set as the verdicts in [known] see it. A fold is applied only where the set
     * holds a survivor; an absent survivor re-elects the best present member, through
     * [Collapsed.standIns] and never [Collapsed.aliases], so the group stays one relay.
     */
    fun collapse(
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

    /**
     * Who may stand in for a server, one rule for fitness and the roster: a url [electorate]
     * admits that stands ungraded or `prime`. A refusal moves the stand-in on to the next url,
     * or one refusing path would keep the whole server off the roster.
     */
    fun eligibleIn(
        held: RelayVerdictRecord.Verdicts,
        electorate: (NormalizedRelayUrl) -> Boolean,
    ): (NormalizedRelayUrl) -> Boolean =
        { url ->
            electorate(url) && url !in held.inconsistent && held.fitness[url]?.value.let { it == null || it == Verdict.PRIME.value }
        }
}
