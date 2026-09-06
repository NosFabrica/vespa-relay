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

import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException

/**
 * Every url the monitor measures, derived from the store when a pass runs rather than taken from
 * a cache. The corpus is the union of what [derivations] name and what this router already holds
 * records about; which derivations exist is the operator's declaration, never a stream's.
 */
internal class StreamWorld(
    private val store: IEventStore,
    /** The labelled relay-list derivations to walk, from `RouterConfig.monitorDerivations`. */
    private val derivations: List<Pair<String, RelayDiscoveryConfig>>,
    private val probe: ReachabilityProbe,
    /** Whose `dead` verdicts may hold a url out; never unscoped, because a hold-out forecloses. */
    private val monitorAuthors: List<String>,
    /** This router's own signing identity; the scope of [ownRecords]. */
    private val self: String?,
    private val tor: TorTransport?,
    override val sockets: Sockets,
    /**
     * Where an event a probe dial happened to see goes. The mirror supplies it, because who wants
     * an event is the mirror's question; this plane only hands over what it saw.
     */
    private val onProbeEvent: suspend (Event) -> Unit,
    /** Where this derivation reports. Null in a test asserting the numbers rather than the row. */
    override val progress: Processors.Handle? = null,
) : AliasMonitor.CandidateSource {
    /** What the last derivation started from and dropped. */
    @Volatile
    var lastDerivation: Derivation = Derivation()
        private set

    /** Whether a derivation has run; a fresh [Derivation] and one that found nothing are the same zeros. */
    @Volatile
    var derived: Boolean = false
        private set

    /**
     * One derivation's arithmetic: `sourced = excluded + heldOutDead + candidates`, with
     * [recordedOnly] beside it.
     */
    data class Derivation(
        /** Every url the relay lists yielded, before anything was dropped. */
        val sourced: Int = 0,
        /** Dropped by an operator's instruction: a stream's `exclude` list, or this relay's own url. */
        val excluded: Int = 0,
        /** Dropped by a current `dead` verdict of ours. */
        val heldOutDead: Int = 0,
        /** Urls this router holds a record about that no relay list named this round; outside [sourced]. */
        val recordedOnly: Int = 0,
        /** What the relay lists named the round before, or null on a process's first round. */
        val sourcedLastRound: Int? = null,
        /** What was left for the passes; every number on the pass rows is a share of it. */
        val candidates: Int = 0,
    )

    /** The author-scoped dead set, read from our own `dead` verdicts; [among] null reads the whole hold-out. */
    private suspend fun ownDead(among: Collection<NormalizedRelayUrl>? = null): Set<NormalizedRelayUrl> =
        RelayDiscovery.undialable(
            store,
            monitorAuthors = monitorAuthors,
            maxAgeSeconds = DEAD_TTL_SECONDS,
            allowOnion = tor != null,
            among = among,
        )

    /** Every url one of our own records is about, on the verdict TTL. */
    private suspend fun ownRecords(): Set<NormalizedRelayUrl> =
        RelayDiscovery.recorded(
            store,
            self = self,
            maxAgeSeconds = RelayVerdictRecord.DEFAULT_TTL_SECONDS,
            allowOnion = tor != null,
        )

    /**
     * The sweep's candidate set. Urls a signed record calls dead are held out here rather than
     * declined in [canDial], where the fold would report them as declined by our own transport.
     */
    override suspend fun candidates(): List<NormalizedRelayUrl> {
        val dead = ownDead()
        // One unit per configured source, timed from after the dead-set read.
        progress?.measuring(derivations.sumOf { it.second.sources.size }, Processors.UNIT_SOURCE)
        val all = LinkedHashSet<NormalizedRelayUrl>()
        val excluded = LinkedHashSet<NormalizedRelayUrl>()
        // Only the sweep ticks the position; the fast lane runs the same `derive` and must not move it.
        derive("alias source", { it }, onSource = { progress?.attempted() }) { url, kept ->
            if (kept) all += url else excluded += url
        }
        // `exclude` is per stream: a url one stream excludes and another asks for is a candidate.
        val onlyExcluded = excluded - all
        val recorded = ownRecords()
        val recordedOnly = recorded.filterNot { it in all || it in onlyExcluded }
        val known = all + recordedOnly
        val live = known.filterNot { it in dead }
        lastDerivation =
            Derivation(
                sourced = all.size + onlyExcluded.size,
                excluded = onlyExcluded.size,
                heldOutDead = known.size - live.size,
                recordedOnly = recordedOnly.size,
                candidates = live.size,
                sourcedLastRound = lastSourced,
            )
        derived = true
        // A derivation that collapsed is a fault in the read, not a new baseline. Said once;
        // nothing is retried.
        val previous = lastSourced
        if (previous != null && previous >= SHRINK_FLOOR && all.size < previous * SHRINK_SHARE) {
            System.err.println(
                "router: alias source DERIVED ${all.size} url(s) WHERE THE LAST ROUND DERIVED $previous — " +
                    "a drop of ${(100 - 100 * all.size / previous)}%. The relay lists in the store do not change that " +
                    "fast; suspect the read (a loading content node, a degraded search) before believing the network. " +
                    "The passes still walk ${live.size} url(s), because the corpus is our own records too.",
            )
        }
        lastSourced = all.size
        System.err.println(
            "router: alias source derived ${live.size} url(s) across ${derivations.size} declared source group(s)" +
                "; ${all.size} named by a relay list this round" +
                (if (known.size > live.size) "; ${known.size - live.size} held out as known dead" else "") +
                (
                    lastDerivation.recordedOnly
                        .takeIf { it > 0 }
                        ?.let { "; $it more from our own records that nothing named this round" }
                        .orEmpty()
                ),
        )
        return live
    }

    /** What the last round's relay lists named; null until a round has run. */
    private var lastSourced: Int? = null

    /**
     * One walk over every derivation, [bound] applied to each config first. Discovery is asked
     * for the unfiltered set; `kept` applies the exclude list and the self check here.
     */
    private suspend fun derive(
        what: String,
        bound: (RelayDiscoveryConfig) -> RelayDiscoveryConfig,
        onSource: () -> Unit = {},
        onUrl: (NormalizedRelayUrl, kept: Boolean) -> Unit,
    ) {
        for ((label, discovery) in derivations) {
            // Topped up after a throw: a source we could not read is still behind us.
            var ticked = 0
            val found =
                try {
                    RelayDiscovery.discover(
                        store,
                        bound(discovery).copy(exclude = RelayExcludes.NONE),
                        skip = emptySet(),
                        allowOnion = tor != null,
                        onSource = {
                            ticked++
                            onSource()
                        },
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    System.err.println("router: $what could not derive $label: ${e.message}")
                    emptyList()
                }
            repeat(discovery.sources.size - ticked) { onSource() }
            found.forEach { onUrl(it.url, it.url !in discovery.exclude && it.url != store.relay) }
        }
    }

    /**
     * The fast lane's derivation: the same sources, bounded to relay-list events ingested at or
     * after [since].
     */
    override suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> {
        val fresh = LinkedHashSet<NormalizedRelayUrl>()
        derive("fast lane", { discovery ->
            discovery.copy(sources = discovery.sources.map { it.copy(filter = it.filter.copy(since = since)) })
        }) { url, kept -> if (kept) fresh += url }
        // Derive first, then ask the hold-out about what was found; most ticks find nothing.
        if (fresh.isEmpty()) return emptyList()
        val dead = ownDead(among = fresh)
        return fresh.filterNot { it in dead }
    }

    override suspend fun canDial(url: NormalizedRelayUrl): Boolean = probe.canDial(url)

    /** Handed straight over; whether anything wants it, and on whose word, is the mirror's call. */
    override suspend fun onEvent(event: Event) = onProbeEvent(event)

    companion object {
        const val DEAD_TTL_SECONDS = 24L * 60 * 60

        /** How far a derivation may fall against the round before without being called out. */
        const val SHRINK_SHARE = 0.5

        /** The size below which the shrink comparison is not made. */
        const val SHRINK_FLOOR = 100
    }
}
