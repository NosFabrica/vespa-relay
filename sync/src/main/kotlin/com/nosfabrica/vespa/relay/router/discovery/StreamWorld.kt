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

import com.nosfabrica.vespa.relay.router.IngestPipeline
import com.nosfabrica.vespa.relay.router.TorTransport
import com.nosfabrica.vespa.relay.router.config.MonitorConfig
import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import kotlinx.coroutines.CancellationException

/**
 * EVERY URL EVERY STREAM WOULD DIAL, derived when the pass runs.
 *
 * The probe passes used to see only what a stream had pushed at them, which made
 * the candidate set a function of discovery timing. Measured: a 16-url stream
 * finished discovering in one second, the first pass ran two minutes later
 * against those 16 alone, and two 17,499-url streams submitted 190 seconds after
 * that — so 34,997 urls waited six hours for a pass they missed by three
 * minutes, while the fan-out dialled the same server once per alias.
 *
 * Deriving it here rather than in [AliasMonitor] keeps the monitor free of a
 * store, a transport and an ingest queue; deriving it per pass rather than from
 * the streams' caches is the point — those caches are what may not exist yet on
 * the boot this is for.
 */
internal class StreamWorld(
    private val store: IEventStore,
    private val streams: List<SyncStream>,
    private val probe: ReachabilityProbe,
    private val ingest: IngestPipeline,
    private val monitor: RelayMonitor?,
    private val tor: TorTransport?,
    override val sockets: AliasFolding.Sockets,
    /**
     * The monitor's OWN url sources — the `monitor { sources = [...] }` block —
     * unioned with whatever the streams' parsed sources yield. This is what
     * lets a deployment move relay-list parsing off the streams entirely: a
     * stream running on verdict sources alone contributes no candidates, and
     * the monitor block is then the one place urls enter the system.
     */
    private val monitorSources: MonitorConfig? = null,
) : AliasMonitor.Source {
    /**
     * The monitor block dressed as a discovery config, which is all
     * [RelayDiscovery.discover] reads of one. Cadence fields are inert here —
     * the monitor's clock belongs to [AliasMonitor].
     */
    private val monitorDiscovery: RelayDiscoveryConfig? =
        monitorSources?.takeIf { it.sources.isNotEmpty() }?.let {
            RelayDiscoveryConfig(
                sources = it.sources,
                refreshSeconds = it.sweepSeconds,
                concurrency = 1,
                exclude = it.exclude,
            )
        }

    /**
     * What the last derivation started from and what it dropped — the two nodes
     * ABOVE `candidates`, which nothing could see before.
     *
     * The candidate set is where both probe passes begin, so every number they
     * publish is a share of it — and it is already a filtered set. A url a
     * signed record calls dead never reaches them, so a reader watching the
     * gate's coverage had no way to tell a corpus that shrank from one that was
     * never that big. Held here rather than logged only, because the log line
     * this pairs with rotates out of a container's buffer within the hour and
     * the funnel it belongs to is drawn from the published document.
     *
     * Read live at snapshot time through [Processors.Handle.counts], for the
     * reason that class documents: a copy kept in step by hand is the shape that
     * produces a report disagreeing with the thing it reports on.
     */
    @Volatile
    var lastDerivation: Derivation = Derivation()
        private set

    /** One derivation's arithmetic: `sourced = excluded + heldOutDead + candidates`. */
    data class Derivation(
        /** Every url the streams' relay lists yielded, before anything was dropped. */
        val sourced: Int = 0,
        /**
         * …of those, how many an OPERATOR's instruction dropped: on a stream's
         * `exclude` list, or this relay's own url.
         *
         * Its own number rather than folded into [heldOutDead], for the reason
         * [CycleTally.excluded] gives: one is an instruction and the other is a
         * measurement, they have different fixes, and a reader who cannot tell
         * them apart cannot act on either.
         */
        val excluded: Int = 0,
        /** …and how many carried a current unreachability record. */
        val heldOutDead: Int = 0,
    )

    /**
     * Urls a signed record already calls dead are held out: they cannot be
     * fingerprinted, so they cannot be folded, and dialling them is a connect
     * timeout spent re-learning what the record says. Not permanent — the record
     * ages out (24h) or the host delivers something, and the url is back.
     *
     * Held out HERE rather than declined in [canDial], where the fold would
     * report it as `declined by our own transport` — a false statement about us.
     * How many, and out of what, is [lastDerivation].
     */
    override suspend fun candidates(): List<NormalizedRelayUrl> {
        val dead = monitor?.deadSet().orEmpty()
        val all = LinkedHashSet<NormalizedRelayUrl>()
        // Kept rather than only skipped, so the funnel's first branch divides.
        // An operator who excluded a hundred urls and then asks why the fan-out
        // is a hundred short is asking about a number nothing published.
        val excluded = LinkedHashSet<NormalizedRelayUrl>()
        for ((label, dynamic) in derivations()) {
            val found =
                try {
                    RelayDiscovery.discover(store, dynamic, skip = setOfNotNull(store.relay), allowOnion = tor != null)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    System.err.println("router: alias source could not derive $label: ${e.message}")
                    emptyList()
                }
            found.forEach {
                if (it.url !in dynamic.exclude && it.url != store.relay) all += it.url else excluded += it.url
            }
        }
        // `exclude` is PER STREAM, so a url one stream excludes and another asks
        // for is a candidate — it is dialled, and counting it as excluded would
        // put it on both sides of a partition that has to divide exactly once.
        val onlyExcluded = excluded - all
        val live = all.filterNot { it in dead }
        lastDerivation =
            Derivation(
                sourced = all.size + onlyExcluded.size,
                excluded = onlyExcluded.size,
                heldOutDead = all.size - live.size,
            )
        System.err.println(
            "router: alias source derived ${live.size} url(s) across ${streams.size} stream(s)" +
                (if (all.size > live.size) "; ${all.size - live.size} held out as known dead" else ""),
        )
        return live
    }

    /** Every derivation the world runs: each stream's parsed sources, plus the monitor's own block. */
    private fun derivations(): List<Pair<String, RelayDiscoveryConfig>> =
        streams.mapNotNull { s -> s.dynamic?.let { s.name to it } } +
            listOfNotNull(monitorDiscovery?.let { "monitor sources" to it })

    /**
     * The fast lane's derivation: the same sources, `since`-bounded to
     * relay-list events ingested at or after [since]. Reads minutes of events
     * where [candidates] walks the store — which is the whole reason a new
     * relay can be verdicted in minutes without the lane costing a sweep.
     *
     * Known-dead urls are held out on the same reasoning as [candidates];
     * the exclude lists apply inside [RelayDiscovery.discover] as ever.
     */
    override suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> {
        val dead = monitor?.deadSet().orEmpty()
        val fresh = LinkedHashSet<NormalizedRelayUrl>()
        for ((label, dynamic) in derivations()) {
            val bounded =
                dynamic.copy(
                    sources = dynamic.sources.map { it.copy(filter = it.filter.copy(since = since)) },
                )
            val found =
                try {
                    RelayDiscovery.discover(store, bounded, skip = setOfNotNull(store.relay), allowOnion = tor != null)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    System.err.println("router: fast lane could not derive $label: ${e.message}")
                    emptyList()
                }
            found.forEach { if (it.url !in dynamic.exclude && it.url != store.relay) fresh += it.url }
        }
        return fresh.filterNot { it in dead }
    }

    override suspend fun canDial(url: NormalizedRelayUrl): Boolean = probe.canDial(url)

    /**
     * ONCE, not once per stream that wants it. [IngestPipeline.submit] queues
     * before the store dedups, so a per-stream loop spends one slot of a bounded
     * queue per match on a single event. Verified unless every stream that wants
     * it trusts its source — `skipVerify` is a claim about provenance, and the
     * probe's provenance is one thing for all of them.
     */
    override suspend fun onEvent(event: Event) {
        val wanted = streams.filter { it.filter.match(event) }
        if (wanted.isEmpty()) return
        ingest.submit(event, wanted.all { it.trusted })
    }
}
