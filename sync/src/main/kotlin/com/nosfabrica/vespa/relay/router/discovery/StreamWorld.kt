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
import com.nosfabrica.vespa.relay.router.config.RelayExcludes
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
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
    /**
     * Whose `dead` verdicts may hold a url out of the candidate set — this
     * router's signer, plus every monitor npub the config names. NEVER every
     * author, and deliberately NOT the rule the roster read follows: a
     * `syncable` admits, and everything admitted still has to survive a dial,
     * so reading those unscoped costs at worst a connect attempt. A hold-out
     * forecloses. Unscoped, anyone whose 30166s we mirror could starve a relay
     * out indefinitely — held out of the candidate set it is never dialled,
     * never re-measured, and never earns the verdict that would clear the mark.
     *
     * Empty means nothing is held out, which is the honest answer for a router
     * with no signer and no named monitors: it has no standing to call
     * anything dead.
     */
    private val monitorAuthors: List<String>,
    private val tor: TorTransport?,
    override val sockets: AliasFolding.Sockets,
    /**
     * The monitor's OWN url sources — the `monitor { sources = [...] }` block —
     * unioned with whatever the streams' parsed sources yield. This is what
     * lets a deployment move relay-list parsing off the streams entirely: a
     * stream running on verdict sources alone contributes no candidates, and
     * the monitor block is then the one place urls enter the system.
     */
    private val monitorConfig: MonitorConfig? = null,
) : AliasMonitor.CandidateSource {
    /**
     * The monitor block dressed as a discovery config, which is all
     * [RelayDiscovery.discover] reads of one. Cadence fields are inert here —
     * the monitor's clock belongs to [AliasMonitor].
     */
    private val monitorDiscovery: RelayDiscoveryConfig? =
        monitorConfig?.takeIf { it.sources.isNotEmpty() }?.let {
            RelayDiscoveryConfig(
                sources = it.sources,
                refreshSeconds = it.sweepSeconds,
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
        /** …and how many carried a current `dead` verdict of ours. */
        val heldOutDead: Int = 0,
    )

    /**
     * The author-scoped dead set — see [monitorAuthors] for why it must be
     * scoped at all.
     *
     * Reads OUR OWN `dead` verdicts, not an absence. It used to infer death
     * from quartz's convention — within the TTL, a 30166 carrying no
     * `rtt-open` is "checked and could not open" — which meant every record
     * quartz's passive monitor wrote about a relay it had merely failed to
     * reach counted as a verdict, and meant the router depended on a writer it
     * also had to work around. The fitness pass states it outright now, so the
     * hold-out reads the same tag the roster does.
     */
    private suspend fun ownDead(): Set<NormalizedRelayUrl> =
        RelayDiscovery.undialable(
            store,
            monitorAuthors = monitorAuthors,
            maxAgeSeconds = DEAD_TTL_SECONDS,
            allowOnion = tor != null,
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
        val dead = ownDead()
        val all = LinkedHashSet<NormalizedRelayUrl>()
        // Kept rather than only skipped, so the funnel's first branch divides.
        // An operator who excluded a hundred urls and then asks why the fan-out
        // is a hundred short is asking about a number nothing published.
        val excluded = LinkedHashSet<NormalizedRelayUrl>()
        derive("alias source", { it }) { url, kept -> if (kept) all += url else excluded += url }
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
        streams.mapNotNull { s -> s.discovery?.let { s.name to it } } +
            listOfNotNull(monitorDiscovery?.let { "monitor sources" to it })

    /**
     * One walk over every derivation, [bound] applied to each config first —
     * the shared core of [candidates] and [candidatesSince], so a source that
     * fails to derive is reported (and survived) the same way on both paths.
     * [onUrl]'s `kept` says whether the url survived the per-stream exclude
     * list and the self check; the caller decides what a dropped url means.
     *
     * The discovery underneath is asked for the UNFILTERED set — no `exclude`,
     * no `skip` — because those are the two tests `kept` makes, and letting
     * [RelayDiscovery.discover] make them first is what silently pinned the
     * funnel's `excluded` row at 0 for as long as it existed: everything that
     * would have failed the predicate had already been dropped a frame down, so
     * `kept` was true for every url that reached it. The urls were excluded
     * correctly; the count of them was structurally unreachable, and `sourced`
     * — "every url the streams named" — was quietly a post-exclusion number
     * too. One place applies the rule, and it is the one place that can also
     * count what the rule cost.
     */
    private suspend fun derive(
        what: String,
        bound: (RelayDiscoveryConfig) -> RelayDiscoveryConfig,
        onUrl: (NormalizedRelayUrl, kept: Boolean) -> Unit,
    ) {
        for ((label, discovery) in derivations()) {
            val found =
                try {
                    RelayDiscovery.discover(
                        store,
                        bound(discovery).copy(exclude = RelayExcludes.NONE),
                        skip = emptySet(),
                        allowOnion = tor != null,
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    System.err.println("router: $what could not derive $label: ${e.message}")
                    emptyList()
                }
            found.forEach { onUrl(it.url, it.url !in discovery.exclude && it.url != store.relay) }
        }
    }

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
        val dead = ownDead()
        val fresh = LinkedHashSet<NormalizedRelayUrl>()
        derive("fast lane", { discovery ->
            discovery.copy(sources = discovery.sources.map { it.copy(filter = it.filter.copy(since = since)) })
        }) { url, kept -> if (kept) fresh += url }
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

    companion object {
        /**
         * How long a `dead` verdict keeps a url out of the candidate set. The
         * same 24h quartz's `RelayReachabilityStore` used, kept deliberately:
         * a hold-out is self-healing only because it lapses, and a bound
         * shorter than a sweep would re-dial the corpse every pass while a
         * much longer one starves a host that came back.
         */
        const val DEAD_TTL_SECONDS = 24L * 60 * 60
    }
}
