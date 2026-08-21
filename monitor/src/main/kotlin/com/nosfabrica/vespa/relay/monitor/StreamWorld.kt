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
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
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
     * `prime` admits, and everything admitted still has to survive a dial,
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
    /**
     * THIS router's own signing identity, or null where it has none — the scope
     * of [ownRecords] and of nothing else.
     *
     * Deliberately not [monitorAuthors], which is wider by design: that set is
     * the identities an operator vouched for, and a hold-out is a decision an
     * operator may delegate. "How big is our corpus" is not a decision at all,
     * and answered over the wider set it is somebody else's corpus — a
     * deployment mirroring a busy foreign monitor's 30166s would draw that
     * monitor's whole world as the mouth of its own coverage tree.
     */
    private val self: String?,
    private val tor: TorTransport?,
    override val sockets: Sockets,
    /**
     * The monitor's OWN url sources — the `monitor { sources = [...] }` block —
     * unioned with whatever the streams' parsed sources yield. This is what
     * lets a deployment move relay-list parsing off the streams entirely: a
     * stream running on verdict sources alone contributes no candidates, and
     * the monitor block is then the one place urls enter the system.
     */
    private val monitorConfig: MonitorConfig? = null,
    /**
     * Where this derivation reports — its position while it walks, and what it
     * yielded when it is done. Null in a test that is asserting the numbers
     * rather than the row.
     */
    override val progress: Processors.Handle? = null,
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

    /**
     * …and whether one has actually RUN, which the numbers above cannot say.
     *
     * A fresh [Derivation] and a derivation that found nothing are the same
     * five zeros, and they are opposite states: the first is a router two
     * minutes into its boot with the walk still ahead of it, the second is a
     * store holding no relay lists at all. The alias source's row is drawn from
     * these counts, so without this it would open every boot claiming `0 url(s)
     * named` — a measurement nobody has taken, which is the exact reading
     * "absent is not zero" exists to stop.
     */
    @Volatile
    var derived: Boolean = false
        private set

    /**
     * One derivation's arithmetic: `sourced = excluded + heldOutDead +
     * candidates`, with [recordedOnly] beside it rather than inside it — see
     * there.
     */
    data class Derivation(
        /** Every url the streams' relay lists yielded, before anything was dropped. */
        val sourced: Int = 0,
        /**
         * …of those, how many an OPERATOR's instruction dropped: on a stream's
         * `exclude` list, or this relay's own url.
         *
         * Its own number rather than folded into [heldOutDead], for the reason
         * an exclusion count gives: one is an instruction and the other is a
         * measurement, they have different fixes, and a reader who cannot tell
         * them apart cannot act on either.
         */
        val excluded: Int = 0,
        /** …and how many carried a current `dead` verdict of ours. */
        val heldOutDead: Int = 0,
        /**
         * Urls this router HOLDS A RECORD ABOUT that no relay list named this
         * round — outside [sourced] rather than a slice of it, and the reason
         * the funnel's mouth is `sourced + recordedOnly`.
         *
         * **The corpus is not what the last derivation happened to yield.** A
         * url leaves the streams' relay lists for reasons of its own: the author
         * who listed it revised their 10002, a source was reconfigured, a stream
         * was retired. Every measurement this router ever took of it is still in
         * the store — the fold reads exactly these records to group a new url
         * against its host's history — but the coverage card's tree started at
         * [sourced] and so lost them without a word. On a deployment holding
         * records for five figures of urls whose current relay lists name a
         * couple of thousand, the card claimed to draw "every relay url this
         * router knows of" and drew an eighth of it.
         *
         * NOT a branch of the card's tree, and it was: these urls are already
         * inside [candidates] and [heldOutDead] by the time they are counted
         * here — `known` is the union and the split comes after — so a row of
         * their own double-counts every one of them. It is a cross-cutting fact
         * about the corpus, how much of it no relay list names any more, rather
         * than a slice of it, and it is stated on the round-up's own line where
         * nothing can add it to anything. Not a drop either: nothing was decided
         * against these urls this round, they simply were not asked for. Zero on
         * a router with no signer and no named monitors, which holds no records
         * and is telling the truth by saying so.
         */
        val recordedOnly: Int = 0,
        /**
         * What the relay lists named the round BEFORE this one, or null on the
         * first round of a process.
         *
         * Published rather than left to the log, for the reason every other
         * number on this row is: a reader asking "did the corpus shrink or did
         * our read of it" needs both sides of the comparison, and a log line is
         * not somewhere a card or an alert can look. See the shrink check in
         * [candidates] for why the question matters at all.
         */
        val sourcedLastRound: Int? = null,
        /**
         * …and what was left for the passes — the derivation's YIELD, and the
         * one number on this row that every number on the three rows below it
         * is a share of.
         *
         * Published rather than left as `sourced - excluded - heldOutDead`,
         * though that identity holds and the glossary states it. A reader
         * subtracting three numbers to find the one they came for is a reader
         * who cannot tell an arithmetic slip from a small corpus, and the row
         * that says "this is what the fold was handed" should say it.
         */
        val candidates: Int = 0,
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
    private suspend fun ownDead(among: Collection<NormalizedRelayUrl>? = null): Set<NormalizedRelayUrl> =
        RelayDiscovery.undialable(
            store,
            monitorAuthors = monitorAuthors,
            maxAgeSeconds = DEAD_TTL_SECONDS,
            allowOnion = tor != null,
            // Null from the sweep, which is about to walk the whole corpus and
            // needs the whole hold-out; the fast lane's own handful otherwise —
            // see [RelayDiscovery.undialable]'s `among`.
            among = among,
        )

    /**
     * Every url one of our own records is about, on the verdict TTL rather than
     * the dead one — see [Derivation.recordedOnly].
     *
     * [RelayVerdictRecord.DEFAULT_TTL_SECONDS] because that is how long a
     * verdict is worth anything: a record past it is refused by every read that
     * matters, so counting its url as "known" would put a number on the card
     * that nothing downstream can use. This is the same population
     * [RelayVerdictRecord.loadAll] hands the fold, which is what makes the
     * card's mouth and the fold's world one corpus rather than two.
     */
    private suspend fun ownRecords(): Set<NormalizedRelayUrl> =
        RelayDiscovery.recorded(
            store,
            self = self,
            maxAgeSeconds = RelayVerdictRecord.DEFAULT_TTL_SECONDS,
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
        // WHAT THIS WALK SET OUT TO DO, declared AFTER the dead-set read and
        // before the walk it describes.
        //
        // One unit per configured SOURCE — see [Processors.UNIT_SOURCE] for why
        // it cannot be urls, and `RelayDiscovery.discover`'s `onSource` for why
        // it cannot be per derivation: a deployment that has moved its parsing
        // into `monitor { sources }` has one config holding several sources,
        // and a position that reads "0 of 1" until it reads "1 of 1" is not a
        // position.
        //
        // After [ownDead] because the rate this implies is timed from HERE —
        // see [Processors.Run.startedMs]. That read is two indexed queries
        // rather than a walk, and left inside the timing it lands in the
        // numerator of every estimate while contributing nothing to the
        // numerator's units. The row still says `collecting` throughout it; it
        // simply has no position to give yet, which is the same thing every
        // probe pass does while it works out its own set.
        progress?.measuring(derivations().sumOf { it.second.sources.size }, Processors.UNIT_SOURCE)
        val all = LinkedHashSet<NormalizedRelayUrl>()
        // Kept rather than only skipped, so the funnel's first branch divides.
        // An operator who excluded a hundred urls and then asks why the fan-out
        // is a hundred short is asking about a number nothing published.
        val excluded = LinkedHashSet<NormalizedRelayUrl>()
        // One tick per source as its walk ends — the sweep's position, and the
        // only path that reports one: the fast lane runs the same `derive` and
        // must not move a sweep's row. See the lane's comment in [AliasMonitor].
        derive("alias source", { it }, onSource = { progress?.attempted() }) { url, kept ->
            if (kept) all += url else excluded += url
        }
        // `exclude` is PER STREAM, so a url one stream excludes and another asks
        // for is a candidate — it is dialled, and counting it as excluded would
        // put it on both sides of a partition that has to divide exactly once.
        val onlyExcluded = excluded - all
        // WHAT WE KNOW BEYOND WHAT WAS NAMED — and it is CANDIDATE SET, not a
        // number on a card.
        //
        // These urls used to be counted and dropped: the corpus was whatever
        // the relay lists happened to yield this round, so a derivation that
        // came back short took the corpus with it. Measured on staging, one
        // round yielded 127 urls out of a store holding 3.09M relay lists and
        // records for 19,844 relays — and the card, drawn from what came back,
        // reported the other 19,717 as urls "no relay list names now". A short
        // read and a shrunk network are the same picture from in here.
        //
        // A url we hold a signed record about is a url we have measured and are
        // telling the network about. Re-measuring it is OUR job on OUR clock,
        // and it does not need somebody's 10002 to name it again first. So the
        // corpus is the union, and a bad derivation now costs freshness on the
        // urls it failed to name rather than the whole population.
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
        // A DERIVATION THAT COLLAPSED IS A FAULT, NOT A NEW BASELINE.
        //
        // The read that yields these urls is a projection over every relay list
        // in the store, and it can come back short for reasons that have
        // nothing to do with the network: a content node still loading answers
        // a query with `coverage: 100, full: true` over zero documents, and a
        // degraded or soft-timed-out search returns a partial answer that looks
        // exactly like a small one. Nothing downstream can tell those apart —
        // which is how staging went from naming ~17,000 urls to naming 127
        // without a single line of log saying anything had gone wrong.
        //
        // The corpus itself is no longer at risk (the candidate set is the
        // union with our own records above), so this does not refuse or retry.
        // It says so, loudly, once, which is the one thing that was missing.
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
            "router: alias source derived ${live.size} url(s) across ${streams.size} stream(s)" +
                "; ${all.size} named by a relay list this round" +
                (if (known.size > live.size) "; ${known.size - live.size} held out as known dead" else "") +
                // The number that says a shrinking DERIVATION from a shrinking
                // relay list — it no longer shrinks the corpus, but it is still
                // the first thing to look at when the lists go quiet.
                (
                    lastDerivation.recordedOnly
                        .takeIf { it > 0 }
                        ?.let { "; $it more from our own records that nothing named this round" }
                        .orEmpty()
                ),
        )
        return live
    }

    /**
     * What the last round's relay lists named, for the shrink check in
     * [candidates]. Null until a round has run — the first derivation has
     * nothing to be compared against and must not warn about itself.
     */
    private var lastSourced: Int? = null

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
        onSource: () -> Unit = {},
        onUrl: (NormalizedRelayUrl, kept: Boolean) -> Unit,
    ) {
        for ((label, discovery) in derivations()) {
            // Ticked by `discover` as each source ends, and TOPPED UP here if
            // it threw partway: the position's denominator counts every source
            // this pass set out to walk, so a config that failed on its second
            // of three would otherwise leave the row a unit short forever —
            // `4 of 6` under `idle`, which reads as a walk that stopped rather
            // than one that finished badly. A source we could not read is still
            // behind us.
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
     * The fast lane's derivation: the same sources, `since`-bounded to
     * relay-list events ingested at or after [since]. Reads minutes of events
     * where [candidates] walks the store — which is the whole reason a new
     * relay can be verdicted in minutes without the lane costing a sweep.
     *
     * Known-dead urls are held out on the same reasoning as [candidates];
     * the exclude lists apply inside [RelayDiscovery.discover] as ever.
     */
    override suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> {
        val fresh = LinkedHashSet<NormalizedRelayUrl>()
        derive("fast lane", { discovery ->
            discovery.copy(sources = discovery.sources.map { it.copy(filter = it.filter.copy(since = since)) })
        }) { url, kept -> if (kept) fresh += url }
        // DERIVED FIRST, AND THE HOLD-OUT ASKED ABOUT WHAT IT FOUND. This read
        // the whole dead set before deriving anything, which made a lane tick
        // cost one unbounded materializing query per `fastLaneSeconds` —
        // thirty an hour at the stock 120s, five figures of records each, to
        // decide a question about a dozen urls. Most ticks find nothing at all,
        // and now those cost nothing: an empty `fresh` returns without a second
        // read, and a non-empty one is bounded by its own size.
        //
        // Same answer either way — the hold-out only ever applied to the urls
        // in `fresh`, so asking about the rest of the corpus was work whose
        // result was discarded.
        if (fresh.isEmpty()) return emptyList()
        val dead = ownDead(among = fresh)
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

        /**
         * How far a derivation may fall against the round before it without
         * being called out — see the shrink check in [candidates].
         *
         * Half, which is far looser than any honest movement in a corpus of
         * relay lists: authors revise a 10002 one at a time, and the population
         * that names a relay does not halve between two sweeps. Loose on
         * purpose — this line has to be believable when it fires, so it is set
         * where nothing but a broken read can reach it.
         */
        const val SHRINK_SHARE = 0.5

        /**
         * …and the size below which the comparison is not worth making. A
         * deployment deriving a dozen urls is a cold store or a small
         * configuration, and both move by whole percentages for ordinary
         * reasons.
         */
        const val SHRINK_FLOOR = 100
    }
}
