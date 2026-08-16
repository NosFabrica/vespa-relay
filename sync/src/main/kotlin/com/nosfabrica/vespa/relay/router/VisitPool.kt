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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.DeleteMissing
import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.discovery.DiscoveredRelay
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.nosfabrica.vespa.relay.router.discovery.RelaySockets
import com.nosfabrica.vespa.relay.router.heal.Healer
import com.nosfabrica.vespa.relay.router.progress.InFlight
import com.nosfabrica.vespa.relay.router.progress.Processors
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * THE ROTATING POOL — the sync plane with the control plane removed.
 *
 * There are no walks, cycles or rounds here, and no admission gates, transfer
 * pools or holding phases either. One rotating queue holds every relay the
 * monitor currently certifies syncable; a fixed set of workers pulls from it,
 * and the only loop that exists is the inner one a specific relay needs. When
 * a relay finishes, it re-enters the queue on a revisit delay and the next one
 * starts. A slot IS a socket by construction — the worker that holds one is
 * connected or inside a bounded connect — so "200 transferring, 16 sockets"
 * is unrepresentable.
 *
 * ## One dial serves every stream
 *
 * The unit of work is a RELAY VISIT: the union of every visit-mode stream's
 * outstanding asks against one relay, over one connection. Today that relay is
 * dialled once per stream per lap, and every guard, probe and wedge is paid
 * per stream. The roster maps url → streams, and the visit runs them in turn.
 *
 * ## Fetch forward, audit the past, tail the present
 *
 * Per stream, a visit is: catch-up pages over the band's outstanding legs
 * (kinds-only — no author narrowing, so the asks are a few hundred bytes),
 * then — when the stream sets `verifySeconds` and the band's last full pass
 * has aged past it — a windowed negentropy audit of the covered history that
 * downloads only the diff. After the asks, the visit leaves a LIVE TAIL on the
 * open socket: new events arrive the moment they exist, and freshness stops
 * being a lap property at all. The tail is a subscription plus a held socket
 * claim, not a parked worker — the worker moves on, and the revisit only has
 * to cover what a dropped tail missed.
 *
 * ## What bounds a visit
 *
 * quartz's own endings, believed: every page ends inside one idle window, and
 * a walk that was refused with nothing delivered
 * ([DynamicSync.refusedOutright]) ends the visit rather than re-opening the
 * same conversation once per remaining leg. A wedged relay costs one worker
 * one bounded visit, not one slot for hours — and the monitor's next sweep is
 * what decides whether it stays on the roster at all.
 */
internal class VisitPool(
    private val client: NostrClient,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val pager: NegentropyPager,
    private val healer: Healer,
    /**
     * The deleteMissing comparison, run in the audit slot of a retracting
     * stream's asks — see [RetractionAudit]. Null for callers with no
     * refused-ids plumbing (the probes): their retracting asks audit nothing
     * and delete nothing, loudly ordinary.
     */
    private val retraction: RetractionAudit? = null,
    private val sockets: RelaySockets,
    private val tor: TorTransport?,
    private val scope: CoroutineScope,
    /** Whose 30166 verdicts build the roster — see [RelayDiscovery.syncable]. */
    private val monitorAuthor: String?,
    /** The visit-mode streams: every relaySource entry a kind-30166 verdict source. */
    private val streams: List<SyncStream>,
    private val progress: Processors.Handle,
    /**
     * The streams' own rows in the progress document. Registered at boot by
     * the engine so silence never reads as "not configured" — and before this
     * was passed in, that was ALL a visit stream's row ever said: the pool
     * kept every fact on its processor row and left the stream row frozen on
     * `starting` forever, a zombie that read as a stream that never began.
     * The pool sets the one phase it has ([StreamPhases.Phase.Rotating]) and
     * registers the in-flight source that names which relays a worker is on.
     * Null for callers with no document to keep (the probes).
     */
    private val phases: StreamPhases? = null,
    /**
     * How many relays are VISITED — and therefore dialled — at once. This is
     * the herd control: a fresh roster floods the queue, every worker pulls,
     * and whatever this number is becomes the count of simultaneous TLS
     * handshakes. The first 440-relay integration run let it equal the whole
     * socket budget and watched 436 of the dials time out inside one minute —
     * a thundering herd against its own connect timeout. A visit is seconds
     * long, so this bounds the burst, not the throughput; the pool's steady
     * state — sockets held open — is the tails' budget, not this one.
     */
    private val visitConcurrency: Int = DEFAULT_VISIT_CONCURRENCY,
    /**
     * How many live tails may be held at once. The visit half of the pool is
     * bounded by its workers; the tails were not, and a roster past the OkHttp
     * dispatcher's ceiling would have strangled every NEW connect with held
     * sockets — the exact occlusion the pool replaced, rebuilt out of its best
     * feature. Under the budget every visited relay keeps its tail; over it,
     * tails are EARNED: the relay with more content lately takes the socket of
     * the tail that has delivered least.
     */
    private val tailBudget: Int = DEFAULT_TAIL_BUDGET,
) {
    /**
     * ONE UNIT OF WORK against one relay: the stream asking, and the exact
     * filter it asks — the stream's own for a verdict source, or the
     * scan-paired narrow for a bound scan (one Ask PER BOUND AUTHOR, fixed:
     * the tag structure already decided the granularity, and a per-author ask
     * is what keeps a `(relay, provider)` band valid forever — see the
     * `asksOf` arithmetic). Bands, catch-ups, audits and tails all key on
     * this filter, which is why the port is a type change and not an engine.
     */
    internal data class Ask(
        val stream: SyncStream,
        val filter: Filter,
    )

    /** url → the asks that want it, rebuilt on the roster clock. */
    @Volatile
    private var roster: Map<NormalizedRelayUrl, List<Ask>> = emptyMap()

    /**
     * Per stream: authors the roster found at MORE THAN ONE relay. One
     * relay's empty answer does not retract what a sibling relay may still
     * be serving, so the retraction audit never judges their asks — the same
     * rule the legacy cycle applied, computed here on the roster clock.
     */
    @Volatile
    private var sharedAuthors: Map<String, Set<String>> = emptyMap()

    /** One certified scan's discovery, held for its stream's `refreshSeconds` — a store walk is not a poll. */
    private class ScannedList(
        val expiresAtMs: Long,
        val relays: List<DiscoveredRelay>,
    )

    private val scans = ConcurrentHashMap<String, ScannedList>()

    private val queue = Channel<NormalizedRelayUrl>(Channel.UNLIMITED)
    private val queued = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val inFlight = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** url → live tail subscription id. A held [sockets] claim rides with each. */
    private val tails = ConcurrentHashMap<NormalizedRelayUrl, String>()
    private val tailSeq = AtomicInteger()

    /**
     * WHAT EACH RELAY HAS DELIVERED LATELY — the pool's one priority signal.
     *
     * A half-life decayed score: every event a relay delivers (visit, audit or
     * tail) adds one, and the total halves every [YIELD_HALF_LIFE_MS]. "More
     * content lately" is then a number that can be compared across relays
     * without a window to maintain: a relay that served a thousand events this
     * hour outranks one that served a thousand yesterday, and both outrank the
     * one that has been quiet all week. It decides two things — which relays
     * hold tails when the budget is short, and how soon a relay is revisited —
     * and deliberately nothing else: admission is the monitor's verdict alone.
     */
    private class Yield {
        val arrived = AtomicLong()

        @Volatile var score: Double = 0.0

        @Volatile var foldedAtMs: Long = System.currentTimeMillis()

        /** Fold what arrived into the decayed score and read it, cheaply racy — a priority hint, not a ledger. */
        fun current(nowMs: Long): Double {
            val fresh = arrived.getAndSet(0)
            val dtMs = (nowMs - foldedAtMs).coerceAtLeast(0)
            if (fresh > 0 || dtMs > YIELD_HALF_LIFE_MS / 8) {
                score = score * Math.pow(0.5, dtMs.toDouble() / YIELD_HALF_LIFE_MS) + fresh
                foldedAtMs = nowMs
            }
            return score
        }
    }

    private val yields = ConcurrentHashMap<NormalizedRelayUrl, Yield>()

    private fun yieldOf(url: NormalizedRelayUrl): Yield = yields.getOrPut(url) { Yield() }

    /**
     * ONE RELAY MID-VISIT, for the in-flight list — the clocks and the stage,
     * on the same terms as the legacy engine's [InFlight.Relay] so the same
     * card column reads both. Removed the moment the visit ends: a tail is not
     * a worker, and listing 400 held tails as in-flight rows would bury the
     * one wedged visit the list exists to name.
     */
    private class Ongoing(
        val startedMs: Long,
    ) {
        /** Which stream's asks the visit is on right now — visits serve streams in turn. */
        @Volatile var stream: String? = null

        @Volatile var doing: String = "claiming the socket"

        /** The `created_at` second the walk or audit window is at — time-axis progress. */
        @Volatile var pagingUntil: Long? = null

        val events = AtomicLong()

        /** Any sign of life: an event, a negentropy frame, a window opening. */
        @Volatile var lastActivityMs: Long = startedMs
    }

    private val ongoing = ConcurrentHashMap<NormalizedRelayUrl, Ongoing>()

    /**
     * The in-flight rows for one stream: every relay whose visit is currently
     * serving it, quietest first — the same ordering argument as
     * [InFlight]'s, because the row worth reading is the one nothing is
     * arriving on. Bounded here as everywhere a list leaves the process, with
     * the cut disclosed.
     */
    private fun inFlightFor(stream: String): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            ongoing
                .entries
                .filter { it.value.stream == stream }
                .map { (url, o) ->
                    InFlight.Relay(
                        relay = url.url,
                        heldForSec = ((nowMs - o.startedMs) / 1000).coerceAtLeast(0),
                        // A visit IS on the socket from its first moment — the
                        // claim and the dial are inside it — so the two clocks
                        // agree by construction. Published anyway: this is the
                        // member the card reads as "has a transfer slot".
                        transferringForSec = ((nowMs - o.startedMs) / 1000).coerceAtLeast(0),
                        events = o.events.get(),
                        quietForSec = ((nowMs - o.lastActivityMs) / 1000).coerceAtLeast(0),
                        doing = o.doing,
                        pagingUntil = o.pagingUntil,
                    )
                }.sortedWith(compareByDescending<InFlight.Relay> { it.quietForSec }.thenByDescending { it.heldForSec }.thenBy { it.relay })
        return InFlight(
            relays = rows.take(MAX_IN_FLIGHT_ROWS),
            omitted = (rows.size - MAX_IN_FLIGHT_ROWS).coerceAtLeast(0),
        )
    }

    /**
     * The stream rows' one phase, refreshed wherever the numbers it carries
     * change hands — the roster rebuild, a tail opening, a tail dropping.
     */
    private fun publishPhases() {
        val phases = phases ?: return
        val current = roster
        for (stream in streams) {
            val mine = current.entries.filter { entry -> entry.value.any { it.stream === stream } }
            phases.set(
                stream.name,
                StreamPhases.Phase.Rotating(
                    relays = mine.size,
                    tailed = mine.count { tails.containsKey(it.key) },
                ),
            )
        }
    }

    private val tailsEvicted = AtomicLong()

    private val downloaded = AtomicLong()
    private val visits = AtomicLong()
    private val audits = AtomicLong()
    private val aborted = AtomicLong()

    fun start() {
        if (streams.isEmpty()) return
        progress.phase("rotating")
        progress.counts {
            // Named to collide with NOTHING the document already publishes:
            // `queued` is ingest's depth and `received` a cycle's socket count
            // elsewhere on this card, and one word meaning two quantities on
            // adjacent rows is the exact bug the vocabulary test exists for.
            listOf(
                Processors.Count("roster", roster.size.toLong()),
                Processors.Count("awaitingVisit", queued.size.toLong()),
                Processors.Count("visiting", inFlight.size.toLong()),
                Processors.Count("tails", tails.size.toLong()),
                Processors.Count("visitsRun", visits.get()),
                // The gauge beside the odometer: audits RUNNING against
                // auditsRun's total. A deep history's audit holds a worker for
                // minutes, and without this the only trace was one unit of
                // `visiting` that could not be told from a catch-up.
                Processors.Count("auditing", ongoing.values.count { it.doing == STAGE_AUDITING || it.doing == STAGE_RETRACTING }.toLong()),
                Processors.Count("auditsRun", audits.get()),
                Processors.Count("retracted", retraction?.deleted?.get() ?: 0L),
                Processors.Count("abortedVisits", aborted.get()),
                Processors.Count("evictedTails", tailsEvicted.get()),
                Processors.Count("poolReceived", downloaded.get()),
            )
        }
        // The streams' own rows: the pool's one phase, and the source that
        // names which relays a worker is on — see the [phases] parameter.
        for (stream in streams) {
            phases?.namesInFlight(stream.name) { inFlightFor(stream.name) }
        }
        publishPhases()
        scope.launch { rosterLoop() }
        repeat(visitConcurrency) {
            scope.launch { workerLoop() }
        }
    }

    /**
     * Rebuild the roster from the monitor's verdicts and feed the queue. Half
     * the tightest freshness bound, so a verdict never expires between two
     * looks at it; the floor keeps an aggressive bound from turning this into
     * a poll.
     */
    private suspend fun rosterLoop() {
        val cadence =
            (
                streams
                    .flatMap { it.dynamic?.verdictSources.orEmpty() }
                    .map { it.maxAgeSeconds * 1000L / 2 } +
                    // Scan-built halves rebuild on their stream's own refresh
                    // clock; the loop just has to tick at least that often —
                    // the walk itself is cached, so the extra ticks are cheap.
                    streams
                        .filter { it.dynamic?.scanSources?.isNotEmpty() == true }
                        .mapNotNull { it.dynamic?.refreshSeconds?.times(1000L) }
            ).minOrNull()
                ?.coerceAtLeast(60_000L) ?: 300_000L
        while (scope.isActive) {
            try {
                rebuildRoster()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: visit roster rebuild failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            }
            // "Nothing certified yet" and "nothing certified" are different
            // facts, and only the first is worth retrying for — the same
            // distinction AliasMonitor's empty-retry draws, caught here by the
            // first integration run: a fresh boot rebuilt its empty roster
            // seconds before the monitor's first verdicts landed, then slept
            // half the freshness bound while a certified network waited.
            delay(if (roster.isEmpty()) EMPTY_ROSTER_RETRY_MS else cadence)
        }
    }

    private suspend fun rebuildRoster() {
        val next = HashMap<NormalizedRelayUrl, MutableList<Ask>>()
        val shared = HashMap<String, Set<String>>()

        fun want(
            url: NormalizedRelayUrl,
            ask: Ask,
        ) {
            val wanting = next.getOrPut(url) { mutableListOf() }
            if (ask !in wanting) wanting += ask
        }
        for (stream in streams) {
            val dynamic = stream.dynamic ?: continue
            for (source in dynamic.verdictSources) {
                // The source's configured monitor keys, or our own signer
                // where none are named. A stream with neither is configured
                // to run on verdicts nobody here can write — said once per
                // rebuild rather than once ever, because an operator fixing
                // the signer should see it take effect.
                val authors = source.authors.ifEmpty { listOfNotNull(monitorAuthor) }
                if (authors.isEmpty()) {
                    System.err.println(
                        "router: ${stream.name} has a verdict source, no `authors` and no signer — no monitor identity, roster stays empty",
                    )
                    continue
                }
                val certified =
                    RelayDiscovery.syncable(
                        store,
                        monitorAuthors = authors,
                        maxAgeSeconds = source.maxAgeSeconds,
                        exclude = dynamic.exclude,
                        skip = setOfNotNull(store.relay),
                        allowOnion = tor != null,
                    )
                for (relay in certified) want(relay.url, Ask(stream, stream.filter))
            }
            if (dynamic.scanSources.isNotEmpty()) {
                val urlsByAuthor = HashMap<String, MutableSet<NormalizedRelayUrl>>()
                for (relay in certifiedScan(stream, dynamic)) {
                    for (filter in asksOf(stream.filter, relay)) {
                        want(relay.url, Ask(stream, filter))
                        filter.authors?.forEach { urlsByAuthor.getOrPut(it) { mutableSetOf() } += relay.url }
                    }
                }
                shared[stream.name] = urlsByAuthor.filterValues { it.size > 1 }.keys
            }
        }
        sharedAuthors = shared
        val previous = roster.keys
        roster = next
        // A relay the monitor stopped certifying loses its tail and its socket
        // claim: the verdict is the admission, and holding a connection to a
        // relay we no longer trust to sync is the old machine's habit.
        for (url in previous - next.keys) {
            dropTail(url)
            // The score dies with the certificate: a relay that comes back
            // after a week earns its tail on what it delivers then, not on a
            // decayed memory of what it was.
            yields.remove(url)
        }
        var enqueued = 0
        for (url in next.keys) {
            if (url !in previous && queued.add(url)) {
                queue.trySend(url)
                enqueued++
            }
        }
        if (enqueued > 0 || previous.size != next.size) {
            System.err.println(
                "router: visit roster — ${next.size} syncable relay(s) across ${streams.size} stream(s)" +
                    (if (enqueued > 0) ", $enqueued newly queued" else ""),
            )
        }
        publishPhases()
    }

    /**
     * A certified scan's relay list: the same `discover -> certifiedOnly`
     * chain the legacy engine runs, cached for the stream's `refreshSeconds`
     * — deriving a scan is a store walk, and the roster loop ticks far more
     * often than the list changes. An EMPTY result is cached only briefly:
     * "no provider lists yet" is the state the next ingested event ends, and
     * a full refresh period of blindness to it was the legacy engine's
     * `waiting` retry, relearned.
     */
    private suspend fun certifiedScan(
        stream: SyncStream,
        dynamic: RelayDiscoveryConfig,
    ): List<DiscoveredRelay> {
        val nowMs = System.currentTimeMillis()
        scans[stream.name]?.takeIf { it.expiresAtMs > nowMs }?.let { return it.relays }
        val scanned =
            RelayDiscovery.discover(
                store,
                dynamic,
                skip = setOfNotNull(store.relay),
                allowOnion = tor != null,
            )
        // The fork only admits all-certified scans, but the strictest gate is
        // taken rather than assumed — same arithmetic as the legacy engine's.
        val gate = dynamic.scanSources.mapNotNull { it.certified }.minByOrNull { it.maxAgeSeconds }
        val relays =
            if (gate == null) {
                scanned
            } else {
                val authors = gate.authors.ifEmpty { listOfNotNull(monitorAuthor) }
                if (authors.isEmpty()) {
                    System.err.println(
                        "router: ${stream.name} gates its scan on verdicts, has no `authors` and no signer — no monitor identity, no relays pass",
                    )
                    emptyList()
                } else {
                    RelayDiscovery
                        .certifiedOnly(store, scanned, authors, gate.maxAgeSeconds, allowOnion = tor != null)
                        .also {
                            if (it.size != scanned.size) {
                                System.err.println(
                                    "router: ${stream.name} — ${scanned.size - it.size} of ${scanned.size} scanned relay(s) " +
                                        "held out uncertified (no fresh syncable verdict); the monitor's fast lane is their way in",
                                )
                            }
                        }
                }
            }
        val holdMs = if (relays.isEmpty()) EMPTY_ROSTER_RETRY_MS else dynamic.refreshSeconds * 1000L
        scans[stream.name] = ScannedList(nowMs + holdMs, relays)
        return relays
    }

    private suspend fun workerLoop() {
        for (url in queue) {
            queued.remove(url)
            if (!inFlight.add(url)) continue
            try {
                if (roster.containsKey(url)) visit(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                aborted.incrementAndGet()
                System.err.println("router: visit ${url.url} failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            } finally {
                inFlight.remove(url)
            }
            scheduleRevisit(url)
        }
    }

    /**
     * Back on the queue, if the roster still wants it — on a delay the relay's
     * own recent yield sets. A tailed relay's revisit only serves the audit
     * clock and dropped-tail recovery, so its base is long; an untailed one
     * carries its whole freshness on this cadence, so its base is short; and
     * within either, more content lately means sooner ([revisitDelayMs]).
     */
    private fun scheduleRevisit(url: NormalizedRelayUrl) {
        scope.launch {
            delay(revisitDelayMs(yieldOf(url).current(System.currentTimeMillis()), tails.containsKey(url)))
            if (roster.containsKey(url) && queued.add(url)) queue.trySend(url)
        }
    }

    /** One relay's turn: every stream's catch-up, the audit where due, the heal drain, then the tail. */
    private suspend fun visit(url: NormalizedRelayUrl) {
        visits.incrementAndGet()
        val o = Ongoing(System.currentTimeMillis())
        ongoing[url] = o
        sockets.claim(url)
        try {
            for (ask in roster[url].orEmpty()) {
                o.stream = ask.stream.name
                val clean = catchUp(ask, url)
                // A refusal ends the whole visit, not just this ask's part:
                // the next ask is the same conversation with the same relay,
                // and the monitor's sweep — not a retry loop — is what
                // re-admits it.
                if (!clean) {
                    aborted.incrementAndGet()
                    return
                }
                auditIfDue(ask, url)
            }
            o.doing = "draining queued heals, then the tail"
            healer.drain(url)
            openTail(url)
        } finally {
            ongoing.remove(url)
            sockets.release(url)
        }
    }

    /**
     * The catch-up: walk what the band says is outstanding. Returns false when
     * the relay refused with nothing delivered — the visit's stop signal.
     */
    private suspend fun catchUp(
        ask: Ask,
        url: NormalizedRelayUrl,
    ): Boolean {
        val stream = ask.stream
        val o = ongoing[url]
        for (leg in bands.legs(stream.name, url, ask.filter)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            val relayYield = yieldOf(url)
            o?.doing = STAGE_PAGING
            val onEvent: suspend (Event) -> Unit = { event ->
                downloaded.incrementAndGet()
                relayYield.arrived.incrementAndGet()
                o?.let {
                    it.events.incrementAndGet()
                    it.lastActivityMs = System.currentTimeMillis()
                    // Newest-first is the walk's own order, so the oldest event
                    // seen IS the cursor's depth, near enough for a reader.
                    if (SyncCoverage.isPlausible(event.createdAt) && event.createdAt < (it.pagingUntil ?: Long.MAX_VALUE)) {
                        it.pagingUntil = event.createdAt
                    }
                }
                if (ask.filter.match(event)) {
                    if (SyncCoverage.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions))
                }
            }
            val flooredLeg = leg.flooredForPaging()
            val walked = client.fetchAllPages(url, listOf(flooredLeg), NEG_IDLE_MS, onEvent = onEvent)
            if (DynamicSync.refusedOutright(walked)) {
                // No band for the refused leg: nothing was observed, nothing
                // drained, and a record would re-stamp a walk that never
                // happened. Same rule as the legacy engine's.
                return false
            }
            bands.record(
                stream.name,
                url,
                ask.filter,
                seenMin,
                seenMax,
                paged = true,
                observedByKind = seenByKind,
                drained = drainSettlesThePast(walked, flooredLeg, ask.filter),
            )
        }
        return true
    }

    /**
     * The weekly (or whatever `verifySeconds` says) negentropy audit: when the
     * band's last full pass has aged past the knob, reconcile the covered past
     * in windows and download only the diff. Staggering is free — each relay's
     * band ages on its own clock — so the steady state is
     * `roster / verifySeconds`, a trickle, and no cap is needed.
     */
    private suspend fun auditIfDue(
        ask: Ask,
        url: NormalizedRelayUrl,
    ) {
        val stream = ask.stream
        val verifySeconds = stream.verifySeconds ?: return
        // A retracting stream's audit IS the deleteMissing comparison: the
        // same full-history reconcile, plus the licence to act on what we
        // hold that the provider no longer serves. The ordinary sweep would
        // double the round trips to say half as much.
        if (stream.deleteMissing != DeleteMissing.OFF) {
            retractionIfDue(ask, url, verifySeconds)
            return
        }
        val now = nowSeconds()
        val band = bands.band(stream.name, url, ask.filter)
        if (!auditDue(band?.fullAt ?: 0L, now, verifySeconds)) return
        val auditStarted = now
        var received = 0
        val o = ongoing[url]
        o?.doing = STAGE_AUDITING
        val outcome =
            pager.sweep(
                stream.name,
                url,
                ask.filter,
                ask.filter,
                // Frames are life. A clean audit downloads NOTHING — every
                // window already agrees — so without this a relay whose whole
                // history verifies reads as a worker gone quiet for minutes.
                onProgress = { _, _ -> o?.lastActivityMs = System.currentTimeMillis() },
                onWindow = { _, until ->
                    o?.let {
                        it.lastActivityMs = System.currentTimeMillis()
                        it.pagingUntil = until
                    }
                },
            ) { event ->
                received++
                downloaded.incrementAndGet()
                yieldOf(url).arrived.incrementAndGet()
                o?.let {
                    it.events.incrementAndGet()
                    it.lastActivityMs = System.currentTimeMillis()
                }
                if (ask.filter.match(event)) {
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions))
                }
            }
        audits.incrementAndGet()
        if (outcome.complete) {
            // The audit compared every window, so the whole covered range is
            // verified as of when it STARTED — events since then belong to the
            // tail and the next catch-up, not to this claim.
            bands.record(
                stream.name,
                url,
                ask.filter,
                observedMin = null,
                observedMax = null,
                paged = false,
                reconciledThrough = auditStarted,
            )
        }
        System.err.println(
            "router: audit ${stream.name} ${url.url} — $received event(s) recovered, " +
                (if (outcome.complete) "history verified" else "incomplete (negentropy usable: ${outcome.negentropyUsable})"),
        )
    }

    /**
     * The retraction audit for one ask, on the same `verifySeconds` clock as
     * every other audit — due when the OWNED ask's band ages out, because the
     * reconcile is what stamps it. See [RetractionAudit] for what runs.
     */
    private suspend fun retractionIfDue(
        ask: Ask,
        url: NormalizedRelayUrl,
        verifySeconds: Long,
    ) {
        val retraction = retraction ?: return
        val stream = ask.stream
        val ownedKinds =
            ask.filter.kinds
                .orEmpty()
                .filter { it in stream.ownedKinds }
        if (ownedKinds.isEmpty()) return
        val ownedAsk = ask.filter.copy(kinds = ownedKinds)
        val band = bands.band(stream.name, url, ownedAsk)
        if (!auditDue(band?.fullAt ?: 0L, nowSeconds(), verifySeconds)) return
        val o = ongoing[url]
        o?.doing = STAGE_RETRACTING
        retraction.reconcileAndDelete(
            stream,
            url,
            ask.filter,
            sharedAuthors[stream.name].orEmpty(),
            onActivity = { o?.lastActivityMs = System.currentTimeMillis() },
        ) { event ->
            downloaded.incrementAndGet()
            yieldOf(url).arrived.incrementAndGet()
            o?.let {
                it.events.incrementAndGet()
                it.lastActivityMs = System.currentTimeMillis()
            }
        }
        audits.incrementAndGet()
    }

    /**
     * The live tail: one subscription per relay carrying every wanting
     * stream's filter, `since` a small overlap behind now so the seam with the
     * catch-up cannot drop an event that landed between them. The socket claim
     * taken here is released only when the roster drops the relay — the tail
     * is what "constantly connected" means.
     */
    private fun openTail(url: NormalizedRelayUrl) {
        if (tails.containsKey(url) || !roster.containsKey(url)) return
        val wanting = roster[url].orEmpty()
        if (wanting.isEmpty()) return
        // THE BUDGET, and how a tail is earned past it. Under it every visited
        // relay keeps its tail. Over it, the candidate must outrank the
        // weakest sitting tail on recent yield — the socket goes to the relay
        // with more content lately, and the evicted one falls back to the
        // untailed revisit cadence, promptly requeued so its freshness gap is
        // one queue wait and not a timer.
        if (tails.size >= tailBudget) {
            val nowMs = System.currentTimeMillis()
            val candidate = yieldOf(url).current(nowMs)
            val weakest = tails.keys.minByOrNull { yieldOf(it).current(nowMs) } ?: return
            if (yieldOf(weakest).current(nowMs) >= candidate) return
            tailsEvicted.incrementAndGet()
            dropTail(weakest)
            if (roster.containsKey(weakest) && queued.add(weakest)) queue.trySend(weakest)
        }
        val subId = "visit-tail-${tailSeq.incrementAndGet()}"
        if (tails.putIfAbsent(url, subId) != null) return
        sockets.claim(url)
        val since = nowSeconds() - TAIL_OVERLAP_SECONDS
        val filters = wanting.map { it.filter.copy(since = since) }.distinct()
        client.subscribe(
            subId,
            mapOf(url to filters),
            object : SubscriptionListener {
                override suspend fun onEvent(
                    event: Event,
                    isLive: Boolean,
                    relay: NormalizedRelayUrl,
                    forFilters: List<Filter>?,
                ) {
                    if (relay != url) return
                    downloaded.incrementAndGet()
                    yieldOf(url).arrived.incrementAndGet()
                    // Bind trust per stream, and re-check scope so a broken
                    // relay cannot widen what we ingest — the same rule the
                    // static tails follow. Matching is against each ASK's
                    // filter: a narrowed ask admits only its own provider's
                    // events off the tail, exactly as it does off a page.
                    val wanted = roster[url].orEmpty().filter { it.filter.match(event) }
                    if (wanted.isEmpty()) return
                    ingest.submit(
                        event,
                        wanted.all { it.stream.trusted },
                        IngestOrigin(
                            url,
                            healContent = wanted.any { it.stream.healContent },
                            healRetractions = wanted.any { it.stream.healRetractions },
                        ),
                    )
                }
            },
        )
        publishPhases()
    }

    private fun dropTail(url: NormalizedRelayUrl) {
        val subId = tails.remove(url) ?: return
        runCatching { client.unsubscribe(subId) }
        sockets.release(url)
        publishPhases()
    }

    companion object {
        /**
         * Does [stream] ride the pool? Yes when every relaySource entry
         * answers to the monitor — a verdict source, or a `certified` scan.
         * A retracting stream rides too: its `deleteMissing` comparison IS
         * its audit ([RetractionAudit]), on the `verifySeconds` clock the
         * loader requires it to set.
         */
        internal fun ridesThePool(stream: SyncStream): Boolean {
            val dynamic = stream.dynamic ?: return false
            return dynamic.sources.isNotEmpty() &&
                dynamic.sources.all { it.verdicts != null || it.certified != null }
        }

        /**
         * Is the band's history due its audit? A `fullAt` of zero is a band
         * that has NEVER had a full pass — always due, which is what makes the
         * first audit of a fresh relay happen on its first visit rather than
         * a week later.
         */
        internal fun auditDue(
            fullAt: Long,
            now: Long,
            verifySeconds: Long,
        ): Boolean = fullAt <= 0L || now - fullAt >= verifySeconds

        /**
         * The ask filters one discovered relay contributes: ONE PER BOUND
         * AUTHOR, fixed — no knob. The tag structure already decided the
         * granularity (a `30382:rank` tag pairs one provider with one relay),
         * and the per-author split is what keeps each `(relay, provider)`
         * band's filter — and therefore the band — valid forever: a new
         * provider naming the relay is a new band beside the old ones, never
         * an invalidation of them. This is `authorsPerLeg = 1` made
         * structural; every other value of that knob answered a question the
         * data answers better. A select that binds nothing keeps one ask with
         * the stream's whole filter; narrow keys other than `authors` ride
         * along in every split, sorted by the same argument
         * [DiscoveredRelay.narrowed] sorts — a band is keyed on the filter's
         * serialized form.
         */
        internal fun asksOf(
            base: Filter,
            discovered: DiscoveredRelay,
        ): List<Filter> {
            val authors = discovered.narrow["authors"]
            if (authors.isNullOrEmpty()) return listOf(discovered.narrowed(base))
            return authors.sorted().map { author ->
                DiscoveredRelay(discovered.url, discovered.narrow + ("authors" to setOf(author))).narrowed(base)
            }
        }

        /**
         * Concurrent visits, which is concurrent DIALS — see the constructor
         * parameter for the herd it exists to break up. The default lives on
         * the config: `visitConcurrency` in router.conf is the operator's
         * knob, and [RouterConfig.DEFAULT_VISIT_CONCURRENCY] carries the
         * sizing argument.
         */
        const val DEFAULT_VISIT_CONCURRENCY = RouterConfig.DEFAULT_VISIT_CONCURRENCY

        /**
         * The visit's two stages worth a word, in the in-flight rows' `doing`
         * column. Constants because the `auditing` gauge counts rows by the
         * audit stage — a reworded string there would silently zero the gauge.
         */
        const val STAGE_PAGING = "paging"
        const val STAGE_AUDITING = "auditing history (negentropy)"
        const val STAGE_RETRACTING = "reconciling the provider's own records (negentropy)"

        /** In-flight rows published per stream — matches the report side's own ceiling. */
        const val MAX_IN_FLIGHT_ROWS = 20

        /**
         * Held tails, the pool's steady-state socket count — `tailBudget` in
         * router.conf, defaulted from [RouterConfig.DEFAULT_TAIL_BUDGET].
         * Tails plus the visit width stay under the OkHttp dispatcher's 1,024
         * so the static upstreams, the probe passes and the healer keep
         * theirs.
         */
        const val DEFAULT_TAIL_BUDGET = RouterConfig.DEFAULT_TAIL_BUDGET

        /**
         * The revisit delay one relay has earned: the base its tail status
         * sets, shrunk by its recent yield, floored so a firehose relay is a
         * frequent guest and not a busy loop.
         *
         * A TAILED relay's revisit only serves the audit clock and
         * dropped-tail recovery — the tail carries its present — so its base
         * is half an hour. An UNTAILED relay carries its whole freshness on
         * this cadence, so its base is five minutes. Within either, "more
         * content lately" divides: fifty decayed events halves the wait, five
         * hundred cuts it to the floor. The scale is events, not events/sec,
         * because the score already decays — see [Yield].
         */
        internal fun revisitDelayMs(
            yieldScore: Double,
            tailed: Boolean,
        ): Long {
            val base = if (tailed) REVISIT_TAILED_MS else REVISIT_UNTAILED_MS
            return (base / (1.0 + yieldScore / YIELD_HALVES_THE_WAIT)).toLong().coerceAtLeast(REVISIT_FLOOR_MS)
        }

        const val REVISIT_TAILED_MS = 30L * 60 * 1000
        const val REVISIT_UNTAILED_MS = 5L * 60 * 1000
        const val REVISIT_FLOOR_MS = 60_000L

        /** The decayed-event count at which a relay's revisit wait halves. */
        const val YIELD_HALVES_THE_WAIT = 50.0

        /** How long recent content stays recent: an hour halves the score. */
        const val YIELD_HALF_LIFE_MS = 60L * 60 * 1000

        /** An empty roster re-checks the records on this clock, not the freshness bound's. */
        const val EMPTY_ROSTER_RETRY_MS = 60_000L

        /**
         * How far behind now a tail's `since` starts: the seam with the
         * catch-up that just ran. Events land at a relay after their
         * `created_at` (arrival, verification, indexing), so a tail opened at
         * `now` can miss what the catch-up's ceiling also missed. One minute
         * of overlap costs a few duplicate submissions ingest drops anyway.
         */
        const val TAIL_OVERLAP_SECONDS = 60L
    }
}
