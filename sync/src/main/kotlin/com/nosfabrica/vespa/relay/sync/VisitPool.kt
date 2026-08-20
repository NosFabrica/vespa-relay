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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.progress.VisitLedger
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
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
 * monitor currently grades prime; a fixed set of workers pulls from it,
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
 * then — when the stream sets `negentropySyncThePastSeconds` and the band's last full pass
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
 * ([refusedOutright]) ends the visit rather than re-opening the
 * same conversation once per remaining leg. A wedged relay costs one worker
 * one bounded visit, not one slot for hours — and the monitor's next sweep is
 * what decides whether it stays on the roster at all.
 */
internal class VisitPool(
    private val client: NostrClient,
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
    private val scope: CoroutineScope,
    /**
     * WHAT to sync, derived apart from the machinery that syncs it — the
     * verdict reads, the certified scans, the fold. The pool asks it to
     * [RosterBuilder.rebuild] on the roster clock and owns everything after.
     */
    private val rosterBuilder: RosterBuilder,
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
     * The whole rebuild, swapped as ONE reference — the asks and the shared
     * authors they were computed against. Two separate volatiles let a
     * delete decision judge an old roster's ask against a newer rebuild's
     * shrunken shared set; one snapshot cannot mix generations.
     */
    @Volatile
    private var currentRoster: RosterBuilder.Roster = RosterBuilder.Roster(asks = emptyMap(), sharedAuthors = emptyMap())

    private val roster: Map<NormalizedRelayUrl, List<RosterBuilder.Ask>> get() = currentRoster.asks

    /** The rotation's bookkeeping — offers, collisions, revisit timers. See [VisitQueue]. */
    private val queue = VisitQueue(scope)

    /**
     * One held live subscription: the id to unsubscribe, and [wants] of the
     * roster entry it was opened on. The set is what lets the next visit see
     * that the roster changed its asks since — a scan finding a new provider
     * pairing on a relay another stream already tails — and re-open the tail
     * on the current want list instead of returning early forever. A held
     * [sockets] claim rides with each.
     */
    private class Tail(
        val subId: String,
        val wantsAtOpen: Set<String>,
    )

    private val tails = ConcurrentHashMap<NormalizedRelayUrl, Tail>()
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
        fun foldedScore(nowMs: Long): Double {
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

    /**
     * WHEN EACH RELAY WAS LAST SYNCED, and what happened the last time it was
     * tried — the one piece of per-relay state here that OUTLIVES the visit it
     * describes. Everything else the pool publishes is either live ([ongoing],
     * dropped the instant a visit ends) or an odometer over the whole roster.
     * See [VisitLedger].
     */
    private val ledger = VisitLedger()

    /**
     * The ledger's rows, with the live half read HERE rather than stored there:
     * a tail can open and a revisit can be re-armed between two status ticks,
     * and the three lookups below are what keep a published row internally
     * consistent. One pass per collection, not a lookup per row — this walks
     * every relay the pool has ever visited.
     */
    fun visits(): VisitLedger.Snapshot {
        val nowMs = System.currentTimeMillis()
        val tailed = tails.keys.mapTo(HashSet()) { it.url }
        val due = queue.revisitsDueInSec(nowMs)
        val held = ongoing.entries.associate { (url, row) -> url.url to ((nowMs - row.startedMs) / 1000).coerceAtLeast(0) }
        return ledger.snapshot(
            nowMs = nowMs,
            tailed = { it in tailed },
            nextVisitInSec = { due[it] },
            heldForSec = { held[it] },
        )
    }

    private fun yieldOf(url: NormalizedRelayUrl): Yield = yields.getOrPut(url) { Yield() }

    /**
     * ONE RELAY MID-VISIT, for the in-flight list — the clocks and the stage,
     * on the same terms as the legacy engine's [InFlight.Relay] so the same
     * card column reads both. Removed the moment the visit ends: a tail is not
     * a worker, and listing 400 held tails as in-flight rows would bury the
     * one wedged visit the list exists to name.
     */
    private class OngoingVisit(
        val startedMs: Long,
    ) {
        /** Which stream's asks the visit is on right now — visits serve streams in turn. */
        @Volatile var stream: String? = null

        @Volatile var stage: String = "claiming the socket"

        /** The `created_at` second the walk or audit window is at — time-axis progress. */
        @Volatile var pagingUntil: Long? = null

        val events = AtomicLong()

        /** Any sign of life: an event, a negentropy frame, a window opening. */
        @Volatile var lastActivityMs: Long = startedMs
    }

    private val ongoing = ConcurrentHashMap<NormalizedRelayUrl, OngoingVisit>()

    /**
     * Every arrival's shared bookkeeping, whatever path delivered it: the
     * pool's odometer, the relay's yield score, and — when a visit is on the
     * socket — its event count and quiet clock. One helper because four
     * paths repeated it, and the fifth someone adds must not be able to
     * forget a counter. A null [ongoingVisit] is a tail: arrivals there belong to no
     * visit's clocks.
     */
    private fun arrived(
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit?,
    ) {
        val nowMs = System.currentTimeMillis()
        poolReceived.incrementAndGet()
        yieldOf(url).arrived.incrementAndGet()
        // WHEN THIS RELAY LAST DELIVERED ANYTHING. Recorded per event, and
        // therefore here rather than at the end of a visit, because for a
        // tailed relay the TAIL is the sync: a freshness reading taken only at
        // visit boundaries calls a relay streaming events right now half an
        // hour stale. One map lookup and one volatile write, the same order of
        // cost as the yield fold on the line above it.
        ledger.received(url.url, nowMs)
        ongoingVisit?.let {
            it.events.incrementAndGet()
            it.lastActivityMs = nowMs
        }
    }

    /**
     * The in-flight rows for one stream: every relay whose visit is currently
     * serving it, quietest first — the same ordering argument as
     * [InFlight]'s, because the row worth reading is the one nothing is
     * arriving on.
     *
     * UNBOUNDED, unlike every other list that leaves this process, and the
     * exception is earned. The others are derived from the url universe and
     * have no ceiling but discovery; this one cannot exceed
     * [visitConcurrency], because a row IS a worker. Twenty rows against 128
     * workers meant the card answered "what is this mirror connected to" with
     * a sixth of the answer and an `omitted` nobody reads as "you are seeing
     * 16% of it" — an operator watching one stream's rows drain to a single
     * relay was reading a truncation, not the mirror. The whole set is the
     * question, so the whole set is published.
     *
     * Note this is per CURRENT ASK, not per stream membership: one visit
     * serves every stream's asks in turn ([visit]), so a relay appears under
     * whichever stream it is on at this instant and the rows across streams
     * still sum to the worker count rather than multiplying by it.
     */
    private fun inFlightFor(stream: String): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            ongoing
                .entries
                .filter { it.value.stream == stream }
                .map { (url, row) ->
                    InFlight.Relay(
                        relay = url.url,
                        heldForSec = ((nowMs - row.startedMs) / 1000).coerceAtLeast(0),
                        // A visit IS on the socket from its first moment — the
                        // claim and the dial are inside it — so the two clocks
                        // agree by construction. Published anyway: this is the
                        // member the card reads as "has a transfer slot".
                        transferringForSec = ((nowMs - row.startedMs) / 1000).coerceAtLeast(0),
                        events = row.events.get(),
                        quietForSec = ((nowMs - row.lastActivityMs) / 1000).coerceAtLeast(0),
                        stage = row.stage,
                        pagingUntil = row.pagingUntil,
                    )
                }.sortedWith(compareByDescending<InFlight.Relay> { it.quietForSec }.thenByDescending { it.heldForSec }.thenBy { it.relay })
        // Zero, always, and published anyway: `omitted` is the schema's promise
        // that a list says what it dropped, and a reader that finds the member
        // missing cannot tell "nothing was dropped" from "this router does not
        // disclose". Kept so the answer stays explicit.
        return InFlight(relays = rows, omitted = 0)
    }

    /**
     * The stream rows' one phase, marked stale wherever its numbers change
     * hands — the roster rebuild, a tail opening, a tail dropping — and
     * PUBLISHED by [flushPhases] on the ticker. The publish walks
     * streams × roster, and it used to run INLINE on every tail open and
     * drop: a 600-tail boot storm paid ~45M entry visits on the visit
     * workers for numbers nobody reads faster than the report tick.
     */
    private fun phasesChanged() {
        phasesDirty.set(true)
    }

    private fun flushPhases() {
        val phases = phases ?: return
        val currentRoster = roster
        for (stream in streams) {
            val mine = currentRoster.entries.filter { entry -> entry.value.any { it.stream === stream } }
            phases.set(
                stream.name,
                StreamPhases.Phase.Rotating(
                    relays = mine.size,
                    tailed = mine.count { tails.containsKey(it.key) },
                ),
            )
        }
    }

    private val phasesDirty =
        java.util.concurrent.atomic
            .AtomicBoolean(false)

    private val evictedTails = AtomicLong()

    private val poolReceived = AtomicLong()
    private val visitsRun = AtomicLong()
    private val auditsRun = AtomicLong()

    /**
     * Audits not attempted because the monitor measured the relay as not
     * answering a NEG-OPEN — see [auditIfDue]. Counted rather than logged: it
     * is a per-ask, per-visit decision on a roster of thousands, and the
     * number beside `auditsRun` is what says whether a stream's history is
     * being re-checked by reconcile or is waiting on `refetchThePastSeconds`.
     */
    private val auditsSkipped = AtomicLong()
    private val abortedVisits = AtomicLong()

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
                Processors.Count("awaitingVisit", queue.waiting.toLong()),
                Processors.Count("visiting", queue.visiting.toLong()),
                Processors.Count("tails", tails.size.toLong()),
                Processors.Count("visitsRun", visitsRun.get()),
                // The gauge beside the odometer: audits RUNNING against
                // auditsRun's total. A deep history's audit holds a worker for
                // minutes, and without this the only trace was one unit of
                // `visiting` that could not be told from a catch-up.
                Processors.Count("auditing", ongoing.values.count { it.stage == STAGE_AUDITING || it.stage == STAGE_RETRACTING }.toLong()),
                Processors.Count("auditsRun", auditsRun.get()),
                Processors.Count("auditsSkipped", auditsSkipped.get()),
                Processors.Count("retracted", retraction?.deleted?.get() ?: 0L),
                Processors.Count("abortedVisits", abortedVisits.get()),
                Processors.Count("evictedTails", evictedTails.get()),
                Processors.Count("poolReceived", poolReceived.get()),
            )
        }
        // The streams' own rows: the pool's one phase, and the source that
        // names which relays a worker is on — see the [phases] parameter.
        for (stream in streams) {
            phases?.namesInFlight(stream.name) { inFlightFor(stream.name) }
        }
        flushPhases()
        scope.launch { rosterLoop() }
        scope.launch {
            while (scope.isActive) {
                delay(PHASE_FLUSH_MS)
                if (phasesDirty.getAndSet(false)) flushPhases()
            }
        }
        repeat(visitConcurrency) {
            scope.launch {
                queue.visitLoop(
                    stillWanted = { currentRoster.asks.containsKey(it) },
                    // Read at finish time: the delay depends on what the
                    // visit just delivered and whether a tail now carries
                    // this relay's present. Read, never getOrPut — a roster
                    // drop prunes the yield, and a finishing visit racing
                    // that prune must not resurrect the entry.
                    revisitDelayMs = { url ->
                        revisitDelayMs(yields[url]?.foldedScore(System.currentTimeMillis()) ?: 0.0, tails.containsKey(url))
                    },
                    visit = ::guardedVisit,
                )
            }
        }
    }

    /**
     * Rebuild the roster and feed the queue, on the tightest clock any source
     * asks for. Each source's own read is cached for its `refreshSeconds`, so
     * a tick that finds nothing expired costs a map lookup — the loop only has
     * to run often enough that no source's cache outlives its bound. The floor
     * keeps an aggressive setting from turning this into a poll.
     */
    private suspend fun rosterLoop() {
        val cadence =
            streams
                .flatMap { s -> s.discovery?.let { d -> (d.sources + d.gatedBy).map { it.refreshSeconds ?: d.refreshSeconds } }.orEmpty() }
                .minOrNull()
                ?.times(1000L)
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
        val built = rosterBuilder.rebuild()
        val next = built.asks
        val previous = currentRoster.asks
        val previousWants = currentRoster.wants
        currentRoster = built
        // A relay the monitor stopped certifying loses its tail and its socket
        // claim: the verdict is the admission, and holding a connection to a
        // relay we no longer trust to sync is the old machine's habit.
        for (url in previous.keys - next.keys) {
            dropTail(url)
            // The score dies with the certificate: a relay that comes back
            // after a week earns its tail on what it delivers then, not on a
            // decayed memory of what it was.
            yields.remove(url)
        }
        // WHAT THE ROSTER WANTS, into the ledger, so a relay that has LEFT it
        // says so on its row: "it stopped syncing" reads as a broken relay and
        // is as often this router declining to dial one.
        ledger.roster(rosterStreams(next))
        var enqueued = 0
        for ((url, asks) in next) {
            // (Re)queued when the url's ASK SET is news — a relay new to the
            // roster, or one already tailed whose want list changed (a scan
            // found a new provider pairing on a relay another stream holds).
            // Without the second half, that new ask would wait out the TAILED
            // revisit base for its first catch-up — and its retraction audit.
            if (previousWants[url] != built.wants[url] && queue.offer(url)) enqueued++
        }
        if (enqueued > 0 || previous.size != next.size) {
            System.err.println(
                "router: visit roster — ${next.size} prime relay(s) across ${streams.size} stream(s)" +
                    (if (enqueued > 0) ", $enqueued newly queued" else ""),
            )
        }
        phasesChanged()
    }

    /**
     * One visit, its failures counted and said, and its ENDING recorded — the
     * shape [VisitQueue.visitLoop] expects.
     *
     * The row is written here rather than inside [visit] so that every exit
     * writes exactly one, the throw included: a visit that dies is the state an
     * operator is most often chasing, and it was the one that left no trace
     * beyond a log line. The row is written AFTER [visit] returns, so the
     * relay has already left [ongoing] and no reader can see it both held and
     * finished.
     */
    private suspend fun guardedVisit(url: NormalizedRelayUrl) {
        val ongoingVisit = OngoingVisit(System.currentTimeMillis())
        try {
            val ending = visit(url, ongoingVisit)
            ledger.visited(url.url, ongoingVisit.startedMs, ending, ongoingVisit.events.get())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            abortedVisits.incrementAndGet()
            val what = "${e.javaClass.simpleName}: ${e.message?.take(80)}"
            ledger.visited(
                url.url,
                ongoingVisit.startedMs,
                VisitLedger.Ending.FAILED,
                ongoingVisit.events.get(),
                detail = "The visit threw and was abandoned — $what",
            )
            System.err.println("router: visit ${url.url} failed: $what")
        }
    }

    /**
     * One relay's turn: every stream's catch-up, the audit where due, the heal
     * drain, then the tail. Returns HOW IT ENDED, which is what the ledger row
     * — and the page's "why is this relay not syncing" column — is built from.
     */
    private suspend fun visit(
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
    ): VisitLedger.Ending {
        visitsRun.incrementAndGet()
        ongoing[url] = ongoingVisit
        sockets.claim(url)
        // One generation for the whole visit: the asks below and the shared
        // authors the retraction consults were computed together.
        val snapshot = currentRoster
        try {
            for (ask in snapshot.asks[url].orEmpty()) {
                // The legacy leg give-up, kept across the port: [NEG_IDLE_MS]
                // bounds one ask, this bounds the SEQUENCE of them. A relay
                // with hundreds of bound authors that answers each with a
                // full, empty idle window costs `asks * NEG_IDLE_MS` of a
                // worker — measured at 5h00m on one url. Silence, not a
                // deadline: any sign of life resets [OngoingVisit.lastActivityMs],
                // so a visit that is delivering is never cut, and the clock
                // starts at the claim so it cannot fire before the first ask.
                if (System.currentTimeMillis() - ongoingVisit.lastActivityMs > LEG_QUIET_GIVE_UP_MS) {
                    abortedVisits.incrementAndGet()
                    System.err.println(
                        "router: visit ${url.url} gave up after ${LEG_QUIET_GIVE_UP_MS / 60_000} quiet minute(s) — the revisit takes the remaining asks",
                    )
                    return VisitLedger.Ending.QUIET
                }
                // THE TWO MOVE TOGETHER OR THE ROW LIES. `stream` changes
                // here, per ask; the depth beside it is the previous ask's
                // until a leg overwrites it — and an ask whose band has no
                // outstanding legs never enters the loop that would, so the
                // reset inside `catchUp` is not enough on its own.
                ongoingVisit.stream = ask.stream.name
                ongoingVisit.pagingUntil = null
                val clean = catchUp(ask, url, ongoingVisit)
                // A refusal ends the whole visit, not just this ask's part:
                // the next ask is the same conversation with the same relay,
                // and the monitor's sweep — not a retry loop — is what
                // re-admits it.
                if (!clean) {
                    abortedVisits.incrementAndGet()
                    return VisitLedger.Ending.REFUSED
                }
                auditIfDue(
                    ask,
                    url,
                    ongoingVisit,
                    snapshot.sharedAuthors[ask.stream.name].orEmpty(),
                    snapshot.speaksNegentropy[url],
                )
            }
            ongoingVisit.stage = "draining queued heals, then the tail"
            healer.drain(url)
            openTail(url)
            return VisitLedger.Ending.SYNCED
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
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
    ): Boolean {
        val stream = ask.stream
        for (leg in bands.legs(stream.name, url, ask.filter)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            ongoingVisit.stage = STAGE_PAGING
            // PER LEG, like the three locals above it — and it is on the shared
            // visit object only because the status row reads it live.
            //
            // It only ever DECREASES (`event.createdAt < pagingUntil` is the
            // guard that assigns it), and one visit serves every stream's asks
            // on that relay in turn, each with its own legs. So once any leg
            // walked deep, every later leg's events were newer than the value
            // and the guard never fired again: the in-flight row went on
            // reporting the deepest point of an EARLIER leg's walk. The ask
            // loop resets it too, for the ask that has no legs at all; this one
            // is for the second and later legs of an ask that does.
            ongoingVisit.pagingUntil = null
            val onEvent: suspend (Event) -> Unit = { event ->
                arrived(url, ongoingVisit)
                // Newest-first is the walk's own order, so the oldest event
                // seen IS the cursor's depth, near enough for a reader.
                if (SyncCoverage.isPlausible(event.createdAt) && event.createdAt < (ongoingVisit.pagingUntil ?: Long.MAX_VALUE)) {
                    ongoingVisit.pagingUntil = event.createdAt
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
            if (refusedOutright(walked)) {
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
     * Which audit does this ask get? A retracting stream's audit IS the
     * deleteMissing comparison — the same full-history reconcile, plus the
     * licence to act on what we hold that the provider no longer serves; the
     * ordinary sweep would double the round trips to say half as much. Every
     * other stream with the knob set gets the plain history sweep. No
     * `negentropySyncThePastSeconds`, no audit of either kind.
     *
     * **A relay the monitor measured as not answering a NEG-OPEN is not asked.**
     * Both audits are negentropy end to end, so against such a relay the
     * attempt cannot succeed — and it was being made every `attemptSpacing`
     * (six hours at the top of its clamp) per ask, forever, because a failed
     * audit advances no clock. The verdict is already signed on the same 30166
     * record the roster admits the relay by; [speaksNegentropy] reads it.
     * UNMEASURED still tries: no verdict, an expired one, or a deployment with
     * no signer to read one by all mean "find out", not "give up". What
     * re-checks such a relay's past instead is `refetchThePastSeconds`.
     */
    private suspend fun auditIfDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
        sharedAuthors: Set<String>,
        speaksNegentropy: Boolean?,
    ) {
        val negentropySyncThePastSeconds = ask.stream.negentropySyncThePastSeconds ?: return
        if (speaksNegentropy == false) {
            auditsSkipped.incrementAndGet()
            return
        }
        if (ask.stream.deleteMissing != DeleteMissing.OFF) {
            retractionIfDue(ask, url, negentropySyncThePastSeconds, ongoingVisit, sharedAuthors)
        } else {
            sweepAudit(ask, url, negentropySyncThePastSeconds, ongoingVisit)
        }
    }

    /**
     * The weekly (or whatever `negentropySyncThePastSeconds` says) negentropy audit: when the
     * band's last full pass has aged past the knob, reconcile the covered past
     * in windows and download only the diff. Staggering is free — each relay's
     * band ages on its own clock — so the steady state is
     * `roster / negentropySyncThePastSeconds`, a trickle, and no cap is needed.
     */
    private suspend fun sweepAudit(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
    ) {
        val stream = ask.stream
        val now = nowSeconds()
        if (!bands.claimAudit(stream.name, url, ask.filter, negentropySyncThePastSeconds, now)) return
        val auditStarted = now
        var received = 0
        ongoingVisit.stage = STAGE_AUDITING
        val outcome =
            pager.sweep(
                stream.name,
                url,
                ask.filter,
                ask.filter,
                // Frames are life. A clean audit downloads NOTHING — every
                // window already agrees — so without this a relay whose whole
                // history verifies reads as a worker gone quiet for minutes.
                onProgress = { _, _ -> ongoingVisit.lastActivityMs = System.currentTimeMillis() },
                // The window's SINCE, which is the same reading as a paging
                // leg's cursor: how far BACK the audit has got. It was `until`,
                // the newer edge — so a sweep that had years left to compare
                // reported the row `back to <today>` and only moved once a
                // whole window finished. The pager announces a window after the
                // cut, so this descends.
                onWindow = { since, _ ->
                    ongoingVisit.lastActivityMs = System.currentTimeMillis()
                    ongoingVisit.pagingUntil = since
                },
            ) { event ->
                received++
                arrived(url, ongoingVisit)
                if (ask.filter.match(event)) {
                    ingest.submit(event, stream.trusted, IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions))
                }
            }
        auditsRun.incrementAndGet()
        if (outcome.complete) {
            // The audit compared every window up to the sweep's own head —
            // `slackSeconds` below its start, because a window still filling
            // is not swept — so the claim stops there too. Claiming through
            // the start over-ran that head by the slack, a seam the tail
            // only covered while it lived.
            bands.record(
                stream.name,
                url,
                ask.filter,
                observedMin = null,
                observedMax = null,
                paged = false,
                reconciledThrough = auditStarted - pager.slackSeconds,
            )
        }
        System.err.println(
            "router: audit ${stream.name} ${url.url} — $received event(s) recovered, " +
                (if (outcome.complete) "history verified" else "incomplete (negentropy usable: ${outcome.negentropyUsable})"),
        )
    }

    /**
     * The retraction audit for one ask, on the same `negentropySyncThePastSeconds` clock as
     * every other audit. The dueness, like the comparison, is
     * [RetractionAudit]'s own — the owned-ask band that schedules it is the
     * band the reconcile stamps, so both are derived in one place there.
     */
    private suspend fun retractionIfDue(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
        sharedAuthors: Set<String>,
    ) {
        val retraction = retraction ?: return
        if (!retraction.claimAudit(ask.stream, url, ask.filter, negentropySyncThePastSeconds)) return
        ongoingVisit.stage = STAGE_RETRACTING
        retraction.reconcileAndDelete(
            ask.stream,
            url,
            ask.filter,
            sharedAuthors,
            onActivity = { ongoingVisit.lastActivityMs = System.currentTimeMillis() },
        ) { arrived(url, ongoingVisit) }
        auditsRun.incrementAndGet()
    }

    /**
     * The live tail: one subscription per relay carrying every wanting
     * stream's filter, `since` a small overlap behind now so the seam with the
     * catch-up cannot drop an event that landed between them. The socket claim
     * taken here is released only when the roster drops the relay — the tail
     * is what "constantly connected" means.
     */
    private fun openTail(url: NormalizedRelayUrl) {
        val snapshot = currentRoster
        val urlAsks = snapshot.asks[url].orEmpty()
        if (urlAsks.isEmpty()) return
        // The rebuild fills `wants` for every url it puts in `asks`, so a
        // missing entry cannot happen while `wanting` is non-empty; said as
        // a return rather than carrying a live-looking recompute path.
        val wantsNow = snapshot.wants[url] ?: return
        val sitting = tails[url]
        if (sitting != null) {
            if (sitting.wantsAtOpen == wantsNow) return
            // The live subscription upstream still carries the want list from
            // when it was opened; the roster has since changed its mind about
            // this relay. Re-opened below on the current asks — a tail that
            // never asks for the new filter would silently miss its live
            // events until eviction did the re-open by accident.
            dropTail(url)
        }
        // THE BUDGET, and how a tail is earned past it. Under it every visited
        // relay keeps its tail. Over it, the candidate must outrank the
        // weakest sitting tail on recent yield — the socket goes to the relay
        // with more content lately, and the evicted one falls back to the
        // untailed revisit cadence, promptly requeued so its freshness gap is
        // one queue wait and not a timer.
        if (tails.size >= tailBudget) {
            val nowMs = System.currentTimeMillis()
            val candidate = yieldOf(url).foldedScore(nowMs)
            val weakest = tails.keys.minByOrNull { yieldOf(it).foldedScore(nowMs) } ?: return
            if (yieldOf(weakest).foldedScore(nowMs) >= candidate) return
            evictWeakestTail(sparing = url)
        }
        val subId = "visit-tail-${tailSeq.incrementAndGet()}"
        // CLAIM AND SUBSCRIBE BEFORE PUBLISHING: a concurrent dropTail — a
        // roster drop, another worker's eviction — must only ever meet a
        // FULLY-FORMED tail, a subscription it can unsubscribe and a claim it
        // can release. The old order (publish, claim, subscribe) let a
        // dropTail landing inside that window release someone else's claim
        // and strand a ghost subscription that re-attached on every
        // reconnect.
        sockets.claim(url)
        try {
            client.subscribe(
                subId,
                mapOf(url to tailFilters(urlAsks, nowSeconds() - TAIL_OVERLAP_SECONDS)),
                object : SubscriptionListener {
                    override suspend fun onEvent(
                        event: Event,
                        isLive: Boolean,
                        relay: NormalizedRelayUrl,
                        forFilters: List<Filter>?,
                    ) {
                        if (relay != url) return
                        arrived(url, ongoingVisit = null)
                        // Bind trust per stream, and re-check scope so a broken
                        // relay cannot widen what we ingest — the same rule the
                        // static tails follow. Matching is against each ASK's
                        // filter: a narrowed ask admits only its own provider's
                        // events off the tail, exactly as it does off a page.
                        // One pass, no intermediate list — this runs per event
                        // on every tail.
                        var any = false
                        var allTrusted = true
                        var healContent = false
                        var healRetractions = false
                        for (ask in roster[url].orEmpty()) {
                            if (!ask.filter.match(event)) continue
                            any = true
                            allTrusted = allTrusted && ask.stream.trusted
                            healContent = healContent || ask.stream.healContent
                            healRetractions = healRetractions || ask.stream.healRetractions
                        }
                        if (!any) return
                        ingest.submit(event, allTrusted, IngestOrigin(url, healContent, healRetractions))
                    }
                },
            )
        } catch (e: CancellationException) {
            sockets.release(url)
            throw e
        } catch (e: Exception) {
            // No entry was published, so nothing believes this relay is
            // tailed: it keeps the untailed revisit cadence and the next
            // visit tries again.
            sockets.release(url)
            System.err.println("router: tail ${url.url} failed to open: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            return
        }
        if (tails.putIfAbsent(url, Tail(subId, wantsNow)) != null) {
            // Another opener won this url. Visits are inFlight-guarded, so
            // this is nearly unreachable — handled because ours would
            // otherwise leak a subscription and a claim.
            runCatching { client.unsubscribe(subId) }
            sockets.release(url)
            return
        }
        // The rebuild may have decertified this url between the roster read
        // above and the publish — its dropTail then found nothing to drop.
        // Re-checking AFTER the publish closes the window: whichever side
        // runs second sees the other's write.
        if (!roster.containsKey(url)) {
            dropTail(url)
            return
        }
        trimTails(keep = url)
        phasesChanged()
    }

    /**
     * The check-then-act budget admits a few extra tails under concurrency
     * (N workers can pass the size check together), and an overshoot that is
     * never repaired holds sockets past the ceiling forever. Trimmed back to
     * budget after each publication, sparing the tail that just EARNED its
     * way in.
     */
    private fun trimTails(keep: NormalizedRelayUrl) {
        while (tails.size > tailBudget) {
            if (!evictWeakestTail(sparing = keep)) return
        }
    }

    /**
     * Drop the weakest sitting tail — sparing [sparing], the candidate that
     * just earned its way in — and requeue it promptly, so its freshness gap
     * is one queue wait and not a timer. False when there is nothing left to
     * evict. The one spelling of eviction, for the earn check and the
     * overshoot trim both.
     */
    private fun evictWeakestTail(sparing: NormalizedRelayUrl?): Boolean {
        val nowMs = System.currentTimeMillis()
        val weakest = tails.keys.filter { it != sparing }.minByOrNull { yieldOf(it).foldedScore(nowMs) } ?: return false
        evictedTails.incrementAndGet()
        dropTail(weakest)
        if (roster.containsKey(weakest)) queue.offer(weakest)
        return true
    }

    private fun dropTail(url: NormalizedRelayUrl) {
        val tail = tails.remove(url) ?: return
        runCatching { client.unsubscribe(tail.subId) }
        sockets.release(url)
        // The revisit timer this url earned WHILE TAILED is now the wrong one:
        // it was armed at [REVISIT_TAILED_MS] and this relay is on the
        // [REVISIT_UNTAILED_MS] cadence from here. Dropping it lets the visit
        // that follows arm the cadence the url actually has — see
        // [VisitQueue.disarm].
        queue.disarm(url)
        phasesChanged()
    }

    companion object {
        /**
         * EVERY down stream rides the pool now — declared `urls` and discovered
         * relays alike. The fork this used to draw was the crossing's, not a
         * design: a `urls` stream ran the legacy backfill, which walked each
         * relay ONCE per process and then live-tailed, so it had no way to
         * reconcile or re-fetch its past on any clock. One engine, one policy:
         * live tail, page forward from the band's edge, reconcile the past on
         * `negentropySyncThePastSeconds`, re-fetch it on `refetchThePastSeconds`.
         *
         * A retracting stream rides it like any other: its `deleteMissing`
         * comparison IS that reconcile, on the clock the loader requires it to
         * set ([RetractionAudit]).
         */
        internal fun ridesThePool(stream: SyncStream): Boolean = stream.dir != SyncDirection.UP && (stream.urls.isNotEmpty() || stream.discovery?.sources?.isNotEmpty() == true)

        /**
         * url → the streams asking for it, as the ledger keys them: url
         * STRINGS, and each stream named once however many asks it split into.
         *
         * Distinct is the whole point. A `relaySource` that binds authors
         * makes one ask PER PROVIDER against one relay, so a relay paired with
         * forty providers arrives here as forty asks of one stream — and the
         * row would have read `content, content, content, …` forty times.
         */
        internal fun rosterStreams(asks: Map<NormalizedRelayUrl, List<RosterBuilder.Ask>>): Map<String, List<String>> = asks.entries.associate { (url, a) -> url.url to a.map { it.stream.name }.distinct() }

        /**
         * Did this leg's walk end in a way that makes the NEXT leg futile?
         *
         * Only when it delivered nothing: a walk that carried events did real
         * work whatever ended it, and the later legs may fare the same.
         * [PagedFetchResult.End.DRAINED] is the opposite of a refusal — an
         * empty page the relay honestly EOSEd — and LIMIT_REACHED stopped on
         * our own instruction; every other ending is the relay (or the path
         * to it) declining the conversation the next leg would re-open, at
         * the price of an idle window of silence per leg.
         */
        internal fun refusedOutright(walked: PagedFetchResult): Boolean =
            walked.downloaded == 0 &&
                when (walked.end) {
                    PagedFetchResult.End.DRAINED, PagedFetchResult.End.LIMIT_REACHED -> false

                    PagedFetchResult.End.IDLE, PagedFetchResult.End.CLOSED,
                    PagedFetchResult.End.AUTH_REQUIRED, PagedFetchResult.End.CANNOT_CONNECT,
                    PagedFetchResult.End.UNPAGEABLE,
                    -> true
                }

        /**
         * The tail subscription's filters: every ask the roster wants at the
         * relay, single-author asks MERGED by the rest of their shape — a
         * relay paired with N providers gets one filter naming N authors,
         * not N filters. The old `.distinct()` deduplicated nothing
         * (quartz's Filter compares by reference), so a many-provider
         * relay's REQ carried hundreds of filters and filter-capped relays
         * refused the whole tail. Safe for the TAIL alone: trust and heal
         * are re-derived per event against each ask, so nothing downstream
         * needs per-filter granularity. An unbound ask absorbs the bound
         * ones of its shape — it already asks for every author.
         */
        internal fun tailFilters(
            asks: List<RosterBuilder.Ask>,
            since: Long,
        ): List<Filter> {
            val byShape = LinkedHashMap<String, MutableList<Filter>>()
            for (ask in asks) byShape.getOrPut(ask.filter.copy(authors = null).toJson()) { mutableListOf() } += ask.filter
            return byShape.values.map { group ->
                val unbound = group.firstOrNull { it.authors.isNullOrEmpty() }
                val authors = if (unbound != null) null else group.flatMap { it.authors.orEmpty() }.distinct().sorted()
                (unbound ?: group.first()).copy(authors = authors, since = since)
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
         * The visit's stages worth a word, in the in-flight rows' `doing`
         * column. Two independent axes, and each word names both:
         *
         *  - **What for.** CATCHING UP is everything new since the band's last
         *    pass; AUDITING is the whole past re-checked on the `negentropySyncThePastSeconds`
         *    clock, whose purpose is to find what a catch-up never saw.
         *  - **How.** Paging walks a REQ newest-first; negentropy compares set
         *    reconciliation windows and downloads only the difference.
         *
         * They do not imply each other, which is the whole reason the words
         * carry both: negentropy is not "the audit" — an audit can page (a
         * window the peer will not reconcile is drained over REQ inside the
         * sweep, and a static stream backfills either way), and a catch-up
         * could reconcile. This pool happens to page its catch-up and
         * reconcile its audits, and a reader must be able to see that rather
         * than infer it from the transport.
         *
         * Constants because the `auditing` gauge counts rows by the two audit
         * stages — a reworded string there would silently zero the gauge.
         */
        const val STAGE_PAGING = "catching up (paging)"
        const val STAGE_AUDITING = "auditing history (negentropy)"
        const val STAGE_RETRACTING = "auditing the provider's own records (negentropy)"

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

        /** How often stale phase numbers reach the document — see [phasesChanged]. */
        const val PHASE_FLUSH_MS = 1_000L

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
