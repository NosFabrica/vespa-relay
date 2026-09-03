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
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.sync.heal.Healer
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
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
 * ## Four jobs, one pool, and a word that says which
 *
 * The tail, the catch-up, the audit and the `refetchThePastSeconds` re-walk all
 * run out of the same queue and the same workers, so every count over the pool
 * added them together: `visiting` covered a mirror keeping up and a mirror
 * re-downloading years alike, and `tails` counted the fourth without naming
 * anybody. Each held row carries a POOL word beside its stage sentence for
 * exactly that reason, and the tails are published as a list of their own —
 * see the `POOL_` constants, [livePool] and [Stage].
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
    /**
     * The three things this pool asks of a relay — see [RelayReads]. An
     * interface rather than the client itself so the SCHEDULING can be driven
     * without a network: `fetchAllPages` is a quartz extension, so a pool
     * holding a `NostrClient` can only be tested by being one.
     */
    private val reads: RelayReads,
    /**
     * WHAT A RELAY SAID when it would not answer — see [RelayComplaints].
     * Beside [reads] rather than inside it because it is not a read: nothing
     * here asks the relay anything, it listens to what the relay volunteers on
     * a socket somebody else opened. Deaf by default, which is the probes and
     * every test that does not care: an abort then names its reason and not the
     * relay's own words, exactly as it did before this existed.
     */
    private val complaints: RelayComplaints = RelayComplaints.DEAF,
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
    private val sockets: Sockets,
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
     * HOW MANY WORKERS the pool runs, which is the SUM of what its streams
     * allow themselves — see [workersFor].
     *
     * There is no router-wide width any more. A worker that cannot get a visit
     * permit for any stream wanting the relay never dials, so the herd this
     * used to bound is bounded by the permits instead: the most simultaneous
     * TLS handshakes any configuration can produce IS that sum, and running
     * fewer workers than it would leave a configured share unreachable.
     *
     * The herd is worth naming because it was measured. The first 440-relay
     * integration run let the width equal the whole socket budget and watched
     * 436 of the dials time out inside one minute — a thundering herd against
     * its own connect timeout. A visit is seconds long, so a width bounds the
     * BURST rather than the throughput; the pool's steady state is the tails'
     * budgets, which are the streams' too.
     */
    private val workers: Int = DEFAULT_VISIT_CONCURRENCY,
    /**
     * WHAT EACH STREAM MAY SPEND on each of the four jobs — see [PoolLimits].
     * The width above is a DIAL count and says nothing about what the visits
     * behind it are doing; this is where an audit's cost is bounded apart from
     * a catch-up's, and one stream's share apart from another's. Uncapped by
     * default, which is exactly the behaviour it arrived beside.
     */
    private val limits: PoolLimits = PoolLimits(emptyMap()),
) {
    /**
     * WHEN EACH STREAM'S RE-READS OF THE PAST COME DUE — the engine's own
     * gate and the status row's countdown, from one place. See
     * [AuditSchedule], which is where `deleteMissing` picks the clock and
     * which is where the walk behind the status row lives.
     */
    private val schedule = AuditSchedule(streams, bands, retraction)

    /**
     * The whole rebuild, swapped as ONE reference — the asks and the shared
     * authors they were computed against. Two separate volatiles let a
     * delete decision judge an old roster's ask against a newer rebuild's
     * shrunken shared set; one snapshot cannot mix generations.
     */
    @Volatile
    private var currentRoster: RosterBuilder.Roster = RosterBuilder.Roster(asks = emptyMap(), sharedAuthors = emptyMap())

    /** The relays this pool is riding — `currentRoster.asks` is url → stream → asks. */
    private val roster: Map<NormalizedRelayUrl, Map<String, RosterBuilder.UnitAsks>> get() = currentRoster.asks

    /**
     * ONE UNIT OF WORK: a relay AND the stream working it.
     *
     * The unit used to be the relay alone, and one visit served every stream's
     * asks in turn over one socket. That made a relay's slowest stream
     * everyone's: fifty provider asks for `content` ran to completion before
     * `indexers` got its one, both shared a single revisit timer, and the
     * status page had to say a relay appears "under whichever stream it is on
     * at this instant" because the visit had no single answer.
     *
     * Splitting it lets every stream work a relay at the same time and keeps
     * each stream seeing that relay in ONE state — which is also the
     * correctness boundary rather than merely a tidy one: bands key on
     * (stream, url, filter), so two streams on one relay touch disjoint state
     * and need no lock, while two jobs of the SAME stream on one relay would
     * write the same band.
     *
     * The socket is still one. [RelaySockets] refcounts claims — it was built
     * for exactly this, "two streams routinely land on one relay while a probe
     * fingerprints it" — so N concurrent stream-visits share one connection.
     */
    internal data class VisitKey(
        val url: NormalizedRelayUrl,
        val stream: String,
    )

    /** The rotation's bookkeeping — offers, collisions, revisit timers. See [VisitQueue]. */
    private val queue = VisitQueue<VisitKey>(scope)

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
        /**
         * Since the subscription was opened — the live pool's own `held`
         * clock, and NOT the visit's: the worker that opened this tail moved
         * on seconds later, and dating the row from the visit would report
         * every tail as a few seconds old forever.
         */
        val openedMs: Long = System.currentTimeMillis(),
        /**
         * The live permit this subscription is charged for. Held for the
         * TAIL'S life rather than a piece of work's, which is why it travels
         * on the object and is released by `dropTail`: a tail is what the pool
         * does BETWEEN visits, and its cost is the socket it keeps rather than
         * any moment of work.
         *
         * ONE, because a tail is one unit's. It was a list — and carried the
         * stream names beside it — back when a subscription held every wanting
         * stream's filter and was charged to each of them. The unit of work is
         * a (relay, stream) pair now, so the stream is the map key and the
         * permit is a permit.
         */
        val hold: PoolLimits.Hold,
    ) {
        /**
         * What has arrived ON THE TAIL, and when the last one did.
         *
         * The pair that makes a live row worth listing at all. A tail costs a
         * socket for as long as it is held, and the two failure modes are
         * invisible from the count of tails: a relay that has published
         * nothing in a week is holding a socket for nothing, and a tail whose
         * subscription died upstream looks identical to a quiet relay from
         * here. Both read off `quiet` beside `events`, which is the same
         * reading a visiting leg's row is drawn for.
         *
         * Counted here rather than folded into the yield score because the
         * score is decayed and shared with the visits — a priority hint, not
         * a ledger, and not a number to show anyone.
         */
        val events = AtomicLong()

        @Volatile var lastEventMs: Long = openedMs
    }

    private val tails = ConcurrentHashMap<VisitKey, Tail>()
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

    private fun yieldOf(url: NormalizedRelayUrl): Yield = yields.getOrPut(url) { Yield() }

    /**
     * ONE ROW'S WORKLOAD, as the pair that must never come apart: the pool a
     * reader groups by, and the sentence a reader reads.
     *
     * The gauge below counts audits by [pool] rather than by the words, which
     * is the fragility this class removes — `auditing` used to be a count of
     * rows whose stage string equalled one of two literals, so rewording
     * either would have zeroed it silently. A [pool] of null is honest and
     * ordinary: a visit between jobs — claiming its socket, working out what
     * an ask still owes, draining the healer's queue — is in none of the four,
     * and the page draws it under its own word rather than dropping it.
     */
    private class Stage(
        /** The machine word — one of the `POOL_` constants, or null for a row in none of them. */
        val pool: String?,
        /** …and the sentence it is published beside, in the words the glossary defines. */
        val word: String,
    )

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

        /**
         * WHAT THIS VISIT IS DOING, as the one value that carries both the
         * sentence and the pool word — see [Stage]. One field rather than two
         * because the pool is what the page GROUPS by and the sentence is what
         * it prints, and a row whose two halves disagreed would file a
         * catch-up under the audits.
         */
        @Volatile var stage: Stage = CLAIMING

        /** The `created_at` second the walk or audit window is at — time-axis progress. */
        @Volatile var pagingUntil: Long? = null

        val events = AtomicLong()

        /** Any sign of life: an event, a negentropy frame, a window opening. */
        @Volatile var lastActivityMs: Long = startedMs
    }

    private val ongoing = ConcurrentHashMap<VisitKey, OngoingVisit>()

    /**
     * Every arrival's shared bookkeeping, whatever path delivered it: the
     * pool's odometer, the relay's yield score, and — when a visit is on the
     * socket — its event count and quiet clock. One helper because four
     * paths repeated it, and the fifth someone adds must not be able to
     * forget a counter. Exactly one of [ongoingVisit] and [tail] carries the
     * arrival's own clocks — a visit's leg has no tail and a tail has no
     * visit — and passing neither is a caller that has decided this event
     * belongs to nobody's row.
     */
    private fun arrived(
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit?,
        tail: Tail? = null,
    ) {
        poolReceived.incrementAndGet()
        yieldOf(url).arrived.incrementAndGet()
        ongoingVisit?.let {
            it.events.incrementAndGet()
            it.lastActivityMs = System.currentTimeMillis()
        }
        tail?.let {
            it.events.incrementAndGet()
            it.lastEventMs = System.currentTimeMillis()
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
     * A row is a UNIT OF WORK, and the unit is a (relay, stream) pair: a
     * visit serves one stream's asks on one relay ([visit]), so a relay two
     * streams both want appears under both at once and each row is its own
     * worker. The rows across streams therefore sum to the workers running,
     * which is what makes them comparable with the pool's own `visiting`.
     */
    private fun inFlightFor(stream: String): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            ongoing
                .entries
                .filter { it.key.stream == stream }
                .map { (key, row) ->
                    val heldForSec = ((nowMs - row.startedMs) / 1000).coerceAtLeast(0)
                    InFlight.Relay(
                        relay = key.url.url,
                        heldForSec = heldForSec,
                        // A visit IS on the socket from its first moment — the
                        // claim and the dial are inside it — so the two clocks
                        // agree by construction. Published anyway: this is the
                        // member the card reads as "has a transfer slot".
                        transferringForSec = heldForSec,
                        events = row.events.get(),
                        quietForSec = ((nowMs - row.lastActivityMs) / 1000).coerceAtLeast(0),
                        stage = row.stage.word,
                        pool = row.stage.pool,
                        pagingUntil = row.pagingUntil,
                    )
                }.sortedWith(QUIETEST_FIRST)
        // Zero, always, and published anyway: `omitted` is the schema's promise
        // that a list says what it dropped, and a reader that finds the member
        // missing cannot tell "nothing was dropped" from "this router does not
        // disclose". Kept so the answer stays explicit.
        return InFlight(relays = rows, omitted = 0)
    }

    /**
     * THE LIVE POOL — every relay whose tail subscription is open right now,
     * quietest first.
     *
     * The pool's steady state, and the half of it that published nothing but
     * its own SIZE until this existed. `tails: 412` is a number every healthy
     * deployment renders and no operator can act on: which relay holds a
     * socket, how long it has held it, and whether anything has ever come down
     * it were all unanswerable from outside this process — the same complaint
     * [InFlight] was written for, one pool over.
     *
     * ONE LIST AT THE ROOT, and every row NAMES ITS STREAM. It sits beside the
     * streams rather than inside them because it is the pool's steady state
     * and reads as one table, not because a tail has no owner: the unit of
     * work is a (relay, stream) pair, [tails] is keyed by it, and one
     * subscription therefore carries exactly one stream's filter and counts
     * exactly that stream's arrivals.
     *
     * It did not always. A tail used to be keyed by URL and carry every
     * wanting stream's filter at once, so a row belonged to all of them and to
     * none — splitting it per stream would have published one undivided event
     * count once per stream. That is the reason this list is at the root and
     * it is no longer a reason it cannot be grouped: a page that wants the
     * four pools per stream reads `stream` off the row like it does everywhere
     * else.
     *
     * WHOLE, on [InFlight]'s rule: the set is bounded by the streams' own tail
     * budgets — configuration, not the network — so publishing all of it is
     * bounded by construction, and a cut would only pick which sockets an
     * operator is not shown.
     *
     * Same shape as a visiting row, deliberately, down to the `doing` sentence
     * and the `pool` word: one table renderer, one glossary, one truncation
     * promise. The two clocks read the same way they do there — `held` is the
     * age of the subscription and `quiet` is how long since it last delivered,
     * which together separate a relay that has nothing to say from a tail that
     * has silently died upstream.
     */
    internal fun livePool(): InFlight {
        val nowMs = System.currentTimeMillis()
        val rows =
            tails
                .entries
                .map { (key, tail) ->
                    val heldForSec = ((nowMs - tail.openedMs) / 1000).coerceAtLeast(0)
                    InFlight.Relay(
                        relay = key.url.url,
                        // WHOSE tail it is. One stream per subscription now,
                        // so the live rows can name their owner where they
                        // used to belong to every wanting stream at once.
                        stream = key.stream,
                        heldForSec = heldForSec,
                        // A tail IS a socket for its whole life — the claim is
                        // taken before the subscribe and released only when the
                        // roster drops the relay — so the two clocks agree by
                        // construction, exactly as they do on a visit's row.
                        transferringForSec = heldForSec,
                        events = tail.events.get(),
                        quietForSec = ((nowMs - tail.lastEventMs) / 1000).coerceAtLeast(0),
                        stage = TAILING.word,
                        pool = TAILING.pool,
                    )
                }.sortedWith(QUIETEST_FIRST)
        // Zero, always, and published anyway — see [inFlightFor].
        return InFlight(relays = rows, omitted = 0)
    }

    /**
     * WHAT ONE STREAM MAY SPEND, per job — the caps, what is out against them,
     * and the work each has turned away. See [PoolLimits] and
     * [StreamPhases.Limit].
     *
     * Every job, including the ones nothing caps, so a reader can tell "this
     * stream is bounded by the pool alone" from "this build does not publish
     * that job".
     */
    private fun limitsFor(stream: String): List<StreamPhases.Limit> =
        PoolLimits.JOBS.map { (job, _) ->
            StreamPhases.Limit(
                job = job,
                cap = limits.capFor(stream, job),
                inUse = limits.heldBy(stream, job),
                deferred = limits.deferred(stream, job),
            )
        }

    /** The last walk and when it was taken — see [scheduleFor]. */
    private class ScheduleCache(
        val atMs: Long,
        val rows: Map<String, List<StreamPhases.Scheduled>>,
    )

    @Volatile
    private var scheduleCache: ScheduleCache? = null

    /**
     * WHEN THIS STREAM'S TWO SCHEDULED RE-READS COME DUE — see
     * [AuditSchedule], which owns the walk and the arithmetic.
     *
     * CACHED for a minute, and the cache is here rather than there because it
     * is a property of who ASKS: these periods are days, the walk is thousands
     * of asks, and the status tick asks every fifteen seconds. A minute old is
     * indistinguishable on a row measured in days.
     */
    private fun scheduleFor(stream: String): List<StreamPhases.Scheduled> {
        val nowMs = System.currentTimeMillis()
        val cached = scheduleCache
        if (cached != null && nowMs - cached.atMs < SCHEDULE_CACHE_MS) return cached.rows[stream].orEmpty()
        val fresh = schedule.rows(currentRoster.asks, nowSeconds())
        scheduleCache = ScheduleCache(nowMs, fresh)
        return fresh[stream].orEmpty()
    }

    private fun phasesChanged() {
        phasesDirty.set(true)
    }

    /**
     * ONE WALK PER COLLECTION, not one per stream — and each walk over the
     * SMALLEST collection that knows the answer.
     *
     * The three numbers used to be gathered per stream: a filter of the whole
     * roster, then a `VisitKey` allocated per surviving entry to ask the tail
     * map about it. On a 5,000-relay roster with three streams that is three
     * ~5,000-entry lists, ~15,000 throwaway keys and ~300,000 identity
     * comparisons — every flush, and the ticker runs at 1 Hz through the boot
     * storm that marks it dirty on every tail open and drop.
     *
     * Counted from the other side instead. `tails` is bounded by the streams'
     * live budgets rather than by discovery and already carries the stream in
     * its key, so grouping it is one walk of the small map and no allocation
     * per roster entry. The roster is walked once for all streams rather than
     * once each.
     */
    private fun flushPhases() {
        val phases = phases ?: return
        // Each of the three off the collection that knows it, once. See
        // [VisitQueue.waitingBy] for the queue's own split.
        val queuedByStream = queue.waitingBy { it.stream }
        val tailedByStream = tails.keys.groupingBy { it.stream }.eachCount()
        // …and the roster's own nesting, whose inner KEYS are the streams
        // wanting that relay. One walk for every stream, no allocation, and
        // off the SAME map `rosterVisits` counts units from — they were two
        // maps and these two adjacent numbers were read from different ones.
        val relaysByStream = HashMap<String, Int>()
        for (byStream in currentRoster.asks.values) {
            for (name in byStream.keys) relaysByStream.merge(name, 1, Int::plus)
        }
        for (stream in streams) {
            phases.set(
                stream.name,
                StreamPhases.Phase.Rotating(
                    relays = relaysByStream[stream.name] ?: 0,
                    tailed = tailedByStream[stream.name] ?: 0,
                    queued = queuedByStream[stream.name] ?: 0,
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

    /**
     * WHY visits are ending early, counted and said — the instrument
     * `abortedVisits` alone could not be. See [VisitAborts]; the total it owns
     * IS the `abortedVisits` this row used to publish from a bare counter.
     */
    private val aborts = VisitAborts()

    /**
     * WHAT EACH RELAY WILL TAKE IN ONE FILTER, learned from its own refusals —
     * see [FilterWidths]. Per pool rather than per stream: a relay's limit is
     * the relay's, and a stream that learns it should not leave the next one to
     * find out again.
     */
    private val widths = FilterWidths()

    fun start() {
        if (streams.isEmpty()) return
        // What this configuration will hold open, said once — see
        // [warnOnSocketBudget]. Here rather than at parse time because it is a
        // property of the streams that ride the POOL, and the loader does not
        // know which those are.
        warnOnSocketBudget(streams)
        progress.phase("rotating")
        progress.counts {
            // Named to collide with NOTHING the document already publishes:
            // `queued` is ingest's depth and `received` a cycle's socket count
            // elsewhere on this card, and one word meaning two quantities on
            // adjacent rows is the exact bug the vocabulary test exists for.
            listOf(
                Processors.Count("roster", roster.size.toLong()),
                // …and the same roster counted in UNITS OF WORK, which is
                // what the three numbers below partition. A relay three
                // streams want is one `roster` entry and three
                // `rosterVisits`: the pair is what is queued, visited and
                // revisited, so `visiting + awaitingVisit + between` adds up
                // to this and never to `roster`.
                Processors.Count(
                    "rosterVisits",
                    currentRoster.asks.values
                        .sumOf { it.size }
                        .toLong(),
                ),
                Processors.Count("awaitingVisit", queue.waiting.toLong()),
                Processors.Count("visiting", queue.visiting.toLong()),
                Processors.Count("liveHeld", tails.size.toLong()),
                Processors.Count("visitsRun", visitsRun.get()),
                // The gauge beside the odometer: audits RUNNING against
                // auditsRun's total. A deep history's audit holds a worker for
                // minutes, and without this the only trace was one unit of
                // `visiting` that could not be told from a catch-up.
                Processors.Count("negentropyRunning", ongoing.values.count { it.stage.pool == POOL_NEGENTROPY }.toLong()),
                Processors.Count("negentropyRuns", auditsRun.get()),
                Processors.Count("negentropySkipped", auditsSkipped.get()),
                Processors.Count("retracted", retraction?.deleted?.get() ?: 0L),
                Processors.Count("liveEvicted", evictedTails.get()),
                Processors.Count("poolReceived", poolReceived.get()),
                // Relays that have told us how wide a filter they will take, so
                // this stream's asks go to them in chunks — see [FilterWidths].
                // Zero on a deployment whose streams ask for few enough kinds
                // that nobody has ever complained, which is the fact to read it
                // for; it climbing is a roster acquiring width-capped relays,
                // not a fault.
                Processors.Count("narrowedRelays", widths.narrowed.toLong()),
                // …and the abort partition: `abortedVisits`, then the reasons
                // it is made of. Appended as a block rather than spelled out
                // here so the total and its parts cannot drift apart — see
                // [VisitAborts.counts].
            ) + aborts.counts()
        }
        // The streams' own rows: the pool's one phase, and the source that
        // names which relays a worker is on — see the [phases] parameter.
        for (stream in streams) {
            phases?.names(
                stream.name,
                // Which relays a worker is on…
                inFlight = { inFlightFor(stream.name) },
                // …what it may SPEND on each job, registered for every stream
                // whatever the config says: a row of uncapped jobs is the
                // answer "this stream is bounded by the pool alone", and a
                // stream absent from the list cannot be told from one the caps
                // forgot…
                limits = { limitsFor(stream.name) },
                // …and WHEN its two scheduled re-reads of the past come due,
                // which is the only thing that can show an audit ran because
                // its clock ran out rather than because something asked.
                schedule = { scheduleFor(stream.name) },
            )
        }
        flushPhases()
        scope.launch { rosterLoop() }
        scope.launch {
            while (scope.isActive) {
                delay(PHASE_FLUSH_MS)
                if (phasesDirty.getAndSet(false)) flushPhases()
            }
        }
        repeat(workers) {
            scope.launch {
                queue.visitLoop(
                    stillWanted = { key -> wantedBy(currentRoster, key) },
                    // Read at finish time: the delay depends on what the
                    // visit just delivered and whether a tail now carries
                    // this relay's present. Read, never getOrPut — a roster
                    // drop prunes the yield, and a finishing visit racing
                    // that prune must not resurrect the entry.
                    revisitDelayMs = { key ->
                        // The yield is the RELAY's — what it has delivered
                        // lately, whoever it delivered to — while the tail is
                        // this unit's. A relay that is busy for one stream is
                        // worth revisiting sooner for all of them; whether the
                        // freshness gap is a tail's or a timer's is per stream.
                        revisitDelayMs(yields[key.url]?.foldedScore(System.currentTimeMillis()) ?: 0.0, tails.containsKey(key))
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
        currentRoster = built
        // A UNIT the roster no longer wants loses its tail and its socket
        // claim: the verdict is the admission, and holding a connection for a
        // stream we no longer sync that relay for is the old machine's habit.
        // Per unit rather than per relay, because a relay can leave ONE
        // stream's roster and stay on another's — the scan that paired it
        // with a provider is per stream — and dropping the relay's whole tail
        // there would cut a stream that still wants it.
        for (key in tails.keys.filter { !wantedBy(built, it) }) {
            dropTail(key)
        }
        for (url in previous.keys - next.keys) {
            // The score dies with the certificate: a relay that comes back
            // after a week earns its tail on what it delivers then, not on a
            // decayed memory of what it was. Keyed by URL because that is what
            // it measures — what this relay delivers, to anyone.
            yields.remove(url)
        }
        var enqueued = 0
        for (url in next.keys) {
            // (Re)queued when the url's ASK SET is news — a relay new to the
            // roster, or one already tailed whose want list changed (a scan
            // found a new provider pairing on a relay another stream holds).
            // Without the second half, that new ask would wait out the TAILED
            // revisit base for its first catch-up — and its retraction audit.
            //
            // ONE OFFER PER STREAM that wants it: the unit of work is the
            // pair, so a relay three streams want is three units, queued and
            // revisited on three independent clocks.
            //
            // OFF `wants`, whose keys ARE that stream set — `want()` fills the
            // two in lockstep, so the map read on the very next line already
            // answers the question. Deriving it a second time from `asks`
            // allocated a list and a set per url per rebuild, and made the
            // requeue depend on two answers agreeing: where they did not, a
            // stream would be silently never queued, or queued against a want
            // set that was not its own.
            for ((stream, unit) in built.asks[url].orEmpty()) {
                // PER UNIT, so one stream's news is not another's: comparing
                // the url's whole want set requeued every stream on a relay
                // whenever a scan paired it with a new provider for ONE of
                // them.
                if (previous[url]?.get(stream)?.identity == unit.identity) continue
                if (queue.offer(VisitKey(url, stream))) enqueued++
            }
        }
        if (enqueued > 0 || previous.size != next.size) {
            System.err.println(
                "router: visit roster — ${next.size} prime relay(s) across ${streams.size} stream(s)" +
                    (if (enqueued > 0) ", $enqueued newly queued" else ""),
            )
        }
        phasesChanged()
    }

    /** One visit, its failures counted and said — the shape [VisitQueue.visitLoop] expects. */
    private suspend fun guardedVisit(key: VisitKey) {
        try {
            visit(key)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Through [VisitAborts] like every other abort, so this ending is
            // in the partition rather than beside it. It used to be the ONE
            // abort with a line of its own — two lines against ~4,400 aborts
            // over twenty minutes — which read as a pool whose visits almost
            // never failed.
            aborts
                .record(
                    key.stream,
                    key.url,
                    VisitAborts.Reason.FAILED,
                    asked = "${e.javaClass.simpleName}: ${e.message?.take(80)}",
                    said = null,
                )?.let(System.err::println)
        }
    }

    /** Every ask this unit of work owes — one stream's, against one relay. */
    private fun asksFor(
        snapshot: RosterBuilder.Roster,
        key: VisitKey,
    ): List<RosterBuilder.Ask> =
        snapshot.asks[key.url]
            ?.get(key.stream)
            ?.asks
            .orEmpty()

    /**
     * …and the same question asked as a PREDICATE — does this unit still exist
     * on [snapshot].
     *
     * Both are two map lookups and no allocation now that the roster is nested
     * by the unit of work. They used to scan a url's whole ask list and
     * allocate a filtered copy, on questions asked per queue draw, per revisit
     * arm, once per tail on every rebuild, after every eviction — and, before
     * the nesting, once per EVENT on every tail.
     */
    private fun wantedBy(
        snapshot: RosterBuilder.Roster,
        key: VisitKey,
    ): Boolean = snapshot.asks[key.url]?.containsKey(key.stream) == true

    /**
     * ONE STREAM'S TURN ON ONE RELAY: its catch-up, the audit where due, the
     * heal drain, then its tail.
     *
     * Every other stream may be doing the same on this relay at the same time,
     * over the same socket — see [VisitKey]. What is serialised is one
     * stream's own jobs, which is what keeps a band from being written by two
     * things at once.
     */
    private suspend fun visit(key: VisitKey) {
        val url = key.url
        // One generation for the whole visit: the asks below and the shared
        // authors the retraction consults were computed together.
        val snapshot = currentRoster
        val wanted = asksFor(snapshot, key)
        // THE DIAL WIDTH, decided BEFORE the socket is claimed — that ordering
        // is what makes a stream's `visitConcurrency` a bound on simultaneous
        // TLS handshakes and not merely on work. A worker refused here returns
        // without dialling, and the queue's revisit timer brings the unit back.
        val permit = limits.tryHold(key.stream, JOB_VISITING) ?: return
        visitsRun.incrementAndGet()
        val ongoingVisit = OngoingVisit(System.currentTimeMillis())
        ongoingVisit.stream = key.stream
        ongoing[key] = ongoingVisit
        sockets.claim(url)
        try {
            for (ask in wanted) {
                // The legacy leg give-up, kept across the port: [NEG_IDLE_MS]
                // bounds one ask, this bounds the SEQUENCE of them. A relay
                // with hundreds of bound authors that answers each with a
                // full, empty idle window costs `asks * NEG_IDLE_MS` of a
                // worker — measured at 5h00m on one url. Silence, not a
                // deadline: any sign of life resets [OngoingVisit.lastActivityMs],
                // so a visit that is delivering is never cut, and the clock
                // starts at the claim so it cannot fire before the first ask.
                if (System.currentTimeMillis() - ongoingVisit.lastActivityMs > LEG_QUIET_GIVE_UP_MS) {
                    aborts
                        .record(
                            key.stream,
                            url,
                            VisitAborts.Reason.GAVE_UP,
                            asked = "${LEG_QUIET_GIVE_UP_MS / 60_000} quiet minute(s), ${VisitAborts.asked(ask.filter)}",
                            said = null,
                        )?.let(System.err::println)
                    return
                }
                // BOTH MOVE TOGETHER OR THE ROW LIES. The depth is the
                // previous ask's until a leg overwrites it, and an ask whose
                // band has no outstanding legs never enters the loop that
                // would — so the reset inside `catchUp` is not enough on its
                // own. The STAGE is here for the same reason and it costs
                // more: most asks on a many-provider relay owe nothing and run
                // no audit, so a row left holding the previous ask's word went
                // on reporting `negentropy sync of the past` for the rest of the visit,
                // which under a page that files rows into a table by that word
                // would keep a relay in the audit pool long after the audit
                // ended. What is true between them is that the visit is
                // working out what this ask owes, which is in none of the four
                // pools and says so by carrying no pool word.
                ongoingVisit.pagingUntil = null
                ongoingVisit.stage = ASKING
                val refusal = catchUp(ask, url, ongoingVisit)
                // A refusal ends this stream's visit, not just this ask's
                // part: the next ask is the same conversation with the same
                // relay, and the monitor's sweep — not a retry loop — is what
                // re-admits it. Another stream's visit is its own
                // conversation and is unaffected.
                //
                // AND IT SAYS WHICH REFUSAL, which this path did not: it
                // returned here naming neither the relay, the stream, the ask
                // nor the reason, and it is ~90% of every abort this pool
                // counts. See [VisitAborts].
                if (refusal != null) {
                    aborts
                        .record(
                            key.stream,
                            url,
                            VisitAborts.of(refusal.end),
                            asked = VisitAborts.asked(refusal.filter),
                            said = complaints.since(url, refusal.askedAtMs),
                        )?.let(System.err::println)
                    return
                }
                auditIfDue(
                    ask,
                    url,
                    ongoingVisit,
                    snapshot.sharedAuthors[ask.stream.name].orEmpty(),
                    snapshot.speaksNegentropy[url],
                )
            }
            ongoingVisit.stage = FINISHING
            // Per url rather than per stream, and safe from several at once:
            // the healer's queue REMOVES what it hands out, so two streams
            // draining one relay take disjoint work.
            healer.drain(url)
            openTail(key)
        } finally {
            ongoing.remove(key)
            sockets.release(url)
            // The dial width goes back at the END of the visit, not with the
            // last ask: what it bounds is how many relays this stream is
            // visiting, and the heal drain and the tail open are still that.
            permit.release()
        }
    }

    /**
     * ONE REFUSED WALK — quartz's ending, the ask that met it, and when.
     *
     * The instant is the load-bearing member and the reason this is a class
     * rather than the `End` alone: [RelayComplaints] keeps one sentence per
     * relay, so it can only be attributed to THIS refusal by the clock the REQ
     * went out at. The filter is the CHUNK the relay actually saw, not the
     * leg — against a width-capped relay those differ, and the one an operator
     * needs on the line is the one that was refused.
     */
    class Refusal(
        val end: PagedFetchResult.End,
        val filter: Filter,
        val askedAtMs: Long,
    )

    /**
     * The catch-up: walk what the band says is outstanding. Returns the refusal
     * when the relay refused with nothing delivered — the visit's stop signal —
     * and null when every leg came back clean.
     *
     * **A leg refused on WIDTH is re-walked here rather than abandoned.** Nine
     * relays on staging reject this router's 139-kind ask outright instead of
     * trimming it, which under the stop signal above meant they could never
     * complete a single ask however often they were visited. [FilterWidths]
     * reads the relay's own complaint, learns what it will take, and the leg
     * goes back out as chunks — see there for why the cap cannot be a constant
     * of ours. Bounded at [MAX_NARROWINGS] per leg because each retry re-walks
     * the chunks that already succeeded; a relay that names no limit converges
     * by halving across visits instead, since the cap outlives the visit.
     */
    private suspend fun catchUp(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val stream = ask.stream
        // WHAT THIS ASK ALREADY HAD when the visit opened, read once and read
        // BEFORE the first `record` below widens it. It is the only thing that
        // tells the pool's two paging workloads apart: a leg that walks time
        // outside the band is the ordinary catch-up, and one that walks time
        // the band already covers is the `refetchThePastSeconds` re-walk —
        // the same transport, the same rows, an entirely different answer to
        // "why is this relay downloading its whole history again".
        val covered = bands.band(stream.name, url, ask.filter)
        for (leg in bands.legs(stream.name, url, ask.filter)) {
            val stage = if (rewalksCovered(leg, covered)) REFETCHING else CATCHING_UP
            // THE LEG'S OWN JOB DECIDES WHICH CAP IT PAYS, which is the whole
            // reason the classification exists in the engine rather than only
            // on the page: a re-fetch of years of history and a catch-up over
            // the last minute are the same call to `fetchAllPages`, and an
            // operator who wants to bound the first cannot do it by bounding
            // the transport.
            //
            // ONLY THE RE-FETCH PAYS A CAP. A catch-up is what a visit is
            // FOR and runs inside one — legs one at a time — so it is already
            // bounded by the stream's dial width, and a separate cap could
            // only bite below it, by making a relay we have already dialled do
            // less. The re-walk is the opposite: due-gated, independent of the
            // visit rate, and able to put every band on the walk at once.
            //
            // Refused is SKIPPED, not queued — see [PoolLimits]. The leg stays
            // outstanding because no band is recorded for it, so the next
            // visit walks it; what a full cap costs is a revisit delay.
            val hold =
                if (stage.pool == POOL_REFETCHING) {
                    limits.tryHold(stream.name, POOL_REFETCHING) ?: continue
                } else {
                    null
                }
            ongoingVisit.stage = stage
            val flooredLeg = leg.flooredForPaging()
            try {
                var narrowings = 0
                while (true) {
                    val refusal = walkLeg(ask, url, flooredLeg, ongoingVisit) ?: break
                    // THE ONE REFUSAL THIS POOL CAN TAKE DOWN ITSELF. Everything
                    // else — an auth wall, a policy refusal, a dead socket — is
                    // the relay declining to serve US and ends the visit; a
                    // width refusal is the relay declining an ask we can simply
                    // make smaller. Asked in this order so the ordinary refusal
                    // costs one map read: `learn` returns false for every
                    // sentence that is not about kinds, and for a cap this relay
                    // has already given us.
                    if (narrowings < MAX_NARROWINGS &&
                        widths.learn(url, complaints.since(url, refusal.askedAtMs), refusal.filter.kinds?.size ?: 0)
                    ) {
                        narrowings++
                        System.err.println(
                            "router: visit ${stream.name} ${url.url} — the relay refused a " +
                                "${refusal.filter.kinds?.size}-kind filter; asking in chunks of ${widths.capFor(url)} from here",
                        )
                        continue
                    }
                    return refusal
                }
            } finally {
                // From a `finally` including the `return` above: a refusal ends
                // the visit, and a permit left behind shrinks its cap by one
                // for the life of the process. Null on a catch-up, which pays
                // no cap of its own.
                hold?.release()
            }
        }
        return null
    }

    /**
     * ONE LEG, as the REQs this relay will actually take — itself, or its kinds
     * in chunks where the relay has told us it will not take them all
     * ([FilterWidths]). Returns the first chunk that was refused, or null when
     * every one of them came back clean.
     *
     * **A chunk records its own band, and that is safe rather than merely
     * convenient.** `SyncCoverage.Band` is per kind and `record` keeps only the
     * kinds the ask NAMED, so the chunks of one leg widen one band between them
     * and each carries evidence for exactly the kinds it walked. A split on
     * authors or on time would have no such property — which is why this splits
     * on kinds and nothing else.
     */
    private suspend fun walkLeg(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        flooredLeg: Filter,
        ongoingVisit: OngoingVisit,
    ): Refusal? {
        val stream = ask.stream
        for (chunk in widths.chunk(url, flooredLeg)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            // PER CHUNK, like the three locals above it — and it is on the
            // shared visit object only because the status row reads it live.
            //
            // It only ever DECREASES (`event.createdAt < pagingUntil` is the
            // guard that assigns it), and one visit serves every stream's asks
            // on that relay in turn, each with its own legs. So once any leg
            // walked deep, every later leg's events were newer than the value
            // and the guard never fired again: the in-flight row went on
            // reporting the deepest point of an EARLIER leg's walk. The ask
            // loop resets it too, for the ask that has no legs at all; this one
            // is for the second and later walks of an ask that does.
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
            // READ BEFORE THE REQ GOES OUT, and that ordering is the whole
            // contract with [RelayComplaints]: a sentence stamped before this
            // instant belongs to some earlier ask and must not be reported as
            // this one's cause.
            val askedAtMs = System.currentTimeMillis()
            val walked = reads.page(url, chunk, NEG_IDLE_MS, onEvent)
            if (refusedOutright(walked)) {
                // No band for the refused chunk: nothing was observed, nothing
                // drained, and a record would re-stamp a walk that never
                // happened. Same rule as the legacy engine's.
                return Refusal(walked.end, chunk, askedAtMs)
            }
            bands.record(
                stream.name,
                url,
                ask.filter,
                seenMin,
                seenMax,
                paged = true,
                observedByKind = seenByKind,
                drained = drainSettlesThePast(walked, chunk, ask.filter),
            )
        }
        return null
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
        // IS THERE ANY WORK HERE — asked first, read-only, stamping nothing.
        //
        // Most asks on most visits are inside their period: a stream's audits
        // are a trickle by design (roster / negentropySyncThePastSeconds), so
        // the overwhelming majority of times this is reached, the answer is
        // "nothing to do". Taking a permit to discover that let asks with NO
        // work refuse a permit to an ask that had some — a cap of 4 spent on
        // four map lookups while the one relay whose week was up went away
        // with a `deferred` that read as a cap set too low.
        if (!schedule.isDue(ask, url, negentropySyncThePastSeconds, nowSeconds())) return
        // THEN the cap, and THEN the claim, because claiming STAMPS the
        // attempt clock — `attemptSpacingSeconds`, up to six hours — so taking
        // the claim first and finding the cap full would spend the ask's next
        // attempt on work that never ran. Asked in this order a refusal costs
        // one revisit, and the dueness check above costs a permit to nobody.
        val hold = limits.tryHold(ask.stream.name, POOL_NEGENTROPY) ?: return
        try {
            if (ask.stream.deleteMissing != DeleteMissing.OFF) {
                retractionIfDue(ask, url, negentropySyncThePastSeconds, ongoingVisit, sharedAuthors)
            } else {
                sweepAudit(ask, url, negentropySyncThePastSeconds, ongoingVisit)
            }
        } finally {
            hold.release()
        }
    }

    /**
     * The weekly (or whatever `negentropySyncThePastSeconds` says) negentropy audit: when the
     * band's last full pass has aged past the knob, reconcile the WHOLE past in
     * windows and download only the diff. Staggering is free — each relay's
     * band ages on its own clock — so the steady state is
     * `roster / negentropySyncThePastSeconds`, a trickle, and no cap is needed.
     *
     * **THE BANDS SCHEDULE THIS PASS; THEY DO NOT BOUND IT.** The ask's filter
     * goes to [NegentropyPager.sweep] verbatim, so the range is
     * `filter.since ?: PLAUSIBLE_FLOOR` up to the sweep's head — no
     * [SyncBands.legs], no subtraction of what is already covered. That is the
     * whole point of the pass and not an oversight: a relay that back-filled
     * behind a catch-up leaves the band claiming that ground and the store
     * missing the events, so a sweep narrowed to what the band does NOT cover
     * could never find them. The bands decide only WHEN — `auditDueAt` off the
     * last `reconciledThrough` stamp, and an ask with no stamp is
     * [AuditClock.NEVER_AUDITED], which is always due — so a relay's first
     * audit reconciles a history the bands cover none of.
     *
     * The affordable part is that a reconcile compares ID SETS: a range that
     * already agrees costs the round trips and no events, which is what lets
     * the range be the whole of it every time.
     */
    private suspend fun sweepAudit(
        ask: RosterBuilder.Ask,
        url: NormalizedRelayUrl,
        negentropySyncThePastSeconds: Long,
        ongoingVisit: OngoingVisit,
    ) {
        val stream = ask.stream
        val now = nowSeconds()
        // Read BEFORE the claim, which is what the line below reports on: the
        // reconcile stamps a new verified-at when it completes, so asking
        // afterwards would print this audit's own clock every time.
        val verifiedBefore = bands.verifiedAt(stream.name, url, ask.filter)
        if (!bands.claimAudit(stream.name, url, ask.filter, negentropySyncThePastSeconds, now)) return
        val auditStarted = now
        var received = 0
        ongoingVisit.stage = NEGENTROPY
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
                (if (outcome.complete) "history verified" else "incomplete (negentropy usable: ${outcome.negentropyUsable})") +
                // WHY THIS ONE RAN, on the line that says it did. The document
                // carries the schedule as a distribution; this is the per-audit
                // half, and it is what turns "the audits look busy" into a
                // checkable claim — every line either names a clock that ran
                // out or says the ask had never been audited at all.
                ", last verified ${verifiedBefore?.let { "${auditStarted - it}s ago" } ?: "never"}",
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
        ongoingVisit.stage = RETRACTING
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
    private suspend fun openTail(key: VisitKey) {
        val url = key.url
        val snapshot = currentRoster
        val urlAsks = asksFor(snapshot, key)
        if (urlAsks.isEmpty()) return
        // THIS STREAM'S want set, not the url's. The rebuild fills `wants` for
        // every unit it puts in `asks`, so a missing entry cannot happen while
        // `urlAsks` is non-empty; said as a return rather than carrying a
        // live-looking recompute path.
        val wantsNow = snapshot.asks[url]?.get(key.stream)?.identity ?: return
        val sitting = tails[key]
        if (sitting != null) {
            if (sitting.wantsAtOpen == wantsNow) return
            // The live subscription upstream still carries the want list from
            // when it was opened; the roster has since changed its mind about
            // this relay. Re-opened below on the current asks — a tail that
            // never asks for the new filter would silently miss its live
            // events until eviction did the re-open by accident.
            dropTail(key)
        }
        // THIS STREAM'S TAIL AGAINST ITS OWN BUDGET, and how one is earned
        // past a full one. Under it every visited relay keeps its tail. At it,
        // the candidate must outrank this stream's weakest sitting tail on
        // recent yield — the socket goes to the relay with more content
        // lately, and the evicted one is promptly requeued so its freshness
        // gap is one queue wait and not a timer.
        //
        // ONE STREAM PER TAIL now, which is what makes `maxLiveConcurrency`
        // exact. A tail used to be one subscription carrying every wanting
        // stream's filter, charged to each of them — so the budgets summed to
        // more than the sockets held, and the earn check picked the GLOBALLY
        // weakest tail on a score (events per url) that says nothing about
        // which stream is paying. A low-volume stream lost every tail it held
        // to a firehose content relay with its own budget free.
        // A SPARE PERMIT, not a hold that fails — see [PoolLimits.trySpare].
        // A full live gate is where a tail is EARNED, not where it is dropped,
        // so a deferral here would mark every stream sitting at its live
        // budget as having work refused. `earnTail`'s own second ask is a
        // `tryHold` and does count: reaching it means the candidate could not
        // outrank anything this stream already holds.
        val hold = limits.trySpare(key.stream, POOL_LIVE) ?: earnTail(key) ?: return
        val subId = "visit-tail-${tailSeq.incrementAndGet()}"
        // BUILT BEFORE THE LISTENER CLOSES OVER IT, so the row's counters are
        // the ones the subscription feeds. Built and published as ONE object
        // for the same reason: a tail whose counters were looked up per event
        // in `tails` would lose everything that arrived between `subscribe`
        // and the `putIfAbsent` below — which on a busy relay is the whole
        // first burst, and the row would open reading `0 events` on a socket
        // that had already delivered thousands.
        val tail = Tail(subId, wantsNow, hold = hold)

        // EVERY WAY OUT BEFORE THE PUBLISH, in one place. A tail that never
        // reached `tails` is unwound by whoever built it — three paths reach
        // here (cancelled, failed to subscribe, lost the publish race) and
        // each has to hand back exactly what was taken. Written three times it
        // was three copies of one invariant, and the fourth resource a tail
        // ever takes would leak from whichever copy was missed.
        //
        // `untail` only where a subscription was actually opened; the other
        // two land here because it was not.
        fun abandon(untail: Boolean = false) {
            if (untail) reads.untail(subId)
            sockets.release(url)
            hold.release()
        }
        // CLAIM AND SUBSCRIBE BEFORE PUBLISHING: a concurrent dropTail — a
        // roster drop, another worker's eviction — must only ever meet a
        // FULLY-FORMED tail, a subscription it can unsubscribe and a claim it
        // can release.
        sockets.claim(url)
        try {
            // …AND THE TAIL PAYS THE SAME WIDTH. A relay that refuses a
            // 139-kind catch-up refuses the identical filter on a live
            // subscription, so a tail opened at full width against a
            // width-capped relay is one that silently never delivers. The
            // chunks are one REQ between them, exactly as the merged shapes
            // beside them are.
            reads.tail(subId, url, widths.chunkAll(url, tailFilters(urlAsks, nowSeconds() - TAIL_OVERLAP_SECONDS))) { event ->
                arrived(url, ongoingVisit = null, tail = tail)
                // Bind trust per stream, and re-check scope so a broken relay
                // cannot widen what we ingest. Matching is against THIS
                // stream's asks alone: the subscription carries only its
                // filters now, so an event matching none of them is not ours
                // to ingest under this tail.
                //
                // THIS STREAM'S ASKS, straight off the roster's nesting —
                // no scan and no allocation. `currentRoster` and not the
                // captured snapshot, because a tail outlives the rebuild that
                // opened it and must ingest against what is wanted NOW.
                var any = false
                var allTrusted = true
                var healContent = false
                var healRetractions = false
                for (ask in currentRoster.asks[url]
                    ?.get(key.stream)
                    ?.asks
                    .orEmpty()) {
                    if (!ask.filter.match(event)) continue
                    any = true
                    allTrusted = allTrusted && ask.stream.trusted
                    healContent = healContent || ask.stream.healContent
                    healRetractions = healRetractions || ask.stream.healRetractions
                }
                if (any) ingest.submit(event, allTrusted, IngestOrigin(url, healContent, healRetractions))
            }
        } catch (e: CancellationException) {
            abandon()
            throw e
        } catch (e: Exception) {
            // No entry was published, so nothing believes this unit is
            // tailed: it keeps the untailed revisit cadence and the next
            // visit tries again.
            abandon()
            System.err.println("router: tail ${key.stream} ${url.url} failed to open: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            return
        }
        if (tails.putIfAbsent(key, tail) != null) {
            // Another opener won this unit. Visits are inFlight-guarded, so
            // this is nearly unreachable — handled because ours would
            // otherwise leak a subscription and a claim.
            abandon(untail = true)
            return
        }
        // The rebuild may have decertified this url between the roster read
        // above and the publish — its dropTail then found nothing to drop.
        // Re-checking AFTER the publish closes the window: whichever side
        // runs second sees the other's write.
        if (!wantedBy(currentRoster, key)) {
            dropTail(key)
            return
        }
        phasesChanged()
    }

    /**
     * EARN THIS UNIT A TAIL PERMIT by giving up its stream's weakest one, or
     * return null and leave it untailed.
     *
     * Called only when the stream's budget refused, so the comparison is the
     * one the budget exists to force: is this relay worth more to this stream
     * than the least productive relay it is already holding a socket for. Both
     * sides are the decayed yield score — events lately, per url — and the
     * candidate has to WIN, not tie, so a pool of equals does not churn its
     * sockets on every visit.
     *
     * Only this stream's tails are candidates, which is the point: another
     * stream's socket is not this stream's to spend.
     *
     * There is no overshoot to trim. The budget used to be a check-then-act on
     * `tails.size`, which N workers could pass together and hold more sockets
     * than configured until something noticed; a permit is taken or it is not.
     */
    private fun earnTail(candidate: VisitKey): PoolLimits.Hold? {
        val nowMs = System.currentTimeMillis()
        val mine = yieldOf(candidate.url).foldedScore(nowMs)
        // ONE FOLD PER TAIL, and the winner's score kept rather than re-read.
        // `foldedScore` DRAINS what has arrived since the last fold, so asking
        // twice is not free and not even the same question: a tail that
        // delivered between the two reads answers higher the second time, and
        // the comparison this budget turns on would be decided by that timing
        // rather than by the scores it was given.
        var weakest: VisitKey? = null
        var weakestScore = Double.MAX_VALUE
        for (key in tails.keys) {
            if (key == candidate || key.stream != candidate.stream) continue
            val score = yieldOf(key.url).foldedScore(nowMs)
            if (score < weakestScore) {
                weakest = key
                weakestScore = score
            }
        }
        if (weakest == null || weakestScore >= mine) return null
        evictedTails.incrementAndGet()
        dropTail(weakest)
        if (wantedBy(currentRoster, weakest)) queue.offer(weakest)
        // THE PERMIT IS RE-TAKEN, NOT HANDED OVER, and another opener of this
        // stream can win it in between — the candidate that paid for the
        // eviction then gets nothing and returns without a tail.
        //
        // Left as it is, deliberately. The socket cannot escape: gates are per
        // (stream, job) and the weakest is chosen from this stream's own tails,
        // so whoever wins is this stream spending its own budget, and the
        // budget is never under-used. Both units come back — the evicted one
        // was just requeued, the candidate arms its revisit as its visit ends
        // — so the cost of losing the race is one revisit delay for one unit.
        // Closing it means `dropTail` returning the freed hold instead of
        // releasing it, on a path four other callers share, to buy back a
        // delay the pool already tolerates everywhere else.
        return limits.tryHold(candidate.stream, POOL_LIVE)
    }

    private fun dropTail(key: VisitKey) {
        val tail = tails.remove(key) ?: return
        reads.untail(tail.subId)
        sockets.release(key.url)
        // The stream gets its share back here and only here — `remove` above
        // is what makes this the one path, however the tail ends (eviction, a
        // roster drop, a re-open on changed wants).
        tail.hold.release()
        // The revisit timer this unit earned WHILE TAILED is now the wrong
        // one: it was armed at [REVISIT_TAILED_MS] and this unit is on the
        // [REVISIT_UNTAILED_MS] cadence from here. Dropping it lets the visit
        // that follows arm the cadence it actually has — see
        // [VisitQueue.disarm].
        queue.disarm(key)
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
         * THE ORDER EVERY HELD LIST LEAVES IN: quietest first, then
         * longest-held, then the url so ties are stable across ticks.
         *
         * One comparator because the two lists are deliberately one shape —
         * `inFlightFor` and `livePool` publish the same row and the page draws
         * them with one renderer, so a re-sort in either that disagreed would
         * put the row worth looking at somewhere else in one table than the
         * other. The front end re-applies this same order when it merges them.
         */
        private val QUIETEST_FIRST =
            compareByDescending<InFlight.Relay> { it.quietForSec }
                .thenByDescending { it.heldForSec }
                .thenBy { it.relay }

        /**
         * Concurrent visits, which is concurrent DIALS — see [workersFor] for
         * how a set of streams adds up to one, and
         * [RouterConfig.DEFAULT_VISIT_CONCURRENCY] for the herd the number
         * exists to break up.
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
         * Each word is paired with the POOL it belongs to below, and the two
         * travel as one [Stage] value — the gauge and the page group by the
         * pool word, so rewording a sentence here can no longer silently zero
         * a count or empty a table.
         */
        const val STAGE_PAGING = "catching up (paging)"
        const val STAGE_REFETCHING = "re-fetching the past (paging)"
        const val STAGE_NEGENTROPY = "negentropy sync of the past"
        const val STAGE_RETRACTING = "negentropy sync of the provider's own records"
        const val STAGE_TAILING = "holding a live tail"
        const val STAGE_CLAIMING = "claiming the socket"
        const val STAGE_ASKING = "checking what this ask still owes"
        const val STAGE_FINISHING = "draining queued heals, then the tail"

        /**
         * THE POOL'S FOUR WORKLOADS, as the words a reader may group rows by —
         * `pool` on every row the mirror publishes, and the four lists the
         * status page draws from them.
         *
         * They are not four pieces of machinery. One rotating queue and one
         * set of workers run all of it (see this class's head); what differs is
         * what a relay is being asked FOR at this instant, and that was the
         * question nothing could answer. `visiting: 100` counted a catch-up, a
         * history audit and a whole-corpus re-walk as one number, and
         * `tails: 412` counted the fourth without naming anybody.
         *
         *  - [POOL_LIVE] a held subscription: no worker, events as they exist.
         *  - [POOL_CATCHING_UP] paging forward over what the band does not cover.
         *  - [POOL_REFETCHING] paging over what it DOES — `refetchThePastSeconds`.
         *  - [POOL_AUDITING] reconciling the whole past, both audits — see
         *    [sweepAudit] for why the bands schedule it and do not bound it.
         *
         * A visit between them — claiming its socket, working out what an ask
         * still owes, draining the healer — carries no pool word at all, and
         * the page draws it under its own sentence rather than filing it under
         * one of these.
         */
        const val POOL_LIVE = "live"
        const val POOL_CATCHING_UP = "catching-up"
        const val POOL_REFETCHING = "re-fetching"
        const val POOL_NEGENTROPY = "negentropy"

        /**
         * The fifth budgeted job, and the one that is NOT a pool: how many
         * relays may be visited for a stream at once — its dial width. A held
         * row never carries this word (a row is in one of the four pools, or
         * between them), so it appears only in the limits, which is where a
         * dial width belongs.
         */
        const val JOB_VISITING = "visiting"

        /** The pairings themselves — the only place a word and a pool are put together. */
        private val CLAIMING = Stage(null, STAGE_CLAIMING)
        private val ASKING = Stage(null, STAGE_ASKING)
        private val CATCHING_UP = Stage(POOL_CATCHING_UP, STAGE_PAGING)
        private val REFETCHING = Stage(POOL_REFETCHING, STAGE_REFETCHING)
        private val NEGENTROPY = Stage(POOL_NEGENTROPY, STAGE_NEGENTROPY)
        private val RETRACTING = Stage(POOL_NEGENTROPY, STAGE_RETRACTING)
        private val FINISHING = Stage(null, STAGE_FINISHING)
        private val TAILING = Stage(POOL_LIVE, STAGE_TAILING)

        /**
         * Is this leg walking time the band ALREADY COVERS — the re-fetch — or
         * time outside it, which is the ordinary catch-up?
         *
         * Derived from the leg and the band rather than from the clock that
         * produced them. `refetchThePastSeconds` expires a band inside quartz's
         * [SyncCoverage], and re-deriving its dueness here would be a second
         * copy of a rule we do not own — right until the day it changes, when
         * the row would name the wrong pool and nothing would fail. What a leg
         * OVERLAPS is observable, holds whatever the rule is, and is the fact
         * the reader actually wants.
         *
         * STRICT overlap on both edges, because the two ordinary legs touch the
         * band exactly at its edges: quartz asks for the newer leg from the
         * band's max and the older one down to its min, so a `<=` here would
         * file every routine catch-up as a re-walk of everything.
         *
         * AGAINST THE LEG'S OWN KINDS, never the band's aggregate edges. A
         * band holds one span PER KIND and quartz emits one leg per kind
         * group, each windowed on that group's own span —
         * `Band.minCreatedAt`/`maxCreatedAt` are a fold over all of them. On a
         * stream over many kinds (contentViaOutbox rides ~130) the kinds do not
         * cover the same time, so comparing a 30023 leg against a band whose
         * edges come from kind 1 put ordinary catch-up inside the aggregate and
         * called it a re-walk. That is not a labelling nit — a re-walk takes a
         * `refetchConcurrency` permit and is SKIPPED when the cap is full, and
         * that cap is small by design.
         *
         * NOTHING RECORDED FOR THESE KINDS is a first walk, so: an absent
         * band, a band with no spans, and a leg whose own kinds have no span
         * are one answer. The last is the case the aggregate could not see at
         * all — a kind added to a live stream's filter walks from scratch
         * while the band beside it is full.
         *
         * A leg naming NO kinds asks for everything, so it is judged against
         * everything the band holds — which is also the shape quartz records
         * under `ALL_KINDS` for a filter that names none.
         */
        internal fun rewalksCovered(
            leg: Filter,
            covered: SyncCoverage.Band?,
        ): Boolean {
            if (covered == null || covered.spans.isEmpty()) return false
            val mine = leg.kinds?.mapNotNull { covered.spans[it] } ?: covered.spans.values.toList()
            if (mine.isEmpty()) return false
            val from = leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR
            val to = leg.until ?: Long.MAX_VALUE
            return from < mine.maxOf { it.max } && to > mine.minOf { it.min }
        }

        /**
         * What a stream that names no `maxLiveConcurrency` contributes to the
         * sockets this pool holds — see
         * [RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY]. The sum of
         * them plus the summed dial widths is what has to stay under the
         * OkHttp dispatcher's 1,024 so the static upstreams, the probe passes
         * and the healer keep theirs; [warnOnSocketBudget] says so at boot.
         */
        const val DEFAULT_MAX_LIVE_CONCURRENCY = RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY

        /**
         * How long a computed audit/re-fetch schedule is reused. A minute:
         * these periods are days, the walk is thousands of asks, and the
         * status tick asks every fifteen seconds.
         */
        internal const val SCHEDULE_CACHE_MS = 60_000L

        /**
         * HOW MANY WORKERS a set of streams asks for: the sum of their dial
         * widths, since that is the most dialling any arrangement of their
         * permits can produce, with [RouterConfig.UNCAPPED_STREAM_VISITS]
         * standing in for a stream that names none.
         *
         * Fewer would leave a configured share unreachable — a stream allowed
         * 64 visits cannot have them if only 32 workers exist to draw its
         * relays — and more would be workers that can never get a permit.
         * Floored at one so a deployment with no visit streams still has a
         * pool it can start.
         */
        internal fun workersFor(streams: List<SyncStream>): Int =
            streams
                .sumOf { it.visitConcurrency ?: RouterConfig.UNCAPPED_STREAM_VISITS }
                .coerceAtLeast(1)

        /**
         * SAY WHAT THIS CONFIGURATION WILL HOLD OPEN, once, at boot.
         *
         * The socket ceiling used to be arithmetic an operator could do in
         * their head from two router-wide numbers: dial width plus tail budget,
         * kept under OkHttp's dispatcher limit so the static upstreams, the
         * monitor's probes and the healer keep theirs. Both numbers moved
         * inside the streams, so the sum is now spread across the config file
         * and nothing was checking it.
         *
         * AN UPPER BOUND, and it says so. A tail is ONE subscription carrying
         * every wanting stream's filter, charged to each of them — so a relay
         * two streams both want is one socket against two budgets, and the
         * sum over-counts by however much the rosters overlap. Summing anyway
         * is the right conservative move for a ceiling nobody wants to
         * discover by hitting it, but a number that claimed to be the socket
         * count would be wrong on every multi-stream deployment.
         *
         * Printed rather than refused, on the same terms as
         * `announceUncheckedPasts`: a deployment on a tuned dispatcher is a
         * legitimate deployment, and the router's job is to make sure nobody is
         * over the line by accident. The failure it warns about is the worst
         * kind — not an error but occlusion, every NEW connect strangled behind
         * sockets already held.
         */
        internal fun warnOnSocketBudget(streams: List<SyncStream>) {
            val dials = workersFor(streams)
            val tails = streams.sumOf { it.liveBudget }
            if (dials + tails <= SOCKET_HEADROOM) return
            System.err.println(
                "router: the streams together ask for up to $dials simultaneous dial(s) and $tails tail(s) — " +
                    "at most ${dials + tails} sockets against a dispatcher ceiling of $DISPATCHER_CEILING, which " +
                    "leaves little for the static upstreams, the monitor's probes and the healer. An UPPER bound: " +
                    "one relay two streams both want is one socket charged to both budgets, so the real count is " +
                    "lower by however much their rosters overlap. Lower `visitConcurrency` or `maxLiveConcurrency` on the " +
                    "streams that need them least",
            )
        }

        /**
         * OkHttp's dispatcher ceiling, which is the real concurrency limit
         * every plane in this process shares.
         */
        internal const val DISPATCHER_CEILING = 1_024

        /** …and what the visit pool may take of it, leaving the other planes theirs. */
        internal const val SOCKET_HEADROOM = 900

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
