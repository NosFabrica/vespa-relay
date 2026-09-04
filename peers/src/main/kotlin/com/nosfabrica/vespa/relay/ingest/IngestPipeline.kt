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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusalSink
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.crypto.verifyId
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

/**
 * The download-to-store pipeline every mirrored event funnels through: a
 * bounded channel, a pool of workers draining it in batches through
 * [IEventStore.batchInsert], with duplicates dropped and the rest signature-
 * verified off the download threads (verification is skipped for trusted
 * upstreams).
 *
 * The channel is bounded so a fast download (negentropy can deliver >10k/s)
 * cannot outrun Vespa ingest and pile events onto the heap: when it fills,
 * [submit] suspends the producing coroutine and the upstream throttles to the
 * ingest rate — flat memory instead of an OOM.
 */
class IngestPipeline(
    private val store: IEventStore,
    /**
     * The two knobs this needs, rather than the whole `RouterConfig` it used to
     * take. Both planes write through this pipeline, and a queue does not need
     * to see a stream list to size itself — the narrower argument is what lets
     * it sit below the config that configures it.
     */
    tuning: IngestTuning,
    // When set, every mirrored event is also run through quartz's
    // search-indexing parse to collect what quartz cannot read.
    private val audit: ParseAudit?,
    // Clients first: ingest yields when their reads slow down.
    private val servingPressure: ServingPressure?,
    private val scope: CoroutineScope,
    /**
     * Which of these ids the store ALREADY holds — `VespaEventIndex.existingIds`
     * in the router, the same summary-free existence check the store's own bulk
     * path runs. Null disables the probe entirely, which is only slower, never
     * wrong: the store deduplicates again regardless. See [dropDuplicates].
     */
    private val knownIds: (suspend (List<String>) -> Set<String>)? = null,
    /**
     * The newest stored version of each `(kind, author)` address in a chunk —
     * the read that lets a superseded replaceable be dropped before it is
     * verified. Null disables it; the store then resolves supersession itself,
     * as it always did. See [dropSuperseded].
     */
    private val newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)? = null,
    /**
     * Where store refusals are reported and where suppression is asked about.
     * Defaults to off, so every existing caller and test behaves exactly as it
     * did before this existed.
     */
    private val refusals: RefusalSink = RefusalSink.None,
    /**
     * How long a batch pass has to have been running before a full queue reads
     * as wedged rather than as backpressure — see [wedged].
     *
     * A parameter only so a test can reach the state without waiting two
     * minutes for it; nothing configures it, and [WEDGE_AFTER_MS] is the number
     * that ships.
     */
    private val wedgeAfterMs: Long = WEDGE_AFTER_MS,
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
        val origin: IngestOrigin,
    )

    private val workers = tuning.concurrency
    private val configuredBatch = tuning.batch

    /**
     * How many downloaded events may wait for ingest. Bounded at both ends —
     * this was `batch * 4` with only a floor, so raising the batch to 20000
     * silently sized the queue at 80,000 events and the heap went over. Batch
     * size and queue depth are separate concerns: the batch decides how much
     * each mutex hold amortises, the queue how much memory sits between
     * download and write.
     */
    val capacity = (tuning.batch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

    /**
     * How many events one worker takes per pass — capped to its fair share of
     * the channel.
     *
     * **The fairness this protects is worth less than the width it costs, and
     * the cap is the binding constraint on a mirror's throughput.** The
     * original argument was that a wider batch lets one worker take everything
     * while the rest idle, collapsing ingest to a single thread. That is true
     * and it is not a problem: the store takes ONE writer mutex for the whole
     * of `commit`, so commits never run in parallel anyway — the other workers
     * were never going to write concurrently, only queue. What a lock hold is
     * worth is the SURVIVORS it carries, `batchSize x (1 - dropRate)`, and at
     * a mirror's 98% duplicate rate a 512-event batch carries ten.
     *
     * `IngestCostBench`'s shape sweep, on identical 100k work: `8 x 1024` —
     * which this formula turns into a real batch of 512 — ran at 6,685 ev/s,
     * spending 99.1s of aggregate `lock.ingest.wait` across a 15s wall to
     * perform 0.2s of writing. `2 x 8192` ran the same work at 60,492 ev/s.
     * Nine times, from shape alone, and `1 x 16384` matched it — concurrency
     * past one or two buys nothing once batches are wide, exactly as a
     * serializing mutex predicts.
     *
     * **Left as it is on purpose.** Widening is not free in the other
     * direction: every other writer — the monitor's verdict edits, the healer,
     * the sweep — queues on that same mutex, and
     * `RelayVerdictRecord.EDIT_DEADLINE_MS` exists because they already wait
     * behind "~10s per 20k-event batch, several deep under load". A wider
     * batch makes ingest faster and that tail longer, and nothing has measured
     * the tail. The operator lever needs no code
     * (`SYNC_INGEST_CONCURRENCY=2 SYNC_INGEST_BATCH=8192`), and `start` says so
     * when the cap bites.
     */
    private val batchSize = tuning.batch.coerceAtMost((capacity / workers).coerceAtLeast(1))

    private val inbound = Channel<Inbound>(capacity)

    /**
     * Threads the ingest workers own outright, which no producer can occupy.
     *
     * This was the FIRST attempt at the deadlock [submit] describes, and on its
     * own it does not hold: the loop body starts here, then calls the store,
     * which reaches for `Dispatchers.IO` — the same shared pool the producers
     * were parked on. The drain therefore still queued behind them and the
     * process still stopped. [submit] suspending is what actually fixes it.
     *
     * Kept because it is still worth having: batch work stays off the shared
     * pool, so ingest and the download fan-out do not compete for the same
     * threads under normal load.
     */
    private val pool =
        Executors
            .newFixedThreadPool(workers) { r ->
                Thread(r, "vespa-relay-ingest").apply { isDaemon = true }
            }.asCoroutineDispatcher()

    /**
     * How full [inbound] is. Channel does not expose its depth, and this one
     * number decides whether the pipeline is starved or backpressured — the
     * question every stall comes down to.
     */
    val queued = AtomicInteger()

    val accepted = AtomicLong()
    val rejected = AtomicLong()

    /**
     * Events HANDED TO THE QUEUE since boot — the arrival side of it, where
     * [accepted] + [rejected] is the drain side.
     *
     * The pair is what the health line could not say. Staging sat with the
     * queue pinned at 16,400 of 16,400, `bottleneck: ingest`, and `0 ev/s`, and
     * that rate is the DRAIN: every reading of it is consistent with a store
     * that has stopped answering AND with producers that have stopped
     * producing, because it counts what came OUT of a batch. Whether anything
     * is still arriving — whether the downloads are backpressured or merely
     * quiet — was the question, and nothing counted at the entrance.
     *
     * Counted once the send has returned, so this and [queued] agree: an event
     * is in here exactly when it went into the channel, and a submit that lost
     * the race with shutdown is in neither. A [suppressed] event never reaches
     * the channel and is not here either — it has its own line, and folding it
     * in would make an arrival rate that a suppression storm holds up while the
     * queue sits empty.
     */
    val submitted = AtomicLong()

    /**
     * Good events the store refused for structural reasons, which nothing
     * will re-offer. Distinct from [rejected], most of which is the protocol
     * working (duplicates, invalid signatures). A schema drift once lost 2.3M
     * events this way while every status line read healthy — surfaced on the
     * health line so it cannot accumulate quietly again.
     */
    val lostToStore = AtomicLong()

    // Bad signatures, separated because on a wide fan-out "already have it"
    // routinely dwarfs accepts and reads like an emergency when it is the
    // system working — while a bad signature means an upstream serves junk.
    private val badSignatures = AtomicLong()

    /** Rejections by reason. Only ever written through [noteRejection] — see there for why the ceiling matters. */
    private val rejectReasons = ConcurrentHashMap<String, Long>()

    /**
     * Events dropped before the store because a filter says we have twice
     * refused them already. Counted on their OWN line, never folded into
     * accepted or rejected: a suppression is neither, and a number that hides
     * inside either one cannot answer "is the filter doing anything" or the far
     * more urgent "is the filter eating everything".
     */
    val suppressed = AtomicLong()

    /**
     * Refusals broken out by class, because the aggregate cannot answer the one
     * question this subsystem exists for. `REPLACED` is the loop; `duplicate`
     * is the system working.
     */
    val replacedRejects = AtomicLong()

    // Store failures already reported in full, so the raw-event dump stays
    // one per distinct defect.
    private val poisonSeen = ConcurrentHashMap.newKeySet<String>()

    /**
     * Measured break-even for the id probe: a dropped duplicate saves ~33-44µs
     * (the whole 49-56µs a duplicate costs, less the few it costs to drop it)
     * and the probe costs 11-23µs per id it covers — so it stops paying below
     * roughly a third. See `IngestCostBench`.
     */
    private val idGate = ProbeGate(minHitRate = 0.35)

    /**
     * And for the version probe: 29µs/event against the 123 a dropped stale
     * replaceable saves (158 to ~35), so break-even is ~20%.
     */
    private val versionGate = ProbeGate(minHitRate = 0.20)

    /**
     * When each worker entered its current batch pass, or 0 while it is waiting
     * on the channel — the instrument [inBatch] and [oldestBatchMs] read.
     *
     * A suspended coroutine has no frame, so a worker parked inside a store
     * round trip is invisible in a thread dump: its pool thread is back in
     * `LinkedBlockingQueue.take` looking idle, which reads as "the workers are
     * starving" at the exact moment they are all stuck. That contradiction — a
     * queue reported FULL with every ingest thread apparently idle — is what
     * #167 had to be diagnosed around, and these two numbers are what settles
     * it from outside the process.
     */
    private val busySince = AtomicLongArray(workers)

    /** Worker loops still running. Anything below [workers] means one exited and nothing drains its share. */
    private val loopsRunning = AtomicInteger()

    fun start() {
        // Announced when the batch is capped: an operator who set
        // SYNC_INGEST_BATCH and silently got a different number would be
        // tuning a knob that is not connected.
        //
        // This line used to end "…collapses ingest to a single thread", offered
        // as the reason the cap is a good thing. THAT IS BACKWARDS on a mirror,
        // and `IngestCostBench`'s shape sweep is what settled it: the store
        // takes one writer mutex for the whole of `commit`, so commits never
        // run in parallel and extra workers only queue for it. What a lock hold
        // is worth is the SURVIVORS it writes — `batchSize x (1 - dropRate)` —
        // and at a mirror's 98% duplicate rate a 512-event batch carries ten.
        // Measured on identical 100k work: 8x1024 (a real batch of 512) ran at
        // 6,685 ev/s with 99.1s of aggregate `lock.ingest.wait` to perform 0.2s
        // of writing, against 60,492 ev/s at 2x8192. Nine times, from shape.
        // So the cap is a COST, and the line now says so rather than
        // congratulating itself.
        if (batchSize < configuredBatch) {
            System.err.println(
                "router: SYNC_INGEST_BATCH=$configuredBatch capped to $batchSize — " +
                    "$workers worker(s) share a $capacity-event queue. Every commit serializes on the store's " +
                    "one writer mutex, so a narrow batch buys nothing back: it writes fewer surviving events per " +
                    "lock hold and takes the lock more often. Fewer, WIDER workers ingest faster on a mirror " +
                    "(measured 9x) — at the cost of a longer lock hold for every other writer",
            )
        }
        // The dedup probe needs a wide batch to be worth its round trip, so
        // below that width every copy of an event is signature-checked before
        // the store drops it. Silent, and the operator who lowered the batch to
        // cut memory is the one who most needs to know they bought that.
        if (knownIds != null && batchSize < PROBE_MIN_VERIFIABLE) {
            System.err.println(
                "router: ingest batch $batchSize is under the $PROBE_MIN_VERIFIABLE-event width the dedup " +
                    "probe needs — duplicates will be verified before the store rejects them",
            )
        }
        repeat(workers) { worker -> scope.launch(pool) { loop(worker) } }
    }

    /**
     * Hand an event to the pool, SUSPENDING the caller if the buffer is full.
     *
     * Suspending rather than blocking is the whole point. This used to call
     * `trySendBlocking`, and quartz's subscription callbacks were not
     * suspending, so backpressure had to park a thread. But those callbacks
     * run on the shared coroutine pool, and so does the store the drain must
     * reach — so a parked producer was holding a thread the drain needed.
     * Measured twice, ~13 minutes after each start: all 64 shared workers
     * parked in `runBlocking` under `trySendBlocking`, the drain unable to get
     * a thread, and the entire process silent — every stream, the health line,
     * all of it — at 2% CPU with Vespa idle and healthy. A full queue was the
     * symptom; producers eating the drain's threads was the cause.
     *
     * `send` releases the thread instead of holding it, so the drain always
     * runs and backpressure still reaches the download. Nothing is dropped.
     */
    suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        origin: IngestOrigin = IngestOrigin.Local,
    ) {
        // Checked HERE rather than in the callers' `onEvent`, and the placement
        // is deliberate: by the time an event reaches this method its caller
        // has already run `SyncCoverage.observe` and widened the leg's seen
        // span. Dropping any earlier would leave the leg without per-kind
        // evidence, quartz would record no band, and the stream would re-walk
        // that relay every cycle — costing far more than the drop saves.
        if (refusals.isSuppressed(event)) {
            suppressed.incrementAndGet()
            return
        }
        // Counted BEFORE the send, and taken back if the send fails. The
        // event is in the channel the instant `send` returns, so a worker can
        // take it and decrement before a post-send increment ever runs — which
        // drove the depth NEGATIVE (`ingest queue -1/4096` on the health line).
        // Harmless to ingest, but that depth is the number every stall
        // diagnosis in this repo starts from, and a wrong one sends the next
        // reader the wrong way.
        queued.incrementAndGet()
        var handedOff = false
        try {
            inbound.send(Inbound(event, skipVerify, origin))
            handedOff = true
            submitted.incrementAndGet()
        } catch (_: ClosedSendChannelException) {
            // Shutdown (closeIntake) raced this event in. Not an error.
        } finally {
            // In a finally, not just the catch: `send` also throws
            // CancellationException on shutdown, and a catch that named only
            // the closed-channel case would leak the count on every event in
            // flight when the router stops.
            if (!handedOff) queued.decrementAndGet()
        }
    }

    /**
     * One worker: take a batch off the channel, run it through the store, and
     * go back for the next one — for as long as the scope lives.
     *
     * The loop's exit is reported rather than silent. The scope carries a
     * `SupervisorJob`, so a throw out of here kills THIS worker and leaves the
     * others running: ingest quietly loses an eighth of its throughput, and
     * with every worker gone the queue fills, backpressures the downloads and
     * reports itself full while nothing is draining it. [loopsRunning] is what
     * says which of those happened.
     */
    private suspend fun loop(worker: Int) {
        loopsRunning.incrementAndGet()
        try {
            drain(worker)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            System.err.println(
                "router: ingest worker $worker STOPPED on ${e.javaClass.simpleName}: ${e.message} — " +
                    "${loopsRunning.get() - 1} of $workers worker(s) left to drain the queue",
            )
            throw e
        } finally {
            loopsRunning.decrementAndGet()
        }
    }

    private suspend fun drain(worker: Int) {
        val batch = ArrayList<Inbound>(batchSize)
        while (scope.isActive) {
            // Clients first: a batch's dedup and projection queries land in
            // the same engine a REQ does, and the only lever is to stop
            // adding to the queue. Zero while reads are healthy.
            servingPressure?.backoffMs()?.takeIf { it > 0 }?.let { delay(it) }
            val first = inbound.receiveCatching().getOrNull() ?: break
            queued.decrementAndGet()
            batch.clear()
            batch.add(first)
            while (batch.size < batchSize) {
                val next = inbound.tryReceive().getOrNull() ?: break
                queued.decrementAndGet()
                batch.add(next)
            }
            // Marked around the WHOLE pass, and in a finally so a throw
            // clears it: this is the only record that a worker is inside a
            // store round trip rather than waiting on the channel, and a
            // worker that never comes out is what [wedged] reports.
            busySince.set(worker, System.currentTimeMillis())
            try {
                // BEFORE verify, which is the whole point — see [dropDuplicates]
                // and [dropSuperseded].
                val fresh = dropSuperseded(dropDuplicates(batch))
                if (fresh.isEmpty()) continue
                val valid = ArrayList<Event>(fresh.size)
                // Only built when something will read it. The sink is inert unless
                // SYNC_REFUSED_DIR is set, and the pipeline is shared by every
                // stream, so an unconditional map made every existing deployment
                // allocate and hash one entry per event for a lookup that never
                // happens.
                val origins = if (refusals.tracksOrigins) HashMap<String, IngestOrigin>(fresh.size) else null
                var verifyRejected = 0
                // Booked as a stage so it lands on the same `router: ingest stages`
                // line as the store's own dedup/guards/write. It was invisible
                // there for as long as it existed, which made "is verification the
                // limit?" a question no instrument in this repo could answer.
                IngestStats.timed("verify") {
                    for (msg in fresh) {
                        if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                            valid.add(msg.event)
                            // A bad signature never reaches this map, and so can
                            // never reach the refusal sink: an id is the hash of
                            // the CONTENT, not of the signature, so the same id can
                            // arrive correctly signed from another relay.
                            // Remembering it would make one relay's corruption
                            // permanent.
                            origins?.put(msg.event.id, msg.origin)
                        } else {
                            verifyRejected++
                        }
                    }
                }
                if (verifyRejected > 0) {
                    rejected.addAndGet(verifyRejected.toLong())
                    badSignatures.addAndGet(verifyRejected.toLong())
                }
                if (valid.isEmpty()) continue
                // Before the batch write: the store feeds Vespa in parallel, so a
                // parse report raised inside batchInsert cannot be attributed to
                // one event. Inspecting here keeps the audit's ThreadLocal exact.
                audit?.let { for (event in valid) it.inspect(event) }
                insertIsolating(valid, origins ?: emptyMap())
            } finally {
                busySince.set(worker, 0)
            }
        }
    }

    /**
     * The batch minus everything that cannot be written because we already hold
     * it — dropped BEFORE the signature check, which is the entire reason this
     * exists. A schnorr verify costs ~48µs/event isolated (quartz over JNI
     * secp256k1; the id re-hash is 1.5µs of it and event size barely moves it)
     * and **~70-95µs in situ**, because the router shares its cores with the
     * engine it is feeding. On a duplicate every one of those microseconds buys
     * nothing: the event is already stored, and it was verified when it first
     * landed. Verification used to run over the whole batch, so a mirror paid it
     * per COPY — a popular event held by 40 discovered relays was verified 40
     * times to be stored once.
     *
     * Measured end to end against a real Vespa (`IngestCostBench`, 4 cores
     * shared with the engine, 72k-doc corpus, 20k-event batches): a batch of
     * duplicates went **56µs/event to 21µs/event, and 49 to 16 on the
     * interleaved repeat — 2.7-3.1x**, with the `verify` stage disappearing
     * from the ingest stage line entirely.
     *
     * Two passes, cheapest first:
     *
     *  - **in batch**, by id, no I/O. This is the fan-out case: the same event
     *    arrives from every relay carrying it, usually inside one batch.
     *    Ephemeral kinds are exempt — the store counts a repeat of one as
     *    accepted-not-stored rather than as a duplicate, and this must not
     *    quietly move a number the health line prints.
     *  - **in the store**, via [knownIds]. Same existence check the store's own
     *    stage B runs, so it costs one extra round trip — 11-23µs per id at
     *    full batch width, against the 70-95µs a verification costs and the
     *    ~600µs/event a fresh batch spends being written. Gated on
     *    [PROBE_MIN_VERIFIABLE] because that trade only holds at width: on a
     *    small live-tail batch the round trip can cost more than the
     *    verifications it saves, and it adds dedup load to the engine the
     *    relay is serving reads from.
     *
     * **Why this is safe.** An event dropped here is never stored, so its
     * signature is a fact about a document nobody will read. The id it is
     * matched on is the CLAIMED id, unverified at this point — a forged event
     * naming an id we hold is dropped without being checked, which is the same
     * outcome verifying it would have produced. Nothing unverified reaches
     * [IEventStore.batchInsert]: everything that survives this is verified in
     * full, id hash included, so a lying id cannot smuggle a document in under
     * some other id.
     *
     * What it costs in exchange: an upstream serving junk that happens to
     * collide with our corpus no longer shows up as `bad signature` on the
     * stats line. Junk naming events we already have is the one flavour of it
     * this relay was never going to store anyway.
     */
    private suspend fun dropDuplicates(batch: List<Inbound>): List<Inbound> {
        val ids = HashSet<String>(batch.size)
        val once = ArrayList<Inbound>(batch.size)
        var dropped = 0
        for (msg in batch) {
            if (msg.event.kind.isEphemeral() || ids.add(msg.event.id)) once.add(msg) else dropped++
        }

        val probe = knownIds
        // The count that justifies the round trip is what it would save, and it
        // saves verifications — a batch of trusted events skips those already.
        val verifiable = once.count { !it.skipVerify }
        val probed = probe != null && verifiable >= PROBE_MIN_VERIFIABLE && idGate.worthIt()
        val stored =
            if (!probed) {
                emptySet()
            } else {
                try {
                    // Booked as ONE store call round the whole probe rather than
                    // per chunk, and the boundary is the point: what an operator
                    // needs from `oldestBatchSec` at 794 is the call this worker
                    // is suspended in, and the worker is suspended here — inside
                    // a fan-out the store owns — not in any one chunk. The row
                    // says `2,048 ids` because that is what this pass is waiting
                    // on. See [StoreCalls].
                    storeCall(StoreCalls.CALLER_INGEST_DEDUP, StoreCalls.OP_EXISTING_IDS, StoreCalls.ids(once.size)) {
                        IngestStats.timed("dedup.pre") {
                            once
                                .map { it.event.id }
                                .chunked(DEDUP_CHUNK)
                                .mapBounded(QUERY_FANOUT) { probe!!(it) }
                                .flatMapTo(HashSet()) { it }
                        }
                    }
                } catch (e: CancellationException) {
                    // NOT runCatching: it swallows this too, and shutdown
                    // reaches the probe as a cancellation. Swallowed, the batch
                    // would go on to verify and WRITE into a store the process
                    // is closing. Same rethrow-first shape as insertBisecting.
                    throw e
                } catch (_: Throwable) {
                    // A failed probe must cost time, never correctness: fall
                    // through knowing nothing and let the store's stage B
                    // decide, exactly as it did before this existed.
                    emptySet()
                }
            }

        val fresh = if (stored.isEmpty()) once else once.filter { it.event.id !in stored }
        // Recorded whenever the probe RAN, including when it found nothing —
        // a round trip that drops nothing is precisely what the gate has to
        // learn from, and only the probe's own verdict teaches it (the
        // in-batch pass is free and would flatter a query it says nothing
        // about).
        if (probed) idGate.record(once.size, once.size - fresh.size)
        dropped += once.size - fresh.size
        if (dropped > 0) {
            rejected.addAndGet(dropped.toLong())
            // The store's own word for it, verbatim, so dropping a duplicate
            // here and dropping it there are ONE line on the stats breakdown
            // rather than two that have to be added up.
            noteRejection(RejectionReason.DUPLICATE.take(48), dropped.toLong())
        }
        return fresh
    }

    /**
     * Write a batch through the store's bulk path; if it throws, bisect and
     * isolate the offending event so one bad event does not silently cost a
     * whole batch. See [insertBisecting].
     */
    private suspend fun insertIsolating(
        events: List<Event>,
        origins: Map<String, IngestOrigin>,
    ) = insertBisecting(
        events = events,
        // THE WRITE, named apart from the two probes above it. All three are
        // one `oldestBatchSec`, and they fail for unrelated reasons: a probe
        // waits on the query path, this waits on the single writer mutex and
        // the feed behind it. Booked per bisection attempt, so an isolation
        // pass reports the batch it is actually writing rather than the one it
        // started with.
        write = { batch -> storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(batch.size)) { store.batchInsert(batch) } },
        onOutcomes = { written, outcomes ->
            // Positional alignment between the batch and its outcomes is the
            // store's contract, and this is the one place where being wrong
            // about it is unrecoverable: a rejection attributed to the wrong
            // row suppresses an id we wanted, silently and permanently. The
            // counters below would survive a mismatch; the refusal sink would
            // not. So it is checked rather than trusted, and attribution — and
            // only attribution — is withheld when it fails.
            val aligned = outcomes.size == written.size
            if (!aligned) reportMisalignment(written.size, outcomes.size)
            for ((i, outcome) in outcomes.withIndex()) {
                when (outcome) {
                    is IEventStore.InsertOutcome.Accepted -> {
                        accepted.incrementAndGet()
                    }

                    is IEventStore.InsertOutcome.Rejected -> {
                        rejected.incrementAndGet()
                        noteRejection(outcome.reason.take(48), 1L)
                        // Attributed to the event that earned it. Only the
                        // REJECTED branch reports: a Failed outcome is the
                        // STORE's fault and the event is good, so recording
                        // it would turn a transient fault into permanent
                        // silent loss.
                        if (aligned) {
                            written.getOrNull(i)?.let { event ->
                                reportRefusal(event, origins[event.id] ?: IngestOrigin.Local, outcome.reason)
                            }
                        }
                    }

                    is IEventStore.InsertOutcome.Failed -> {
                        // The store's fault, attributed per row: the event
                        // was good and is lost — nothing re-offers it.
                        // Tallied like onGaveUp's batch case, plus
                        // lostToStore so the loss is loud on the health
                        // line instead of blending into the duplicates.
                        rejected.incrementAndGet()
                        noteRejection("store failed: ${outcome.reason.take(40)}", 1L)
                        lostToStore.incrementAndGet()
                    }
                }
            }
        },
        onPoison = { event, e ->
            rejected.incrementAndGet()
            noteRejection("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L)
            reportPoison(event, e)
        },
        onGaveUp = { batch, e ->
            // Isolation ran out of budget: counted but unnamed, and
            // tallied apart from the isolated ones — "we could not say
            // which" is a different fact from "this event is bad".
            rejected.addAndGet(batch.size.toLong())
            noteRejection("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong())
            // These are LOST, not merely rejected: the events were good,
            // the failure is the store's, and nothing re-offers them.
            lostToStore.addAndGet(batch.size.toLong())
        },
    )

    /**
     * One permanent refusal, reported to the filter and the healer.
     *
     * Shared by the store's own verdict above and by [dropSuperseded], which
     * is the part that matters: the fast path drops a superseded replaceable
     * BEFORE the store ever sees it, so the `replaced:` rejection this
     * subsystem is built on stops being produced there. Without this call at
     * both sites the suppression filter learns nothing and the healer never
     * discovers a stale relay — the feature would still be configured, still
     * report zero, and still be doing nothing.
     */
    private fun reportRefusal(
        event: Event,
        origin: IngestOrigin,
        reason: String,
    ) {
        if (reason.startsWith(RejectionReason.PREFIX_REPLACED)) replacedRejects.incrementAndGet()
        refusals.onRefused(event, origin, reason)
    }

    /**
     * The batch minus every replaceable event a NEWER version already beats —
     * dropped, like a duplicate, before it can be verified.
     *
     * This is the arrival the id probe structurally cannot see. A newer
     * generation of a profile is a DIFFERENT id (different `created_at`,
     * different hash), so [dropDuplicates] calls it new, it pays full
     * verification, and only then does the store reject it as `replaced`.
     * Measured against a real Vespa (`IngestCostBench`): **158µs/event**, of
     * which `versions` is 38% and `verify` 32%.
     *
     * It is not a rare shape. Under the outbox model different relays hold
     * different generations of the same address — relay B never received the
     * generation the author published to relay A — so a fan-out is offered
     * stale versions permanently, and negentropy cannot converge them away:
     * it reconciles ids, and a replaceable's identity is its ADDRESS. One
     * production backfill runs at 94% replaced-or-duplicate.
     *
     * Two passes again, and only the second costs anything:
     *
     *  - **in batch**, by address. Saves only the losers' VERIFICATION — the
     *    store's stage D already collapses an in-run group to one write, so
     *    there is no write here to save.
     *  - **in the store**, via [newestVersions]: 29µs/event for the batched
     *    read, against the 158 a stale arrival costs today. It pays for itself
     *    once ~20% of replaceable arrivals are stale, which [versionGate]
     *    measures rather than assumes.
     *
     * **Plain replaceable kinds only** (0, 3, 10002, 10040 — not 30382 and the
     * other addressables). Their version query is one `(kind, authors…)` per
     * chunk. An addressable's is not: the store recalls it per (kind, author)
     * with the d-set, never across authors, because a (authors × d-tags) query
     * is a cross product that is unbounded on the ingest path and silently
     * truncated where hits are capped — and a truncated answer here is a
     * DROPPED event, not a slow one. Reproducing that shape in the router is
     * the fork AGENTS.md warns about; addressables stay the store's business.
     *
     * **Why dropping is safe, and the one place it is not.** A superseded
     * event is not stored either way, so its signature decides nothing. The
     * comparison is NIP-01's own — newest `created_at`, ties to the lower id —
     * copied from the rule stage D applies. Where this differs from letting
     * the store decide: the store re-reads under the writer lock, and this
     * reads before it. If a NIP-09 deletion removes the newer version in that
     * window, the store would have accepted the older event and this drops it.
     * The event is re-offered on the next full resync, and the window is one
     * batch wide.
     */
    private suspend fun dropSuperseded(batch: List<Inbound>): List<Inbound> {
        // NOT for a batch carrying a deletion or a vanish. Those take the
        // store's replay path, where an event's fate depends on its POSITION
        // among the others: `[v1, delete(v2), v2]` stores v1, because v2 lands
        // on its own tombstone. Choosing v2 here and dropping v1 would leave
        // that address empty. [dropDuplicates] needs no such guard — it only
        // ever drops an event identical to one already held, and a deletion
        // reaching either copy reaches both — but this one CHOOSES between
        // distinct events, and the replay is entitled to disagree. Keyed on
        // the KIND, not the type: an Event that never went through quartz's
        // factory is a plain Event whatever its kind, and `is DeletionEvent`
        // would wave it through.
        if (batch.any { it.event.kind == DeletionEvent.KIND || it.event.kind == RequestToVanishEvent.KIND }) return batch

        // Winner per address, by first appearance so the batch keeps its order.
        val keys = arrayOfNulls<Pair<Int, String>>(batch.size)
        val winners = LinkedHashMap<Pair<Int, String>, Int>()
        var candidates = 0
        batch.forEachIndexed { i, msg ->
            val e = msg.event
            if (!e.kind.isReplaceable() || e.kind.isAddressable()) return@forEachIndexed
            candidates++
            // Held rather than rebuilt on the second pass: one Pair per
            // candidate, not two, on a path that runs per batch forever.
            val key = e.kind to e.pubKey
            keys[i] = key
            val held = winners[key]
            if (held == null || beats(e, batch[held].event)) winners[key] = i
        }
        if (candidates == 0) return batch

        val drop = BooleanArray(batch.size)
        keys.forEachIndexed { i, key -> if (key != null && winners[key] != i) drop[i] = true }
        var dropped = candidates - winners.size

        val probe = newestVersions
        if (probe != null && winners.size >= PROBE_MIN_VERIFIABLE && versionGate.worthIt()) {
            val stored = readNewestVersions(probe, winners.keys)
            var beaten = 0
            for ((key, i) in winners) {
                val held = stored[key] ?: continue
                // Strictly beaten only: an equal stamp is the same event, and
                // the id probe already had its chance at that.
                if (held.createdAt > batch[i].event.createdAt ||
                    (held.createdAt == batch[i].event.createdAt && held.id < batch[i].event.id)
                ) {
                    drop[i] = true
                    beaten++
                }
            }
            versionGate.record(winners.size, beaten)
            dropped += beaten
        }

        if (dropped == 0) return batch
        noteRejection(RejectionReason.REPLACED.take(48), dropped.toLong())
        rejected.addAndGet(dropped.toLong())
        // The store never sees these, so the `replaced:` verdict it would have
        // returned has to be reported from here instead.
        //
        // This is load-bearing rather than tidy. The refused-id filter and the
        // healer are both fed by exactly one signal — a store refusal — and
        // this fast path exists precisely to stop the store producing it for
        // the commonest case. Report only at `insertIsolating` and the two
        // structures go quiet in proportion to how well this optimisation
        // works: at the 94%-replaced backfill quoted above, almost nothing
        // would ever reach the gate. Configured, reporting zero, doing
        // nothing.
        //
        // Both drop classes belong here. An in-batch loser is not necessarily
        // this relay's fault — a batch is shared by every stream, so the
        // winner may have come from a different relay entirely — and a
        // store-beaten arrival is the loop itself.
        //
        // SAFETY: the id is checked, the signature is not, and the asymmetry
        // is the point. Nothing here has been verified yet, so an unchecked id
        // is ATTACKER-CHOSEN: forge a kind 0 for any pubkey with an old
        // `created_at`, stamp it with the id of an event you want this relay
        // never to fetch, and two of those suppress that id permanently.
        // `verifyId` closes it for ~1.5us against the ~48-95us a signature
        // costs, because it binds the id to the content that earned the
        // verdict; naming someone else's id would take a preimage.
        //
        // The signature genuinely is not needed for THIS class. Supersession
        // is decided by (kind, pubkey, created_at), all of them inside the
        // hashed content, so a correctly-signed twin carrying the same id is
        // superseded identically and suppressing it costs nothing. That is
        // what makes this narrower check sufficient here and nowhere else —
        // see `PermanentRefusals`, where "a bad signature must never become
        // permanent" is about ids whose storability differs between copies.
        for (i in batch.indices) {
            if (!drop[i]) continue
            val msg = batch[i]
            if (msg.event.verifyId()) reportRefusal(msg.event, msg.origin, RejectionReason.REPLACED)
        }
        return batch.filterIndexed { i, _ -> !drop[i] }
    }

    /** NIP-01 newest-wins, tie to the lower id — the rule the store's stage D resolves by. */
    private fun beats(
        candidate: Event,
        incumbent: Event,
    ): Boolean =
        candidate.createdAt > incumbent.createdAt ||
            (candidate.createdAt == incumbent.createdAt && candidate.id < incumbent.id)

    /** One query per kind per chunk of authors, same width and fan-out the store's version stage uses. */
    private suspend fun readNewestVersions(
        probe: suspend (Int, List<String>) -> Map<String, AddressVersion>,
        addresses: Set<Pair<Int, String>>,
    ): Map<Pair<Int, String>, AddressVersion> =
        try {
            // One call round the whole fan-out, for [dropDuplicates]'s reason —
            // the summary is the ask this pass is waiting on, not one chunk of it.
            storeCall(
                StoreCalls.CALLER_INGEST_VERSIONS,
                StoreCalls.OP_NEWEST_VERSIONS,
                "${addresses.map { it.first }.distinct().size} kind(s), ${addresses.size} address(es)",
            ) {
                IngestStats.timed("versions.pre") {
                    addresses
                        .groupBy({ it.first }, { it.second })
                        .flatMap { (kind, authors) -> authors.chunked(CHECK_CHUNK).map { kind to it } }
                        .mapBounded(QUERY_FANOUT) { (kind, authors) -> probe(kind, authors).mapKeys { (author, _) -> kind to author } }
                        .fold(HashMap()) { all, part -> all.apply { putAll(part) } }
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Throwable) {
            // Same rule as the id probe: a failed optimisation costs time, not
            // correctness. The store still resolves supersession itself.
            emptyMap()
        }

    /**
     * Tally [count] rejections under [reason], keeping at most
     * [REASON_LIMIT] distinct reasons.
     *
     * The store's own reasons are a fixed vocabulary on purpose (`Rejections`
     * builds one constant string rather than one per field, so a tally cannot
     * fragment). Its *throws* are not: they embed per-event content — a Vespa
     * 400 quotes the document — so a store failing on every event mints a new
     * key here per event. That is the same run [poisonSeen] is capped for, and
     * this map was left uncapped two fields away from that guard: 2.3M distinct
     * failures would have been 2.3M retained strings, during the one incident
     * where heap is already the thing to protect.
     *
     * Past the ceiling everything folds into one bucket, which costs nothing
     * real — [rejectionBreakdown] prints the top two.
     */
    private fun noteRejection(
        reason: String,
        count: Long,
    ) {
        // Racy by a worker or two at the boundary: the point is a bound, not an
        // exact size, and each worker can add at most one key past it.
        if (rejectReasons.size >= REASON_LIMIT && !rejectReasons.containsKey(reason)) {
            rejectReasons.merge(OVERFLOW_REASON, count, Long::plus)
        } else {
            rejectReasons.merge(reason, count, Long::plus)
        }
    }

    /**
     * Log an event the store threw on, once per distinct failure, with the
     * raw JSON — the store-level throw has no other trace, and without the
     * raw event the defect cannot be reproduced.
     */
    private fun reportPoison(
        event: Event,
        error: Throwable,
    ) {
        // Size checked BEFORE add: store errors embed per-event content in
        // their messages (a Vespa 400 quotes the document), so past the print
        // limit the set must stop growing too — 2.3M distinct rejections in
        // one schema-drift run would otherwise be 2.3M retained strings.
        if (poisonSeen.size >= POISON_SAMPLE_LIMIT) return
        val signature = "${error.javaClass.name}: ${error.message}"
        if (!poisonSeen.add(signature)) return
        System.err.println(
            "router: store rejected event ${event.id} (kind ${event.kind}, pubkey ${event.pubKey}) — " +
                "${error.javaClass.simpleName}: ${error.message}\n" +
                "router: the event, verbatim: ${event.toJson().take(POISON_JSON_CHARS)}",
        )
    }

    /**
     * The store returned a different number of outcomes than it was handed.
     *
     * Reported once and loudly: it means the assumption every per-event
     * attribution rests on has broken, so refusals stop being recorded until
     * it is fixed. Better a filter that learns nothing than one that learns
     * the wrong ids.
     */
    private fun reportMisalignment(
        sent: Int,
        got: Int,
    ) {
        if (misalignmentReported.compareAndSet(false, true)) {
            System.err.println(
                "router: BUG — batchInsert returned $got outcome(s) for $sent event(s). Rejections cannot be " +
                    "attributed to the events that earned them, so refused-id recording is suppressed for " +
                    "these batches. Counters remain correct.",
            )
        }
    }

    private val misalignmentReported = AtomicBoolean(false)

    /** The two numbers Step 0 exists to produce, for the health line. */
    fun suppressionBreakdown(): String =
        if (suppressed.get() == 0L && replacedRejects.get() == 0L) {
            ""
        } else {
            " [replaced x${replacedRejects.get()}; suppressed x${suppressed.get()}]"
        }

    /**
     * What each drop-probe is currently doing, for the stats line. Without it
     * a gated-off probe is invisible: ingest slows, the `dedup.pre` and
     * `versions.pre` stages simply stop appearing, and nothing says whether
     * that is a converged stream (working as designed) or a probe that was
     * never wired. Empty until a gate has judged anything.
     */
    fun probeStatus(): String {
        val parts =
            listOfNotNull(
                describeGate("id", idGate, knownIds != null),
                describeGate("version", versionGate, newestVersions != null),
            )
        return if (parts.isEmpty()) "" else "router: ingest probes ${parts.joinToString(", ")}"
    }

    private fun describeGate(
        name: String,
        gate: ProbeGate,
        wired: Boolean,
    ): String? {
        if (!wired) return "$name off (not wired)"
        val rate = gate.hitRate()
        if (rate == 0.0 && !gate.hasJudged()) return null
        return "$name ${"%.0f".format(rate * 100)}% dropped${if (gate.paying()) "" else ", sampling only"}"
    }

    /**
     * WORKERS INSIDE A BATCH PASS RIGHT NOW, and how long the oldest has been
     * there.
     *
     * The pair that tells a backpressured pipeline from a wedged one, which
     * every other number this class publishes reports identically. A full queue
     * with every worker in a pass that started seconds ago is ingest keeping up
     * badly; a full queue with every worker in a pass minutes old is ingest not
     * running at all, and the events counted as `queued` will never reach a
     * worker. Without these the only evidence was a thread dump, where a
     * suspended worker has no frame and its pool thread looks idle — see
     * [busySince].
     */
    fun inBatch(): Int = (0 until workers).count { busySince.get(it) != 0L }

    fun oldestBatchMs(): Long {
        val now = System.currentTimeMillis()
        var oldest = 0L
        for (i in 0 until workers) {
            busySince.get(i).takeIf { it != 0L }?.let { oldest = maxOf(oldest, now - it) }
        }
        return oldest
    }

    /** How many workers were configured, so [inBatch] has a denominator outside this class. */
    val workerCount: Int get() = workers

    /** …and how many of them are still looping. Below [workerCount] means a worker exited — see [loop]. */
    fun workersRunning(): Int = loopsRunning.get()

    /**
     * Whether ingest has STOPPED: no worker is waiting on the channel, and none
     * has started a batch within [wedgeAfterMs]. That is the definition of
     * nothing draining, stated directly.
     *
     * **It deliberately says nothing about the queue depth.** This asked for
     * the queue to be at its ceiling as well, which is how the wedge PRESENTED
     * in #167 — but presentation is not definition. A wedge behind a slow
     * upstream holds every worker with the queue only part full, and the depth
     * test sent that case to `mixed`, which the card renders as "keeping up —
     * nothing here is the constraint" for a pipeline writing nothing. Dropping
     * the depth also removes a second reading of `queued` from a caller that
     * had already read it once, which is exactly the drift `SyncEngine`'s
     * health loop comments say it fixed.
     *
     * The false positive it has to avoid is an honestly slow batch, and the
     * margin is wide: the widest batch this can take is `MAX_INBOUND_QUEUE`,
     * and the slowest measured write is ~2,400 ev/s (`IngestCostBench`'s
     * all-fresh burst), so a real batch lands in about seven seconds against a
     * two-minute threshold. Requiring EVERY worker to be held, and none of them
     * recently, is the other half: one worker back on the channel means ingest
     * is moving, however long a sibling has been away.
     *
     * **This REPORTS the wedge; nothing here ends it.** Deliberately so: the
     * only way to end one from this side is to cut the pass, and cutting a
     * pass discards a batch of good events that nothing re-offers before the
     * next full resync. A router that quietly bleeds twenty thousand events
     * every few minutes while the store is sick is a worse failure than one
     * that stops and says which store call it stopped in. So the worker stays
     * where it is, the health line names it, and the operator decides.
     */
    fun wedged(): Boolean {
        // An empty pool cannot be wedged, and must not read as one: the loop
        // below is vacuously true over no workers. Unreachable today —
        // `newFixedThreadPool(0)` throws first — and left because a predicate
        // that answers "stopped" for a pipeline that does not exist is the kind
        // of landmine this file's comments exist to defuse.
        if (workers == 0) return false
        val now = System.currentTimeMillis()
        for (i in 0 until workers) {
            val since = busySince.get(i)
            if (since == 0L || now - since < wedgeAfterMs) return false
        }
        return true
    }

    /**
     * WHY events were rejected, as counts rather than as the log line's prose.
     *
     * A mirror is offered the same event once per relay holding it, so rejecting
     * most of what arrives is the pipeline working — 7.9M against 524k accepted
     * on the run this came from. The total alone cannot say that; the split
     * between "already have this" and "a newer version exists" and a bad
     * signature is what makes it readable, and it existed only inside a string.
     *
     * Biggest first, bounded, and the tail is not silently dropped: the map's
     * own overflow bucket is one of the reasons it can return.
     */
    fun rejectionReasons(limit: Int = REJECTION_ROWS): List<Pair<String, Long>> =
        (
            rejectReasons.entries.map { it.key to it.value } +
                listOfNotNull(badSignatures.get().takeIf { it > 0 }?.let { "bad signature" to it })
        ).sortedByDescending { it.second }
            .take(limit)

    /**
     * What the rejections actually were, for the stats line — the bare total
     * hides whether you are looking at routine duplicates or a failing store.
     */
    fun rejectionBreakdown(): String {
        if (rejected.get() == 0L) return ""
        val why =
            rejectReasons.entries
                .sortedByDescending { it.value }
                .take(2)
                .joinToString { "${it.key} x${it.value}" }
        val bad = if (badSignatures.get() > 0) "bad signature x${badSignatures.get()}" else ""
        val parts = listOf(bad, why).filter { it.isNotEmpty() }
        return if (parts.isEmpty()) "" else " [${parts.joinToString("; ")}]"
    }

    /** Stop accepting events; parked producers are released. */
    fun closeIntake() {
        inbound.close()
    }

    /** After the scope is cancelled, so a worker mid-batch is cancelled rather than stranded. */
    override fun close() {
        runCatching { pool.close() }
    }

    companion object {
        /** How many rejection reasons the report publishes. Four covers every shape seen here. */
        const val REJECTION_ROWS = 4

        /**
         * Ceiling on queued-but-not-yet-ingested events, independent of batch
         * size. 16k events is a few hundred MB at Nostr's event sizes — far
         * short of the 80,000 that killed the process.
         *
         * **It is doing a second job it was never sized for.** [batchSize] is
         * derived from [capacity], which this bounds, so this number also caps
         * how wide a batch can ever be — 2048 at eight workers, whatever
         * `SYNC_INGEST_BATCH` says — and batch width is what decides survivors
         * per lock hold. One constant, two unrelated concerns: heap here, write
         * efficiency there. Splitting them is the change [batchSize] describes
         * and declines to make blind.
         */
        private const val MAX_INBOUND_QUEUE = 16_384

        /**
         * How many events a batch must expect to VERIFY before the dedup probe
         * is worth its round trip.
         *
         * The per-id price falls with batch width as the round trip amortises
         * — 23µs/id over 4k ids, 11µs over 20k (`IngestCostBench`) — while a
         * verification is a flat 70-95µs. So at full width the probe wins once
         * roughly a sixth of the batch is duplicate, and at a single chunk's
         * width the fixed cost of the round trip makes it about a wash. 128 is
         * where that wash sits, and it keeps small live-tail batches — whose
         * events are mostly new, and whose duplicates the in-batch pass has
         * already caught — off the engine the relay serves reads from.
         */
        private const val PROBE_MIN_VERIFIABLE = 128

        /**
         * Authors per version query. Deliberately NOT [DEDUP_CHUNK]: that is
         * stage B's width and carries `VESPA_DEDUP_CHUNK`, which widens the id
         * check alone. Widening a version query is a different trade — the
         * store keeps it at its own CHECK_CHUNK — and riding the dedup knob
         * would silently retune this one too.
         */
        private const val CHECK_CHUNK = 500

        /**
         * Ids per probe query. Read from the store's OWN knob, not a private
         * one: stage B chunks at `VESPA_DEDUP_CHUNK` (default 500), and a
         * deployment that widens it for sync speed should not have to discover
         * that the router in front of it kept probing at the old width.
         */
        private val DEDUP_CHUNK: Int = System.getenv("VESPA_DEDUP_CHUNK")?.toIntOrNull()?.coerceAtLeast(1) ?: 500

        /** Distinct rejection reasons kept before [noteRejection] folds the rest into one. */
        private const val REASON_LIMIT = 64

        /** Where reasons past [REASON_LIMIT] land — named, so the line says a tally was folded rather than implying two. */
        private const val OVERFLOW_REASON = "other store failures"

        // Distinct store failures to dump a raw event for; past a handful it
        // is a stuck loop, not news.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it, even with a long kind-0 content.
        private const val POISON_JSON_CHARS = 4_000

        /**
         * How long a batch pass has to have been running before a full queue
         * is reported as WEDGED rather than as backpressure — see [wedged].
         *
         * TEN MINUTES, and the first number was wrong. This was two minutes,
         * derived from `IngestCostBench` against a ~500k-document corpus where
         * the slowest measured throughput was ~2,400 ev/s — a 17x margin over
         * any honest batch. Staging then ran it: on a ~200M-document store,
         * with 67 concurrent visits and negentropy reads against the same
         * engine, ingest oscillates between ~11,400 ev/s and **136**. At that
         * floor, two workers on 8192-event batches take `8192 / 68` = **120
         * seconds** — the threshold exactly, on a pipeline that is merely slow.
         * `oldestBatchSec` was observed at 43 in a healthy sample.
         *
         * The bench could not have found this: dedup cost scales with the
         * index, and its corpus is ~400x smaller than production's. Ten minutes
         * restores a ~5x margin over the observed floor and still catches a
         * wedge long before anyone notices — #167's pod was stopped for hours.
         * **A false `wedged` is worse than a late one**: this word exists
         * because the router cried "keeping up" through a real outage, and it
         * would retire itself just as fast by crying wedge through a slow hour.
         *
         * So a pass still running at ten minutes is not slow, it is stopped —
         * and NOTHING here stops it. The
         * store's query client sets `readTimeout(0)`/`callTimeout(0)` on
         * purpose (an unlimited query may run as long as it takes, and it
         * cannot tell "engine still matching" from "connection dead"), so a
         * response that never comes holds this worker for the life of the
         * process. This number does not bound that; it only makes the router
         * say so, which is the whole of what #167 could not do.
         */
        const val WEDGE_AFTER_MS = 600_000L
    }
}

/** The newest stored version of one address — what an arriving replaceable has to beat. */
data class AddressVersion(
    val createdAt: Long,
    val id: String,
)

/**
 * How much of the store's throughput ingest may use — `ingestConcurrency` and
 * `ingestBatch` from `router.conf`, and nothing else.
 *
 * A type rather than two parameters so a caller cannot swap them, and so the
 * pipeline can be constructed by something that has never read a `router.conf`.
 */
data class IngestTuning(
    val concurrency: Int,
    val batch: Int,
)
