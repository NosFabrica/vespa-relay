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

import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.mapBounded
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.relay.ingest.ParseAudit
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusalSink
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.util.BootClock
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.crypto.verifyId
import com.vitorpamplona.quartz.nip01Core.crypto.verifySignature
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

/**
 * The pipeline every event this deployment stores funnels through, mirrored or published: a
 * bounded channel and a pool of workers draining it in batches through [IEventStore.batchInsert].
 * Only a verified event may shadow another, in batch or in the store. A full channel suspends
 * [submit]. As an [EventIngest] it is the relay's writer too, answering each publish's OK.
 */
class IngestPipeline(
    private val store: IEventStore,
    tuning: IngestTuning,
    private val audit: ParseAudit?,
    // Clients first: ingest yields when their reads slow down.
    private val servingPressure: ServingPressure?,
    private val scope: CoroutineScope,
    /** Which of these ids the store already holds. Null disables the probe, which is only slower, never wrong. */
    private val knownIds: (suspend (List<String>) -> Set<String>)? = null,
    /** The newest stored version of each `(kind, author)` address. Null leaves supersession to the store. */
    private val newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)? = null,
    private val refusals: RefusalSink = RefusalSink.None,
    /** How long a batch pass runs before the pipeline reads as wedged rather than backpressured. */
    private val wedgeAfterMs: Long = WEDGE_AFTER_MS,
    /** Where a batch is written: the store, or the serving relay ([RelayWriter]). The probes read [store] either way. */
    private val writer: StoreWriter = StoreWriter.local(store),
    /** Names the first event submitted and the first stored as `boot:` lines ([BootClock]); null prints neither. */
    private val bootName: String? = null,
    /** How long one drop-probe may take before the pass goes on without it; see [PROBE_TIMEOUT_MS]. */
    private val probeTimeoutMs: Long = PROBE_TIMEOUT_MS,
) : EventIngest {
    private class Inbound(
        val event: Event,
        val skipVerify: Boolean,
        val origin: IngestOrigin,
        val onOutcome: ((IEventStore.InsertOutcome) -> Unit)?,
        /** What it holds of [intakeBudget], returned once its pass is over. */
        val weight: Long,
    )

    private val workers = tuning.concurrency
    private val configuredBatch = tuning.batch
    private val dedupChunk = tuning.dedupChunk

    /** How many downloaded events may wait for ingest. */
    val capacity = tuning.queue?.coerceIn(1, MAX_INBOUND_QUEUE) ?: (tuning.batch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

    /** Parsed bytes the events taken in and not yet written may hold, when [IngestTuning.queueBytes] asks for a bound. */
    private val intakeBudget = tuning.queueBytes?.let { ByteBudget(it) }

    /** How many events one worker takes per pass, capped to its fair share of the channel. */
    private val batchSize = tuning.batch.coerceAtMost((capacity / workers).coerceAtLeast(1))

    private val inbound = Channel<Inbound>(capacity)

    /** Threads the ingest workers own outright, so batch work stays off the shared pool. */
    private val pool =
        Executors
            .newFixedThreadPool(workers) { r ->
                Thread(r, "vespa-relay-ingest").apply { isDaemon = true }
            }.asCoroutineDispatcher()

    /** How full [inbound] is; Channel does not expose its depth. */
    val queued = AtomicInteger()

    /** Producers suspended in a full queue, per relay, counted only while the send actually suspends. */
    private val parked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()

    /** How many producers are suspended in a full queue on this relay's events right now. */
    fun parkedOn(url: NormalizedRelayUrl): Int = parked[url]?.get() ?: 0

    /** At capacity: the next [submit] would suspend its caller, and with it that caller's socket. */
    fun isFull(): Boolean = queued.get() >= capacity

    /**
     * At most half full, by count AND by bytes when [IngestTuning.queueBytes] bounds them: room for
     * a resumed download's page without parking its socket again. A spent byte budget parks a
     * submit as surely as a full channel does, before it is ever counted as queued.
     */
    fun hasHeadroom(): Boolean = queued.get() <= capacity / 2 && (intakeBudget?.let { it.heldBytes <= it.limit / 2 } ?: true)

    val accepted = AtomicLong()
    val rejected = AtomicLong()

    /** Events handed to the queue since boot, the arrival side of [accepted] + [rejected]. */
    val submitted = AtomicLong()

    /**
     * Good events the store failed to write: a store error, not a refusal. Not a loss by itself in
     * the sync, where a walk or retraction audit that has one records no band ([IngestAcks]) and
     * fetches its events again; a count that keeps climbing is a store that keeps failing.
     */
    val lostToStore = AtomicLong()

    private val badSignatures = AtomicLong()

    /** Rejections by reason. Only ever written through [noteRejection], which bounds it. */
    private val rejectReasons = ConcurrentHashMap<String, Long>()

    /** Events dropped before the store because a filter says we have twice refused them. */
    val suppressed = AtomicLong()

    /** `REPLACED` refusals on their own: the loop the refused-id filter exists for. */
    val replacedRejects = AtomicLong()

    /** Store failures already reported in full. */
    private val poisonSeen = ConcurrentHashMap.newKeySet<String>()

    private val idGate = ProbeGate(minHitRate = 0.35)

    private val versionGate = ProbeGate(minHitRate = 0.20)

    /** Probes that threw. A failed probe teaches its gate nothing, so it is counted and backed off here instead. */
    private val idProbeTrouble = ProbeTrouble("id")

    private val versionProbeTrouble = ProbeTrouble("version")

    /** When each worker entered its current batch pass, or 0 while it waits on the channel. */
    private val busySince = AtomicLongArray(workers)

    /** Worker loops still running. Below [workers] means one exited and nothing drains its share. */
    private val loopsRunning = AtomicInteger()

    fun start() {
        if (batchSize < configuredBatch) {
            System.err.println(
                "router: SYNC_INGEST_BATCH=$configuredBatch capped to $batchSize — " +
                    "$workers worker(s) share a $capacity-event queue. Every commit serializes on the store's " +
                    "one writer mutex, so a narrow batch buys nothing back: it writes fewer surviving events per " +
                    "lock hold and takes the lock more often. Fewer, WIDER workers ingest faster on a mirror " +
                    "(measured 9x) — at the cost of a longer lock hold for every other writer",
            )
        }
        if (knownIds != null && batchSize < PROBE_MIN_VERIFIABLE) {
            System.err.println(
                "router: ingest batch $batchSize is under the $PROBE_MIN_VERIFIABLE-event width the dedup " +
                    "probe needs — duplicates will be verified before the store rejects them",
            )
        }
        repeat(workers) { worker -> scope.launch(pool) { loop(worker) } }
    }

    /**
     * Hand an event to the pool, suspending the caller if the buffer is full. Never blocks:
     * producers and the store share the coroutine pool.
     */
    suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        origin: IngestOrigin = IngestOrigin.Local,
        onOutcome: ((IEventStore.InsertOutcome) -> Unit)? = null,
    ) {
        // Checked after the caller's SyncCoverage.observe, so the leg keeps its per-kind evidence.
        if (refusals.isSuppressed(event)) {
            suppressed.incrementAndGet()
            onOutcome?.let { deliver(it, SUPPRESSED) }
            return
        }
        // Before it is counted as queued: a publish waiting here is the connection's backpressure,
        // like a full channel, and holds nothing of the pipeline yet. Counted as PARKED while it
        // actually waits, as a suspended send is: either way the caller's socket stops reading, and
        // a walk that sees silence must be able to tell it was ours.
        val weight =
            intakeBudget?.let { budget ->
                approxHeapBytes(event).also { bytes ->
                    if (!budget.tryAcquire(bytes)) {
                        val held = origin.url?.let { parked.getOrPut(it) { AtomicInteger() } }
                        held?.incrementAndGet()
                        try {
                            budget.acquire(bytes)
                        } finally {
                            held?.decrementAndGet()
                        }
                    }
                }
            } ?: 0L
        // Counted before the send and taken back if it fails, or a fast worker decrements first.
        queued.incrementAndGet()
        var handedOff = false
        try {
            val inbound = Inbound(event, skipVerify, origin, onOutcome, weight)
            // The fast path first, so `parked` counts only a send that suspends.
            if (this.inbound.trySend(inbound).isSuccess) {
                handedOff = true
            } else {
                val held = origin.url?.let { parked.getOrPut(it) { AtomicInteger() } }
                held?.incrementAndGet()
                try {
                    this.inbound.send(inbound)
                    handedOff = true
                } finally {
                    held?.decrementAndGet()
                }
            }
            submitted.incrementAndGet()
            bootName?.let { BootClock.once("$it: first event submitted") }
        } catch (_: ClosedSendChannelException) {
            // Shutdown raced this event in. A publish still hears once, as quartz's own queue
            // answers it: EventIngest owes every submission exactly one callback.
            onOutcome?.let { deliver(it, SHUTTING_DOWN) }
        } finally {
            // `send` also throws CancellationException on shutdown.
            if (!handedOff) {
                queued.decrementAndGet()
                intakeBudget?.release(weight)
            }
        }
    }

    /** A publish: answered once with its outcome, from a worker thread. */
    override suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = submit(event, skipVerify, IngestOrigin.Local, onComplete)

    override fun hasBacklog(): Boolean = queued.get() > 0 || inBatch() > 0

    /**
     * One worker: take a batch off the channel, run it through the store, repeat. The exit is
     * reported because the scope's SupervisorJob lets one worker die while the others run on.
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
            servingPressure?.backoffMs()?.takeIf { it > 0 }?.let { delay(it) }
            val first = inbound.receiveCatching().getOrNull() ?: break
            batch.clear()
            batch.add(first)
            while (batch.size < batchSize) {
                val next = inbound.tryReceive().getOrNull() ?: break
                batch.add(next)
            }
            // Around the whole pass: the only record that a worker is inside a store round trip.
            // Marked before the batch leaves [queued], so hasBacklog() never reads an event that
            // is out of the channel and not yet answered as no backlog at all.
            busySince.set(worker, System.currentTimeMillis())
            queued.addAndGet(-batch.size)
            // Events of this pass no counter has taken yet; what a fault loses.
            var untallied = batch.size
            val answers = Answers(batch)
            try {
                val valid = admit(batch, answers)
                untallied = valid.size
                if (valid.isEmpty()) continue
                // Before the batch write, where a report raised in parallel has no single event.
                audit?.let { for (i in valid) it.inspect(batch[i].event) }
                val origins =
                    if (refusals.tracksOrigins) valid.associateTo(HashMap(valid.size)) { batch[it].event.id to batch[it].origin } else emptyMap()
                // The write tallies every event it is handed, through its own callbacks.
                untallied = 0
                insertIsolating(valid, batch, answers, origins)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                lostToFault(untallied, e)
            } catch (e: StackOverflowError) {
                // A pathological event costs its batch, never the worker; other Errors still end it.
                lostToFault(untallied, e)
            } finally {
                // Whatever the pass did not answer, a fault took: every submission hears exactly once.
                answers.rest(IEventStore.InsertOutcome.Failed(RejectionReason.INSERT_FAILED))
                intakeBudget?.let { budget -> budget.release(batch.sumOf { it.weight }) }
                busySince.set(worker, 0)
            }
        }
    }

    /** A batch pass threw; its untallied events are counted lost and the worker carries on. */
    private fun lostToFault(
        untallied: Int,
        e: Throwable,
    ) {
        if (untallied > 0) {
            reject("ingest fault: ${e.javaClass.simpleName}", untallied.toLong())
        }
        val signature = "fault ${e.javaClass.name}: ${e.message?.take(200)}"
        if (poisonSeen.size < POISON_SAMPLE_LIMIT && poisonSeen.add(signature)) {
            System.err.println("router: ingest batch pass FAILED, $untallied event(s) lost — ${e.javaClass.simpleName}: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * The batch's events worth writing, in batch order: duplicates, superseded replaceables and
     * bad signatures dropped and tallied. Only a verified copy may shadow another, and only a copy
     * whose id matches its content may place its group, so no forgery costs the batch a genuine event.
     */
    private suspend fun admit(
        batch: List<Inbound>,
        answers: Answers,
    ): List<Int> {
        val fresh = dropDuplicates(batch, answers)
        val hashStartedNs = System.nanoTime()
        val (placed, forged) = ledByContent(batch, fresh, answers)
        // Booked to `verify` without a call of its own, since it is the id half of the check below.
        IngestStats.add("verify", System.nanoTime() - hashStartedNs)
        tallyBadSignatures(forged)
        val contests = dropSuperseded(batch, placed, answers)
        if (contests.isEmpty()) return emptyList()
        val winners = ArrayList<Int>(contests.size)
        val superseded = ArrayList<List<Int>>()
        var badSigs = 0
        var duplicates = 0
        IngestStats.timed("verify") {
            for (versions in contests) {
                // One version, one copy: nothing for a check to choose between. A writer whose far
                // side verifies (the relay) refuses a bad one itself, and nothing here is shadowed
                // by it. A contest still verifies, since only a verified candidate may beat another.
                val unchecked = writer.checksSignatures && versions.size == 1 && versions[0].size == 1
                var won = false
                for (copies in versions) {
                    if (won) {
                        superseded.add(copies)
                        continue
                    }
                    var winner = -1
                    for ((n, i) in copies.withIndex()) {
                        val msg = batch[i]
                        when {
                            won -> {
                                duplicates++
                                // Heard only once the copy that is written has its own verdict.
                                answers.follow(i, winner)
                            }

                            // The leader's id is already checked against its content; the rest are not.
                            msg.skipVerify || unchecked ||
                                runCatching { if (n == 0) msg.event.verifySignature() else msg.event.verify() }.getOrDefault(false) -> {
                                winners.add(i)
                                won = true
                                winner = i
                            }

                            else -> {
                                badSigs++
                                answers.answer(i, BAD_SIGNATURE)
                            }
                        }
                    }
                }
            }
        }
        tallyBadSignatures(badSigs)
        if (duplicates > 0) {
            reject(RejectionReason.DUPLICATE.take(48), duplicates.toLong())
        }
        if (superseded.isNotEmpty()) reportSuperseded(batch, superseded, answers)
        winners.sort()
        return winners
    }

    private fun tallyBadSignatures(count: Int) {
        if (count == 0) return
        // The reason first, then the total: see [reject].
        badSignatures.addAndGet(count.toLong())
        rejected.addAndGet(count.toLong())
    }

    /**
     * The batch's copies of each event, as batch indices by arrival (ephemeral kinds each their
     * own), minus every id the store already holds via [knownIds] when the batch is wide enough
     * to pay for the round trip. Safe on an unverified id: a dropped event is never stored.
     */
    private suspend fun dropDuplicates(
        batch: List<Inbound>,
        answers: Answers,
    ): List<List<Int>> {
        val byId = HashMap<String, MutableList<Int>>(batch.size)
        val copies = ArrayList<MutableList<Int>>(batch.size)
        batch.forEachIndexed { i, msg ->
            if (msg.event.kind.isEphemeral()) {
                copies.add(mutableListOf(i))
            } else {
                byId.getOrPut(msg.event.id) { ArrayList<Int>(1).also { copies.add(it) } }.add(i)
            }
        }

        val probe = knownIds
        // Trusted events skip verification, so they earn the probe nothing.
        val verifiable = copies.count { !batch[it[0]].skipVerify }
        val probed = probe != null && verifiable >= PROBE_MIN_VERIFIABLE && idProbeTrouble.ready() && idGate.worthIt()
        // Null where the probe did not run or failed; a failed probe costs time, never correctness.
        val stored =
            if (!probed) {
                null
            } else {
                probing(idProbeTrouble) {
                    // One store call round the whole probe: the worker is suspended in the fan-out.
                    storeCall(StoreCalls.CALLER_INGEST_DEDUP, StoreCalls.OP_EXISTING_IDS, StoreCalls.ids(copies.size)) {
                        bounded("id") {
                            IngestStats.timed("dedup.pre") {
                                copies
                                    .map { batch[it[0]].event.id }
                                    .chunked(dedupChunk)
                                    .mapBounded(QUERY_FANOUT) { probe(it) }
                                    .flatMapTo(HashSet()) { it }
                            }
                        }
                    }
                }
            }

        val fresh =
            if (stored.isNullOrEmpty()) {
                copies
            } else {
                copies.filter { c -> (batch[c[0]].event.id !in stored).also { new -> if (!new) c.forEach { answers.answer(it, DUPLICATE) } } }
            }
        // Only an answered probe teaches the gate.
        if (stored != null) idGate.record(copies.size, copies.size - fresh.size)
        val dropped = copies.sumOf { it.size } - fresh.sumOf { it.size }
        if (dropped > 0) {
            // The store's own word, so duplicates dropped here and there tally on one line.
            reject(RejectionReason.DUPLICATE.take(48), dropped.toLong())
        }
        return fresh
    }

    /**
     * Each group of [copies] led by its first trusted copy or copy whose id hashes to its content,
     * the only copy whose kind, author and stamp may place the group. Copies ahead of it, and
     * groups with none, are forged; their count is returned beside the groups.
     */
    private fun ledByContent(
        batch: List<Inbound>,
        copies: List<List<Int>>,
        answers: Answers,
    ): Pair<List<List<Int>>, Int> {
        var forged = 0
        val led = ArrayList<List<Int>>(copies.size)
        for (c in copies) {
            val lead = c.indexOfFirst { batch[it].skipVerify || runCatching { batch[it].event.verifyId() }.getOrDefault(false) }
            if (lead < 0) {
                forged += c.size
                c.forEach { answers.answer(it, BAD_SIGNATURE) }
                continue
            }
            forged += lead
            for (k in 0 until lead) answers.answer(c[k], BAD_SIGNATURE)
            led.add(if (lead == 0) c else c.subList(lead, c.size))
        }
        return led to forged
    }

    /** Write a batch through the store's bulk path; if it throws, bisect so one bad event does not cost the batch. */
    private suspend fun insertIsolating(
        valid: List<Int>,
        batch: List<Inbound>,
        answers: Answers,
        origins: Map<String, IngestOrigin>,
    ) {
        val events = valid.map { batch[it].event }
        // By identity: the bisection hands back sublists of [events], and copies of one id are separate submissions.
        val indexOf = IdentityHashMap<Event, Int>(events.size).apply { events.forEachIndexed { k, e -> put(e, valid[k]) } }
        // Outcomes of the current write not yet in a counter; the bisection writes one half at a time.
        var owed = 0
        insertBisecting(
            events = events,
            // Booked apart from the probes: this waits on the writer mutex, they wait on the query path.
            write = { batch -> storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(batch.size)) { writer.batchInsert(batch) } },
            onOutcomes = { written, outcomes ->
                owed = outcomes.size
                // A misattributed rejection would suppress a wanted id, so only attribution is withheld.
                val aligned = outcomes.size == written.size
                if (!aligned) reportMisalignment(written.size, outcomes.size)
                for ((i, outcome) in outcomes.withIndex()) {
                    // Ahead of the branch, whose first statement counts the outcome.
                    owed--
                    if (aligned) indexOf[written[i]]?.let { answers.answer(it, outcome) }
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> {
                            accepted.incrementAndGet()
                            bootName?.let { BootClock.once("$it: first event stored") }
                        }

                        is IEventStore.InsertOutcome.Rejected -> {
                            // The relay's verdict on an event left to its check, in the pipeline's own words.
                            if (outcome.reason == BAD_SIGNATURE.reason) badSignatures.incrementAndGet()
                            reject(outcome.reason.take(48), 1L)
                            // Only the Rejected branch reports: a Failed outcome is the store's fault.
                            if (aligned) {
                                written.getOrNull(i)?.let { event ->
                                    reportRefusal(event, origins[event.id] ?: IngestOrigin.Local, outcome.reason)
                                }
                            }
                        }

                        is IEventStore.InsertOutcome.Failed -> {
                            reject("store failed: ${outcome.reason.take(40)}", 1L)
                            lostToStore.incrementAndGet()
                        }
                    }
                }
            },
            onOutcomesFailed = { _, e ->
                if (owed > 0) {
                    reject("ingest fault: ${e.javaClass.simpleName}", owed.toLong())
                }
                reportBookkeepingFailure("outcome bookkeeping", e, "$owed uncounted outcome(s) tallied as lost, their refusal records lost")
            },
            onPoison = { event, e ->
                indexOf[event]?.let { answers.answer(it, IEventStore.InsertOutcome.Failed("error: the store could not take this event")) }
                reject("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L)
                reportPoison(event, e)
            },
            onGaveUp = { lost, e ->
                // Tallied apart from the isolated ones: "could not say which" is not "this event is bad".
                reject("store ${e.javaClass.simpleName} (batch, unisolated)", lost.size.toLong())
                lostToStore.addAndGet(lost.size.toLong())
            },
        )
    }

    /** One permanent refusal, reported to the filter and the healer, from the store's verdict or [dropSuperseded]. */
    private fun reportRefusal(
        event: Event,
        origin: IngestOrigin,
        reason: String,
    ) {
        if (reason.startsWith(RejectionReason.PREFIX_REPLACED)) replacedRejects.incrementAndGet()
        // Bookkeeping: a sink that throws must not cost the event's batch, which is already tallied.
        try {
            refusals.onRefused(event, origin, reason)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            reportBookkeepingFailure("refusal sink", e, "the counts stand, the refusal record is lost")
        }
    }

    /** A failure in the refusal bookkeeping, logged once per distinct failure. */
    private fun reportBookkeepingFailure(
        where: String,
        e: Throwable,
        consequence: String,
    ) {
        val signature = "$where ${e.javaClass.name}: ${e.message?.take(200)}"
        if (poisonSeen.size >= POISON_SAMPLE_LIMIT || !poisonSeen.add(signature)) return
        System.err.println("router: ingest $where FAILED — ${e.javaClass.simpleName}: ${e.message}; $consequence")
        e.printStackTrace()
    }

    /**
     * Each event's [copies] as one contest, except that every version of a plain replaceable's
     * address joins one contest, newest first, minus those the store already beats via
     * [newestVersions]. Addressables stay the store's business. Read before the writer lock, so not exact.
     */
    private suspend fun dropSuperseded(
        batch: List<Inbound>,
        copies: List<List<Int>>,
        answers: Answers,
    ): List<List<List<Int>>> {
        // A batch carrying a deletion or a vanish goes to the store whole: an event's fate there
        // depends on its position among the others. Keyed on the kind, not the type.
        val carriesRetraction =
            copies.any { batch[it[0]].event.kind.let { k -> k == DeletionRequestEvent.KIND || k == RequestToVanishEvent.KIND } }
        if (carriesRetraction) return copies.map { listOf(it) }

        val contests = ArrayList<MutableList<List<Int>>>(copies.size)
        val byAddress = HashMap<Pair<Int, String>, MutableList<List<Int>>>()
        for (c in copies) {
            val e = batch[c[0]].event
            if (!e.kind.isReplaceable() || e.kind.isAddressable()) {
                contests.add(mutableListOf(c))
            } else {
                byAddress.getOrPut(e.kind to e.pubKey) { ArrayList<List<Int>>(1).also { contests.add(it) } }.add(c)
            }
        }
        if (byAddress.isEmpty()) return contests
        // NIP-01 newest-wins, tie to the lower id.
        val newestFirst = compareByDescending<List<Int>> { batch[it[0]].event.createdAt }.thenBy { batch[it[0]].event.id }
        for (versions in byAddress.values) versions.sortWith(newestFirst)

        val probe = newestVersions
        if (probe == null || byAddress.size < PROBE_MIN_VERIFIABLE || !versionProbeTrouble.ready() || !versionGate.worthIt()) return contests
        val stored = readNewestVersions(probe, byAddress.keys) ?: return contests
        val beaten = ArrayList<List<Int>>()
        var beatenAddresses = 0
        for ((key, versions) in byAddress) {
            val held = stored[key] ?: continue
            // Newest first, so whatever the stored version beats is a suffix.
            val cut = versions.indexOfFirst { held.beats(batch[it[0]].event) }
            if (cut < 0) continue
            val lost = versions.subList(cut, versions.size)
            beaten.addAll(lost)
            lost.clear()
            if (cut == 0) beatenAddresses++
        }
        versionGate.record(byAddress.size, beatenAddresses)
        // The stored version is verified, so these lose before any signature is checked.
        if (beaten.isNotEmpty()) reportSuperseded(batch, beaten, answers)
        return contests.filter { it.isNotEmpty() }
    }

    /**
     * Tally versions a verified one beats as `replaced:` and report each to the sink, since the
     * store never sees them. Each is reported by its leader, and only once its id is bound to its
     * content: an unchecked id would be attacker-chosen.
     */
    private fun reportSuperseded(
        batch: List<Inbound>,
        versions: List<List<Int>>,
        answers: Answers,
    ) {
        for (copies in versions) copies.forEach { answers.answer(it, REPLACED) }
        val dropped = versions.sumOf { it.size }.toLong()
        reject(RejectionReason.REPLACED.take(48), dropped)
        for (copies in versions) {
            val msg = batch[copies[0]]
            // A trusted leader skipped the id check every other leader passed.
            if (msg.skipVerify && !runCatching { msg.event.verifyId() }.getOrDefault(false)) continue
            reportRefusal(msg.event, msg.origin, RejectionReason.REPLACED)
        }
    }

    /** One query per kind per chunk of authors; null if the probe failed. */
    private suspend fun readNewestVersions(
        probe: suspend (Int, List<String>) -> Map<String, AddressVersion>,
        addresses: Set<Pair<Int, String>>,
    ): Map<Pair<Int, String>, AddressVersion>? =
        probing(versionProbeTrouble) {
            storeCall(
                StoreCalls.CALLER_INGEST_VERSIONS,
                StoreCalls.OP_NEWEST_VERSIONS,
                "${addresses.map { it.first }.distinct().size} kind(s), ${addresses.size} address(es)",
            ) {
                bounded("version") {
                    IngestStats.timed("versions.pre") {
                        addresses
                            .groupBy({ it.first }, { it.second })
                            .flatMap { (kind, authors) -> authors.chunked(CHECK_CHUNK).map { kind to it } }
                            .mapBounded(QUERY_FANOUT) { (kind, authors) -> probe(kind, authors).mapKeys { (author, _) -> kind to author } }
                            .fold(HashMap()) { all, part -> all.apply { putAll(part) } }
                    }
                }
            }
        }

    /**
     * [read] cut at [probeTimeoutMs], the cut thrown as a [ProbeTimeoutException]: inside the store
     * call it books as `failed`, not as the `cancelled` of a shutdown, and [probing] counts and backs
     * it off like any failed probe. Cancelling the read cancels its HTTP call.
     */
    private suspend fun <T : Any> bounded(
        name: String,
        read: suspend () -> T,
    ): T = withTimeoutOrNull(probeTimeoutMs) { read() } ?: throw ProbeTimeoutException(name, probeTimeoutMs)

    /** [read]'s answer, or null when it threw: counted, logged and backed off in [trouble]. */
    private suspend fun <T> probing(
        trouble: ProbeTrouble,
        read: suspend () -> T,
    ): T? =
        try {
            read().also { trouble.succeeded() }
        } catch (e: CancellationException) {
            // Swallowed, a shutdown cancellation would write into a closing store.
            throw e
        } catch (e: Exception) {
            trouble.failed(e)
            null
        }

    /**
     * Count [count] rejections under [reason]: the REASON FIRST, then the total. Whoever reads
     * them reads the total first — [rejectionBreakdown] returns "" while it is 0, and a test or a
     * stats line waits on [rejected] — so with the total bumped first a reader could see a
     * rejection with no reason behind it yet. IngestDedupTest did, intermittently on a loaded
     * machine: one rejected, an empty breakdown. The atomic's write publishes the map's.
     */
    private fun reject(
        reason: String,
        count: Long,
    ) {
        noteRejection(reason, count)
        rejected.addAndGet(count)
    }

    /**
     * Tally [count] rejections under [reason], keeping at most [REASON_LIMIT] distinct reasons:
     * the store's throws embed per-event content, so a failing store would mint a key per event.
     */
    private fun noteRejection(
        reason: String,
        count: Long,
    ) {
        if (rejectReasons.size >= REASON_LIMIT && !rejectReasons.containsKey(reason)) {
            rejectReasons.merge(OVERFLOW_REASON, count, Long::plus)
        } else {
            rejectReasons.merge(reason, count, Long::plus)
        }
    }

    /** Log an event the store threw on, once per distinct failure, with the raw JSON. */
    private fun reportPoison(
        event: Event,
        error: Throwable,
    ) {
        // Size checked before add: messages embed event content, so the set must stop growing.
        if (poisonSeen.size >= POISON_SAMPLE_LIMIT) return
        val signature = "${error.javaClass.name}: ${error.message}"
        if (!poisonSeen.add(signature)) return
        System.err.println(
            "router: store rejected event ${event.id} (kind ${event.kind}, pubkey ${event.pubKey}) — " +
                "${error.javaClass.simpleName}: ${error.message}\n" +
                "router: the event, verbatim: ${event.toJson().take(POISON_JSON_CHARS)}",
        )
    }

    /** The store returned a different number of outcomes than it was handed. Reported once. */
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

    /** The refused-id figures for the health line. */
    fun suppressionBreakdown(): String =
        if (suppressed.get() == 0L && replacedRejects.get() == 0L) {
            ""
        } else {
            " [replaced x${replacedRejects.get()}; suppressed x${suppressed.get()}]"
        }

    /** What each drop-probe is currently doing, for the stats line. Empty until a gate has judged anything. */
    fun probeStatus(): String {
        val parts =
            listOfNotNull(
                describeGate("id", idGate, knownIds != null, idProbeTrouble.failures()),
                describeGate("version", versionGate, newestVersions != null, versionProbeTrouble.failures()),
            )
        return if (parts.isEmpty()) "" else "router: ingest probes ${parts.joinToString(", ")}"
    }

    private fun describeGate(
        name: String,
        gate: ProbeGate,
        wired: Boolean,
        failures: Long,
    ): String? {
        if (!wired) return "$name off (not wired)"
        val rate = gate.hitRate()
        val judged =
            if (rate == 0.0 && !gate.hasJudged()) null else "${"%.0f".format(rate * 100)}% dropped${if (gate.paying()) "" else ", sampling only"}"
        val failed = if (failures > 0) "$failures failed" else null
        val parts = listOfNotNull(judged, failed)
        return if (parts.isEmpty()) null else "$name ${parts.joinToString(", ")}"
    }

    /** Workers inside a batch pass right now; with [oldestBatchMs], what tells backpressure from a wedge. */
    fun inBatch(): Int = (0 until workers).count { busySince.get(it) != 0L }

    fun oldestBatchMs(): Long {
        val now = System.currentTimeMillis()
        var oldest = 0L
        for (i in 0 until workers) {
            busySince.get(i).takeIf { it != 0L }?.let { oldest = maxOf(oldest, now - it) }
        }
        return oldest
    }

    val workerCount: Int get() = workers

    /** How many workers are still looping. Below [workerCount] means a worker exited. */
    fun workersRunning(): Int = loopsRunning.get()

    /**
     * Whether ingest has stopped: no worker is waiting on the channel, and none has started a
     * batch within [wedgeAfterMs]. Defined by the workers, not the queue depth. Reports, never ends.
     */
    fun wedged(): Boolean {
        // The loop below is vacuously true over no workers.
        if (workers == 0) return false
        val now = System.currentTimeMillis()
        for (i in 0 until workers) {
            val since = busySince.get(i)
            if (since == 0L || now - since < wedgeAfterMs) return false
        }
        return true
    }

    /** Why events were rejected, as counts, biggest first. The overflow bucket is one of the reasons. */
    fun rejectionReasons(limit: Int = REJECTION_ROWS): List<Pair<String, Long>> =
        (
            rejectReasons.entries.map { it.key to it.value } +
                listOfNotNull(badSignatures.get().takeIf { it > 0 }?.let { "bad signature" to it })
        ).sortedByDescending { it.second }
            .take(limit)

    /** The top rejection reasons for the stats line. */
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

    /** Called after the scope is cancelled, so a worker mid-batch is cancelled rather than stranded. */
    override fun close() {
        runCatching { pool.close() }
    }

    /**
     * Each submission's one answer for a pass, by batch index. A copy that [follow]s the copy
     * written in its place hears that copy's verdict, as a duplicate when it was accepted. A
     * callback that throws costs nothing but its own delivery.
     */
    private inner class Answers(
        private val batch: List<Inbound>,
    ) {
        private val answered = BooleanArray(batch.size)
        private var followers: HashMap<Int, MutableList<Int>>? = null

        fun follow(
            copy: Int,
            leader: Int,
        ) {
            (followers ?: HashMap<Int, MutableList<Int>>().also { followers = it }).getOrPut(leader) { ArrayList(1) }.add(copy)
        }

        fun answer(
            i: Int,
            outcome: IEventStore.InsertOutcome,
        ) {
            if (answered[i]) return
            answered[i] = true
            batch[i].onOutcome?.let { deliver(it, outcome) }
            followers?.remove(i)?.forEach { answer(it, if (outcome is IEventStore.InsertOutcome.Accepted) DUPLICATE else outcome) }
        }

        fun rest(outcome: IEventStore.InsertOutcome) {
            for (i in batch.indices) answer(i, outcome)
        }
    }

    private fun deliver(
        onOutcome: (IEventStore.InsertOutcome) -> Unit,
        outcome: IEventStore.InsertOutcome,
    ) {
        try {
            onOutcome(outcome)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            reportBookkeepingFailure("outcome callback", e, "that submission's answer is lost")
        }
    }

    companion object {
        const val REJECTION_ROWS = 4

        // What a publisher hears, in the store's own words, for the verdicts reached before the store.
        private val DUPLICATE = IEventStore.InsertOutcome.Rejected(RejectionReason.DUPLICATE)
        private val REPLACED = IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED)
        private val BAD_SIGNATURE = IEventStore.InsertOutcome.Rejected("invalid: bad signature or id")
        private val SUPPRESSED = IEventStore.InsertOutcome.Rejected("blocked: refused twice before")
        private val SHUTTING_DOWN = IEventStore.InsertOutcome.Failed("error: relay shutting down")

        private const val MAX_INBOUND_QUEUE = 16_384

        /** Events a batch must expect to verify before the dedup probe is worth its round trip. */
        private const val PROBE_MIN_VERIFIABLE = 128

        /** Authors per version query. */
        private const val CHECK_CHUNK = 500

        /** Distinct rejection reasons kept before [noteRejection] folds the rest into one. */
        private const val REASON_LIMIT = 64

        private const val OVERFLOW_REASON = "other store failures"

        private const val POISON_SAMPLE_LIMIT = 20

        private const val POISON_JSON_CHARS = 4_000

        /** A false `wedged` is worse than a late one. Nothing here stops the pass. */
        const val WEDGE_AFTER_MS = 600_000L

        /**
         * How long a drop-probe may take before its batch goes to the store unprobed. A probe only
         * saves the verifying of events the store would refuse anyway, so a slow one is worth
         * nothing and an unanswered one was the costliest call in the process: on staging one
         * `newestVersions` query got its headers and part of its body and then nothing, for
         * 4 h 40 min (NosFabrica/vespa-relay#289). Its pass held one of the two workers, and every
         * visit with an event in that batch waited on it. Neither the store's client nor Vespa's
         * query profile bounds a query, so this is the bound. Measured there: `versions.pre` mean
         * 737 ms and max 10.3 s, `dedup.pre` max 5.6 s.
         */
        const val PROBE_TIMEOUT_MS = 60_000L
    }
}

/** A drop-probe the store did not answer within the pipeline's bound; the batch goes on unprobed. */
class ProbeTimeoutException(
    probe: String,
    timeoutMs: Long,
) : Exception("the $probe probe got no answer from the store in ${timeoutMs}ms")

/** The newest stored version of one address, which an arriving replaceable has to beat. */
data class AddressVersion(
    val createdAt: Long,
    val id: String,
) {
    /** Strictly: an equal stamp and id is the same event. */
    fun beats(event: Event): Boolean = createdAt > event.createdAt || (createdAt == event.createdAt && id < event.id)
}

/** `SYNC_INGEST_CONCURRENCY`, `SYNC_INGEST_BATCH` and `VESPA_DEDUP_CHUNK`, read at boot. A type so a caller cannot swap them. */
data class IngestTuning(
    val concurrency: Int,
    val batch: Int,
    /** Ids per dedup probe query: the store's own knob, so a widened stage B is matched here. */
    val dedupChunk: Int = DEFAULT_DEDUP_CHUNK,
    /**
     * Events that may wait in the channel; null is the mirror's sizing (four batches, at least
     * 4,096), deep enough to ride out a download's bursts. A relay's publishes are already
     * buffered by their senders, so it asks for one batch per worker: every event waiting here
     * holds a parsed copy in the heap, up to the relay's frame limit each.
     */
    val queue: Int? = null,
    /**
     * Parsed bytes the events taken in and not yet written may hold, waiting in the channel or in
     * a worker's pass, estimated by [approxHeapBytes]; null is no bound beyond [queue]'s count. A
     * relay sets one: its publishes may each be as large as its frame limit, and a count alone lets
     * a flood of such events take the heap. One event over the budget is still admitted alone.
     */
    val queueBytes: Long? = null,
) {
    companion object {
        const val DEFAULT_DEDUP_CHUNK = 500
    }
}
