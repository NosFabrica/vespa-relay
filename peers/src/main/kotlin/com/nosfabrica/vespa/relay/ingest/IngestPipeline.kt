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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
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
 * bounded channel and a pool of workers draining it in batches through
 * [IEventStore.batchInsert], with duplicates and superseded replaceables
 * dropped before the signature check.
 *
 * The channel is bounded so a fast download cannot outrun ingest and pile
 * events onto the heap: when it fills, [submit] suspends the producer and
 * the upstream throttles to the ingest rate.
 */
class IngestPipeline(
    private val store: IEventStore,
    tuning: IngestTuning,
    // Runs every mirrored event through quartz's search-indexing parse when set.
    private val audit: ParseAudit?,
    // Clients first: ingest yields when their reads slow down.
    private val servingPressure: ServingPressure?,
    private val scope: CoroutineScope,
    /**
     * Which of these ids the store already holds. Null disables the probe,
     * which is only slower, never wrong. See [dropDuplicates].
     */
    private val knownIds: (suspend (List<String>) -> Set<String>)? = null,
    /**
     * The newest stored version of each `(kind, author)` address in a chunk.
     * Null leaves supersession to the store. See [dropSuperseded].
     */
    private val newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)? = null,
    private val refusals: RefusalSink = RefusalSink.None,
    /**
     * How long a batch pass runs before the pipeline reads as wedged rather
     * than backpressured. A parameter only so a test can reach the state.
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
     * How many downloaded events may wait for ingest. Bounded at both ends:
     * batch size and queue depth are separate concerns.
     */
    val capacity = (tuning.batch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

    /**
     * How many events one worker takes per pass, capped to its fair share of
     * the channel. The cap bounds a mirror's throughput: commits serialise on
     * the store's one writer mutex, so survivors per lock hold is what counts,
     * and a mirror's batches are mostly duplicates. Widening is an operator
     * lever, left to the operator because every other writer waits on that mutex.
     */
    private val batchSize = tuning.batch.coerceAtMost((capacity / workers).coerceAtLeast(1))

    private val inbound = Channel<Inbound>(capacity)

    /**
     * Threads the ingest workers own outright, so batch work stays off the
     * shared pool. On its own this does not prevent the deadlock [submit]
     * describes: the store reaches for `Dispatchers.IO` regardless.
     */
    private val pool =
        Executors
            .newFixedThreadPool(workers) { r ->
                Thread(r, "vespa-relay-ingest").apply { isDaemon = true }
            }.asCoroutineDispatcher()

    /**
     * How full [inbound] is. Channel does not expose its depth, and this
     * number decides whether the pipeline is starved or backpressured.
     */
    val queued = AtomicInteger()

    /**
     * Producers suspended in a full queue, per relay. quartz drains a socket
     * through one consumer that awaits every listener, so a parked producer
     * silences every subscription on that relay; counted here so every
     * producer is covered, and only while the send actually suspends.
     */
    private val parked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()

    /** How many producers are suspended in a full queue on this relay's events right now. */
    fun parkedOn(url: NormalizedRelayUrl): Int = parked[url]?.get() ?: 0

    /** At capacity: the next [submit] would suspend its caller, and with it that caller's socket. */
    fun isFull(): Boolean = queued.get() >= capacity

    val accepted = AtomicLong()
    val rejected = AtomicLong()

    /**
     * Events handed to the queue since boot, the arrival side of [accepted]
     * + [rejected]. Counted once the send has returned so this and [queued]
     * agree; a [suppressed] event never reaches the channel and is not here.
     */
    val submitted = AtomicLong()

    /**
     * Good events the store refused for structural reasons, which nothing
     * will re-offer. Distinct from [rejected], most of which is the protocol working.
     */
    val lostToStore = AtomicLong()

    // Separated because duplicates dwarf accepts on a wide fan-out, while a
    // bad signature means an upstream serves junk.
    private val badSignatures = AtomicLong()

    /** Rejections by reason. Only ever written through [noteRejection], which bounds it. */
    private val rejectReasons = ConcurrentHashMap<String, Long>()

    /**
     * Events dropped before the store because a filter says we have twice
     * refused them. Its own line, never folded into accepted or rejected.
     */
    val suppressed = AtomicLong()

    /** `REPLACED` refusals on their own: they are the loop the refused-id filter exists for. */
    val replacedRejects = AtomicLong()

    // Store failures already reported in full, so the raw-event dump stays
    // one per distinct defect.
    private val poisonSeen = ConcurrentHashMap.newKeySet<String>()

    /** Break-even for the id probe, measured by `IngestCostBench`. */
    private val idGate = ProbeGate(minHitRate = 0.35)

    /** Break-even for the version probe, measured the same way. */
    private val versionGate = ProbeGate(minHitRate = 0.20)

    /**
     * When each worker entered its current batch pass, or 0 while it waits
     * on the channel. A suspended worker has no frame in a thread dump, so
     * this is the only record that it is inside a store round trip.
     */
    private val busySince = AtomicLongArray(workers)

    /** Worker loops still running. Anything below [workers] means one exited and nothing drains its share. */
    private val loopsRunning = AtomicInteger()

    fun start() {
        // Announced when the batch is capped, so an operator who set
        // SYNC_INGEST_BATCH knows the knob did not connect.
        if (batchSize < configuredBatch) {
            System.err.println(
                "router: SYNC_INGEST_BATCH=$configuredBatch capped to $batchSize — " +
                    "$workers worker(s) share a $capacity-event queue. Every commit serializes on the store's " +
                    "one writer mutex, so a narrow batch buys nothing back: it writes fewer surviving events per " +
                    "lock hold and takes the lock more often. Fewer, WIDER workers ingest faster on a mirror " +
                    "(measured 9x) — at the cost of a longer lock hold for every other writer",
            )
        }
        // Below the probe's width every copy of an event is signature-checked
        // before the store drops it.
        if (knownIds != null && batchSize < PROBE_MIN_VERIFIABLE) {
            System.err.println(
                "router: ingest batch $batchSize is under the $PROBE_MIN_VERIFIABLE-event width the dedup " +
                    "probe needs — duplicates will be verified before the store rejects them",
            )
        }
        repeat(workers) { worker -> scope.launch(pool) { loop(worker) } }
    }

    /**
     * Hand an event to the pool, suspending the caller if the buffer is full.
     * Suspending rather than blocking matters: producers and the store share
     * the coroutine pool, so a parked producer holds a thread the drain needs.
     */
    suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        origin: IngestOrigin = IngestOrigin.Local,
    ) {
        // Checked here, not in the callers' onEvent: by now the caller has run
        // SyncCoverage.observe, so the leg keeps its per-kind evidence.
        if (refusals.isSuppressed(event)) {
            suppressed.incrementAndGet()
            return
        }
        // Counted before the send and taken back if it fails: a worker can
        // take the event and decrement before a post-send increment runs.
        queued.incrementAndGet()
        var handedOff = false
        try {
            val inbound = Inbound(event, skipVerify, origin)
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
        } catch (_: ClosedSendChannelException) {
            // Shutdown (closeIntake) raced this event in. Not an error.
        } finally {
            // A finally, because `send` also throws CancellationException on shutdown.
            if (!handedOff) queued.decrementAndGet()
        }
    }

    /**
     * One worker: take a batch off the channel, run it through the store,
     * and repeat while the scope lives. The exit is reported: the scope's
     * SupervisorJob lets one worker die while the others keep running.
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
            // Clients first: the batch's probes land in the same engine a REQ
            // does. Zero while reads are healthy.
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
            // Around the whole pass and cleared in a finally: the only record
            // that a worker is inside a store round trip.
            busySince.set(worker, System.currentTimeMillis())
            try {
                // Before verify; see [dropDuplicates] and [dropSuperseded].
                val fresh = dropSuperseded(dropDuplicates(batch))
                if (fresh.isEmpty()) continue
                val valid = ArrayList<Event>(fresh.size)
                // Only built when the sink will read it.
                val origins = if (refusals.tracksOrigins) HashMap<String, IngestOrigin>(fresh.size) else null
                var verifyRejected = 0
                // Booked as a stage so it lands on the `router: ingest stages`
                // line beside the store's own.
                IngestStats.timed("verify") {
                    for (msg in fresh) {
                        if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                            valid.add(msg.event)
                            // A bad signature never reaches the sink: the same
                            // id can arrive correctly signed from another relay.
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
                // Before the batch write: the store feeds Vespa in parallel, so
                // a report raised inside batchInsert has no single event.
                audit?.let { for (event in valid) it.inspect(event) }
                insertIsolating(valid, origins ?: emptyMap())
            } finally {
                busySince.set(worker, 0)
            }
        }
    }

    /**
     * The batch minus everything we already hold, dropped before the
     * signature check so a mirror does not verify a popular event once per
     * relay carrying it. Two passes, cheapest first: in batch by id
     * (ephemeral kinds exempt, since the store counts a repeat of one as
     * accepted-not-stored), then in the store via [knownIds], gated on
     * [PROBE_MIN_VERIFIABLE] because the round trip only pays at width.
     *
     * Safe because a dropped event is never stored: the claimed id is
     * unverified, but a forgery naming an id we hold is dropped either way,
     * and everything that survives is verified in full before the store.
     */
    private suspend fun dropDuplicates(batch: List<Inbound>): List<Inbound> {
        val ids = HashSet<String>(batch.size)
        val once = ArrayList<Inbound>(batch.size)
        var dropped = 0
        for (msg in batch) {
            if (msg.event.kind.isEphemeral() || ids.add(msg.event.id)) once.add(msg) else dropped++
        }

        val probe = knownIds
        // The round trip saves verifications, and trusted events skip those already.
        val verifiable = once.count { !it.skipVerify }
        val probed = probe != null && verifiable >= PROBE_MIN_VERIFIABLE && idGate.worthIt()
        val stored =
            if (!probed) {
                emptySet()
            } else {
                try {
                    // Booked as one store call round the whole probe: the worker
                    // is suspended in the fan-out, not in one chunk. See [StoreCalls].
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
                    // Not runCatching: shutdown reaches the probe as a cancellation,
                    // and swallowed it would write into a closing store.
                    throw e
                } catch (_: Throwable) {
                    // A failed probe costs time, never correctness; the store's
                    // stage B decides.
                    emptySet()
                }
            }

        val fresh = if (stored.isEmpty()) once else once.filter { it.event.id !in stored }
        // Recorded whenever the probe ran, including when it found nothing;
        // the in-batch pass is free and teaches the gate nothing.
        if (probed) idGate.record(once.size, once.size - fresh.size)
        dropped += once.size - fresh.size
        if (dropped > 0) {
            rejected.addAndGet(dropped.toLong())
            // The store's own word, so a duplicate dropped here and there tally on one line.
            noteRejection(RejectionReason.DUPLICATE.take(48), dropped.toLong())
        }
        return fresh
    }

    /**
     * Write a batch through the store's bulk path; if it throws, bisect so
     * one bad event does not cost the batch. See [insertBisecting].
     */
    private suspend fun insertIsolating(
        events: List<Event>,
        origins: Map<String, IngestOrigin>,
    ) = insertBisecting(
        events = events,
        // Booked apart from the probes: this waits on the writer mutex, they
        // wait on the query path. Per bisection attempt, so an isolation pass
        // reports the batch it is actually writing.
        write = { batch -> storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(batch.size)) { store.batchInsert(batch) } },
        onOutcomes = { written, outcomes ->
            // Positional alignment is the store's contract, and a misattributed
            // rejection would suppress a wanted id; so it is checked, and only
            // attribution is withheld when it fails.
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
                        // Only the Rejected branch reports: a Failed outcome is
                        // the store's fault and the event is good.
                        if (aligned) {
                            written.getOrNull(i)?.let { event ->
                                reportRefusal(event, origins[event.id] ?: IngestOrigin.Local, outcome.reason)
                            }
                        }
                    }

                    is IEventStore.InsertOutcome.Failed -> {
                        // The event was good and is lost; lostToStore keeps it loud.
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
            // Out of budget: tallied apart from the isolated ones, since "we
            // could not say which" differs from "this event is bad".
            rejected.addAndGet(batch.size.toLong())
            noteRejection("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong())
            // Lost, not merely rejected: nothing re-offers them.
            lostToStore.addAndGet(batch.size.toLong())
        },
    )

    /**
     * One permanent refusal, reported to the filter and the healer. Called
     * from [dropSuperseded] as well as from the store's verdict: the fast
     * path drops a superseded replaceable before the store can say `replaced`.
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
     * The batch minus every replaceable a newer version already beats,
     * dropped before verification like a duplicate. The id probe cannot see
     * this arrival, since a newer generation is a different id. Two passes:
     * in batch by address, then in the store via [newestVersions], gated by
     * [versionGate].
     *
     * Plain replaceable kinds only: an addressable's version query is an
     * (authors x d-tags) cross product, and a truncated answer here is a
     * dropped event. The comparison is NIP-01's own, the rule the store's
     * stage D applies, but read before the writer lock: a NIP-09 deletion of
     * the newer version inside that window drops an event the store would
     * have taken, until the next full resync re-offers it.
     */
    private suspend fun dropSuperseded(batch: List<Inbound>): List<Inbound> {
        // Not for a batch carrying a deletion or a vanish: on the store's
        // replay path an event's fate depends on its position among the
        // others, and choosing here could leave an address empty. Keyed on the
        // kind, not the type: an Event that skipped quartz's factory is a plain Event.
        if (batch.any { it.event.kind == DeletionEvent.KIND || it.event.kind == RequestToVanishEvent.KIND }) return batch

        // Winner per address, by first appearance so the batch keeps its order.
        val keys = arrayOfNulls<Pair<Int, String>>(batch.size)
        val winners = LinkedHashMap<Pair<Int, String>, Int>()
        var candidates = 0
        batch.forEachIndexed { i, msg ->
            val e = msg.event
            if (!e.kind.isReplaceable() || e.kind.isAddressable()) return@forEachIndexed
            candidates++
            // Held for the second pass: one Pair per candidate.
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
                // Strictly beaten only: an equal stamp is the same event.
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
        // The store never sees these, so the `replaced:` verdict is reported
        // from here, or the filter and the healer go quiet in proportion to
        // how well this path works. `verifyId` binds the id to the content
        // that earned the verdict: nothing here is verified yet, so an
        // unchecked id would be attacker-chosen. The signature is not needed
        // because supersession is decided inside the hashed content.
        for (i in batch.indices) {
            if (!drop[i]) continue
            val msg = batch[i]
            if (msg.event.verifyId()) reportRefusal(msg.event, msg.origin, RejectionReason.REPLACED)
        }
        return batch.filterIndexed { i, _ -> !drop[i] }
    }

    /** NIP-01 newest-wins, tie to the lower id: the rule the store's stage D resolves by. */
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
            // One call round the whole fan-out, for [dropDuplicates]'s reason.
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
            // A failed probe costs time, not correctness.
            emptyMap()
        }

    /**
     * Tally [count] rejections under [reason], keeping at most [REASON_LIMIT]
     * distinct reasons. The store's throws embed per-event content, so a
     * store failing on every event would otherwise mint a key per event.
     */
    private fun noteRejection(
        reason: String,
        count: Long,
    ) {
        // Racy by a worker or two at the boundary; the point is a bound.
        if (rejectReasons.size >= REASON_LIMIT && !rejectReasons.containsKey(reason)) {
            rejectReasons.merge(OVERFLOW_REASON, count, Long::plus)
        } else {
            rejectReasons.merge(reason, count, Long::plus)
        }
    }

    /**
     * Log an event the store threw on, once per distinct failure, with the
     * raw JSON so the defect can be reproduced.
     */
    private fun reportPoison(
        event: Event,
        error: Throwable,
    ) {
        // Size checked before add: messages embed event content, so the set
        // must stop growing past the print limit.
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
     * Reported once; refusals stop being recorded until it is fixed.
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

    /** The refused-id figures for the health line. */
    fun suppressionBreakdown(): String =
        if (suppressed.get() == 0L && replacedRejects.get() == 0L) {
            ""
        } else {
            " [replaced x${replacedRejects.get()}; suppressed x${suppressed.get()}]"
        }

    /**
     * What each drop-probe is currently doing, for the stats line; a gated-off
     * probe is otherwise invisible. Empty until a gate has judged anything.
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
     * Workers inside a batch pass right now, and how long the oldest has been
     * there: the pair that tells a backpressured pipeline from a wedged one.
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

    /** How many workers are still looping. Below [workerCount] means a worker exited; see [loop]. */
    fun workersRunning(): Int = loopsRunning.get()

    /**
     * Whether ingest has stopped: no worker is waiting on the channel, and
     * none has started a batch within [wedgeAfterMs]. Says nothing about the
     * queue depth, since a wedge behind a slow upstream holds every worker
     * with the queue only part full. Reports the wedge and never ends it:
     * cutting a pass discards a batch of good events nothing re-offers.
     */
    fun wedged(): Boolean {
        // An empty pool must not read as wedged: the loop below is vacuously
        // true over no workers.
        if (workers == 0) return false
        val now = System.currentTimeMillis()
        for (i in 0 until workers) {
            val since = busySince.get(i)
            if (since == 0L || now - since < wedgeAfterMs) return false
        }
        return true
    }

    /**
     * Why events were rejected, as counts. Biggest first, bounded, and the
     * overflow bucket is one of the reasons it can return.
     */
    fun rejectionReasons(limit: Int = REJECTION_ROWS): List<Pair<String, Long>> =
        (
            rejectReasons.entries.map { it.key to it.value } +
                listOfNotNull(badSignatures.get().takeIf { it > 0 }?.let { "bad signature" to it })
        ).sortedByDescending { it.second }
            .take(limit)

    /**
     * The top rejection reasons for the stats line.
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
        /** How many rejection reasons the report publishes. */
        const val REJECTION_ROWS = 4

        /**
         * Ceiling on queued-but-not-yet-ingested events, independent of batch
         * size. It also caps [batchSize], which is derived from [capacity].
         */
        private const val MAX_INBOUND_QUEUE = 16_384

        /**
         * Events a batch must expect to verify before the dedup probe is worth
         * its round trip: the per-id price falls with width while a verify is flat.
         */
        private const val PROBE_MIN_VERIFIABLE = 128

        /**
         * Authors per version query. Not [DEDUP_CHUNK]: `VESPA_DEDUP_CHUNK`
         * widens the id check alone.
         */
        private const val CHECK_CHUNK = 500

        /**
         * Ids per probe query, read from the store's own knob so a widened
         * stage B is matched here.
         */
        private val DEDUP_CHUNK: Int = System.getenv("VESPA_DEDUP_CHUNK")?.toIntOrNull()?.coerceAtLeast(1) ?: 500

        /** Distinct rejection reasons kept before [noteRejection] folds the rest into one. */
        private const val REASON_LIMIT = 64

        /** Where reasons past [REASON_LIMIT] land. */
        private const val OVERFLOW_REASON = "other store failures"

        // Distinct store failures to dump a raw event for; past a handful it is a stuck loop.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it, even with a long kind-0 content.
        private const val POISON_JSON_CHARS = 4_000

        /**
         * How long a batch pass runs before it is reported as wedged. Sized
         * against the throughput floor observed on a production-scale store,
         * not the bench; a false `wedged` is worse than a late one. Nothing
         * here stops the pass: the store's query client has no read timeout.
         */
        const val WEDGE_AFTER_MS = 600_000L
    }
}

/** The newest stored version of one address, which an arriving replaceable has to beat. */
data class AddressVersion(
    val createdAt: Long,
    val id: String,
)

/**
 * How much of the store's throughput ingest may use: `ingestConcurrency`
 * and `ingestBatch` from `router.conf`. A type so a caller cannot swap them.
 */
data class IngestTuning(
    val concurrency: Int,
    val batch: Int,
)
