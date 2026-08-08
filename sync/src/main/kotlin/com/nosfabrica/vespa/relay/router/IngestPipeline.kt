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

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.relay.maintenance.ParseAudit
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

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
internal class IngestPipeline(
    private val store: IEventStore,
    config: RouterConfig,
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
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
    )

    private val workers = config.ingestConcurrency
    private val configuredBatch = config.ingestBatch

    /**
     * How many downloaded events may wait for ingest. Bounded at both ends —
     * this was `batch * 4` with only a floor, so raising the batch to 20000
     * silently sized the queue at 80,000 events and the heap went over. Batch
     * size and queue depth are separate concerns: the batch decides how much
     * each mutex hold amortises, the queue how much memory sits between
     * download and write.
     */
    val capacity = (config.ingestBatch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

    /**
     * How many events one worker takes per pass — capped to its fair share of
     * the channel. A batch bigger than that lets the first worker take
     * everything while the rest idle, collapsing ingest to one thread
     * grinding a very long batch.
     */
    private val batchSize = config.ingestBatch.coerceAtMost((capacity / workers).coerceAtLeast(1))

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
    private val unverified = AtomicLong()

    /** Rejections by reason. Only ever written through [noteRejection] — see there for why the ceiling matters. */
    private val rejectReasons = ConcurrentHashMap<String, Long>()

    // Store failures already reported in full, so the raw-event dump stays
    // one per distinct defect.
    private val poisonSeen = ConcurrentHashMap.newKeySet<String>()

    fun start() {
        // Announced when the batch is capped: an operator who set
        // SYNC_INGEST_BATCH and silently got a different number would be
        // tuning a knob that is not connected.
        if (batchSize < configuredBatch) {
            System.err.println(
                "router: SYNC_INGEST_BATCH=$configuredBatch capped to $batchSize — " +
                    "$workers worker(s) share a $capacity-event queue, and a batch bigger than " +
                    "one worker's share collapses ingest to a single thread",
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
        repeat(workers) { scope.launch(pool) { loop() } }
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
    ) {
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
            inbound.send(Inbound(event, skipVerify))
            handedOff = true
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

    private suspend fun loop() {
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
            // BEFORE verify, which is the whole point — see [dropDuplicates].
            val fresh = dropDuplicates(batch)
            if (fresh.isEmpty()) continue
            val valid = ArrayList<Event>(fresh.size)
            var verifyRejected = 0
            // Booked as a stage so it lands on the same `router: ingest stages`
            // line as the store's own dedup/guards/write. It was invisible
            // there for as long as it existed, which made "is verification the
            // limit?" a question no instrument in this repo could answer.
            IngestStats.timed("verify") {
                for (msg in fresh) {
                    if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                        valid.add(msg.event)
                    } else {
                        verifyRejected++
                    }
                }
            }
            if (verifyRejected > 0) {
                rejected.addAndGet(verifyRejected.toLong())
                unverified.addAndGet(verifyRejected.toLong())
            }
            if (valid.isEmpty()) continue
            // Before the batch write: the store feeds Vespa in parallel, so a
            // parse report raised inside batchInsert cannot be attributed to
            // one event. Inspecting here keeps the audit's ThreadLocal exact.
            audit?.let { for (event in valid) it.inspect(event) }
            insertIsolating(valid)
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
        val stored =
            if (probe == null || verifiable < PROBE_MIN_VERIFIABLE) {
                emptySet()
            } else {
                try {
                    IngestStats.timed("dedup.pre") {
                        once
                            .map { it.event.id }
                            .chunked(DEDUP_CHUNK)
                            .mapBounded(QUERY_FANOUT) { probe(it) }
                            .flatMapTo(HashSet()) { it }
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
    private suspend fun insertIsolating(events: List<Event>) =
        insertBisecting(
            events = events,
            write = { store.batchInsert(it) },
            onOutcomes = { outcomes ->
                for (outcome in outcomes) {
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> {
                            accepted.incrementAndGet()
                        }

                        is IEventStore.InsertOutcome.Rejected -> {
                            rejected.incrementAndGet()
                            noteRejection(outcome.reason.take(48), 1L)
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
        val bad = if (unverified.get() > 0) "bad signature x${unverified.get()}" else ""
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
        /**
         * Ceiling on queued-but-not-yet-ingested events, independent of batch
         * size. 16k events is a few hundred MB at Nostr's event sizes — far
         * short of the 80,000 that killed the process.
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
    }
}
