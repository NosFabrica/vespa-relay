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

import com.nosfabrica.vespa.relay.maintenance.ParseAudit
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusalSink
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
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

/**
 * The download-to-store pipeline every mirrored event funnels through: a
 * bounded channel, a pool of workers draining it in batches through
 * [IEventStore.batchInsert], and signature verification off the download
 * threads (skipped for trusted upstreams).
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
     * Where store refusals are reported and where suppression is asked about.
     * Defaults to off, so every existing caller and test behaves exactly as it
     * did before this existed.
     */
    private val refusals: RefusalSink = RefusalSink.None,
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
        val origin: IngestOrigin,
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
            val valid = ArrayList<Event>(batch.size)
            // Only built when something will read it. The sink is inert unless
            // SYNC_REFUSED_DIR is set, and the pipeline is shared by every
            // stream, so an unconditional map made every existing deployment
            // allocate and hash one entry per event for a lookup that never
            // happens.
            val origins = if (refusals.tracksOrigins) HashMap<String, IngestOrigin>(batch.size) else null
            var verifyRejected = 0
            for (msg in batch) {
                if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                    valid.add(msg.event)
                    // A bad signature never reaches this map, and so can never
                    // reach the refusal sink: an id is the hash of the CONTENT,
                    // not of the signature, so the same id can arrive correctly
                    // signed from another relay. Remembering it would make one
                    // relay's corruption permanent.
                    origins?.put(msg.event.id, msg.origin)
                } else {
                    verifyRejected++
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
            insertIsolating(valid, origins ?: emptyMap())
        }
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
        write = { store.batchInsert(it) },
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
                        rejectReasons.merge(outcome.reason.take(48), 1L, Long::plus)
                        // Attributed to the event that earned it. Only the
                        // REJECTED branch reports: a Failed outcome is the
                        // STORE's fault and the event is good, so recording
                        // it would turn a transient fault into permanent
                        // silent loss.
                        if (aligned) {
                            written.getOrNull(i)?.let { event ->
                                if (outcome.reason.startsWith(RejectionReason.PREFIX_REPLACED)) replacedRejects.incrementAndGet()
                                refusals.onRefused(event, origins[event.id] ?: IngestOrigin.Local, outcome.reason)
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
                        rejectReasons.merge("store failed: ${outcome.reason.take(40)}", 1L, Long::plus)
                        lostToStore.incrementAndGet()
                    }
                }
            }
        },
        onPoison = { event, e ->
            rejected.incrementAndGet()
            rejectReasons.merge("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L, Long::plus)
            reportPoison(event, e)
        },
        onGaveUp = { batch, e ->
            // Isolation ran out of budget: counted but unnamed, and
            // tallied apart from the isolated ones — "we could not say
            // which" is a different fact from "this event is bad".
            rejected.addAndGet(batch.size.toLong())
            rejectReasons.merge("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong(), Long::plus)
            // These are LOST, not merely rejected: the events were good,
            // the failure is the store's, and nothing re-offers them.
            lostToStore.addAndGet(batch.size.toLong())
        },
    )

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

        // Distinct store failures to dump a raw event for; past a handful it
        // is a stuck loop, not news.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it, even with a long kind-0 content.
        private const val POISON_JSON_CHARS = 4_000
    }
}
