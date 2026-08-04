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
import com.nosfabrica.vespa.relay.server.ServingPressure
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
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
 * [IEventStore.batchInsert], and signature verification off the download
 * threads (skipped for trusted upstreams).
 *
 * The channel is bounded so a fast download (negentropy can deliver >10k/s)
 * cannot outrun Vespa ingest and pile events onto the heap: when it fills,
 * [submit] blocks the producing thread and the upstream throttles to the
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
     * [submit] parks its caller when the channel is full — deliberate
     * backpressure — but on a shared dispatcher enough parked producers starve
     * the very workers that must make room, and that is a deadlock (observed:
     * `queue 8000/8000 FULL, 0 ev/s` permanently, workers parked in
     * trySendBlocking). A dedicated pool makes the invariant true rather than
     * probable: however many producers park, these threads still drain.
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
     * Hand an event to the pool, blocking the caller if the buffer is full.
     * The subscription callbacks that call this are not suspending, so a
     * blocking send is how backpressure reaches the download.
     */
    fun submit(
        event: Event,
        skipVerify: Boolean,
    ) {
        // Counted only when the channel actually took it: during shutdown
        // (closeIntake) the send fails, and counting a dropped event would
        // leave a phantom queue depth on the final health line.
        if (inbound.trySendBlocking(Inbound(event, skipVerify)).isSuccess) {
            queued.incrementAndGet()
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
            var verifyRejected = 0
            for (msg in batch) {
                if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                    valid.add(msg.event)
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
            insertBatch(valid)
        }
    }

    /**
     * Write a batch through the store's bulk path. Per-row attribution is the
     * store's contract now ([IEventStore.batchInsert]): `Rejected` is the
     * event's fault and final, `Failed` is the store's — the event was good
     * and is lost, so it is counted loudly and reported with its raw JSON. A
     * THROW is environmental (engine unreachable, transaction never started):
     * no per-event answer exists, so the batch is tallied lost unisolated.
     * This replaced a bisecting re-try dance the contract made unnecessary.
     */
    private suspend fun insertBatch(events: List<Event>) {
        val outcomes =
            try {
                store.batchInsert(events)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                rejected.addAndGet(events.size.toLong())
                rejectReasons.merge("store ${e.javaClass.simpleName} (batch, unisolated)", events.size.toLong(), Long::plus)
                // LOST, not merely rejected: the events were good, the
                // failure is the store's, and nothing re-offers them.
                lostToStore.addAndGet(events.size.toLong())
                return
            }
        outcomes.forEachIndexed { i, outcome ->
            when (outcome) {
                is IEventStore.InsertOutcome.Accepted -> {
                    accepted.incrementAndGet()
                }

                is IEventStore.InsertOutcome.Rejected -> {
                    rejected.incrementAndGet()
                    rejectReasons.merge(outcome.reason.take(48), 1L, Long::plus)
                }

                is IEventStore.InsertOutcome.Failed -> {
                    // The store's fault, attributed per row: the event was
                    // good and is lost — nothing re-offers it. lostToStore
                    // keeps the loss loud on the health line instead of
                    // blending into the duplicates.
                    rejected.incrementAndGet()
                    rejectReasons.merge("store failed: ${outcome.reason.take(40)}", 1L, Long::plus)
                    lostToStore.incrementAndGet()
                    events.getOrNull(i)?.let { reportPoison(it, outcome.reason) }
                }
            }
        }
    }

    /**
     * Log an event the store failed on, once per distinct failure, with the
     * raw JSON — the per-row Failed has no other trace, and without the
     * raw event the defect cannot be reproduced.
     */
    private fun reportPoison(
        event: Event,
        reason: String,
    ) {
        // Size checked BEFORE add: store errors embed per-event content in
        // their messages (a Vespa 400 quotes the document), so past the print
        // limit the set must stop growing too — 2.3M distinct rejections in
        // one schema-drift run would otherwise be 2.3M retained strings.
        if (poisonSeen.size >= POISON_SAMPLE_LIMIT) return
        if (!poisonSeen.add(reason)) return
        System.err.println(
            "router: store failed on event ${event.id} (kind ${event.kind}, pubkey ${event.pubKey}) — $reason\n" +
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

        // Distinct store failures to dump a raw event for; past a handful it
        // is a stuck loop, not news.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it, even with a long kind-0 content.
        private const val POISON_JSON_CHARS = 4_000
    }
}
