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
import com.nosfabrica.vespa.relay.util.strictInt
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.crypto.verifyId
import com.vitorpamplona.quartz.nip01Core.crypto.verifySignature
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

/**
 * The download-to-store pipeline every mirrored event funnels through: a bounded channel and
 * a pool of workers draining it in batches through [IEventStore.batchInsert]. Only a verified
 * event may shadow another, in batch or in the store. A full channel suspends [submit].
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
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
        val origin: IngestOrigin,
    )

    private val workers = tuning.concurrency
    private val configuredBatch = tuning.batch

    /** How many downloaded events may wait for ingest. */
    val capacity = (tuning.batch * 4).coerceIn(4_096, MAX_INBOUND_QUEUE)

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

    val accepted = AtomicLong()
    val rejected = AtomicLong()

    /** Events handed to the queue since boot, the arrival side of [accepted] + [rejected]. */
    val submitted = AtomicLong()

    /** Good events the store refused for structural reasons, which nothing will re-offer. */
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
    ) {
        // Checked after the caller's SyncCoverage.observe, so the leg keeps its per-kind evidence.
        if (refusals.isSuppressed(event)) {
            suppressed.incrementAndGet()
            return
        }
        // Counted before the send and taken back if it fails, or a fast worker decrements first.
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
            // Shutdown raced this event in.
        } finally {
            // `send` also throws CancellationException on shutdown.
            if (!handedOff) queued.decrementAndGet()
        }
    }

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
            queued.decrementAndGet()
            batch.clear()
            batch.add(first)
            while (batch.size < batchSize) {
                val next = inbound.tryReceive().getOrNull() ?: break
                queued.decrementAndGet()
                batch.add(next)
            }
            // Around the whole pass: the only record that a worker is inside a store round trip.
            busySince.set(worker, System.currentTimeMillis())
            // Events of this pass no counter has taken yet; what a fault loses.
            var untallied = batch.size
            try {
                val valid = admit(batch)
                untallied = valid.size
                if (valid.isEmpty()) continue
                // Before the batch write, where a report raised in parallel has no single event.
                audit?.let { for (msg in valid) it.inspect(msg.event) }
                val origins =
                    if (refusals.tracksOrigins) valid.associateTo(HashMap(valid.size)) { it.event.id to it.origin } else emptyMap()
                // The write tallies every event it is handed, through its own callbacks.
                untallied = 0
                insertIsolating(valid.map { it.event }, origins)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                lostToFault(untallied, e)
            } catch (e: StackOverflowError) {
                // A pathological event costs its batch, never the worker; other Errors still end it.
                lostToFault(untallied, e)
            } finally {
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
            rejected.addAndGet(untallied.toLong())
            noteRejection("ingest fault: ${e.javaClass.simpleName}", untallied.toLong())
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
    private suspend fun admit(batch: List<Inbound>): List<Inbound> {
        val fresh = dropDuplicates(batch)
        val hashStartedNs = System.nanoTime()
        val (placed, forged) = ledByContent(batch, fresh)
        // Booked to `verify` without a call of its own, since it is the id half of the check below.
        IngestStats.add("verify", System.nanoTime() - hashStartedNs)
        tallyBadSignatures(forged)
        val contests = dropSuperseded(batch, placed)
        if (contests.isEmpty()) return emptyList()
        val winners = ArrayList<Int>(contests.size)
        val superseded = ArrayList<List<Int>>()
        var badSigs = 0
        var duplicates = 0
        IngestStats.timed("verify") {
            for (versions in contests) {
                var won = false
                for (copies in versions) {
                    if (won) {
                        superseded.add(copies)
                        continue
                    }
                    for ((n, i) in copies.withIndex()) {
                        val msg = batch[i]
                        when {
                            won -> {
                                duplicates++
                            }

                            // The leader's id is already checked against its content; the rest are not.
                            msg.skipVerify ||
                                runCatching { if (n == 0) msg.event.verifySignature() else msg.event.verify() }.getOrDefault(false) -> {
                                winners.add(i)
                                won = true
                            }

                            else -> {
                                badSigs++
                            }
                        }
                    }
                }
            }
        }
        tallyBadSignatures(badSigs)
        if (duplicates > 0) {
            rejected.addAndGet(duplicates.toLong())
            noteRejection(RejectionReason.DUPLICATE.take(48), duplicates.toLong())
        }
        if (superseded.isNotEmpty()) reportSuperseded(batch, superseded)
        winners.sort()
        return winners.map { batch[it] }
    }

    private fun tallyBadSignatures(count: Int) {
        if (count == 0) return
        rejected.addAndGet(count.toLong())
        badSignatures.addAndGet(count.toLong())
    }

    /**
     * The batch's copies of each event, as batch indices by arrival (ephemeral kinds each their
     * own), minus every id the store already holds via [knownIds] when the batch is wide enough
     * to pay for the round trip. Safe on an unverified id: a dropped event is never stored.
     */
    private suspend fun dropDuplicates(batch: List<Inbound>): List<List<Int>> {
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
        val probed = probe != null && verifiable >= PROBE_MIN_VERIFIABLE && idGate.worthIt()
        val stored =
            if (!probed) {
                emptySet()
            } else {
                try {
                    // One store call round the whole probe: the worker is suspended in the fan-out.
                    storeCall(StoreCalls.CALLER_INGEST_DEDUP, StoreCalls.OP_EXISTING_IDS, StoreCalls.ids(copies.size)) {
                        IngestStats.timed("dedup.pre") {
                            copies
                                .map { batch[it[0]].event.id }
                                .chunked(DEDUP_CHUNK)
                                .mapBounded(QUERY_FANOUT) { probe(it) }
                                .flatMapTo(HashSet()) { it }
                        }
                    }
                } catch (e: CancellationException) {
                    // Swallowed, a shutdown cancellation would write into a closing store.
                    throw e
                } catch (_: Throwable) {
                    // A failed probe costs time, never correctness.
                    emptySet()
                }
            }

        val fresh = if (stored.isEmpty()) copies else copies.filter { batch[it[0]].event.id !in stored }
        // Only the probe teaches the gate.
        if (probed) idGate.record(copies.size, copies.size - fresh.size)
        val dropped = copies.sumOf { it.size } - fresh.sumOf { it.size }
        if (dropped > 0) {
            rejected.addAndGet(dropped.toLong())
            // The store's own word, so duplicates dropped here and there tally on one line.
            noteRejection(RejectionReason.DUPLICATE.take(48), dropped.toLong())
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
    ): Pair<List<List<Int>>, Int> {
        var forged = 0
        val led = ArrayList<List<Int>>(copies.size)
        for (c in copies) {
            val lead = c.indexOfFirst { batch[it].skipVerify || runCatching { batch[it].event.verifyId() }.getOrDefault(false) }
            if (lead < 0) {
                forged += c.size
                continue
            }
            forged += lead
            led.add(if (lead == 0) c else c.subList(lead, c.size))
        }
        return led to forged
    }

    /** Write a batch through the store's bulk path; if it throws, bisect so one bad event does not cost the batch. */
    private suspend fun insertIsolating(
        events: List<Event>,
        origins: Map<String, IngestOrigin>,
    ) {
        // Outcomes of the current write not yet in a counter; the bisection writes one half at a time.
        var owed = 0
        insertBisecting(
            events = events,
            // Booked apart from the probes: this waits on the writer mutex, they wait on the query path.
            write = { batch -> storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(batch.size)) { store.batchInsert(batch) } },
            onOutcomes = { written, outcomes ->
                owed = outcomes.size
                // A misattributed rejection would suppress a wanted id, so only attribution is withheld.
                val aligned = outcomes.size == written.size
                if (!aligned) reportMisalignment(written.size, outcomes.size)
                for ((i, outcome) in outcomes.withIndex()) {
                    // Ahead of the branch, whose first statement counts the outcome.
                    owed--
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> {
                            accepted.incrementAndGet()
                        }

                        is IEventStore.InsertOutcome.Rejected -> {
                            rejected.incrementAndGet()
                            noteRejection(outcome.reason.take(48), 1L)
                            // Only the Rejected branch reports: a Failed outcome is the store's fault.
                            if (aligned) {
                                written.getOrNull(i)?.let { event ->
                                    reportRefusal(event, origins[event.id] ?: IngestOrigin.Local, outcome.reason)
                                }
                            }
                        }

                        is IEventStore.InsertOutcome.Failed -> {
                            rejected.incrementAndGet()
                            noteRejection("store failed: ${outcome.reason.take(40)}", 1L)
                            lostToStore.incrementAndGet()
                        }
                    }
                }
            },
            onOutcomesFailed = { _, e ->
                if (owed > 0) {
                    rejected.addAndGet(owed.toLong())
                    noteRejection("ingest fault: ${e.javaClass.simpleName}", owed.toLong())
                }
                reportBookkeepingFailure("outcome bookkeeping", e, "$owed uncounted outcome(s) tallied as lost, their refusal records lost")
            },
            onPoison = { event, e ->
                rejected.incrementAndGet()
                noteRejection("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L)
                reportPoison(event, e)
            },
            onGaveUp = { batch, e ->
                // Tallied apart from the isolated ones: "could not say which" is not "this event is bad".
                rejected.addAndGet(batch.size.toLong())
                noteRejection("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong())
                lostToStore.addAndGet(batch.size.toLong())
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
        if (probe == null || byAddress.size < PROBE_MIN_VERIFIABLE || !versionGate.worthIt()) return contests
        val stored = readNewestVersions(probe, byAddress.keys)
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
        if (beaten.isNotEmpty()) reportSuperseded(batch, beaten)
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
    ) {
        val dropped = versions.sumOf { it.size }.toLong()
        noteRejection(RejectionReason.REPLACED.take(48), dropped)
        rejected.addAndGet(dropped)
        for (copies in versions) {
            val msg = batch[copies[0]]
            // A trusted leader skipped the id check every other leader passed.
            if (msg.skipVerify && !runCatching { msg.event.verifyId() }.getOrDefault(false)) continue
            reportRefusal(msg.event, msg.origin, RejectionReason.REPLACED)
        }
    }

    /** One query per kind per chunk of authors. */
    private suspend fun readNewestVersions(
        probe: suspend (Int, List<String>) -> Map<String, AddressVersion>,
        addresses: Set<Pair<Int, String>>,
    ): Map<Pair<Int, String>, AddressVersion> =
        try {
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
            emptyMap()
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

    companion object {
        const val REJECTION_ROWS = 4

        private const val MAX_INBOUND_QUEUE = 16_384

        /** Events a batch must expect to verify before the dedup probe is worth its round trip. */
        private const val PROBE_MIN_VERIFIABLE = 128

        /** Authors per version query. */
        private const val CHECK_CHUNK = 500

        /** Ids per probe query, read from the store's own knob so a widened stage B is matched here. */
        private val DEDUP_CHUNK: Int = System.getenv().strictInt("VESPA_DEDUP_CHUNK", 1..Int.MAX_VALUE) ?: 500

        /** Distinct rejection reasons kept before [noteRejection] folds the rest into one. */
        private const val REASON_LIMIT = 64

        private const val OVERFLOW_REASON = "other store failures"

        private const val POISON_SAMPLE_LIMIT = 20

        private const val POISON_JSON_CHARS = 4_000

        /** A false `wedged` is worse than a late one. Nothing here stops the pass. */
        const val WEDGE_AFTER_MS = 600_000L
    }
}

/** The newest stored version of one address, which an arriving replaceable has to beat. */
data class AddressVersion(
    val createdAt: Long,
    val id: String,
) {
    /** Strictly: an equal stamp and id is the same event. */
    fun beats(event: Event): Boolean = createdAt > event.createdAt || (createdAt == event.createdAt && id < event.id)
}

/** `ingestConcurrency` and `ingestBatch` from `sync.conf`. A type so a caller cannot swap them. */
data class IngestTuning(
    val concurrency: Int,
    val batch: Int,
)
