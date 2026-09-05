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
package com.nosfabrica.vespa.relay.ingest.refused

import com.nosfabrica.vespa.relay.util.fmtCount
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** What [RefusedIds.record] did with one refusal. */
enum class RecordOutcome {
    /** First refusal seen for this id; downloaded again next time, on purpose. */
    CANDIDATE,

    /** Second independent refusal; the id is suppressed from now on. */
    SUPPRESSED,

    /** Already suppressed. */
    ALREADY,

    /** The partition is sealed; nothing was recorded. */
    REFUSED_FULL,
}

/**
 * The ids this relay has decided it will never store, so a reconcile stops asking for them.
 * A first refusal only makes an id a candidate; it must be refused a second time before it is
 * suppressed. Partitioned by `created_at` epoch, and a window lookup consults every epoch it overlaps.
 */
class RefusedIds(
    private val dir: File?,
    private val epochSeconds: Long = DEFAULT_EPOCH_SECONDS,
    private val capacityPerEpoch: Int = DEFAULT_EPOCH_CAPACITY,
) : AutoCloseable {
    private class Epoch(
        val candidate: CuckooFilter,
        val suppress: CuckooFilter,
    ) {
        /** Set when either table refuses an insert; a sealed epoch answers lookups and takes nothing new. */
        @Volatile var sealedOff: Boolean = false
    }

    private val epochs = ConcurrentHashMap<Long, Epoch>()

    /** Ids suppressed since boot, for the health line. */
    val suppressedHits = AtomicLong()
    val candidatesAdded = AtomicLong()
    val suppressionsAdded = AtomicLong()

    @Volatile private var flusher: Thread? = null

    init {
        // Epochs open lazily on record, so the partitions on disk must be adopted here.
        dir
            ?.listFiles { f -> f.name.startsWith("refused-e") && f.name.endsWith("-supp.cf") }
            ?.forEach { file ->
                file.name
                    .removePrefix("refused-e")
                    .removeSuffix("-supp.cf")
                    .toLongOrNull()
                    ?.let { openEpoch(it) }
            }
        if (epochs.isNotEmpty()) {
            System.err.println("router: refused-ids reopened ${epochs.size} epoch(s) from ${dir?.path}")
        }
    }

    /** Off with no directory. */
    val enabled: Boolean get() = dir != null

    fun epochOf(createdAt: Long): Long = Math.floorDiv(createdAt, epochSeconds)

    /** Is this id twice refused, given the exact `created_at` it carries? */
    fun suppressed(
        id: String,
        createdAt: Long,
    ): Boolean {
        if (!enabled) return false
        val hit = epochs[epochOf(createdAt)]?.suppress?.contains(id) == true
        if (hit) suppressedHits.incrementAndGet()
        return hit
    }

    /** Is this id suppressed, when all we know is the window it was offered in? */
    fun suppressedInWindow(
        id: String,
        since: Long?,
        until: Long?,
    ): Boolean {
        if (!enabled || epochs.isEmpty()) return false
        val lo = epochOf(since ?: 0L)
        val hi = epochOf(until ?: (System.currentTimeMillis() / 1000))
        if (hi < lo) return false
        // Walk the epochs that exist, not lo to hi: an open-ended window starts at epoch 0.
        for ((key, epoch) in epochs) {
            if (key < lo || key > hi) continue
            if (epoch.suppress.contains(id)) {
                suppressedHits.incrementAndGet()
                return true
            }
        }
        return false
    }

    /** One completed download that the store refused. */
    fun record(
        id: String,
        createdAt: Long,
    ): RecordOutcome {
        val epoch = openEpoch(epochOf(createdAt))
        if (epoch.suppress.contains(id)) return RecordOutcome.ALREADY
        if (epoch.candidate.contains(id)) {
            return when (epoch.suppress.add(id)) {
                AddResult.FULL -> {
                    seal(epochOf(createdAt), "suppress")
                }

                else -> {
                    suppressionsAdded.incrementAndGet()
                    RecordOutcome.SUPPRESSED
                }
            }
        }
        if (epoch.sealedOff) return RecordOutcome.REFUSED_FULL
        return when (epoch.candidate.add(id)) {
            AddResult.FULL -> {
                seal(epochOf(createdAt), "candidate")
            }

            else -> {
                candidatesAdded.incrementAndGet()
                RecordOutcome.CANDIDATE
            }
        }
    }

    /** Straight to suppressed, for a relay's own `OK false` in the permanent class. */
    fun suppressNow(
        id: String,
        createdAt: Long,
    ): RecordOutcome {
        val key = epochOf(createdAt)
        val epoch = openEpoch(key)
        if (epoch.suppress.contains(id)) return RecordOutcome.ALREADY
        return when (epoch.suppress.add(id)) {
            AddResult.FULL -> {
                seal(key, "suppress")
            }

            else -> {
                suppressionsAdded.incrementAndGet()
                RecordOutcome.SUPPRESSED
            }
        }
    }

    /** Drop every epoch entirely below [floor], the lowest `created_at` any stream still asks for. */
    fun retireBelow(floor: Long) {
        val cutoff = epochOf(floor)
        epochs.keys.filter { it < cutoff }.forEach { key ->
            epochs.remove(key)?.let {
                it.candidate.close()
                it.suppress.close()
            }
            dir?.let { d ->
                File(d, fileName(key, "cand")).delete()
                File(d, fileName(key, "supp")).delete()
            }
            System.err.println("router: refused-ids retired epoch $key (entirely below created_at $floor)")
        }
    }

    fun startPeriodicFlush(intervalSec: Long = DEFAULT_FLUSH_SECONDS): RefusedIds {
        if (dir == null) return this
        flusher =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(intervalSec * 1000)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                    flush()
                }
            }.apply {
                isDaemon = true
                name = "sync-refused-flush"
                start()
            }
        return this
    }

    fun flush() =
        epochs.values.forEach {
            it.candidate.flush()
            it.suppress.flush()
        }

    override fun close() {
        flusher?.interrupt()
        flush()
    }

    /** What the health line prints. */
    fun summary(): String {
        if (epochs.isEmpty()) return "refused 0 epochs"
        val worst = epochs.values.maxOf { maxOf(it.candidate.load, it.suppress.load) }
        val suppressing = epochs.values.sumOf { it.suppress.count }
        // The candidate count tells a gate in its first cycles apart from an inert one.
        val candidates = epochs.values.sumOf { it.candidate.count }
        val sealedCount = epochs.values.count { it.sealedOff }
        return "refused ${epochs.size} epoch(s), ${fmtCount(candidates)} candidate(s), " +
            "${fmtCount(suppressing)} suppressing, peak load ${(worst * 100).toInt()}%" +
            (if (sealedCount > 0) ", $sealedCount SEALED" else "")
    }

    private fun seal(
        key: Long,
        which: String,
    ): RecordOutcome {
        val epoch = epochs[key] ?: return RecordOutcome.REFUSED_FULL
        if (!epoch.sealedOff) {
            epoch.sealedOff = true
            // The table's real ceiling, not the configured request, which is not the binding number.
            val held = CuckooFilter.capacityOf(epoch.suppress.buckets)
            System.err.println(
                "router: refused-ids epoch $key SEALED — its $which table would not take another id " +
                    "(held ~${fmtCount(held)}, configured ${fmtCount(capacityPerEpoch)}). Suppression keeps working " +
                    "for what it already holds; nothing new is recorded there. Raise SYNC_REFUSED_EPOCH_CAPACITY " +
                    "or shorten SYNC_REFUSED_EPOCH_SECONDS.",
            )
        }
        return RecordOutcome.REFUSED_FULL
    }

    private fun openEpoch(key: Long): Epoch =
        epochs.computeIfAbsent(key) {
            Epoch(
                CuckooFilter.open(dir?.let { d -> File(d, fileName(key, "cand")) }, capacityPerEpoch),
                CuckooFilter.open(dir?.let { d -> File(d, fileName(key, "supp")) }, capacityPerEpoch),
            )
        }

    private fun fileName(
        key: Long,
        which: String,
    ) = "refused-e$key-$which.cf"

    companion object {
        const val DEFAULT_EPOCH_SECONDS = 90L * 24 * 60 * 60

        /** Ids one epoch's table is sized for; the tables live in page cache, not heap. */
        const val DEFAULT_EPOCH_CAPACITY = 8_000_000

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /** Answers "no" to everything and records nothing. */
        fun disabled(): RefusedIds = RefusedIds(null, DEFAULT_EPOCH_SECONDS, 1024)

        /** `SYNC_REFUSED_DIR` is where the per-epoch filters live; unset is off. */
        fun fromEnv(env: Map<String, String>): RefusedIds {
            val dir = env["SYNC_REFUSED_DIR"]?.trim()?.takeIf { it.isNotEmpty() }?.let(::File)
            val epoch =
                env["SYNC_REFUSED_EPOCH_SECONDS"]?.trim()?.toLongOrNull()?.takeIf { it > 0 }
                    ?: DEFAULT_EPOCH_SECONDS
            val capacity =
                env["SYNC_REFUSED_EPOCH_CAPACITY"]?.trim()?.toIntOrNull()?.takeIf { it > 0 }
                    ?: DEFAULT_EPOCH_CAPACITY
            if (dir == null) {
                System.err.println("router: SYNC_REFUSED_DIR unset — refused-id suppression is off")
                return disabled()
            }
            System.err.println(
                "router: refused-id suppression on at ${dir.path} " +
                    "(epoch ${epoch}s, capacity ${fmtCount(capacity)}/epoch)",
            )
            return RefusedIds(dir, epoch, capacity).startPeriodicFlush()
        }
    }
}
