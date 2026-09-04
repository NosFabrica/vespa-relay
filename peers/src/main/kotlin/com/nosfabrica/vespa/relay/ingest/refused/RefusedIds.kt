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
    /** First refusal seen for this id. Downloaded again next time, on purpose. */
    CANDIDATE,

    /** Second independent refusal. From now on the id is suppressed. */
    SUPPRESSED,

    /** Already suppressed; nothing to do. */
    ALREADY,

    /** The partition is full and sealed. Nothing was recorded. */
    REFUSED_FULL,
}

/**
 * The ids this relay has decided it will never store, so a reconcile stops
 * asking for them. Two filters per epoch: a first refusal only makes an id
 * a candidate, and it must be refused a second time before it is suppressed.
 * A sighting is a completed download plus a store refusal, never an
 * appearance in a diff.
 *
 * Partitioned by `created_at` epoch so an epoch entirely below the lowest
 * `since` any stream asks for can be dropped exactly. Insertion keys on the
 * event's own `created_at`; a window lookup must consult every epoch the
 * window overlaps, since windows do not respect epoch edges.
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
        /** Set when either table refuses an insert; a sealed epoch answers lookups and accepts nothing new. */
        @Volatile var sealedOff: Boolean = false
    }

    private val epochs = ConcurrentHashMap<Long, Epoch>()

    /** Ids suppressed since boot, for the health line. */
    val suppressedHits = AtomicLong()
    val candidatesAdded = AtomicLong()
    val suppressionsAdded = AtomicLong()

    @Volatile private var flusher: Thread? = null

    init {
        // Epochs open lazily on record, so after a restart the partitions on
        // disk must be adopted here or suppression is silent until a fresh refusal.
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

    /** Whether this instance records anything at all. Off with no directory. */
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

    /**
     * Is this id suppressed, when all we know is the window it was offered in?
     * Consults every epoch the window touches.
     */
    fun suppressedInWindow(
        id: String,
        since: Long?,
        until: Long?,
    ): Boolean {
        if (!enabled || epochs.isEmpty()) return false
        val lo = epochOf(since ?: 0L)
        val hi = epochOf(until ?: (System.currentTimeMillis() / 1000))
        if (hi < lo) return false
        // Walk the epochs that exist rather than counting from lo to hi: an
        // open-ended window starts at epoch 0.
        for ((key, epoch) in epochs) {
            if (key < lo || key > hi) continue
            if (epoch.suppress.contains(id)) {
                suppressedHits.incrementAndGet()
                return true
            }
        }
        return false
    }

    /** One completed download that the store refused. First time makes a candidate; second time suppresses. */
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

    /**
     * Straight to suppressed, no candidate stage, for a relay's own `OK false`
     * in the permanent class (auth-required, restricted, blocked).
     */
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

    /**
     * Drop every epoch entirely below [floor], the lowest `created_at` any
     * configured stream still asks for. Nothing can ask inside a retired range.
     */
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

    /** What the health line prints. A partition's load approaching 1.0 is about to seal. */
    fun summary(): String {
        if (epochs.isEmpty()) return "refused 0 epochs"
        val worst = epochs.values.maxOf { maxOf(it.candidate.load, it.suppress.load) }
        val suppressing = epochs.values.sumOf { it.suppress.count }
        // A healthy gate spends its first cycles with nothing suppressed; the
        // candidate count is what tells that apart from an inert mechanism.
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
            // Report the table's real ceiling: bucket counts round up to a
            // power of two, so the configured request is not the binding number.
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
        /** One partition per quarter: few files a year, and retiring one frees something. */
        const val DEFAULT_EPOCH_SECONDS = 90L * 24 * 60 * 60

        /**
         * Ids one epoch's table is sized for. The bucket count rounds up to a
         * power of two, so the tables are larger than this number suggests;
         * they live in page cache, not heap.
         */
        const val DEFAULT_EPOCH_CAPACITY = 8_000_000

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /** Disabled: answers "no" to everything and records nothing. */
        fun disabled(): RefusedIds = RefusedIds(null, DEFAULT_EPOCH_SECONDS, 1024)

        /**
         * `SYNC_REFUSED_DIR` is where the per-epoch filters live; unset is off.
         * `SYNC_REFUSED_EPOCH_SECONDS` and `SYNC_REFUSED_EPOCH_CAPACITY` size
         * the partitions; a capacity set too low seals loudly rather than silently.
         */
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
