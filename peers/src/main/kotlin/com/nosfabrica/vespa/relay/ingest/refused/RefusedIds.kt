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

    /** The partition is full and sealed. Nothing was recorded — see [RefusedIds]. */
    REFUSED_FULL,
}

/**
 * The ids this relay has decided it will never store, so a reconcile stops
 * asking for them.
 *
 * **Two filters, not one, and that is the safety property.** A first refusal
 * only makes an id a *candidate*; it is downloaded again next cycle and must be
 * refused a SECOND time before it is suppressed. Only the suppress filter's own
 * ε can lose data, and every id in it is backed by a store refusal that really
 * happened. It also means a `needIds` id that reappears because a fetch died
 * mid-transfer can never suppress anything — a sighting is a completed download
 * plus a store refusal, never a mere appearance in a diff.
 *
 * Be exact about what a false positive in the CANDIDATE filter does, because
 * the obvious phrasing is wrong: it does not cost a wasted download, it
 * promotes the id to the suppress filter on its FIRST refusal instead of its
 * second ([record] reads `candidate.contains` and goes straight to
 * `suppress.add`). That is still safe, but for a different reason than the
 * gate — the id it fast-tracks is one the store has already refused, so no
 * wanted event is lost, only a cycle of patience. What the gate actually buys
 * is that the suppress filter is never populated from a bare *sighting*, and
 * at ε ≈ 1.9e-9 the fast-track is expected ~0.2 times per 100M refusals.
 *
 * **Partitioned by `created_at` epoch**, because the set being remembered grows
 * monotonically and forever while any fixed structure does not. An epoch whose
 * whole range sits below the lowest `since` any stream asks for can be dropped
 * outright — nothing will ever generate a `needId` there, so retirement is
 * *exact* rather than the lossy guess a timer-based rotation makes. Growth is
 * a handful of files a year.
 *
 * The one boundary to respect: insertion keys on the event's exact
 * `created_at`, lookup from a sweep keys on the window, and windows do not
 * respect epoch boundaries. [suppressedInWindow] therefore consults every epoch
 * the window overlaps. Get that wrong and suppression silently stops working
 * near every boundary.
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
        /**
         * Set when either table refuses an insert. A sealed epoch answers
         * lookups from what it already holds and accepts nothing new — the
         * alternative, continuing to insert into a table that cannot take it,
         * is the silent failure this whole design exists to avoid.
         */
        @Volatile var sealedOff: Boolean = false
    }

    private val epochs = ConcurrentHashMap<Long, Epoch>()

    /** Ids suppressed since boot, for the health line. */
    val suppressedHits = AtomicLong()
    val candidatesAdded = AtomicLong()
    val suppressionsAdded = AtomicLong()

    @Volatile private var flusher: Thread? = null

    init {
        // Epochs are opened lazily by [record], which is fine while the process
        // runs and WRONG the moment it restarts: nothing has recorded yet, so
        // every map lookup misses and suppression quietly does nothing until a
        // fresh refusal happens to reopen that partition. Exactly the silent
        // no-op this whole mechanism exists to avoid, so the partitions already
        // on disk are adopted at construction.
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

    /**
     * Whether this instance records anything at all. Off with no directory:
     * the whole mechanism is opt-in, and a router that has not been given
     * somewhere to keep its filters behaves exactly as it did before they
     * existed rather than keeping a heap-only set nobody asked for.
     */
    val enabled: Boolean get() = dir != null

    fun epochOf(createdAt: Long): Long = Math.floorDiv(createdAt, epochSeconds)

    /**
     * Is this id one we have twice refused, given the exact time it carries?
     * The ingest path knows `created_at` outright, so it never has to guess.
     */
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
     * Consults every epoch the window touches — a sweep window is an arbitrary
     * `created_at` range and does not line up with epoch edges.
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
        // Walk the epochs that EXIST and keep the ones the window covers,
        // rather than counting from `lo` to `hi` and probing each number.
        // An open-ended window (`since = null`, which is the ordinary case for
        // a `deleteMissing` ask) starts at epoch 0, so the counting version
        // probed every epoch index since 1970 for every id: measured at 0.57 ms
        // per call against 0.0007 ms here — a 780x difference paid once per id
        // in `diff.needIds`, which is thousands of ids per relay per sweep.
        for ((key, epoch) in epochs) {
            if (key < lo || key > hi) continue
            if (epoch.suppress.contains(id)) {
                suppressedHits.incrementAndGet()
                return true
            }
        }
        return false
    }

    /**
     * One completed download that the store refused. First time makes a
     * candidate; second time suppresses.
     */
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
     * Straight to suppressed, no candidate stage — for the one signal that is
     * certain on its own: the relay answering `OK false` in the permanent class
     * (auth-required, restricted, blocked). It has told us it will never take
     * our repair, so waiting for a second refusal buys nothing.
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
     * Drop every epoch entirely below [floor] — the lowest `created_at` any
     * configured stream still asks for. Correct rather than approximate:
     * nothing will ever ask inside a retired range, so nothing can be wrongly
     * un-suppressed by dropping it.
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

    /**
     * What the health line prints. The load figures are the ones that matter:
     * a partition approaching 1.0 is one that is about to stop accepting, and
     * an operator who cannot see that coming finds out by the fix quietly
     * ceasing to work.
     */
    fun summary(): String {
        if (epochs.isEmpty()) return "refused 0 epochs"
        val worst = epochs.values.maxOf { maxOf(it.candidate.load, it.suppress.load) }
        val suppressing = epochs.values.sumOf { it.suppress.count }
        // The candidate count is not decoration. Suppression only begins on an
        // id's SECOND refusal, so a healthy gate spends its first cycles with
        // nothing suppressed at all — and without this number that is
        // indistinguishable from the whole mechanism being inert. Measured on a
        // live three-cycle run that ended 23 candidates / 0 suppressing, where
        // the health line alone could not tell the two apart.
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
            // The table's REAL ceiling, not the configured request. Bucket
            // counts round up to a power of two, so a partition asked for 8M
            // ids actually seals near 15.9M — quoting the request would tell
            // an operator to raise a number that was never the binding one.
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
        /**
         * One partition per quarter. Long enough that the file count stays in
         * single digits per year, short enough that retiring one actually frees
         * something when a stream's `since` moves up.
         */
        const val DEFAULT_EPOCH_SECONDS = 90L * 24 * 60 * 60

        /**
         * Ids one epoch's table is sized for.
         *
         * MEASURED, not derived: 8M rounds the bucket count up to 2^22, which
         * is **64 MiB per table and 128 MiB per epoch** with the candidate
         * filter beside it — 8.4 B/id, not the ~4.3 B/id the structure costs
         * at a bucket count that happens to land near a power of two. The
         * rounding is why: 8M needs 2,105,263 buckets and gets 4,194,304, so
         * nearly half the table is headroom. That headroom is not waste —
         * accuracy here is load-independent, so the extra slots simply push
         * the seal out to ~15.9M ids — but it must be budgeted for, because
         * the earlier figure on this line understated the disk and page cache
         * an operator needs by a factor of two.
         *
         * Page cache, not heap, which is the whole reason this is mmap'd
         * rather than a HashMap. Step 0's measurement is what should replace
         * this default.
         */
        const val DEFAULT_EPOCH_CAPACITY = 8_000_000

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /** Disabled: answers "no" to everything and records nothing. */
        fun disabled(): RefusedIds = RefusedIds(null, DEFAULT_EPOCH_SECONDS, 1024)

        /**
         * `SYNC_REFUSED_DIR` — where the per-epoch filters live. **Unset is
         * off**, and off means the router behaves exactly as it did before
         * this existed: nothing is remembered and nothing is suppressed.
         * `SYNC_REFUSED_EPOCH_SECONDS` and `SYNC_REFUSED_EPOCH_CAPACITY` size
         * the partitions; the capacity is the number Step 0's measurement is
         * meant to replace, and getting it too LOW is loud (the epoch seals and
         * says so) rather than silent.
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
