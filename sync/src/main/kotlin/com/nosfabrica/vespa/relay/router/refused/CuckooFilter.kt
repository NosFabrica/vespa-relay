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
package com.nosfabrica.vespa.relay.router.refused

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/** What [CuckooFilter.add] did. */
internal enum class AddResult {
    /** Already there. Idempotent: re-refusing the same id costs no space. */
    PRESENT,
    ADDED,

    /**
     * The table is full — the relocation chain gave up. NOT a soft failure to
     * shrug at: the caller must open a new generation or stop suppressing.
     * See [RefusedIds].
     */
    FULL,
}

/**
 * A fixed-size cuckoo filter over Nostr event ids, mmap'd to one file.
 *
 * Chosen over a Bloom filter for one property above all: **it cannot saturate
 * silently.** A Bloom filter sized for 50M holding 500M keeps answering, with a
 * false-positive rate in the double digits, discarding a sixth of everything and
 * logging nothing. This one refuses the insert instead ([AddResult.FULL]) once
 * the relocation chain gives up around 95% load, which is both the alarm and an
 * exact signal that the partition is done. Its accuracy is also
 * load-INDEPENDENT — `ε ≈ 2b/2^f`, fixed by the fingerprint width — so a
 * half-full table answers exactly as well as a nearly-full one, and
 * over-provisioning costs space and nothing else.
 *
 * With `b = 4` slots and a 32-bit fingerprint, `ε ≈ 8/2^32 ≈ 1.9e-9`: at that
 * rate a 100M-event backfill is expected to lose ~0.2 events to false
 * positives. That number is the whole safety argument, because **a false
 * positive here is silent, permanent data loss** — we skip an event we wanted,
 * nothing logs it, and the same id hits the same bits forever. It is why the
 * fingerprint is 32 bits and not the 8 or 16 a textbook uses, and why the caller
 * still demands two independent refusals before an id is allowed in
 * ([RefusedIds]).
 *
 * **No hash function.** An event id is already a 32-byte cryptographic hash, so
 * the bucket index and the fingerprint are disjoint bit-fields sliced straight
 * out of its hex. There is nothing to choose, tune, or get wrong. Only the
 * alternate-bucket displacement needs mixing, and that is a fixed finalizer.
 *
 * **One writer.** The gate only ever records refusals observed on the sync
 * path, so the sync process is the sole writer and cross-process coordination
 * does not arise. Inside it, [add] is synchronized (relocation moves other
 * fingerprints, so concurrent adds would corrupt each other) while [contains]
 * is deliberately lock-free: the worst a read racing a relocation can see is a
 * fingerprint mid-move, which reports absent, which costs one re-download.
 * Never a wrong suppression.
 */
internal class CuckooFilter private constructor(
    private val buffer: ByteBuffer,
    /** Always a power of two — the xor displacement below depends on it. */
    val buckets: Int,
    initialCount: Int,
) : AutoCloseable {
    @Volatile private var entries: Int = initialCount

    /** How many ids are in the table. */
    val count: Int get() = entries

    /** Slots in use over slots available. The number that says "rotate soon". */
    val load: Double get() = entries.toDouble() / (buckets.toLong() * SLOTS).toDouble()

    private val mask = buckets - 1

    fun contains(id: String): Boolean {
        val fp = fingerprint(id)
        val i1 = bucketOf(id)
        if (slotIndex(i1, fp) >= 0) return true
        return slotIndex(alt(i1, fp), fp) >= 0
    }

    @Synchronized
    fun add(id: String): AddResult {
        val fp = fingerprint(id)
        val i1 = bucketOf(id)
        val i2 = alt(i1, fp)
        if (slotIndex(i1, fp) >= 0 || slotIndex(i2, fp) >= 0) return AddResult.PRESENT

        if (insertInto(i1, fp) || insertInto(i2, fp)) {
            entries++
            writeCount()
            return AddResult.ADDED
        }

        // Both buckets are full: evict a resident and re-home it, repeatedly.
        // Deterministic PRNG seeded from the fingerprint so a failing insert is
        // reproducible in a test rather than "sometimes".
        var rng = (fp.toLong() and 0xFFFFFFFFL) or 1L
        var bucket = if (rng and 1L == 0L) i1 else i2
        var carried = fp
        repeat(MAX_KICKS) {
            rng = nextRandom(rng)
            val slot = ((rng ushr 17) and (SLOTS - 1).toLong()).toInt()
            val evicted = readSlot(bucket, slot)
            writeSlot(bucket, slot, carried)
            carried = evicted
            bucket = alt(bucket, carried)
            if (insertInto(bucket, carried)) {
                entries++
                writeCount()
                return AddResult.ADDED
            }
        }
        // The chain gave up. `carried` is now homeless and has REPLACED a
        // fingerprint that was legitimately stored — so the table has lost one
        // entry, which can only ever cause a false negative (one extra
        // download), never a false positive. Report FULL and let the caller
        // stop feeding this partition.
        return AddResult.FULL
    }

    /**
     * Force every mapped page to disk. Called on close and on the periodic
     * flush; a lost page costs re-downloads, never correctness, so this does
     * not need to be on the insert path.
     */
    fun flush() {
        runCatching { (buffer as? MappedByteBuffer)?.force() }
    }

    override fun close() {
        flush()
    }

    // ---- id → (bucket, fingerprint), with no hashing ------------------------

    /**
     * The low 32 bits of the id's second 8 hex characters, never zero (zero is
     * the empty slot). A non-hex id falls back to the string hash, so a
     * malformed id is still answerable rather than an exception on the ingest
     * path.
     */
    private fun fingerprint(id: String): Int {
        val raw = hexToLong(id, 16, 8)?.toInt() ?: id.hashCode()
        return if (raw == 0) 1 else raw
    }

    private fun bucketOf(id: String): Int {
        val raw = hexToLong(id, 0, 16) ?: id.hashCode().toLong()
        return (raw.toInt() xor (raw ushr 32).toInt()) and mask
    }

    /**
     * The partner bucket. Derived from the fingerprint alone — that is what
     * makes relocation possible without holding the original id — and forced
     * non-zero so an unlucky fingerprint cannot collapse both choices onto one
     * bucket and halve its capacity.
     */
    private fun alt(
        bucket: Int,
        fp: Int,
    ): Int {
        val delta = (mix32(fp) and mask).let { if (it == 0) 1 else it }
        return (bucket xor delta) and mask
    }

    // ---- slots --------------------------------------------------------------

    private fun slotIndex(
        bucket: Int,
        fp: Int,
    ): Int {
        for (s in 0 until SLOTS) if (readSlot(bucket, s) == fp) return s
        return -1
    }

    private fun insertInto(
        bucket: Int,
        fp: Int,
    ): Boolean {
        for (s in 0 until SLOTS) {
            if (readSlot(bucket, s) == 0) {
                writeSlot(bucket, s, fp)
                return true
            }
        }
        return false
    }

    private fun readSlot(
        bucket: Int,
        slot: Int,
    ): Int = buffer.getInt(HEADER_BYTES + ((bucket.toLong() * SLOTS + slot) * 4).toInt())

    private fun writeSlot(
        bucket: Int,
        slot: Int,
        fp: Int,
    ) = buffer.putInt(HEADER_BYTES + ((bucket.toLong() * SLOTS + slot) * 4).toInt(), fp)

    private fun writeCount() = buffer.putInt(OFFSET_COUNT, entries)

    companion object {
        /** Slots per bucket. Four is the cuckoo-filter sweet spot for load factor. */
        const val SLOTS = 4

        /**
         * Relocations before an insert is declared impossible. 500 is the
         * figure from the original paper; past ~95% load it is reached almost
         * immediately, which is what makes FULL a prompt signal rather than a
         * slow degradation.
         */
        private const val MAX_KICKS = 500

        private const val HEADER_BYTES = 32
        private const val OFFSET_COUNT = 16
        private val MAGIC = byteArrayOf('V'.code.toByte(), 'R'.code.toByte(), 'C'.code.toByte(), 'F'.code.toByte(), 1, 0, 0, 0)

        /**
         * Bytes per id at ~95% load: 4 slots × 4 bytes ÷ 0.95. Used to turn a
         * configured capacity into a table size, and quoted in the proposal as
         * ~4.3 B/id at ε = 1e-9.
         */
        fun bucketsFor(capacity: Int): Int {
            val needed = (capacity / (SLOTS * TARGET_LOAD)).toLong().coerceAtLeast(1L)
            var b = 1L
            while (b < needed) b = b shl 1
            return b.coerceAtMost(MAX_BUCKETS.toLong()).toInt()
        }

        /**
         * The largest table a `ByteBuffer` can actually address.
         *
         * At 4 slots × 4 bytes this is 1 GiB plus the header, and the next
         * power of two would be 2 GiB + 32 — past `Int.MAX_VALUE`, where
         * `FileChannel.map` throws "Size exceeds Integer.MAX_VALUE" and
         * `allocateDirect` gets a negative size from the truncating `toInt()`.
         * The slot offsets in [readSlot] overflow at the same point. The cap
         * used to be `1 shl 28`, which promised four times what either could
         * hold and turned a large `SYNC_REFUSED_EPOCH_CAPACITY` into a crash
         * on the first refusal rather than a bigger table.
         *
         * Clamping rather than throwing is deliberate: an oversized capacity
         * is an operator asking for more headroom than one partition can give,
         * and the honest response is the biggest partition we can build plus
         * the SEALED warning when it fills — not a dead router.
         */
        const val MAX_BUCKETS = 1 shl 26

        /**
         * Ids one table of [bucketsFor] buckets holds before the relocation
         * chain starts failing. Worth asking for rather than assuming
         * [bucketsFor]'s input: rounding the bucket count up to a power of two
         * routinely leaves the real ceiling well above the requested capacity
         * (8M asked for, ~15.9M available), and quoting the request in a
         * "table full" warning would understate it by up to 2×.
         */
        fun capacityOf(buckets: Int): Int = (buckets.toLong() * SLOTS * TARGET_LOAD).toInt()

        private const val TARGET_LOAD = 0.95

        /**
         * Map [file], creating it at the size [capacity] implies. An existing
         * file whose header disagrees with the requested geometry is REBUILT
         * rather than reinterpreted: reading a table with the wrong bucket
         * count would scatter every lookup, which is the one way this structure
         * could produce confident wrong answers.
         */
        fun open(
            file: File?,
            capacity: Int,
        ): CuckooFilter {
            val buckets = bucketsFor(capacity)
            val bytes = HEADER_BYTES + buckets.toLong() * SLOTS * 4
            if (file == null) {
                // Memory-only: same geometry, same header, so the two paths
                // cannot drift in behaviour.
                val buf = ByteBuffer.allocateDirect(bytes.toInt())
                writeHeader(buf, buckets)
                return CuckooFilter(buf, buckets, 0)
            }
            file.parentFile?.mkdirs()
            val reusable = file.isFile && file.length() == bytes && headerMatches(file, buckets)
            val raf = RandomAccessFile(file, "rw")
            if (!reusable) {
                raf.setLength(0)
                raf.setLength(bytes)
            }
            val buffer = raf.channel.map(FileChannel.MapMode.READ_WRITE, 0, bytes)
            raf.close()
            var count = 0
            if (reusable) count = buffer.getInt(OFFSET_COUNT) else writeHeader(buffer, buckets)
            return CuckooFilter(buffer, buckets, count)
        }

        private fun writeHeader(
            buf: ByteBuffer,
            buckets: Int,
        ) {
            for (i in MAGIC.indices) buf.put(i, MAGIC[i])
            buf.putInt(8, buckets)
            buf.putInt(12, SLOTS)
            buf.putInt(OFFSET_COUNT, 0)
        }

        private fun headerMatches(
            file: File,
            buckets: Int,
        ): Boolean =
            runCatching {
                RandomAccessFile(file, "r").use { raf ->
                    val head = ByteArray(HEADER_BYTES)
                    raf.readFully(head)
                    val magicOk = MAGIC.indices.all { head[it] == MAGIC[it] }
                    val storedBuckets =
                        ((head[8].toInt() and 0xFF) shl 24) or ((head[9].toInt() and 0xFF) shl 16) or
                            ((head[10].toInt() and 0xFF) shl 8) or (head[11].toInt() and 0xFF)
                    magicOk && storedBuckets == buckets
                }
            }.getOrDefault(false)

        internal fun hexToLong(
            s: String,
            from: Int,
            len: Int,
        ): Long? {
            if (s.length < from + len) return null
            var v = 0L
            for (i in from until from + len) {
                val d =
                    when (val c = s[i]) {
                        in '0'..'9' -> c - '0'
                        in 'a'..'f' -> c - 'a' + 10
                        in 'A'..'F' -> c - 'A' + 10
                        else -> return null
                    }
                v = (v shl 4) or d.toLong()
            }
            return v
        }

        /** murmur3's 32-bit finalizer: cheap, and enough to scatter the displacement. */
        private fun mix32(x: Int): Int {
            var h = x
            h = h xor (h ushr 16)
            h *= -0x7A143595
            h = h xor (h ushr 13)
            h *= -0x3D4D51CB
            h = h xor (h ushr 16)
            return h
        }

        private fun nextRandom(state: Long): Long {
            var x = state
            x = x xor (x shl 13)
            x = x xor (x ushr 7)
            x = x xor (x shl 17)
            return x
        }
    }
}
