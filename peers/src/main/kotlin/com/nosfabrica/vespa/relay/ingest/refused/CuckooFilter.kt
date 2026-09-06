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

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/** What [CuckooFilter.add] did. */
internal enum class AddResult {
    /** Already there; re-refusing the same id costs no space. */
    PRESENT,
    ADDED,

    /** The relocation chain gave up. The caller must open a new generation or stop suppressing. */
    FULL,
}

/**
 * A fixed-size cuckoo filter over Nostr event ids, mmap'd to one file. It refuses an insert
 * ([AddResult.FULL]) rather than saturating silently; a false positive is a wanted event
 * skipped forever. [contains] is lock-free: a fingerprint mid-move reports absent, never present.
 */
internal class CuckooFilter private constructor(
    private val buffer: ByteBuffer,
    /** Always a power of two; the xor displacement depends on it. */
    val buckets: Int,
    initialCount: Int,
) : AutoCloseable {
    @Volatile private var entries: Int = initialCount

    /** How many ids are in the table. */
    val count: Int get() = entries

    /** Slots in use over slots available. */
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

        // The PRNG is seeded from the fingerprint so a failing insert is reproducible.
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
        // `carried` is homeless: the table lost one entry, a false negative and never a false positive.
        return AddResult.FULL
    }

    /** Force every mapped page to disk. */
    fun flush() {
        runCatching { (buffer as? MappedByteBuffer)?.force() }
    }

    override fun close() {
        flush()
    }

    /** The id's second 8 hex characters, never zero (zero is the empty slot). */
    private fun fingerprint(id: String): Int {
        val raw = hexToLong(id, 16, 8)?.toInt() ?: id.hashCode()
        return if (raw == 0) 1 else raw
    }

    private fun bucketOf(id: String): Int {
        val raw = hexToLong(id, 0, 16) ?: id.hashCode().toLong()
        return (raw.toInt() xor (raw ushr 32).toInt()) and mask
    }

    /** The partner bucket, from the fingerprint alone so relocation needs no id; never the same bucket. */
    private fun alt(
        bucket: Int,
        fp: Int,
    ): Int {
        val delta = (mix32(fp) and mask).let { if (it == 0) 1 else it }
        return (bucket xor delta) and mask
    }

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
        const val SLOTS = 4

        private const val MAX_KICKS = 500

        private const val HEADER_BYTES = 32
        private const val OFFSET_COUNT = 16
        private val MAGIC = byteArrayOf('V'.code.toByte(), 'R'.code.toByte(), 'C'.code.toByte(), 'F'.code.toByte(), 1, 0, 0, 0)

        /** Buckets for [capacity] ids at [TARGET_LOAD], rounded up to a power of two and clamped to [MAX_BUCKETS]. */
        fun bucketsFor(capacity: Int): Int {
            val needed = (capacity / (SLOTS * TARGET_LOAD)).toLong().coerceAtLeast(1L)
            var b = 1L
            while (b < needed) b = b shl 1
            return b.coerceAtMost(MAX_BUCKETS.toLong()).toInt()
        }

        /** The largest table a `ByteBuffer` can address; the next power of two overflows the slot offsets. */
        const val MAX_BUCKETS = 1 shl 26

        /** Ids a table of [buckets] holds before relocation starts failing. */
        fun capacityOf(buckets: Int): Int = (buckets.toLong() * SLOTS * TARGET_LOAD).toInt()

        private const val TARGET_LOAD = 0.95

        /** Map [file], creating it at the size [capacity] implies. A header of another geometry is rebuilt. */
        fun open(
            file: File?,
            capacity: Int,
        ): CuckooFilter {
            val buckets = bucketsFor(capacity)
            val bytes = HEADER_BYTES + buckets.toLong() * SLOTS * 4
            if (file == null) {
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

        /** murmur3's 32-bit finalizer. */
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
