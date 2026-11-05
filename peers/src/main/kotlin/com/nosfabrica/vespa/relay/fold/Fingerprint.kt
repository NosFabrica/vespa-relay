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
package com.nosfabrica.vespa.relay.fold

import kotlin.random.Random

/**
 * One url's window as the set of event ids in it, each 64-hex id held as its four 64-bit words,
 * sorted, so two windows intersect in one merge with no allocation. Exact: an id that is not 64
 * lowercase hex is kept as written, so [shared] is the intersection of the id sets, never an estimate.
 */
class Fingerprint private constructor(
    /** [WORDS] per id, big-endian, ids in ascending unsigned order. */
    private val words: LongArray,
    /** The ids that are not 64 lowercase hex. */
    private val odd: Set<String>,
) {
    /** How many distinct ids the window holds. */
    val size: Int get() = words.size / WORDS + odd.size

    fun isEmpty(): Boolean = size == 0

    /** How many ids this window and [other] have in common. */
    fun shared(other: Fingerprint): Int {
        val a = words
        val b = other.words
        var i = 0
        var j = 0
        var n = 0
        while (i < a.size && j < b.size) {
            val c = compareAt(a, i, b, j)
            when {
                c == 0 -> {
                    n++
                    i += WORDS
                    j += WORDS
                }

                c < 0 -> {
                    i += WORDS
                }

                else -> {
                    j += WORDS
                }
            }
        }
        if (odd.isNotEmpty() && other.odd.isNotEmpty()) {
            n += if (odd.size <= other.odd.size) odd.count { it in other.odd } else other.odd.count { it in odd }
        }
        return n
    }

    /** How many of the ids are hex, the ones [idHash] and [sameId] index. */
    internal fun hexCount(): Int = words.size / WORDS

    /** Hex id [index] hashed over all four words under [seed]; a relay picks its ids, so no part of one is trusted to spread. */
    internal fun idHash(
        index: Int,
        seed: Long,
    ): Long {
        var h = seed
        for (w in 0 until WORDS) h = mix(h xor words[index * WORDS + w])
        return h
    }

    /** Is hex id [index] of this window hex id [otherIndex] of [other]? */
    internal fun sameId(
        index: Int,
        other: Fingerprint,
        otherIndex: Int,
    ): Boolean = compareAt(words, index * WORDS, other.words, otherIndex * WORDS) == 0

    internal fun oddIds(): Set<String> = odd

    override fun equals(other: Any?): Boolean = other is Fingerprint && words.contentEquals(other.words) && odd == other.odd

    override fun hashCode(): Int = 31 * words.contentHashCode() + odd.hashCode()

    companion object {
        private const val WORDS = 4
        private const val HEX_ID_LENGTH = 64
        private const val CHARS_PER_WORD = 16

        val EMPTY = Fingerprint(LongArray(0), emptySet())

        /** The window holding exactly [ids]. */
        fun of(ids: Collection<String>): Fingerprint {
            if (ids.isEmpty()) return EMPTY
            var odd: HashSet<String>? = null
            val hex = ArrayList<String>(ids.size)
            for (id in ids) {
                if (isHexId(id)) hex += id else (odd ?: HashSet<String>().also { odd = it }) += id
            }
            // Lowercase hex sorts as its value does, so string order is the words' unsigned order.
            hex.sort()
            val words = LongArray(hex.size * WORDS)
            var n = 0
            for ((k, id) in hex.withIndex()) {
                if (k > 0 && id == hex[k - 1]) continue
                for (w in 0 until WORDS) words[n * WORDS + w] = wordAt(id, w * CHARS_PER_WORD)
                n++
            }
            return Fingerprint(if (n * WORDS == words.size) words else words.copyOf(n * WORDS), odd ?: emptySet())
        }

        private fun wordAt(
            id: String,
            from: Int,
        ): Long {
            var v = 0L
            for (k in from until from + CHARS_PER_WORD) v = (v shl 4) or Character.digit(id[k], 16).toLong()
            return v
        }

        private fun isHexId(id: String): Boolean {
            if (id.length != HEX_ID_LENGTH) return false
            for (c in id) if (c !in '0'..'9' && c !in 'a'..'f') return false
            return true
        }

        /** MurmurHash3's 64-bit finalizer: a bijection whose every output bit depends on every input bit. */
        private fun mix(key: Long): Long {
            var k = key
            k = (k xor (k ushr 33)) * -49064778989728563L
            k = (k xor (k ushr 33)) * -4265267296055464877L
            return k xor (k ushr 33)
        }

        private fun compareAt(
            a: LongArray,
            i: Int,
            b: LongArray,
            j: Int,
        ): Int {
            for (w in 0 until WORDS) {
                val c = a[i + w].toULong().compareTo(b[j + w].toULong())
                if (c != 0) return c
            }
            return 0
        }
    }
}

/**
 * The windows added so far, keyed by id, so one more window's shared count against all of them
 * costs its own size plus the ids it shares, not one merge per window. Sized up front by [capacity],
 * the hex ids of every window that will be added.
 */
internal class FingerprintIndex(
    private val capacity: Int,
) {
    private val bits = Integer.numberOfTrailingZeros(tableSize(capacity))
    private val mask = (1 shl bits) - 1

    /** Per index, so a relay that sees one pass's slots cannot aim the next pass's ids at one. */
    private val seed = Random.nextLong()

    /** Slot -> the [Fingerprint.idHash] of the one id every entry in its chain holds. */
    private val keys = LongArray(1 shl bits)

    /** Slot -> its first entry, or -1 for an empty slot. */
    private val chains = IntArray(1 shl bits) { -1 }

    /** Entry -> the window holding it, the id's index in that window, and the next entry in its chain. */
    private val owner = IntArray(capacity)
    private val at = IntArray(capacity)
    private val next = IntArray(capacity)
    private var entries = 0
    private val windows = ArrayList<Fingerprint>()
    private val odd = HashMap<String, MutableList<Int>>()

    /** How many windows are in the index; the next one added gets this number. */
    val size: Int get() = windows.size

    fun add(window: Fingerprint) {
        val index = windows.size
        check(entries + window.hexCount() <= capacity) { "index sized for $capacity ids" }
        windows += window
        for (i in 0 until window.hexCount()) {
            val slot = slotOf(window, i)
            val e = entries++
            owner[e] = index
            at[e] = i
            next[e] = chains[slot]
            chains[slot] = e
        }
        for (id in window.oddIds()) odd.getOrPut(id) { ArrayList(1) } += index
    }

    /**
     * Adds to `counts[w]` the ids [window] shares with window `w`, for every `w` it shares one with,
     * and lists each such `w` once in [touched]. Returns how many it listed.
     */
    fun count(
        window: Fingerprint,
        counts: IntArray,
        touched: IntArray,
    ): Int {
        var n = 0
        for (i in 0 until window.hexCount()) {
            val slot = find(window, i)
            if (slot < 0) continue
            // A chain holds one id, so every entry on it is a window sharing this one.
            var e = chains[slot]
            while (e >= 0) {
                val w = owner[e]
                if (counts[w]++ == 0) touched[n++] = w
                e = next[e]
            }
        }
        for (id in window.oddIds()) {
            for (w in odd[id] ?: continue) {
                if (counts[w]++ == 0) touched[n++] = w
            }
        }
        return n
    }

    /** Does occupied [slot] hold hex id [index] of [window]? Compared whole, against the chain's first entry. */
    private fun holds(
        slot: Int,
        key: Long,
        window: Fingerprint,
        index: Int,
    ): Boolean {
        if (keys[slot] != key) return false
        val e = chains[slot]
        return window.sameId(index, windows[owner[e]], at[e])
    }

    /** The slot holding hex id [index] of [window], claimed if it is new. */
    private fun slotOf(
        window: Fingerprint,
        index: Int,
    ): Int {
        val key = window.idHash(index, seed)
        var slot = (key ushr (Long.SIZE_BITS - bits)).toInt()
        while (chains[slot] >= 0 && !holds(slot, key, window, index)) slot = (slot + 1) and mask
        keys[slot] = key
        return slot
    }

    /** The slot holding hex id [index] of [window], or -1. */
    private fun find(
        window: Fingerprint,
        index: Int,
    ): Int {
        val key = window.idHash(index, seed)
        var slot = (key ushr (Long.SIZE_BITS - bits)).toInt()
        while (chains[slot] >= 0) {
            if (holds(slot, key, window, index)) return slot
            slot = (slot + 1) and mask
        }
        return -1
    }

    private companion object {
        /** A power of two at least twice [capacity], so a probe stays short. */
        fun tableSize(capacity: Int): Int {
            var size = 2
            while (size < capacity * 2) size = size shl 1
            return size
        }
    }
}
