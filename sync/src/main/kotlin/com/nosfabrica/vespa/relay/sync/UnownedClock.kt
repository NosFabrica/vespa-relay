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
package com.nosfabrica.vespa.relay.sync

import java.util.concurrent.ConcurrentHashMap

/**
 * When each piece of held state lost its last roster owner, so it is forgotten only after
 * [ttlSeconds] unowned. A unit can flap off the roster and back; the clock restarts when it does.
 */
internal class UnownedClock<K>(
    private val ttlSeconds: Long = UNOWNED_TTL_SECONDS,
) {
    private val since = ConcurrentHashMap<K, Long>()

    /** The owned count of the last roster that was believed, for the shrink guard. */
    private var lastOwned = 0

    /**
     * Stamps what in [held] has newly lost its owner, clears what regained one, and returns
     * what has been unowned past the TTL. An empty roster, or one under [MIN_ROSTER_SHARE] of
     * the last, is a failed read rather than a decision, and expires nothing.
     */
    @Synchronized
    fun expired(
        held: Set<K>,
        owned: Set<K>,
        now: Long,
    ): Set<K> {
        if (owned.isEmpty()) return emptySet()
        val previous = lastOwned
        // Believed from the next rebuild on, so a lasting shrink starts its clocks one rebuild late.
        lastOwned = owned.size
        if (owned.size < previous * MIN_ROSTER_SHARE) return emptySet()
        since.keys.removeIf { it !in held || it in owned }
        val out = HashSet<K>()
        for (key in held) {
            if (key in owned) continue
            val at = since.putIfAbsent(key, now) ?: now
            if (now - at >= ttlSeconds) out += key
        }
        for (key in out) since.remove(key)
        return out
    }

    /** Every running clock, for the state file. */
    fun stamps(): Map<K, Long> = HashMap(since)

    /** A clock read back from the state file, so a restart does not restart it. */
    fun restore(
        key: K,
        at: Long,
    ) {
        since[key] = at
    }

    companion object {
        /** How long state may sit unowned before it goes. A unit that returns within it resumes. */
        const val UNOWNED_TTL_SECONDS = 30L * 24 * 60 * 60

        /** A roster smaller than this share of the last one is not believed for one rebuild. */
        const val MIN_ROSTER_SHARE = 0.5
    }
}
