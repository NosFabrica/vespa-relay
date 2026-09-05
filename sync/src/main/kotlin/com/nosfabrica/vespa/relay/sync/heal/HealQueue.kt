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
package com.nosfabrica.vespa.relay.sync.heal

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Which repair a queued entry is asking for. */
enum class HealMode {
    /** Hand over the current version of a replaceable/addressable address. */
    CONTENT,

    /** Hand over the kind 5 that retracts an event the relay still serves. */
    DELETION,

    /** Hand over the author's `ALL_RELAYS` kind 62. */
    VANISH,
}

/**
 * What needs repairing at one relay. The key is an address or an author, never a resolved
 * event: the healer finds the thing to push at drain time.
 */
data class HealKey(
    val mode: HealMode,
    /** The kind to resolve and push: the address's own kind, or 5 / 62. */
    val kind: Int,
    val pubkey: String,
    /** [HealMode.CONTENT] only. */
    val dTag: String? = null,
    /** [HealMode.DELETION] only: the event the kind 5 must cover. */
    val victimId: String? = null,
) {
    val isRetraction: Boolean get() = mode != HealMode.CONTENT

    companion object {
        fun content(
            kind: Int,
            pubkey: String,
            dTag: String?,
        ) = HealKey(HealMode.CONTENT, kind, pubkey, dTag)

        fun deletion(
            pubkey: String,
            victimId: String,
        ) = HealKey(HealMode.DELETION, 5, pubkey, victimId = victimId)

        /** One entry per author: every vanished event of theirs wants the same push. */
        fun vanish(pubkey: String) = HealKey(HealMode.VANISH, 62, pubkey)
    }
}

/**
 * The stale copy that triggered the repair, so a permanent refusal can suppress the exact id
 * the relay is serving. Overwritten on coalesce with the newest.
 */
data class StaleRef(
    val id: String,
    val createdAt: Long,
)

/**
 * Repairs the reconcile discovered and left for the healer. Bounded, coalescing on
 * (relay, key), and it drops rather than blocks: a dropped heal is rediscovered on the next
 * offer of the stale copy.
 */
class HealQueue(
    private val perRelayLimit: Int = DEFAULT_PER_RELAY,
    private val totalLimit: Int = DEFAULT_TOTAL,
) {
    private val byRelay = ConcurrentHashMap<NormalizedRelayUrl, ConcurrentHashMap<HealKey, StaleRef>>()
    private val total = AtomicInteger()

    val dropped = AtomicLong()
    val enqueued = AtomicLong()

    fun size(): Int = total.get()

    fun sizeFor(url: NormalizedRelayUrl): Int = byRelay[url]?.size ?: 0

    /**
     * Records that [url] serves a stale [stale] for [key]. False when the entry was dropped
     * for want of room; never blocking.
     */
    fun offer(
        url: NormalizedRelayUrl,
        key: HealKey,
        stale: StaleRef,
    ): Boolean {
        val slot = byRelay.computeIfAbsent(url) { ConcurrentHashMap() }
        // Overwriting an existing key costs no room. Atomic, not check-then-act: a drain
        // removing the key in between would re-insert an entry [total] never counted.
        if (slot.computeIfPresent(key) { _, _ -> stale } != null) return true
        if (slot.size >= perRelayLimit || total.get() >= totalLimit) {
            dropped.incrementAndGet()
            return false
        }
        if (slot.putIfAbsent(key, stale) == null) {
            total.incrementAndGet()
            enqueued.incrementAndGet()
        }
        return true
    }

    /**
     * Takes up to [limit] repairs queued for [url], leaving the remainder. Entries are removed
     * one at a time from the live map, never by swapping it out, so [total] stays exact.
     */
    fun drain(
        url: NormalizedRelayUrl,
        limit: Int = Int.MAX_VALUE,
    ): Map<HealKey, StaleRef> {
        if (limit <= 0) return emptyMap()
        val slot = byRelay[url] ?: return emptyMap()
        val taken = HashMap<HealKey, StaleRef>(minOf(limit, slot.size).coerceAtLeast(1))
        for (key in slot.keys) {
            if (taken.size >= limit) break
            slot.remove(key)?.let {
                taken[key] = it
                total.decrementAndGet()
            }
        }
        return taken
    }

    /** Throws away what is queued for [url] without pushing any of it. */
    fun discard(url: NormalizedRelayUrl) {
        drain(url)
    }

    companion object {
        /** One relay's worth; more than this will not finish in one pass anyway. */
        const val DEFAULT_PER_RELAY = 2_000

        /** Across every relay, so the queue is never why the process runs out of room. */
        const val DEFAULT_TOTAL = 200_000
    }
}
