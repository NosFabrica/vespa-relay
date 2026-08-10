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
package com.nosfabrica.vespa.relay.router.heal

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
 * What needs repairing at one relay. The key is an ADDRESS or an author, never
 * a resolved event — finding the thing to push is the healer's job at drain
 * time, so the sweep pays nothing but a map insert, and whatever is current
 * when the drain runs is what goes out.
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
 * The stale copy that triggered the repair, kept beside the key so a permanent
 * refusal can suppress the exact id the relay is serving rather than guessing.
 * Overwritten on coalesce, which is right: the newest stale copy is the one
 * that relay is actually still handing out.
 */
data class StaleRef(
    val id: String,
    val createdAt: Long,
)

/**
 * Work the reconcile discovered and refuses to do itself.
 *
 * **Bounded and coalescing, and it DROPS rather than blocks.** That is
 * deliberately the inverse of [com.nosfabrica.vespa.relay.router.IngestPipeline.submit],
 * which suspends its producer rather than lose an event, and the difference is
 * the point: an event dropped there is data lost, a heal dropped here is a
 * retry — the next cycle rediscovers it the moment the relay offers the stale
 * copy again. Blocking the sweep to guarantee a repair would trade the thing we
 * are fixing for the fix.
 *
 * Coalescing on the address means a profile that a thousand relays are stale on
 * costs one entry per relay, not one per rejected copy.
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
     * Record that [url] is serving a stale [stale] for [key]. Returns false when
     * the entry was dropped for want of room — logged by the caller, never
     * retried, never blocking.
     */
    fun offer(
        url: NormalizedRelayUrl,
        key: HealKey,
        stale: StaleRef,
    ): Boolean {
        val slot = byRelay.computeIfAbsent(url) { ConcurrentHashMap() }
        // Overwriting an existing key is always allowed: it costs no new room
        // and keeps the freshest stale reference.
        if (slot.containsKey(key)) {
            slot[key] = stale
            return true
        }
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

    /** Take everything queued for [url]. The queue is empty for it afterwards. */
    fun drain(url: NormalizedRelayUrl): Map<HealKey, StaleRef> {
        val slot = byRelay.remove(url) ?: return emptyMap()
        total.addAndGet(-slot.size)
        return slot
    }

    /** Throw away what is queued for [url] without pushing any of it. */
    fun discard(url: NormalizedRelayUrl) {
        drain(url)
    }

    companion object {
        /**
         * One relay's worth. A fan-out cycle touches thousands of relays, and
         * a single relay being stale on more addresses than this is a relay
         * whose repair will not finish in one pass anyway.
         */
        const val DEFAULT_PER_RELAY = 2_000

        /**
         * Across every relay. At ~120 bytes an entry this is a few tens of MB
         * worst case, and the drop path keeps it from ever being the reason
         * the process runs out of room.
         */
        const val DEFAULT_TOTAL = 200_000
    }
}
