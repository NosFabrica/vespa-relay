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

import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.sync.VisitPool.VisitKey
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The pool's live tails: one subscription per unit with its socket claim and live permit, and the
 * yield scores that decide which relays keep a tail when a stream's live budget is short.
 */
internal class TailBook(
    private val reads: RelayReads,
    private val sockets: Sockets,
    private val limits: PoolLimits,
    private val widths: FilterWidths,
    private val ingest: IngestPipeline,
    private val queue: VisitQueue<VisitKey>,
    /** The live roster, read per event so a relay cannot widen ingest past what it is asked. */
    private val roster: () -> RosterBuilder.Roster,
    /** One event a tail delivered, counted on the pool's own total. */
    private val onArrived: () -> Unit,
    /** The set of tails changed, so the phase rows are stale. */
    private val onChange: () -> Unit,
) {
    /** One held live subscription and the socket claim that rides with it. */
    class Tail(
        val subId: String,
        /** The roster's ask identity when the tail was opened; a change re-opens the tail. */
        val wantsAtOpen: Set<String>,
        /** The kind cap the tail was opened at; a newly learned cap re-opens the tail. */
        val capAtOpen: Int? = null,
        val openedMs: Long = System.currentTimeMillis(),
        /** The live permit, held for the tail's life and released by [drop]. */
        val hold: PoolLimits.Hold,
    ) {
        val events = AtomicLong()

        @Volatile var lastEventMs: Long = openedMs
    }

    private val tails = ConcurrentHashMap<VisitKey, Tail>()
    private val tailSeq = AtomicInteger()

    /**
     * What a relay has delivered lately: a score that halves every [VisitPool.YIELD_HALF_LIFE_MS].
     * Decides which relays keep tails when the budget is short and how soon a relay is revisited.
     */
    private class Yield {
        val arrived = AtomicLong()

        @Volatile var score: Double = 0.0

        @Volatile var foldedAtMs: Long = System.currentTimeMillis()

        /** Folds what arrived into the decayed score and returns it. Racy by design: a priority hint. */
        fun foldedScore(nowMs: Long): Double {
            val fresh = arrived.getAndSet(0)
            val dtMs = (nowMs - foldedAtMs).coerceAtLeast(0)
            if (fresh > 0 || dtMs > VisitPool.YIELD_HALF_LIFE_MS / 8) {
                score = score * Math.pow(0.5, dtMs.toDouble() / VisitPool.YIELD_HALF_LIFE_MS) + fresh
                foldedAtMs = nowMs
            }
            return score
        }
    }

    private val yields = ConcurrentHashMap<NormalizedRelayUrl, Yield>()

    private fun yieldOf(url: NormalizedRelayUrl): Yield = yields.getOrPut(url) { Yield() }

    /** Units whose tail was just evicted: the prompt visit catches up and does not evict in turn. */
    private val evicted = ConcurrentHashMap.newKeySet<VisitKey>()

    /** Tails closed to hand their permit to a unit with more yield, since boot. */
    val evictions = AtomicLong()

    val size: Int get() = tails.size

    fun has(key: VisitKey): Boolean = tails.containsKey(key)

    /** Every open tail, for the status rows. */
    fun open(): Map<VisitKey, Tail> = tails

    /** Credits [url]'s yield with one arrived event. */
    fun credit(url: NormalizedRelayUrl) {
        yieldOf(url).arrived.incrementAndGet()
    }

    /** [url]'s decayed yield, or null for a relay with none. Never creates one, so a pruned yield stays pruned. */
    fun yieldScore(
        url: NormalizedRelayUrl,
        nowMs: Long,
    ): Double? = yields[url]?.foldedScore(nowMs)

    /** Whether [key]'s tail was just evicted, consuming the mark: its prompt visit must not evict in turn. */
    fun takeEvicted(key: VisitKey): Boolean = evicted.remove(key)

    /** Drops the tails and marks of units [wanted] no longer holds, and the yields of [goneUrls]. */
    fun retain(
        wanted: (VisitKey) -> Boolean,
        goneUrls: Set<NormalizedRelayUrl>,
    ) {
        // Per unit: a relay can leave one stream's roster and stay on another's.
        for (key in tails.keys.filter { !wanted(it) }) drop(key)
        for (url in goneUrls) yields.remove(url)
        evicted.removeIf { !wanted(it) }
    }

    /**
     * Opens this unit's live tail, `since` [VisitPool.TAIL_OVERLAP_SECONDS] behind now so the seam
     * with the catch-up cannot drop an event. A sitting tail whose asks or kind cap changed is
     * re-opened. Without [mayEvict] it takes only a spare permit: a unit just evicted would evict back.
     */
    suspend fun open(
        key: VisitKey,
        mayEvict: Boolean,
    ) {
        val url = key.url
        val unit = roster().asks[url]?.get(key.stream) ?: return
        if (unit.asks.isEmpty()) return
        val wantsNow = unit.identity
        val capNow = widths.capFor(url)
        val sitting = tails[key]
        if (sitting != null) {
            if (sitting.wantsAtOpen == wantsNow && sitting.capAtOpen == capNow) return
            drop(key)
        }
        // A spare permit within the stream's budget, or one earned by evicting its weakest tail.
        // `trySpare` does not count a deferral: a full live budget is normal, not refused work.
        val hold = limits.trySpare(key.stream, VisitPool.POOL_LIVE) ?: (if (mayEvict) earn(key) else null) ?: return
        val subId = "visit-tail-${tailSeq.incrementAndGet()}"
        // Built before the listener closes over it, so the first burst lands on the counters.
        val tail = Tail(subId, wantsNow, capAtOpen = capNow, hold = hold)

        // Every way out before the publish hands back exactly what was taken.
        fun abandon(untail: Boolean = false) {
            if (untail) reads.untail(subId)
            sockets.release(url)
            hold.release()
        }
        // Claim and subscribe before publishing, so a concurrent drop meets a fully formed tail.
        sockets.claim(url)
        try {
            val filters = VisitPool.tailFilters(unit.asks, nowSeconds() - VisitPool.TAIL_OVERLAP_SECONDS)
            reads.tail(subId, url, widths.chunkAll(url, filters)) { event ->
                onArrived()
                credit(url)
                tail.events.incrementAndGet()
                tail.lastEventMs = System.currentTimeMillis()
                // Scope is re-checked per event against the live roster, so a relay cannot widen ingest.
                var any = false
                var allTrusted = true
                var healContent = false
                var healRetractions = false
                roster().asks[url]?.get(key.stream)?.forEachCandidate(event.pubKey) { ask ->
                    if (!ask.filter.match(event)) return@forEachCandidate
                    any = true
                    allTrusted = allTrusted && ask.stream.trusted
                    healContent = healContent || ask.stream.healContent
                    healRetractions = healRetractions || ask.stream.healRetractions
                }
                if (any) ingest.submit(event, allTrusted, IngestOrigin(url, healContent, healRetractions))
            }
        } catch (e: CancellationException) {
            abandon()
            throw e
        } catch (e: Exception) {
            abandon()
            System.err.println("router: tail ${key.stream} ${url.url} failed to open: ${e.javaClass.simpleName}: ${e.message?.take(80)}")
            return
        }
        if (tails.putIfAbsent(key, tail) != null) {
            abandon(untail = true)
            return
        }
        // The roster may have dropped this unit between the read above and the publish.
        if (!wanted(key)) {
            drop(key)
            return
        }
        onChange()
    }

    /**
     * Earns [candidate] a live permit by evicting the same stream's weakest tail, or returns null.
     * The candidate must win on yield, not tie, so a pool of equals does not churn. The evicted
     * tail's permit is handed over, never released, so no other worker can take it between.
     */
    private fun earn(candidate: VisitKey): PoolLimits.Hold? {
        val nowMs = System.currentTimeMillis()
        val mine = yieldOf(candidate.url).foldedScore(nowMs)
        // One fold per tail: `foldedScore` drains what arrived, so a second read differs.
        var weakest: VisitKey? = null
        var weakestScore = Double.MAX_VALUE
        for (key in tails.keys) {
            if (key == candidate || key.stream != candidate.stream) continue
            val score = yieldOf(key.url).foldedScore(nowMs)
            if (score < weakestScore) {
                weakest = key
                weakestScore = score
            }
        }
        if (weakest == null || weakestScore >= mine) return null
        // Another path dropped it first and released its permit; ask for one like anyone else.
        val handedOver = detach(weakest) ?: return limits.tryHold(candidate.stream, VisitPool.POOL_LIVE)
        evictions.incrementAndGet()
        if (wanted(weakest)) {
            evicted += weakest
            queue.offer(weakest)
        }
        return handedOver
    }

    fun drop(key: VisitKey) {
        detach(key)?.release()
    }

    /** Takes [key]'s tail down and returns its live permit unreleased, or null when none was up. */
    private fun detach(key: VisitKey): PoolLimits.Hold? {
        val tail = tails.remove(key) ?: return null
        reads.untail(tail.subId)
        sockets.release(key.url)
        // The revisit timer was armed on the tailed cadence; let the next visit arm the untailed one.
        queue.disarm(key)
        onChange()
        return tail.hold
    }

    private fun wanted(key: VisitKey): Boolean = roster().asks[key.url]?.containsKey(key.stream) == true
}
