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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.relay.router.progress.InFlight
import com.nosfabrica.vespa.relay.router.progress.LegProgress
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Which relays of a dynamic stream are being synced RIGHT NOW, so the rotation
 * never hands the same one to two workers.
 *
 * ## Why this exists at all
 *
 * A dynamic stream used to fan out and JOIN: launch every discovered relay,
 * wait for all of them, then start the next cycle. The join is what made one
 * relay able to stop a mirror. `fetchAllPages` against purplepag.es never
 * returned at all before the paging floor landed — measured, the leg downloaded
 * 1.49M events and then spent the rest of the process's life stepping `until`
 * one second further negative per page — and a single leg like that held a
 * 16,752-relay stream at "cycle in progress" indefinitely. Every other relay in
 * the list had long since finished and nothing would dial any of them again.
 *
 * So the workers stopped being a batch and became a rotation: the stream walks
 * its relay list handing work to a fixed pool, and a slow relay costs one slot
 * out of the pool instead of the whole pass. A pass ENDS when the last url has
 * been handed out, not when the last worker returns — the stragglers keep their
 * slots and finish across the boundary.
 *
 * ## What that makes this class responsible for
 *
 * Passes now overlap, so the same url can come round again while a worker from
 * the previous pass still has it. Dialling it twice is not merely wasteful: the
 * two asks share a socket ([DynamicSync] refcounts them), race on the same
 * cursor band, and each hold a slot the rest of the list is queueing for.
 *
 * [take] is therefore the admission: whoever adds the url wins, everyone else
 * is told to move on. The pair with [release] is what the caller must not get
 * wrong — a `finally`, always, since a url never released is a relay this
 * stream will never dial again for the life of the process.
 *
 * Not a general lock: the set is per stream. Two STREAMS landing on one relay
 * at once is ordinary and deliberate (they ask different filters), which is why
 * `DynamicSync.inFlight` — the socket refcount — is a different, wider thing.
 *
 * ## …and it is the only thing that knows WHICH relays are running
 *
 * The claim is stamped with the moment it was made, so [held] can name the legs
 * that are not ending. That state used to be a bare set and the counts taken off
 * it — `running`, `pending`, `busy` — were the whole of what left this process:
 * a stream stuck on two relays for eleven hours published the number 2 and no
 * way to learn which two. See [InFlight] for the full account.
 */
internal class RelayRotation {
    private val busy = ConcurrentHashMap<NormalizedRelayUrl, Hold>()

    private val passes = AtomicLong()

    private val transferring = AtomicInteger()

    /**
     * One worker's claim on one relay.
     *
     * Two clocks rather than one, because "held for six hours" says nothing
     * about WHERE the six hours went: a leg that never got a transfer slot and
     * one that has held a slot for six hours are different faults with different
     * fixes — the first is our own pool being saturated, the second is one
     * relay — and they are indistinguishable from the claim alone.
     */
    private class Hold(
        val sinceMs: Long,
    ) {
        /** Set while the worker holds a transfer slot — see [transferring]. */
        @Volatile
        var transferringSinceMs: Long? = null

        /**
         * What the leg has actually delivered, which is the third clock and the
         * one that decides: held-and-downloading and held-and-wedged are the
         * same two timestamps otherwise. See [LegProgress].
         */
        val leg = LegProgress(sinceMs)
    }

    /**
     * Claim this relay for the caller, or refuse because a worker still has it.
     *
     * `false` is the ordinary answer for a slow relay whose pass came round
     * again, not an error: the url keeps its slot and the walk moves on.
     */
    fun take(
        url: NormalizedRelayUrl,
        nowMs: Long = System.currentTimeMillis(),
    ): Boolean = busy.putIfAbsent(url, Hold(nowMs)) == null

    /** Give it back. Always from a `finally` — see the class header. */
    fun release(url: NormalizedRelayUrl) {
        busy.remove(url)
    }

    /**
     * How many relays this stream has a worker on right now — probing, queued
     * for a transfer slot, or transferring. See [transferringCount] for the
     * narrower number.
     *
     * This is what tells a rotation still working from one that has stopped:
     * between two passes it is what remains of the previous one, and while a
     * pass is walking it is how much of the admission gate is committed.
     */
    fun busyCount(): Int = busy.size

    /** Urls with a worker right now, for a log line that has to name them. */
    fun busyUrls(): List<NormalizedRelayUrl> = busy.keys.toList()

    /**
     * The claim's own event counter, for the worker holding it to tick as events
     * arrive.
     *
     * Null when nothing holds [url] — which the worker's own path cannot
     * produce, since it asks between [take] and [release]. Nullable all the same
     * rather than asserted: this is reporting, and a report that can throw into
     * the sync path is worse than a report with a gap in it.
     */
    fun leg(url: NormalizedRelayUrl): LegProgress? = busy[url]?.leg

    /**
     * The longest-held of them, with both clocks, ready to publish.
     *
     * Ordered by how long each has been held and then by url — the ordering is
     * total on purpose, so two rollups of the same state produce the same
     * document and a reader can diff them, exactly as `CycleTally.foldedOnto`
     * does. [limit] is a cap on the ROWS, never on the truth: whatever it cuts
     * is counted in `omitted`.
     */
    fun held(
        nowMs: Long,
        limit: Int = DEFAULT_IN_FLIGHT_ROWS,
    ): InFlight {
        // One traversal into a list before sorting: the map is live and a
        // comparator reading it twice could be handed a hold that was released
        // in between, which is an IllegalArgumentException out of the sort
        // rather than a wrong number.
        val rows =
            busy.entries
                .map { (url, hold) -> url.url to hold }
                .sortedWith(compareBy({ it.second.sinceMs }, { it.first }))
        return InFlight(
            relays =
                rows.take(limit).map { (url, hold) ->
                    InFlight.Relay(
                        relay = url,
                        heldForSec = ((nowMs - hold.sinceMs) / 1000).coerceAtLeast(0),
                        transferringForSec = hold.transferringSinceMs?.let { ((nowMs - it) / 1000).coerceAtLeast(0) },
                        events = hold.leg.events(),
                        quietForSec = hold.leg.quietForMs(nowMs) / 1000,
                    )
                },
            omitted = (rows.size - limit).coerceAtLeast(0),
        )
    }

    /**
     * Relays holding a TRANSFER SLOT right now, which is NOT [busyCount].
     *
     * The two are far apart by design and reporting one as the other was a
     * small lie this codebase does not get to tell. A worker spends most of its
     * life deciding whether the relay is worth dialling at all — a discovered
     * list is mostly dead hosts and a connect timeout is the answer — so a
     * stream with 8 transfer slots routinely has 128 workers, of which 120 are
     * probing or queued. `busyCount` is how much of the ADMISSION gate is
     * committed; this is how much of the transfer pool is.
     *
     * "Slot", not "socket", and the wording is load-bearing: the websocket
     * connect happens INSIDE the block [transferring] wraps, so a url that
     * cannot be connected to at all counts here for as long as it is trying.
     * Measured — `InFlightReportProbe` watched exactly that and got
     * `CANNOT_CONNECT` at the end of it.
     */
    fun transferringCount(): Int = transferring.get()

    /**
     * Run [block] counted as an open transfer on [url].
     *
     * The url is what makes [held] able to say which of a stuck leg's clocks is
     * running — the claim's, or the slot's. Tolerates a url with no hold (the
     * caller released it, or never took it) rather than asserting: this is
     * reporting, and a report that can throw into the sync path is worse than a
     * report with a gap in it.
     */
    suspend fun <T> transferring(
        url: NormalizedRelayUrl,
        nowMs: Long = System.currentTimeMillis(),
        block: suspend () -> T,
    ): T {
        val hold = busy[url]
        hold?.transferringSinceMs = nowMs
        transferring.incrementAndGet()
        try {
            return block()
        } finally {
            transferring.decrementAndGet()
            // Cleared rather than left standing: a worker that finished its
            // transfer and is still holding the claim is doing something else,
            // and saying "transferring" about it would send the reader after the
            // wrong half.
            hold?.transferringSinceMs = null
        }
    }

    /** Passes begun over the relay list. Starts at 1 for the first one. */
    fun pass(): Long = passes.get()

    /**
     * Open a pass over [relays], skipping whatever is still being synced.
     *
     * Returns the urls to hand out IN ORDER, and reports how many were passed
     * over as busy — a count the caller has to publish, because "this url was
     * skipped, it is still going from last time" and "this url was never
     * reached" are the same silence otherwise.
     *
     * Deliberately a snapshot rather than a lazy sequence: a url that becomes
     * free halfway through a pass is picked up by the NEXT one. Re-testing as
     * the walk goes would let a fast relay be dialled several times in one pass
     * while the list behind it waits, which is the opposite of a rotation.
     */
    fun beginPass(relays: List<DiscoveredRelay>): Pass {
        passes.incrementAndGet()
        val free = ArrayList<DiscoveredRelay>(relays.size)
        var skipped = 0
        for (relay in relays) {
            if (busy.containsKey(relay.url)) skipped++ else free += relay
        }
        return Pass(free, skipped)
    }

    /** One pass's worth of work: what to hand out, and what was already running. */
    class Pass(
        val relays: List<DiscoveredRelay>,
        /** Urls a worker from an earlier pass still had. */
        val busy: Int,
    )

    companion object {
        /**
         * How many held relays [held] names.
         *
         * Comfortably more than a transfer pool — 8 slots is the configured
         * default and 30 is the widest measured here — so every leg that could
         * be wedged on a socket is named, while the probe storm behind the
         * admission gate (128 workers against those 8 slots is ordinary) is
         * summarised into `omitted` where it belongs.
         */
        const val DEFAULT_IN_FLIGHT_ROWS = 20
    }
}
