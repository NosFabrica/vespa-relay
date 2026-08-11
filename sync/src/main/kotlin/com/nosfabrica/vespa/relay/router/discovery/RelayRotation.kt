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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap
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
 */
internal class RelayRotation {
    private val busy = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    private val passes = AtomicLong()

    /**
     * Claim this relay for the caller, or refuse because a worker still has it.
     *
     * `false` is the ordinary answer for a slow relay whose pass came round
     * again, not an error: the url keeps its slot and the walk moves on.
     */
    fun take(url: NormalizedRelayUrl): Boolean = busy.add(url)

    /** Give it back. Always from a `finally` — see the class header. */
    fun release(url: NormalizedRelayUrl) {
        busy.remove(url)
    }

    /**
     * How many relays this stream is syncing right now.
     *
     * The number that tells a rotation still working from one that has stopped:
     * between two passes it is what remains of the previous one, and while a
     * pass is walking it is how much of the pool is committed.
     */
    fun busyCount(): Int = busy.size

    /** Urls being synced right now, for a log line that has to name them. */
    fun busyUrls(): List<NormalizedRelayUrl> = busy.toList()

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
            if (relay.url in busy) skipped++ else free += relay
        }
        return Pass(free, skipped)
    }

    /** One pass's worth of work: what to hand out, and what was already running. */
    class Pass(
        val relays: List<DiscoveredRelay>,
        /** Urls a worker from an earlier pass still had. */
        val busy: Int,
    )
}
