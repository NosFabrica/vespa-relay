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
package com.nosfabrica.vespa.relay.router.progress

import java.util.concurrent.atomic.AtomicLong

/**
 * ONE RUNNING LEG'S OWN EVENT CLOCK — is this relay delivering, or holding a
 * slot and saying nothing?
 *
 * ## Why the duration alone is not the answer
 *
 * Naming the relays a stream has been holding for eleven hours ([InFlight]) says
 * WHICH; it does not say whether holding them is a fault. Both of these are
 * ordinary and they want opposite responses:
 *
 *  - a relay with a real backlog — directory.yabu.me served 1,225,329 events
 *    below one band floor, and 120s of walking reached 259,616 of them — is
 *    working, and the slot is being spent well;
 *  - a leg that cannot terminate — the measured purplepag.es loop pulled 500
 *    events per page and delivered NONE of them, forever, at 5.5 pages/s with
 *    the socket saturated — is spending the same slot on nothing, and the phase
 *    line's frozen event count was the only trace it left.
 *
 * From the outside those two are one state: "held for hours, transferring". What
 * separates them is whether anything is still ARRIVING, which is this.
 *
 * ## Two numbers, and the second is the one that decides
 *
 * [events] is what the leg has delivered so far and [quietForMs] is how long
 * since the last one. A large count going nowhere reads exactly like a large
 * count still growing unless the second number is published beside it, which is
 * why neither is useful alone. Before the first event there is nothing to
 * measure from, so the quiet clock runs from the CLAIM — a leg that has never
 * delivered has been quiet for its whole life, which is the true answer and not
 * a missing one.
 *
 * ## Counted per event, on purpose
 *
 * The alternative — ticking per ask, or per leg — cannot see the failure this
 * exists to catch: the purplepag.es loop is a SINGLE leg that never returns, so
 * every boundary-based counter reports zero of everything for as long as it
 * lasts. The cost is an atomic increment and a volatile store of the clock on a
 * path that already matches the event against a filter, records a per-kind span
 * and offers it to the ingest queue.
 */
class LegProgress(
    /** When the worker claimed the relay — where the quiet clock starts. */
    private val claimedMs: Long,
) {
    private val count = AtomicLong()

    /**
     * What this leg is doing — see [InFlight.Relay.doing]. Written by the worker
     * as it moves, read by the report; last write wins, because the rollup wants
     * whatever is true at that instant.
     */
    @Volatile
    var stage: String? = null

    @Volatile
    private var lastMs: Long = claimedMs

    /** An event arrived from this relay. */
    fun received(nowMs: Long = System.currentTimeMillis()) {
        count.incrementAndGet()
        lastMs = nowMs
    }

    /** Events this leg has received at the socket, before ingest drops any of them. */
    fun events(): Long = count.get()

    /** How long since the last one — or since the claim, when none has arrived. */
    fun quietForMs(nowMs: Long): Long = (nowMs - lastMs).coerceAtLeast(0)
}
