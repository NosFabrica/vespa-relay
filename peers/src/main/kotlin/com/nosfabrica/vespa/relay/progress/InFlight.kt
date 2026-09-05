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
package com.nosfabrica.vespa.relay.progress

/**
 * The relays a stream has a worker on right now, named rather than counted: not the roster,
 * not the queue. Published whole, quietest first.
 */
class InFlight(
    val relays: List<Relay>,
    /** How many more had a worker and are not named here. */
    val omitted: Int,
) {
    /** One relay a worker is holding, and the clocks that say what it is doing with it. */
    class Relay(
        val relay: String,
        /** Since the rotation claimed it, which is before the guards and the wait for a slot. */
        val heldForSec: Long,
        /**
         * Since it took a transfer slot, or null while it waits for one. The connect happens
         * inside the slot, so a leg stuck on a handshake is transferring.
         */
        val transferringForSec: Long?,
        /** Events this leg has received off the wire, counted before ingest. */
        val events: Long,
        /** Since the last event arrived, or since the claim if none did. */
        val quietForSec: Long,
        /** What the leg is doing, as a sentence. Null before a leg reaches a stage worth the word. */
        val stage: String? = null,
        /** The owning stream, on the pool-wide list only. */
        val stream: String? = null,
        /**
         * The pool workload this row is in (`live`, `catching-up`, `re-fetching`, `negentropy`),
         * the word a reader may group by. Absent for a visit between jobs.
         */
        val pool: String? = null,
        /**
         * How far back the leg has got, always the older edge: the paged cursor's `created_at`,
         * or the `since` of the negentropy window. Null in the guards, queued, or on a retraction.
         */
        val pagingUntil: Long? = null,
    )

    companion object {
        val NONE = InFlight(emptyList(), 0)
    }
}
