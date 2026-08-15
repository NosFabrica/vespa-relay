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
package com.nosfabrica.vespa.relay.router.presence

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import java.util.concurrent.atomic.AtomicLong

/**
 * Who is signed in to the served relay, as this process last heard it.
 *
 * The mirror side of `AuthedReaders`, and the same division of labour
 * `ServingPressure` already has across this boundary: one small object holds
 * the number, one poller keeps it current, and the consumer reads it without
 * knowing an HTTP request exists. Written by [AuthedPoller]'s thread and read
 * by every presence stream's loop, so the state is one volatile reference to an
 * immutable list rather than a mutable collection under a lock.
 */
class AuthedFeed {
    @Volatile
    private var current: List<HexKey> = emptyList()

    /**
     * Readers the last successful poll could not fit in its response.
     *
     * Carried through rather than dropped because it changes what this list
     * MEANS: below the cap it is everyone, at the cap it is the oldest N
     * connections and the rest are being mirrored by nobody. A stream that
     * cannot say which it has is a stream whose silence about somebody is
     * unexplainable.
     */
    @Volatile
    var omitted: Int = 0
        private set

    /** Polls that have landed, ever. Zero is "the feed has never spoken", which is not "nobody is here". */
    private val polls = AtomicLong()

    /** Who is here. Empty before the first poll, and empty again if the feed is lost — see [clear]. */
    fun readers(): List<HexKey> = current

    /** Has this feed ever answered? Separates "nobody signed in" from "we have not been told". */
    fun everFed(): Boolean = polls.get() > 0

    fun adopt(
        readers: List<HexKey>,
        omitted: Int,
    ) {
        // Distinct and sorted: the far end orders by connection age, which is
        // the right cut for its own bound and the wrong key for ours — a
        // subscription set that reorders every poll for no reason would make
        // every diff look like churn.
        current = readers.distinct().sorted()
        this.omitted = omitted.coerceAtLeast(0)
        polls.incrementAndGet()
    }

    /**
     * The feed is gone: nobody is signed in as far as this process may claim.
     *
     * EMPTY is the correct answer to a lost feed, not the last known set, and
     * the reason is the same one that lets `PressurePoller` reset the throttle:
     * the sockets those readers were signed in ON live in the relay we can no
     * longer reach. If it is down they are gone; if only the route is down we
     * have no evidence either way, and holding subscriptions open for people we
     * cannot show are present is exactly the "mirroring for nobody" this stream
     * shape exists to avoid.
     *
     * [everFed] is deliberately not reset — that flag is about whether the feed
     * has ever worked, which is what tells a typo'd url from a relay that died.
     */
    fun clear() {
        current = emptyList()
        omitted = 0
    }
}
