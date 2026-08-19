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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * The stream's socket bookkeeping, because a fingerprint opens a websocket
 * and NOTHING in quartz ever closes one.
 *
 * `fetchAll` unsubscribes when it returns — it sends a CLOSE — and leaves
 * the connection in the pool; the client's own keep-alive only ever
 * RECONNECTS. So a pass used to leave one open socket per url it
 * fingerprinted, against a router whose whole dispatcher budget is 1024 and
 * whose per-HOST budget is 20 — and the fold probes widest group first,
 * i.e. the hosts wearing 55 urls. Every one of those sockets is a slot the
 * fan-out cannot have. That is what makes this refcount load-bearing rather
 * than tidy: with the per-pass cap gone, the number of urls a single pass
 * touches is the whole candidate set, so a leak here would be unbounded
 * where it used to be merely large.
 *
 * It is the STREAM's, not this component's, for the reason
 * `DynamicSync.releaseSocket` exists at all: two streams routinely land on
 * one relay, so closing a socket is only safe behind a refcount, and this
 * pass runs alongside a fan-out that may be holding the same url. Claiming
 * before the dial is what puts the probe INTO that count instead of
 * decrementing somebody else's.
 */
interface Sockets {
    /** Take a share of this url's socket before dialling it. */
    fun claim(url: NormalizedRelayUrl)

    /** Give it back — and close the socket if nothing else holds one. */
    fun release(url: NormalizedRelayUrl)

    companion object {
        /**
         * Leaves every socket where it is. The honest default for a caller
         * with no refcount to offer: leaking a connection is recoverable,
         * closing one out from under a live transfer is not.
         */
        val NONE =
            object : Sockets {
                override fun claim(url: NormalizedRelayUrl) = Unit

                override fun release(url: NormalizedRelayUrl) = Unit
            }
    }
}
