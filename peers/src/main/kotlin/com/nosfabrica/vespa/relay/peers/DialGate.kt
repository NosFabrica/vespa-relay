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
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * What bounds a probe pass's dials: one gate per transport, because a
 * clearnet dial draws on the router's OkHttp dispatcher and an onion dial on
 * the Tor client's own, and a url only ever wants one. [routesTor] is
 * [TorTransport.routes], the predicate that also picks the OkHttp client, so
 * the gate a url waits on and the dispatcher it lands in cannot disagree.
 *
 * One instance per pass object, not per run: `AliasMonitor` serialises
 * passes behind a mutex and every permit returns in `withPermit`'s finally.
 * Not "probe gate": `ingest.ProbeGate` is the store-probe hit-rate gate.
 */
class DialGate(
    /** Dials in flight over the direct client: the operator's `monitor { dialConcurrency }`. */
    val clearnetPermits: Int,
    /** Whether there is a proxy at all; [describe] needs it because equal permit counts do not mean one gate. */
    private val proxied: Boolean = false,
    /** Dials in flight over the proxy, sized to the Tor dispatcher and capped by `dialConcurrency`. */
    val torPermits: Int = clearnetPermits,
    /** [TorTransport.routes], or "nothing does" on a deployment with no proxy. */
    private val routesTor: (NormalizedRelayUrl) -> Boolean = { false },
) {
    private val clearnet = Semaphore(clearnetPermits)

    private val tor = Semaphore(torPermits)

    /**
     * Hold [url]'s transport's permit for the length of [block]. The deadline
     * goes inside this, never around it: outside it would time the wait for a
     * permit, which is the pass's shape and no relay's fault.
     */
    suspend fun <T> withPermit(
        url: NormalizedRelayUrl,
        block: suspend () -> T,
    ): T = (if (routesTor(url)) tor else clearnet).withPermit { block() }

    /** What a pass is bounded by, for the line the router prints when the monitor starts. */
    fun describe(): String = if (!proxied) "$clearnetPermits dial(s)" else "$clearnetPermits clearnet dial(s), $torPermits over Tor"

    companion object {
        /**
         * The gate the monitor passes run behind. The Tor side is `min` of the
         * knob and the dispatcher width: more than `maxSockets` buys queueing,
         * and a deployment that lowered `dialConcurrency` did not ask for
         * thirty-two onion dials.
         */
        fun over(
            concurrency: Int,
            tor: TorTransport?,
        ): DialGate =
            if (tor == null) {
                DialGate(concurrency)
            } else {
                DialGate(
                    clearnetPermits = concurrency,
                    proxied = true,
                    torPermits = minOf(concurrency, tor.settings.maxSockets),
                    routesTor = tor::routes,
                )
            }
    }
}
