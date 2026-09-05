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
 * What bounds a probe pass's dials: one gate per transport, chosen by [routesTor] so the gate
 * a url waits on and the OkHttp dispatcher it lands in cannot disagree. Not to be confused
 * with `ingest.ProbeGate`, the store-probe hit-rate gate.
 */
class DialGate(
    /** Dials in flight over the direct client: the operator's `monitor { dialConcurrency }`. */
    val clearnetPermits: Int,
    /** Whether there is a proxy at all; equal permit counts do not mean one gate. */
    private val proxied: Boolean = false,
    /** Dials in flight over the proxy, sized to the Tor dispatcher. */
    val torPermits: Int = clearnetPermits,
    /** [TorTransport.routes], or "nothing does" on a deployment with no proxy. */
    private val routesTor: (NormalizedRelayUrl) -> Boolean = { false },
) {
    private val clearnet = Semaphore(clearnetPermits)

    private val tor = Semaphore(torPermits)

    /**
     * Hold [url]'s transport's permit for the length of [block]. A deadline goes inside this,
     * never around it, or it times the wait for a permit and blames the relay.
     */
    suspend fun <T> withPermit(
        url: NormalizedRelayUrl,
        block: suspend () -> T,
    ): T = (if (routesTor(url)) tor else clearnet).withPermit { block() }

    /** What a pass is bounded by, for the monitor's boot line. */
    fun describe(): String = if (!proxied) "$clearnetPermits dial(s)" else "$clearnetPermits clearnet dial(s), $torPermits over Tor"

    companion object {
        /** The gate the monitor passes run behind; the Tor side is capped by the Tor dispatcher width. */
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
