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
 * WHAT BOUNDS A PROBE PASS'S DIALS — one gate per transport, because the two
 * transports are two different resources and a url can only ever want one.
 *
 * The three monitor passes used to share a single `Semaphore(dialConcurrency)`
 * over both. That is not one resource: a clearnet dial draws on the router's
 * 1024-socket OkHttp dispatcher, and an onion dial draws on the Tor client's
 * own dispatcher, which is [TorSettings.maxSockets] wide (32 by default) and
 * shares nothing with it — see [TorTransport]. Pooling them meant the
 * slower class of work could take the whole gate, and it did.
 *
 * **Measured on `vespa-eventstore-staging`**, `dialConcurrency = 100`, Tor at
 * its defaults, 936 hosts in one `aliasFold` pass:
 *
 * ```
 * 14:36:29  509/936  inflight=99   onion=60 (median held 34s)  clearnet=39 (median  9s)
 * 14:37:15  606/936  inflight=100  onion=74 (median held 72s)  clearnet=26 (median 10s)
 * 14:38:00  654/936  inflight=100  onion=71 (median held 67s)  clearnet=29 (median 21s)
 * ```
 *
 * The gate was saturated, so that is contention rather than idleness. `.onion`
 * held 60-74% of every permit while being 10% of the population — of 400 sampled
 * kind-30166 records the `n` tag read `clearnet 357 / tor 42` — because
 * [probeIdleMs] allows a Tor-routed url the circuit budget ON TOP of the
 * clearnet one (90s + 20s against 20s), and a dead hidden service spends all of
 * it. Roughly a 6x over-representation in the one resource the passes are
 * bounded by.
 *
 * **And 30-40 of those permits had no socket behind them.** `maxSockets` is 32,
 * so onions past the 32nd were queued inside the Tor dispatcher while still
 * holding a probe permit a clearnet relay would have finished with in nine
 * seconds. The two limits were set independently and nothing reconciled them:
 * raising `dialConcurrency` past `maxSockets` could not buy Tor throughput, only
 * convert clearnet permits into Tor queue slots.
 *
 * So the Tor gate is sized to the Tor dispatcher — see [torPermits]. The
 * ceiling on dials in flight rises from `dialConcurrency` to
 * `dialConcurrency + torPermits` and that is deliberate: the two draw on
 * different dispatchers, and holding them to one number is what made
 * `dialConcurrency` mean dials-plus-Tor-queueing instead of dials.
 *
 * **What this does NOT buy is "a permit is a socket", and the difference is
 * worth stating rather than hoping nobody checks.** The Tor dispatcher is
 * process-wide — one [TorTransport] behind `PeerClient.httpFor`, shared with
 * the mirror — so the mirror's onion tails and catch-ups hold slots in the same
 * 32, and a monitor permit can still be a place in a queue behind them. What it
 * can no longer be is a place in that queue held WHILE occupying a clearnet
 * permit, which is the whole of the measurement above. Sizing the monitor's
 * share against live dispatcher occupancy would be the next step if onion
 * relays ever stop being a handful of the roster; it is not worth a moving
 * target today.
 *
 * Nothing here decides WHICH transport a url takes. [routesTor] is
 * [TorTransport.routes], the same predicate that picks the OkHttp client, so the
 * gate a url waits on and the dispatcher it lands in cannot disagree — including
 * under `SYNC_TOR_ALL`, where every url routes through the proxy and the Tor
 * gate is correctly the only one that admits anything.
 *
 * One instance per pass object rather than per pass RUN: `AliasMonitor` holds a
 * `passGate` mutex, so no two passes overlap, and a permit is always returned in
 * `withPermit`'s `finally`.
 *
 * Named for the thing it bounds — `monitor { dialConcurrency }` — and not
 * "probe gate", which is taken: `ingest.ProbeGate` is the store-probe hit-rate
 * gate on the ingest path and has nothing to do with this.
 */
class DialGate(
    /** Dials in flight over the direct client — the operator's `monitor { dialConcurrency }`. */
    val clearnetPermits: Int,
    /**
     * Is there a proxy at all? Only [describe] reads it, and only because the
     * two numbers being EQUAL is not the same fact as there being one number:
     * at `dialConcurrency = 16` against the default 32 sockets both gates are
     * 16, and a line that printed "16 dial(s)" there would read exactly like
     * the no-Tor deployment while the real ceiling is 32.
     */
    private val proxied: Boolean = false,
    /**
     * …and over the proxy. Sized to the Tor dispatcher rather than to the
     * operator's knob, and capped by it: more than [TorSettings.maxSockets] buys
     * queueing rather than throughput, and more than `dialConcurrency` would let
     * a deployment that asked for four dials run thirty-two.
     */
    val torPermits: Int = clearnetPermits,
    /** [TorTransport.routes], or "nothing does" on a deployment with no proxy. */
    private val routesTor: (NormalizedRelayUrl) -> Boolean = { false },
) {
    private val clearnet = Semaphore(clearnetPermits)

    private val tor = Semaphore(torPermits)

    /**
     * Hold [url]'s transport's permit for the length of [block].
     *
     * The deadline goes INSIDE this, never around it — see
     * [com.nosfabrica.vespa.relay.monitor.AliasProbe.deadlineMs]: out there it
     * would be timing the wait for a permit, which is the pass's own shape and
     * no relay's fault.
     */
    suspend fun <T> withPermit(
        url: NormalizedRelayUrl,
        block: suspend () -> T,
    ): T = (if (routesTor(url)) tor else clearnet).withPermit { block() }

    /** What a pass is actually bounded by, for the line the router prints when the monitor starts. */
    fun describe(): String = if (!proxied) "$clearnetPermits dial(s)" else "$clearnetPermits clearnet dial(s), $torPermits over Tor"

    companion object {
        /**
         * The gate the monitor passes run behind: the operator's knob for the
         * direct client, and the Tor dispatcher's own width for the proxy.
         *
         * `min` rather than `maxSockets` outright, because `dialConcurrency` is
         * a ceiling on the whole plane's appetite and a deployment that lowered
         * it did not ask for thirty-two onion dials.
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
