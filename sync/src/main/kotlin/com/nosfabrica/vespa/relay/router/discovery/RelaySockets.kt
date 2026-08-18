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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * WHO IS STILL USING THIS SOCKET — one refcount across every stream and every
 * probe pass.
 *
 * Quartz closes none of its own connections, so whatever opens one has to hand
 * it back. It has to be shared: two streams routinely land on one relay while a
 * probe fingerprints it, and closing on the first release would cut a transfer
 * still running underneath. Pinned urls are never closed.
 */
internal class RelaySockets(
    private val client: NostrClient,
    private val pinnedUrls: Set<NormalizedRelayUrl>,
) : AliasFolding.Sockets {
    private val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    override fun claim(url: NormalizedRelayUrl) {
        held.merge(url, 1, Int::plus)
    }

    override fun release(url: NormalizedRelayUrl) {
        var released = false
        held.compute(url) { _, n ->
            if (n == null) {
                null
            } else {
                released = true
                (n - 1).takeIf { it > 0 }
            }
        }
        // A release nobody claimed used to be treated as a 1-count — which
        // turned a bookkeeping bug elsewhere into disconnecting a socket its
        // real holder was still on. Said loudly and dropped instead.
        if (!released) {
            System.err.println("router: socket release for ${url.url} that nobody claimed — a claim/release imbalance upstream of this line")
            return
        }
        // Re-checked after the compute: a claim landing in between keeps the
        // socket. The residual race (claim after this check) costs one
        // reconnect, not a wrong count.
        if (held[url] == null && url !in pinnedUrls) {
            close(url)
        }
    }

    /**
     * Close this url's socket IF QUARTZ STILL HAS ONE — and never create a
     * relay in order to close it.
     *
     * This used to be a bare `client.getOrCreateRelay(url).disconnect()`, which
     * reads as a no-op on a url with no connection and is the opposite:
     * **`getOrCreateRelay` is a constructor**, and quartz's pool has usually
     * dropped the url before we get here. `NostrClient` reconciles its pool
     * against the relays its live subscriptions want (`updatePool`, sampled at
     * 300ms), so the url leaves the pool shortly after the fetch's own
     * `unsubscribe` — and a `getOrCreate` after that puts a FRESH
     * `BasicRelayClient` back into it. Nothing is subscribed to it, so nothing
     * will ever remove it again.
     *
     * That entry is then dialled. `NostrClient` runs a 60-second keep-alive
     * (`reconnectIfNeedsTo`) that calls `connectAndSyncFiltersIfDisconnected`
     * on every disconnected relay the pool holds, and `disconnect()` sets
     * `lastConnectTentativeInSeconds = 0` — documented in quartz as "this is
     * not an error, so prepare to reconnect as soon as requested" — so the
     * backoff does not hold it back either. The socket opens, carries no
     * subscription, and is never closed by anything.
     *
     * Measured on a staging pod: 105 threads in
     * `okhttp3.internal.ws.RealWebSocket.loopReader`, of which 5 were the
     * static stream upstreams and about 100 were left over from probe passes
     * that had FINISHED, the oldest silent for over ninety minutes against a
     * 20-second idle budget. On a 1,024-socket dispatcher that is a second
     * failure waiting to happen.
     *
     * So the membership check is the fix, and there is nothing to close when it
     * fails: a url quartz has already dropped was disconnected on its way out
     * of the pool. What this cannot do is REMOVE the entry — `removeRelay` is
     * `RelayPool`'s and the pool is private to `NostrClient` — so the one case
     * left is a url still in the pool because our own release beat the 300ms
     * reconcile, which is the case where disconnecting is exactly right.
     */
    private fun close(url: NormalizedRelayUrl) {
        if (url !in client.availableRelaysFlow().value) return
        runCatching { client.getOrCreateRelay(url).disconnect() }
    }
}
