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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * One socket refcount across every stream and every probe pass. Two streams
 * routinely land on one relay while a probe fingerprints it, so a socket
 * closes only when its last holder releases. Pinned urls are never closed.
 */
class RelaySockets(
    private val client: NostrClient,
    private val pinnedUrls: Set<NormalizedRelayUrl>,
) : Sockets {
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
        // A release nobody claimed is an upstream bug; it must not disconnect
        // a socket its real holder is still on.
        if (!released) {
            System.err.println("router: socket release for ${url.url} that nobody claimed — a claim/release imbalance upstream of this line")
            return
        }
        // Re-checked after the compute: a claim landing in between keeps the
        // socket. A claim after this check costs one reconnect, not a wrong count.
        if (held[url] == null && url !in pinnedUrls) {
            close(url)
        }
    }

    /**
     * Close this url's socket only if quartz still holds one. `getOrCreateRelay`
     * is a constructor: on a url the pool has already dropped it would put a
     * fresh, unsubscribed relay back in, which the keep-alive then dials and
     * nothing ever closes.
     */
    private fun close(url: NormalizedRelayUrl) {
        if (url !in client.availableRelaysFlow().value) return
        runCatching { client.getOrCreateRelay(url).disconnect() }
    }
}
