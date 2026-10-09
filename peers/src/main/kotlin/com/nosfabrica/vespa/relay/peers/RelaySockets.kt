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
 * One socket refcount across every stream and every probe pass: a socket closes only when
 * its last holder releases it, under the url's lock, so no claim lands between that decision
 * and the close. Pinned urls are never closed.
 */
class RelaySockets(
    private val client: NostrClient,
    private val pinnedUrls: Set<NormalizedRelayUrl>,
) : Sockets {
    private val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /** Striped rather than per url, so the set of locks does not grow with every url ever dialled. */
    private val locks = Array(LOCK_STRIPES) { Any() }

    private fun lockFor(url: NormalizedRelayUrl): Any = locks[Math.floorMod(url.hashCode(), LOCK_STRIPES)]

    override fun claim(url: NormalizedRelayUrl) {
        synchronized(lockFor(url)) { held.merge(url, 1, Int::plus) }
    }

    override fun release(url: NormalizedRelayUrl) {
        synchronized(lockFor(url)) {
            val n = held[url]
            // A release nobody claimed must not disconnect a socket its real holder is still on.
            if (n == null) {
                System.err.println("router: socket release for ${url.url} that nobody claimed — a claim/release imbalance upstream of this line")
                return
            }
            if (n > 1) {
                held[url] = n - 1
                return
            }
            held.remove(url)
            if (url !in pinnedUrls) close(url)
        }
    }

    /**
     * Close this url's socket only if quartz still holds one: `getOrCreateRelay` on a url the
     * pool has dropped would put a fresh relay back in that nothing ever closes.
     */
    private fun close(url: NormalizedRelayUrl) {
        if (url !in client.availableRelaysFlow().value) return
        runCatching { client.getOrCreateRelay(url).disconnect() }
    }

    private companion object {
        const val LOCK_STRIPES = 64
    }
}
