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
 * One socket refcount across every stream and every probe pass: a socket closes only when its
 * last holder releases it, inside that url's atomic remap of [held], so no claim lands between
 * the count reaching zero and the close. Pinned urls are never closed.
 */
class RelaySockets(
    private val client: NostrClient,
    private val pinnedUrls: Set<NormalizedRelayUrl>,
    /** The owning process's log prefix: `router` for the sync, `monitor` for the monitor. */
    private val logPrefix: String = "router",
) : Sockets {
    private val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    override fun claim(url: NormalizedRelayUrl) {
        held.merge(url, 1, Int::plus)
    }

    override fun release(url: NormalizedRelayUrl) {
        var unclaimed = false
        held.compute(url) { _, n ->
            when {
                // A release nobody claimed must not disconnect a socket its real holder is still on.
                n == null -> {
                    unclaimed = true
                    null
                }

                n > 1 -> {
                    n - 1
                }

                else -> {
                    // Inside the remap: [close] touches only the client, never [held].
                    if (url !in pinnedUrls) close(url)
                    null
                }
            }
        }
        if (unclaimed) {
            System.err.println("$logPrefix: socket release for ${url.url} that nobody claimed — a claim/release imbalance upstream of this line")
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
}
