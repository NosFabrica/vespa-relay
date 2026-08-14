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
        val stillInUse = held.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } } != null
        if (!stillInUse && url !in pinnedUrls) {
            runCatching { client.getOrCreateRelay(url).disconnect() }
        }
    }

    /** Urls with at least one holder, for the cross-stream in-flight count. */
    fun size(): Int = held.size
}
