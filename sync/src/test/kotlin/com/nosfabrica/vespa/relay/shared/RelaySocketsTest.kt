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
package com.nosfabrica.vespa.relay.shared

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * THE REFCOUNT MUST NOT OPEN THE SOCKET IT WAS ASKED TO CLOSE.
 *
 * ## What was leaking
 *
 * A staging pod carried 105 threads in `okhttp3.internal.ws.RealWebSocket.loopReader`,
 * each parked on a read. Five were the static stream upstreams. About a hundred
 * were left over from probe passes that had FINISHED — the oldest silent for
 * over ninety minutes against a 20-second idle budget — and no probe socket had
 * been opened for eighteen minutes before the pod was looked at.
 *
 * The release path is what opened them. It read
 * `client.getOrCreateRelay(url).disconnect()`, which looks like a no-op on a
 * url with no connection and is the opposite: **`getOrCreateRelay` is a
 * constructor.** Quartz reconciles its pool against the relays its live
 * subscriptions want (`updatePool`, off a flow sampled at 300ms), so a probed
 * url normally leaves the pool moments after the fetch's own `unsubscribe` —
 * and a `getOrCreate` after that puts a fresh relay client back in. Nothing is
 * subscribed to it, so nothing removes it again, and `NostrClient`'s 60-second
 * keep-alive (`reconnectIfNeedsTo`) then dials every disconnected relay the
 * pool holds. `disconnect()` even clears the backoff on its way out —
 * "this is not an error, so prepare to reconnect as soon as requested".
 *
 * So the leak is per RELEASE, not per dial, which is why it grew with passes
 * that had already reported themselves finished.
 *
 * ## What is asserted
 *
 * Pool membership, on a real `NostrClient` over a socket builder that counts
 * dials. Membership is the fact that matters: an entry in the pool is what the
 * keep-alive walks, so a url that is not in it cannot be resurrected however
 * many times the keep-alive runs.
 */
class RelaySocketsTest {
    private val probed = RelayUrlNormalizer.normalize("wss://probed.example")
    private val pinned = RelayUrlNormalizer.normalize("wss://pinned.example")

    /** Never connected, never fails — this test is about the POOL, not the wire. */
    private class CountingSockets : WebsocketBuilder {
        val built = AtomicInteger()
        val live = ConcurrentHashMap.newKeySet<String>()

        override fun build(
            url: NormalizedRelayUrl,
            out: WebSocketListener,
        ): WebSocket {
            built.incrementAndGet()
            return object : WebSocket {
                override fun needsReconnect() = false

                override fun connect() {
                    live += url.url
                }

                override fun disconnect() {
                    live -= url.url
                }

                override fun send(msg: String) = true
            }
        }
    }

    @Test
    fun `releasing a url quartz has already dropped does not put it back in the pool`() {
        val builder = CountingSockets()
        val client = NostrClient(builder)
        try {
            val sockets = RelaySockets(client, pinnedUrls = emptySet())

            // The ordinary probe-pass shape, minus the fetch: claim, dial,
            // release. Nothing ever subscribed to this url, so quartz's pool
            // holds nothing for it — which is exactly the state a release lands
            // in once `unsubscribe` and the 300ms reconcile have run.
            sockets.claim(probed)
            sockets.release(probed)

            assertTrue(
                probed !in client.availableRelaysFlow().value,
                "the release created a pool entry for a url nothing is using — the keep-alive dials those every 60s",
            )
            assertEquals(0, builder.built.get(), "…and building a relay client at all is what made the entry")
        } finally {
            client.close()
        }
    }

    @Test
    fun `a pinned url is left alone, and a second holder keeps the socket`() {
        val builder = CountingSockets()
        val client = NostrClient(builder)
        try {
            val sockets = RelaySockets(client, pinnedUrls = setOf(pinned))
            // Two holders, one release: the count is what decides, and closing
            // on the first release would cut a transfer still running under it.
            sockets.claim(probed)
            sockets.claim(probed)
            sockets.release(probed)
            assertEquals(0, builder.built.get(), "a url still held must not be touched at all")

            // A pinned url is never closed, so it is never asked for either.
            sockets.claim(pinned)
            sockets.release(pinned)
            assertEquals(0, builder.built.get())
        } finally {
            client.close()
        }
    }

    @Test
    fun `a release nobody claimed is refused rather than treated as a holder leaving`() {
        val builder = CountingSockets()
        val client = NostrClient(builder)
        try {
            val sockets = RelaySockets(client, pinnedUrls = emptySet())
            // The bookkeeping bug this guard exists for: an unmatched release
            // used to read as a 1-count going to zero, which disconnected a
            // socket its real holder was still on.
            sockets.release(probed)
            assertEquals(0, builder.built.get())
            assertTrue(probed !in client.availableRelaysFlow().value)
        } finally {
            client.close()
        }
    }
}
