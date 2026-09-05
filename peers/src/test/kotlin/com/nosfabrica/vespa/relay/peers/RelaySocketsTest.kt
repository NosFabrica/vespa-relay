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
 * The refcount must not open the socket it was asked to close: quartz's
 * `getOrCreateRelay` is a constructor, and its keep-alive dials every
 * disconnected relay the pool holds. Asserted on pool membership.
 */
class RelaySocketsTest {
    private val probed = RelayUrlNormalizer.normalize("wss://probed.example")
    private val pinned = RelayUrlNormalizer.normalize("wss://pinned.example")

    /** Never connects and never fails; the tests are about the pool, not the wire. */
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

            // The pool holds nothing for this url: the state a release lands in once the fetch has unsubscribed.
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
            // Two holders, one release: closing on the first would cut a transfer still running under it.
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
            // Read as a 1-count going to zero, an unmatched release disconnects a socket its real holder is on.
            sockets.release(probed)
            assertEquals(0, builder.built.get())
            assertTrue(probed !in client.availableRelaysFlow().value)
        } finally {
            client.close()
        }
    }
}
