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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.net.ServerSocket
import java.net.SocketTimeoutException
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/** `dialAt`: the socket goes to the given address while the relay keeps its own url. */
class PeerClientDialAtTest {
    @Test
    fun `a redirected relay is dialled at its address`() {
        val public = RelayUrlNormalizer.normalize("wss://relay.example.com/")
        ServerSocket(0).use { listener ->
            listener.soTimeout = 10_000
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            val peers = PeerClient(scope, dialAt = mapOf(public to "ws://127.0.0.1:${listener.localPort}/inside"))
            try {
                peers.client.getOrCreateRelay(public).connect()
                val socket =
                    try {
                        listener.accept()
                    } catch (e: SocketTimeoutException) {
                        fail("nothing dialled the redirect address")
                    }
                socket.use {
                    val head = it.getInputStream().bufferedReader().let { r -> generateSequence { r.readLine() }.takeWhile { l -> l.isNotEmpty() }.toList() }
                    assertTrue(head.first() == "GET /inside HTTP/1.1", "the handshake goes to the redirect's path: $head")
                    assertTrue(head.any { h -> h.equals("Upgrade: websocket", ignoreCase = true) }, "a websocket handshake: $head")
                }
            } finally {
                peers.close()
                scope.cancel()
            }
        }
    }
}
