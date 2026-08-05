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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * `Onion-Location` on the clearnet endpoint: the standard way a service says
 * "I am also this hidden service".
 *
 * The consumers decide the shape, so the tests are written against them rather
 * than against our own idea of the feature:
 *
 *  - **Amethyst** records the header from ANY response — its
 *    `OnionLocationInterceptor` is an OkHttp application interceptor over every
 *    request the app makes — and, with Tor on, rewrites the host so the
 *    connection never crosses an exit node. A Nostr client may only ever open
 *    the WEBSOCKET, so the 101 handshake carrying the header is the case that
 *    decides whether this works at all for the clients it is for.
 *  - **Tor Browser** turns it into the ".onion available" button on the page.
 *
 * The value is an `http://` url, not `ws://`, because both parse it as one:
 * okhttp's `toHttpUrl()` returns null for a ws scheme, and a null there is a
 * feature that silently does nothing.
 */
class RelayOnionLocationTest {
    private val clearnet = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val onion = "http://${"n".repeat(56)}.onion/"

    private fun <T> serving(
        advertise: () -> String?,
        block: (port: Int) -> T,
    ): T {
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = clearnet), clearnet)
        // Port 0: the OS picks a free one, so tests never collide with a
        // developer's running relay.
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), onionLocation = advertise, wait = false)
        return try {
            block(
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                },
            )
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    /**
     * One request, spoken by hand: a websocket upgrade is not something an
     * http client library will let us inspect the 101 of, and the `Host` we
     * send is itself under test.
     */
    private fun headersOf(
        port: Int,
        host: String,
        upgrade: Boolean,
    ): List<String> =
        Socket().use { socket ->
            socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
            socket.soTimeout = 5_000
            val request =
                buildString {
                    append("GET / HTTP/1.1\r\n")
                    append("Host: $host\r\n")
                    if (upgrade) {
                        append("Upgrade: websocket\r\n")
                        append("Connection: Upgrade\r\n")
                        append("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
                        append("Sec-WebSocket-Version: 13\r\n")
                    } else {
                        append("Accept: application/nostr+json\r\n")
                        append("Connection: close\r\n")
                    }
                    append("\r\n")
                }
            socket.getOutputStream().apply {
                write(request.toByteArray())
                flush()
            }
            val reader = socket.getInputStream().bufferedReader()
            buildList {
                while (true) {
                    val line = reader.readLine() ?: break
                    if (line.isEmpty()) break
                    add(line)
                }
            }
        }

    private fun List<String>.header(name: String): String? = firstOrNull { it.startsWith("$name:", ignoreCase = true) }?.substringAfter(':')?.trim()

    @Test
    fun `the NIP-11 response advertises the hidden service`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "localhost:$port", upgrade = false)
            assertTrue(headers.first().contains("200"), "the doc still serves: ${headers.first()}")
            assertEquals(onion, headers.header("Onion-Location"))
        }
    }

    /** The case the clients this is for actually hit. */
    @Test
    fun `the websocket handshake advertises the hidden service`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "localhost:$port", upgrade = true)
            assertTrue(headers.first().contains("101"), "the upgrade still happens: ${headers.first()}")
            assertEquals(onion, headers.header("Onion-Location"), "a client that only opens the socket must still learn it")
        }
    }

    /**
     * Amethyst keys its cache by the host it asked for. A request that already
     * arrived over Tor would cache the onion as its own alternative — noise at
     * best, and a self-referential entry at worst.
     */
    @Test
    fun `a request that already arrived on the onion is not told about it`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "${"n".repeat(56)}.onion", upgrade = false)
            assertNull(headers.header("Onion-Location"))
        }
    }

    @Test
    fun `a relay with no hidden service advertises nothing`() {
        serving({ null }) { port ->
            val headers = headersOf(port, "localhost:$port", upgrade = false)
            assertNull(headers.header("Onion-Location"))
            assertTrue(headers.first().contains("200"))
        }
    }
}
