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
 * `Onion-Location` as Amethyst (on any response, the 101 included) and Tor
 * Browser read it: an `http://` url, since okhttp parses a ws scheme to null.
 */
class RelayOnionLocationTest {
    private val clearnet = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val onion = "http://${"n".repeat(56)}.onion/"

    private fun <T> serving(
        advertise: () -> String?,
        landingPage: String? = null,
        block: (port: Int) -> T,
    ): T {
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = clearnet), clearnet)
        // Port 0, so tests never collide with a developer's running relay.
        val server =
            serveRelay(
                relay = relay,
                port = 0,
                nip11 = Nip11Info(),
                landingPage = landingPage,
                onionLocation = advertise,
                wait = false,
            )
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

    /** Spoken by hand: no http client exposes a 101's headers, and the `Host` sent is under test. */
    private fun headersOf(
        port: Int,
        host: String,
        upgrade: Boolean,
        // Null asks for the page; the default asks for the NIP-11 doc.
        accept: String? = "application/nostr+json",
        acceptGzip: Boolean = false,
        ifNoneMatch: String? = null,
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
                        accept?.let { append("Accept: $it\r\n") }
                        if (acceptGzip) append("Accept-Encoding: gzip\r\n")
                        ifNoneMatch?.let { append("If-None-Match: $it\r\n") }
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

    @Test
    fun `the websocket handshake advertises the hidden service`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "localhost:$port", upgrade = true)
            assertTrue(headers.first().contains("101"), "the upgrade still happens: ${headers.first()}")
            assertEquals(onion, headers.header("Onion-Location"), "a client that only opens the socket must still learn it")
        }
    }

    /** Amethyst keys its cache by the host asked for, so the onion would become its own alternative. */
    @Test
    fun `a request that already arrived on the onion is not told about it`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "${"n".repeat(56)}.onion", upgrade = false)
            assertNull(headers.header("Onion-Location"))
        }
    }

    /** `Host: <onion>:80` is what a client configured with the port sends. */
    @Test
    fun `a request on the onion with an explicit port is not told about it either`() {
        serving({ onion }) { port ->
            val headers = headersOf(port, "${"n".repeat(56)}.onion:80", upgrade = false)
            assertNull(headers.header("Onion-Location"))
        }
    }

    /** Compression replaces the body; a header set before the handler ran has to survive that. */
    @Test
    fun `a gzipped page still carries the header`() {
        // Over the plugin's 1KB floor, or there is nothing to compress.
        val page = "<html><body>" + "search ".repeat(500) + "</body></html>"
        serving({ onion }, landingPage = page) { port ->
            val headers = headersOf(port, "localhost:$port", upgrade = false, accept = null, acceptGzip = true)
            assertEquals("gzip", headers.header("Content-Encoding"), "the body is compressed: $headers")
            assertEquals(onion, headers.header("Onion-Location"))
        }
    }

    /** A 304 carries a trimmed header set; the advertisement has to survive it. */
    @Test
    fun `a 304 still carries the header`() {
        val page = "<html><body>search</body></html>"
        serving({ onion }, landingPage = page) { port ->
            val fresh = headersOf(port, "localhost:$port", upgrade = false, accept = null)
            val etag = fresh.header("ETag")
            assertTrue(etag != null, "the page revalidates with an ETag: $fresh")

            val revalidated = headersOf(port, "localhost:$port", upgrade = false, accept = null, ifNoneMatch = etag)
            assertTrue(revalidated.first().contains("304"), "the second visit revalidates: ${revalidated.first()}")
            assertEquals(onion, revalidated.header("Onion-Location"), "…and is still told about the hidden service")
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
