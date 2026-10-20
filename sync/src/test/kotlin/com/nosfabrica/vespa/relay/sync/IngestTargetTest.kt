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
package com.nosfabrica.vespa.relay.sync

import com.sun.net.httpserver.HttpServer
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** `SYNC_INGEST` and its url, read strictly, and the relay's key read off its NIP-11 document. */
class IngestTargetTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://relay.example.com/")

    @Test
    fun `local is the default and ignores the url compose always sets`() {
        assertFalse(ingestTargetFromEnv(emptyMap(), relayUrl).viaRelay)
        assertNull(ingestTargetFromEnv(mapOf(SYNC_INGEST_URL_ENV to "ws://relay:7777"), relayUrl).dial)
    }

    @Test
    fun `relay dials the url, or RELAY_URL without one`() {
        val compose = ingestTargetFromEnv(mapOf(SYNC_INGEST_ENV to "relay", SYNC_INGEST_URL_ENV to "ws://relay:7777"), relayUrl)
        assertEquals(RelayUrlNormalizer.normalize("ws://relay:7777"), compose.dial)
        assertEquals(relayUrl, ingestTargetFromEnv(mapOf(SYNC_INGEST_ENV to "Relay"), relayUrl).dial)
    }

    @Test
    fun `a mode or a url that does not parse stops the boot`() {
        val mode = assertFailsWith<IllegalStateException> { ingestTargetFromEnv(mapOf(SYNC_INGEST_ENV to "store"), relayUrl) }
        assertTrue("local, relay" in mode.message.orEmpty(), mode.message)
        assertFailsWith<IllegalStateException> {
            ingestTargetFromEnv(mapOf(SYNC_INGEST_ENV to "relay", SYNC_INGEST_URL_ENV to "not a url"), relayUrl)
        }
    }

    @Test
    fun `the relay's key is its NIP-11 self, and a relay that does not answer states none`() {
        val key = "a".repeat(64)
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    val asked = exchange.requestHeaders.getFirst("Accept")
                    val body = (if (asked == "application/nostr+json") """{"name":"r","self":"${key.uppercase()}"}""" else "<html/>").toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                start()
            }
        try {
            assertEquals(key, relaySelfPubkey(RelayUrlNormalizer.normalize("ws://127.0.0.1:${server.address.port}")))
        } finally {
            server.stop(0)
        }
        assertNull(relaySelfPubkey(RelayUrlNormalizer.normalize("ws://127.0.0.1:${server.address.port}")))
    }
}
