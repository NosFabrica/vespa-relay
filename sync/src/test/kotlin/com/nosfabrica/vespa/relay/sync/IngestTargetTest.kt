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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Where the writes go, read strictly, and the relay's key read off its NIP-11 document. */
class IngestTargetTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://relay.example.com/")

    @Test
    fun `the writes dial the url compose sets, or RELAY_URL without one`() {
        assertEquals(RelayUrlNormalizer.normalize("ws://relay:7777"), ingestDialFromEnv(mapOf(SYNC_INGEST_URL_ENV to "ws://relay:7777"), relayUrl))
        assertEquals(relayUrl, ingestDialFromEnv(emptyMap(), relayUrl))
        assertEquals(relayUrl, ingestDialFromEnv(mapOf(SYNC_INGEST_URL_ENV to " "), relayUrl))
    }

    @Test
    fun `a leftover SYNC_INGEST=relay is the only mode, and accepted`() {
        assertEquals(relayUrl, ingestDialFromEnv(mapOf(SYNC_INGEST_ENV to "Relay"), relayUrl))
    }

    @Test
    fun `SYNC_INGEST=local stops the boot and says why, rather than quietly writing through the relay`() {
        val local = assertFailsWith<IllegalStateException> { ingestDialFromEnv(mapOf(SYNC_INGEST_ENV to "local"), relayUrl) }
        assertTrue("removed" in local.message.orEmpty() && "RELAY_NSEC" in local.message.orEmpty(), local.message)
        assertFailsWith<IllegalStateException> { ingestDialFromEnv(mapOf(SYNC_INGEST_ENV to "store"), relayUrl) }
    }

    @Test
    fun `a url that does not parse stops the boot`() {
        assertFailsWith<IllegalStateException> { ingestDialFromEnv(mapOf(SYNC_INGEST_URL_ENV to "not a url"), relayUrl) }
    }

    @Test
    fun `only the relay's own key passes, another key or none is refused, and silence is not yet known`() {
        val ours = "a".repeat(64)
        assertEquals(KeyVerdict.Ours, keyVerdict(RelaySelf.Key(ours), relayUrl, ours))
        assertEquals(KeyVerdict.Pending, keyVerdict(RelaySelf.Unreachable, relayUrl, ours))
        val other = keyVerdict(RelaySelf.Key("b".repeat(64)), relayUrl, ours)
        assertTrue(other is KeyVerdict.Refused && "same RELAY_NSEC" in other.reason, "$other")
        val none = keyVerdict(RelaySelf.NoKey, relayUrl, ours)
        assertTrue(none is KeyVerdict.Refused && "states no key" in none.reason, "$none")
    }

    /** [answers] in turn, the last one repeated: a relay that is down, then comes up. */
    private fun relayAnswering(vararg answers: RelaySelf): (NormalizedRelayUrl) -> RelaySelf {
        val next = AtomicInteger()
        return { answers[minOf(next.getAndIncrement(), answers.size - 1)] }
    }

    @Test
    fun `a relay that comes up after the boot gave up, with another key, is still caught`() {
        val refused = CompletableFuture<String>()
        val read = relayAnswering(RelaySelf.Unreachable, RelaySelf.Unreachable, RelaySelf.Key("b".repeat(64)))
        watchRelayKey(relayUrl, "a".repeat(64), pollMillis = 1, read = read, onVerified = { refused.complete("verified") }, onRefused = refused::complete)
        val reason = refused.get(5, TimeUnit.SECONDS)
        assertTrue("same RELAY_NSEC" in reason, reason)
    }

    @Test
    fun `a relay that comes up with no key is caught too, and one with ours ends the watch quietly`() {
        val none = CompletableFuture<String>()
        watchRelayKey(relayUrl, "a".repeat(64), pollMillis = 1, read = relayAnswering(RelaySelf.Unreachable, RelaySelf.NoKey), onVerified = { none.complete("verified") }, onRefused = none::complete)
        assertTrue("states no key" in none.get(5, TimeUnit.SECONDS))

        val ours = CompletableFuture<String>()
        val watch = watchRelayKey(relayUrl, "a".repeat(64), pollMillis = 1, read = relayAnswering(RelaySelf.Unreachable, RelaySelf.Key("a".repeat(64))), onVerified = { ours.complete("verified") }, onRefused = ours::complete)
        assertEquals("verified", ours.get(5, TimeUnit.SECONDS))
        watch.join(5_000)
        assertFalse(watch.isAlive, "the watch stops once the relay has answered")
        assertTrue(watch.isDaemon, "it never holds the process up")
    }

    @Test
    fun `the relay's key is its NIP-11 self, a relay without one states none, and a silent one is unreachable`() {
        val key = "a".repeat(64)

        fun serving(document: String): HttpServer =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    val asked = exchange.requestHeaders.getFirst("Accept")
                    val body = (if (asked == "application/nostr+json") document else "<html/>").toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                start()
            }

        fun at(server: HttpServer) = RelayUrlNormalizer.normalize("ws://127.0.0.1:${server.address.port}")
        val keyed = serving("""{"name":"r","self":"${key.uppercase()}"}""")
        val keyless = serving("""{"name":"r"}""")
        try {
            assertEquals(RelaySelf.Key(key), relaySelf(at(keyed)))
            assertEquals(RelaySelf.NoKey, relaySelf(at(keyless)))
        } finally {
            keyed.stop(0)
            keyless.stop(0)
        }
        assertEquals(RelaySelf.Unreachable, relaySelf(at(keyed)))
    }
}
