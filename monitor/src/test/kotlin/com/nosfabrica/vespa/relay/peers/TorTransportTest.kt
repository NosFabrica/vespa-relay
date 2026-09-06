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

import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.monitor.Unreachability
import com.nosfabrica.vespa.relay.monitor.shouldPreProbe
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.DataInputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Which client a url gets, and that a `.onion` name reaches the proxy unresolved, on a real SOCKS5 handshake. */
class TorTransportTest {
    /** A SOCKS5 server that records the address type and host asked for, and hangs up unless given a [replyCode]. */
    private class FakeSocks(
        /** 4 is "host unreachable", what a working Tor says about a hidden service it could not reach. */
        private val replyCode: Int? = null,
    ) : AutoCloseable {
        private val server = ServerSocket(0)
        val port: Int get() = server.localPort
        val asked = ArrayBlockingQueue<String>(8)

        val accepted =
            java.util.concurrent.atomic
                .AtomicInteger()

        /** [accepted] once it has stopped moving; a connect returns before the accept loop has counted it. */
        fun settledAccepts(
            quietMs: Long = 250,
            capMs: Long = 5_000,
        ): Int {
            val deadline = System.nanoTime() + capMs * 1_000_000
            var seen = -1
            var changedAt = System.nanoTime()
            while (System.nanoTime() < deadline) {
                val now = accepted.get()
                if (now != seen) {
                    seen = now
                    changedAt = System.nanoTime()
                } else if (now > 0 && System.nanoTime() - changedAt > quietMs * 1_000_000) {
                    return now
                }
                Thread.sleep(5)
            }
            return accepted.get()
        }

        init {
            thread(isDaemon = true) {
                while (!server.isClosed) {
                    val socket = runCatching { server.accept() }.getOrNull() ?: return@thread
                    accepted.incrementAndGet()
                    thread(isDaemon = true) { runCatching { serve(socket) } }
                }
            }
        }

        private fun serve(socket: Socket) {
            socket.use {
                val input = DataInputStream(it.getInputStream())
                val out = it.getOutputStream()
                // Greeting: version, method count, methods.
                assertEquals(5, input.readUnsignedByte())
                val methods = ByteArray(input.readUnsignedByte())
                input.readFully(methods)
                // "No authentication", which is what a stock tor SocksPort wants.
                out.write(byteArrayOf(5, 0))
                out.flush()
                // Request: version, command, reserved, address type.
                input.readUnsignedByte()
                input.readUnsignedByte()
                input.readUnsignedByte()
                when (val type = input.readUnsignedByte()) {
                    // DOMAINNAME: the proxy resolves it.
                    3 -> {
                        val host = ByteArray(input.readUnsignedByte())
                        input.readFully(host)
                        asked.offer("domain:${String(host)}:${input.readUnsignedShort()}")
                    }

                    // IPV4: something on this side already resolved the name.
                    1 -> {
                        val ip = ByteArray(4)
                        input.readFully(ip)
                        asked.offer("ipv4:${ip.joinToString(".") { b -> (b.toInt() and 0xff).toString() }}")
                    }

                    else -> {
                        asked.offer("atyp:$type")
                    }
                }
                // VER, REP, RSV, ATYP=IPv4, BND.ADDR, BND.PORT.
                replyCode?.let { code ->
                    out.write(byteArrayOf(5, code.toByte(), 0, 1, 0, 0, 0, 0, 0, 0))
                    out.flush()
                }
            }
        }

        /** What the proxy was asked to connect to, or null if it was never asked. */
        fun awaitAsk(): String? = asked.poll(10, TimeUnit.SECONDS)

        override fun close() = server.close()
    }

    private fun settings(
        host: String = "127.0.0.1",
        port: Int,
        routeAll: Boolean = false,
    ) = TorSettings(
        socksHost = host,
        socksPort = port,
        routeAll = routeAll,
        connectTimeoutSec = 5,
        maxSockets = 4,
    )

    private val onion = RelayUrlNormalizer.normalize("ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion")
    private val clearnet = RelayUrlNormalizer.normalize("wss://relay.example.com")

    @Test
    fun `an onion host reaches the proxy as a name, never as an address`() {
        FakeSocks().use { socks ->
            val transport = TorTransport(settings(port = socks.port), OkHttpClient())
            val client = transport.clientFor(onion)
            // The dial fails at the fake's hang-up; what is under test is what the proxy was asked.
            runCatching {
                client
                    .newCall(Request.Builder().url("http://${java.net.URI(onion.url).host}/").build())
                    .execute()
                    .close()
            }
            val ask = assertNotNull(socks.awaitAsk(), "the proxy was never asked to connect — nothing was routed through it")
            assertTrue(
                ask.startsWith("domain:"),
                "the hostname must travel to the proxy; this resolved it locally first ($ask)",
            )
            assertEquals("domain:vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion:80", ask)
        }
    }

    @Test
    fun `a SOCKS failure is never proof the relay is unreachable`() {
        // "Host unreachable" from the proxy does not separate their service being down from our circuit failing.
        FakeSocks(replyCode = 4).use { socks ->
            val transport = TorTransport(settings(port = socks.port), OkHttpClient())
            val failure =
                runCatching {
                    transport
                        .clientFor(onion)
                        .newCall(Request.Builder().url("http://${java.net.URI(onion.url).host}/").build())
                        .execute()
                        .close()
                }.exceptionOrNull()

            val e = assertNotNull(failure, "the dial should have failed — the proxy refused it") as Exception
            assertFalse(
                Unreachability.proves(e),
                "a ${e.javaClass.simpleName} through the proxy would be published as a signed claim about someone else's server",
            )
        }
    }

    @Test
    fun `clearnet keeps the direct client, onion takes the tor one`() {
        val direct = OkHttpClient()
        val transport = TorTransport(settings(port = 9050), direct)
        assertSame(direct, transport.clientFor(clearnet))
        assertTrue(transport.routes(onion))
        assertFalse(transport.routes(clearnet))
        assertTrue(transport.clientFor(onion) !== direct)
    }

    @Test
    fun `SYNC_TOR_ALL routes clearnet through Tor too`() {
        val direct = OkHttpClient()
        val transport = TorTransport(settings(port = 9050, routeAll = true), direct)
        assertTrue(transport.routes(clearnet))
        assertTrue(transport.clientFor(clearnet) !== direct)
    }

    @Test
    fun `the tor client gets its own socket budget`() {
        // `newBuilder()` shares the dispatcher, so onion dials would draw down the clearnet budget.
        val direct = OkHttpClient()
        val transport = TorTransport(settings(port = 9050), direct)
        val tor = transport.clientFor(onion)
        assertTrue(tor.dispatcher !== direct.dispatcher)
        assertEquals(4, tor.dispatcher.maxRequests)
    }

    @Test
    fun `a proxy that is not there answers false, and is not asked again inside the TTL`() {
        // Port 1 on loopback: nothing listens, and the refusal is immediate.
        val transport = TorTransport(settings(port = 1), OkHttpClient())
        val start = 1_000_000L
        assertFalse(transport.socksAnswers(start))
        assertFalse(transport.socksAnswers(start + TorSettings.PROBE_TTL_MS - 1))
    }

    @Test
    fun `a listening proxy answers true`() {
        FakeSocks().use { socks ->
            assertTrue(TorTransport(settings(port = socks.port), OkHttpClient()).socksAnswers())
        }
    }

    @Test
    fun `a fan-out asking at once costs one probe, not one per caller`() {
        // When the TTL expires every runnable thread reaches the check together.
        FakeSocks().use { socks ->
            val transport = TorTransport(settings(port = socks.port), OkHttpClient())
            val start = java.util.concurrent.CountDownLatch(1)
            val done = java.util.concurrent.CountDownLatch(32)
            repeat(32) {
                thread(isDaemon = true) {
                    start.await()
                    transport.socksAnswers()
                    done.countDown()
                }
            }
            start.countDown()
            assertTrue(done.await(20, TimeUnit.SECONDS), "the callers should not be blocked behind each other")
            assertEquals(1, socks.settledAccepts(), "32 callers opened ${socks.accepted.get()} connections to the proxy")
        }
    }

    @Test
    fun `SYNC_TOR_SOCKS parses host and port, with or without a scheme`() {
        val plain = assertNotNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:9050")))
        assertEquals("tor", plain.socksHost)
        assertEquals(9050, plain.socksPort)
        assertFalse(plain.routeAll)
        assertEquals(TorSettings.DEFAULT_CONNECT_TIMEOUT_SEC, plain.connectTimeoutSec)

        val scheme = assertNotNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "socks5://127.0.0.1:9150")))
        assertEquals("127.0.0.1", scheme.socksHost)
        assertEquals(9150, scheme.socksPort)

        // InetSocketAddress wants the IPv6 literal without its brackets.
        val v6 = assertNotNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "[::1]:9050")))
        assertEquals("::1", v6.socksHost)
        assertEquals(9050, v6.socksPort)
    }

    @Test
    fun `unset is the clearnet deployment, blank included`() {
        assertNull(TorSettings.fromEnv(emptyMap()))
        assertNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "   ")))
    }

    @Test
    fun `a malformed proxy address is a hard error, not a fallback`() {
        // Silently ignoring a bad value is how a mirror stops mirroring while every log line reads healthy.
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor")) }
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:abc")) }
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:99999")) }
    }

    @Test
    fun `the TCP pre-probe is skipped for onion relays and kept for everything else`() {
        // The pre-probe is a plain socket to a resolved address, which would hand the name to the local resolver.
        val tor = TorTransport(settings(port = 9050), OkHttpClient())
        assertFalse(shouldPreProbe(onion, tor))
        assertTrue(shouldPreProbe(clearnet, tor))
        assertTrue(shouldPreProbe(clearnet, null))
    }

    @Test
    fun `an onion fingerprint gets the Tor budget on top of the clearnet window`() {
        // Quartz's idle window runs from the start of the fetch, so an onion fingerprint pays for the circuit.
        val tor = TorTransport(settings(port = 9050), OkHttpClient())
        assertEquals(25_000L, probeIdleMs(onion, tor, 20_000L))
        assertEquals(20_000L, probeIdleMs(clearnet, tor, 20_000L))
        // No transport: nothing routes through a proxy, so nothing pays for one.
        assertEquals(20_000L, probeIdleMs(onion, null, 20_000L))
    }

    @Test
    fun `SYNC_TOR_ALL takes the onion rules with it to clearnet relays`() {
        // A direct pre-probe would dial every relay from this box's own address, which the setting hides.
        val all = TorTransport(settings(port = 9050, routeAll = true), OkHttpClient())
        assertFalse(shouldPreProbe(clearnet, all), "SYNC_TOR_ALL must not leave a direct probe of every relay running")
        assertFalse(shouldPreProbe(onion, all))
        assertTrue(all.routes(clearnet), "the strike and UNREACHABLE guards key on this")
        // The fold's window follows too: a clearnet relay is reached through a circuit here.
        assertEquals(25_000L, probeIdleMs(clearnet, all, 20_000L))
    }

    @Test
    fun `configured onion upstreams are reported with the stream that names them`() {
        // The parser accepts a `.onion` in `urls`; this report is how [SyncMain] refuses to boot on one.
        val config =
            RouterConfigLoader.parse(
                """
                streams {
                  hidden {
                    dir = "down"
                    filter = { "kinds": [1] }
                    urls = [
                      "ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion",
                      "wss://relay.example.com"
                    ]
                  }
                  clear {
                    dir = "down"
                    filter = { "kinds": [1] }
                    urls = [ "wss://other.example.com" ]
                  }
                }
                """.trimIndent(),
            )

        assertEquals(
            listOf("hidden: ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion/"),
            onionUpstreams(config.streams),
        )
    }

    @Test
    fun `the tunables are read and clamped`() {
        val s =
            assertNotNull(
                TorSettings.fromEnv(
                    mapOf(
                        "SYNC_TOR_SOCKS" to "tor:9050",
                        "SYNC_TOR_ALL" to "true",
                        "SYNC_TOR_CONNECT_TIMEOUT_SECONDS" to "1",
                        "SYNC_TOR_MAX_SOCKETS" to "9000",
                    ),
                ),
            )
        assertTrue(s.routeAll)
        assertEquals(5, s.connectTimeoutSec, "a sub-5s connect timeout cannot survive a rendezvous")
        assertEquals(512, s.maxSockets)
    }
}
