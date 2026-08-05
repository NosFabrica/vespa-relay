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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.router.discovery.Unreachability
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

/**
 * The Tor transport: which client a url gets, and — the one that matters —
 * that a `.onion` name is handed to the proxy rather than looked up here.
 *
 * The second is asserted against a real SOCKS5 handshake into a fake proxy,
 * because it is a property of OkHttp's route selection rather than of any code
 * in this repo: an upgrade that resolved the host before connecting would keep
 * every mock-based test passing while leaking the name of every hidden service
 * this router syncs with to the local resolver, and breaking .onion outright.
 */
class TorTransportTest {
    /**
     * A SOCKS5 server that gets as far as reading the request and then hangs
     * up, recording what it was asked for. Enough to observe the address type
     * and the host — which is the whole question.
     */
    private class FakeSocks(
        // A SOCKS5 reply code to answer the request with, instead of hanging
        // up. 4 is "host unreachable" — what a working Tor says about a hidden
        // service it could not reach.
        private val replyCode: Int? = null,
    ) : AutoCloseable {
        private val server = ServerSocket(0)
        val port: Int get() = server.localPort
        val asked = ArrayBlockingQueue<String>(8)

        init {
            thread(isDaemon = true) {
                while (!server.isClosed) {
                    val socket = runCatching { server.accept() }.getOrNull() ?: return@thread
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
                    // DOMAINNAME — the proxy resolves it. The answer we want.
                    3 -> {
                        val host = ByteArray(input.readUnsignedByte())
                        input.readFully(host)
                        asked.offer("domain:${String(host)}:${input.readUnsignedShort()}")
                    }

                    // IPV4 — something on this side already resolved the name.
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
        everything: Boolean = false,
    ) = TorSettings(
        socksHost = host,
        socksPort = port,
        everything = everything,
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
            // The dial fails — the fake hangs up mid-handshake — and that is
            // fine: what is under test is what the proxy was ASKED.
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

    /**
     * A proxy saying "I could not reach that host" must never become a signed
     * NIP-66 record. It is the PROXY's report, and it does not separate their
     * service being down from our circuit not being built.
     *
     * The type is what decides it — [Unreachability] publishes on
     * `UnknownHostException` — so this asserts what actually comes out of a
     * SOCKS failure. Checked against a live tor while building this: an
     * unreachable `.onion` surfaces as `SocketException`, never as a name that
     * failed to resolve, because nothing here ever tried to resolve it.
     */
    @Test
    fun `a SOCKS failure is never proof the relay is unreachable`() {
        // 4 is SOCKS5 "host unreachable".
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
        val transport = TorTransport(settings(port = 9050, everything = true), direct)
        assertTrue(transport.routes(clearnet))
        assertTrue(transport.clientFor(clearnet) !== direct)
    }

    /**
     * The Tor client must not inherit the clearnet dispatcher. `newBuilder()`
     * shares it, and sharing it would let a handful of onion dials queue in —
     * and draw down — the 1024-socket budget the whole fan-out is sized
     * against.
     */
    @Test
    fun `the tor client gets its own socket budget`() {
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
    fun `SYNC_TOR_SOCKS parses host and port, with or without a scheme`() {
        val plain = assertNotNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:9050")))
        assertEquals("tor", plain.socksHost)
        assertEquals(9050, plain.socksPort)
        assertFalse(plain.everything)
        assertEquals(TorSettings.DEFAULT_CONNECT_TIMEOUT_SEC, plain.connectTimeoutSec)

        val scheme = assertNotNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "socks5://127.0.0.1:9150")))
        assertEquals("127.0.0.1", scheme.socksHost)
        assertEquals(9150, scheme.socksPort)
    }

    @Test
    fun `unset is the clearnet deployment, blank included`() {
        assertNull(TorSettings.fromEnv(emptyMap()))
        assertNull(TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "   ")))
    }

    /**
     * A value that cannot be parsed must not degrade to "no Tor": the setting
     * exists to turn something on, and silently ignoring it is how a mirror
     * stops mirroring while every log line reads healthy.
     */
    @Test
    fun `a malformed proxy address is a hard error, not a fallback`() {
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor")) }
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:abc")) }
        assertFailsWith<IllegalArgumentException> { TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to "tor:99999")) }
    }

    /**
     * The pre-probe is a plain socket to a resolved address, so it cannot say
     * anything about a hidden service — and asking would both fail and hand
     * the name to the local resolver.
     */
    @Test
    fun `the TCP pre-probe is skipped for onion relays and kept for everything else`() {
        assertFalse(shouldPreProbe(onion))
        assertTrue(shouldPreProbe(clearnet))
    }

    /**
     * A `.onion` in a stream's `urls` is accepted by the parser — the config
     * cannot know what transport the process has — and reported here, so
     * [SyncMain] can refuse to boot instead of mirroring nothing forever.
     */
    @Test
    fun `configured onion upstreams are reported with the stream that names them`() {
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
        assertTrue(s.everything)
        assertEquals(5, s.connectTimeoutSec, "a sub-5s connect timeout cannot survive a rendezvous")
        assertEquals(512, s.maxSockets)
    }
}
