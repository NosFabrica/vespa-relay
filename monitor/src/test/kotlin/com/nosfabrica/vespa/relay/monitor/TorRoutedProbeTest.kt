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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.probeIdleMs
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.DataInputStream
import java.net.ServerSocket
import java.net.Socket
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** What routing through Tor changes for this plane: which relays it pre-probes, how long it waits, what it may publish. */
class TorRoutedProbeTest {
    /** A SOCKS5 server that completes the handshake and then refuses with [replyCode]. */
    private class RefusingSocks(
        /** 4 is "host unreachable", what a working Tor says about a hidden service it could not reach. */
        private val replyCode: Int,
    ) : AutoCloseable {
        private val server = ServerSocket(0)
        val port: Int get() = server.localPort

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
                input.readUnsignedByte()
                val methods = ByteArray(input.readUnsignedByte())
                input.readFully(methods)
                // "No authentication", which is what a stock tor SocksPort wants.
                out.write(byteArrayOf(5, 0))
                out.flush()
                // Request: version, command, reserved, address type, then the address it names.
                input.readUnsignedByte()
                input.readUnsignedByte()
                input.readUnsignedByte()
                when (input.readUnsignedByte()) {
                    3 -> input.readFully(ByteArray(input.readUnsignedByte()))
                    1 -> input.readFully(ByteArray(4))
                    else -> Unit
                }
                // VER, REP, RSV, ATYP=IPv4, BND.ADDR, BND.PORT.
                out.write(byteArrayOf(5, replyCode.toByte(), 0, 1, 0, 0, 0, 0, 0, 0))
                out.flush()
            }
        }

        override fun close() = server.close()
    }

    private fun settings(
        port: Int,
        routeAll: Boolean = false,
    ) = TorSettings(
        socksHost = "127.0.0.1",
        socksPort = port,
        routeAll = routeAll,
        connectTimeoutSec = 5,
        maxSockets = 4,
    )

    private val onion = RelayUrlNormalizer.normalize("ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion")
    private val clearnet = RelayUrlNormalizer.normalize("wss://relay.example.com")

    @Test
    fun `a SOCKS failure is never proof the relay is unreachable`() {
        // "Host unreachable" from the proxy does not separate their service being down from our circuit failing.
        RefusingSocks(replyCode = 4).use { socks ->
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
}
