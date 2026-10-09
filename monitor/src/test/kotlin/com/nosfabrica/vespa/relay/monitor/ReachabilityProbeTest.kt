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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.io.File
import java.io.IOException
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.TrustManagerFactory
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A host's answer is judged over every address it has, never by whichever address happened to come last. */
class ReachabilityProbeTest {
    private val url = RelayUrlNormalizer.normalize("wss://dual.example")
    private val v4 = InetAddress.getByAddress("dual.example", byteArrayOf(192.toByte(), 0, 2, 1))
    private val v6 =
        InetAddress.getByAddress(
            "dual.example",
            ByteArray(16).also {
                it[0] = 0x20
                it[1] = 0x01
                it[15] = 1
            },
        )

    private val refused = ConnectException("Connection refused")
    private val unreachableNet = ConnectException("Network is unreachable")
    private val timedOut = SocketTimeoutException("connect timed out")

    /** A probe whose resolver answers [addresses] and whose connects fail as [failures] says, per address. */
    private fun probe(
        addresses: List<InetAddress>,
        failures: Map<InetAddress, IOException?>,
        clock: () -> Long = System::currentTimeMillis,
        onConnect: (InetSocketAddress, Int) -> Unit = { _, _ -> },
    ) = ReachabilityProbe(
        tor = null,
        resolve = { addresses.toTypedArray() },
        connect = { address, timeoutMs ->
            onConnect(address, timeoutMs)
            failures[address.address]?.let { throw it }
            Socket()
        },
        clock = clock,
    )

    private fun reach(
        addresses: List<InetAddress>,
        failures: Map<InetAddress, IOException?>,
    ) = runBlocking { probe(addresses, failures).reach(url) }

    @Test
    fun `one address that connects makes the host reachable whatever the others said`() {
        assertEquals(Reach.REACHABLE, reach(listOf(v4, v6), mapOf(v4 to refused, v6 to null)))
        assertEquals(Reach.REACHABLE, reach(listOf(v4, v6), mapOf(v4 to unreachableNet, v6 to null)))
    }

    @Test
    fun `proof needs every address to have failed with proof`() {
        assertEquals(Reach.PROVED_UNREACHABLE, reach(listOf(v4, v6), mapOf(v4 to refused, v6 to refused)))
        // A timeout proves nothing, so it cannot be outvoted by a refusal on the other family.
        assertEquals(Reach.REACHABLE, reach(listOf(v4, v6), mapOf(v4 to timedOut, v6 to refused)))
        assertEquals(Reach.REACHABLE, reach(listOf(v6, v4), mapOf(v4 to timedOut, v6 to refused)))
    }

    @Test
    fun `our side needs every address to have failed on our side`() {
        assertEquals(Reach.TRANSPORT_DOWN, reach(listOf(v4, v6), mapOf(v4 to unreachableNet, v6 to unreachableNet)))
        // A host refusing on IPv4 and our IPv6 route missing is neither proof nor our outage, in either order.
        assertEquals(Reach.REACHABLE, reach(listOf(v4, v6), mapOf(v4 to refused, v6 to unreachableNet)))
        assertEquals(Reach.REACHABLE, reach(listOf(v6, v4), mapOf(v4 to refused, v6 to unreachableNet)))
    }

    @Test
    fun `a host with many slow addresses costs one deadline, and the untried ones prove nothing`() {
        // Each connect spends the whole timeout it is handed and is then refused.
        var now = 0L
        val handed = mutableListOf<Int>()
        val many = (1..8).map { InetAddress.getByAddress("many.example", byteArrayOf(192.toByte(), 0, 2, it.toByte())) }
        val reach =
            runBlocking {
                probe(many, many.associateWith { refused }, clock = { now }) { _, timeoutMs ->
                    handed += timeoutMs
                    now += timeoutMs
                }.reach(url)
            }
        assertTrue(handed.sum() <= ReachabilityProbe.TOTAL_TIMEOUT_MS, "the probe spent ${handed.sum()}ms across addresses")
        assertEquals(Reach.REACHABLE, reach, "addresses the deadline left untried must not be read as refusals")
    }

    @Test
    fun `a lookup failure with no reason left in it decides nothing`() {
        // The JVM answers a cached failed lookup with the bare hostname, whatever the first lookup said.
        fun lookup(e: UnknownHostException) = runBlocking { ReachabilityProbe(tor = null, resolve = { throw e }).reach(url) }

        assertEquals(Reach.UNEXPLAINED, lookup(UnknownHostException("dual.example")))
        assertEquals(Reach.PROVED_UNREACHABLE, lookup(UnknownHostException("dual.example: Name or service not known")))
        assertEquals(Reach.TRANSPORT_DOWN, lookup(UnknownHostException("dual.example: Temporary failure in name resolution")))
    }

    private val dir =
        File.createTempFile("tls", "").also {
            it.delete()
            it.mkdirs()
        }

    @AfterTest
    fun cleanUp() {
        dir.deleteRecursively()
    }

    /** A self-signed certificate for localhost, made by the JDK's own keytool. */
    private fun keyStore(): KeyStore {
        val file = File(dir, "relay.p12")
        val keytool = File(System.getProperty("java.home"), "bin/keytool").path
        val made =
            ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias",
                "relay",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "2",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=dns:localhost,ip:127.0.0.1",
                "-keystore",
                file.path,
                "-storetype",
                "PKCS12",
                "-storepass",
                PASS,
                "-keypass",
                PASS,
            ).redirectErrorStream(true).start()
        val said = made.inputStream.readAllBytes().decodeToString()
        check(made.waitFor() == 0) { "keytool failed: $said" }
        return KeyStore.getInstance("PKCS12").apply { file.inputStream().use { load(it, PASS.toCharArray()) } }
    }

    /** A TLS server presenting [store]'s certificate, handshaking every connection it takes. */
    private fun tlsServer(store: KeyStore): ServerSocket {
        val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply { init(store, PASS.toCharArray()) }
        val context = SSLContext.getInstance("TLS").apply { init(keys.keyManagers, null, null) }
        val server = context.serverSocketFactory.createServerSocket(0) as SSLServerSocket
        serve(server) { (it as javax.net.ssl.SSLSocket).startHandshake() }
        return server
    }

    private fun serve(
        server: ServerSocket,
        handle: (Socket) -> Unit,
    ) {
        thread(isDaemon = true) {
            while (!server.isClosed) {
                val socket = runCatching { server.accept() }.getOrNull() ?: return@thread
                thread(isDaemon = true) { runCatching { socket.use(handle) } }
            }
        }
    }

    private fun trusting(store: KeyStore) =
        SSLContext
            .getInstance("TLS")
            .apply { init(null, TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(store) }.trustManagers, null) }
            .socketFactory

    @Test
    fun `a direct handshake tells our transport failing TLS from the server's certificate failing it`() {
        val store = keyStore()
        tlsServer(store).use { server ->
            val at = RelayUrlNormalizer.normalize("wss://localhost:${server.localPort}")
            // A certificate our trust accepts: a dial that failed TLS failed on our side.
            assertEquals(TlsAnswer.ACCEPTED, runBlocking { ReachabilityProbe(tor = null, tlsFactory = trusting(store)).tls(at) })
            // One our default trust does not: the server's own answer.
            assertEquals(TlsAnswer.REJECTED, runBlocking { ReachabilityProbe(tor = null).tls(at) })
        }
    }

    @Test
    fun `a handshake that never happened decides nothing`() {
        // A server that hangs up before any TLS is spoken is not a certificate failing.
        ServerSocket(0).use { server ->
            serve(server) { }
            val at = RelayUrlNormalizer.normalize("wss://localhost:${server.localPort}")
            assertEquals(TlsAnswer.UNCHECKED, runBlocking { ReachabilityProbe(tor = null).tls(at) })
        }
        val tor = TorTransport(TorSettings(socksHost = "127.0.0.1", socksPort = 1, routeAll = true, connectTimeoutSec = 5, maxSockets = 4), OkHttpClient())
        assertEquals(TlsAnswer.UNCHECKED, runBlocking { ReachabilityProbe(tor).tls(url) }, "a Tor-routed url cannot be checked on our direct route")
    }

    private companion object {
        const val PASS = "changeit"
    }
}
