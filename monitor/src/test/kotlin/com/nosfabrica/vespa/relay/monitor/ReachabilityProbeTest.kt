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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
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
}
