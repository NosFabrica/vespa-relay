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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/** Static write authorization and the future-dated-event guard, driven over the protocol. */
class RelayWriteAuthTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)

    @Test
    fun `a denied pubkey cannot publish`() =
        runBlocking {
            val store = newStore()
            val server = NostrRelayServer(store, relayUrl, pubkeyDeny = setOf(signer.pubKey))
            publishAndExpectReject(server, store, signer.sign(1_700_000_000L, 1, emptyArray(), "denied"))
            server.close()
        }

    @Test
    fun `an allowlist that omits the author rejects it`() =
        runBlocking {
            val store = newStore()
            val server = NostrRelayServer(store, relayUrl, pubkeyAllow = setOf("f".repeat(64)))
            publishAndExpectReject(server, store, signer.sign(1_700_000_000L, 1, emptyArray(), "not allowlisted"))
            server.close()
        }

    @Test
    fun `a denied kind is rejected`() =
        runBlocking {
            val store = newStore()
            val server = NostrRelayServer(store, relayUrl, kindDeny = setOf(1))
            publishAndExpectReject(server, store, signer.sign(1_700_000_000L, 1, emptyArray(), "banned kind"))
            server.close()
        }

    @Test
    fun `an event dated too far in the future is rejected but a current one is not`() =
        runBlocking {
            val store = newStore()
            val server = NostrRelayServer(store, relayUrl, rejectFutureSeconds = 60)
            val now = System.currentTimeMillis() / 1000

            val future = signer.sign<Event>(now + 3_600, 1, emptyArray(), "from the future")
            publishAndExpectReject(server, store, future)

            val current = signer.sign<Event>(now, 1, emptyArray(), "right now")
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            session.receive("""["EVENT",${current.toJson()}]""")
            awaitMessage(out) { it.startsWith("""["OK","${current.id}",true""") }
            session.close()
            server.close()
        }

    private fun publishAndExpectReject(
        server: NostrRelayServer,
        store: NostrSemanticsStore,
        event: Event,
    ) = runBlocking {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        session.receive("""["EVENT",${event.toJson()}]""")
        val ok = awaitMessage(out) { it.startsWith("""["OK","${event.id}"""") }
        assertTrue(",false," in ok, "publish should be rejected: $ok")
        assertEquals(0, store.count(Filter(kinds = listOf(1))))
        session.close()
    }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
