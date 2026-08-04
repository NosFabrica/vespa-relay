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

import com.nosfabrica.vespa.eventstore.store.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * NIP-86 enforcement: a pubkey banned in the shared [BanStore] is rejected by
 * the relay's [com.vitorpamplona.quartz.nip86RelayManagement.server.BanListPolicy]
 * before ingest, and un-banning restores publishing. This exercises the same
 * BanStore instance the NIP-86 admin endpoint mutates.
 */
class RelayAdminTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val banStore = BanStore {}
    private val server = NostrRelayServer(store, relayUrl, banStore = banStore)
    private val signer = NostrSignerSync()

    @Test
    fun `a banned pubkey is rejected, and un-banning restores publishing`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                banStore.banPubkey(signer.pubKey, "spam")

                val blocked = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "while banned")
                session.receive("""["EVENT",${blocked.toJson()}]""")
                val rejected = awaitMessage(out) { it.startsWith("""["OK","${blocked.id}"""") }
                assertTrue(",false," in rejected, "the ban must reject the publish: $rejected")
                assertEquals(0, store.count(Filter(kinds = listOf(1))))

                banStore.unbanPubkey(signer.pubKey)

                val allowed = signer.sign<Event>(1_700_000_001L, 1, emptyArray(), "after unban")
                session.receive("""["EVENT",${allowed.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${allowed.id}",true""") }
                assertEquals(1, store.count(Filter(kinds = listOf(1))))
            } finally {
                session.close()
            }
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
