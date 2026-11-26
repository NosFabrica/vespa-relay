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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.negentropy.Negentropy
import com.vitorpamplona.negentropy.storage.IStorage
import com.vitorpamplona.negentropy.storage.StorageVector
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.runBlocking
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * A NEG-OPEN's cached snapshot, driven over the wire through the relay's own wiring: a write that
 * cannot change its set keeps it, and one the trust gate reads, or one the filter matches, drops it.
 * Which writes and searches are local is the store's call (`FilterLocality`); this holds that the relay asks it.
 */
class NegentropySnapshotReuseTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val inner = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = relayUrl)

    /** Every snapshot walk the store is asked for. */
    private val walks = AtomicInteger()

    private val probe =
        object : IEventStore by inner {
            override suspend fun sealedNegentropyStorage(
                filters: List<Filter>,
                maxEntries: Int,
            ): IStorage? {
                walks.incrementAndGet()
                return inner.sealedNegentropyStorage(filters, maxEntries)
            }
        }

    private val server = NostrRelayServer(probe, relayUrl)

    @AfterTest
    fun tearDown() {
        server.close()
    }

    /** A reconcile of notes as a signed-in caller, its snapshot taken under the caller's lens. */
    @Test
    fun `a signed-in reconcile walks again only for a write that can change its set`(): Unit = reusedAcrossOrdinaryWrites("""{"kinds":[1]}""", signIn = true)

    /** The waived heartbeat an anonymous mirror peer sends, on the shared snapshot. */
    @Test
    fun `a peer's waived reconcile walks again only for a write that can change its set`(): Unit = reusedAcrossOrdinaryWrites("""{"kinds":[1],"search":"include:spam"}""", signIn = false)

    /** A reconcile through a named lens, on the shared snapshot. */
    @Test
    fun `a reconcile through a named observer walks again only for a write that can change its set`(): Unit = reusedAcrossOrdinaryWrites("""{"kinds":[1],"search":"observer:${NostrSignerSync().pubKey}"}""", signIn = false)

    /** Text ranks against corpus-wide statistics, so any write may move its set. */
    @Test
    fun `a text reconcile walks again after any write`(): Unit =
        runBlocking {
            wire(signIn = false) {
                val filter = """{"kinds":[1],"search":"apple include:spam"}"""
                reconcile(filter)
                reconcile(filter)
                assertEquals(1, walks.get(), "with no write between them, the second open is served from the cache")
                publish(NostrSignerSync().sign<Event>(now, 7, arrayOf(arrayOf("e", "d".repeat(64))), "+"))
                reconcile(filter)
                assertEquals(2, walks.get(), "a reaction can move a bm25 score floor")
            }
        }

    private fun reusedAcrossOrdinaryWrites(
        filter: String,
        signIn: Boolean,
    ) = runBlocking {
        wire(signIn) {
            reconcile(filter)
            reconcile(filter)
            assertEquals(1, walks.get(), "a second open of the same filter is served from the cache")

            val reactor = NostrSignerSync()
            val reaction = reactor.sign<Event>(now, 7, arrayOf(arrayOf("e", "d".repeat(64))), "+")
            publish(reaction)
            reconcile(filter)
            assertEquals(1, walks.get(), "a reaction can change neither the notes nor what any lens admits")

            publish(reactor.sign<Event>(now, 1, emptyArray(), "a note"))
            reconcile(filter)
            assertEquals(2, walks.get(), "a note joins a reconcile of notes")

            publish(NostrSignerSync().sign<Event>(now, TrustProviderListEvent.KIND, arrayOf(arrayOf("30382:rank", NostrSignerSync().pubKey, "wss://scores.example.com/")), ""))
            reconcile(filter)
            assertEquals(3, walks.get(), "a provider list can change every lens")

            publish(NostrSignerSync().sign<Event>(now, UserAssertionEvent.KIND, arrayOf(arrayOf("d", reactor.pubKey), arrayOf("rank", "90")), ""))
            reconcile(filter)
            assertEquals(4, walks.get(), "a card can change an author's rank")

            publish(reactor.sign<Event>(now, DeletionRequestEvent.KIND, arrayOf(arrayOf("e", reaction.id)), ""))
            reconcile(filter)
            assertEquals(5, walks.get(), "a deletion by id may remove a card or a provider list")
        }
    }

    /** One connection to [server], signed in first when [signIn]. */
    private suspend fun wire(
        signIn: Boolean,
        body: suspend Wire.() -> Unit,
    ) {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        try {
            val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
            if (signIn) {
                val auth = NostrSignerSync().sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }
            }
            Wire(out) { session.receive(it) }.body()
        } finally {
            session.close()
        }
    }

    private inner class Wire(
        val out: List<String>,
        val send: suspend (String) -> Unit,
    ) {
        val now = System.currentTimeMillis() / 1000
        private var round = 0

        /** One NEG-OPEN from an empty local set, closed once the relay answers its first round. */
        suspend fun reconcile(filter: String) {
            val sub = "neg${round++}"
            val neg = Negentropy(StorageVector().also { it.seal() }, 60_000)
            send("""["NEG-OPEN","$sub",$filter,"${Hex.encode(neg.initiate())}"]""")
            val answer = awaitMessage(out) { it.startsWith("""["NEG-MSG","$sub",""") || it.startsWith("""["NEG-ERR","$sub",""") }
            if (answer.startsWith("""["NEG-ERR""")) fail("the reconcile was refused: $answer")
            send("""["NEG-CLOSE","$sub"]""")
        }

        suspend fun publish(e: Event) {
            send("""["EVENT",${e.toJson()}]""")
            awaitMessage(out) { it.startsWith("""["OK","${e.id}",true""") }
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
