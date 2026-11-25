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
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.runBlocking
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A write that cannot change what the trust gate lets through keeps the NEG-OPEN snapshots it does
 * not match; one that can (a card, a provider list, a deletion or vanish that may remove one) drops them all.
 */
class FilterLocalWritesTest {
    private val someone = NostrSignerSync()
    private val pk = someone.pubKey

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = someone.sign<Event>(1_000L, kind, arrayOf(*tags), "")

    @Test
    fun `writes the gate reads drop every snapshot`() {
        val gateWrites =
            mapOf(
                "a card" to event(UserAssertionEvent.KIND, arrayOf("d", "b".repeat(64)), arrayOf("rank", "90")),
                "a provider list" to event(TrustProviderListEvent.KIND, arrayOf("30382:rank", "c".repeat(64), "wss://scores.example.com/")),
                "a vanish" to event(RequestToVanishEvent.KIND, arrayOf("relay", "ALL_RELAYS")),
                "a deletion by id" to event(DeletionRequestEvent.KIND, arrayOf("e", "d".repeat(64))),
                "a deletion by id that says its kind" to event(DeletionRequestEvent.KIND, arrayOf("e", "d".repeat(64)), arrayOf("k", "1")),
                "a deletion of a card's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "30382:$pk:${"b".repeat(64)}")),
                "a deletion of a provider list's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "10040:$pk:")),
                "a deletion of an address it cannot parse" to event(DeletionRequestEvent.KIND, arrayOf("a", "nonsense")),
                "a deletion of an article and a card" to event(DeletionRequestEvent.KIND, arrayOf("a", "30023:$pk:post"), arrayOf("a", "30382:$pk:x")),
            )
        for ((what, e) in gateWrites) assertFalse(isFilterLocalWrite(e), "$what can change the gate")
    }

    /** Follow lists, reports, other assertions and Trusted Lists feed only search reads, whose snapshots every write drops. */
    @Test
    fun `writes the gate does not read keep the snapshots they do not match`() {
        val ordinary =
            mapOf(
                "a note" to event(1),
                "a reaction" to event(7, arrayOf("e", "d".repeat(64))),
                "a repost" to event(6, arrayOf("e", "d".repeat(64))),
                "a profile" to event(0),
                "a follow list" to event(3, arrayOf("p", "b".repeat(64))),
                "a report" to event(1984, arrayOf("p", "b".repeat(64), "spam")),
                "an article" to event(30023, arrayOf("d", "post")),
                "an event assertion" to event(30383, arrayOf("d", "d".repeat(64))),
                "a Trusted List" to event(30392, arrayOf("d", "list")),
                "a deletion of an article's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "30023:$pk:post"), arrayOf("k", "30023")),
                "a deletion naming nothing" to event(DeletionRequestEvent.KIND),
            )
        for ((what, e) in ordinary) assertTrue(isFilterLocalWrite(e), "$what leaves the gate alone")
    }

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

    /** A signed-in reconcile of notes, its snapshot taken as the caller, across a publish of each kind of write. */
    @Test
    fun `an ordinary publish no longer forces a signed-in reconcile to walk again`(): Unit =
        runBlocking {
            val observer = NostrSignerSync()
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = observer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                var round = 0

                suspend fun reconcile() {
                    val sub = "neg${round++}"
                    val neg = Negentropy(StorageVector().also { it.seal() }, 60_000)
                    session.receive("""["NEG-OPEN","$sub",{"kinds":[1]},"${Hex.encode(neg.initiate())}"]""")
                    awaitMessage(out) { it.startsWith("""["NEG-MSG","$sub",""") }
                    session.receive("""["NEG-CLOSE","$sub"]""")
                }

                suspend fun publish(e: Event) {
                    session.receive("""["EVENT",${e.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${e.id}",true""") }
                }

                val now = System.currentTimeMillis() / 1000
                reconcile()
                reconcile()
                assertEquals(1, walks.get(), "a second open of the same filter is served from the cache")

                val reactor = NostrSignerSync()
                val reaction = reactor.sign<Event>(now, 7, arrayOf(arrayOf("e", "d".repeat(64))), "+")
                publish(reaction)
                reconcile()
                assertEquals(1, walks.get(), "a reaction cannot change which notes the caller's lens admits")

                publish(observer.sign<Event>(now, TrustProviderListEvent.KIND, arrayOf(arrayOf("30382:rank", NostrSignerSync().pubKey, "wss://scores.example.com/")), ""))
                reconcile()
                assertEquals(2, walks.get(), "a provider list can change every lens")

                publish(NostrSignerSync().sign<Event>(now, UserAssertionEvent.KIND, arrayOf(arrayOf("d", reactor.pubKey), arrayOf("rank", "90")), ""))
                reconcile()
                assertEquals(3, walks.get(), "a card can change an author's rank")

                publish(reactor.sign<Event>(now, DeletionRequestEvent.KIND, arrayOf(arrayOf("e", reaction.id)), ""))
                reconcile()
                assertEquals(4, walks.get(), "a deletion by id may remove a card or a provider list")
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
