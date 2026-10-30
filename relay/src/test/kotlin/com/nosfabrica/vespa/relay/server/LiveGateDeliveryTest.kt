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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * LIVE DELIVERY ANSWERS TO THE SAME LENS AS THE REPLAY.
 *
 * The stored page of a lensed subscription is gated in the engine; events that arrive after EOSE
 * are matched in memory by quartz, and used to stream a below-floor author the replay had just
 * dropped (RelayContractIT: "an AUTH'd live feed is trusted-only"). ObserverBackend now holds
 * them to the store's LiveGate. The in-memory index cannot gate the replay, so only the live half
 * is asserted here; the contract IT covers both against a real Vespa.
 */
class LiveGateDeliveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)

    private val observer = NostrSignerSync()
    private val provider = NostrSignerSync()
    private val trusted = NostrSignerSync()
    private val spammer = NostrSignerSync()

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun seedLens() =
        runBlocking {
            store.batchInsert(
                listOf(trusted to 90, spammer to 1).map { (s, r) ->
                    provider.sign<UserAssertionEvent>(1_000L + r, UserAssertionEvent.KIND, arrayOf(arrayOf("d", s.pubKey), arrayOf("rank", r.toString())), "")
                },
            )
            store.insert(observer.sign<TrustProviderListEvent>(1_000L, TrustProviderListEvent.KIND, arrayOf(arrayOf("30382:rank", provider.pubKey, "wss://scores.example.com/")), ""))
        }

    @Test
    fun `an authenticated live feed streams only authors its lens admits`(): Unit =
        runBlocking {
            seedLens()
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            val watcher = Collections.synchronizedList(mutableListOf<String>())
            val anonymous = server.connect { watcher.add(it) }
            try {
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = observer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                val now = System.currentTimeMillis() / 1000
                session.receive("""["REQ","feed",{"kinds":[1],"since":${now - 5}}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","feed"]""") }
                anonymous.receive("""["REQ","all",{"kinds":[1],"since":${now - 5},"search":"include:spam"}]""")
                awaitMessage(watcher) { it.startsWith("""["EOSE","all"]""") }

                val good = trusted.sign<Event>(now, 1, emptyArray(), "from a trusted author")
                val bad = spammer.sign<Event>(now, 1, emptyArray(), "from a below-floor author")
                session.receive("""["EVENT",${bad.toJson()}]""")
                session.receive("""["EVENT",${good.toJson()}]""")

                awaitMessage(out) { it.startsWith("""["EVENT","feed",""") && good.id in it }
                awaitMessage(watcher) { it.startsWith("""["EVENT","all",""") && good.id in it }
                awaitMessage(watcher) { it.startsWith("""["EVENT","all",""") && bad.id in it }
                // The trusted note overtook nothing: give the cold-author read time to have answered.
                Thread.sleep(500)
                assertTrue(out.none { it.startsWith("""["EVENT","feed",""") && bad.id in it }, "the below-floor author was streamed: $out")
            } finally {
                session.close()
                anonymous.close()
            }
        }

    /**
     * Quartz takes a [com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveSubscriptionBackend]'s
     * registration over the parked read, so the gate has to hold there too: for events accepted
     * before the replay ran as well as after its EOSE, on both the raw and the screened replay.
     */
    @Test
    fun `a registered subscription streams only authors its lens admits`(): Unit =
        runBlocking {
            seedLens()
            val ingest = IngestQueue(store, SupervisorJob())
            val backend = ObserverBackend(LiveEventStore(store, ingest), liveGates = store::liveGate)
            val ctx =
                object : RequestContext {
                    override val connectionId = 1L
                    override val policy: IRelayPolicy get() = error("not asked")
                    override val authenticatedUsers = setOf(observer.pubKey)
                }
            val now = System.currentTimeMillis() / 1000
            val filters = listOf(Filter(kinds = listOf(1), since = now - 5))
            val raw = Collections.synchronizedList(mutableListOf<String>())
            val screened = Collections.synchronizedList(mutableListOf<String>())
            val rawSub = backend.openSubscription(ctx, filters, LiveListener { e, _ -> raw.add(e.id) })
            val screenedSub = backend.openSubscription(ctx, filters, LiveListener { e, _ -> screened.add(e.id) })
            try {
                suspend fun publish(event: Event) = CompletableDeferred<Unit>().also { done -> backend.submit(event) { done.complete(Unit) } }.await()

                // Accepted while the subscriptions are registered but have not replayed yet.
                val earlyGood = trusted.sign<Event>(now, 1, emptyArray(), "early, trusted")
                val earlyBad = spammer.sign<Event>(now, 1, emptyArray(), "early, below the floor")
                publish(earlyBad)
                publish(earlyGood)

                rawSub.replayRaw({}, {})
                screenedSub.replay({}, {})

                val good = trusted.sign<Event>(now, 1, emptyArray(), "after EOSE, trusted")
                val bad = spammer.sign<Event>(now, 1, emptyArray(), "after EOSE, below the floor")
                publish(bad)
                publish(good)

                for (out in listOf(raw, screened)) awaitId(out, good.id)
                // The trusted note overtook nothing: give the cold-author read time to have answered.
                delay(500)
                for (out in listOf(raw, screened)) {
                    assertTrue(earlyGood.id in out, "a held live event from a trusted author was never sent: $out")
                    assertEquals(emptyList(), out.filter { it == bad.id || it == earlyBad.id }, "a below-floor author was streamed")
                }
            } finally {
                rawSub.cancel()
                screenedSub.cancel()
                ingest.close()
            }
        }

    /** Suspends rather than sleeps: a cold-author check runs on the dispatcher its replay ran on, here this test's. */
    private suspend fun awaitId(
        out: List<String>,
        id: String,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            if (synchronized(out) { id in out }) return
            delay(20)
        }
        fail("timed out waiting for $id; got: $out")
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
