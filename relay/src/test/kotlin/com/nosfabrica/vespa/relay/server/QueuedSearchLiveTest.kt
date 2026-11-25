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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A ranked REQ queued behind another on its connection holds a bounded number of live events, then ends. */
class QueuedSearchLiveTest {
    private val inner = NostrSemanticsStore(InMemoryEventIndex())
    private val released = CompletableDeferred<Unit>()
    private val entered = CompletableDeferred<Unit>()

    /** A store whose stored read for the first search never answers until the test lets it. */
    private val store =
        object : IEventStore by inner {
            override suspend fun rawQuery(
                filters: List<Filter>,
                onEach: (RawEvent) -> Unit,
            ) {
                if (filters.any { it.search == "slow" }) {
                    entered.complete(Unit)
                    released.await()
                }
                inner.rawQuery(filters, onEach)
            }
        }

    private val ctx =
        object : RequestContext {
            override val connectionId = 7L
            override val policy: IRelayPolicy get() = error("not asked")
            override val authenticatedUsers = emptySet<String>()
        }

    @Test
    fun `live events past the bound end a search still queued for its turn`(): Unit =
        runBlocking(Dispatchers.Default) {
            val ingest = IngestQueue(store, SupervisorJob())
            val gate = SearchGate(1)
            val backend = ObserverBackend(LiveEventStore(store, ingest), gate = gate)
            val ahead = listOf(Filter(kinds = listOf(1), search = "slow"))
            val behind = listOf(Filter(kinds = listOf(1), search = "live"))
            assertTrue(gate.gates(ahead) && gate.gates(behind), "both reads must take the connection's one ranked permit")
            val first = backend.openSubscription(ctx, ahead, LiveListener { _, _ -> })
            val delivered = AtomicInteger()
            val second = backend.openSubscription(ctx, behind, LiveListener { _, _ -> delivered.incrementAndGet() })
            try {
                launch { first.replayRaw({}, {}) }
                // The first search holds the connection's permit before the second asks for it.
                withTimeout(10_000) { entered.await() }
                val waiting = async { runCatching { second.replayRaw({}, {}) } }
                val signer = NostrSignerSync()
                val now = System.currentTimeMillis() / 1000
                val accepted = AtomicInteger()
                repeat(LIVE_EVENTS) { i ->
                    backend.submit(signer.sign<Event>(now, 1, emptyArray(), "live $i")) { accepted.incrementAndGet() }
                }
                val outcome = withTimeout(30_000) { waiting.await() }

                assertTrue(outcome.isFailure, "the queued search must end, not hold every live event until its turn")
                val reason = outcome.exceptionOrNull()?.message.orEmpty()
                assertTrue("live" in reason, "the CLOSED says why: $reason")
                assertFalse(released.isCompleted, "it ended without waiting for the search ahead")
                assertEquals(0, delivered.get(), "nothing it held was sent ungated")
            } finally {
                released.complete(Unit)
                first.cancel()
                second.cancel()
                ingest.close()
            }
        }

    private companion object {
        /** Past quartz's own hold before EOSE and past ours. */
        const val LIVE_EVENTS = 3_000
    }
}
