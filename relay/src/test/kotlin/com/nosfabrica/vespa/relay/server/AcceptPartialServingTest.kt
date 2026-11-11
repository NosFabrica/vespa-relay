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

import com.nosfabrica.vespa.eventstore.AcceptPartial
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A REQ's stored rows are read under [AcceptPartial]; COUNT and the live gate are not. */
class AcceptPartialServingTest {
    /** Which store reads ran under [AcceptPartial], by the store call they arrived as. */
    private val seen = Collections.synchronizedList(mutableListOf<Pair<String, Boolean>>())

    private val inner = NostrSemanticsStore(InMemoryEventIndex())

    private val probe =
        object : IEventStore by inner {
            private suspend fun saw(call: String) {
                seen += call to (coroutineContext[AcceptPartial] != null)
            }

            override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
                saw("query")
                return inner.query(filters)
            }

            override suspend fun <T : Event> query(
                filters: List<Filter>,
                onEach: (T) -> Unit,
            ) {
                saw("query")
                inner.query(filters, onEach)
            }

            override suspend fun rawQuery(
                filters: List<Filter>,
                onEach: (RawEvent) -> Unit,
            ) {
                saw("rawQuery")
                inner.rawQuery(filters, onEach)
            }

            override suspend fun count(filters: List<Filter>): Int {
                saw("count")
                return inner.count(filters)
            }
        }

    private val ctx =
        object : RequestContext {
            override val connectionId = 1L
            override val policy: IRelayPolicy get() = error("not asked")
            override val authenticatedUsers = emptySet<String>()
        }

    @Test
    fun `a REQ's stored rows are read under AcceptPartial and a COUNT is not`(): Unit =
        runBlocking {
            val ingest = IngestQueue(probe, SupervisorJob())
            val backend =
                ObserverBackend(LiveEventStore(probe, ingest), liveGates = { f ->
                    seen += "liveGate" to (currentCoroutineContext()[AcceptPartial] != null)
                    inner.liveGate(f)
                })
            val filters = listOf(Filter(kinds = listOf(1), limit = 10, search = "hello"))
            try {
                val raw = backend.openSubscription(ctx, filters, LiveListener { _, _ -> })
                val screened = backend.openSubscription(ctx, filters, LiveListener { _, _ -> })
                raw.replayRaw({}, {})
                screened.replay({}, {})
                raw.cancel()
                screened.cancel()
                backend.count(ctx, filters)

                val reads = seen.filter { it.first != "count" && it.first != "liveGate" }
                assertTrue(reads.isNotEmpty(), "no replay reached the store: $seen")
                assertTrue(reads.all { it.second }, "a replay read the store strictly: $seen")
                assertEquals(listOf(false), seen.filter { it.first == "count" }.map { it.second }, "a COUNT must stay strict: $seen")
                val gates = seen.filter { it.first == "liveGate" }
                assertTrue(gates.isNotEmpty() && gates.none { it.second }, "the live gate decides admission, so it is read strict: $seen")
            } finally {
                ingest.close()
            }
        }
}
