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
import com.vitorpamplona.negentropy.storage.IStorage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A NEG-OPEN's snapshot is taken as the caller, as its REQ is: the store sees the connection's
 * NIP-42 identity, so an observer-gated store answers a reconcile the set it serves that caller.
 */
class NegentropyAsCallerTest {
    private val inner = NostrSemanticsStore(InMemoryEventIndex())

    /** Who each snapshot was taken as. */
    private val takenAs = Collections.synchronizedList(mutableListOf<String?>())

    private val probe =
        object : IEventStore by inner {
            override suspend fun sealedNegentropyStorage(
                filters: List<Filter>,
                maxEntries: Int,
            ): IStorage? {
                takenAs += coroutineContext[StoreQueryContext]?.observer
                return inner.sealedNegentropyStorage(filters, maxEntries)
            }
        }

    private fun caller(
        id: Long,
        vararg users: String,
    ) = object : RequestContext {
        override val connectionId = id
        override val policy: IRelayPolicy get() = error("not asked")
        override val authenticatedUsers = users.toSet()
    }

    @Test
    fun `a NEG-OPEN snapshot is taken under the caller's identity`(): Unit =
        runBlocking {
            val ingest = IngestQueue(probe, SupervisorJob())
            val backend = ObserverBackend(LiveEventStore(probe, ingest))
            val filters = listOf(Filter(kinds = listOf(1)))
            val alice = "a".repeat(64)
            try {
                backend.sealedNegentropyStorage(caller(1, alice), filters, 1_000)
                backend.sealedNegentropyStorage(caller(2), filters, 1_000)
                assertEquals(listOf(alice, null), takenAs.toList(), "the signed-in caller's lens, then none: one snapshot each")
            } finally {
                ingest.close()
            }
        }

    /**
     * A reconcile whose set is the same for every caller is answered from one snapshot: a waived
     * mirror heartbeat from several signed-in routers would otherwise evict each other from quartz's
     * single cache slot, a full walk each. One that ranks or gates through the caller is not shared.
     */
    @Test
    fun `callers whose filters answer alike share one snapshot`(): Unit =
        runBlocking {
            val ingest = IngestQueue(probe, SupervisorJob())
            val backend = ObserverBackend(LiveEventStore(probe, ingest))
            try {
                val waived = listOf(Filter(kinds = listOf(1), search = "include:spam"))
                backend.sealedNegentropyStorage(caller(1, "a".repeat(64)), waived, 1_000)
                backend.sealedNegentropyStorage(caller(2, "b".repeat(64)), waived, 1_000)
                backend.sealedNegentropyStorage(caller(3), waived, 1_000)
                assertEquals(1, takenAs.size, "one walk for three callers: $takenAs")
                takenAs.clear()
                val floored = listOf(Filter(kinds = listOf(1), search = "include:spam filter:rank:gte:50"))
                backend.sealedNegentropyStorage(caller(1, "a".repeat(64)), floored, 1_000)
                backend.sealedNegentropyStorage(caller(2, "b".repeat(64)), floored, 1_000)
                assertEquals(listOf("a".repeat(64), "b".repeat(64)), takenAs.toList(), "an explicit floor gates through each caller's lens")
            } finally {
                ingest.close()
            }
        }
}
