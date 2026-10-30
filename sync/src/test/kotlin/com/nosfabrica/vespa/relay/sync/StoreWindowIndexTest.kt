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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** The pager's view of the store: quartz's index, every read on the visit's books, a failed count a failure there too. */
class StoreWindowIndexTest {
    private val window = Filter(kinds = listOf(1), since = 100, until = 200)

    private fun booked(calls: StoreCalls) = calls.snapshot().callers.single { it.caller == StoreCalls.CALLER_VISIT_NEGENTROPY }

    @Test
    fun `a count the store cannot give is no count, and is booked as failed`(): Unit =
        runBlocking {
            val base = NostrSemanticsStore(InMemoryEventIndex())
            val down =
                object : IEventStore by base {
                    override suspend fun count(filter: Filter): Int = throw IOException("store down")
                }
            val calls = StoreCalls()
            val counted = withContext(calls) { StoreWindowIndex(down).count(window) }
            assertNull(counted, "a failed count must leave the pre-split to the relay, not abort the sweep")
            assertEquals(1L, booked(calls).failed)
            assertEquals(0L, booked(calls).returned)
        }

    @Test
    fun `an answered count and an id read are each booked as answered`(): Unit =
        runBlocking {
            val calls = StoreCalls()
            val index = StoreWindowIndex(NostrSemanticsStore(InMemoryEventIndex()))
            withContext(calls) {
                assertEquals(0, index.count(window))
                assertEquals(emptyList(), index.entriesFor(window))
            }
            assertEquals(2L, booked(calls).issued)
            assertEquals(2L, booked(calls).returned)
        }
}
