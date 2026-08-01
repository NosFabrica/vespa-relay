/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.eventstore.store.NostrSemanticsStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class RelayExpirationTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerSync()

    @Test
    fun `sweepOnce prunes without touching live events`() =
        runBlocking {
            // The store refuses already-expired inserts, so the sweeper's job is
            // events that expire while stored (the store's own deletion logic is
            // tested upstream). Here we verify the sweeper drives that path
            // cleanly and never over-deletes a live, non-expiring event.
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val kept = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "stays")
            store.insert(kept)

            ExpirationSweeper(store, intervalSeconds = 3_600).sweepOnce()

            assertEquals(1, store.count(Filter(ids = listOf(kept.id))), "non-expiring event should remain")
        }

    @Test
    fun `a non-positive interval never starts the loop`() {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
        // Should not throw, and close() is safe even though nothing was launched.
        ExpirationSweeper(store, intervalSeconds = 0).start().close()
    }
}
