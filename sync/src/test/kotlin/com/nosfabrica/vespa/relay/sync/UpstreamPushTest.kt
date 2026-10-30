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
import com.nosfabrica.vespa.relay.config.SyncUpstream
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyStoreSync
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The snapshot gate bounds building an id set, never a slow push; a stream keeps one id set alive, not one per url. */
class UpstreamPushTest {
    private val up = SyncUpstream("out", RelayUrlNormalizer.normalize("wss://up.example"), Filter(kinds = listOf(1)), trusted = false)

    private fun pushed(
        target: SyncUpstream,
        uploaded: Int,
        error: String? = null,
        left: Int = 0,
    ) = NegentropyStoreSync.GroupResult(target.url, target.filter, need = 0, have = uploaded + left, downloaded = 0, uploaded = uploaded, deletionsSentUp = 0, deletionsAppliedDown = 0, pagedFallback = error != null, error = error, uploadsLeft = left)

    @Test
    fun `a slow push does not hold the snapshot gate`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val gate = Semaphore(1)
                val reconciling = CompletableDeferred<Unit>()
                val release = CompletableDeferred<Unit>()
                val push =
                    UpstreamPush(
                        client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope),
                        store = NostrSemanticsStore(InMemoryEventIndex()),
                        intervalSec = 60,
                        streamGate = gate,
                        scope = scope,
                        pushGroup = { target, _ ->
                            reconciling.complete(Unit)
                            release.await()
                            pushed(target, uploaded = 3)
                        },
                    )
                val pass = scope.async { push.pass(listOf(up)) }
                withTimeout(5_000) { reconciling.await() }

                assertEquals(1, gate.availablePermits, "a round in flight must leave the gate free for the next stream's snapshot")

                release.complete(Unit)
                assertEquals(3, withTimeout(5_000) { pass.await() }.getValue(up).uploaded, "and the pass still finishes its push")
                assertEquals(3L, push.pushed.get(), "what the upstream accepted is what the stream reports")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a stream holds one snapshot however many urls it pushes to`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val urls = listOf("wss://a.example", "wss://b.example", "wss://c.example").map { up.copy(url = RelayUrlNormalizer.normalize(it)) }
                val seen = mutableListOf<List<IdAndTime>>()
                val inFlight = AtomicInteger()
                var mostAtOnce = 0
                val push =
                    UpstreamPush(
                        client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope),
                        store = NostrSemanticsStore(InMemoryEventIndex()),
                        intervalSec = 60,
                        streamGate = Semaphore(1),
                        scope = scope,
                        pushGroup = { target, local ->
                            mostAtOnce = maxOf(mostAtOnce, inFlight.incrementAndGet())
                            seen += local
                            yield()
                            inFlight.decrementAndGet()
                            if (target == urls.first()) error("refused")
                            pushed(target, uploaded = 0)
                        },
                    )
                val outcomes = withTimeout(5_000) { push.pass(urls) }

                assertEquals(3, seen.size, "every url gets its turn, even after one fails")
                assertTrue(seen.all { it === seen.first() }, "and every url reconciles against the one snapshot")
                assertEquals(1, mostAtOnce, "the urls are pushed in turn, never with copies alive side by side")
                assertEquals(urls.drop(1), outcomes.keys.toList())
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a url whose reconcile failed is reported as failed and leaves the rest their turn`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val urls = listOf("wss://a.example", "wss://b.example").map { up.copy(url = RelayUrlNormalizer.normalize(it)) }
                val push =
                    UpstreamPush(
                        client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope),
                        store = NostrSemanticsStore(InMemoryEventIndex()),
                        intervalSec = 60,
                        streamGate = Semaphore(1),
                        scope = scope,
                        pushGroup = { target, _ -> if (target == urls.first()) pushed(target, 0, error = "no NIP-77") else pushed(target, 2, left = 5) },
                    )
                val outcomes = withTimeout(5_000) { push.pass(urls) }
                assertEquals(listOf(urls[1]), outcomes.keys.toList())
                assertEquals(5, outcomes.getValue(urls[1]).uploadsLeft, "what the budget cut is the next pass's")
                assertEquals(2L, push.pushed.get())
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `the push is up only, under a per-url budget, and never writes our store`() {
        val config = UpstreamPush.upOnly(intervalSec = 300)
        assertTrue(config.up && !config.down, "an up stream pulls nothing")
        assertTrue(!config.syncDeletions, "the deletion settle's applyDown would write a relay's kind 5 into a read-only store")
        assertTrue(!config.pageFallback)
        assertEquals(300_000L, config.uploadDeadlineMs, "one slow upstream may hold its siblings back by one interval at most")
        assertTrue(config.uploadWindow > 0, "a relay that stops answering stalls the push once, not once per event")
    }
}
