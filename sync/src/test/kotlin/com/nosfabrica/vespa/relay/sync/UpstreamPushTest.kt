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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

/** The engine's snapshot gate bounds building an id set, never a push that is slow to finish. */
class UpstreamPushTest {
    private val up = SyncUpstream("out", RelayUrlNormalizer.normalize("wss://up.example"), Filter(kinds = listOf(1)), trusted = false)

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
                        reconcile = { _, _, _ ->
                            reconciling.complete(Unit)
                            release.await()
                        },
                    )
                val pass = scope.async { push.pass(up) }
                withTimeout(5_000) { reconciling.await() }

                assertEquals(1, gate.availablePermits, "a round in flight must leave the gate free for the next stream's snapshot")

                release.complete(Unit)
                assertEquals(1, withTimeout(5_000) { pass.await() }.first, "and the pass still finishes its round")
            } finally {
                scope.cancel()
            }
        }
}
