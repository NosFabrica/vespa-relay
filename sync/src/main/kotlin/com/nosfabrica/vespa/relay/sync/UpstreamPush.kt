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

import com.nosfabrica.vespa.relay.config.SyncUpstream
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.atomic.AtomicLong

/**
 * The `up` direction: pushes our matching events to an upstream that lacks them, every
 * [intervalSec], reconciling until nothing more is missing.
 */
internal class UpstreamPush(
    private val client: NostrClient,
    private val store: IEventStore,
    private val intervalSec: Long,
    /** The engine-wide one-id-snapshot-at-a-time gate. */
    private val streamGate: Semaphore,
    private val scope: CoroutineScope,
) {
    val pushed = AtomicLong()

    suspend fun loop(up: SyncUpstream) {
        while (scope.isActive) {
            try {
                var rounds = 0
                var pushedThisRound = 0L
                var pushedThisPass = 0L
                streamGate.withPermit {
                    // One snapshot per pass: a round changes the upstream's set, never ours.
                    val local: List<IdAndTime> =
                        storeCall(StoreCalls.CALLER_PUSH_UPSTREAM, StoreCalls.OP_SNAPSHOT_IDS, StoreCalls.summarise(up.filter)) {
                            store.snapshotIdsForNegentropy(listOf(up.filter))
                        }
                    do {
                        pushedThisRound = 0
                        client.negentropyReconcile(
                            relay = up.url,
                            filter = up.filter,
                            localEntries = local,
                            idleTimeoutMs = NEG_IDLE_MS,
                            onHaveIds = { ids ->
                                // Chunked so the store never materialises a whole diff as one query.
                                for (chunk in ids.chunked(ID_FETCH_CHUNK)) {
                                    val events: List<Event> =
                                        storeCall(StoreCalls.CALLER_PUSH_UPSTREAM, StoreCalls.OP_QUERY, StoreCalls.ids(chunk.size)) {
                                            store.query(Filter(ids = chunk))
                                        }
                                    for (event in events) {
                                        client.publish(event, setOf(up.url))
                                        pushed.incrementAndGet()
                                        pushedThisRound++
                                        delay(PUBLISH_PACE_MS)
                                    }
                                }
                            },
                            onNeedIds = { /* up-only: nothing is pulled here */ },
                        )
                        pushedThisPass += pushedThisRound
                        rounds++
                    } while (pushedThisRound > 0 && rounds < MAX_ROUNDS && scope.isActive)
                }
                System.err.println(
                    "router: up ${up.url.url} pushed $pushedThisPass event(s) upstream ($rounds round(s))",
                )
            } catch (e: CancellationException) {
                // Shutdown, not a failed push.
                throw e
            } catch (e: Exception) {
                System.err.println("router: up ${up.url.url} failed: ${e.message}")
            }
            delay(intervalSec * 1000)
        }
    }

    companion object {
        private const val PUBLISH_PACE_MS = 40L
        private const val MAX_ROUNDS = 8
        private const val ID_FETCH_CHUNK = 500
    }
}
