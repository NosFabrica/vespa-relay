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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.SyncUpstream
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
 * The `up` direction: push our matching events to an upstream that lacks
 * them, repeating every [intervalSec] to carry newly-arrived local events.
 *
 * Each pass negentropy-reconciles the store against the upstream a few rounds
 * until the upstream reports nothing more is missing. Reconciliation gives
 * echo-suppression for free: an event we just pulled down from a relay is one
 * that relay already has, so it is never pushed back.
 */
internal class UpstreamPush(
    private val client: NostrClient,
    private val store: IEventStore,
    private val intervalSec: Long,
    // The engine-wide one-snapshot-at-a-time gate, shared with the static
    // and dynamic streams. An id snapshot of a broad filter is gigabytes;
    // the gate is what keeps two of them from being resident at once.
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
                    // One snapshot per pass, reused across the rounds: a
                    // round changes the UPSTREAM's set (it gains what we
                    // push), never ours — re-reading gigabytes of ids per
                    // round bought nothing.
                    val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(up.filter))
                    do {
                        pushedThisRound = 0
                        client.negentropyReconcile(
                            relay = up.url,
                            filter = up.filter,
                            localEntries = local,
                            idleTimeoutMs = NEG_IDLE_MS,
                            onHaveIds = { ids ->
                                // Chunked: a reconcile diff can be arbitrarily
                                // large, and the store should not have to
                                // materialize it as one query.
                                for (chunk in ids.chunked(ID_FETCH_CHUNK)) {
                                    val events: List<Event> = store.query(Filter(ids = chunk))
                                    for (event in events) {
                                        client.publish(event, setOf(up.url))
                                        pushed.incrementAndGet()
                                        pushedThisRound++
                                        delay(PUBLISH_PACE_MS)
                                    }
                                }
                            },
                            onNeedIds = { /* up-only: the down tail pulls, not this */ },
                        )
                        pushedThisPass += pushedThisRound
                        rounds++
                    } while (pushedThisRound > 0 && rounds < MAX_ROUNDS && scope.isActive)
                }
                System.err.println(
                    "router: up ${up.url.url} pushed $pushedThisPass event(s) upstream ($rounds round(s))",
                )
            } catch (e: CancellationException) {
                // Shutdown, not a failed push — the log must not cry wolf.
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
