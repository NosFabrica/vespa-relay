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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyStoreSync
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The `up` direction: every [intervalSec], pushes our matching events to each upstream of a stream
 * that lacks them, through quartz's [NegentropyStoreSync], each url within one interval's budget.
 */
internal class UpstreamPush(
    private val client: NostrClient,
    private val store: IEventStore,
    private val intervalSec: Long,
    /** The engine-wide one-id-snapshot-at-a-time gate, held for the snapshot and released before any push. */
    private val streamGate: Semaphore,
    private val scope: CoroutineScope,
    /**
     * Which upstreams take the lens waiver: a web-of-trust upstream reconciles its whole corpus
     * against ours with it, and only its gated subset without, which would push the rest every pass.
     */
    private val waivers: LensWaivers = LensWaivers.none(),
    /**
     * How many of the filter's events the upstream holds, by a reconcile against our snapshot, before
     * anything is pushed; null when it could not be reconciled. Asked only when the pass before left no
     * answer to read it off. A test stands in for the relay.
     */
    private val peerHeld: suspend (SyncUpstream, List<IdAndTime>) -> Int? = { up, local ->
        try {
            client.negentropyReconcileIds(up.url, waivers.onWire(up.url, up.filter), local, idleTimeoutMs = NEG_IDLE_MS).let { local.size - it.haveIds.size + it.needIds.size }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("router: up ${up.url.url} could not be reconciled to size it: ${e.message}")
            null
        }
    },
    /** One url's push against the pass's snapshot; quartz's group unless a test stands in. */
    private val pushGroup: suspend (SyncUpstream, List<IdAndTime>) -> NegentropyStoreSync.GroupResult = { up, local ->
        NegentropyStoreSync(client, BookedReads(store), upOnly(intervalSec)).syncGroup(up.url, waivers.onWire(up.url, up.filter), local)
    },
) {
    /** Holds back a push to an upstream whose set shrank too far to believe. */
    private val shrink = ShrinkGuard()

    /** What each upstream held after the last pass pushed to it, and how the gate read it then. */
    private val heldAfter = ConcurrentHashMap<ShrinkGuard.Key, Held>()

    private class Held(
        val count: Int,
        val stance: LensWaivers.Stance,
    )

    /** Push rounds held back because the upstream's set shrank too far to believe. */
    val withheld = AtomicLong()

    /** Events an upstream accepted, its `OK` true. */
    val pushed = AtomicLong()

    /** Pushes one stream to each of its [urls] in turn, every [intervalSec]. */
    suspend fun loop(urls: List<SyncUpstream>) {
        while (scope.isActive) {
            try {
                pass(urls)
            } catch (e: CancellationException) {
                // Shutdown, not a failed push.
                throw e
            } catch (e: Exception) {
                System.err.println("router: up ${urls.first().streamName} failed: ${e.message}")
            }
            delay(intervalSec * 1000)
        }
    }

    /**
     * One pass: one id snapshot under the gate, shared by the stream's urls in turn, so a stream
     * holds at most one snapshot however many urls it pushes to. Returns each url's outcome.
     */
    internal suspend fun pass(urls: List<SyncUpstream>): Map<SyncUpstream, NegentropyStoreSync.GroupResult> {
        val filter = urls.first().filter
        require(urls.all { it.streamName == urls.first().streamName && it.filter == filter }) { "one pass is one stream's urls" }
        val local: List<IdAndTime> =
            streamGate.withPermit {
                storeCall(StoreCalls.CALLER_PUSH_UPSTREAM, StoreCalls.OP_SNAPSHOT_IDS, StoreCalls.summarise(filter)) {
                    store.snapshotIdsForNegentropy(listOf(filter))
                }
            }
        val outcomes = LinkedHashMap<SyncUpstream, NegentropyStoreSync.GroupResult>()
        for (up in urls) {
            if (!scope.isActive) break
            try {
                val key = ShrinkGuard.Key(up.streamName, up.url, up.filter)
                if (!mayPush(up, key, local)) continue
                val stance = waivers.stance(up.url)
                val result = pushGroup(up, local)
                pushed.addAndGet(result.uploaded.toLong())
                // A group reports a failed reconcile rather than throwing it.
                val failure = result.error
                if (failure != null) {
                    System.err.println("router: up ${up.url.url} failed: $failure")
                    continue
                }
                // The group's reconcile sized the upstream already; the next pass reads it off this.
                heldAfter[key] = Held(local.size - result.have + result.need + result.uploaded, stance)
                outcomes[up] = result
                val left = if (result.uploadsLeft > 0) ", ${result.uploadsLeft} left for the next pass" else ""
                System.err.println("router: up ${up.url.url} pushed ${result.uploaded} event(s) upstream (${result.have} it lacked$left)")
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // One url failing still leaves the stream's other urls their turn.
                System.err.println("router: up ${up.url.url} failed: ${e.message}")
            }
        }
        return outcomes
    }

    /**
     * Fail closed: an upstream reading our signed-in key through a trust lens answers empty and
     * refuses nothing, and pushing on that answer would push it everything we hold. Judged on the
     * last pass's answer while the gate reads the upstream as it did then; else sized afresh.
     */
    private suspend fun mayPush(
        up: SyncUpstream,
        key: ShrinkGuard.Key,
        local: List<IdAndTime>,
    ): Boolean {
        val lastPass = heldAfter.remove(key)?.takeIf { it.stance == waivers.stance(up.url) }
        val held = lastPass?.count ?: peerHeld(up, local) ?: return false
        if (shrink.mayPush(key, local.size, held, waivers.mayBeLensed(up.url))) return true
        withheld.incrementAndGet()
        System.err.println(
            "router: up ${up.url.url} holds $held of the ${local.size} event(s) we hold — too few to believe from a relay " +
                "that may read us through a trust lens; pushing nothing (pushesWithheld)",
        )
        return false
    }

    /** The store as quartz's group reads it: the by-id fetches of what an upstream lacks, booked like every other store call. */
    private class BookedReads(
        private val store: IEventStore,
    ) : IEventStore by store {
        override suspend fun <T : Event> query(filter: Filter): List<T> =
            storeCall(StoreCalls.CALLER_PUSH_UPSTREAM, StoreCalls.OP_QUERY, StoreCalls.ids(filter.ids?.size ?: 0)) {
                store.query(filter)
            }
    }

    companion object {
        /** Events awaiting an upstream's `OK` at once: its answers pace the push. */
        const val PUSH_WINDOW = 4

        private const val ID_FETCH_CHUNK = 500

        /**
         * Up only, over the pass's own snapshot. No deletion settle: its `applyDown` writes a relay's
         * kind 5 into a store this process opens read-only. The budget counts from the reconcile's start.
         */
        fun upOnly(intervalSec: Long): NegentropyStoreSync.Config =
            NegentropyStoreSync.Config(
                down = false,
                up = true,
                syncDeletions = false,
                pageFallback = false,
                idChunk = ID_FETCH_CHUNK,
                reconcileConcurrency = 1,
                idleTimeoutMs = NEG_IDLE_MS,
                uploadWindow = PUSH_WINDOW,
                uploadDeadlineMs = intervalSec * 1000,
            )
    }
}
