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

import com.nosfabrica.vespa.relay.config.defaultRelayLimits
import com.vitorpamplona.negentropy.storage.IStorage
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.CountResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerBase
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.SessionBackend
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.KindAllowDenyPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyStack
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PubkeyAllowDenyPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RejectFutureEventsPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.VerifyAuthOnlyPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanListPolicy
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/**
 * The Nostr relay: Quartz's protocol engine ([RelayServerBase]) over the
 * Vespa-backed store. NIP-01 filters, NIP-50 search, live subscriptions,
 * NIP-45 COUNT and server-side NIP-77 come from the engine and the store.
 *
 * The policy stack only verifies AUTH inline; EVENT publishes are verified by
 * [IngestQueue] in a parallel stage off the hot path. [limits] are enforced by
 * the engine and also drive the NIP-11 `limitation` block, so the doc and the
 * enforcement can never disagree.
 *
 * NIP-42 runs through [MultiAddressAuthPolicy] rather than quartz's
 * OptionalAuthPolicy, so a client that dialled the relay's hidden service — and
 * therefore signs that address — authenticates as itself instead of quietly
 * losing its ranking lens.
 *
 * [close] shuts down the connections and the ingest writer, but not the store —
 * the composition root owns that — and the sync process holds its own handle
 * to the same cluster, so "done with the store" is per-process anyway.
 */
class NostrRelayServer(
    store: IEventStore,
    relayUrl: NormalizedRelayUrl,
    // Other addresses the same relay answers at — a `.onion` in front of this
    // port. Asked per connection, not once at boot: Tor generates the hidden
    // service's address the first time it starts, which can be after this
    // process is already serving.
    alsoServedAt: () -> Set<NormalizedRelayUrl> = { emptySet() },
    parentContext: CoroutineContext = SupervisorJob(),
    listener: RelayServerListener = RelayServerListener.None,
    limits: RelayLimits? = defaultRelayLimits(),
    negentropySettings: NegentropySettings = NegentropySettings.Default,
    // When set, published events from banned pubkeys / event ids are rejected
    // before ingest. Shared with the NIP-86 admin endpoint.
    banStore: BanStore? = null,
    // Static write authorization (deploy-time, unlike the banStore). Empty
    // allowlists ⇒ permissive; denylists always subtract.
    pubkeyAllow: Set<String> = emptySet(),
    pubkeyDeny: Set<String> = emptySet(),
    kindAllow: Set<Int> = emptySet(),
    kindDeny: Set<Int> = emptySet(),
    // Reject events dated more than this many seconds in the future; 0 disables.
    rejectFutureSeconds: Int = 0,
    // Fires with each authenticated pubkey seen on a ranked read.
    onObserver: ((String) -> Unit)? = null,
    // Only recorded into here; the consumer is the sync process, which polls
    // the mean over GET /pressure and yields its ingest on it.
    private val servingPressure: ServingPressure? = null,
) : RelayServerBase(
        // Cheap rejections (bans, allow/deny lists, future-dated events) run
        // first; only the policies an operator configured are installed.
        policyBuilder = {
            PolicyStack(
                *listOfNotNull(
                    banStore?.let(::BanListPolicy),
                    if (pubkeyAllow.isNotEmpty() || pubkeyDeny.isNotEmpty()) PubkeyAllowDenyPolicy(pubkeyAllow, pubkeyDeny) else null,
                    if (kindAllow.isNotEmpty() || kindDeny.isNotEmpty()) KindAllowDenyPolicy(kindAllow, kindDeny) else null,
                    if (rejectFutureSeconds > 0) RejectFutureEventsPolicy(rejectFutureSeconds) else null,
                    VerifyAuthOnlyPolicy,
                    MultiAddressAuthPolicy(relayUrl, alsoServedAt()),
                ).toTypedArray<IRelayPolicy>(),
            )
        },
        parentContext = parentContext,
        negentropySettings = negentropySettings,
        listener = listener,
        limits = limits,
    ) {
    private val ingest = IngestQueue(store = store, parentContext = parentContext, verify = { it.verify() })

    override val backend: SessionBackend =
        ObserverBackend(LiveEventStore(store, ingest), onObserver, servingPressure)

    override fun close() {
        closeConnections()
        ingest.close()
        scope.cancel()
    }
}

/**
 * The session-facing wrapper over [LiveEventStore]. Every read path is
 * delegated — including [queryRaw] (the zero-decode replay splice and the
 * fanout's shared wire body) and [sealedNegentropyStorage] (the NEG-OPEN
 * snapshot cache); falling back to the interface defaults on either would
 * silently rebuild events and snapshots the engine already optimized away.
 *
 * Every filter that reaches the store passes through [quotingDashedLiterals]
 * first, so a search token the reader decorated with dashes is read as the text
 * it is instead of as NIP-50's exclusion operator. That rewrite is here rather
 * than in the web UI's query builder because the misreading belongs to the
 * relay's answer, not to one client's search box: it made THIS relay reply to a
 * text search with an unrelated recency feed, whoever asked.
 *
 * Reads from an authenticated connection run in a [StoreQueryContext]
 * carrying that caller's pubkey, which the store turns into the caller's
 * web-of-trust ranking lens. An anonymous caller gets NO observer,
 * deliberately. The store treats the observer as a filter, so a house
 * observer would gate anonymous visitors to the sliver of the corpus that
 * observer has scored (~0.1% measured here) and tell them nothing about it.
 * Anonymous means the whole corpus, unranked; a caller who wants a lens asks
 * with NIP-50's `observer:` extension.
 */
internal class ObserverBackend(
    private val inner: LiveEventStore,
    private val onObserver: ((String) -> Unit)? = null,
    /**
     * Serving latency is sampled from the start of a read to its EOSE — the
     * replay is the engine work. The call itself does NOT return at EOSE: a
     * REQ parks until the client closes it, so timing the whole call would
     * record subscription lifetimes (minutes, hours) as read latency and pin
     * the ingest backoff at max forever. Prompt calls (COUNT) are timed
     * whole, in a finally — a count that timed out is the most important
     * sample there is.
     */
    private val pressure: ServingPressure? = null,
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) { inner.query(ctx, filters.quotingDashedLiterals(), onEach, timedEose(onEose)) }

    override suspend fun queryRaw(
        ctx: RequestContext,
        filters: List<Filter>,
        onEachStored: (RawEvent) -> Unit,
        onEachLive: (Event, String) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) { inner.queryRaw(ctx, filters.quotingDashedLiterals(), onEachStored, onEachLive, timedEose(onEose)) }

    override suspend fun count(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Int = ranked(ctx) { timed { inner.count(ctx, filters.quotingDashedLiterals()) } }

    override suspend fun countResult(
        ctx: RequestContext,
        filters: List<Filter>,
    ): CountResult = ranked(ctx) { timed { inner.countResult(ctx, filters.quotingDashedLiterals()) } }

    override suspend fun submit(
        event: Event,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = inner.submit(event, onComplete)

    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> = inner.snapshotIdsForNegentropy(filters.quotingDashedLiterals(), maxEntries)

    override suspend fun sealedNegentropyStorage(
        filters: List<Filter>,
        maxEntries: Int,
    ): IStorage? = inner.sealedNegentropyStorage(filters.quotingDashedLiterals(), maxEntries)

    private suspend fun <T> ranked(
        ctx: RequestContext,
        block: suspend () -> T,
    ): T {
        val observer = ctx.authenticatedUsers.firstOrNull()
        observer?.let { onObserver?.invoke(it) }
        if (observer == null) return block()
        return withContext(StoreQueryContext(setOf(observer))) { block() }
    }

    /** Starts the clock now; the returned EOSE records the replay span once. */
    private fun timedEose(onEose: () -> Unit): () -> Unit {
        if (pressure == null) return onEose
        val startedNs = System.nanoTime()
        return {
            pressure.record((System.nanoTime() - startedNs) / 1_000_000)
            onEose()
        }
    }

    private suspend fun <T> timed(block: suspend () -> T): T {
        if (pressure == null) return block()
        val startedNs = System.nanoTime()
        try {
            return block()
        } finally {
            pressure.record((System.nanoTime() - startedNs) / 1_000_000)
        }
    }
}
