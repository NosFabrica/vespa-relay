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
 * The Nostr relay: quartz's protocol engine ([RelayServerBase]) over the
 * Vespa-backed store.
 *
 * The policy stack verifies AUTH inline; EVENT publishes are verified by
 * [IngestQueue] off the hot path. [limits] are enforced by the engine and
 * render the NIP-11 `limitation` block, so the two cannot disagree.
 *
 * Reads declare their lens: [LensRequiredPolicy] refuses an unauthenticated
 * REQ or COUNT that names no observer and does not waive one. NIP-42 runs
 * through [MultiAddressAuthPolicy] so a client that dialled the hidden service
 * authenticates as itself; that policy also carries [onAuthenticated].
 *
 * [close] shuts the connections and the ingest writer, not the store, which
 * the composition root owns.
 */
class NostrRelayServer(
    store: IEventStore,
    relayUrl: NormalizedRelayUrl,
    // Asked per connection: Tor mints the hidden service's address on its first start, which can be after ours.
    alsoServedAt: () -> Set<NormalizedRelayUrl> = { emptySet() },
    parentContext: CoroutineContext = SupervisorJob(),
    listener: RelayServerListener = RelayServerListener.None,
    limits: RelayLimits? = defaultRelayLimits(),
    negentropySettings: NegentropySettings = NegentropySettings.Default,
    // Shared with the NIP-86 admin endpoint; bans apply before ingest.
    banStore: BanStore? = null,
    // Deploy-time write authorization. Empty allowlists are permissive; denylists always subtract.
    pubkeyAllow: Set<String> = emptySet(),
    pubkeyDeny: Set<String> = emptySet(),
    kindAllow: Set<Int> = emptySet(),
    kindDeny: Set<Int> = emptySet(),
    rejectFutureSeconds: Int = 0,
    // See [LensRequiredPolicy]; false answers anonymous reads out of the whole corpus.
    requireReadLens: Boolean = true,
    onObserver: ((String) -> Unit)? = null,
    // Fires once per successful NIP-42 AUTH with the connection's send.
    onAuthenticated: AuthNotifier? = null,
    // Only recorded here; the sync process polls the mean over GET /pressure.
    private val servingPressure: ServingPressure? = null,
    // Ranked reads one connection may run at once; 0 turns the gate off. See [SearchGate].
    searchConcurrencyPerConnection: Int = SearchGate.DEFAULT_PERMITS,
    // A default parameter so the gate exists before the super constructor takes it as the listener.
    private val searchGate: SearchGate = SearchGate(searchConcurrencyPerConnection),
) : RelayServerBase(
        // Cheap rejections first; only the policies an operator configured are installed.
        policyBuilder = {
            PolicyStack(
                *listOfNotNull(
                    banStore?.let(::BanListPolicy),
                    if (pubkeyAllow.isNotEmpty() || pubkeyDeny.isNotEmpty()) PubkeyAllowDenyPolicy(pubkeyAllow, pubkeyDeny) else null,
                    if (kindAllow.isNotEmpty() || kindDeny.isNotEmpty()) KindAllowDenyPolicy(kindAllow, kindDeny) else null,
                    if (rejectFutureSeconds > 0) RejectFutureEventsPolicy(rejectFutureSeconds) else null,
                    if (requireReadLens) LensRequiredPolicy() else null,
                    VerifyAuthOnlyPolicy,
                    MultiAddressAuthPolicy(relayUrl, alsoServedAt(), onAuthenticated),
                ).toTypedArray<IRelayPolicy>(),
            )
        },
        parentContext = parentContext,
        negentropySettings = negentropySettings,
        listener = searchGate.listening(listener),
        limits = limits,
    ) {
    private val ingest = IngestQueue(store = store, parentContext = parentContext, verify = { it.verify() })

    override val backend: SessionBackend =
        ObserverBackend(LiveEventStore(store, ingest), onObserver, servingPressure, searchGate)

    /** Connections holding a search lane right now. See [SearchGate.lanesOpen]. */
    val searchLanesOpen: Int get() = searchGate.lanesOpen

    override fun close() {
        closeConnections()
        ingest.close()
        scope.cancel()
    }
}

/**
 * The session-facing wrapper over [LiveEventStore]. Every read path is
 * delegated, [queryRaw] and [sealedNegentropyStorage] included: the interface
 * defaults would rebuild events and snapshots the engine already optimized away.
 *
 * A read from an authenticated connection runs in a [StoreQueryContext]
 * carrying the caller's pubkey, which the store applies as a web-of-trust
 * filter. An anonymous caller gets no observer and the whole corpus, unranked;
 * [LensRequiredPolicy] decides whether such a read is answered at all, and
 * this class decides only what a read is answered through.
 *
 * The reference expansion lives in the store: it needs a suspending lookup
 * mid-page, which no callback on this interface can make.
 */
internal class ObserverBackend(
    private val inner: LiveEventStore,
    private val onObserver: ((String) -> Unit)? = null,
    /**
     * Sampled from the start of a read to its EOSE. A REQ parks until the
     * client closes it, so timing the whole call would record subscription
     * lifetimes as read latency. Prompt calls (COUNT) are timed whole.
     */
    private val pressure: ServingPressure? = null,
    /** Taken inside the timed span: a read queued behind a previous search took that long from where the client sits. */
    private val gate: SearchGate = SearchGate(0),
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) { gate.through(ctx, filters, timedEose(onEose)) { eose -> inner.query(ctx, filters, onEach, eose) } }

    override suspend fun queryRaw(
        ctx: RequestContext,
        filters: List<Filter>,
        onEachStored: (RawEvent) -> Unit,
        onEachLive: (Event, String) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) { gate.through(ctx, filters, timedEose(onEose)) { eose -> inner.queryRaw(ctx, filters, onEachStored, onEachLive, eose) } }

    override suspend fun count(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Int = ranked(ctx) { timed { gate.throughPrompt(ctx, filters) { inner.count(ctx, filters) } } }

    override suspend fun countResult(
        ctx: RequestContext,
        filters: List<Filter>,
    ): CountResult = ranked(ctx) { timed { gate.throughPrompt(ctx, filters) { inner.countResult(ctx, filters) } } }

    override suspend fun submit(
        event: Event,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = inner.submit(event, onComplete)

    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> = inner.snapshotIdsForNegentropy(filters, maxEntries)

    override suspend fun sealedNegentropyStorage(
        filters: List<Filter>,
        maxEntries: Int,
    ): IStorage? = inner.sealedNegentropyStorage(filters, maxEntries)

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
