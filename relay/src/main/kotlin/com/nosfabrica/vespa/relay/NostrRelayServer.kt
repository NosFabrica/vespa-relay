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
package com.nosfabrica.vespa.relay

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
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyStack
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PubkeyAllowDenyPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RejectFutureEventsPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.VerifyAuthOnlyPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanListPolicy
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/**
 * The Nostr relay: Quartz's protocol engine over the Vespa-backed store.
 * It provides one store, full NIP-01 filters, NIP-50 search, live
 * subscriptions, EVENT publishes, NIP-45 COUNT, and server-side NIP-77
 * negentropy. Most of this is inherited from [RelayServerBase] and
 * [LiveEventStore]. The store supplies storage semantics and
 * `snapshotIdsForNegentropy`.
 *
 * Each connection runs a small policy stack. [VerifyAuthOnlyPolicy] verifies
 * NIP-42 AUTH events inline; EVENT publishes are NOT verified here — instead
 * [IngestQueue] verifies their id and signature in a parallel stage off the
 * hot path (the store itself never verifies). An optional [BanListPolicy]
 * (present only when NIP-86 admin is configured) rejects banned pubkeys and
 * event ids before they reach the queue. [OptionalAuthPolicy] sends a NIP-42
 * challenge on connect but gates nothing on it.
 *
 * [limits] are enforced by the engine (which auto-installs a LimitsPolicy) and
 * also drive the NIP-11 `limitation` block, so what the doc advertises and
 * what the relay rejects can never disagree. [negentropySettings] bound NIP-77
 * reconciliation (frame size, max events synced, sessions per connection).
 *
 * What auth does change is ranking. [ObserverRoutingBackend] gives every
 * REQ/COUNT from an authenticated connection that caller's own web of trust as
 * its lens. An UNAUTHENTICATED connection gets no observer at all — see
 * [ObserverRoutingBackend] for why it no longer gets a house one.
 *
 * [close] shuts down the connections and the ingest writer, but not the
 * store. The composition root owns the store, and the sync service shares it.
 */
class NostrRelayServer(
    store: IEventStore,
    relayUrl: NormalizedRelayUrl,
    parentContext: CoroutineContext = SupervisorJob(),
    listener: RelayServerListener = RelayServerListener.None,
    limits: RelayLimits? = defaultRelayLimits(),
    negentropySettings: NegentropySettings = NegentropySettings.Default,
    // When set, published events from banned pubkeys / banned event ids are
    // rejected before ingest. Shared with the NIP-86 admin endpoint, which
    // mutates the same store.
    banStore: BanStore? = null,
    // Static write authorization (deploy-time, not runtime like the banStore).
    // Empty allowlists ⇒ permissive; denylists always subtract.
    pubkeyAllow: Set<String> = emptySet(),
    pubkeyDeny: Set<String> = emptySet(),
    kindAllow: Set<Int> = emptySet(),
    kindDeny: Set<Int> = emptySet(),
    // Reject events dated more than this many seconds in the future; 0 disables.
    rejectFutureSeconds: Int = 0,
    // Fires with each authenticated pubkey seen on a ranked read. This lets
    // the composition root enroll NIP-42 logins as sync observers
    // (SyncService.enroll dedups).
    onObserver: ((String) -> Unit)? = null,
    // Shared with the router so mirroring yields when clients start waiting.
    // Null leaves reads untimed and ingest unthrottled.
    private val servingPressure: ServingPressure? = null,
) : RelayServerBase(
        // Events are verified in the ingest queue's parallel stage, so the
        // policy only verifies AUTH. Cheap rejections (bans, allow/deny lists,
        // future-dated events) run first; OptionalAuthPolicy issues the NIP-42
        // challenge. Only the policies an operator configured are installed.
        policyBuilder = {
            PolicyStack(
                *listOfNotNull(
                    banStore?.let(::BanListPolicy),
                    if (pubkeyAllow.isNotEmpty() || pubkeyDeny.isNotEmpty()) PubkeyAllowDenyPolicy(pubkeyAllow, pubkeyDeny) else null,
                    if (kindAllow.isNotEmpty() || kindDeny.isNotEmpty()) KindAllowDenyPolicy(kindAllow, kindDeny) else null,
                    if (rejectFutureSeconds > 0) RejectFutureEventsPolicy(rejectFutureSeconds) else null,
                    VerifyAuthOnlyPolicy,
                    OptionalAuthPolicy(relayUrl),
                ).toTypedArray<IRelayPolicy>(),
            )
        },
        parentContext = parentContext,
        negentropySettings = negentropySettings,
        listener = listener,
        limits = limits,
    ) {
    // The queue verifies each event's id + signature in its own parallel stage
    // (off the connection's hot path), which is why the policy stack only needs
    // VerifyAuthOnlyPolicy. Invalid events get an InsertOutcome.Rejected -> OK:false.
    private val ingest = IngestQueue(store = store, parentContext = parentContext, verify = { it.verify() })

    override val backend: SessionBackend =
        ObserverRoutingBackend(LiveEventStore(store, ingest), onObserver, servingPressure)

    override fun close() {
        closeConnections()
        ingest.close()
        scope.cancel()
    }
}

/**
 * Delegates everything to [LiveEventStore], wrapping each read from an
 * AUTHENTICATED connection in a [StoreQueryContext] carrying that caller's
 * pubkey. The store reads the element back out when it builds the Vespa query;
 * this is how a per-connection fact crosses the caller-agnostic `IEventStore`
 * interface.
 *
 * ## An anonymous caller gets no observer, deliberately
 *
 * There used to be a `DEFAULT_OBSERVER` here: anonymous reads ran through an
 * operator-chosen pubkey's web of trust, on the reasoning that some ranking
 * beats none over a 50M-event corpus.
 *
 * That reasoning does not survive the store treating the observer as a FILTER
 * rather than an ordering — which it now does, counting and returning only
 * authors the observer has scored. Measured on this deployment: 335,971 of
 * 12.28M profiles are scored by anyone at all (2.7%), the configured observer's
 * provider publishes 145,968 scores, and we hold 14,195 of them. An anonymous
 * visitor would have been gated to roughly a thousandth of the corpus and told
 * nothing about it.
 *
 * It was also an editorial claim nobody made out loud — *this relay ranks the
 * world through this one person's web of trust* — applied to every reader who
 * had not logged in, and invisible to all of them.
 *
 * So anonymous now means anonymous: the whole corpus, unranked. A caller who
 * wants a specific lens asks for it with NIP-50's `observer:` extension, which
 * the store already parses, and that request is visible in the query rather
 * than baked into the deployment.
 */
internal class ObserverRoutingBackend(
    private val inner: LiveEventStore,
    private val onObserver: ((String) -> Unit)? = null,
    // Every read is timed here, so the mirror can yield when clients start
    // waiting. This is the only place that sees serving latency.
    private val pressure: ServingPressure? = null,
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx, filters) { inner.query(ctx, filters, onEach, onEose) }

    override suspend fun count(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Int = ranked(ctx, filters) { inner.count(ctx, filters) }

    override suspend fun countResult(
        ctx: RequestContext,
        filters: List<Filter>,
    ): CountResult = ranked(ctx, filters) { inner.countResult(ctx, filters) }

    override suspend fun submit(
        event: Event,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = inner.submit(event, onComplete)

    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> = inner.snapshotIdsForNegentropy(filters, maxEntries)

    private suspend fun <T> ranked(
        ctx: RequestContext,
        filters: List<Filter>,
        block: suspend () -> T,
    ): T {
        val observer = ctx.authenticatedUsers.firstOrNull()
        observer?.let { onObserver?.invoke(it) }
        // Crosses IEventStore's caller-agnostic interface on the coroutine
        // context, so no query/count signature has to widen to carry it.
        //
        // Quartz's own relay path installs this element for authenticated
        // connections too. This wrapper stays because it is also where every
        // read is timed for [ServingPressure], and because the observer we
        // install is the one this relay enrolls as a sync observer.
        //
        // There used to be a second element here, OriginalFilters, carrying the
        // NIP-50 extensions quartz's engine stripped before the store saw them.
        // The IEventStore contract now passes `search` VERBATIM and the store
        // parses `sort:`/`filter:rank:`/`include:spam`/`observer:` itself, so
        // there is nothing left to preserve.
        val startedMs = System.currentTimeMillis()
        try {
            if (observer == null) return block()
            return withContext(StoreQueryContext(setOf(observer))) { block() }
        } finally {
            // In a finally: a read that THREW still occupied the engine, and a
            // read that timed out is the most important sample there is.
            pressure?.record(System.currentTimeMillis() - startedMs)
        }
    }
}
