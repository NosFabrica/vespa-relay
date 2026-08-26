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
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
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
 * READS DECLARE THEIR LENS. [LensRequiredPolicy] refuses a REQ or a COUNT from
 * a connection that has not authenticated unless the query names an observer
 * or waives one with `include:spam` — this store has no house observer, so a
 * read with neither is the whole corpus with the trust switched off, and that
 * is an answer a client should have to ask for rather than one it gets by
 * saying nothing.
 *
 * NIP-42 runs through [MultiAddressAuthPolicy] rather than quartz's
 * OptionalAuthPolicy, so a client that dialled the relay's hidden service — and
 * therefore signs that address — authenticates as itself instead of quietly
 * losing its ranking lens. That same policy is what carries [onAuthenticated],
 * the login hook [TrustNotice] hangs off.
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
    // Whether a read from a connection that has not authenticated must declare
    // its lens — `observer:` or `include:spam` — to be answered at all. See
    // [LensRequiredPolicy]; false is the older relay that answered anonymous
    // reads out of the whole corpus without either side saying so.
    requireReadLens: Boolean = true,
    // Fires with each authenticated pubkey seen on a ranked read.
    onObserver: ((String) -> Unit)? = null,
    // Whether a NIP-50 search also answers with the records its Trusted Lists,
    // Trusted Assertions and NIP-32 labels point at, and how much of the feed
    // that splice may be. See [SearchReferenceExpansion].
    searchExpansion: SearchExpansionLimits = SearchExpansionLimits.Default,
    // Fires once per successful NIP-42 AUTH, with the connection's send —
    // TrustNotice::check is what the composition root puts here. Unset is a
    // relay that stays silent on login.
    onAuthenticated: AuthNotifier? = null,
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
                    if (requireReadLens) LensRequiredPolicy() else null,
                    VerifyAuthOnlyPolicy,
                    MultiAddressAuthPolicy(relayUrl, alsoServedAt(), onAuthenticated),
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
        ObserverBackend(LiveEventStore(store, ingest), store, onObserver, servingPressure, searchExpansion)

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
 * Reads from an authenticated connection run in a [StoreQueryContext]
 * carrying that caller's pubkey, which the store turns into the caller's
 * web-of-trust ranking lens. An anonymous caller gets NO observer,
 * deliberately. The store treats the observer as a filter, so a house
 * observer would gate anonymous visitors to the sliver of the corpus that
 * observer has scored (~0.1% measured here) and tell them nothing about it.
 * Anonymous means the whole corpus, unranked; a caller who wants a lens asks
 * with NIP-50's `observer:` extension.
 *
 * What CHANGED around that is who may ask for the unranked corpus without
 * saying so: nobody. [LensRequiredPolicy] now refuses an anonymous read that
 * declares neither `observer:` nor `include:spam` before it reaches here, so
 * the no-observer branch below is the deliberate `include:spam` answer rather
 * than the default one. This class is unchanged by that on purpose — the
 * policy decides what is ANSWERED, this decides what a read is answered
 * THROUGH, and an operator who turns the policy off still gets exactly the
 * behaviour documented above.
 *
 * A SEARCH ALSO CARRIES ITS SUBJECTS. When a REQ actually searches — free
 * text, not just the lens tokens every anonymous read has to carry — the
 * stored replay is passed through [SearchReferenceExpansion], which splices
 * the record each Trusted List / Trusted Assertion / NIP-32 label points at
 * into the feed right behind it — the two provider-published families only for
 * a reader whose own kind-10040 named the signer, which is why this is also
 * where the read's observer is resolved. Everything else — a mirror's paging, a
 * negentropy catch-up, a plain `#p` recall — takes the delegating path below
 * untouched, which is why the gate is `isSearch` rather than "has a `search`
 * field".
 */
internal class ObserverBackend(
    private val inner: LiveEventStore,
    /**
     * The store underneath [inner], for the expansion's subject lookup alone.
     * It goes straight to the store rather than through `LiveEventStore.
     * snapshotQuery` because that helper walks a multi-filter list ONE FILTER
     * AT A TIME, while the store's own `query(List<Filter>)` recalls them
     * concurrently under its fan-out bound and dedups across them — which is
     * the whole reason the expansion batches its lookups.
     */
    private val store: IEventStore,
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
    private val searchExpansion: SearchExpansionLimits = SearchExpansionLimits.Default,
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) {
        val expansion = expansionFor(ctx, filters)
        if (expansion == null) {
            inner.query(ctx, filters, onEach, timedEose(onEose))
        } else {
            expanded(
                expansion = expansion,
                onEose = timedEose(onEose),
                replay = { gate, eose -> inner.query(ctx, filters, gate::offer, eose) },
                idOf = Event::id,
                pointerOf = { it },
                emit = onEach,
                emitExtra = onEach,
            )
        }
    }

    override suspend fun queryRaw(
        ctx: RequestContext,
        filters: List<Filter>,
        onEachStored: (RawEvent) -> Unit,
        onEachLive: (Event, String) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) {
        val expansion = expansionFor(ctx, filters)
        if (expansion == null) {
            inner.queryRaw(ctx, filters, onEachStored, onEachLive, timedEose(onEose))
        } else {
            expanded(
                expansion = expansion,
                onEose = timedEose(onEose),
                replay = { gate, eose ->
                    inner.queryRaw(
                        ctx,
                        filters,
                        onEachStored = { gate.offer(RawRow.Stored(it)) },
                        onEachLive = { event, body -> gate.offer(RawRow.Live(event, body)) },
                        onEose = eose,
                    )
                },
                idOf = RawRow::id,
                pointerOf = RawRow::pointer,
                emit = { row ->
                    when (row) {
                        is RawRow.Stored -> onEachStored(row.raw)
                        is RawRow.Live -> onEachLive(row.event, row.body)
                    }
                },
                // A spliced subject is a stored event by construction — the
                // lookup that found it is a store recall — so it rides the
                // same frame path as the rest of the replay.
                emitExtra = { onEachStored(RawEvent.fromEvent(it)) },
            )
        }
    }

    /** The expansion this REQ gets, or null for the untouched delegating path. */
    private fun expansionFor(
        ctx: RequestContext,
        filters: List<Filter>,
    ): SearchReferenceExpansion? {
        if (!searchExpansion.enabled) return null
        if (!SearchReferenceExpansion.isSearch(filters)) return null
        // Cheapest gate of the three, and the one that keeps the ordinary
        // note-or-profile search on the untouched path: a REQ whose `kinds`
        // hold no pointer kind can never be answered with a pointer.
        if (!SearchReferenceExpansion.couldPoint(filters)) return null
        val observers = SearchReferenceExpansion.observersOf(filters, ctx.authenticatedUsers.firstOrNull())
        return SearchReferenceExpansion(filters, observers, searchExpansion) { store.query<Event>(it) }
    }

    /**
     * The replay, held just long enough to look its subjects up.
     *
     * The shape is forced by the seam: a delivery callback is not suspending —
     * it is called from inside the store's own loop, and after EOSE from the
     * ingest queue's drain coroutine — so a store lookup cannot happen inside
     * one. So the rows land in a [ReplayGate] instead, and the flush that
     * drains it is launched from the EOSE callback as a coroutine that CAN
     * suspend.
     *
     * UNDISPATCHED, and that is the load-bearing word. Quartz's session runs a
     * REQ undispatched precisely so a small read's frames and its EOSE go out
     * with no scheduler hop (its own comment cites `SmallReqFloorBenchmark`:
     * the hop was most of the dispatch slice on small REQs). An earlier draft
     * here ran the replay as a child and had the outer coroutine `await` its
     * EOSE, which put that hop back on EVERY search — `SearchExpansionCostBench`
     * measured it at ~90us flat, some 20% of a 50-row search on the in-memory
     * index. Launched undispatched from inside the callback, the flush runs
     * INLINE on the replay's own thread and only suspends if it actually has a
     * subject to look up: [SearchReferenceExpansion.expand] returns without
     * ever suspending when no row's kind points anywhere, which is the common
     * search. The hop is then paid only by the pages that earn it.
     *
     * Buffering the replay to do this costs almost nothing: the store
     * materializes the whole result before it calls back at all
     * (`NostrSemanticsStore.query(filters, onEach)` is `query(filters).
     * forEach(onEach)`), so the gate holds references to rows that are already
     * resident, not a second copy of the page.
     *
     * The loop drains until the gate is empty rather than once, because a live
     * event accepted mid-replay lands in the same gate — it is part of the page
     * the client sees before EOSE, exactly as it was before this — and gets its
     * subjects looked up with it. It cannot spin: [ReplayGate.take] returns
     * null the first time it finds nothing, and only real ingest refills it.
     */
    private suspend fun <T> expanded(
        expansion: SearchReferenceExpansion,
        onEose: () -> Unit,
        replay: suspend (ReplayGate<T>, () -> Unit) -> Unit,
        idOf: (T) -> String,
        pointerOf: (T) -> Event?,
        emit: (T) -> Unit,
        emitExtra: (Event) -> Unit,
    ): Unit =
        coroutineScope {
            val gate = ReplayGate<T>()
            replay(gate) {
                launch(start = CoroutineStart.UNDISPATCHED) {
                    while (true) {
                        val batch = gate.take() ?: break
                        // `record` both marks and answers "first time?", so
                        // one pass does the cross-batch dedupe AND the
                        // within-batch one a concurrent live delivery needs.
                        val fresh = batch.map { expansion.record(idOf(it)) }
                        val subjects = expansion.expand(batch, pointerOf)
                        batch.forEachIndexed { i, row ->
                            if (fresh[i]) emit(row)
                            subjects[i].forEach(emitExtra)
                        }
                    }
                    onEose()
                    // Past EOSE this is a live tail, and a lookup on the ingest
                    // queue's drain coroutine would stall the batch writer for
                    // every other subscriber — so live events pass straight
                    // through from here.
                    gate.open(emit)
                }
            }
        }

    override suspend fun count(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Int = ranked(ctx) { timed { inner.count(ctx, filters) } }

    override suspend fun countResult(
        ctx: RequestContext,
        filters: List<Filter>,
    ): CountResult = ranked(ctx) { timed { inner.countResult(ctx, filters) } }

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

/**
 * One subscription's deliveries, held while [SearchReferenceExpansion] looks
 * the page's subjects up, then let through for good.
 *
 * Two coroutines write here — the replay and the [IngestQueue] drain that
 * fans live events out — so every transition takes the lock, and the sink is
 * called UNDER it. That is deliberate: it is the only thing keeping a live
 * event that lands mid-[open] from overtaking the leftovers being drained
 * beside it. The cost is a lock this subscription already effectively had, on
 * a path that was already serialized per connection.
 */
internal class ReplayGate<T> {
    private val lock = Any()

    /** Non-null while the gate is holding; null once [open] has let it go. */
    private var buffered: MutableList<T>? = ArrayList()

    private var sink: ((T) -> Unit)? = null

    /** Takes one delivery — buffering it, or passing it on once the gate is open. */
    fun offer(row: T) {
        synchronized(lock) {
            val buf = buffered
            if (buf != null) buf.add(row) else sink?.invoke(row)
        }
    }

    /** What has arrived since the last call, or null when nothing has — the loop's exit. */
    fun take(): List<T>? =
        synchronized(lock) {
            val buf = buffered ?: return null
            if (buf.isEmpty()) return null
            val batch = ArrayList(buf)
            buf.clear()
            batch
        }

    /** Stops holding: the leftovers go to [to], and so does everything after. */
    fun open(to: (T) -> Unit) {
        synchronized(lock) {
            val buf = buffered ?: return
            buffered = null
            sink = to
            buf.forEach(to)
        }
    }
}

/**
 * A row of the zero-decode replay: a stored event still in storage form, or a
 * live one that already carries its serialized wire body.
 *
 * [pointer] is where the raw path keeps its win. `kind` is a field on a
 * [RawEvent], so the tags parse and the `EventFactory` dispatch the expansion
 * needs are paid ONLY by the handful of rows whose kind points at something;
 * every other row reaches the wire spliced, exactly as before, and answers
 * null here.
 */
internal sealed interface RawRow {
    val id: String

    val pointer: Event?

    class Stored(
        val raw: RawEvent,
    ) : RawRow {
        override val id: String get() = raw.id

        override val pointer: Event? get() = if (raw.kind in SearchReferences.KINDS) raw.toEvent() else null
    }

    class Live(
        val event: Event,
        val body: String,
    ) : RawRow {
        override val id: String get() = event.id

        override val pointer: Event get() = event
    }
}
