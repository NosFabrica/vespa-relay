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

import com.nosfabrica.vespa.eventstore.LiveGate
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.server.config.defaultRelayLimits
import com.vitorpamplona.negentropy.storage.IStorage
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.CountResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerBase
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The Nostr relay: quartz's protocol engine ([RelayServerBase]) over the Vespa-backed store.
 * [limits] are enforced by the engine and render the NIP-11 `limitation` block. Publishes go to
 * [ingest] when given, else to a quartz [IngestQueue] built here. [close] shuts the connections and
 * that queue, never a given [ingest] or the store, which the composition root owns.
 */
class NostrRelayServer(
    store: IEventStore,
    relayUrl: NormalizedRelayUrl,
    // Asked per connection: the hidden service's address can appear after this server started.
    alsoServedAt: () -> Set<NormalizedRelayUrl> = { emptySet() },
    parentContext: CoroutineContext = SupervisorJob(),
    listener: RelayServerListener = RelayServerListener.None,
    limits: RelayLimits? = defaultRelayLimits(),
    negentropySettings: NegentropySettings = NegentropySettings.Default,
    // Shared with the NIP-86 admin endpoint; bans apply before ingest.
    banStore: BanStore? = null,
    // Empty allowlists are permissive; denylists always subtract.
    pubkeyAllow: Set<String> = emptySet(),
    pubkeyDeny: Set<String> = emptySet(),
    kindAllow: Set<Int> = emptySet(),
    kindDeny: Set<Int> = emptySet(),
    rejectFutureSeconds: Int = 0,
    // False answers anonymous reads out of the whole corpus, see [LensRequiredPolicy].
    requireReadLens: Boolean = true,
    onObserver: ((String) -> Unit)? = null,
    // Fires once per successful NIP-42 AUTH with the connection's send.
    onAuthenticated: AuthNotifier? = null,
    private val servingPressure: ServingPressure? = null,
    // Ranked reads one connection may run at once; 0 turns the gate off.
    searchConcurrencyPerConnection: Int = SearchGate.DEFAULT_PERMITS,
    // Published events one store write may take; see ingestBatchFromEnv.
    ingestBatch: Int = IngestQueue.DEFAULT_MAX_BATCH,
    ingest: EventIngest? = null,
    // The sync process's lane: what a connection signed in as this relay's own key publishes.
    private val mirrorLane: MirrorLane? = null,
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
                    // Last: a tag must mean every other policy accepted the event.
                    mirrorLane?.policy(),
                ).toTypedArray<IRelayPolicy>(),
            )
        },
        parentContext = parentContext,
        negentropySettings = negentropySettings,
        listener = listener,
        limits = limits,
    ) {
    /** The queue this server built, and so the one it closes; null when the root handed it an [EventIngest]. */
    private val ownQueue =
        if (ingest != null) {
            null
        } else {
            IngestQueue(
                store = store,
                parentContext = parentContext,
                maxBatch = ingestBatch,
                capacity = ingestCapacity(ingestBatch),
                verify = { it.verify() },
            )
        }

    private val ingest: EventIngest = (ingest ?: ownQueue!!).let { clients -> mirrorLane?.over(clients) ?: clients }

    private val searchGate = SearchGate(searchConcurrencyPerConnection)

    private val coldChecks = ColdAuthorChecks()

    override val backend: SessionBackend =
        ObserverBackend(LiveEventStore(store, this.ingest), onObserver, servingPressure, searchGate, liveGatesOf(store), coldChecks)

    /** Connections with a ranked read running or queued right now. */
    val searchLanesOpen: Int get() = searchGate.lanesOpen

    /** Live events the gate dropped since boot: their cold-author check failed, or got no read in time. */
    val liveGateFailed: Long get() = coldChecks.failures
    val liveGateShed: Long get() = coldChecks.shedding

    override fun close() {
        closeConnections()
        ownQueue?.close()
        scope.cancel()
    }
}

/**
 * Room for four batches before a publisher's submit suspends, and never less than quartz's own
 * queue, so the default batch keeps the default capacity.
 */
internal fun ingestCapacity(batch: Int): Int = (batch * 4).coerceAtLeast(IngestQueue.DEFAULT_CAPACITY)

/**
 * The session-facing wrapper over [LiveEventStore]. Every read path is delegated, [queryRaw] and
 * [sealedNegentropyStorage] included, or the interface defaults would rebuild what the engine
 * optimized away. An authenticated read runs in a [StoreQueryContext] carrying the caller's pubkey.
 */
internal class ObserverBackend(
    private val inner: LiveEventStore,
    private val onObserver: ((String) -> Unit)? = null,
    /**
     * Sampled from the start of a read to its EOSE: a REQ parks until the client closes it, so
     * timing the whole call would record subscription lifetimes as read latency.
     */
    private val pressure: ServingPressure? = null,
    /** Taken inside the timed span: a read queued behind a previous search took that long for the client. */
    private val gate: SearchGate = SearchGate(0),
    /**
     * The store's observer gate for LIVE events (see [LiveGate]); null for a store that has none.
     * The stored replay is gated in the engine; events arriving after EOSE are matched in memory
     * by quartz and would otherwise stream a below-floor author the replay just dropped.
     */
    private val liveGates: (suspend (List<Filter>) -> LiveGate?)? = null,
    /** Shared by every subscription, so a burst of cold authors costs a bounded number of store reads. */
    private val coldChecks: ColdAuthorChecks = ColdAuthorChecks(),
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) {
        timedEose(onEose) { onReplayed ->
            gate.through(ctx, filters, onReplayed) { eose -> gatedLive(filters) { admit -> inner.query(ctx, filters, { e -> admit(e) { onEach(e) } }, eose) } }
        }
    }

    override suspend fun queryRaw(
        ctx: RequestContext,
        filters: List<Filter>,
        onEachStored: (RawEvent) -> Unit,
        onEachLive: (Event, String) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) {
        timedEose(onEose) { onReplayed ->
            gate.through(ctx, filters, onReplayed) { eose ->
                gatedLive(filters) { admit -> inner.queryRaw(ctx, filters, onEachStored, { e, body -> admit(e) { onEachLive(e, body) } }, eose) }
            }
        }
    }

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

    /**
     * Runs a subscription with its live deliveries held to [LiveGate], built inside [ranked] so the
     * gate reads the connection's observer. A cold author is judged in a child of this subscription
     * (see [ColdAuthorChecks]) before delivery, never after: quartz's ingest drain must not wait.
     */
    private suspend fun gatedLive(
        filters: List<Filter>,
        block: suspend (admit: (Event, () -> Unit) -> Unit) -> Unit,
    ) {
        val live = liveGates?.invoke(filters) ?: return block { _, send -> send() }
        coroutineScope {
            block { event, send ->
                when (live.admitsNow(event)) {
                    true -> send()
                    false -> Unit
                    null -> coldChecks.start(this, event.pubKey, { live.admitsNow(event) }, { live.admits(event) }, send)
                }
            }
        }
    }

    /** Runs [block] with an EOSE that records the replay span; a read that ends before its EOSE records nothing. */
    private suspend fun <T> timedEose(
        onEose: () -> Unit,
        block: suspend (eose: () -> Unit) -> T,
    ): T {
        val read = pressure?.begin() ?: return block(onEose)
        try {
            return block {
                read.finish()
                onEose()
            }
        } finally {
            // A no-op after the EOSE; otherwise the read leaves the in-flight count unsampled.
            read.abandon()
        }
    }

    private suspend fun <T> timed(block: suspend () -> T): T {
        val read = pressure?.begin() ?: return block()
        try {
            return block()
        } finally {
            read.finish()
        }
    }
}

/**
 * The store reads that judge a live event from an author the gate has not cached, bounded across the
 * relay. Fail closed: a check whose read fails, or that gets no read slot within [queueMillis], drops its event, counted.
 */
internal class ColdAuthorChecks(
    val max: Int = MAX_IN_FLIGHT,
    private val maxWaiting: Int = MAX_WAITING,
    private val queueMillis: Long = QUEUE_MS,
    private val everyMillis: Long = SAY_EVERY_MS,
    private val now: () -> Long = System::currentTimeMillis,
    private val log: (String) -> Unit = System.err::println,
) {
    private enum class Read { DONE, FAILED, SHED, ABANDONED }

    private val slots = Semaphore(max)
    private val checks = AtomicInteger()
    private val reads = ConcurrentHashMap<String, CompletableDeferred<Read>>()
    private val failed = AtomicLong()
    private val shed = AtomicLong()
    private val lastSaidAt = AtomicLong(Long.MIN_VALUE)

    /** Events dropped because their check failed, and because no read slot came free in time. */
    val failures: Long get() = failed.get()
    val shedding: Long get() = shed.get()

    /**
     * Judges one event by [author] in [scope] and calls [send] if admitted; returns at once. Checks on
     * one author wait on the read already in flight, then ask [known], which answers once the cache is warm.
     */
    fun start(
        scope: CoroutineScope,
        author: String,
        known: () -> Boolean?,
        admits: suspend () -> Boolean,
        send: () -> Unit,
    ) {
        if (checks.incrementAndGet() > maxWaiting) {
            checks.decrementAndGet()
            dropShed()
            return
        }
        // Counted down on completion, not in the body: a launch into a cancelled scope never runs its body.
        scope.launch { if (judge(author, known, admits)) send() }.invokeOnCompletion { checks.decrementAndGet() }
    }

    private suspend fun judge(
        author: String,
        known: () -> Boolean?,
        admits: suspend () -> Boolean,
    ): Boolean {
        val mine = CompletableDeferred<Read>()
        val ahead = reads.putIfAbsent(author, mine)
        if (ahead != null) {
            when (ahead.await()) {
                Read.FAILED -> return false.also { failed.incrementAndGet() }
                Read.SHED -> return false.also { dropShed() }
                Read.DONE, Read.ABANDONED -> known()?.let { return it }
            }
            // This subscription's rule reads a cell the read ahead did not warm: read it alongside.
        }
        val leads = ahead == null || reads.putIfAbsent(author, mine) == null
        var outcome = Read.ABANDONED
        try {
            if (!takeSlot()) {
                outcome = Read.SHED
                dropShed()
                return false
            }
            try {
                return admits().also { outcome = Read.DONE }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                outcome = Read.FAILED
                failed.incrementAndGet()
                say(e)
                return false
            } finally {
                slots.release()
            }
        } finally {
            if (leads) {
                reads.remove(author, mine)
                mine.complete(outcome)
            }
        }
    }

    /** A read slot within [queueMillis]; the flag keeps a slot won just as the timeout fires. */
    private suspend fun takeSlot(): Boolean {
        if (slots.tryAcquire()) return true
        var held = false
        withTimeoutOrNull(queueMillis) {
            slots.acquire()
            held = true
        }
        return held
    }

    private fun dropShed() {
        shed.incrementAndGet()
        say(null)
    }

    private fun say(cause: Exception?) {
        val t = now()
        val last = lastSaidAt.get()
        if (last != Long.MIN_VALUE && t - last < everyMillis) return
        if (!lastSaidAt.compareAndSet(last, t)) return
        val why = cause?.let { " (last: ${it::class.simpleName}: ${it.message?.take(80)})" } ?: ""
        log(
            "live gate: ${failed.get()} cold-author check(s) failed$why, ${shed.get()} shed waiting on $max reads in flight" +
                " — those live events were not delivered",
        )
    }

    companion object {
        /** Store reads in flight for cold authors across every subscription. */
        const val MAX_IN_FLIGHT = 512

        /** Checks running or parked at once; one waiting on another's read holds no read slot. */
        const val MAX_WAITING = 16_384

        /** How long a check waits for a read slot before its event is dropped. */
        const val QUEUE_MS = 5_000L

        private const val SAY_EVERY_MS = 60_000L
    }
}

/** The live gate of the store behind [store], when it is this repo's store; null for any other. */
private fun liveGatesOf(store: IEventStore): (suspend (List<Filter>) -> LiveGate?)? =
    when (store) {
        is VespaEventStore -> store.store::liveGate
        is NostrSemanticsStore -> store::liveGate
        else -> null
    }
