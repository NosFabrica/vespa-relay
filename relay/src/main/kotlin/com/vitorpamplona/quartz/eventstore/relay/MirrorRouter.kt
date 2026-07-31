/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcile
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * The router: a strfry-style mirror. For each configured upstream it moves
 * events between that relay and the store the relay serves, so what the
 * network holds shows up in our search and (optionally) what we hold shows up
 * on the upstream.
 *
 * Down (`dir = down`/`both`): a live REQ subscription per upstream streams new
 * matching events into the store; with a backfill window it first
 * negentropy-reconciles history, then the tail keeps it current. Structure
 * follows geode's MirrorWorker — [SubscriptionListener.onEvent] can't suspend,
 * so events land on a bounded [inbound] channel (a blocking send backpressures
 * the download when ingest falls behind) and a pool of ingest workers drains
 * it in batches through [IEventStore.batchInsert] — the store's bulk feed
 * (the store serializes writes on one mutex, so throughput comes from the batch
 * size, not the worker count). Every event is re-checked against its filter;
 * untrusted upstreams have every signature verified off the download threads.
 *
 * Up (`dir = up`/`both`): periodically negentropy-reconciles the store against
 * the upstream and publishes the events the upstream is missing. Reconciliation
 * gives echo-suppression for free — an event we just pulled down from a relay
 * is one that relay already has, so it is never pushed back.
 *
 * Dynamic (`relaySource = [ ... ]`): the stream has no configured relays. Every
 * refresh it reads each configured source out of our store ([RelayDiscovery]) —
 * relay lists, provider lists, relay hints on ordinary tags — and negentropy-syncs
 * the stream filter against every relay they name, all of them, with `concurrency`
 * pacing the fan-out rather than capping it. A set that size is synced on a period
 * rather than held open, so these streams have no live tail — the refresh *is* the
 * tail — and each relay's socket is dropped again once its sync returns.
 *
 * While backfilling, a progress line reports overall percent and an ETA to
 * "useful" (backfill complete), so an operator can tell how long the initial
 * fill will take. [close] stops touching the store before the store closes.
 */
class MirrorRouter(
    private val store: IEventStore,
    private val config: RouterConfig,
    parentContext: CoroutineContext = SupervisorJob(),
    // When set (PARSE_AUDIT_FILE), every mirrored event is also run through
    // quartz's search-indexing parse to collect what quartz cannot read. Off by
    // default: it costs one extra parse per event. See [ParseAudit].
    private val audit: ParseAudit? = null,
    // How much of each filter's history we have already pulled from each relay,
    // so a paged relay is not re-read from scratch every restart. See [SyncCursors].
    private val cursors: SyncCursors = SyncCursors(null),
    // Answers NIP-42 challenges from upstreams that gate reads behind AUTH.
    // Null (the default) leaves challenges unanswered. See [RouterIdentity].
    private val signer: NostrSigner? = null,
) : AutoCloseable {
    private data class Inbound(
        val event: Event,
        val skipVerify: Boolean,
    )

    private val scope = CoroutineScope(Dispatchers.IO + parentContext)

    // One OkHttp client for every upstream. A 120s ping surfaces half-open
    // connections (peer vanished without a FIN) as a failed pong, which routes
    // into quartz's reconnect path instead of a silently dead subscription.
    private val okhttp =
        OkHttpClient
            .Builder()
            // The single most important number in the fan-out, and the least
            // obvious. OkHttp opens a websocket by ENQUEUING a call whose callback
            // runs loopReader() inline, and the Dispatcher counts a call as
            // running until its callback returns — so an open websocket holds one
            // of these slots for its entire life and never gives it back.
            //
            // At the stock 64 that is the real concurrency ceiling for the whole
            // router, whatever a stream's `concurrency` says: the permits are
            // handed out, the sockets queue in readyAsyncCalls, and the semaphore
            // stops throttling anything. Measured on a 20,340-relay outbox list:
            // 52 relays done and an ETA of 330 hours. Every static upstream also
            // holds one permanently, so they come straight off this budget.
            //
            // Must exceed (static upstreams + the sum of every stream's
            // `concurrency`) or the config silently does not mean what it says.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_SOCKETS
                    // Per HOST, and every relay is a different host — this only
                    // ever bites when one host serves several of a list's urls.
                    maxRequestsPerHost = MAX_CONCURRENT_SOCKETS_PER_HOST
                },
            ).pingInterval(Duration.ofSeconds(120))
            .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSec))
            .build()

    private val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

    // NIP-42. Relays that gate reads behind AUTH serve nothing until we answer
    // their challenge — and an unanswered challenge looks exactly like an
    // ordinary empty relay from here, so this is invisible when it is missing.
    // Attaching the authenticator is enough: it listens for AUTH on every
    // upstream, signs the 22242 back, and re-authenticates when a relay CLOSEs a
    // subscription with `auth-required`. Null when no key is configured, in
    // which case challenges are ignored exactly as before.
    private val authenticator =
        signer?.let { s ->
            RelayAuthenticator(client, scope) { _, template, _ -> listOf(s.sign(template)) }
        }

    // Bounded, so a fast download (negentropy/paged can deliver >10k/s) can't
    // outrun Vespa ingest and pile millions of events onto the heap. When it
    // fills, the producing thread blocks in [offer] and the upstream download
    // throttles to the ingest rate — flat memory instead of an OOM.
    // Workers and batch come from config; the buffer is sized to a few batches
    // so producers block (backpressure) rather than pile events onto the heap.
    private val ingestWorkers = config.ingestConcurrency
    private val ingestBatch = config.ingestBatch
    private val inbound = Channel<Inbound>((ingestBatch * 4).coerceAtLeast(4096))
    private val accepted = AtomicLong()
    private val rejected = AtomicLong()
    private val pushed = AtomicLong()

    // Why events were rejected. Worth separating, because on a wide fan-out the
    // two are wildly different news: a bad signature means an upstream is
    // serving junk, while "the store already has this" is the expected result of
    // asking a thousand relays for the same replaceable profile — and the second
    // routinely outnumbers accepts. One `rejected` number for both reads like an
    // emergency when it is the system working.
    private val unverified = AtomicLong()
    private val rejectReasons = java.util.concurrent.ConcurrentHashMap<String, Long>()

    // Store failures already reported in full, so the raw-event dump stays one
    // per distinct defect however many events trip it. Ingest workers share it.
    private val poisonSeen =
        java.util.concurrent.ConcurrentHashMap
            .newKeySet<String>()

    private val downUpstreams = config.downUpstreams()
    private val upUpstreams = config.upUpstreams()
    private val dynamicStreams = config.dynamicStreams()
    private val progress = BackfillProgress()

    // The relays we hold a live subscription on. A dynamic sync drops its socket
    // when it finishes, and must not drop one of these out from under its tail.
    private val pinnedUrls = (downUpstreams + upUpstreams).map { it.url }.toSet()

    // How many dynamic syncs are currently using each relay. Streams discover
    // from the same store, so two of them (an outbox and a NIP-85 one, say)
    // routinely land on the same relay at the same time — and whichever finished
    // first used to close the socket out from under the other, failing a sync
    // that was working. Only the last one out disconnects.
    private val inFlight = java.util.concurrent.ConcurrentHashMap<NormalizedRelayUrl, Int>()

    fun start(): MirrorRouter {
        if (downUpstreams.isEmpty() && upUpstreams.isEmpty() && dynamicStreams.isEmpty()) {
            System.err.println("router: no upstreams configured; nothing to mirror")
            return this
        }

        // A pool of consumers drains the channel in batches and writes each batch
        // through the store's bulk path (batchInsert -> parallel Vespa feed), which
        // is what actually exploits the store's ingest parallelism. Feeding it one
        // event at a time — the old path — left that parallelism unused.
        repeat(ingestWorkers) { scope.launch { ingestLoop() } }

        // Down live tail: subscribe on each upstream from now forward. History,
        // when asked for, is the backfill's job — so the tail never floods on connect.
        val liveSince = nowSeconds()
        downUpstreams.forEachIndexed { i, up ->
            client.subscribe(
                subId = "vespa-mirror-down-$i",
                filters = mapOf(up.url to listOf(up.filter.copy(since = liveSince))),
                listener = downListener(up),
            )
        }
        client.connect()

        // Every down upstream backfills: the stream's filter says how far, and a
        // filter naming no `since` is unbounded, exactly as NIP-01 reads it.
        val backfillers = downUpstreams
        if (backfillers.isNotEmpty()) {
            progress.begin(backfillers.size)
            scope.launch { backfill(backfillers) }
            scope.launch { progressLoop() }
        }

        // Up: one reconcile loop per up-upstream.
        upUpstreams.forEach { up -> scope.launch { upLoop(up) } }

        // Dynamic: one refresh loop per stream, each discovering its own relays.
        dynamicStreams.forEach { stream -> scope.launch { dynamicLoop(stream) } }

        scope.launch { statsLoop() }

        System.err.println(
            "router: ${downUpstreams.size} down + ${upUpstreams.size} up relay(s)" +
                (if (backfillers.isNotEmpty()) "; backfilling ${backfillers.size}" else "; live-tail only") +
                (if (upUpstreams.isNotEmpty()) "; up every ${config.upIntervalSec}s" else "") +
                (
                    if (dynamicStreams.isNotEmpty()) {
                        "; ${dynamicStreams.size} dynamic stream(s): " +
                            dynamicStreams.joinToString { "${it.name} (${it.dynamic?.sources?.size} source(s))" }
                    } else {
                        ""
                    }
                ),
        )
        return this
    }

    // ---- ingest ------------------------------------------------------------

    /**
     * Hand an event to the ingest pool, blocking the caller if the buffer is
     * full. The negentropy/subscription callbacks that call this are not
     * suspending, so a blocking send is how backpressure reaches the download:
     * when ingest can't keep up, the producing thread parks here and the
     * upstream stops being drained until there's room.
     */
    private fun offer(
        event: Event,
        skipVerify: Boolean,
    ) {
        inbound.trySendBlocking(Inbound(event, skipVerify))
    }

    /**
     * Drain the channel in batches and write each batch through [IEventStore.batchInsert]
     * (the store's bulk path: replaceable-dedup preload + a parallel Vespa feed).
     * Signatures are verified here, off the download threads, and skipped for
     * trusted upstreams. [ingestWorkers] of these run at once.
     */
    private suspend fun ingestLoop() {
        val batch = ArrayList<Inbound>(ingestBatch)
        while (scope.isActive) {
            val first = inbound.receiveCatching().getOrNull() ?: break
            batch.clear()
            batch.add(first)
            while (batch.size < ingestBatch) {
                val next = inbound.tryReceive().getOrNull() ?: break
                batch.add(next)
            }
            val valid = ArrayList<Event>(batch.size)
            var verifyRejected = 0
            for (msg in batch) {
                if (msg.skipVerify || runCatching { msg.event.verify() }.getOrDefault(false)) {
                    valid.add(msg.event)
                } else {
                    verifyRejected++
                }
            }
            if (verifyRejected > 0) {
                rejected.addAndGet(verifyRejected.toLong())
                unverified.addAndGet(verifyRejected.toLong())
            }
            if (valid.isEmpty()) continue
            // Before the batch write: the store feeds Vespa in parallel, so a parse
            // report raised inside batchInsert cannot be attributed to one event.
            // Inspecting here keeps each parse on this worker thread, where the
            // audit's ThreadLocal makes the attribution exact.
            audit?.let { for (event in valid) it.inspect(event) }
            insertIsolating(valid)
        }
    }

    /**
     * Write a batch, and if it throws, bisect it and write the halves.
     *
     * [IEventStore.batchInsert] fails as a unit: one event the store cannot handle
     * takes the whole batch with it. At the default batch size that is 999 good
     * events lost per bad one, permanently and with no retry — the loss is silent
     * except for a count, and the count is a multiple of the batch size rather
     * than a number of malformed events, which is itself misleading.
     *
     * Halving turns that into ~2·log2(n) extra writes on the rare failing batch
     * and isolates the offender to a single event. Re-writing the good halves is
     * safe: a batch that threw may have applied some of its events already, and
     * re-inserting those is just a duplicate the store rejects.
     *
     * The isolated event is then reported in full — that is the diagnostic this
     * exists for. A store-level throw has no other trace: it never reaches the
     * parse audit (which only covers the search-indexing path) and the exception
     * is caught here, so without this the raw event is unrecoverable.
     */
    private suspend fun insertIsolating(events: List<Event>) =
        insertBisecting(
            events = events,
            write = { store.batchInsert(it) },
            onOutcomes = { outcomes ->
                for (outcome in outcomes) {
                    when (outcome) {
                        is IEventStore.InsertOutcome.Accepted -> {
                            accepted.incrementAndGet()
                        }

                        is IEventStore.InsertOutcome.Rejected -> {
                            rejected.incrementAndGet()
                            rejectReasons.merge(outcome.reason.take(48), 1L, Long::plus)
                        }
                    }
                }
            },
            onPoison = { event, e ->
                // Name the exception type too — a batch can fail with a
                // message-less throwable, and "store: null" tells nobody
                // anything about the event it just cost us.
                rejected.incrementAndGet()
                rejectReasons.merge("store ${e.javaClass.simpleName}: ${e.message?.take(40)}", 1L, Long::plus)
                reportPoison(event, e)
            },
            onGaveUp = { batch, e ->
                // Isolation ran out of budget, so these are counted but unnamed.
                // Tallied apart from the isolated ones on purpose: "we could not
                // say which" is a different fact from "this event is bad", and
                // reading them as one number hides a store-wide outage.
                rejected.addAndGet(batch.size.toLong())
                rejectReasons.merge("store ${e.javaClass.simpleName} (batch, unisolated)", batch.size.toLong(), Long::plus)
            },
        )

    /**
     * Log an event the store threw on, once per distinct failure, with the raw
     * JSON. One line per occurrence would be a flood at mirror rates and the
     * hundredth copy of a defect teaches nothing the first did not, so the
     * signature is what is deduplicated — and [POISON_SAMPLE_LIMIT] caps even
     * that, because a genuinely novel corpus could otherwise print all day.
     */
    private fun reportPoison(
        event: Event,
        error: Throwable,
    ) {
        val signature = "${error.javaClass.name}: ${error.message}"
        if (!poisonSeen.add(signature) || poisonSeen.size > POISON_SAMPLE_LIMIT) return
        System.err.println(
            "router: store rejected event ${event.id} (kind ${event.kind}, pubkey ${event.pubKey}) — " +
                "${error.javaClass.simpleName}: ${error.message}\n" +
                "router: the event, verbatim: ${event.toJson().take(POISON_JSON_CHARS)}",
        )
    }

    // ---- down --------------------------------------------------------------

    private fun downListener(up: MirrorUpstream): SubscriptionListener =
        object : SubscriptionListener {
            override fun onEvent(
                event: Event,
                isLive: Boolean,
                relay: NormalizedRelayUrl,
                forFilters: List<Filter>?,
            ) {
                // Bind trust to the relay we dialed, not the subscription id, and
                // re-check scope so a broken upstream can't widen what we ingest.
                if (relay != up.url) return
                if (!up.filter.match(event)) return
                offer(event, up.trusted)
            }

            override fun onCannotConnect(
                relay: NormalizedRelayUrl,
                message: String,
                forFilters: List<Filter>?,
            ) {
                System.err.println("router: cannot connect ${up.url.url}: $message")
            }
        }

    /**
     * One-shot historical catch-up per upstream: negentropy-reconcile the
     * stream's filter against what we already hold and
     * download only the diff. quartz falls back to paged REQ automatically for
     * upstreams without NIP-77. Downloaded events funnel through the same
     * [inbound] channel, so ingest, verification, and dedup match the live path.
     * Progress is reported through [progress]. Failures are logged, never fatal.
     */
    private suspend fun backfill(ups: List<MirrorUpstream>) {
        // Concurrently: a fast upstream (ditto reconciles in seconds) shouldn't
        // wait on a slow or stuck one. Nothing bounds a backfill by wall clock —
        // the only bound is quartz's idle timeout, seconds since the last message
        // — so a relay that keeps talking without converging holds its slot for
        // as long as it keeps talking. Deliberate: a wall-clock cap truncates the
        // download of a big honest relay, which is the worse failure.
        coroutineScope {
            ups.forEachIndexed { idx, up -> launch { backfillOne(idx, up) } }
        }
    }

    private suspend fun backfillOne(
        idx: Int,
        up: MirrorUpstream,
    ) {
        // The filter as the operator wrote it. `since`/`until` are NIP-01's own,
        // so absent means unbounded and this reaches the upstream's whole history
        // — minus whatever a previous run already walked, when this relay paged.
        val legs = cursors.legs(up.url, up.filter)
        if (legs.isEmpty()) {
            // Only reachable if a future change makes the legs exclusive again;
            // today the boundary seconds always leave something to ask for.
            progress.done(idx, 0)
            System.err.println("router: static backfill ${up.url.url} already covers its filter — nothing outside the synced band")
            return
        }
        try {
            var downloaded = 0
            var paged = false
            for (window in legs) {
                // Track the span this leg actually saw. The client reports how
                // many events came back, not when they were from, and the band
                // is the whole point of the exercise.
                var seenMin: Long? = null
                var seenMax: Long? = null
                // No deadline. Every timeout in the client is measured from the last
                // message, so a relay that stops answering is dropped in seconds by
                // [NEG_IDLE_MS] — and one still sending is doing the work we asked
                // for, however long its history takes. A wall clock could only fire
                // on the healthy case, which is how a 4h cap came to truncate four
                // working upstreams at exactly 14400s.
                // Stamped BEFORE the sync, not after: everything the reconcile
                // compared against is our state as of now, so this is the instant
                // we are provably in sync through. Taking it afterwards would
                // claim coverage of events that arrived while it ran.
                val syncStartedAt = System.currentTimeMillis() / 1000
                val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(window))
                val result =
                    client.negentropySyncOrFetch(
                        relay = up.url,
                        filter = window,
                        idleTimeoutMs = NEG_IDLE_MS,
                        localEntries = local,
                        onProgress = { needSoFar, done -> progress.update(idx, needSoFar, downloaded + done) },
                        onEvent = { event ->
                            if (up.filter.match(event)) {
                                // Only PLAUSIBLE stamps widen the band. Filtering
                                // here rather than on the aggregate matters: one
                                // future-dated event among 700k used to discard
                                // the whole upstream's band, so purplepag.es
                                // recorded nothing at all after a run that
                                // downloaded 700,767 events.
                                if (SyncCursors.isPlausible(event.createdAt)) {
                                    seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                    seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                }
                                offer(event, up.trusted)
                            }
                        },
                    )
                downloaded += result.downloaded
                paged = paged || result.pagedFallback
                // Per leg, not once at the end: a crash between legs then keeps
                // the ground the first one gained.
                cursors.record(
                    up.url,
                    up.filter,
                    seenMin,
                    seenMax,
                    result.pagedFallback,
                    // A reconcile that did not fall back covered the whole leg.
                    reconciledThrough = syncStartedAt.takeUnless { result.pagedFallback },
                )
            }
            run {
                progress.done(idx, downloaded)
                val band = cursors.band(up.url, up.filter)
                System.err.println(
                    "router: static backfill ${up.url.url} downloaded $downloaded" +
                        (if (paged) " (paged REQ fallback — no NIP-77)" else " (negentropy)") +
                        (if (legs.size > 1) " [resumed: ${legs.size} leg(s) outside the synced band]" else "") +
                        (if (band != null) " [synced ${band.minCreatedAt}..${band.maxCreatedAt}]" else ""),
                )
            }
        } catch (e: Exception) {
            progress.done(idx, 0)
            System.err.println("router: static backfill ${up.url.url} failed: ${e.message}")
        }
    }

    // ---- dynamic (relaySource) ---------------------------------------------

    /**
     * One dynamic stream, forever: read the relay lists our store already holds,
     * sync the stream's filter against every relay they name, sleep, repeat.
     *
     * The discovery is deliberately inside the loop — the store keeps filling
     * with relay lists while we run, so each cycle syncs against a wider (and
     * better-ranked) set than the last. A cycle that finds nothing is normal on
     * a cold store: it means no 10002/10040 has been mirrored in yet, and the
     * next refresh will find some.
     */
    private suspend fun dynamicLoop(stream: MirrorStream) {
        val dynamic = stream.dynamic ?: return
        val sourceNames =
            dynamic.sources.joinToString { s ->
                "kinds ${s.filter.kinds?.joinToString("/") ?: "?"} x${s.selects.size} select(s)"
            }
        // How long to wait before trying again when a cycle did NOT run. The full
        // refresh interval is the wrong answer there: a store that is still
        // filling has no relays to find yet, and a store that failed one query
        // (a degraded Vespa response aborts discovery by design, rather than
        // syncing against a half-read relay list) is usually fine moments later.
        // Waiting hours to notice either would be self-inflicted downtime, so
        // back off from short and climb, capped at the refresh interval.
        var retrySec = DYNAMIC_RETRY_BASE_SECONDS
        while (scope.isActive) {
            var ran = false
            try {
                // Never fan out onto ourselves: our own url is in plenty of lists.
                val relays = RelayDiscovery.discover(store, dynamic, skip = setOfNotNull(store.relay))
                if (relays.isEmpty()) {
                    System.err.println(
                        "router: ${stream.name} found no relays in [$sourceNames] yet — retrying in ${retrySec}s",
                    )
                } else {
                    dynamicCycle(stream, dynamic, sourceNames, relays)
                    ran = true
                }
            } catch (e: CancellationException) {
                // Shutdown, not a failure — a cycle can be mid-fan-out for a long
                // time, so close() almost always lands inside one. Let it end the
                // loop quietly instead of logging a scary line on every stop.
                throw e
            } catch (e: Exception) {
                System.err.println("router: ${stream.name} refresh failed: ${e.message} — retrying in ${retrySec}s")
            }

            if (ran) {
                retrySec = DYNAMIC_RETRY_BASE_SECONDS
                delay(dynamic.refreshSeconds * 1000)
            } else {
                delay(retrySec * 1000)
                retrySec = (retrySec * 2).coerceAtMost(dynamic.refreshSeconds)
            }
        }
    }

    /** Sync every discovered relay, [DynamicRelayList.concurrency] of them at a time. */
    private suspend fun dynamicCycle(
        stream: MirrorStream,
        dynamic: DynamicRelayList,
        sourceNames: String,
        relays: List<DiscoveredRelay>,
    ) {
        val startedMs = System.currentTimeMillis()
        val gate = Semaphore(dynamic.concurrency)
        val downloaded = AtomicLong()
        val failed = AtomicLong()

        // ONE window and ONE local snapshot for the whole cycle, not one per
        // relay. Every relay in this stream reconciles the same filter, so a
        // per-relay snapshot re-scanned the identical range once per relay —
        // hundreds of full store scans per cycle, all returning the same thing.
        //
        // The cost of sharing it is that a relay synced late in the cycle
        // reconciles against the store as it was at the start, so an event two
        // relays both have can be downloaded twice. That was already true:
        // ingest is asynchronous (events queue through [inbound] and land in
        // batches), so a snapshot taken mid-cycle wouldn't have seen the earlier
        // relay's events either. The store dedups on insert; the scans were pure
        // waste.
        val window = stream.filter
        // ONE snapshot for the fan-out, but NOT over the whole filter. Taken over
        // the full stream filter this walked every kind-0 and kind-30382 we hold
        // — 24.8M ids on a real store, ~15 minutes of visit requests and gigabytes
        // of IdAndTime, all of it BEFORE the first log line, so a stream that was
        // busy was indistinguishable from one that had hung. Once every relay in
        // the list carries a complete band, this is the sliver since the oldest
        // of their ceilings instead. See [SyncCursors.coveringWindow].
        val snapshotWindow = cursors.coveringWindow(relays.map { it.url }, window)
        val snapStartedMs = System.currentTimeMillis()
        val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(snapshotWindow))
        System.err.println(
            "router: ${stream.name} local snapshot ${local.size} id(s) in ${fmtDuration(System.currentTimeMillis() - snapStartedMs)}" +
                (snapshotWindow.since?.let { ", since $it" } ?: ", full filter (no relay is caught up yet)"),
        )
        // Why the unreachable ones were unreachable, tallied — a relay list is
        // full of dead hosts, and the shape of the failures is what tells an
        // operator whether that is normal or whether the whole cycle is broken.
        val reasons = java.util.concurrent.ConcurrentHashMap<String, Long>()
        System.err.println(
            "router: ${stream.name} syncing ${relays.size} relay(s) from [$sourceNames]" +
                " against ${local.size} local id(s)" +
                " (top: ${relays.take(3).joinToString { "${it.url.url} x${it.references}" }})",
        )
        val done = AtomicLong()
        coroutineScope {
            // A cycle over a relay list runs for HOURS with nothing but its
            // opening line, so a stalled fan-out and a working one look the same
            // from outside. The static backfill has a progress line for exactly
            // this reason; the bigger job had none.
            val ticker =
                launch {
                    while (true) {
                        delay(PROGRESS_INTERVAL_MS)
                        val finished = done.get()
                        val elapsed = System.currentTimeMillis() - startedMs
                        val rate = if (elapsed > 0) finished * 1000.0 / elapsed else 0.0
                        val etaSec = if (rate > 0) ((relays.size - finished) / rate).toLong() else -1
                        System.err.println(
                            "router: ${stream.name} $finished/${relays.size} relay(s), ${downloaded.get()} event(s)" +
                                (if (failed.get() > 0) ", ${failed.get()} unreachable" else "") +
                                (if (etaSec >= 0) ", ETA ~${fmtDuration(etaSec * 1000)}" else ""),
                        )
                    }
                }
            try {
                // The inner scope is what makes the ticker work. `forEach { launch }`
                // returns the moment the jobs are *issued*, so cancelling on the way
                // out of it killed the ticker microseconds after it started — before
                // its first delay ever elapsed, which is why a cycle over thousands
                // of relays printed its opening line and then nothing for hours.
                // Awaiting the jobs here keeps the ticker alive for the whole fan-out.
                coroutineScope {
                    relays.forEach { relay ->
                        launch {
                            gate.withPermit {
                                val got =
                                    dynamicSyncOne(stream, relay.url, window, local) { reason ->
                                        reasons.merge(reason, 1L, Long::plus)
                                    }
                                if (got < 0) failed.incrementAndGet() else downloaded.addAndGet(got.toLong())
                                done.incrementAndGet()
                            }
                        }
                    }
                }
            } finally {
                ticker.cancel()
            }
        }
        val topReasons =
            reasons.entries
                .sortedByDescending { it.value }
                .take(3)
                .joinToString { "${it.key} x${it.value}" }
        // One write for the whole fan-out, not one per relay.
        cursors.flush()
        System.err.println(
            "router: ${stream.name} cycle done — ${downloaded.get()} event(s) from ${relays.size - failed.get()}/${relays.size} relay(s)" +
                " in ${fmtDuration(System.currentTimeMillis() - startedMs)}" +
                (if (topReasons.isNotEmpty()) "; unreachable: $topReasons" else "") +
                "; next in ${dynamic.refreshSeconds}s",
        )
    }

    /**
     * Reconcile one discovered relay against our store and pull what it has that
     * we don't — negentropy when the relay speaks NIP-77, paged REQ when it
     * doesn't. Returns the download count, or -1 when the relay never delivered.
     *
     * [window] and [local] are the cycle's, shared by every relay in it — see
     * [dynamicCycle] for why they are not recomputed here.
     */
    private suspend fun dynamicSyncOne(
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        onFailure: (String) -> Unit,
    ): Int {
        inFlight.merge(url, 1, Int::plus)
        return try {
            // No wall clock here either: these relays are strangers off a list,
            // but a slot held by one that is delivering is a slot doing its job.
            // Silence is what costs us, and [NEG_IDLE_MS] already answers that.
            var downloaded = 0
            for (leg in cursors.legs(url, window)) {
                var seenMin: Long? = null
                var seenMax: Long? = null
                val syncStartedAt = System.currentTimeMillis() / 1000
                val result =
                    client.negentropySyncOrFetch(
                        relay = url,
                        filter = leg,
                        idleTimeoutMs = NEG_IDLE_MS,
                        localEntries = local,
                        onEvent = { event ->
                            if (stream.filter.match(event)) {
                                if (SyncCursors.isPlausible(event.createdAt)) {
                                    seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                    seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                }
                                offer(event, stream.trusted)
                            }
                        },
                    )
                downloaded += result.downloaded
                cursors.record(
                    url,
                    window,
                    seenMin,
                    seenMax,
                    result.pagedFallback,
                    reconciledThrough = syncStartedAt.takeUnless { result.pagedFallback },
                )
            }
            downloaded
        } catch (e: Exception) {
            // A dead host in a relay list is the common case, not an incident:
            // tally it and move on — one line per cycle carries the totals.
            onFailure(e.message?.take(60) ?: e.javaClass.simpleName)
            -1
        } finally {
            releaseSocket(url)
        }
    }

    /**
     * Drop a dynamic relay's socket once nothing is using it. Hundreds of relays
     * a cycle would otherwise leave hundreds of idle connections open until the
     * next one; relays carrying a live subscription ([pinnedUrls]) and relays
     * another stream is still syncing are left alone.
     */
    private fun releaseSocket(url: NormalizedRelayUrl) {
        val stillInUse = inFlight.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } } != null
        if (!stillInUse && url !in pinnedUrls) {
            runCatching { client.getOrCreateRelay(url).disconnect() }
        }
    }

    // ---- up ----------------------------------------------------------------

    /**
     * Push our matching events to an upstream that lacks them, then repeat every
     * [RouterConfig.upIntervalSec] to carry newly-arrived local events. Each pass
     * reconciles a few rounds until the upstream reports nothing more is missing.
     * `negentropyReconcile.onHaveIds` yields the ids the upstream needs; we load
     * those events from the store and publish them (paced, fire-and-forget).
     */
    private suspend fun upLoop(up: MirrorUpstream) {
        while (scope.isActive) {
            try {
                var rounds = 0
                var pushedThisPass: Long
                var pushedThisWindow = 0L
                do {
                    pushedThisPass = 0
                    run {
                        val local: List<IdAndTime> = store.snapshotIdsForNegentropy(listOf(up.filter))
                        client.negentropyReconcile(
                            relay = up.url,
                            filter = up.filter,
                            localEntries = local,
                            idleTimeoutMs = NEG_IDLE_MS,
                            onHaveIds = { ids ->
                                val events: List<Event> = store.query(Filter(ids = ids))
                                for (event in events) {
                                    client.publish(event, setOf(up.url))
                                    pushed.incrementAndGet()
                                    pushedThisPass++
                                    delay(UP_PUBLISH_PACE_MS)
                                }
                            },
                            onNeedIds = { /* up-only: we don't pull here, the down tail does */ },
                        )
                    }
                    pushedThisWindow += pushedThisPass
                    rounds++
                } while (pushedThisPass > 0 && rounds < UP_MAX_ROUNDS && scope.isActive)
                System.err.println(
                    "router: up ${up.url.url} pushed $pushedThisWindow event(s) upstream ($rounds round(s))",
                )
            } catch (e: Exception) {
                System.err.println("router: up ${up.url.url} failed: ${e.message}")
            }
            delay(config.upIntervalSec * 1000)
        }
    }

    // ---- reporting ---------------------------------------------------------

    private suspend fun progressLoop() {
        while (scope.isActive) {
            delay(PROGRESS_INTERVAL_MS)
            val s = progress.snapshot()
            if (s.allDone) {
                cursors.flush()
                // "backfill complete" used to read as "the relay is caught up",
                // while the dynamic streams — the larger half of the fill, by an
                // order of magnitude on a real relay list — were still going.
                val stillSyncing = dynamicStreams.size
                System.err.println(
                    "router: static backfill complete — ${s.downloaded} events from ${s.total} relay(s)" +
                        " in ${fmtDuration(s.elapsedMs)}; live tail now streaming" +
                        if (stillSyncing > 0) "; $stillSyncing dynamic stream(s) still syncing" else "",
                )
                return
            }
            // ETA from the average download rate since start — steadier than an
            // instantaneous window, which flickers to zero between negentropy pages.
            val elapsedSec = s.elapsedMs / 1000.0
            val avgRate = if (elapsedSec > 0) s.downloaded / elapsedSec else 0.0
            val remaining = (s.need - s.downloaded).coerceAtLeast(0)
            val etaSec = if (avgRate > 1) (remaining / avgRate).toLong() else -1
            System.err.println(
                // Named, because "backfill 5/12" says nothing about WHICH 12 —
                // and the dynamic streams, which do the larger share of the work,
                // are not in this count at all. "done" because all 12 run at
                // once: this counts the finished ones, not a position in a queue.
                "router: static backfill ${s.done}/${s.total} relay(s) done, ${s.downloaded}/${s.need} events (${s.percent()}%)" +
                    ", ${"%.0f".format(avgRate)}/s avg" +
                    (if (etaSec >= 0) ", ETA ~${fmtDuration(etaSec * 1000)} to useful" else ", ETA —"),
            )
        }
    }

    private suspend fun statsLoop() {
        while (scope.isActive) {
            delay(60_000)
            System.err.println(
                "router: ingested ${accepted.get()} accepted, ${rejected.get()} rejected${rejectionBreakdown()}" +
                    (if (upUpstreams.isNotEmpty()) ", pushed ${pushed.get()} up" else "") +
                    // A dynamic cycle connects relays that are in no upstream list,
                    // so the connected count is reported against the pinned ones
                    // rather than as a fraction of them.
                    "; ${client.connectedRelaysFlow().value.size} relay(s) connected, ${pinnedUrls.size} pinned" +
                    (if (dynamicStreams.isNotEmpty()) " + dynamic" else ""),
            )
        }
    }

    /** Accepted/rejected/pushed counters, for tests and a final log line. */
    fun stats(): Triple<Long, Long, Long> = Triple(accepted.get(), rejected.get(), pushed.get())

    /**
     * What the rejections actually were. A wide fan-out asks a thousand relays
     * for the same replaceable events, so "already have it" dwarfing the accept
     * count is the system working — while a bad signature or a failing store is
     * not, and the bare total hides which one you are looking at.
     */
    private fun rejectionBreakdown(): String {
        if (rejected.get() == 0L) return ""
        val why =
            rejectReasons.entries
                .sortedByDescending { it.value }
                .take(2)
                .joinToString { "${it.key} x${it.value}" }
        val bad = if (unverified.get() > 0) "bad signature x${unverified.get()}" else ""
        val parts = listOf(bad, why).filter { it.isNotEmpty() }
        return if (parts.isEmpty()) "" else " [${parts.joinToString("; ")}]"
    }

    /** Number of distinct configured upstreams (down + up) being mirrored. */
    fun upstreamCount(): Int = pinnedUrls.size

    /** Number of streams whose relays are discovered from the store, not configured. */
    fun dynamicStreamCount(): Int = dynamicStreams.size

    override fun close() {
        // First: a backfill killed mid-flight still keeps the ground it gained.
        runCatching { cursors.flush() }
        runCatching { authenticator?.destroy() }
        downUpstreams.indices.forEach { runCatching { client.unsubscribe("vespa-mirror-down-$it") } }
        runCatching { client.close() }
        inbound.close()
        scope.cancel()
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        System.err.println(
            "router: stopped (${accepted.get()} accepted, ${rejected.get()} rejected${rejectionBreakdown()}, ${pushed.get()} pushed)",
        )
    }

    private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

    /**
     * Aggregate backfill progress across upstreams. Each upstream reports
     * `(needSoFar, downloaded)` as its negentropy reconciliation proceeds;
     * summing them gives an overall picture and a rate-based ETA. `needSoFar`
     * grows as reconciliation discovers ids, so early percentages are estimates
     * that firm up as the run proceeds.
     */
    private class BackfillProgress {
        private val need = java.util.concurrent.ConcurrentHashMap<Int, Long>()
        private val got = java.util.concurrent.ConcurrentHashMap<Int, Long>()
        private val finished = java.util.concurrent.ConcurrentHashMap<Int, Long>()

        @Volatile private var totalUpstreams = 0

        @Volatile private var startMs = 0L

        fun begin(n: Int) {
            totalUpstreams = n
            startMs = System.currentTimeMillis()
        }

        fun update(
            idx: Int,
            needSoFar: Int,
            downloaded: Int,
        ) {
            need[idx] = needSoFar.toLong()
            got[idx] = downloaded.toLong()
        }

        fun done(
            idx: Int,
            downloaded: Int,
        ) {
            finished[idx] = downloaded.toLong()
            got[idx] = downloaded.toLong()
            // A finished upstream's need is exactly what it downloaded.
            need[idx] = maxOf(need[idx] ?: 0L, downloaded.toLong())
        }

        fun snapshot(): Snapshot {
            val need = need.values.sum()
            val got = got.values.sum()
            return Snapshot(
                need = need,
                downloaded = got,
                done = finished.size,
                total = totalUpstreams,
                allDone = finished.size >= totalUpstreams && totalUpstreams > 0,
                elapsedMs = System.currentTimeMillis() - startMs,
            )
        }
    }

    private data class Snapshot(
        val need: Long,
        val downloaded: Long,
        val done: Int,
        val total: Int,
        val allDone: Boolean,
        val elapsedMs: Long,
    ) {
        fun percent(): Int = if (need <= 0) 0 else ((downloaded * 100) / need).coerceIn(0, 100).toInt()
    }

    companion object {
        private const val PROGRESS_INTERVAL_MS = 15_000L
        private const val UP_PUBLISH_PACE_MS = 40L
        private const val UP_MAX_ROUNDS = 8

        // First wait after a dynamic cycle could not run; doubles up to the
        // stream's own refresh interval.
        private const val DYNAMIC_RETRY_BASE_SECONDS = 30L

        // Idle (no protocol frames for this long) aborts a negentropy session.
        // This is the ONLY bound on a session: there is no wall-clock deadline,
        // because a relay that is still answering is one we still want.
        // Each running slot is a thread parked on a socket read, so this is a
        // real resource — but a modest one next to what it buys, and the fan-out
        // is bounded by the streams' own `concurrency` rather than by this.
        private const val MAX_CONCURRENT_SOCKETS = 1024
        private const val MAX_CONCURRENT_SOCKETS_PER_HOST = 20

        private const val NEG_IDLE_MS = 30_000L

        // Distinct store failures to dump a raw event for. A handful names every
        // defect a real corpus carries; past that it is a stuck loop, not news.
        private const val POISON_SAMPLE_LIMIT = 20

        // Enough of the event to reproduce it. Kind 0 content runs long, and a
        // truncated tail still leaves the id, pubkey, kind and tags readable.
        private const val POISON_JSON_CHARS = 4_000

        private fun fmtDuration(ms: Long): String {
            val s = ms / 1000
            val h = s / 3600
            val m = (s % 3600) / 60
            val sec = s % 60
            return if (h > 0) "%d:%02d:%02d".format(h, m, sec) else "%d:%02d".format(m, sec)
        }
    }
}

/**
 * Write [events] through [write]; if that throws, split the batch and write the
 * halves, down to the single event the writer cannot take.
 *
 * A bulk write fails as a unit, so one event the store chokes on costs the whole
 * batch — at a 1000-event batch that is 999 good events lost per bad one, with no
 * retry and no way to tell which one did it. The failure count is then a multiple
 * of the batch size rather than a number of bad events, which reads as far worse
 * damage than it is.
 *
 * Bisecting costs ~2·log2(n) extra writes on a failing batch and nothing at all on
 * a healthy one, and it ends holding the offender by itself. Re-writing the good
 * halves is safe: a batch that threw may already have applied some of its events,
 * and re-inserting those is a duplicate the store rejects.
 *
 * Free-standing and injectable so the isolation can be tested without a store.
 */
internal suspend fun insertBisecting(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit = { _, _ -> },
) = bisect(events, write, onOutcomes, onPoison, onGaveUp, intArrayOf(ISOLATION_WRITE_BUDGET))

private suspend fun bisect(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit,
    budget: IntArray,
) {
    if (events.isEmpty()) return
    try {
        onOutcomes(write(events))
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        if (events.size == 1) {
            onPoison(events.single(), e)
            return
        }
        // Splitting assumes ONE event is at fault and the rest are fine. When the
        // store itself is refusing — a full disk, a dead engine — that assumption
        // inverts: every half fails, all the way down, and isolation turns one
        // failed write into ~2n. Precisely the wrong moment to multiply the load.
        //
        // Rather than reading the exception to guess which case this is (engine
        // error strings are not an API), spend a fixed budget of writes trying to
        // isolate, and give up on the remainder when it runs out. A batch with a
        // handful of bad events finishes well inside it; a store-wide failure
        // stops after a bounded probe instead of hammering the store 2n times.
        if (budget[0] <= 0) {
            onGaveUp(events, e)
            return
        }
        budget[0] -= 2
        val mid = events.size / 2
        bisect(events.subList(0, mid), write, onOutcomes, onPoison, onGaveUp, budget)
        bisect(events.subList(mid, events.size), write, onOutcomes, onPoison, onGaveUp, budget)
    }
}

/**
 * Writes one batch may spend isolating its bad events before giving up on the
 * rest.
 *
 * Isolating k bad events out of n costs about `2·k·log2(n)` writes, so 64 covers
 * three of them in a 1000-event batch — well past the one-per-batch rate seen in
 * practice. What it really bounds is the store-wide case, where every write fails
 * and the alternative is ~2000.
 */
private const val ISOLATION_WRITE_BUDGET = 64
