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

import com.nosfabrica.vespa.relay.router.config.DeleteMissing
import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.SyncMode
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.discovery.DiscoveredRelay
import com.nosfabrica.vespa.relay.router.discovery.HostStrikes
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.nosfabrica.vespa.relay.router.discovery.Unreachability
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.TcpProber
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Is the cheap TCP pre-probe able to answer anything about this relay?
 *
 * Only when the dial it precedes takes the same route it does. [TcpProber]
 * opens a plain socket to `InetSocketAddress(host, port)` — a DNS lookup and a
 * direct connection from this box's own address — so for anything the router
 * reaches THROUGH Tor it measures a path the transfer will never use.
 *
 * For a `.onion` that is a wrong answer: no resolver can answer the name, so
 * the probe reports `UnknownHostException` for a service that is up, and
 * [Unreachability] accepts that as proof and publishes it, signed, about
 * someone else's server. Under `SYNC_TOR_ALL` it is worse than wrong — the
 * probe would resolve and connect to every discovered relay directly, which is
 * precisely the exposure that setting exists to remove.
 *
 * There is nothing to replace it with: reachability through Tor is exactly
 * what the websocket dial measures, so the dial is the only verdict.
 */
internal fun shouldPreProbe(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
): Boolean = tor?.routes(url) != true

/**
 * The dynamic streams: no configured relays — every refresh reads the relay
 * lists our own store holds ([RelayDiscovery]), syncs the stream's filter
 * against every relay they name, sleeps, repeats. The discovery is inside the
 * loop on purpose: the store keeps filling, so each cycle fans out to a wider
 * set than the last. There is no live tail — the refresh IS the tail — and
 * each relay's socket is dropped once its sync returns.
 */
internal class DynamicSync(
    private val client: NostrClient,
    private val store: IEventStore,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val phases: StreamPhases,
    private val paging: PagingProgress,
    private val streamGate: Semaphore,
    private val transferring: AtomicInteger,
    // NIP-66: publishes strike verdicts and hands back the known-dead set.
    private val monitor: RelayMonitor?,
    // Relays with a live static subscription, whose sockets must never be
    // dropped out from under their tail.
    private val pinnedUrls: Set<NormalizedRelayUrl>,
    // The Tor transport, when configured: what makes discovered .onion urls
    // dialable at all, and what decides whether they may be dialled today.
    private val tor: TorTransport?,
    private val scope: CoroutineScope,
) {
    /**
     * `SYNC_DIAGNOSE=<stream>` — log one line per relay for that stream: how many
     * authors it was paired with, how many asks that became, how many legs the
     * cursor left, and what came back. Off by default because this fan-out is
     * 16,000 relays wide.
     */
    private val diagnose: String? = System.getenv("SYNC_DIAGNOSE")?.trim()?.takeIf { it.isNotEmpty() }

    private val deleteMissingSync = DeleteMissingSync(client, store, bands, ingest, paging)

    /** Records dropped because an upstream retracted them — see [DeleteMissingSync]. */
    val deleted: AtomicLong get() = deleteMissingSync.deleted

    /**
     * How many dynamic syncs are currently using each relay. Streams discover
     * from the same store, so two of them routinely land on the same relay at
     * once — and whichever finished first used to close the socket out from
     * under the other. Only the last one out disconnects.
     */
    private val inFlight = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /** One stream, forever: discover, sync, sleep, repeat. */
    suspend fun loop(stream: SyncStream) {
        val dynamic = stream.dynamic ?: return
        val sourceNames =
            dynamic.sources.joinToString { s ->
                "kinds ${s.filter.kinds?.joinToString("/") ?: "?"} x${s.selects.size} select(s)"
            }
        // Back off from short when a cycle could NOT run (an empty store, a
        // degraded engine) instead of waiting the full refresh interval —
        // both are usually fine again in moments.
        var retrySec = RETRY_BASE_SECONDS
        while (scope.isActive) {
            var ran = false
            try {
                // Never fan out onto ourselves: our own url is in plenty of lists.
                phases.set(stream.name, StreamPhases.Phase.Discovering(sourceNames))
                val relays =
                    RelayDiscovery.discover(
                        store,
                        dynamic,
                        skip = setOfNotNull(store.relay),
                        // A relay list full of .onion urls is only worth
                        // reading when something can dial them.
                        allowOnion = tor != null,
                    )
                if (relays.isEmpty()) {
                    phases.set(stream.name, StreamPhases.Phase.Waiting(sourceNames, retrySec))
                } else if (holdsIdSet(stream)) {
                    // Serialised with every other stream: this holds its id
                    // set for the whole fan-out, and two large sets resident
                    // at once is what pushed the heap to its ceiling.
                    phases.set(stream.name, StreamPhases.Phase.Queued(relays.size))
                    streamGate.withPermit { cycle(stream, dynamic, sourceNames, relays) }
                    ran = true
                } else {
                    // No id set, so nothing to serialise for. Taking the permit
                    // anyway is not free caution: measured, a 50-minute
                    // assertions cycle that downloaded 46 events held the only
                    // slot while two fetch streams sat on 15,458 discovered
                    // relays each, for a heap cost neither of them can incur.
                    // StaticBackfill has always gated this way — it takes the
                    // permit only when it has reconcilers to snapshot for.
                    cycle(stream, dynamic, sourceNames, relays)
                    ran = true
                }
            } catch (e: CancellationException) {
                // Shutdown, not a failure — close() almost always lands
                // mid-cycle. End quietly.
                throw e
            } catch (e: Exception) {
                phases.set(stream.name, StreamPhases.Phase.Failed(e.message?.take(80) ?: e.javaClass.simpleName, retrySec))
            }

            if (ran) {
                retrySec = RETRY_BASE_SECONDS
                delay(dynamic.refreshSeconds * 1000)
            } else {
                delay(retrySec * 1000)
                retrySec = (retrySec * 2).coerceAtMost(dynamic.refreshSeconds)
            }
        }
    }

    /**
     * Does a cycle of this stream build the one big local id set?
     *
     * That set — every id we hold for the stream's filter — is what the stream
     * gate serialises, because two of them resident at once is what pushed the
     * heap to its ceiling. A stream that never builds one has nothing to
     * serialise for, and must not queue behind a stream that does.
     */
    private fun holdsIdSet(stream: SyncStream): Boolean = stream.sync != SyncMode.FETCH && stream.deleteMissing == DeleteMissing.OFF

    /** Sync every discovered relay, [RelayDiscoveryConfig.concurrency] of them at a time. */
    private suspend fun cycle(
        stream: SyncStream,
        dynamic: RelayDiscoveryConfig,
        sourceNames: String,
        relays: List<DiscoveredRelay>,
    ) {
        val startedMs = System.currentTimeMillis()
        val gate = Semaphore(dynamic.concurrency)
        val downloaded = AtomicLong()
        val failed = AtomicLong()

        // What earlier runs already proved unreachable — a policy input loaded
        // once for the fan-out, not a per-dial lookup.
        val knownDead = monitor?.deadSet().orEmpty()
        val strikes = HostStrikes(knownDead = knownDead)

        val window = stream.filter
        // ONE snapshot for the whole cycle: every relay reconciles the same
        // filter, so per-relay snapshots were hundreds of identical full store
        // scans. A relay synced late compares against the store as it was at
        // the start — already true anyway, since ingest is asynchronous — and
        // the store dedups on insert. Narrowed to what the hungriest relay
        // still needs ([SyncBands.coveringWindow]).
        val snapshotWindow = bands.coveringWindow(stream.name, relays.map { it.url }, window)
        val snapStartedMs = System.currentTimeMillis()

        val local: List<IdAndTime> =
            if (!holdsIdSet(stream)) {
                // The same predicate that decided whether to hold the stream
                // gate decides whether to build the set it exists to protect.
                // Split in two, they drift, and the drift is invisible: a
                // stream queues behind a slot it never needed.
                System.err.println(
                    if (stream.sync == SyncMode.FETCH) {
                        // A fetch-only stream never reads the id set, and
                        // building one is the most expensive thing this router
                        // does (24.8M ids and gigabytes held live, measured).
                        "router: ${stream.name} sync=fetch — no local id set needed, skipping the snapshot"
                    } else {
                        // [DeleteMissingSync] reads its OWN ids per ask, and
                        // must: the shared snapshot spans every service on the
                        // stream, and handing it to a one-service reconcile
                        // would report every other service's records as
                        // retracted.
                        "router: ${stream.name} deleteMissing — ids are read per ask, skipping the shared snapshot"
                    },
                )
                emptyList()
            } else if (!bands.anyOutstanding(stream.name, relays.map { it.url }, window)) {
                // Nothing outside any relay's band, so every syncOne below
                // returns at its own leg check without ever reading the id set.
                // Distinct from holdsIdSet above, which asks whether this STREAM
                // ever needs one; this asks whether it needs one THIS cycle.
                // coveringWindow cannot save it — with nothing outstanding there
                // is no window to narrow to, and it correctly hands back the
                // whole filter. Asking first is where the saving is.
                System.err.println("router: ${stream.name} — all ${relays.size} relay(s) already cover the filter, skipping the snapshot")
                emptyList()
            } else {
                val expected = runCatching { store.count(snapshotWindow) }.getOrNull()
                phases.set(stream.name, StreamPhases.Phase.Snapshotting(0, expected, relays.size))
                store
                    .snapshotIdsReporting(snapshotWindow) { collected ->
                        phases.set(stream.name, StreamPhases.Phase.Snapshotting(collected, expected, relays.size))
                    }.also {
                        System.err.println(
                            "router: ${stream.name} local snapshot ${it.size} id(s) in ${fmtDuration(System.currentTimeMillis() - snapStartedMs)}" +
                                (snapshotWindow.since?.let { s -> ", since $s" } ?: ", full filter (no relay is caught up yet)"),
                        )
                    }
            }
        // Authors this cycle found at MORE THAN ONE relay. Deletion reads one
        // relay's silence as a retraction, and that only holds while it is the
        // author's sole upstream — measured, 3 of 266 NIP-85 services are
        // bound to several relays and two of those name general relays that
        // will never serve their scores, so an empty answer there is a wrong
        // pointer rather than a withdrawal. They still get mirrored.
        val sharedAuthors: Set<String> =
            if (stream.deleteMissing == DeleteMissing.OFF) {
                emptySet()
            } else {
                relays
                    .flatMap { it.narrow["authors"].orEmpty() }
                    .groupingBy { it }
                    .eachCount()
                    .filterValues { it > 1 }
                    .keys
                    .also {
                        if (it.isNotEmpty()) {
                            System.err.println(
                                "router: ${stream.name} ${it.size} author(s) are bound to more than one relay" +
                                    " — mirroring them, deleting nothing for them",
                            )
                        }
                    }
            }
        // Why the unreachable ones were unreachable: the shape of the
        // failures tells an operator whether that is normal churn or a broken
        // cycle.
        val reasons = ConcurrentHashMap<String, Long>()

        // Relays skipped because OUR proxy was not answering. Counted apart
        // from `reasons`, which prints its top three under "unreachable:" — a
        // failure of this router's own transport must never be filed as, or
        // crowded out by, other people's relays being unreachable.
        val torless = AtomicLong()
        System.err.println(
            "router: ${stream.name} syncing ${relays.size} relay(s) from [$sourceNames]" +
                " against ${local.size} local id(s)" +
                " (e.g. ${relays.take(3).joinToString { it.url.url }})",
        )
        val done = AtomicLong()
        val skipped = AtomicLong()
        coroutineScope {
            // A cycle runs for hours; without a ticker a stalled fan-out and a
            // working one look the same from outside.
            val ticker =
                launch {
                    while (true) {
                        delay(PROGRESS_INTERVAL_MS)
                        val finished = done.get()
                        if (stream.sync == SyncMode.FETCH) {
                            // A fetch-only stream has a real denominator — the
                            // time window each relay is walking.
                            phases.set(
                                stream.name,
                                StreamPhases.Phase.Fetching(
                                    done = finished.toInt(),
                                    total = relays.size,
                                    events = downloaded.get(),
                                    fraction = paging.fraction(stream.name),
                                    etaMs = paging.etaMs(stream.name),
                                    reachedSeconds = paging.reached(stream.name),
                                ),
                            )
                            continue
                        }
                        phases.set(
                            stream.name,
                            StreamPhases.Phase.Syncing(
                                done = finished.toInt(),
                                total = relays.size,
                                events = downloaded.get(),
                                skipped = skipped.get(),
                                unreachable = failed.get(),
                            ),
                        )
                    }
                }
            try {
                // The inner scope is what keeps the ticker alive:
                // `forEach { launch }` returns the moment the jobs are issued,
                // and cancelling on the way out of THAT killed the ticker
                // before its first delay ever elapsed.
                coroutineScope {
                    relays.forEach { relay ->
                        launch {
                            // Our own transport, before anything is said about
                            // theirs. A Tor that is down, restarting or renamed
                            // fails every dial it carries in a way that reads
                            // exactly like the relay being gone — so ask our
                            // SOCKS port instead, and skip rather than dial.
                            //
                            // Per relay, not once per cycle: the answer is
                            // cached for [TorSettings.PROBE_TTL_MS], so this
                            // costs one connect per 30s, and a Tor that comes
                            // back is picked up inside the running cycle. Held
                            // to the cycle boundary it would have cost a full
                            // refresh interval — six hours, by default — to
                            // notice a container that restarted in seconds.
                            if (tor?.routes(relay.url) == true && !tor.socksAnswers()) {
                                torless.incrementAndGet()
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                return@launch
                            }
                            // A TCP connect before the websocket handshake: a
                            // refused connection or unresolvable host answers
                            // in milliseconds, where each of ~20k corpses
                            // would otherwise cost a full connect timeout.
                            if (!tcpReachable(relay.url)) {
                                reasons.merge("tcp: no route or refused", 1L, Long::plus)
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                publishStrike(strikes, relay.url)
                                return@launch
                            }
                            gate.withPermit {
                                // Re-checked INSIDE the permit: with thousands
                                // of relays behind a small gate, the wait for
                                // a slot is exactly when sibling urls strike
                                // an authority out — checked before the wait,
                                // a dead host's urls would still be dialled.
                                if (strikes.isDead(relay.url)) {
                                    reasons.merge("skipped: authority already struck out", 1L, Long::plus)
                                    skipped.incrementAndGet()
                                    done.incrementAndGet()
                                    return@launch
                                }
                                // The relay's own filter, narrowed by what the
                                // tags that named it paired it with; identical
                                // to `window` for a select that binds only the
                                // url.
                                val got =
                                    syncRelay(stream, relay.url, relay.narrowed(window), local, sharedAuthors) { reason ->
                                        reasons.merge(reason, 1L, Long::plus)
                                    }
                                when {
                                    // Could not reach it: strike and publish,
                                    // the finding NIP-66 exists for.
                                    got == UNREACHABLE -> {
                                        failed.incrementAndGet()
                                        publishStrike(strikes, relay.url)
                                    }

                                    // Reached it; the transfer broke. NOT
                                    // struck and NOT published: the relay
                                    // answered our handshake, so calling it
                                    // unreachable would be a false statement
                                    // about someone else's server.
                                    got == TRANSFER_FAILED -> {
                                        failed.incrementAndGet()
                                    }

                                    got > 0 -> {
                                        downloaded.addAndGet(got.toLong())
                                        strikes.produced(relay.url)
                                    }

                                    // Answered cleanly with nothing new — a
                                    // working relay we are in sync with.
                                    else -> {
                                        strikes.produced(relay.url)
                                    }
                                }
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
        bands.flush()
        val elapsedMs = System.currentTimeMillis() - startedMs
        System.err.println(
            "router: ${stream.name} cycle done — ${downloaded.get()} event(s) from ${relays.size - failed.get() - skipped.get()}/${relays.size} relay(s)" +
                " in ${fmtDuration(elapsedMs)}" +
                (if (elapsedMs >= 1_000 && downloaded.get() > 0) " (${downloaded.get() * 1000 / elapsedMs}/s)" else "") +
                "; ${strikes.summary(relays.size)}" +
                (if (topReasons.isNotEmpty()) "; unreachable: $topReasons" else "") +
                (
                    if (torless.get() > 0) {
                        "; ${torless.get()} skipped — tor SOCKS ${tor?.settings?.socksAddress} not answering, nothing published about them"
                    } else {
                        ""
                    }
                ) +
                "; next in ${dynamic.refreshSeconds}s",
        )
        phases.set(stream.name, StreamPhases.Phase.Idle(downloaded.get(), dynamic.refreshSeconds))
    }

    /**
     * Sync one discovered relay: negentropy when it speaks NIP-77, paged REQ
     * when it doesn't. Returns the download count, or [UNREACHABLE] /
     * [TRANSFER_FAILED].
     */
    private suspend fun syncRelay(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        sharedAuthors: Set<String>,
        onFailure: (String) -> Unit,
    ): Int {
        inFlight.merge(url, 1, Int::plus)
        transferring.incrementAndGet()
        return try {
            var downloaded = 0
            val asks = splitByAuthors(window, stream.dynamic?.authorsPerLeg)
            for (ask in asks) {
                downloaded += syncOneFilter(stream, url, ask, local, sharedAuthors)
            }
            // DIAGNOSTIC: what this relay was asked and what came back. Enabled
            // by SYNC_DIAGNOSE, which names one stream — the fan-out is 16k
            // relays wide and a line each would be the log.
            if (diagnose == stream.name) {
                System.err.println(
                    "router: [diag] ${url.url} authors=${window.authors?.size ?: 0} " +
                        "ask(s)=${asks.size} leg(s)=${asks.sumOf { bands.legs(stream.name, url, it).size }} " +
                        "downloaded=$downloaded",
                )
            }
            downloaded
        } catch (e: CancellationException) {
            // Shutdown, not a dead relay: neither a tally nor a strike.
            throw e
        } catch (e: Exception) {
            // A dead host in a relay list is the common case, not an incident:
            // tally it and move on.
            onFailure("${e.javaClass.simpleName}: ${e.message?.take(50) ?: ""}".trim(':', ' '))
            // UNREACHABLE costs the relay a signed NIP-66 record — only say it
            // when it is true. See [Unreachability].
            //
            // Never for anything dialled through Tor. What arrives here from
            // a SOCKS dial is the PROXY's report — "host unreachable" from a
            // failed rendezvous, or an UnknownHostException from a Tor that is
            // not there — and none of it separates their server being down
            // from our circuit not being built. Under SYNC_TOR_ALL that covers
            // every relay, which is the case this guard exists for: a proxy
            // that stops answering would otherwise sign a false record about
            // every clearnet relay in the fan-out. The verdict costs one
            // skipped relay per cycle, which is the price of not guessing.
            if (tor?.routes(url) != true && Unreachability.proves(e)) UNREACHABLE else TRANSFER_FAILED
        } finally {
            transferring.decrementAndGet()
            releaseSocket(url)
        }
    }

    /**
     * One relay, one filter: walk what the cursor says is outside its band.
     * A narrowed stream asks the same relay once per author chunk, each its
     * own band; the socket is held once around all of them.
     */
    private suspend fun syncOneFilter(
        stream: SyncStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        sharedAuthors: Set<String>,
    ): Int {
        if (stream.deleteMissing != DeleteMissing.OFF) {
            return deleteMissingSync.reconcileAndDelete(stream, url, window, sharedAuthors)
        }
        var downloaded = 0
        for (leg in bands.legs(stream.name, url, window)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            // Per-kind spans, which quartz's SyncCoverage requires before it
            // will record a band for a multi-kind filter at all.
            val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
            val syncStartedAt = System.currentTimeMillis() / 1000
            val onEvent: suspend (Event) -> Unit = { event ->
                if (stream.filter.match(event)) {
                    if (SyncCoverage.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    // See StaticBackfill: without per-kind evidence quartz
                    // records no band for a multi-kind filter, so a discovery
                    // stream would re-walk every relay every cycle.
                    SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                    ingest.submit(event, stream.trusted)
                }
            }
            // Fetch-only: the leg came off the band, so this asks only
            // for what is outside what we already walked — the band IS the
            // mechanism here, there is no id set to fall back on.
            val fetched = stream.sync == SyncMode.FETCH
            val result =
                if (fetched) {
                    null.also {
                        val walk = "${stream.name}|${url.url}"
                        paging.begin(walk, leg.until ?: nowSeconds(), leg.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
                        downloaded +=
                            client.fetchAllPages(
                                url,
                                listOf(leg),
                                NEG_IDLE_MS,
                                onNewPage = { until -> paging.mark(walk, until) },
                                onEvent = onEvent,
                            )
                        paging.finish(walk)
                    }
                } else {
                    client
                        .negentropySyncOrFetch(
                            relay = url,
                            filter = leg,
                            idleTimeoutMs = NEG_IDLE_MS,
                            localEntries = local,
                            onEvent = onEvent,
                        ).also { downloaded += it.downloaded }
                }
            bands.record(
                stream.name,
                url,
                window,
                seenMin,
                seenMax,
                paged = fetched || result?.pagedFallback == true,
                reconciledThrough = syncStartedAt.takeIf { result != null && !result.pagedFallback },
                observedByKind = seenByKind,
            )
        }
        return downloaded
    }

    /**
     * [window] as one ask, or as several with at most [per] authors each. A
     * band is keyed on its filter, so the chunk size decides how often
     * a band survives — see [RelayDiscoveryConfig.authorsPerLeg].
     */
    private fun splitByAuthors(
        window: Filter,
        per: Int?,
    ): List<Filter> {
        val authors = window.authors
        if (per == null || authors == null || authors.size <= per) return listOf(window)
        return authors.chunked(per).map { window.copy(authors = it) }
    }

    /**
     * Strike a relay, and publish the verdict if it takes its whole host
     * down. Eviction is the only point where evidence exists — after it,
     * every sibling url is skipped without being dialled, so nothing will
     * ever observe them again.
     */
    private fun publishStrike(
        strikes: HostStrikes,
        url: NormalizedRelayUrl,
    ) {
        val evicted = strikes.strike(url) ?: return
        // Struck locally either way — a host that answers nothing should stop
        // costing the cycle sockets — but nothing we reach THROUGH Tor is ever
        // published on this evidence. The verdict is built from silence, and
        // silence arriving through three relays and a rendezvous is at least as
        // likely to be our circuit as their server. The test is the transport,
        // not the address: under SYNC_TOR_ALL an ordinary wss:// relay is
        // behind exactly the same circuit and the claim is exactly as weak.
        // quartz's own observer still records what a failed connection said;
        // this is the claim we synthesise, and we cannot support it.
        if (tor?.routes(url) == true) return
        monitor?.observer?.record(
            url,
            reachable = false,
            error = "host ${evicted.authority} silent after ${evicted.strikes} attempts",
        )
    }

    /**
     * Can we open a TCP connection to this relay at all? Fail-OPEN: any error
     * deciding this returns true, so a broken probe can never silently
     * amputate the fan-out.
     *
     * Only a NEGATIVE result is published: a completed TCP handshake proves a
     * socket, not a relay — the connection that follows says it properly. But
     * the negative IS published, because this probe is the only thing that
     * will ever look at most of these relays.
     */
    private suspend fun tcpReachable(url: NormalizedRelayUrl): Boolean {
        if (!shouldPreProbe(url, tor)) return true
        val ok = runCatching { TcpProber.tcpReachable(url) }.getOrDefault(true)
        // Only claim what we can prove. [TcpProber.tcpReachable] answers with a
        // Boolean, so a refusal and a timeout arrive here as the same value — and
        // they are not the same claim. A refusal proves nobody is listening. A
        // timeout is at least as likely to be OUR socket budget, DNS pressure, or
        // one NAT carrying a 100-wide fan-out.
        //
        // Publishing on the Boolean signed 5,001 unreachable records in a single
        // hour. Re-probed one at a time afterwards: 3,279 had no socket at all
        // and 986 answered nothing, but 732 urls across 423 HOSTS answered a REQ
        // perfectly well — 120 of them by challenging us for NIP-42 AUTH. Those
        // are signed public statements about other people's servers, and they
        // were wrong.
        //
        // So the failure is re-run once to capture its cause, and published only
        // for what [Unreachability] already accepts as proof. A relay that merely
        // timed out is skipped this cycle and nothing is said about it. The extra
        // connect is paid only on the failing path.
        if (!ok) {
            tcpFailure(url)?.takeIf { Unreachability.proves(it) }?.let { cause ->
                monitor?.observer?.record(url, reachable = false, error = "tcp: ${cause.javaClass.simpleName}")
            }
        }
        return ok
    }

    /**
     * Re-run the TCP connect, keeping the exception instead of a Boolean.
     *
     * Null when it unexpectedly succeeds — the pre-probe's budget is tight and the
     * host may merely have been slow, which is itself a reason not to have
     * published — or when the url has no host to dial.
     */
    private suspend fun tcpFailure(url: NormalizedRelayUrl): Exception? =
        withContext(Dispatchers.IO) {
            val uri = runCatching { java.net.URI(url.url) }.getOrNull() ?: return@withContext null
            val host = uri.host ?: return@withContext null
            val port =
                when {
                    uri.port > 0 -> uri.port
                    url.url.startsWith("wss://", ignoreCase = true) -> 443
                    else -> 80
                }
            try {
                java.net.Socket().use { it.connect(java.net.InetSocketAddress(host, port), CLAIM_PROBE_TIMEOUT_MS) }
                null
            } catch (e: java.io.IOException) {
                e
            }
        }

    /**
     * Drop a dynamic relay's socket once nothing is using it — hundreds of
     * relays a cycle would otherwise leave hundreds of idle connections open.
     * Pinned relays and relays another stream is still syncing are left alone.
     */
    private fun releaseSocket(url: NormalizedRelayUrl) {
        val stillInUse = inFlight.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } } != null
        if (!stillInUse && url !in pinnedUrls) {
            runCatching { client.getOrCreateRelay(url).disconnect() }
        }
    }

    companion object {
        // First wait after a cycle could not run; doubles up to the stream's
        // own refresh interval.
        private const val RETRY_BASE_SECONDS = 30L

        // syncRelay's two failure returns, distinct because only one of them
        // is publishable. Both negative so `got > 0` (delivered) and
        // `got == 0` (nothing new) keep meaning what they say.

        /**
         * How long the confirming connect waits before we decline to claim.
         *
         * Looser than the pre-probe's tight budget on purpose: that one is an
         * optimisation and may skip a slow host cheaply, while this one decides
         * whether to sign a public statement about somebody's server. When the
         * two disagree, the quiet answer wins.
         */
        private const val CLAIM_PROBE_TIMEOUT_MS = 5_000

        private const val UNREACHABLE = -1
        private const val TRANSFER_FAILED = -2
    }
}
