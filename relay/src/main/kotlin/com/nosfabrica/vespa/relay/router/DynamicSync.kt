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

import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

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
    private val cursors: SyncCursors,
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
    private val scope: CoroutineScope,
) {
    private val deleteMissingSync = DeleteMissingSync(client, store, cursors, ingest)

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
    suspend fun loop(stream: MirrorStream) {
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
                val relays = RelayDiscovery.discover(store, dynamic, skip = setOfNotNull(store.relay))
                if (relays.isEmpty()) {
                    phases.set(stream.name, StreamPhases.Phase.Waiting(sourceNames, retrySec))
                } else {
                    // Serialised with every other stream: this holds its id
                    // set for the whole fan-out, and two large sets resident
                    // at once is what pushed the heap to its ceiling.
                    phases.set(stream.name, StreamPhases.Phase.Queued(relays.size))
                    streamGate.withPermit { cycle(stream, dynamic, sourceNames, relays) }
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

    /** Sync every discovered relay, [DynamicRelayList.concurrency] of them at a time. */
    private suspend fun cycle(
        stream: MirrorStream,
        dynamic: DynamicRelayList,
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
        val health = RelayHealth(knownDead = knownDead)

        val window = stream.filter
        // ONE snapshot for the whole cycle: every relay reconciles the same
        // filter, so per-relay snapshots were hundreds of identical full store
        // scans. A relay synced late compares against the store as it was at
        // the start — already true anyway, since ingest is asynchronous — and
        // the store dedups on insert. Narrowed to what the hungriest relay
        // still needs ([SyncCursors.coveringWindow]).
        val snapshotWindow = cursors.coveringWindow(relays.map { it.url }, window)
        val snapStartedMs = System.currentTimeMillis()

        val local: List<IdAndTime> =
            if (stream.sync == SyncMode.FETCH) {
                // A fetch-only stream never reads the id set, and building one
                // is the most expensive thing this router does (24.8M ids and
                // gigabytes held live, measured).
                System.err.println("router: ${stream.name} sync=fetch — no local id set needed, skipping the snapshot")
                emptyList()
            } else if (stream.deleteMissing != DeleteMissing.OFF) {
                // [DeleteMissingSync] reads its OWN ids per ask, and must:
                // the shared snapshot spans every service on the stream, and
                // handing it to a one-service reconcile would report every
                // other service's records as retracted.
                System.err.println("router: ${stream.name} deleteMissing — ids are read per ask, skipping the shared snapshot")
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
        // Why the unreachable ones were unreachable: the shape of the
        // failures tells an operator whether that is normal churn or a broken
        // cycle.
        val reasons = ConcurrentHashMap<String, Long>()
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
                            // A TCP connect before the websocket handshake: a
                            // refused connection or unresolvable host answers
                            // in milliseconds, where each of ~20k corpses
                            // would otherwise cost a full connect timeout.
                            if (!tcpReachable(relay.url)) {
                                reasons.merge("tcp: no route or refused", 1L, Long::plus)
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                publishStrike(health, relay.url)
                                return@launch
                            }
                            // Re-checked here rather than filtered up front:
                            // an authority struck out while this one waited
                            // for a slot should not still be dialled.
                            if (health.isDead(relay.url)) {
                                reasons.merge("skipped: authority already struck out", 1L, Long::plus)
                                skipped.incrementAndGet()
                                done.incrementAndGet()
                                return@launch
                            }
                            gate.withPermit {
                                // The relay's own filter, narrowed by what the
                                // tags that named it paired it with; identical
                                // to `window` for a select that binds only the
                                // url.
                                val got =
                                    syncRelay(stream, relay.url, relay.narrowed(window), local) { reason ->
                                        reasons.merge(reason, 1L, Long::plus)
                                    }
                                when {
                                    // Could not reach it: strike and publish,
                                    // the finding NIP-66 exists for.
                                    got == UNREACHABLE -> {
                                        failed.incrementAndGet()
                                        publishStrike(health, relay.url)
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
                                        health.produced(relay.url)
                                    }

                                    // Answered cleanly with nothing new — a
                                    // working relay we are in sync with.
                                    else -> {
                                        health.produced(relay.url)
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
        cursors.flush()
        val elapsedMs = System.currentTimeMillis() - startedMs
        System.err.println(
            "router: ${stream.name} cycle done — ${downloaded.get()} event(s) from ${relays.size - failed.get() - skipped.get()}/${relays.size} relay(s)" +
                " in ${fmtDuration(elapsedMs)}" +
                (if (elapsedMs >= 1_000 && downloaded.get() > 0) " (${downloaded.get() * 1000 / elapsedMs}/s)" else "") +
                "; ${health.summary(relays.size)}" +
                (if (topReasons.isNotEmpty()) "; unreachable: $topReasons" else "") +
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
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
        onFailure: (String) -> Unit,
    ): Int {
        inFlight.merge(url, 1, Int::plus)
        transferring.incrementAndGet()
        return try {
            var downloaded = 0
            for (ask in splitByAuthors(window, stream.dynamic?.authorsPerLeg)) {
                downloaded += syncOneFilter(stream, url, ask, local)
            }
            downloaded
        } catch (e: Exception) {
            // A dead host in a relay list is the common case, not an incident:
            // tally it and move on.
            onFailure("${e.javaClass.simpleName}: ${e.message?.take(50) ?: ""}".trim(':', ' '))
            // UNREACHABLE costs the relay a signed NIP-66 record — only say it
            // when it is true. See [Unreachability].
            if (Unreachability.proves(e)) UNREACHABLE else TRANSFER_FAILED
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
        stream: MirrorStream,
        url: NormalizedRelayUrl,
        window: Filter,
        local: List<IdAndTime>,
    ): Int {
        if (stream.deleteMissing != DeleteMissing.OFF) return deleteMissingSync.reconcileAndDelete(stream, url, window)
        var downloaded = 0
        for (leg in cursors.legs(url, window)) {
            var seenMin: Long? = null
            var seenMax: Long? = null
            val syncStartedAt = System.currentTimeMillis() / 1000
            val onEvent: (Event) -> Unit = { event ->
                if (stream.filter.match(event)) {
                    if (SyncCursors.isPlausible(event.createdAt)) {
                        seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                        seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                    }
                    ingest.offer(event, stream.trusted)
                }
            }
            // Fetch-only: the leg came off the cursor band, so this asks only
            // for what is outside what we already walked — the band IS the
            // mechanism here, there is no id set to fall back on.
            val fetched = stream.sync == SyncMode.FETCH
            val result =
                if (fetched) {
                    null.also {
                        val walk = "${stream.name}|${url.url}"
                        paging.begin(walk, leg.until ?: nowSeconds(), leg.since ?: SyncCursors.PLAUSIBLE_FLOOR)
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
            cursors.record(
                url,
                window,
                seenMin,
                seenMax,
                paged = fetched || result?.pagedFallback == true,
                reconciledThrough = syncStartedAt.takeIf { result != null && !result.pagedFallback },
            )
        }
        return downloaded
    }

    /**
     * [window] as one ask, or as several with at most [per] authors each. A
     * cursor band is keyed on its filter, so the chunk size decides how often
     * a band survives — see [DynamicRelayList.authorsPerLeg].
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
        health: RelayHealth,
        url: NormalizedRelayUrl,
    ) {
        val evicted = health.strike(url) ?: return
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
        val ok = runCatching { TcpProber.tcpReachable(url) }.getOrDefault(true)
        if (!ok) monitor?.observer?.record(url, reachable = false, error = "tcp: unreachable")
        return ok
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
        private const val UNREACHABLE = -1
        private const val TRANSFER_FAILED = -2
    }
}
