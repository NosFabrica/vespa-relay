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
import com.nosfabrica.vespa.relay.router.discovery.AliasFolding
import com.nosfabrica.vespa.relay.router.discovery.AliasMonitor
import com.nosfabrica.vespa.relay.router.discovery.CachedRelayList
import com.nosfabrica.vespa.relay.router.discovery.DiscoveredRelay
import com.nosfabrica.vespa.relay.router.discovery.HostStrikes
import com.nosfabrica.vespa.relay.router.discovery.RelayAliases
import com.nosfabrica.vespa.relay.router.discovery.RelayDiscovery
import com.nosfabrica.vespa.relay.router.discovery.RelayRotation
import com.nosfabrica.vespa.relay.router.discovery.Unreachability
import com.nosfabrica.vespa.relay.router.heal.Healer
import com.nosfabrica.vespa.relay.router.progress.CycleTally
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

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
    // NIP-66 again, saying the other thing a dial can prove: that two of the
    // discovered urls are one relay. Null when there is no identity to sign a
    // verdict with, and then every url is dialled as its own relay.
    //
    // Split in two on purpose: this one only READS verdicts, on the cycle's
    // critical path, and the monitor below EARNS them somewhere else.
    private val folding: AliasFolding?,
    // Where the dialling half of that fold lives. Null exactly when [folding]
    // is: same identity, same reason.
    private val aliasMonitor: AliasMonitor?,
    // The Tor transport, when configured: what makes discovered .onion urls
    // dialable at all, and what decides whether they may be dialled today.
    private val tor: TorTransport?,
    private val scope: CoroutineScope,
    // Repairs are queued by ingest and drained here, at the end of each
    // relay's own sync while its socket is still open — a relaySource stream
    // keeps no live tail, so a detached healer would have to re-dial.
    private val healer: Healer,
    private val refusedIds: RefusedIds,
) {
    /**
     * `SYNC_DIAGNOSE=<stream>` — log one line per relay for that stream: how many
     * authors it was paired with, how many asks that became, how many legs the
     * cursor left, and what came back. Off by default because this fan-out is
     * 16,000 relays wide.
     */
    private val diagnose: String? = System.getenv("SYNC_DIAGNOSE")?.trim()?.takeIf { it.isNotEmpty() }

    private val deleteMissingSync = DeleteMissingSync(client, store, bands, ingest, paging, refusedIds)

    /** Records dropped because an upstream retracted them — see [DeleteMissingSync]. */
    val deleted: AtomicLong get() = deleteMissingSync.deleted

    /**
     * How many dynamic syncs are currently using each relay. Streams discover
     * from the same store, so two of them routinely land on the same relay at
     * once — and whichever finished first used to close the socket out from
     * under the other. Only the last one out disconnects.
     */
    private val inFlight = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /**
     * One stream, forever: walk the relay list handing work to a fixed pool of
     * workers, wrap round, walk it again.
     *
     * **There is no join.** A pass ENDS when the last url has been handed out,
     * not when the last worker returns, so a relay that takes hours costs one
     * slot out of [RelayDiscoveryConfig.concurrency] and nothing else. The
     * previous shape — launch every discovered relay, await all of them, then
     * start the next cycle — made a single leg able to stop a mirror: measured,
     * `fetchAllPages` against purplepag.es never returned at all, and a
     * 16,752-relay stream sat at "cycle in progress" for the life of the process
     * with every other relay in the list long since finished and nothing due to
     * dial any of them again.
     *
     * The three things that fall out of removing it, each handled in its own
     * place: passes overlap, so a url still being synced must not be handed out
     * twice ([RelayRotation]); the shared id set outlives the pass that built
     * it, so generations must be bounded ([SharedIdSet]); and "the cycle
     * finished" stops meaning "everything settled", so the count still running
     * is published rather than left to be inferred from silence.
     *
     * The relay list itself is held across passes and re-derived on the refresh
     * period — see [CachedRelayList] and [RelayDiscoveryConfig.recycleSeconds].
     */
    suspend fun loop(stream: SyncStream) {
        val dynamic = stream.dynamic ?: return
        val sourceNames =
            dynamic.sources.joinToString { s ->
                "kinds ${s.filter.kinds?.joinToString("/") ?: "?"} x${s.selects.size} select(s)"
            }
        // Back off from short when a pass could NOT run (an empty store, a
        // degraded engine) instead of waiting the full refresh interval —
        // both are usually fine again in moments.
        var retrySec = RETRY_BASE_SECONDS
        var cached: CachedRelayList? = null
        // THE POOL, and it belongs to the stream rather than to a pass. A slot
        // is held by the relay using it, so a leg still running when the walk
        // wraps keeps its slot and the next pass simply has fewer to hand out.
        // Rebuilt per pass it would be a fresh set of permits with the old ones
        // still occupied, i.e. a concurrency limit that means nothing.
        val pool = Semaphore(dynamic.concurrency)
        val rotation = RelayRotation()
        val idSet = SharedIdSet()
        // What the ticker reports. The ticker belongs to the stream too: between
        // two passes there are still relays syncing, and a stream that went
        // quiet for the gap read exactly like one that had stopped.
        val live = AtomicReference<PassProgress?>(null)
        val ticker = scope.launch { report(stream, dynamic, rotation, live) }
        try {
            while (scope.isActive) {
                var ran = false
                try {
                    val nowMs = System.currentTimeMillis()
                    // Unset `recycleSeconds` is not a long TTL — it is the old
                    // behaviour, where every pass rediscovers and nothing is
                    // ever reused. Checked here rather than inside
                    // [CachedRelayList] so that a stream not opted in never even
                    // holds one.
                    val reused =
                        dynamic.recycleSeconds?.let {
                            cached?.takeIf { c -> c.reusableAt(nowMs, dynamic.refreshSeconds, aliasMonitor?.generation() ?: 0L) }
                        }
                    if (reused != null) {
                        // Said out loud every time. A pass that re-read the
                        // store and one that started on a five-hour-old list are
                        // the same fan-out from every other line this stream
                        // prints, and the difference is exactly what an operator
                        // needs when the set looks wrong.
                        System.err.println(
                            "router: ${stream.name} reusing the relay list from ${fmtDuration(reused.ageSec(nowMs) * 1000)} ago" +
                                " — ${reused.relays.size} relay(s), rediscovering after ${dynamic.refreshSeconds}s",
                        )
                    }
                    val list = reused ?: discoverRelayList(stream, dynamic, sourceNames, nowMs)
                    // Held for the next pass even when it is empty: `reusableAt`
                    // refuses an empty list, so this only ever means the next
                    // pass rediscovers, and keeping it makes the "cached or not"
                    // state one variable instead of two.
                    //
                    // A pass that THROWS leaves it in place, so a stream whose
                    // discovery has started failing — a degraded engine, a query
                    // that now times out — keeps mirroring off the last good
                    // list until it ages out, rather than stopping. The failure
                    // is still said once, through `Phase.Failed` below.
                    cached = list
                    if (list.relays.isEmpty()) {
                        // Cleared, or the ticker paints the previous pass's
                        // `Idle` over this within a tick and "the sources named
                        // no relays" — the one thing an operator can act on
                        // here — never reaches the log.
                        live.set(null)
                        phases.set(stream.name, StreamPhases.Phase.Waiting(sourceNames, retrySec))
                    } else {
                        runPass(stream, dynamic, sourceNames, list, rotation, pool, idSet, live)
                        ran = true
                    }
                } catch (e: CancellationException) {
                    // Shutdown, not a failure — close() almost always lands
                    // mid-pass. End quietly.
                    throw e
                } catch (e: Exception) {
                    phases.set(stream.name, StreamPhases.Phase.Failed(e.message?.take(80) ?: e.javaClass.simpleName, retrySec))
                    // A pass that threw at 80% used to leave exactly what one
                    // that finished leaves: nothing. The outcome is stamped here
                    // so the two stop reading the same, and `endCycle` ignores
                    // the call when no pass was running — a stream that failed
                    // during DISCOVERY must not re-date the last one that did.
                    phases.endCycle(stream.name, StreamPhases.DYNAMIC, "failed")
                }

                if (ran) {
                    retrySec = RETRY_BASE_SECONDS
                    // The recycle gap where one is configured, the refresh
                    // period otherwise — and the same number the pass's last
                    // line and `Phase.Idle` just promised. Measured from the end
                    // of the WALK, not from the last worker: the workers are
                    // still going, which is the point.
                    delay(dynamic.nextCycleSeconds * 1000)
                } else {
                    delay(retrySec * 1000)
                    retrySec = (retrySec * 2).coerceAtMost(dynamic.refreshSeconds)
                }
            }
        } finally {
            ticker.cancel()
        }
    }

    /**
     * One pass's live state, shared by its workers and read by the ticker.
     *
     * Per pass rather than per stream because the counters are what
     * [CycleTally] publishes, and a straggler must settle into the pass that
     * handed it out — otherwise a url is counted against a `taken` it was never
     * part of and the partition stops closing.
     */
    private class PassProgress(
        val number: Long,
        /** Urls in the relay list this pass walked, including the ones it skipped as busy. */
        val total: Int,
        val tally: CycleTally,
        val strikes: HostStrikes,
    ) {
        val startedMs = System.currentTimeMillis()

        /** Set when the WALK ends — the workers carry on past it. */
        @Volatile
        var walkEndedMs: Long? = null

        val downloaded = AtomicLong()
        val failed = AtomicLong()
        val skipped = AtomicLong()

        /** Urls of this pass that have reached an outcome. Reaches [total] after the walk, not at it. */
        val done = AtomicLong()

        /** Skipped because OUR proxy was not answering — never filed as a relay being unreachable. */
        val torless = AtomicLong()

        /** Why the unreachable ones were unreachable: normal churn, or a broken pass. */
        val reasons = ConcurrentHashMap<String, Long>()
    }

    /**
     * The stream's progress line, forever.
     *
     * Reads whichever pass is current and reports `running` beside it, because
     * the two answer different questions once passes overlap: `done/total` is
     * how far the WALK got, and `running` is how much of the pool is committed —
     * including legs from passes that ended minutes ago. A rotation with a full
     * pool and a finished walk is working hard and used to render as idle.
     */
    private suspend fun report(
        stream: SyncStream,
        dynamic: RelayDiscoveryConfig,
        rotation: RelayRotation,
        live: AtomicReference<PassProgress?>,
    ) {
        while (true) {
            delay(PROGRESS_INTERVAL_MS)
            val p = live.get() ?: continue
            val running = rotation.busyCount()
            val ended = p.walkEndedMs
            if (ended != null) {
                // The walk is done; what is left is the tail plus the gap. The
                // countdown is real — it is the same delay the loop is sitting
                // in — so it can say when, rather than repeating a constant.
                val waitedSec = (System.currentTimeMillis() - ended) / 1000
                phases.set(
                    stream.name,
                    StreamPhases.Phase.Idle(
                        events = p.downloaded.get(),
                        nextInSec = (dynamic.nextCycleSeconds - waitedSec).coerceAtLeast(0),
                        running = running,
                    ),
                )
                continue
            }
            if (stream.sync == SyncMode.FETCH) {
                // A fetch-only stream has a real denominator — the time window
                // each relay is walking.
                phases.set(
                    stream.name,
                    StreamPhases.Phase.Fetching(
                        done = p.done.get().toInt(),
                        total = p.total,
                        events = p.downloaded.get(),
                        fraction = paging.fraction(stream.name),
                        etaMs = paging.etaMs(stream.name),
                        reachedSeconds = paging.reached(stream.name),
                        running = running,
                    ),
                )
                continue
            }
            phases.set(
                stream.name,
                StreamPhases.Phase.Syncing(
                    done = p.done.get().toInt(),
                    total = p.total,
                    events = p.downloaded.get(),
                    skipped = p.skipped.get(),
                    unreachable = p.failed.get(),
                    running = running,
                ),
            )
        }
    }

    /**
     * Read this stream's fan-out set out of the store: discover, fold, exclude.
     *
     * Everything expensive about starting a cycle is here, which is what makes
     * the result worth holding on to ([CachedRelayList]).
     */
    private suspend fun discoverRelayList(
        stream: SyncStream,
        dynamic: RelayDiscoveryConfig,
        sourceNames: String,
        nowMs: Long,
    ): CachedRelayList {
        // Never fan out onto ourselves: our own url is in plenty of lists.
        phases.set(stream.name, StreamPhases.Phase.Discovering(sourceNames))
        // Read BEFORE the work, not after. A pass that publishes verdicts while
        // this discovery is running has not been applied to its result, and
        // stamping the later generation on the list would mark it current for a
        // fold it never saw — the one case a version check has to get right.
        val aliasGeneration = aliasMonitor?.generation() ?: 0L
        val discovered =
            RelayDiscovery.discover(
                store,
                dynamic,
                skip = setOfNotNull(store.relay),
                // A relay list full of .onion urls is only worth
                // reading when something can dial them.
                allowOnion = tor != null,
            )
        // Before the fan-out, and before the snapshot it sizes itself
        // against: a folded url must never reach [cycle], or it takes
        // a socket, a cursor band and a place in the concurrency gate
        // for events another url in the same list already delivered.
        // The fold hands back an alias MAP, not a rewritten relay list:
        // it deals in urls, and this stream is the only thing that
        // knows each url also carries the authors its tag paired it
        // with. [RelayAliases.fold] is that one line — and dropping a
        // url without moving its authors onto the survivor would stop
        // asking for those authors entirely.
        //
        // READ ONLY here. Applying verdicts is a store query; EARNING
        // them is a probe pass, and that belongs to [AliasMonitor] on
        // its own clock — inline, it sat between "discovery finished"
        // and the first downloaded byte on every cycle. The cost of the
        // split is that a newly discovered url is dialled unfolded once,
        // before the pass that measures it.
        val candidates = discovered.map { it.url }
        val cleaned = folding?.apply(candidates)
        // The state an EARLIER cycle left behind, cleared as the fold
        // takes hold. Everything above is about what this cycle dials;
        // this is about what the last one already did — a url that used
        // to be dialled in its own right has bands on disk, and once it
        // is folded nothing will ever advance them again. Left there
        // they are what `/stats.json` charts, so the coverage card goes
        // on naming a dozen urls of one host as separately walked while
        // exactly one of them is being synced. See [SyncBands.dropFolded]
        // for why they are dropped rather than merged onto the survivor.
        cleaned?.aliases?.keys?.let { aliased ->
            // Never a url a static subscription is holding: one stream
            // may carry both `urls` and `relaySource`, and its
            // backfill records under this same name. See
            // [SyncBands.dropFolded].
            val dropped = bands.dropFolded(stream.name, aliased, keep = pinnedUrls)
            // Only the cycle that changes something says so. After the
            // first these are the same verdicts every time, and after a
            // restart they are verdicts whose state the last process
            // already dropped — the count is what this pass took out of
            // the file, not how many urls are folded.
            if (dropped > 0) {
                System.err.println("router: ${stream.name} dropped the band state of $dropped folded url(s)")
            }
        }
        // The fold's OWN output, held before the exclude filter runs.
        // Both counts below are then differences between two lists this
        // code actually has, rather than inferences from a subtraction —
        // which is what made them wrong: `foldOnto` MERGES onto a
        // survivor, and a survivor discovery did not itself hand over is
        // synthesised into the result, so `candidates.size - relays.size`
        // is not the number of urls the fold removed and the identity
        // `discovered = folded + excluded + taken` did not hold.
        val foldedList = cleaned?.let { RelayAliases.foldOnto(discovered, it.aliases) } ?: discovered
        val relays =
            foldedList
                // A verdict's canonical is whatever the probe measured,
                // which is NOT necessarily a url discovery would hand
                // out today: `exclude` and our own url are applied when
                // relay lists are read, and a fold can name a url from
                // before that config changed. Re-applying them here is
                // what stops a stored verdict putting an excluded relay
                // — or this relay, syncing itself — back into the
                // fan-out through the side door.
                .filter { it.url !in dynamic.exclude && it.url != store.relay }
        // What this stream would like measured before the next cycle.
        // Non-blocking; the callbacks are this stream's own transport
        // guard and ingest, since the monitor has neither.
        //
        // Only on the path that rediscovered: the monitor holds the LATEST
        // candidate set per stream, replacing rather than appending, so
        // re-submitting a list that has not changed hands it the work it is
        // already holding.
        aliasMonitor?.submit(
            label = stream.name,
            candidates = candidates,
            canDial = { url -> (tor?.routes(url) != true || tor.socksAnswers()) && tcpReachable(url) },
            onEvent = { event -> if (stream.filter.match(event)) ingest.submit(event, stream.trusted) },
        )
        // Each member is a difference between two lists this code
        // holds, so the identity closes for every case including a
        // synthesised survivor:
        //   discovered      = candidates
        //   foldedOntoAnother = candidates - foldedList
        //   excluded        = foldedList - relays   (the `exclude` list
        //                     and our own url being obeyed)
        //   taken           = relays                (what is dialled)
        // It was `candidates.size - relays.size` for the fold and a
        // remainder for the rest, which folded an operator's exclusion
        // list into the duplicate-url count and could leave `taken`
        // over-counted — so a healthy cycle ended with `pending` stuck
        // above zero and the page printed "these counts do not add up".
        return CachedRelayList(
            relays = relays,
            discovered = candidates.size,
            foldedOntoAnother = (candidates.size - foldedList.size).coerceAtLeast(0),
            excluded = (foldedList.size - relays.size).coerceAtLeast(0),
            // By AUTHORITY, so the count says how many servers this
            // is, not how many strings. Arithmetic over urls, not
            // the fold's verdict — see [CycleTally].
            hosts =
                relays
                    .mapNotNull {
                        it.url.url
                            .substringAfter("://")
                            .substringBefore("/")
                            .ifEmpty { null }
                    }.distinct()
                    .size,
            folded =
                cleaned
                    ?.aliases
                    ?.let { verdicts -> candidates.mapNotNull { url -> verdicts[url]?.let { url.url to it.url } }.toMap() }
                    .orEmpty(),
            // The clock the cycle about to run was timed from, not a second
            // reading taken after a discovery that may have run for minutes:
            // a list is as old as the walk that produced it.
            builtAtMs = nowMs,
            aliasGeneration = aliasGeneration,
        )
    }

    /**
     * Does a cycle of this stream build the one big local id set?
     *
     * That set — every id we hold for the stream's filter — is the most
     * expensive thing this router builds, which is why the build is serialised
     * behind the stream gate and why [SharedIdSet] bounds how many of them a
     * rotation can leave alive. A stream that never builds one has neither
     * concern, and must not queue behind a stream that does.
     */
    private fun holdsIdSet(stream: SyncStream): Boolean = stream.sync != SyncMode.FETCH && stream.deleteMissing == DeleteMissing.OFF

    /**
     * ONE WALK of the relay list, handing each url to the pool and returning
     * when the last one has been handed out.
     *
     * Not when the last one has FINISHED — that is the join this replaced. The
     * workers this launches run in the engine's scope and outlive the call, so
     * everything after the loop below ("pass done", `endCycle`, the band flush)
     * describes the walk rather than the work, and says how much of the work is
     * still going instead of implying there is none.
     */
    private suspend fun runPass(
        stream: SyncStream,
        dynamic: RelayDiscoveryConfig,
        sourceNames: String,
        list: CachedRelayList,
        rotation: RelayRotation,
        pool: Semaphore,
        idSet: SharedIdSet,
        live: AtomicReference<PassProgress?>,
    ) {
        val relays = list.relays
        // Skipping what is still being synced is the first thing that happens,
        // not something discovered mid-walk: a url a worker still holds is not
        // work this pass can do, and counting it as taken-but-pending would
        // leave the partition open on every pass forever.
        val work = rotation.beginPass(relays)
        val progress =
            PassProgress(
                number = rotation.pass(),
                total = relays.size,
                // The disposition of this pass's urls. Opened here rather than
                // deeper so that `discovered` counts what discovery handed over
                // and `foldedOntoAnother` the difference the fold made — both
                // decided in [discoverRelayList], neither visible from inside
                // the walk. A FRESH one per pass even on a reused list — see
                // [CachedRelayList.tally].
                tally = list.tally(System.currentTimeMillis()),
                // What earlier runs already proved unreachable — a policy input
                // loaded once for the walk, not a per-dial lookup.
                strikes = HostStrikes(knownDead = monitor?.deadSet().orEmpty()),
            )
        // Settled before a single dial: these urls have an outcome for this
        // pass already, and it is not `pending`.
        progress.tally.busy.addAndGet(work.busy.toLong())
        progress.done.addAndGet(work.busy.toLong())
        progress.skipped.addAndGet(work.busy.toLong())
        live.set(progress)
        phases.beginCycle(stream.name, StreamPhases.DYNAMIC, progress.tally)
        // The walks of the PREVIOUS pass, which are this pass's noise. They are
        // retained past their own end on purpose — that is what stops the
        // percentage running backwards as relays finish — so the pass boundary
        // is the one place they can be cleared. `reset` deletes only FINISHED
        // walks, which is also what makes it safe here: a straggler's walk is
        // live and stays.
        paging.reset(stream.name)

        val window = stream.filter
        refreshIdSet(stream, relays, window, idSet)
        val sharedAuthors = sharedAuthors(stream, relays)

        System.err.println(
            "router: ${stream.name} pass ${progress.number} — ${work.relays.size} relay(s) to hand out from [$sourceNames]" +
                (if (work.busy > 0) ", ${work.busy} still syncing from an earlier pass" else "") +
                " against ${idSet.size()} local id(s)" +
                " (e.g. ${work.relays.take(3).joinToString { it.url.url }})",
        )

        for (relay in work.relays) {
            if (!scope.isActive) break
            // ACQUIRED BY THE WALK, not inside the worker. That is what makes
            // this a pool of [RelayDiscoveryConfig.concurrency] workers pulling
            // from a queue rather than 16,752 coroutines contending for a
            // permit: the walk suspends here until a slot frees, so the number
            // of live workers is the pool size and the queue is the rest of the
            // list, where it costs nothing.
            pool.acquire()
            // The claim comes AFTER the slot, so the busy set is relays being
            // synced rather than relays plus whichever one is queued behind a
            // full pool — that set is published as `running`, and a number that
            // counts something not on a socket is the kind of small lie this
            // report exists to stop telling.
            //
            // [beginPass] read the same set at the top of the walk, so this
            // cannot lose a race within a pass: each url appears once. It is
            // the authoritative claim all the same.
            if (!rotation.take(relay.url)) {
                pool.release()
                progress.tally.busy.incrementAndGet()
                progress.skipped.incrementAndGet()
                progress.done.incrementAndGet()
                continue
            }
            scope.launch {
                try {
                    syncOne(stream, relay, window, idSet, sharedAuthors, progress)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // These are launched into the ENGINE's scope now rather
                    // than a per-pass `coroutineScope`, so an exception that
                    // escaped would cancel the scope and take every stream, the
                    // ingest pipeline and the live tails with it. It used to be
                    // caught one level up, by the loop's own handler, and lose
                    // only the cycle.
                    System.err.println(
                        "router: ${stream.name} ${relay.url.url} — worker failed: ${e.javaClass.simpleName}: ${e.message?.take(80)}",
                    )
                    progress.failed.incrementAndGet()
                    progress.tally.transferFailed.incrementAndGet()
                } finally {
                    // Exactly once per url handed out, on every path, which is
                    // what keeps `done` a count of urls rather than of outcomes.
                    progress.done.incrementAndGet()
                    rotation.release(relay.url)
                    pool.release()
                }
            }
        }
        progress.walkEndedMs = System.currentTimeMillis()

        val topReasons =
            progress.reasons.entries
                .sortedByDescending { it.value }
                .take(3)
                .joinToString { "${it.key} x${it.value}" }
        // One write for the walk, not one per relay. The stragglers' bands are
        // written by the next flush, which is what the flush thread is for.
        bands.flush()
        val elapsedMs = System.currentTimeMillis() - progress.startedMs
        val running = rotation.busyCount()
        System.err.println(
            "router: ${stream.name} pass ${progress.number} handed out ${work.relays.size} relay(s)" +
                " in ${fmtDuration(elapsedMs)}" +
                // "so far", because the walk ending is not the work ending —
                // every straggler is still adding to this number.
                "; ${progress.downloaded.get()} event(s) so far" +
                (if (elapsedMs >= 1_000 && progress.downloaded.get() > 0) " (${progress.downloaded.get() * 1000 / elapsedMs}/s)" else "") +
                (if (running > 0) "; $running still syncing" else "") +
                "; ${progress.strikes.summary(relays.size)}" +
                (if (topReasons.isNotEmpty()) "; unreachable: $topReasons" else "") +
                (
                    if (progress.torless.get() > 0) {
                        "; ${progress.torless.get()} skipped — tor SOCKS ${tor?.settings?.socksAddress} not answering," +
                            " nothing published about them"
                    } else {
                        ""
                    }
                ) +
                "; next pass in ${dynamic.nextCycleSeconds}s",
        )
        // The WALK completed. `pending` is what says how many of its urls are
        // still in flight, which on a rotation is the ordinary state at this
        // point rather than a sign the pass was killed — see [CycleTally].
        phases.endCycle(stream.name, StreamPhases.DYNAMIC, "completed")
    }

    /**
     * One relay's turn: the guards that can decline it without dialling, then
     * the sync.
     *
     * Order matters and is not the order the cost suggests. [HostStrikes] is
     * asked FIRST because it is free and because the wait for a pool slot is
     * exactly when a sibling url strikes an authority out — a check made before
     * that wait would dial a host already known dead. The transport question is
     * next (ours, not theirs), then the TCP pre-probe, which is the only guard
     * that costs a round trip.
     */
    private suspend fun syncOne(
        stream: SyncStream,
        relay: DiscoveredRelay,
        window: Filter,
        idSet: SharedIdSet,
        sharedAuthors: Set<String>,
        p: PassProgress,
    ) {
        // WHY it is being skipped, not just that it is. The two reasons carry
        // opposite retry policies — a struck-out authority is dialled again on
        // the next pass, a known-dead url waits out a signed record's TTL — and
        // one counter for both is unreadable in exactly the way "skipped as
        // dead" was.
        val skip = p.strikes.whyDead(relay.url)
        if (skip != null) {
            when (skip) {
                HostStrikes.Skip.KNOWN_DEAD -> {
                    p.reasons.merge("skipped: unreachable in an earlier run, record still current", 1L, Long::plus)
                    p.tally.knownDead.incrementAndGet()
                }

                HostStrikes.Skip.STRUCK_OUT -> {
                    p.reasons.merge("skipped: authority already struck out this pass", 1L, Long::plus)
                    p.tally.hostStruckOut.incrementAndGet()
                }
            }
            p.skipped.incrementAndGet()
            return
        }
        // Our own transport, before anything is said about theirs. A Tor that is
        // down, restarting or renamed fails every dial it carries in a way that
        // reads exactly like the relay being gone — so ask our SOCKS port
        // instead, and skip rather than dial.
        //
        // Per relay, not once per pass: the answer is cached for
        // [TorSettings.PROBE_TTL_MS], so this costs one connect per 30s, and a
        // Tor that comes back is picked up inside the running pass.
        if (tor?.routes(relay.url) == true && !tor.socksAnswers()) {
            p.torless.incrementAndGet()
            p.tally.torUnavailable.incrementAndGet()
            p.skipped.incrementAndGet()
            return
        }
        // A TCP connect before the websocket handshake: a refused connection or
        // unresolvable host answers in milliseconds, where each of ~20k corpses
        // would otherwise cost a full connect timeout.
        if (!tcpReachable(relay.url)) {
            p.reasons.merge("tcp: no route or refused", 1L, Long::plus)
            p.tally.noRoute.incrementAndGet()
            p.skipped.incrementAndGet()
            publishStrike(p.strikes, relay.url)
            return
        }
        // Leased, not passed: the set this ask reconciles against must stay
        // alive for as long as the ask does, and a pass that ends while this
        // one runs may install a newer one. Released in the `finally` — a lease
        // left open pins its generation and stops every future rebuild.
        val lease = idSet.lease()
        val got =
            try {
                // The relay's own filter, narrowed by what the tags that named
                // it paired it with; identical to `window` for a select that
                // binds only the url.
                syncRelay(stream, relay.url, relay.narrowed(window), lease.ids, sharedAuthors) { reason ->
                    p.reasons.merge(reason, 1L, Long::plus)
                }
            } finally {
                lease.release()
            }
        when {
            // Could not reach it: strike and publish, the finding NIP-66 exists
            // for.
            got == UNREACHABLE -> {
                p.failed.incrementAndGet()
                p.tally.unreachable.incrementAndGet()
                publishStrike(p.strikes, relay.url)
            }

            // Reached it; the transfer broke. NOT struck and NOT published: the
            // relay answered our handshake, so calling it unreachable would be a
            // false statement about someone else's server.
            got == TRANSFER_FAILED -> {
                p.failed.incrementAndGet()
                p.tally.transferFailed.incrementAndGet()
            }

            got > 0 -> {
                p.downloaded.addAndGet(got.toLong())
                p.tally.received.addAndGet(got.toLong())
                p.tally.delivered.incrementAndGet()
                p.strikes.produced(relay.url)
            }

            // Answered cleanly with nothing new — a working relay we are in sync
            // with.
            else -> {
                p.tally.nothingNew.incrementAndGet()
                p.strikes.produced(relay.url)
            }
        }
    }

    /**
     * Build this pass's shared local id set, or decide not to.
     *
     * Three reasons not to, and they are different questions. A stream that
     * never reads one ([holdsIdSet]); a pass with nothing outstanding anywhere,
     * where every leg check returns before the set is touched; and a previous
     * generation still being read by a straggler, which is [SharedIdSet]'s
     * bound — the third is the one a rotation introduced, and the answer is to
     * keep reconciling against the set we have rather than put a third
     * gigabyte-scale list on the heap.
     */
    private suspend fun refreshIdSet(
        stream: SyncStream,
        relays: List<DiscoveredRelay>,
        window: Filter,
        idSet: SharedIdSet,
    ) {
        if (!holdsIdSet(stream)) {
            System.err.println(
                if (stream.sync == SyncMode.FETCH) {
                    // A fetch-only stream never reads the id set, and building
                    // one is the most expensive thing this router does (24.8M
                    // ids and gigabytes held live, measured).
                    "router: ${stream.name} sync=fetch — no local id set needed, skipping the snapshot"
                } else {
                    // [DeleteMissingSync] reads its OWN ids per ask, and must:
                    // the shared snapshot spans every service on the stream, and
                    // handing it to a one-service reconcile would report every
                    // other service's records as retracted.
                    "router: ${stream.name} deleteMissing — ids are read per ask, skipping the shared snapshot"
                },
            )
            return
        }
        if (!bands.anyOutstanding(stream.name, relays.map { it.url }, window)) {
            // Nothing outside any relay's band, so every syncOne below returns
            // at its own leg check without ever reading the id set. Distinct
            // from holdsIdSet above, which asks whether this STREAM ever needs
            // one; this asks whether it needs one THIS pass. coveringWindow
            // cannot save it — with nothing outstanding there is no window to
            // narrow to, and it correctly hands back the whole filter. Asking
            // first is where the saving is.
            System.err.println("router: ${stream.name} — all ${relays.size} relay(s) already cover the filter, skipping the snapshot")
            return
        }
        if (!idSet.mayInstall()) {
            // A straggler is still reading the generation this would replace.
            // Building anyway is unbounded — a hung leg holding one for hours
            // while pass after pass installs another is the heap ceiling with
            // extra steps — so the pass reuses what it has. The cost is a diff
            // computed against a slightly old set, i.e. some events arriving
            // that ingest then drops as duplicates.
            System.err.println(
                "router: ${stream.name} — reusing the ${idSet.size()} id snapshot," +
                    " an earlier pass's relays are still reading the previous one",
            )
            return
        }
        // ONE snapshot for the whole pass: every relay reconciles the same
        // filter, so per-relay snapshots were hundreds of identical full store
        // scans. A relay synced late compares against the store as it was at the
        // start — already true anyway, since ingest is asynchronous — and the
        // store dedups on insert. Narrowed to what the hungriest relay still
        // needs ([SyncBands.coveringWindow]).
        val snapshotWindow = bands.coveringWindow(stream.name, relays.map { it.url }, window)
        val startedMs = System.currentTimeMillis()
        val expected = runCatching { store.count(snapshotWindow) }.getOrNull()
        phases.set(stream.name, StreamPhases.Phase.Snapshotting(0, expected, relays.size))
        // THE GATE IS ROUND THE BUILD, not round the pass. It used to wrap the
        // whole fan-out, which on a rotation would be forever — a stream that
        // never finishes never releases, and every other id-set stream and the
        // static backfill would queue behind it for the life of the process.
        // What it still buys is the expensive half: two full store walks are
        // never in flight at once. What it no longer bounds is RESIDENCY, which
        // is [SharedIdSet]'s job now.
        val ids =
            streamGate.withPermit {
                store.snapshotIdsReporting(snapshotWindow) { collected ->
                    phases.set(stream.name, StreamPhases.Phase.Snapshotting(collected, expected, relays.size))
                }
            }
        idSet.install(ids)
        System.err.println(
            "router: ${stream.name} local snapshot ${ids.size} id(s) in ${fmtDuration(System.currentTimeMillis() - startedMs)}" +
                (snapshotWindow.since?.let { s -> ", since $s" } ?: ", full filter (no relay is caught up yet)"),
        )
    }

    /**
     * Authors this pass found at MORE THAN ONE relay.
     *
     * Deletion reads one relay's silence as a retraction, and that only holds
     * while it is the author's sole upstream — measured, 3 of 266 NIP-85
     * services are bound to several relays and two of those name general relays
     * that will never serve their scores, so an empty answer there is a wrong
     * pointer rather than a withdrawal. They still get mirrored.
     */
    private fun sharedAuthors(
        stream: SyncStream,
        relays: List<DiscoveredRelay>,
    ): Set<String> {
        if (stream.deleteMissing == DeleteMissing.OFF) return emptySet()
        return relays
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
        // Invariant for the whole relay: hoisted out of the per-event callback
        // below, which ran on every mirrored event and allocated an identical
        // object each time.
        val origin = originFor(stream, url)
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
                    ingest.submit(event, stream.trusted, origin)
                }
            }
            // Fetch-only: the leg came off the band, so this asks only
            // for what is outside what we already walked — the band IS the
            // mechanism here, there is no id set to fall back on.
            val fetched = stream.sync == SyncMode.FETCH
            // Set only on the fetch branch: the negentropy path below runs through
            // `negentropySyncOrFetch`, which does not surface how its paging ended.
            var walked: PagedFetchResult? = null
            val result =
                if (fetched) {
                    null.also {
                        val walk = "${stream.name}|${url.url}"
                        // Floored on the PAGED branch only: a walk that runs past
                        // `created_at = 0` never returns ([flooredForPaging]), while
                        // narrowing the negentropy branch's leg the same way would
                        // leave the local id set wider than the remote one.
                        val flooredLeg = leg.flooredForPaging()
                        paging.begin(walk, flooredLeg.until ?: nowSeconds(), flooredLeg.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
                        // finally, because `syncRelay` catches Exception around
                        // this: a throw between begin and finish leaves the walk
                        // in PagingProgress with `current` still at `top`, and
                        // `fraction` AVERAGES over every live walk, so one
                        // orphan reads as a relay stuck at 0% and drags the
                        // stream's percentage and ETA down. `begin` overwrites
                        // by key, so it is one stale entry per stream|url rather
                        // than a growing leak — but it never clears on its own,
                        // and a relay that stops being walked keeps it forever.
                        // `DeleteMissingSync.pageAsk` already does this.
                        //
                        // `walked` is assigned INSIDE the try, not from the
                        // expression's value: `finally` runs before that
                        // assignment would, so reading the result out here is
                        // the difference between telling PagingProgress the walk
                        // drained and telling it nothing ever does.
                        try {
                            walked =
                                client.fetchAllPages(
                                    url,
                                    listOf(flooredLeg),
                                    NEG_IDLE_MS,
                                    onNewPage = { until -> paging.mark(walk, until) },
                                    onEvent = onEvent,
                                )
                        } finally {
                            // Still null on the throw path, which is exactly the
                            // case that must not claim cover.
                            paging.finish(walk, covered = walked?.drained == true)
                        }
                        downloaded += walked?.downloaded ?: 0
                    }
                } else {
                    client
                        .negentropySyncOrFetch(
                            relay = url,
                            filter = leg,
                            idleTimeoutMs = NEG_IDLE_MS,
                            localEntries = local,
                            // Declined before the REQ; null when suppression is
                            // off, so quartz keeps its uncopied fast path.
                            wantId = wantIdFor(leg),
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
                // Against `window`, which is what the band is keyed by — the
                // same filter [legs] derived `leg` from.
                drained = drainSettlesThePast(walked, leg, window),
            )
        }
        // The socket is still open here and it will not be after this returns:
        // these streams hold no live tail. Draining now keeps the connection
        // the repairs need without the sweep ever waiting on a publish.
        healer.drain(url)
        return downloaded
    }

    /**
     * The suppression predicate for one window, or null when it is off.
     *
     * Same limit quartz documents: it does NOT cover a window that falls back
     * to paging, because a REQ names no ids before it streams bodies. Those
     * arrive and are dropped on the ingest side instead.
     */
    private fun wantIdFor(window: Filter): ((String) -> Boolean)? =
        if (!refusedIds.enabled) {
            null
        } else {
            { id -> !refusedIds.suppressedInWindow(id, window.since, window.until) }
        }

    /**
     * What this stream lets the healer do about a relay serving a stale copy.
     * Resolved here rather than in the pipeline because the switches are
     * per-stream and the pipeline is shared.
     */
    private fun originFor(
        stream: SyncStream,
        url: NormalizedRelayUrl,
    ) = IngestOrigin(url, healContent = stream.healContent, healRetractions = stream.healRetractions)

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
        // Guarded, because every caller has ALREADY recorded this url's outcome
        // in the tally by the time it gets here. An exception escaping would be
        // caught by the worker's handler and counted a second time, as
        // `transferFailed`, and the partition would stop closing — a publish
        // failing is not worth a document that says its own numbers are wrong.
        runCatching {
            monitor?.observer?.record(
                url,
                reachable = false,
                error = "host ${evicted.authority} silent after ${evicted.strikes} attempts",
            )
        }
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
