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
package com.nosfabrica.vespa.relay.router.presence

import com.nosfabrica.vespa.relay.router.IngestPipeline
import com.nosfabrica.vespa.relay.router.NEG_IDLE_MS
import com.nosfabrica.vespa.relay.router.SyncBands
import com.nosfabrica.vespa.relay.router.config.SyncMode
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.drainSettlesThePast
import com.nosfabrica.vespa.relay.router.flooredForPaging
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.progress.StreamPhases
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * MIRRORING FOR THE PEOPLE WHO ARE ACTUALLY HERE.
 *
 * A `presence` stream holds a live REQ on every relay that a currently
 * signed-in reader's own lists name, and drops it when they go. `DynamicSync`
 * is the same idea aimed at a corpus — discover relays from stored events, walk
 * them on a period — and the difference is not scale but WHAT SETS THE CLOCK:
 * there, a refresh interval; here, somebody signing in. That is the whole
 * feature. A reader whose provider this relay has never mirrored gets an empty
 * ranked search and a `TrustNotice` explaining it, and before this the fix was
 * to wait out a six-hour discovery cycle that had no idea they were waiting.
 *
 * ## The subscription set is a function, and this class recomputes it
 *
 * Every [PresenceConfig.pollSeconds] the loop derives what SHOULD be open from
 * (who is signed in) × (what their own lists say), and moves the open set to
 * match — opening what is new, closing what no longer has a reason. Two
 * properties of that shape are worth stating because both were reached by
 * ruling out the alternative:
 *
 *  - **Subscriptions are keyed by (relay, question), not by reader.** Two
 *    readers naming one provider relay share one REQ; four hundred readers
 *    whose outboxes all include `wss://nos.lol` still put ONE filter on it.
 *    Keying by reader is the obvious shape and it multiplies REQs by
 *    readership on exactly the relays everybody names.
 *  - **The targets of readers already here are recomputed too**, not only those
 *    of arrivals. A 10002 and a 10040 are replaceable events that change while
 *    their author is online — and the reader most likely to edit theirs is the
 *    one who just read a notice telling them their trust chain is not mirrored.
 *    Diffing only the reader set would leave that edit unnoticed until they
 *    signed out and back in.
 *
 * ## What it does NOT do
 *
 * It never reconciles. [SyncMode.LIVE] is the tail alone; [SyncMode.FETCH] adds
 * one paged catch-up per (relay, question), which is what makes a returning
 * reader's backlog arrive rather than only their next post. Negentropy is
 * refused by the config loader, because our side of a per-reader filter is a
 * store walk per reader per poll.
 *
 * It also never writes a band for the TAIL — see [SyncMode.LIVE]. A tail proves
 * nothing about the span it was open for, and a band claiming otherwise would
 * let a later `fetch` skip history nobody walked.
 */
internal class PresenceSync(
    private val client: NostrClient,
    private val store: IEventStore,
    private val feed: AuthedFeed,
    private val bands: SyncBands,
    private val ingest: IngestPipeline,
    private val phases: StreamPhases,
    private val paging: PagingProgress,
    private val transferring: AtomicInteger,
    private val scope: CoroutineScope,
) {
    /** One open subscription: what it asks, and the id it was opened under. */
    private class Held(
        val subId: String,
        val target: PresenceTarget,
    )

    /** Stream name → the subscriptions it currently holds, by [PresenceTarget.key]. */
    private val held = ConcurrentHashMap<String, MutableMap<String, Held>>()

    /**
     * The (stream, relay, question) triples whose history has already been
     * walked THIS PROCESS, so a reader signing in and out repeatedly does not
     * re-enter the catch-up queue every time.
     *
     * In memory and deliberately not persisted: the durable half is
     * [SyncBands], which is what actually stops a second walk re-downloading —
     * this only stops the walk being STARTED, which is worth doing because
     * starting it costs a slot and a socket even when the band ends it in one
     * empty page. A restart re-checks each pair once and the bands answer
     * cheaply.
     *
     * Holds the SUBSCRIPTION ID rather than the target's own key, for two
     * reasons that happen to have one answer. It is scoped per stream, where
     * the raw key is not — two presence streams that resolved to the same
     * (relay, question) would otherwise have the first one's walk suppress the
     * second's, on a band the second stream does not own. And it is 23 bytes
     * against a key that carries a whole url and a whole filter: this set only
     * ever grows over a process's life, once per distinct pair ever seen, and a
     * busy relay's readership times its relays is six figures.
     */
    private val caughtUp = ConcurrentHashMap.newKeySet<String>()

    /** Events this stream's tails and catch-ups have taken, for the phase line. */
    private val events = ConcurrentHashMap<String, AtomicLong>()

    /** Readers whose own list could not be read at all this pass, per stream. */
    private val unreadable = ConcurrentHashMap<String, AtomicInteger>()

    /**
     * Run one presence stream for the life of the process.
     *
     * Every pass is wrapped: this loop is launched into the engine's scope, and
     * an escaping exception there cancels every stream, the ingest pipeline and
     * the live tails with it — the failure mode `DynamicSync`'s workers are
     * wrapped for. A pass that throws costs one interval.
     */
    suspend fun loop(stream: SyncStream) {
        val presence = requireNotNull(stream.presence) { "PresenceSync.loop needs a stream with a presence block" }
        val intervalMs = presence.pollSeconds * 1_000
        held[stream.name] = ConcurrentHashMap()
        events[stream.name] = AtomicLong()
        unreadable[stream.name] = AtomicInteger()
        System.err.println(
            "router: ${stream.name} follows signed-in readers (${presence.source.wire}), " +
                "reconciling every ${presence.pollSeconds}s, ${if (stream.sync == SyncMode.LIVE) "tail only" else "tail + catch-up"}",
        )
        while (scope.isActive) {
            try {
                pass(stream)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: ${stream.name} presence pass failed (${e.message}) — retrying in ${presence.pollSeconds}s")
                phases.set(stream.name, StreamPhases.Phase.Failed(e.message ?: e.javaClass.simpleName, presence.pollSeconds))
            }
            delay(intervalMs)
        }
    }

    /**
     * One reconcile of the open subscriptions against who is here.
     *
     * Ordered close-then-open so a reader leaving frees their sockets before an
     * arrival's are dialled, which keeps the socket ceiling honest on a busy
     * relay: the alternative peaks at the union of both sets for as long as the
     * opens take.
     */
    private suspend fun pass(stream: SyncStream) {
        val presence = stream.presence!!
        val readers = feed.readers()
        val name = stream.name
        val open = held.getValue(name)
        val failures = unreadable.getValue(name).also { it.set(0) }

        if (readers.isEmpty()) {
            // Two different silences, said apart. A feed that has never spoken
            // is a configuration this operator has to fix; an empty one is a
            // relay nobody is signed in to, which is most of the night.
            closeAll(name, open)
            phases.set(
                name,
                StreamPhases.Phase.Waiting(
                    if (feed.everFed()) "nobody signed in" else "the presence feed has not answered yet",
                    presence.pollSeconds,
                ),
            )
            return
        }

        val desired = LinkedHashMap<String, PresenceTarget>()
        for (reader in readers) {
            val list =
                try {
                    store.query<Event>(PresenceTargets.listFilter(presence.source, reader)).firstOrNull()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // A failed read is NOT "they have no list". Counting it
                    // separately is what keeps a sick store from reading as a
                    // readership that publishes nothing — the same distinction
                    // TrustNotice draws between null and empty.
                    failures.incrementAndGet()
                    null
                } ?: continue
            for (target in PresenceTargets.of(presence.source, list, stream.filter, presence)) {
                desired[target.key] = target
            }
        }

        val gone = open.keys - desired.keys
        for (key in gone) open.remove(key)?.let { close(name, it) }

        var opened = 0
        for ((key, target) in desired) {
            if (open.containsKey(key)) continue
            val subId = subIdFor(name, key)
            // Opened BEFORE it is recorded as held. The other order looks
            // tidier and has a hole: a subscribe that threw would leave the key
            // in `open`, so every later pass would skip it as already held and
            // that reader's relay would never be dialled again — with the
            // stream's own counters saying it was.
            subscribe(stream, subId, target)
            open[key] = Held(subId, target)
            opened++
            if (stream.sync != SyncMode.LIVE && caughtUp.add(subId)) {
                scope.launch { catchUp(stream, subId, target, presence.concurrency) }
            }
        }
        // Only when something opened: `connect()` walks the pool, and calling
        // it on a pass that changed nothing is work on every tick for a set
        // that is already dialled.
        if (opened > 0) client.connect()

        phases.set(
            name,
            StreamPhases.Phase.Syncing(
                done = readers.size - failures.get(),
                total = readers.size,
                events = events.getValue(name).get(),
                skipped = failures.get().toLong(),
                unreachable = 0,
                running = open.size,
                transferring = open.values.count { it.target.url in client.connectedRelaysFlow().value },
            ),
        )
        if (gone.isNotEmpty() || opened > 0) {
            System.err.println(
                "router: $name ${readers.size} reader(s) signed in → ${open.size} subscription(s) " +
                    "(+$opened, -${gone.size})" +
                    (if (feed.omitted > 0) "; ${feed.omitted} reader(s) OMITTED by the feed's own cap" else "") +
                    (if (failures.get() > 0) "; ${failures.get()} reader(s) whose list could not be read" else ""),
            )
        }
    }

    /**
     * Open the tail.
     *
     * `since = now` for the same reason `SyncEngine`'s static tails use it:
     * history is the catch-up's job, and a tail that replays a relay's whole
     * matching corpus on every reconnect would make a flapping socket the most
     * expensive thing in this process. A `limit` the operator wrote is left
     * alone — `limit = 0` is NIP-01's own "no stored events, just the tail",
     * which is exactly what a `live` stream is asking for anyway.
     */
    private fun subscribe(
        stream: SyncStream,
        subId: String,
        target: PresenceTarget,
    ) {
        client.subscribe(
            subId = subId,
            filters = mapOf(target.url to listOf(target.filter.copy(since = nowSeconds()))),
            listener = listenerFor(stream, target),
        )
    }

    private fun close(
        stream: String,
        subscription: Held,
    ) {
        runCatching { client.unsubscribe(subscription.subId) }
            .onFailure { System.err.println("router: $stream could not close ${subscription.target.url.url}: ${it.message}") }
    }

    private fun closeAll(
        stream: String,
        open: MutableMap<String, Held>,
    ) {
        if (open.isEmpty()) return
        val closing = open.values.toList()
        open.clear()
        closing.forEach { close(stream, it) }
        System.err.println("router: $stream closed ${closing.size} subscription(s) — no signed-in reader wants them")
    }

    /**
     * The tail's listener, bound to the one (relay, question) it was opened for.
     *
     * Both re-checks are the ones `SyncEngine.downListener` makes and for the
     * same reasons: quartz delivers by subscription id, and a relay that serves
     * the wrong url or answers wider than it was asked must not be able to
     * widen what this mirror ingests.
     */
    private fun listenerFor(
        stream: SyncStream,
        target: PresenceTarget,
    ): SubscriptionListener {
        val counter = events.getValue(stream.name)
        val origin = IngestOrigin(target.url, stream.healContent, stream.healRetractions)
        return object : SubscriptionListener {
            override suspend fun onEvent(
                event: Event,
                isLive: Boolean,
                relay: NormalizedRelayUrl,
                forFilters: List<Filter>?,
            ) {
                if (relay != target.url) return
                if (!target.filter.match(event)) return
                counter.incrementAndGet()
                ingest.submit(event, stream.trusted, origin)
            }

            override fun onCannotConnect(
                relay: NormalizedRelayUrl,
                message: String,
                forFilters: List<Filter>?,
            ) {
                // One line and no retry of our own: quartz's client reconnects
                // this subscription on its own keep-alive, and the pass above
                // re-derives the whole set every interval anyway. A relay that
                // stays unreachable costs a log line per reconnect, not a
                // subscription that silently stops existing.
                System.err.println("router: ${stream.name} cannot connect ${target.url.url}: $message")
            }
        }
    }

    /**
     * Walk one (relay, question) pair's history once, guarded by its band.
     *
     * A near-copy of `StaticBackfill.pageOne`'s inner walk rather than a call
     * into it, and the difference is the reason: that class owns a
     * `BackfillProgress` sized at boot from a fixed upstream list and a
     * `CycleTally` per stream, both of which assume the set of relays is known
     * before anything starts. Here it is a function of who is signed in and
     * changes all day. What is shared is the part that must not fork — the
     * floor, the per-kind spans, the drain rule — and each of those is the
     * common helper rather than a second copy.
     */
    private suspend fun catchUp(
        stream: SyncStream,
        subId: String,
        target: PresenceTarget,
        concurrency: Int,
    ) {
        val gate = gates.computeIfAbsent(stream.name) { Semaphore(concurrency) }
        val counter = events.getValue(stream.name)
        val origin = IngestOrigin(target.url, stream.healContent, stream.healRetractions)
        try {
            gate.withPermit {
                val legs = bands.legs(stream.name, target.url, target.filter)
                if (legs.isEmpty()) return@withPermit
                transferring.incrementAndGet()
                try {
                    for (leg in legs) {
                        // Floored, or a relay holding an event stamped
                        // `created_at = 0` drives this walk past zero and it
                        // never returns — see [flooredForPaging].
                        val window = leg.flooredForPaging()
                        var seenMin: Long? = null
                        var seenMax: Long? = null
                        val seenByKind = mutableMapOf<Int, SyncCoverage.Span>()
                        val walk = PagingProgress.Walked(stream.name, target.url.url)
                        val cursor = paging.begin(walk, window.until ?: nowSeconds(), window.since ?: SyncCoverage.PLAUSIBLE_FLOOR)
                        var walked: PagedFetchResult? = null
                        try {
                            walked =
                                client.fetchAllPages(
                                    target.url,
                                    listOf(window),
                                    NEG_IDLE_MS,
                                    onNewPage = { until -> cursor?.reached(until) },
                                ) { event ->
                                    if (target.filter.match(event)) {
                                        if (SyncCoverage.isPlausible(event.createdAt)) {
                                            seenMin = minOf(seenMin ?: event.createdAt, event.createdAt)
                                            seenMax = maxOf(seenMax ?: event.createdAt, event.createdAt)
                                            cursor?.reached(event.createdAt)
                                        }
                                        SyncCoverage.observe(seenByKind, event.kind, event.createdAt)
                                        counter.incrementAndGet()
                                        ingest.submit(event, stream.trusted, origin)
                                    }
                                }
                        } finally {
                            // In a finally for the same reason the other paged
                            // call sites do it: a throw between begin and finish
                            // strands this walk at 0% and `fraction` averages it
                            // into the stream's number forever.
                            paging.finish(walk, covered = walked?.drained == true)
                        }
                        bands.record(
                            stream.name,
                            target.url,
                            target.filter,
                            seenMin,
                            seenMax,
                            paged = true,
                            observedByKind = seenByKind,
                            drained = drainSettlesThePast(walked, window, target.filter),
                        )
                    }
                } finally {
                    transferring.decrementAndGet()
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Dropped from [caughtUp] so a later pass may try again: this pair
            // came back for a reader who is probably still here, and a failure
            // that permanently retires the walk would leave their backlog
            // missing with nothing anywhere saying why.
            caughtUp.remove(subId)
            System.err.println("router: ${stream.name} catch-up on ${target.url.url} failed: ${e.message}")
        }
    }

    /** One catch-up gate per stream, built on first use from that stream's own concurrency. */
    private val gates = ConcurrentHashMap<String, Semaphore>()

    /** Live gauges for the `presence` processor row — see `Processors`. */
    fun readersFollowed(): Long = feed.readers().size.toLong()

    fun subscriptions(): Long = held.values.sumOf { it.size }.toLong()

    fun relays(): Long =
        held.values
            .flatMap { it.values }
            .mapTo(HashSet()) { it.target.url }
            .size
            .toLong()

    /** Readers the relay's own feed could not fit in its response — nobody is mirroring for them. */
    fun omittedReaders(): Long = feed.omitted.toLong()

    companion object {
        /**
         * A subscription id that is stable, short, and unique per (stream,
         * relay, question).
         *
         * Hashed rather than spelled out because the natural id — the stream
         * name, the url and the filter — is hundreds of characters, and NIP-01
         * caps a subscription id at 64 with relays enforcing it at their own
         * lengths. Stable across passes so a re-derived set that has not changed
         * re-opens nothing, and prefixed so a wire log tells these apart from
         * the mirror's static tails at a glance.
         */
        internal fun subIdFor(
            stream: String,
            key: String,
        ): String {
            val digest =
                MessageDigest
                    .getInstance("SHA-256")
                    .digest("$stream $key".toByteArray(Charsets.UTF_8))
            return "vespa-presence-" + digest.take(8).joinToString("") { "%02x".format(it) }
        }
    }
}
