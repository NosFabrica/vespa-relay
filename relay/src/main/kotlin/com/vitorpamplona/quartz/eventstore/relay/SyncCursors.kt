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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap

/**
 * How much of a filter's history we have already pulled from one relay, so a
 * restart does not pull it again.
 *
 * ## The problem this solves
 *
 * Negentropy relays need none of this: reconciliation compares id sets and
 * downloads only the difference, so re-running a sync costs the diff and nothing
 * more. Most relays do not speak NIP-77 — in one measured run, seven of nine
 * upstreams fell back to paged REQ — and a paged fetch has no such memory. It
 * walks `created_at` newest-first and re-downloads everything it walked last
 * time, every restart, forever.
 *
 * So for those relays we remember the band of `created_at` we have covered, per
 * relay and per filter, and on the next run ask only for what lies outside it:
 *
 *     stored band:        |<-------- covered -------->|
 *     next fetch:  <------|                           |------>
 *                  older than min                newer than max
 *
 * The newer leg catches what was published while we were away. The older leg
 * keeps walking back into history — which is what makes progress against a relay
 * that caps a response: each run reaches a little further, instead of re-reading
 * the same newest N events forever.
 *
 * ## Keyed by filter, deliberately
 *
 * A band only means "covered" with respect to the filter that produced it. Widen
 * the kinds, add an author, change the `since`, and the events the old band
 * skipped were never fetched at all. So the key includes the filter, and any
 * edit to it is a new key with no band — the next run starts over, which is the
 * safe direction to be wrong in.
 *
 * ## What this does NOT guarantee
 *
 * That the covered band is complete. Two ways it can have holes:
 *
 *  - a relay that truncates a page leaves a gap we record as covered;
 *  - Nostr lets an event be published with any `created_at`, so one can land
 *    inside a band we already walked past.
 *
 * The trade is deliberate: re-reading a corpus on every restart is a certain,
 * daily cost, while both holes are occasional and self-heal the next time the
 * filter changes. A relay that speaks NIP-77 has neither problem, which is why
 * only the paged path is tracked at all — see [record].
 */
class SyncCursors(
    private val file: File?,
    // How long a band may narrow work before the whole filter is walked again.
    // Not a tuning knob so much as an honesty interval: everything a band claims
    // is a claim about the past, and this is how long we are willing to trust it
    // without re-testing. See [Band.fullAt].
    private val fullResyncSeconds: Long = DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    /**
     * What is already covered for one (relay, filter) pair.
     *
     * [complete] is the difference between "we walked this span" and "we are in
     * sync below this point". A paged fetch only earns the first: it saw the
     * events it saw, and says nothing about the ones it never asked for. A
     * negentropy reconcile earns the second — it compares the WHOLE filter range
     * against our ids, so when it finishes there is nothing older left to want,
     * whatever it happened to download. Only a complete band may skip its older
     * leg, which is what turns the next run's snapshot from the whole corpus
     * into the sliver since [maxCreatedAt].
     *
     * [fullAt] is when the last pass that started from nothing finished — the
     * clock for the periodic re-walk. A band narrows work indefinitely otherwise,
     * and a relay that later gains an event below our floor (or one we dropped)
     * would be invisible for as long as the filter stays the same.
     */
    data class Band(
        val minCreatedAt: Long,
        val maxCreatedAt: Long,
        val complete: Boolean = false,
        val fullAt: Long = 0,
    )

    private val bands = ConcurrentHashMap<String, Band>()

    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    // filter -> its canonical json. Filter.toJson() walks every field, and an
    // author-scoped filter runs to tens of thousands of characters (67k for a
    // thousand authors) — while the dynamic streams key once per relay per cycle
    // over the SAME handful of filter objects. Identity is the right equality
    // here: the router hands the same instance down the whole fan-out, and a
    // filter that is merely equal still keys correctly, just without the cache.
    private val fingerprints = Collections.synchronizedMap(IdentityHashMap<Filter, String>())

    init {
        load()
    }

    /**
     * The filters to actually run now, given what is already covered.
     *
     * One filter when nothing is recorded — the whole thing. Otherwise the two
     * legs outside the band, each clamped to the caller's own `since`/`until` so
     * a bounded filter never widens: a leg that would reach past the configured
     * edge is dropped rather than trimmed to nothing, and when both are dropped
     * the band already covers everything asked for and the result is empty.
     *
     * ## The boundary seconds are re-read on purpose
     *
     * The legs are INCLUSIVE of the band's own edges — `until = min`, not
     * `min - 1`. A paged relay cuts its pages by count, so a page boundary can
     * fall inside a run of events that share one `created_at`, leaving some of
     * that second fetched and the rest not. Excluding the edge would put those
     * stragglers in no leg at all while the band claimed their second was done —
     * unreachable for as long as the filter stays the same.
     *
     * [RelayDiscovery]'s paging walk hits the same hazard and answers it the same
     * way: "`until` is inclusive, so the next page re-sees them". The cost is
     * re-reading one second's worth of events per leg per run, which the store
     * rejects as duplicates.
     */
    fun legs(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> {
        val band = bands[key(url, filter)] ?: return listOf(filter)
        // Time for another full pass. Everything a band claims is a claim about
        // the past, and relays gain old events — a backfill from elsewhere, a
        // NIP-77 peer catching up. Without this the claim is never re-tested.
        if (isStale(band)) return listOf(filter)
        val legs = mutableListOf<Filter>()

        // Older: up to and including the band's floor, but not past the filter's.
        // A complete band has no older leg at all: the reconcile that produced it
        // already compared the whole range, so anything below the ceiling that we
        // do not have is something the relay does not have either.
        if (!band.complete && (filter.since == null || band.minCreatedAt >= filter.since!!)) {
            legs.add(filter.copy(until = minOf(band.minCreatedAt, filter.until ?: Long.MAX_VALUE)))
        }

        // Newer: from the band's ceiling on, but not past the filter's.
        if (filter.until == null || band.maxCreatedAt <= filter.until!!) {
            legs.add(filter.copy(since = maxOf(band.maxCreatedAt, filter.since ?: Long.MIN_VALUE)))
        }
        return legs
    }

    /**
     * Widen the band for (url, filter) to include what a completed fetch saw.
     *
     * [paged] gates the whole mechanism: a negentropy sync reconciles against our
     * own ids and has no need of a cursor, and recording one for it would only
     * risk narrowing a future reconciliation for no gain. Nothing is recorded for
     * a fetch that saw no events either — an empty result says nothing about what
     * the relay holds, only that this window was empty.
     */
    fun record(
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
    ) {
        // A finished reconcile is the strong case: it compared the filter's whole
        // range against our ids, so we are in sync up to the instant the sync
        // started — regardless of how many events came back, including none.
        // Recorded against that instant rather than the newest event seen, because
        // "the relay had nothing newer" and "we never asked" must not look alike.
        if (reconciledThrough != null) {
            put(url, filter, observedMin ?: reconciledThrough, reconciledThrough, complete = true)
            return
        }
        if (!paged) return
        // Callers widen the band only with plausible stamps (see [isPlausible]),
        // so an outlier never reaches here. Guarded anyway: a band is a claim
        // about coverage, and a 1970 floor or a 2027 ceiling makes that claim
        // over the whole timeline — the leg outside it then asks for a range
        // nothing can be in, forever.
        if (observedMin == null || observedMax == null) return
        if (!isPlausible(observedMin) || !isPlausible(observedMax)) return
        put(url, filter, observedMin, observedMax, complete = false)
        // Marked, not written. A dynamic stream records once per leg per relay
        // and every write serializes the whole map, so saving here would cost
        // O(relays²) per cycle — thousands of full-file rewrites to persist a few
        // thousand integers. [flush] does it once, and losing the last unflushed
        // window to a hard kill costs one partial re-fetch, not correctness.
        dirty = true
    }

    /**
     * Widen (or reset) the band for (url, filter).
     *
     * A pass that ran because the previous band had gone stale REPLACES it rather
     * than widening it — it re-walked the whole filter, so its own span is the
     * complete picture, and [Band.fullAt] restarts from here. Widening a stale
     * band instead would carry its claim forward forever and the re-walk would
     * never actually reset anything.
     */
    private fun put(
        url: NormalizedRelayUrl,
        filter: Filter,
        min: Long,
        max: Long,
        complete: Boolean,
    ) {
        val now = nowSeconds()
        bands.compute(key(url, filter)) { _, prev ->
            if (prev == null || isStale(prev)) {
                Band(min, max, complete, now)
            } else {
                Band(
                    minOf(prev.minCreatedAt, min),
                    maxOf(prev.maxCreatedAt, max),
                    prev.complete || complete,
                    prev.fullAt,
                )
            }
        }
        // Marked, not written. A dynamic stream records once per leg per relay
        // and every write serializes the whole map, so saving here would cost
        // O(relays²) per cycle — thousands of full-file rewrites to persist a few
        // thousand integers. [flush] does it once, and losing the last unflushed
        // window to a hard kill costs one partial re-fetch, not correctness.
        dirty = true
    }

    private fun isStale(band: Band): Boolean = nowSeconds() - band.fullAt >= fullResyncSeconds

    /**
     * Has this relay ever completed a negentropy reconcile for [filter]?
     *
     * False for one never synced and for one that paged, and those are the same
     * answer to the question that matters: it will not read the local id set, so
     * it need not wait for the walk that builds it.
     */
    fun everReconciled(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Boolean = bands[key(url, filter)]?.complete == true

    /**
     * The narrowest single filter that still covers what every one of [urls]
     * needs — the window a shared negentropy snapshot has to be taken over.
     *
     * A dynamic cycle takes ONE snapshot of our own ids for the whole fan-out,
     * so it cannot be narrowed per relay; it has to satisfy the hungriest. In
     * steady state every relay carries a complete band and this collapses to
     * `since = the oldest of their ceilings` — which is the difference between
     * snapshotting 24M ids and snapshotting a few thousand. One relay that has
     * never been synced (or whose band just went stale) puts it back to the full
     * filter, correctly: that relay genuinely needs everything.
     */
    fun coveringWindow(
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter {
        if (urls.isEmpty()) return filter
        var since = Long.MAX_VALUE
        for (url in urls) {
            val legs = legs(url, filter)
            // More than one leg means an older gap this relay still wants, so the
            // snapshot cannot start anywhere above the filter's own floor.
            val only = legs.singleOrNull() ?: return filter
            val legSince = only.since ?: return filter
            since = minOf(since, legSince)
        }
        return if (since == Long.MAX_VALUE) filter else filter.copy(since = since)
    }

    /**
     * Write the map every [intervalSec] on a daemon thread, so progress survives
     * a hard kill.
     *
     * The milestone flushes — a completed backfill, the end of a dynamic cycle —
     * are minutes to hours apart, and a SIGKILL in between loses every band the
     * run earned. That turns the next start into a full re-download, which is the
     * exact cost this class exists to avoid. One write per interval is nothing
     * next to that: the map is small, and unchanged intervals write nothing.
     */
    fun startPeriodicFlush(intervalSec: Long = DEFAULT_FLUSH_SECONDS): SyncCursors {
        if (file == null) return this
        flusher =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(intervalSec * 1000)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                    flush()
                }
            }.apply {
                isDaemon = true
                name = "sync-cursor-flush"
                start()
            }
        return this
    }

    /** Stop the periodic flush and write anything outstanding. */
    override fun close() {
        flusher?.interrupt()
        flush()
    }

    /** Write the map if anything changed since the last write. */
    @Synchronized
    fun flush() {
        if (!dirty) return
        dirty = false
        save()
    }

    /** What is currently covered, for logging and tests. */
    fun band(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Band? = bands[key(url, filter)]

    fun size(): Int = bands.size

    /**
     * The identity of one (relay, filter) pair.
     *
     * [Filter.toJson] is the canonical form the protocol itself uses, so two
     * filters that mean the same thing key the same way and any edit keys
     * differently — which is exactly the "config changed, start over" rule.
     * Serialized once per filter instance through [fingerprints], not once per
     * call — see the field for why.
     */
    private fun key(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): String = "${url.url} ${fingerprints.computeIfAbsent(filter) { it.toJson() }}"

    private fun load() {
        val f = file ?: return
        if (!f.isFile) return
        runCatching {
            Json.parseToJsonElement(f.readText()).jsonObject.forEach { (k, v) ->
                val o = v.jsonObject
                bands[k] =
                    Band(
                        o.getValue("min").jsonPrimitive.long,
                        o.getValue("max").jsonPrimitive.long,
                        // Absent in files written before coverage was tracked. A
                        // missing `complete` reads as "span only", and a missing
                        // `fullAt` as 0 — which is stale, so the first run after an
                        // upgrade re-walks once and records the real thing.
                        o["complete"]?.jsonPrimitive?.boolean ?: false,
                        o["fullAt"]?.jsonPrimitive?.long ?: 0L,
                    )
            }
        }.onFailure {
            // A corrupt cursor file is not worth refusing to start over: the cost
            // of losing it is one re-sync, and the cost of exiting is the relay.
            System.err.println("router: could not read sync cursors from ${f.path} (${it.message}); starting fresh")
        }
    }

    /**
     * Persist, via a temp file and an atomic move so a reader — or a restart
     * landing mid-write — never sees a half-written map.
     */
    @Synchronized
    private fun save() {
        val f = file ?: return
        runCatching {
            val snapshot: JsonObject =
                buildJsonObject {
                    bands.forEach { (k, band) ->
                        put(
                            k,
                            buildJsonObject {
                                put("min", band.minCreatedAt)
                                put("max", band.maxCreatedAt)
                                put("complete", band.complete)
                                put("fullAt", band.fullAt)
                            },
                        )
                    }
                }
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), snapshot))
            Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }.onFailure {
            System.err.println("router: could not write sync cursors to ${f.path}: ${it.message}")
        }
    }

    companion object {
        // Pretty-printed: this file is read by a human debugging why a relay
        // re-synced, and a one-line map of a thousand entries answers nothing.
        private val json = Json { prettyPrint = true }

        // Nostr's first events are from 2020; anything older is a misdated
        // stamp, not history we walked.

        /**
         * A week. Long enough that the narrow path is the normal one — the whole
         * point is that a cycle costs a sliver, not a corpus — and short enough
         * that anything a band is wrong about is wrong for days, not forever.
         */
        const val DEFAULT_FULL_RESYNC_SECONDS = 7L * 24 * 60 * 60

        private const val PLAUSIBLE_FLOOR = 1_577_836_800L // 2020-01-01

        // Clock skew a relay may legitimately be ahead by. Past this, a
        // created_at is the author's fiction rather than a time.
        private const val FUTURE_SKEW_SECONDS = 86_400L

        // Often enough that a kill costs little, rare enough to be free.
        private const val DEFAULT_FLUSH_SECONDS = 30L

        private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

        /**
         * Whether a `created_at` can be believed as evidence of coverage.
         *
         * Filter with this per EVENT, not over a leg's aggregate: rejecting the
         * aggregate throws away the whole upstream's band because one event in
         * it was misdated, which is how an upstream that downloaded 700k events
         * ended up recording nothing.
         */
        fun isPlausible(createdAt: Long): Boolean = createdAt in PLAUSIBLE_FLOOR..(nowSeconds() + FUTURE_SKEW_SECONDS)

        /**
         * `ROUTER_SYNC_STATE_FILE` — where the cursors live. Unset keeps them in
         * memory, which is the same as not having them: the whole point is
         * surviving the restart. Under compose, point it inside a mounted volume.
         */
        fun fromEnv(env: Map<String, String>): SyncCursors =
            SyncCursors(
                env["ROUTER_SYNC_STATE_FILE"]?.trim()?.takeIf { it.isNotEmpty() }?.let(::File),
                env["ROUTER_FULL_RESYNC_SECONDS"]?.trim()?.toLongOrNull()?.takeIf { it > 0 } ?: DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
