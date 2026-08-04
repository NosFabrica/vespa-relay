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
 * A negentropy relay needs none of this — reconciliation downloads only the
 * diff. Most relays lack NIP-77, and a paged fetch re-downloads everything it
 * walked last time, every restart, forever. So for those we remember the band
 * of `created_at` covered per (relay, filter), and the next run asks only for
 * the two legs outside it:
 *
 *     stored band:        |<-------- covered -------->|
 *     next fetch:  <------|                           |------>
 *
 * Keyed by the WHOLE filter deliberately: any edit to a filter is a new key
 * with no band, so the next run starts over — the safe direction to be wrong
 * in, and the intended way to force a re-walk.
 *
 * A band does not guarantee completeness (a truncating relay, an event
 * back-dated into a walked span). The trade is deliberate: re-reading a corpus
 * on every restart is a certain daily cost, while both holes are occasional
 * and self-heal on the next filter change or full re-walk.
 */
class SyncCursors(
    private val file: File?,
    // How long a band may narrow work before the whole filter is walked again.
    // Everything a band claims is a claim about the past; this is how long we
    // trust it without re-testing.
    private val fullResyncSeconds: Long = DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    /**
     * What is already covered for one (relay, filter) pair.
     *
     * [complete] is the difference between "we walked this span" (a paged
     * fetch) and "we are in sync below this point" (a finished negentropy
     * reconcile, which compared the whole range). Only a complete band may
     * skip its older leg.
     *
     * [fullAt] is when the last pass that started from nothing finished — the
     * clock for the periodic re-walk.
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

    // filter -> its canonical json. Filter.toJson() runs to tens of thousands
    // of characters for author-scoped filters, and the dynamic streams key
    // once per relay per cycle over the SAME handful of filter objects.
    // Identity equality is right here: a merely-equal filter still keys
    // correctly, just without the cache.
    private val fingerprints = Collections.synchronizedMap(IdentityHashMap<Filter, String>())

    init {
        load()
    }

    /**
     * The filters to actually run now, given what is already covered: the
     * whole filter when nothing is recorded (or the band went stale),
     * otherwise the legs outside the band, clamped to the filter's own
     * `since`/`until`.
     *
     * The legs are INCLUSIVE of the band's edges (`until = min`, not
     * `min - 1`): a page boundary can split a run of events sharing one
     * `created_at`, and excluding the edge would strand the rest of that
     * second in no leg at all. The cost is re-reading one second's worth of
     * events per leg, which the store rejects as duplicates.
     */
    fun legs(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> {
        val band = bands[key(url, filter)] ?: return listOf(filter)
        // Time for another full pass: relays gain old events, and without
        // this the band's claim is never re-tested.
        if (isStale(band)) return listOf(filter)
        val legs = mutableListOf<Filter>()

        // Older: up to and including the band's floor, but not past the
        // filter's. A complete band has no older leg at all — the reconcile
        // already compared the whole range.
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
     * [paged] gates the mechanism: a negentropy sync needs no cursor, and
     * recording one would only risk narrowing a future reconciliation.
     * Nothing is recorded for a fetch that saw no events — an empty result
     * says nothing about what the relay holds.
     */
    fun record(
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
    ) {
        // A finished reconcile is the strong case: it compared the filter's
        // whole range, so we are in sync up to the instant the sync STARTED —
        // recorded against that instant rather than the newest event seen,
        // because "the relay had nothing newer" and "we never asked" must not
        // look alike.
        if (reconciledThrough != null) {
            put(url, filter, observedMin ?: reconciledThrough, reconciledThrough, complete = true)
            return
        }
        if (!paged) return
        // Guarded even though callers filter with [isPlausible] per event: a
        // 1970 floor or a 2027 ceiling would make the band claim the whole
        // timeline, and the leg outside it would ask for a range nothing can
        // be in, forever.
        if (observedMin == null || observedMax == null) return
        if (!isPlausible(observedMin) || !isPlausible(observedMax)) return
        put(url, filter, observedMin, observedMax, complete = false)
    }

    /**
     * Widen (or reset) the band. A pass that ran because the previous band had
     * gone stale REPLACES it: it re-walked the whole filter, so its own span
     * is the complete picture and [Band.fullAt] restarts from here.
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
        // Marked dirty, not written: a dynamic stream records once per leg per
        // relay, and every write serializes the whole map — saving here would
        // be thousands of full-file rewrites per cycle. [flush] does it once;
        // losing the last unflushed window to a hard kill costs one partial
        // re-fetch, not correctness.
        dirty = true
    }

    private fun isStale(band: Band): Boolean = nowSeconds() - band.fullAt >= fullResyncSeconds

    /**
     * The narrowest single filter that still covers what every one of [urls]
     * needs — the window a shared negentropy snapshot has to be taken over.
     *
     * In steady state every relay carries a complete band and this collapses
     * to `since = the oldest of their ceilings` — the difference between
     * snapshotting 24M ids and a few thousand. One relay that has never
     * synced puts it back to the full filter, correctly.
     */
    fun coveringWindow(
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter {
        if (urls.isEmpty()) return filter
        var since = Long.MAX_VALUE
        for (url in urls) {
            val legs = legs(url, filter)
            // More than one leg means an older gap this relay still wants, so
            // the snapshot cannot start above the filter's own floor.
            val only = legs.singleOrNull() ?: return filter
            val legSince = only.since ?: return filter
            since = minOf(since, legSince)
        }
        return if (since == Long.MAX_VALUE) filter else filter.copy(since = since)
    }

    /**
     * Write the map every [intervalSec] on a daemon thread, so progress
     * survives a hard kill between the milestone flushes (which are minutes
     * to hours apart). Unchanged intervals write nothing.
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
     * The identity of one (relay, filter) pair. [Filter.toJson] is the
     * protocol's own canonical form, so two filters that mean the same thing
     * key the same way and any edit keys differently — exactly the "config
     * changed, start over" rule.
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
                        // Absent in files written before coverage was tracked:
                        // reads as "span only" and stale, so the first run
                        // after an upgrade re-walks once and records the real
                        // thing.
                        o["complete"]?.jsonPrimitive?.boolean ?: false,
                        o["fullAt"]?.jsonPrimitive?.long ?: 0L,
                    )
            }
        }.onFailure {
            // A corrupt cursor file costs one re-sync; exiting costs the relay.
            System.err.println("router: could not read sync cursors from ${f.path} (${it.message}); starting fresh")
        }
    }

    /** Persist via a temp file and an atomic move, so a reader never sees a half-written map. */
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
        // re-synced.
        private val json = Json { prettyPrint = true }

        /**
         * A week. Long enough that the narrow path is the normal one, short
         * enough that anything a band is wrong about is wrong for days, not
         * forever.
         */
        const val DEFAULT_FULL_RESYNC_SECONDS = 7L * 24 * 60 * 60

        /**
         * 2020-01-01. Below this a `created_at` is a bug, not a date — the
         * protocol did not exist. Also the floor a paged walk measures its
         * progress against when a filter names no `since`.
         */
        const val PLAUSIBLE_FLOOR = 1_577_836_800L

        // Clock skew a relay may legitimately be ahead by. Past this, a
        // created_at is the author's fiction rather than a time.
        private const val FUTURE_SKEW_SECONDS = 86_400L

        // Often enough that a kill costs little, rare enough to be free.
        private const val DEFAULT_FLUSH_SECONDS = 30L

        private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

        /**
         * Whether a `created_at` can be believed as evidence of coverage.
         * Filter with this per EVENT, not over a leg's aggregate: one misdated
         * event once discarded a whole upstream's 700k-event band.
         */
        fun isPlausible(createdAt: Long): Boolean = createdAt in PLAUSIBLE_FLOOR..(nowSeconds() + FUTURE_SKEW_SECONDS)

        /**
         * `ROUTER_SYNC_STATE_FILE` — where the cursors live. Unset keeps them
         * in memory, which is the same as not having them.
         */
        fun fromEnv(env: Map<String, String>): SyncCursors =
            SyncCursors(
                env["ROUTER_SYNC_STATE_FILE"]?.trim()?.takeIf { it.isNotEmpty() }?.let(::File),
                env["ROUTER_FULL_RESYNC_SECONDS"]?.trim()?.toLongOrNull()?.takeIf { it > 0 } ?: DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
