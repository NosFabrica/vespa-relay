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
) {
    /** The `created_at` span already pulled for one (relay, filter) pair. */
    data class Band(
        val minCreatedAt: Long,
        val maxCreatedAt: Long,
    )

    private val bands = ConcurrentHashMap<String, Band>()

    @Volatile private var dirty = false

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
        val legs = mutableListOf<Filter>()

        // Older: up to and including the band's floor, but not past the filter's.
        if (filter.since == null || band.minCreatedAt >= filter.since!!) {
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
    ) {
        if (!paged || observedMin == null || observedMax == null) return
        val k = key(url, filter)
        bands.compute(k) { _, prev ->
            if (prev == null) {
                Band(observedMin, observedMax)
            } else {
                Band(minOf(prev.minCreatedAt, observedMin), maxOf(prev.maxCreatedAt, observedMax))
            }
        }
        // Marked, not written. A dynamic stream records once per leg per relay
        // and every write serializes the whole map, so saving here would cost
        // O(relays²) per cycle — thousands of full-file rewrites to persist a few
        // thousand integers. [flush] does it once, and losing the last unflushed
        // window to a hard kill costs one partial re-fetch, not correctness.
        dirty = true
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
     */
    private fun key(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): String = "${url.url} ${filter.toJson()}"

    private fun load() {
        val f = file ?: return
        if (!f.isFile) return
        runCatching {
            Json.parseToJsonElement(f.readText()).jsonObject.forEach { (k, v) ->
                val o = v.jsonObject
                bands[k] = Band(o.getValue("min").jsonPrimitive.long, o.getValue("max").jsonPrimitive.long)
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
                            },
                        )
                    }
                }
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(Json.encodeToString(JsonObject.serializer(), snapshot))
            Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }.onFailure {
            System.err.println("router: could not write sync cursors to ${f.path}: ${it.message}")
        }
    }

    companion object {
        /**
         * `ROUTER_SYNC_STATE_FILE` — where the cursors live. Unset keeps them in
         * memory, which is the same as not having them: the whole point is
         * surviving the restart. Under compose, point it inside a mounted volume.
         */
        fun fromEnv(env: Map<String, String>): SyncCursors = SyncCursors(env["ROUTER_SYNC_STATE_FILE"]?.trim()?.takeIf { it.isNotEmpty() }?.let(::File))
    }
}
