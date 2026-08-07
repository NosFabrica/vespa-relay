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
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * The router's sync state, read as a coverage report the stats page can chart.
 *
 * ## Why the relay reads the router's files at all
 *
 * The bands and the sweep cursors are the ROUTER's, and the router has no HTTP
 * server — it is a worker process with a config file and two JSON files. The
 * relay is the thing that serves pages. They already share `/var/lib/vespa-relay`
 * (see the bind mounts in `docker-compose.yml`, whose comment says so), so the
 * cheapest correct path is the one taken here: the relay READS those two files
 * during its rollup and folds the result into `/stats.json`. No new port, no new
 * file, no second serving mechanism, and the document stays the artifact — a
 * reader charting our coverage elsewhere gets it from the same place as
 * everything else.
 *
 * The cost, stated plainly: this couples the relay to the router's on-disk
 * format. That format is pinned by `SyncBandsTest` and documented in
 * `configuration.md`, and this parser tolerates every part of it being absent,
 * but a change on the router side that this file does not learn about shows up
 * as a thinner card rather than as an error. Read-only and best-effort, on
 * purpose: nothing here may ever cost the relay its rollup.
 *
 * ## The two files, and the two DIFFERENT key formats
 *
 * They do not agree, and this is the detail worth knowing before touching this:
 *
 *  - a band's key is `"<relay-url> <whole-filter-json>"`, joined by a SPACE
 *    (quartz's `SyncCoverage.export`);
 *  - a sweep's key is `"<relay-url>|<filter-json-without-time>"`, joined by a
 *    PIPE, and with `since`/`until`/`limit` stripped ([SweepState.keyFor], which
 *    strips them because time is what a sweep VARIES).
 *
 * Both are safe to split on their first separator because a normalized relay url
 * contains neither a space nor a pipe — `RelayDiscovery` rejects any url with
 * whitespace before it is ever dialled.
 *
 * Joining them therefore cannot be done on the raw key. Both sides are reduced
 * to the same SHAPE — the filter with `since`/`until`/`limit` dropped and its
 * members ordered — so a stream whose filter does carry a time bound still
 * matches its own cursor instead of silently rendering a relay as idle while a
 * sweep runs against it.
 *
 * ## What this cannot say
 *
 * **"Never asked" is not knowable here.** These files hold the relays the router
 * HAS walked; the list a stream was configured with lives in `router.conf`,
 * which the relay does not read. So the denominator is "relays this filter has
 * touched", not "relays this stream names", and the page must not claim
 * otherwise.
 *
 * **Stream names are not knowable here either.** A band is keyed by the FILTER,
 * not by the stream that asked — which is the honest grouping anyway, since the
 * filter is what actually determines coverage, and two streams sharing a filter
 * shape share one band. Groups are labelled by their filter.
 */
internal object SyncCoverageReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /** One relay's coverage of one filter shape, before it is written out. */
    private data class Row(
        val relay: String,
        val min: Long,
        val max: Long,
        val complete: Boolean,
        val fullAt: Long,
        // The intersection of the per-kind spans: the part of [min]..[max] that
        // EVERY kind in the filter has actually been walked over. See [narrowed].
        val everyKindMin: Long,
        val everyKindMax: Long,
    )

    /**
     * Fold the two files into the `data` of a `sync` section, or null when
     * there is nothing to say.
     *
     * Null rather than an empty object for a deployment with no router at all:
     * "this relay does not mirror" and "the mirror has covered nothing" are
     * different facts, and only one of them is worth a card.
     */
    fun build(
        bandsJson: String?,
        sweepsJson: String?,
        nowSeconds: Long,
    ): JsonObject? {
        val bands = parse(bandsJson) ?: JsonObject(emptyMap())
        val sweeps = parse(sweepsJson) ?: JsonObject(emptyMap())
        if (bands.isEmpty() && sweeps.isEmpty()) return null

        // (shape → relay → row). A LinkedHashMap the whole way down: the file's
        // own order is the only ordering the router offers, and a report that
        // reshuffles its groups between rollups is one nobody can diff.
        val byShape = LinkedHashMap<String, LinkedHashMap<String, Row>>()
        val shapeFilters = LinkedHashMap<String, JsonObject>()

        for ((key, value) in bands) {
            val (rawRelay, filterJson) = split(key, ' ') ?: continue
            // The same spelling the relay distribution table uses, so one relay
            // is one string across the whole document. Applied to BOTH files
            // and to the peer map below — it is a deterministic rename, so it
            // cannot split a pair that the router wrote as one.
            val relay = StatsYql.canonicalRelay(rawRelay)
            val band = value as? JsonObject ?: continue
            val filter = parse(filterJson) ?: continue
            val shape = shapeOf(filter)
            val min = band["min"]?.jsonPrimitive?.longOrNull ?: continue
            val max = band["max"]?.jsonPrimitive?.longOrNull ?: continue
            val (everyMin, everyMax) = narrowed(band, min, max)
            shapeFilters.putIfAbsent(shape, filter)
            byShape.getOrPut(shape) { LinkedHashMap() }[relay] =
                Row(
                    relay = relay,
                    min = min,
                    max = max,
                    complete = band["complete"]?.jsonPrimitive?.booleanOrNull ?: false,
                    fullAt = band["fullAt"]?.jsonPrimitive?.longOrNull ?: 0L,
                    everyKindMin = everyMin,
                    everyKindMax = everyMax,
                )
        }

        val peers =
            (sweeps["peers"] as? JsonObject ?: JsonObject(emptyMap()))
                .mapKeys { StatsYql.canonicalRelay(it.key) }
        // (shape, relay) → the in-flight slice. Held apart from the rows because
        // a sweep can exist for a pair that has NO band yet — the first sweep of
        // a relay records nothing until a leg finishes, and that relay being
        // mid-walk is exactly what the card is for.
        val live = LinkedHashMap<Pair<String, String>, JsonObject>()
        for ((key, value) in (sweeps["sweeps"] as? JsonObject ?: JsonObject(emptyMap()))) {
            val (rawRelay, filterJson) = split(key, '|') ?: continue
            val relay = StatsYql.canonicalRelay(rawRelay)
            val mark = value as? JsonObject ?: continue
            val filter = parse(filterJson) ?: continue
            val shape = shapeOf(filter)
            shapeFilters.putIfAbsent(shape, filter)
            byShape.putIfAbsent(shape, LinkedHashMap())
            live[shape to relay] = mark
        }

        // The frame every row is read against, and the one number that makes two
        // groups comparable. Taken from the data rather than configured: these
        // filters carry no `since`, so there is no target to measure against and
        // the honest frame is "as deep as anything here reaches".
        var from = Long.MAX_VALUE
        for (rows in byShape.values) for (r in rows.values) if (r.min < from) from = r.min
        for (mark in live.values) {
            val d = mark["downTo"]?.jsonPrimitive?.longOrNull ?: continue
            if (d < from) from = d
        }
        if (from == Long.MAX_VALUE) return null

        return buildJsonObject {
            put("from", from)
            put("to", nowSeconds)
            putJsonArray("streams") {
                for ((shape, rows) in byShape) {
                    val marks = live.filterKeys { it.first == shape }.mapKeys { it.key.second }
                    if (rows.isEmpty() && marks.isEmpty()) continue
                    add(stream(shapeFilters[shape], rows, marks, peers))
                }
            }
        }
    }

    private fun stream(
        filter: JsonObject?,
        rows: Map<String, Row>,
        marks: Map<String, JsonObject>,
        peers: Map<String, kotlinx.serialization.json.JsonElement>,
    ): JsonObject {
        // Deepest first: the staircase this produces is the shape of the answer.
        // A relay that reaches furthest back is the one carrying the group, and
        // sorting by url would scatter that across a thousand rows. A relay with
        // a sweep but no band yet sorts last — it has covered nothing durable.
        val relays = (rows.keys + marks.keys).distinct().sortedBy { rows[it]?.min ?: Long.MAX_VALUE }
        return buildJsonObject {
            filter?.let { put("filter", it) }
            put("relays", relays.size)
            put("reconciled", rows.values.count { it.complete })
            put("paged", rows.values.count { !it.complete })
            put("sweeping", marks.size)
            putJsonArray("rows") {
                for (relay in relays) {
                    add(
                        buildJsonObject {
                            put("relay", relay)
                            rows[relay]?.let { r ->
                                put("min", r.min)
                                put("max", r.max)
                                put("complete", r.complete)
                                if (r.fullAt > 0) put("fullAt", r.fullAt)
                                // Only when the per-kind spans DISAGREE. Equal
                                // spans are the overwhelming case and writing
                                // them would double this array for nothing.
                                if (r.everyKindMin != r.min || r.everyKindMax != r.max) {
                                    put("everyKindMin", r.everyKindMin)
                                    put("everyKindMax", r.everyKindMax)
                                }
                            }
                            // The learned window size, keyed by relay alone —
                            // it is a property of the peer's config, not of any
                            // one filter, which is why it is in `peers` and not
                            // beside the cursor.
                            peers[relay]?.jsonObject?.let { p ->
                                p["target"]?.jsonPrimitive?.longOrNull?.let { put("target", it) }
                                p["cap"]?.jsonPrimitive?.longOrNull?.let { put("cap", it) }
                            }
                            marks[relay]?.let { m ->
                                val downTo = m["downTo"]?.jsonPrimitive?.longOrNull
                                val upTo = m["upTo"]?.jsonPrimitive?.longOrNull
                                if (downTo != null && upTo != null) {
                                    put(
                                        "sweep",
                                        buildJsonObject {
                                            put("downTo", downTo)
                                            put("upTo", upTo)
                                            m["at"]?.jsonPrimitive?.longOrNull?.let { put("at", it) }
                                        },
                                    )
                                }
                            }
                        },
                    )
                }
            }
        }
    }

    /**
     * The part of a band that EVERY kind in the filter has been walked over.
     *
     * A band's `min`/`max` are the outer edges across every kind, and the known
     * open bug in `AGENTS.md` is exactly that they over-claim: one span per
     * filter means a long-lived kind (0) vouches for a short-lived one (30382),
     * so a row drawn from `min`/`max` alone says a relay is covered for a kind
     * nobody ever walked. The per-kind `spans` the file now carries are enough
     * to say the narrower true thing, so this returns their INTERSECTION — the
     * widest window inside which no kind is missing.
     *
     * Falls back to the outer span when there are no per-kind spans, which is
     * what a file written before they existed carries.
     */
    private fun narrowed(
        band: JsonObject,
        min: Long,
        max: Long,
    ): Pair<Long, Long> {
        val spans = band["spans"] as? JsonObject ?: return min to max
        var lo = Long.MIN_VALUE
        var hi = Long.MAX_VALUE
        var seen = 0
        for ((_, v) in spans) {
            val o = v as? JsonObject ?: continue
            val sMin = o["min"]?.jsonPrimitive?.longOrNull ?: continue
            val sMax = o["max"]?.jsonPrimitive?.longOrNull ?: continue
            if (sMin > lo) lo = sMin
            if (sMax < hi) hi = sMax
            seen++
        }
        // An EMPTY intersection is a real state — two kinds with disjoint spans
        // — and it must not come back inverted, which would draw a bar with a
        // negative width.
        if (seen == 0 || lo > hi) return min to max
        return lo to hi
    }

    /**
     * A filter reduced to what a sweep and a band can agree on.
     *
     * `since`/`until`/`limit` are dropped because [SweepState.keyFor] drops
     * them, and the members are ordered because two files' serializers are not
     * required to agree on key order. Everything else stays: a different ask has
     * not been covered just because this one was, which is the same property the
     * band keys have and the reason editing a stream's filter re-walks it.
     */
    private fun shapeOf(filter: JsonObject): String =
        filter
            .filterKeys { it != "since" && it != "until" && it != "limit" }
            .toSortedMap()
            .entries
            .joinToString(",") { (k, v) -> "$k=${canon(v)}" }

    /** Arrays ordered too — `[0,10002]` and `[10002,0]` are one filter. */
    private fun canon(v: kotlinx.serialization.json.JsonElement): String =
        runCatching {
            v.jsonArray
                .map { it.toString() }
                .sorted()
                .joinToString(",", "[", "]")
        }.getOrElse { v.toString() }

    /** Split a key at its FIRST separator; a normalized relay url contains neither. */
    private fun split(
        key: String,
        sep: Char,
    ): Pair<String, String>? {
        val at = key.indexOf(sep)
        if (at <= 0 || at == key.length - 1) return null
        return key.substring(0, at) to key.substring(at + 1)
    }

    /** Never throws: a corrupt or half-written file costs this card, not the rollup. */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return runCatching { lenient.parseToJsonElement(text).jsonObject }.getOrNull()
    }
}
