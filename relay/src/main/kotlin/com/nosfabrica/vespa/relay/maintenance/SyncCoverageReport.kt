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
 *
 * ## Why a group is keyed on the filter's SHAPE and not its values
 *
 * One router stream does not ask every relay the same filter. A `relaySource`
 * whose select binds `authors` hands each discovered relay the base filter
 * NARROWED by the authors that named it ([DiscoveredRelay.narrowed]), and
 * `authorsPerLeg` chops that again into one ask per author. So a stream
 * configured as `{"kinds":[30023]}` reaches the band file as thousands of
 * distinct keys — `{"kinds":[30023],"authors":["a1"]}`, `..."a2"...` — one per
 * relay, or per author per relay.
 *
 * Keyed on the filter's exact members, those are thousands of groups holding
 * one row each, all labelled `kinds 30023`, and the card stops answering the
 * only question it exists for: how much of a stream's relay list is in sync.
 * So the key stars the members a narrow VARIES — `authors`, `ids`, and tag
 * filters — and keeps everything else. Two asks that differ only in which
 * authors they name are legs of one stream and are charted as one; two that
 * differ in kinds, or in whether they carry authors AT ALL, stay apart, because
 * those are genuinely different asks.
 *
 * The starred members are then absent from the group's `filter` — carrying one
 * arbitrary leg's author list would be a lie about the other 3,999 — and named
 * in `narrowedBy` instead, so a reader can tell a per-author stream from a
 * plain one. `legs` is how many (relay, filter) bands were folded in.
 */
internal object SyncCoverageReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /**
     * A filter member a discovery narrow can vary per relay, and which is
     * therefore starred out of a group's key.
     *
     * The list is [DiscoveredRelay.narrowed]'s own: `authors`, `ids`, and tag
     * filters (`#p`, `#t`, …). `kinds` is the one member that narrow can set
     * which is NOT here — a stream split by kind is asking two different
     * questions, and a group is exactly "one question, many relays".
     */
    private fun varies(member: String) = member == "authors" || member == "ids" || member.startsWith("#")

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
    ) {
        /**
         * Two legs of one stream against the SAME relay, as one row.
         *
         * The outer edges union, because between them is ground the relay has
         * been walked over for something. `complete` is an AND: a relay is
         * reconciled for this group only when every leg of it finished, and one
         * paged leg is exactly the doubt the tone is there to carry. The
         * every-kind window intersects for the same reason it does across kinds
         * — it is the span nothing in this group is missing from — and falls
         * back to the outer edges when the legs share none, which is [narrowed]'s
         * rule and keeps a bar from being drawn inside out.
         */
        fun mergedWith(other: Row): Row {
            val lo = maxOf(everyKindMin, other.everyKindMin)
            val hi = minOf(everyKindMax, other.everyKindMax)
            val outerMin = minOf(min, other.min)
            val outerMax = maxOf(max, other.max)
            return Row(
                relay = relay,
                min = outerMin,
                max = outerMax,
                complete = complete && other.complete,
                fullAt = maxOf(fullAt, other.fullAt),
                everyKindMin = if (lo > hi) outerMin else lo,
                everyKindMax = if (lo > hi) outerMax else hi,
            )
        }
    }

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
        // How many (relay, filter) bands each group folded in. Equal to its
        // relay count for a plain stream, and several times that for one a
        // narrow split per author — which is the difference the card has to be
        // able to state, because it is why 8 relays can carry 40 bands.
        val shapeLegs = LinkedHashMap<String, Int>()

        /**
         * Parse and reduce a filter ONCE per distinct filter, not once per key.
         *
         * A plain stream hands every relay the byte-identical filter, so a
         * 4,000-relay stream handed the same JSON to the parser 4,000 times and
         * re-serialised the same shape 4,000 times. That is affordable for
         * `{"kinds":[0,10002]}` and it is not for a discovery filter: those
         * carry thousands of authors, which is the very cost [SweepState.keyFor]
         * already exists to avoid paying per window ("re-deriving the key per
         * finished window would re-render that JSON for every window, for
         * nothing"). Measured on 1,000 relays with 200 authors: 13.7MB of input,
         * 213ms → the parse is the whole bill.
         *
         * ONLY THE SHAPE IS RETAINED, never the parsed filter. The narrowed
         * streams this report groups across (see the class header) are the exact
         * case where every relay's filter is DIFFERENT, so the cache cannot hit
         * and holding each parsed object would turn a pure miss into a heap copy
         * of the whole file — on a parser whose one hard rule is that it must
         * never cost the relay its rollup. The parsed filter is needed once per
         * SHAPE, for `filter` in the output, so it is banked here on the first
         * leg to reach a shape and the rest are left to the collector.
         *
         * Null is CACHED as null: a filter the parser rejects must be rejected
         * once, not re-attempted for every relay that shares it.
         */
        val shapeCache = HashMap<String, String?>()

        fun shapeFor(filterJson: String): String? {
            shapeCache[filterJson]?.let { return it }
            if (shapeCache.containsKey(filterJson)) return null
            val filter = parse(filterJson)
            val shape = filter?.let { shapeOf(it) }
            shapeCache[filterJson] = shape
            // First leg to reach this shape banks the filter the group is
            // published with; `shared` strips what the other legs disagree on.
            if (filter != null && shape != null) shapeFilters.putIfAbsent(shape, filter)
            return shape
        }

        for ((key, value) in bands) {
            val (rawRelay, filterJson) = split(key, ' ') ?: continue
            // The same spelling the relay distribution table uses, so one relay
            // is one string across the whole document. Applied to BOTH files
            // and to the peer map below — it is a deterministic rename, so it
            // cannot split a pair that the router wrote as one.
            val relay = StatsYql.canonicalRelay(rawRelay)
            val band = value as? JsonObject ?: continue
            val shape = shapeFor(filterJson) ?: continue
            val min = band["min"]?.jsonPrimitive?.longOrNull ?: continue
            val max = band["max"]?.jsonPrimitive?.longOrNull ?: continue
            val (everyMin, everyMax) = narrowed(band, min, max)
            shapeLegs.merge(shape, 1, Int::plus)
            val row =
                Row(
                    relay = relay,
                    min = min,
                    max = max,
                    complete = band["complete"]?.jsonPrimitive?.booleanOrNull ?: false,
                    fullAt = band["fullAt"]?.jsonPrimitive?.longOrNull ?: 0L,
                    everyKindMin = everyMin,
                    everyKindMax = everyMax,
                )
            // MERGED, never overwritten. A relay reached by several legs of one
            // author-narrowed stream lands here once per leg, and taking the
            // last one silently charted whichever author the file happened to
            // write last as the relay's whole coverage.
            byShape.getOrPut(shape) { LinkedHashMap() }.merge(relay, row) { a, b -> a.mergedWith(b) }
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
            val shape = shapeFor(filterJson) ?: continue
            byShape.putIfAbsent(shape, LinkedHashMap())
            // Same merge as the bands, for the same reason: one relay can be
            // mid-sweep on several legs of one stream at once. The widest
            // in-flight slice, stamped with the most recent advance — a cursor
            // is "here is what is moving right now", and two of them moving is
            // still one relay moving.
            live.merge(shape to relay, mark) { a, b -> widest(a, b) }
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
        // The frame must not be able to invert. `created_at` is author-signed
        // and quartz records a band whose edges sit up to about a day ahead of
        // now, so a store holding nothing but future-dated events would put
        // `from` past `nowSeconds` — and a reader computing `to - from` gets a
        // negative span, which is a division every drawing then multiplies by.
        // Widening the frame is the honest repair: it still contains the data.
        if (from > nowSeconds) from = nowSeconds

        return buildJsonObject {
            put("from", from)
            put("to", nowSeconds)
            // Grouped ONCE, not rescanned per stream: `live.filterKeys` inside
            // the loop walked every cursor for every shape.
            val marksByShape = LinkedHashMap<String, LinkedHashMap<String, JsonObject>>()
            for ((k, v) in live) marksByShape.getOrPut(k.first) { LinkedHashMap() }[k.second] = v
            putJsonArray("streams") {
                for ((shape, rows) in byShape) {
                    val marks = marksByShape[shape] ?: emptyMap()
                    if (rows.isEmpty() && marks.isEmpty()) continue
                    add(stream(shapeFilters[shape], shapeLegs[shape] ?: rows.size, rows, marks, peers))
                }
            }
        }
    }

    /**
     * Two sweep cursors on one relay, as the one slice they jointly cover.
     *
     * A cursor with no `downTo`/`upTo` is unreadable and loses to one that has
     * them rather than poisoning the pair with a missing edge.
     */
    private fun widest(
        a: JsonObject,
        b: JsonObject,
    ): JsonObject {
        fun edge(
            o: JsonObject,
            k: String,
        ) = o[k]?.jsonPrimitive?.longOrNull
        val (aDown, aUp) = edge(a, "downTo") to edge(a, "upTo")
        val (bDown, bUp) = edge(b, "downTo") to edge(b, "upTo")
        if (aDown == null || aUp == null) return b
        if (bDown == null || bUp == null) return a
        return buildJsonObject {
            put("downTo", minOf(aDown, bDown))
            put("upTo", maxOf(aUp, bUp))
            val at = listOfNotNull(edge(a, "at"), edge(b, "at")).maxOrNull()
            at?.let { put("at", it) }
        }
    }

    private fun stream(
        filter: JsonObject?,
        legs: Int,
        rows: Map<String, Row>,
        marks: Map<String, JsonObject>,
        peers: Map<String, kotlinx.serialization.json.JsonElement>,
    ): JsonObject {
        // Deepest first: the staircase this produces is the shape of the answer.
        // A relay that reaches furthest back is the one carrying the group, and
        // sorting by url would scatter that across a thousand rows. A relay with
        // a sweep but no band yet sorts last — it has covered nothing durable.
        // `rows.keys + marks.keys` is already a Set, so no dedup pass is needed;
        // and the sort key is read into a pair first because `sortedBy` calls
        // its selector on every comparison — a map lookup per comparison across
        // thousands of relays, for a value that cannot change.
        val relays =
            (rows.keys + marks.keys)
                .map { it to (rows[it]?.min ?: Long.MAX_VALUE) }
                .sortedBy { it.second }
                .map { it.first }
        return buildJsonObject {
            filter?.let {
                put("filter", shared(it))
                val varying = narrowedBy(it)
                if (varying.isNotEmpty()) {
                    putJsonArray("narrowedBy") { for (m in varying) add(m) }
                    // Against `rows`, not `relays`: `legs` counts BANDS, and
                    // `relays` counts bands ∪ cursors. A group with 5 bands on 2
                    // relays plus 3 relays sweeping their first leg has legs=5
                    // and relays=5, and comparing those two hid the merge that
                    // this number exists to disclose. `rows` is the band-bearing
                    // relays, so `legs > rows.size` is exactly "some relay was
                    // reached more than once".
                    if (legs > rows.size) put("legs", legs)
                }
            }
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
     * required to agree on key order. A member a discovery narrow varies per
     * relay ([varies]) keeps its NAME and loses its value, so the legs of one
     * stream share a key — see the class header. Everything else stays whole: a
     * different ask has not been covered just because this one was, which is the
     * same property the band keys have and the reason editing a stream's filter
     * re-walks it.
     */
    private fun shapeOf(filter: JsonObject): String =
        filter
            .filterKeys { it != "since" && it != "until" && it != "limit" }
            .toSortedMap()
            .entries
            .joinToString(",") { (k, v) -> if (varies(k)) "$k=*" else "$k=${canon(v)}" }

    /** The members of [filter] the group's key starred out, in the order they appear. */
    private fun narrowedBy(filter: JsonObject): List<String> = filter.keys.filter { varies(it) }

    /**
     * [filter] without the members that vary across the group's legs.
     *
     * The group holds one leg's parsed filter and every leg's authors are
     * different, so publishing it whole would name 1 of 4,000 author lists as if
     * it were the stream's. What survives is what every leg agrees on — which
     * is what the group key kept, so this drops exactly what [shapeOf] drops.
     * `since`/`until`/`limit` are in that set for the same reason the varying
     * members are: windowed reconciliation writes a band per window, so the
     * bounds on the leg that happened to parse first are that WINDOW's, and
     * publishing them would read as the stream's own.
     */
    private fun shared(filter: JsonObject): JsonObject = JsonObject(filter.filterKeys { !varies(it) && it != "since" && it != "until" && it != "limit" })

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

    /**
     * A corrupt or half-written file costs this card, not the rollup.
     *
     * `Exception`, deliberately NOT `runCatching`, which catches `Throwable`.
     * These files scale as (relays × filter size) and a discovery stream's
     * filter carries thousands of authors, so a large one is genuinely capable
     * of exhausting the heap — and when it did, `runCatching` turned the
     * `OutOfMemoryError` into "no sync state in this document", which is a
     * quiet lie about the mirror on the one page built to report it. An Error
     * belongs to the rollup loop, which already logs it loudly and keeps
     * serving the previous document.
     */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return try {
            lenient.parseToJsonElement(text).jsonObject
        } catch (e: Exception) {
            null
        }
    }
}
