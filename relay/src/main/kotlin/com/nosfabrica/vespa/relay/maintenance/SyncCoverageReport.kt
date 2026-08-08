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
import kotlinx.serialization.json.JsonElement
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
 * ## The two files, and the one key format
 *
 * Both nest the same three levels, stream → filter → relay:
 *
 *  - a band is `{"<stream>": {"<filter>": {"<relay-url>": {min, max, …}}}}`
 *    (`SyncBands`, which takes quartz's `"<relay> <filter>"` key apart to write
 *    it and puts it back together to read it);
 *  - a sweep is the same three levels under `sweeps`, with `since`/`until`/
 *    `limit` stripped from the filter ([SweepState.keyFor], which strips them
 *    because time is what a sweep VARIES). `peers` beside it stays keyed by the
 *    relay alone — a learned window size is a property of the peer's config.
 *
 * So the stream is what a group IS, and joining the two files is looking up the
 * same three names. The filters still have to be reduced to a common shape
 * before they can be compared, because a band's carries the time bounds a sweep
 * strips — that is [shapeOf]'s whole remaining job.
 *
 * The nesting is also why this parser is affordable. A plain stream hands 4,000
 * relays one byte-identical filter, and the flat format wrote that filter into
 * 4,000 keys; here it is one key with 4,000 children, so the expensive parse
 * (measured: 13.7MB of input, 213ms, and the parse is the whole bill) happens
 * once per distinct filter by construction.
 *
 * ## What this cannot say
 *
 * **"Never asked" is not knowable here.** These files hold the relays the router
 * HAS walked; the list a stream was configured with lives in `router.conf`,
 * which the relay does not read. So the denominator is "relays this stream has
 * touched", not "relays this stream names", and the page must not claim
 * otherwise.
 *
 * ## Why a group's `filter` is what its legs AGREE on
 *
 * One router stream does not ask every relay the same filter. A `relaySource`
 * whose select binds `authors` hands each discovered relay the base filter
 * NARROWED by the authors that named it ([DiscoveredRelay.narrowed]), and
 * `authorsPerLeg` chops that again into one ask per author. So a stream
 * configured as `{"kinds":[30023]}` reaches the file as thousands of distinct
 * filters — `{"kinds":[30023],"authors":["a1"]}`, `..."a2"...` — one per relay,
 * or per author per relay, all under the one stream.
 *
 * Publishing one of them as the group's filter would be a lie about the other
 * 3,999, so the group's `filter` is the members every leg agrees on exactly,
 * and the members they disagree on are named in `narrowedBy` instead — which is
 * what lets a reader tell a per-author stream from a plain one. A member a
 * narrow VARIES ([varies]) is named there too even when the legs do agree on
 * it, because publishing thousands of author keys on every stats poll is a cost
 * the page has no use for. `legs` is how many (filter, relay) bands were folded
 * in.
 *
 * ## MIGRATION SHIM
 *
 * A file written before the format nested carries flat keys — `"<relay> <filter>"`
 * for a band, `"<relay>|<filter>"` for a sweep — which name no stream. The
 * router claims those into a stream the first time it asks for that pair, so
 * what is left in the file are pairs nothing has asked about yet, and dropping
 * them would chart a relay as un-walked while the band saying otherwise sits
 * right there. They are charted as UNNAMED groups, keyed by the filter's shape
 * the way every group used to be — see [shapeOf] and [varies]. That block, and
 * the router's own shim, go away together.
 */
internal object SyncCoverageReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /**
     * A filter member a discovery narrow can vary per relay: named in
     * `narrowedBy`, never published by value, and — pre-stream — starred out of
     * the shape a group is gathered under.
     *
     * The list is [DiscoveredRelay.narrowed]'s own: `authors`, `ids`, and tag
     * filters (`#p`, `#t`, …). `kinds` is the one member that narrow can set
     * which is NOT here — a stream split by kind is asking two different
     * questions, and it is small enough to print.
     *
     * A named group no longer GATHERS on this: its legs are grouped by the name
     * the router wrote, and what they disagree on is measured rather than
     * guessed ([Group.fold]).
     */
    private fun varies(member: String) = member == "authors" || member == "ids" || member.startsWith("#")

    /**
     * One group of the card — a stream, or (pre-stream) one filter shape —
     * accumulated as the two files are read.
     *
     * [marks] is held beside [rows] rather than inside them because a sweep can
     * exist for a pair that has NO band yet: the first sweep of a relay records
     * nothing until a leg finishes, and that relay being mid-walk is exactly
     * what the card is for.
     */
    private class Group(
        /** The stream the router wrote, or null for a pre-stream group. */
        val name: String?,
    ) {
        val rows = LinkedHashMap<String, Row>()
        val marks = LinkedHashMap<String, JsonObject>()

        /** How many (filter, relay) bands were folded in. */
        var legs = 0

        /** What every leg folded in so far agrees on, exactly. */
        var filter: JsonObject? = null
        private var seeded = false

        /** The members the legs disagree on, in the order they first differed. */
        val narrowedBy = LinkedHashSet<String>()

        /**
         * Fold one leg's filter into what the group can publish.
         *
         * Equality is EXACT, including array order: two members that differ
         * only in order would be reported as disagreeing, which is the safe
         * direction — one stream's filters are serialised by one writer from
         * one configured filter, so the order does not move under us.
         *
         * `since`/`until`/`limit` never survive: windowed reconciliation writes
         * a band per window, so the bounds on whichever leg parsed first are
         * that WINDOW's, and publishing them would read as the stream's own.
         */
        fun fold(leg: JsonObject) {
            val incoming = JsonObject(leg.filterKeys { it != "since" && it != "until" && it != "limit" })
            val current = filter
            if (!seeded || current == null) {
                filter = incoming
                seeded = true
                return
            }
            if (current == incoming) return
            val agreed = LinkedHashMap<String, JsonElement>()
            for ((member, value) in current) {
                if (incoming[member] == value) agreed[member] = value else narrowedBy += member
            }
            // A member only ONE side carries is a disagreement too: a stream
            // whose legs are `{kinds, authors}` and `{kinds}` does not ask
            // every relay for those authors.
            for (member in incoming.keys) if (!current.containsKey(member)) narrowedBy += member
            filter = JsonObject(agreed)
        }

        /**
         * One relay's band, MERGED into any this group already holds for it,
         * never overwritten. A relay reached by several legs of one
         * author-narrowed stream lands here once per leg, and taking the last
         * one silently charted whichever author the file happened to write last
         * as the relay's whole coverage.
         */
        fun band(
            relay: String,
            o: JsonObject,
        ) {
            val min = o["min"]?.jsonPrimitive?.longOrNull ?: return
            val max = o["max"]?.jsonPrimitive?.longOrNull ?: return
            val (everyMin, everyMax) = narrowed(o, min, max)
            legs++
            val row =
                Row(
                    relay = relay,
                    min = min,
                    max = max,
                    complete = o["complete"]?.jsonPrimitive?.booleanOrNull ?: false,
                    fullAt = o["fullAt"]?.jsonPrimitive?.longOrNull ?: 0L,
                    everyKindMin = everyMin,
                    everyKindMax = everyMax,
                )
            rows.merge(relay, row) { a, b -> a.mergedWith(b) }
        }

        /**
         * One relay's in-flight slice. Merged for the same reason the bands
         * are: one relay can be mid-sweep on several legs of one stream at
         * once. The widest slice, stamped with the most recent advance — a
         * cursor is "here is what is moving right now", and two of them moving
         * is still one relay moving.
         */
        fun mark(
            relay: String,
            o: JsonObject,
        ) {
            marks.merge(relay, o) { a, b -> widest(a, b) }
        }
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

        // group id → group. A LinkedHashMap the whole way down: the file's own
        // order is the only ordering the router offers, and a report that
        // reshuffles its groups between rollups is one nobody can diff. The id
        // is the stream name for a named group and the filter's shape for a
        // pre-stream one, kept apart by a prefix so a stream cannot collide
        // with a shape that happens to spell the same thing.
        val groups = LinkedHashMap<String, Group>()

        /**
         * MIGRATION SHIM — the group a flat, pre-stream key belongs to, found
         * by the filter's shape because the key names no stream.
         *
         * Parsed ONCE per distinct filter and only the shape retained, never
         * the parsed filter: the narrowed streams this groups across are the
         * exact case where every relay's filter is DIFFERENT, so holding each
         * parsed object would turn a pure cache miss into a heap copy of the
         * whole file — on a parser whose one hard rule is that it must never
         * cost the relay its rollup. Null is cached as null: a filter the
         * parser rejects is rejected once, not re-attempted per relay.
         */
        val shapeCache = HashMap<String, String?>()

        fun preStream(filterJson: String): Group? {
            if (shapeCache.containsKey(filterJson)) {
                val shape = shapeCache[filterJson] ?: return null
                return groups.getOrPut("shape:$shape") { Group(null) }
            }
            val parsed = parse(filterJson)
            val shape = parsed?.let { shapeOf(it) }
            shapeCache[filterJson] = shape
            if (shape == null || parsed == null) return null
            val group = groups.getOrPut("shape:$shape") { Group(null) }
            // Folded on the MISS only. A plain stream writes one flat key per
            // relay carrying the byte-identical filter, so folding per call
            // would re-parse a discovery filter's thousands of authors 4,000
            // times — the very cost this cache exists to refuse.
            group.fold(parsed)
            return group
        }

        for ((streamOrFlatKey, value) in bands) {
            val o = value as? JsonObject ?: continue
            // MIGRATION SHIM. Told apart by SHAPE, not by the key: a
            // pre-stream entry is the band itself, a stream is filters all the
            // way down. A filter can never be named `min` — it is serialised
            // JSON and starts with `{`.
            if (o["min"] != null) {
                val (rawRelay, filterJson) = split(streamOrFlatKey, ' ') ?: continue
                preStream(filterJson)?.band(StatsYql.canonicalRelay(rawRelay), o)
                continue
            }
            val group = groups.getOrPut("stream:$streamOrFlatKey") { Group(streamOrFlatKey) }
            for ((filterJson, byRelay) in o) {
                val relays = byRelay as? JsonObject ?: continue
                if (relays.isEmpty()) continue
                // Once per distinct filter, which the nesting makes the same
                // thing as once per key — the reason this parser can afford to
                // hold a parsed filter at all (see the class header).
                val parsed = parse(filterJson) ?: continue
                group.fold(parsed)
                for ((rawRelay, band) in relays) {
                    // The same spelling the relay distribution table uses, so
                    // one relay is one string across the whole document.
                    // Applied to BOTH files and to the peer map below — it is a
                    // deterministic rename, so it cannot split a pair that the
                    // router wrote as one.
                    group.band(StatsYql.canonicalRelay(rawRelay), band as? JsonObject ?: continue)
                }
            }
        }

        val peers =
            (sweeps["peers"] as? JsonObject ?: JsonObject(emptyMap()))
                .mapKeys { StatsYql.canonicalRelay(it.key) }
        for ((streamOrFlatKey, value) in (sweeps["sweeps"] as? JsonObject ?: JsonObject(emptyMap()))) {
            val o = value as? JsonObject ?: continue
            // MIGRATION SHIM, told apart the same way — a filter can never be
            // named `downTo` either.
            if (o["downTo"] != null) {
                val (rawRelay, filterJson) = split(streamOrFlatKey, '|') ?: continue
                preStream(filterJson)?.mark(StatsYql.canonicalRelay(rawRelay), o)
                continue
            }
            val group = groups.getOrPut("stream:$streamOrFlatKey") { Group(streamOrFlatKey) }
            for ((filterJson, byRelay) in o) {
                val relays = byRelay as? JsonObject ?: continue
                if (relays.isEmpty()) continue
                // A stream can be sweeping its first leg with no band yet, and
                // that is the state the card exists for — so its filter is
                // published from the cursor when nothing else has said it.
                parse(filterJson)?.let { group.fold(it) }
                for ((rawRelay, mark) in relays) {
                    group.mark(StatsYql.canonicalRelay(rawRelay), mark as? JsonObject ?: continue)
                }
            }
        }

        // The frame every row is read against, and the one number that makes two
        // groups comparable. Taken from the data rather than configured: these
        // filters carry no `since`, so there is no target to measure against and
        // the honest frame is "as deep as anything here reaches".
        var from = Long.MAX_VALUE
        for (group in groups.values) {
            for (r in group.rows.values) if (r.min < from) from = r.min
            for (mark in group.marks.values) {
                val d = mark["downTo"]?.jsonPrimitive?.longOrNull ?: continue
                if (d < from) from = d
            }
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
            putJsonArray("streams") {
                for (group in groups.values) {
                    if (group.rows.isEmpty() && group.marks.isEmpty()) continue
                    add(stream(group, peers))
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
        group: Group,
        peers: Map<String, JsonElement>,
    ): JsonObject {
        val rows = group.rows
        val marks = group.marks
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
            // The router's own name for this group. Absent only for a
            // pre-stream group, whose flat keys never said one — the page falls
            // back to labelling those by their filter, as it did for every
            // group before the format nested.
            group.name?.let { put("name", it) }
            // What the legs agree on, MINUS the members a narrow varies. Those
            // are dropped even when every leg agrees on them: a discovery
            // filter's `authors` runs to thousands of hex keys, and this
            // document is served on every stats poll. Their NAMES survive in
            // `narrowedBy`, which is all the page draws from them.
            group.filter?.let { put("filter", JsonObject(it.filterKeys { m -> !varies(m) })) }
            val varying = LinkedHashSet(group.narrowedBy)
            group.filter?.keys?.forEach { if (varies(it)) varying += it }
            if (varying.isNotEmpty()) {
                putJsonArray("narrowedBy") { for (m in varying) add(m) }
                // Against `rows`, not `relays`: `legs` counts BANDS, and
                // `relays` counts bands ∪ cursors. A group with 5 bands on 2
                // relays plus 3 relays sweeping their first leg has legs=5
                // and relays=5, and comparing those two hid the merge that
                // this number exists to disclose. `rows` is the band-bearing
                // relays, so `legs > rows.size` is exactly "some relay was
                // reached more than once".
                if (group.legs > rows.size) put("legs", group.legs)
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
     * MIGRATION SHIM — a filter reduced to the key a pre-stream group is
     * gathered under.
     *
     * `since`/`until`/`limit` are dropped because [SweepState.keyFor] drops
     * them, and the members are ordered because two files' serializers are not
     * required to agree on key order. A member a discovery narrow varies per
     * relay ([varies]) keeps its NAME and loses its value, so the legs of one
     * stream land in one group — which is the guess the stream name replaces.
     * Everything else stays whole: a different ask has not been covered just
     * because this one was.
     */
    private fun shapeOf(filter: JsonObject): String =
        filter
            .filterKeys { it != "since" && it != "until" && it != "limit" }
            .toSortedMap()
            .entries
            .joinToString(",") { (k, v) -> if (varies(k)) "$k=*" else "$k=${canon(v)}" }

    /** Arrays ordered too — `[0,10002]` and `[10002,0]` are one filter. */
    private fun canon(v: JsonElement): String =
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
