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
package com.nosfabrica.vespa.relay.status

import com.nosfabrica.vespa.relay.util.canonicalRelay
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
 * The bands and sweep cursors folded into per-stream coverage groups the status page can
 * chart. A group's `filter` is what every leg agrees on exactly; members the legs disagree on
 * are named in `narrowedBy`. Best-effort: a shape this build does not understand costs a card.
 */
internal object SyncCoverageReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /** A filter member a discovery narrow can vary per relay: named in `narrowedBy`, never published. */
    private fun varies(member: String) = member == "authors" || member == "ids" || member.startsWith("#")

    /** One group of the card. [marks] sits beside [rows] because a sweep can exist before a band does. */
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
         * Fold one leg's filter into what the group can publish. Equality is exact, array order
         * included; `since`/`until`/`limit` never survive, being one window's bounds.
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
            // A member only one side carries is a disagreement too.
            for (member in incoming.keys) if (!current.containsKey(member)) narrowedBy += member
            filter = JsonObject(agreed)
        }

        /** One relay's band, merged into any this group already holds for it. */
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

        /** One relay's in-flight slice, merged the same way. */
        fun mark(
            relay: String,
            o: JsonObject,
        ) {
            marks.merge(relay, o) { a, b -> widest(a, b) }
        }
    }

    /** One relay's coverage of one group, before it is written out. */
    private data class Row(
        val relay: String,
        val min: Long,
        val max: Long,
        val complete: Boolean,
        val fullAt: Long,
        // The part of [min]..[max] every kind in the filter has been walked over.
        val everyKindMin: Long,
        val everyKindMax: Long,
    ) {
        /**
         * Two legs of one stream against one relay, as one row: outer edges union, `complete`
         * ands, the every-kind window intersects and falls back to the outer edges.
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

    /** The string form, kept for tests that state their fixtures as the files on disk. */
    fun build(
        bandsJson: String?,
        sweepsJson: String?,
        nowSeconds: Long,
    ): JsonObject? = build(parse(bandsJson), parse(sweepsJson), nowSeconds)

    /**
     * Fold [SyncBands.snapshot] and [SweepState.snapshot] into the `data` of a `sync` section.
     * Null when there is nothing to say: "does not mirror" and "has covered nothing" differ.
     */
    fun build(
        bandsDoc: JsonObject?,
        sweepsDoc: JsonObject?,
        nowSeconds: Long,
    ): JsonObject? {
        val bands = bandsDoc ?: JsonObject(emptyMap())
        val sweeps = sweepsDoc ?: JsonObject(emptyMap())
        if (bands.isEmpty() && sweeps.isEmpty()) return null

        // Insertion-ordered so the report diffs between rollups; the prefix keeps stream and
        // shape ids from colliding.
        val groups = LinkedHashMap<String, Group>()

        /** The group a flat, pre-stream key belongs to, by the filter's shape; null is cached too. */
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
            // Folded on the miss only: a plain stream repeats one filter per relay.
            group.fold(parsed)
            return group
        }

        for ((streamOrFlatKey, value) in bands) {
            val o = value as? JsonObject ?: continue
            // A pre-stream entry is the band itself; a filter can never be named `min`.
            if (o["min"] != null) {
                val (rawRelay, filterJson) = split(streamOrFlatKey, ' ') ?: continue
                preStream(filterJson)?.band(canonicalRelay(rawRelay), o)
                continue
            }
            val group = groups.getOrPut("stream:$streamOrFlatKey") { Group(streamOrFlatKey) }
            for ((filterJson, byRelay) in o) {
                val relays = byRelay as? JsonObject ?: continue
                if (relays.isEmpty()) continue
                val parsed = parse(filterJson) ?: continue
                group.fold(parsed)
                for ((rawRelay, band) in relays) {
                    // One spelling for both inputs and the peer map, so it cannot split a pair.
                    group.band(canonicalRelay(rawRelay), band as? JsonObject ?: continue)
                }
            }
        }

        val peers =
            (sweeps["peers"] as? JsonObject ?: JsonObject(emptyMap()))
                .mapKeys { canonicalRelay(it.key) }
        for ((streamOrFlatKey, value) in (sweeps["sweeps"] as? JsonObject ?: JsonObject(emptyMap()))) {
            val o = value as? JsonObject ?: continue
            // A filter can never be named `downTo` either.
            if (o["downTo"] != null) {
                val (rawRelay, filterJson) = split(streamOrFlatKey, '|') ?: continue
                preStream(filterJson)?.mark(canonicalRelay(rawRelay), o)
                continue
            }
            val group = groups.getOrPut("stream:$streamOrFlatKey") { Group(streamOrFlatKey) }
            for ((filterJson, byRelay) in o) {
                val relays = byRelay as? JsonObject ?: continue
                if (relays.isEmpty()) continue
                // A stream on its first sweep has no band yet, so its filter comes from the cursor.
                parse(filterJson)?.let { group.fold(it) }
                for ((rawRelay, mark) in relays) {
                    group.mark(canonicalRelay(rawRelay), mark as? JsonObject ?: continue)
                }
            }
        }

        // The frame every row is read against: as deep as anything here reaches.
        var from = Long.MAX_VALUE
        for (group in groups.values) {
            for (r in group.rows.values) if (r.min < from) from = r.min
            for (mark in group.marks.values) {
                val d = mark["downTo"]?.jsonPrimitive?.longOrNull ?: continue
                if (d < from) from = d
            }
        }
        if (from == Long.MAX_VALUE) return null
        // `created_at` is author-signed, so a band can sit ahead of now; the frame must not invert.
        if (from > nowSeconds) from = nowSeconds

        val published = groups.values.filter { it.rows.isNotEmpty() || it.marks.isNotEmpty() }
        return buildJsonObject {
            put("from", from)
            put("to", nowSeconds)
            // Distinct across streams, or a relay counts once per stream that walked it.
            val everyRelay = published.flatMap { it.rows.keys + it.marks.keys }.distinct()
            put("relays", everyRelay.size)
            put("hosts", everyRelay.mapNotNull { it.substringAfter("://").substringBefore("/").ifEmpty { null } }.distinct().size)
            put("rows", published.sumOf { (it.rows.keys + it.marks.keys).size })
            putJsonArray("streams") {
                for (group in published) add(stream(group, peers))
            }
        }
    }

    /**
     * Two sweep cursors on one relay, as the one slice they jointly cover. A cursor missing an
     * edge loses to one that has both rather than poisoning the pair.
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

    /** A cursor's span, or null when either edge is unreadable; `sweeping` and the row's `sweep` share it. */
    private fun span(m: JsonObject): Pair<Long, Long>? {
        val downTo = m["downTo"]?.jsonPrimitive?.longOrNull ?: return null
        val upTo = m["upTo"]?.jsonPrimitive?.longOrNull ?: return null
        return downTo to upTo
    }

    private fun stream(
        group: Group,
        peers: Map<String, JsonElement>,
    ): JsonObject {
        val rows = group.rows
        val marks = group.marks
        // Deepest first; a relay with a sweep but no band sorts last.
        val relays =
            (rows.keys + marks.keys)
                .map { it to (rows[it]?.min ?: Long.MAX_VALUE) }
                .sortedBy { it.second }
                .map { it.first }
        // Beside `relays`, not instead of it: the gap is the url inflation the fold has not decided.
        val hosts = relays.mapNotNull { it.substringAfter("://").substringBefore("/").ifEmpty { null } }.distinct().size
        return buildJsonObject {
            group.name?.let { put("name", it) }
            // Said rather than left to be inferred from a missing member.
            if (group.name == null) put("unnamed", true)
            // Varying members are dropped even when every leg agrees on them: a discovery
            // filter's `authors` runs to thousands of keys.
            group.filter?.let { put("filter", JsonObject(it.filterKeys { m -> !varies(m) })) }
            val varying = LinkedHashSet(group.narrowedBy)
            group.filter?.keys?.forEach { if (varies(it)) varying += it }
            if (varying.isNotEmpty()) {
                putJsonArray("narrowedBy") { for (m in varying) add(m) }
                // Against `rows`, not `relays`, which also counts cursors: `legs > rows.size` is
                // exactly "some relay was reached more than once".
                if (group.legs > rows.size) put("legs", group.legs)
            }
            put("relays", relays.size)
            put("hosts", hosts)
            // Settled against still open, not negentropy against REQ: a drained paged walk
            // earns `complete` too.
            put("reconciled", rows.values.count { it.complete })
            put("paged", rows.values.count { !it.complete })
            // Only the cursors the rows below will describe.
            put("sweeping", marks.values.count { span(it) != null })
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
                                // Only when the per-kind spans disagree; equal spans are the overwhelming case.
                                if (r.everyKindMin != r.min || r.everyKindMax != r.max) {
                                    put("everyKindMin", r.everyKindMin)
                                    put("everyKindMax", r.everyKindMax)
                                }
                            }
                            // The learned window size is a property of the peer, keyed by relay alone.
                            peers[relay]?.jsonObject?.let { p ->
                                p["target"]?.jsonPrimitive?.longOrNull?.let { put("target", it) }
                                p["cap"]?.jsonPrimitive?.longOrNull?.let { put("cap", it) }
                            }
                            marks[relay]?.let { m ->
                                span(m)?.let { (downTo, upTo) ->
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
     * The intersection of a band's per-kind `spans`: the widest window inside which no kind is
     * missing, since the outer edges let one long-lived kind vouch for a short-lived one.
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
        // Disjoint kinds are a real state and must not come back inverted.
        if (seen == 0 || lo > hi) return min to max
        return lo to hi
    }

    /**
     * A filter reduced to the key a pre-stream group is gathered under: time bounds dropped,
     * members ordered, and a varying member keeping its name but not its value.
     */
    private fun shapeOf(filter: JsonObject): String =
        filter
            .filterKeys { it != "since" && it != "until" && it != "limit" }
            .toSortedMap()
            .entries
            .joinToString(",") { (k, v) -> if (varies(k)) "$k=*" else "$k=${canon(v)}" }

    /** Arrays ordered too: `[0,10002]` and `[10002,0]` are one filter. */
    private fun canon(v: JsonElement): String =
        runCatching {
            v.jsonArray
                .map { it.toString() }
                .sorted()
                .joinToString(",", "[", "]")
        }.getOrElse { v.toString() }

    /** Split a key at its first separator; a normalized relay url contains neither. */
    private fun split(
        key: String,
        sep: Char,
    ): Pair<String, String>? {
        val at = key.indexOf(sep)
        if (at <= 0 || at == key.length - 1) return null
        return key.substring(0, at) to key.substring(at + 1)
    }

    /**
     * A corrupt input costs this card, not the rollup. `Exception`, not `Throwable`: an
     * `OutOfMemoryError` belongs to the rollup loop, which keeps serving the previous document.
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
