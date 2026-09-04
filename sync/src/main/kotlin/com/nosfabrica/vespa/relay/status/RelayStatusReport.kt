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

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * Where each prime (relay, stream) unit stands: one row per unit the roster
 * admits, worst first, cut at [MAX_ROWS].
 *
 * A row carries two independent readings. `syncStatus` is the past (how far
 * the backfill has got: `notStarted`, `paging`, `complete`, `refused`) and
 * `behindSec` is the present (the age of the newest event held). A tail is a
 * third fact belonging to neither: a tailed pair's present arrives live, so
 * the fault rule is stale and not tailed. The unit is the pool's pair rather
 * than the relay because one relay can be complete for one stream and never
 * started for another.
 *
 * The join is on the unit's owed asks, keyed by the filter JSON the band
 * snapshot is keyed under, and `complete` needs every owed ask settled. Urls
 * join verbatim: both sides write the pool's own normalized string.
 *
 * The table's job is what is wrong on this mirror; one named relay is a
 * question for the document (`jq '.sync.data.relays.rows[] | select(.relay == ...)'`).
 */
object RelayStatusReport {
    /** One prime unit as the pool knows it, built by `VisitPool.primeUnits`. */
    class PrimeUnit(
        /** The relay's normalized url, verbatim. */
        val relay: String,
        val stream: String,
        /** Every ask this unit owes, as the filter JSON the band snapshot keys by (`RosterBuilder.UnitAsks.identity`). */
        val askKeys: Set<String>,
        /** A worker is inside this unit's visit right now. */
        val visiting: Boolean,
        /** The worker is holding a live subscription. */
        val live: Boolean,
        /** The monitor's NIP-77 verdict: true speaks negentropy, false refused a NEG-OPEN, null unmeasured. */
        val speaksNegentropy: Boolean? = null,
        /** The filter width learned from this relay's own refusal ([FilterWidths]); null when it never complained. */
        val kindCap: Int? = null,
        val abortReason: String? = null,
        val abortSaid: String? = null,
        val abortAtSec: Long = 0,
    )

    /**
     * Fold [units] against [bandsDoc] (`SyncBands.snapshot()`, stream → filter
     * → relay → band). Null when there are no units, so a router with no visit
     * streams publishes no section.
     */
    fun build(
        bandsDoc: JsonObject?,
        units: List<PrimeUnit>,
        nowSeconds: Long,
    ): JsonObject? {
        if (units.isEmpty()) return null
        val folded = fold(bandsDoc, units)
        val rows = units.mapIndexed { at, unit -> row(unit, folded[at], nowSeconds) }
        val counts = rows.groupingBy { it.status }.eachCount()
        val fresh = rows.groupingBy { it.freshness }.eachCount()
        // Faults first across both axes, then coldest, then status, then the
        // url so two polls of an unchanged router produce the same document.
        val ordered =
            rows.sortedWith(
                compareByDescending<Row> { it.fault }
                    .thenByDescending { it.behindSec ?: Long.MAX_VALUE }
                    .thenBy { STATUS_ORDER.indexOf(it.status) }
                    .thenBy { it.relay }
                    .thenBy { it.stream },
            )
        return buildJsonObject {
            put("pairs", rows.size)
            // Both partitions as rows rather than members, so `complete` and
            // `counted` keep their one meaning each. Counted over every row,
            // not the cut list.
            putJsonArray("statuses") {
                for (status in STATUS_ORDER) {
                    add(
                        buildJsonObject {
                            put("syncStatus", status)
                            put("pairs", counts[status] ?: 0)
                        },
                    )
                }
            }
            putJsonArray("freshness") {
                for (bucket in FRESHNESS_ORDER) {
                    add(
                        buildJsonObject {
                            put("behind", bucket)
                            put("pairs", fresh[bucket] ?: 0)
                        },
                    )
                }
            }
            putJsonArray("rows") {
                for (r in ordered.take(MAX_ROWS)) {
                    add(
                        buildJsonObject {
                            put("relay", r.relay)
                            put("stream", r.stream)
                            put("syncStatus", r.status)
                            // `settled` against `asks` is the pair's completeness.
                            put("asks", r.asks)
                            if (r.bands > 0) put("bands", r.bands)
                            if (r.settled > 0) put("settled", r.settled)
                            // Absent rather than zero: a 1970 would read as a walk that reached the epoch.
                            r.coveredFrom?.let { put("coveredFrom", it) }
                            r.coveredTo?.let { put("coveredTo", it) }
                            r.behindSec?.let { put("behindSec", it) }
                            put("behind", r.freshness)
                            if (r.fault) put("fault", true)
                            r.speaksNegentropy?.let { put("negentropy", it) }
                            r.kindCap?.let { put("kindCap", it) }
                            r.verifiedAt?.let { put("verifiedAgoSec", (nowSeconds - it).coerceAtLeast(0)) }
                            if (r.visiting) put("visiting", true)
                            // `tailed`, not `live`: `live` is the root's list of held tails.
                            if (r.live) put("tailed", true)
                            r.abortReason?.let { put("refusedFor", it) }
                            r.abortSaid?.let { put("relaySaid", it) }
                            r.abortAtSec.takeIf { it > 0 }?.let { put("refusedAgoSec", (nowSeconds - it).coerceAtLeast(0)) }
                        },
                    )
                }
            }
            put("omitted", (rows.size - MAX_ROWS).coerceAtLeast(0))
        }
    }

    private class Row(
        val relay: String,
        val stream: String,
        val status: String,
        val behindSec: Long?,
        val freshness: String,
        val fault: Boolean,
        val speaksNegentropy: Boolean?,
        val kindCap: Int?,
        val asks: Int,
        val bands: Int,
        val settled: Int,
        val coveredFrom: Long?,
        val coveredTo: Long?,
        val verifiedAt: Long?,
        val visiting: Boolean,
        val live: Boolean,
        val abortReason: String?,
        val abortSaid: String?,
        val abortAtSec: Long,
    )

    private fun row(
        unit: PrimeUnit,
        band: Folded,
        nowSeconds: Long,
    ): Row {
        val status =
            when {
                // No coverage at all: only the abort tells refused from not yet reached.
                band.bands == 0 -> if (unit.abortReason != null) REFUSED else NOT_STARTED

                // Every owed ask settled, never merely every band. A unit
                // owing nothing cannot be complete.
                unit.askKeys.isNotEmpty() && band.settled >= unit.askKeys.size -> COMPLETE

                else -> PAGING
            }
        // Clamped at zero: a future-dated `created_at` would read as a mirror ahead of the network.
        val behind = band.max?.let { (nowSeconds - it).coerceAtLeast(0) }
        return Row(
            relay = unit.relay,
            stream = unit.stream,
            status = status,
            behindSec = behind,
            freshness = freshnessOf(behind),
            // A tailed pair is never stale whatever its age: its present arrives live.
            fault = status == REFUSED || status == NOT_STARTED || (behind != null && behind >= STALE_SEC && !unit.live),
            speaksNegentropy = unit.speaksNegentropy,
            kindCap = unit.kindCap,
            asks = unit.askKeys.size,
            bands = band.bands,
            settled = band.settled,
            coveredFrom = band.min,
            coveredTo = band.max,
            verifiedAt = band.verifiedAt,
            visiting = unit.visiting,
            live = unit.live,
            abortReason = unit.abortReason,
            abortSaid = unit.abortSaid,
            abortAtSec = unit.abortAtSec,
        )
    }

    /** What one unit's owed asks have covered, gathered off the snapshot. */
    private class Folded {
        /** Owed asks with any coverage. */
        var bands = 0

        /** Owed asks whose past is settled. */
        var settled = 0
        var min: Long? = null
        var max: Long? = null
        var verifiedAt: Long? = null
    }

    /**
     * One walk of the snapshot, accumulating straight into the units by index.
     * Best-effort like [SyncCoverageReport]: an entry this build does not
     * understand costs a thinner table, not the document.
     */
    private fun fold(
        doc: JsonObject?,
        units: List<PrimeUnit>,
    ): List<Folded> {
        val out = List(units.size) { Folded() }
        if (doc == null) return out
        // Nested stream → relay → index, so streams the roster no longer names
        // are skipped whole rather than walked per band.
        val at = HashMap<String, HashMap<String, Int>>()
        units.forEachIndexed { i, u -> at.getOrPut(u.stream) { HashMap() }[u.relay] = i }
        for ((stream, byFilter) in doc) {
            val inStream = at[stream] ?: continue
            val filters = byFilter as? JsonObject ?: continue
            for ((filter, byRelay) in filters) {
                val relays = byRelay as? JsonObject ?: continue
                for ((relay, entry) in relays) {
                    val band = entry as? JsonObject ?: continue
                    val i = inStream[relay] ?: continue
                    // A band for a filter this unit no longer asks is another
                    // unit's history.
                    if (filter !in units[i].askKeys) continue
                    val f = out[i]
                    f.bands++
                    // An absent `complete` reads as not settled: the claim that costs a re-walk, not the one that skips history.
                    if (band["complete"]?.jsonPrimitive?.booleanOrNull == true) f.settled++
                    band["min"]
                        ?.jsonPrimitive
                        ?.longOrNull
                        ?.takeIf { it > 0 }
                        ?.let { m -> f.min = minOf(f.min ?: m, m) }
                    band["max"]
                        ?.jsonPrimitive
                        ?.longOrNull
                        ?.takeIf { it > 0 }
                        ?.let { m -> f.max = maxOf(f.max ?: m, m) }
                    // The newest reconcile: asks are audited on independent
                    // clocks, so the oldest says nothing about the relay.
                    band["verifiedAt"]?.jsonPrimitive?.longOrNull?.takeIf { it > 0 }?.let { v ->
                        f.verifiedAt = maxOf(f.verifiedAt ?: v, v)
                    }
                }
            }
        }
        return out
    }

    /** Visited, and no band could be written. */
    const val REFUSED = "refused"

    /** On the roster, no band, no abort recorded. */
    const val NOT_STARTED = "notStarted"

    /** Bands exist and at least one is not settled. */
    const val PAGING = "paging"

    /** Every owed ask is settled. */
    const val COMPLETE = "complete"

    /** Newest event within the hour. */
    const val CURRENT = "current"

    const val TODAY = "today"

    const val THIS_WEEK = "thisWeek"

    /** Older than [STALE_SEC], which untailed is a fault. */
    const val OLDER = "older"

    /** Nothing held, so no age to state. */
    const val NOTHING = "nothing"

    /** Four ages on a log-ish scale and an absence; a linear scale would put every healthy pair in one bucket. */
    internal fun freshnessOf(behindSec: Long?): String =
        when {
            behindSec == null -> NOTHING
            behindSec < 3_600 -> CURRENT
            behindSec < 86_400 -> TODAY
            behindSec < STALE_SEC -> THIS_WEEK
            else -> OLDER
        }

    /** Well past every revisit and audit cadence the pool runs; both the bucket edge and the fault bound. */
    const val STALE_SEC = 7 * 86_400L

    /** Freshest first, unlike [STATUS_ORDER]. */
    val FRESHNESS_ORDER = listOf(CURRENT, TODAY, THIS_WEEK, OLDER, NOTHING)

    /** Worst first; both the published member order and the row sort. */
    val STATUS_ORDER = listOf(REFUSED, NOT_STARTED, PAGING, COMPLETE)

    /** The counts above the rows are complete whatever this is, and the sort puts every fault before the cut. */
    const val MAX_ROWS = 1_000
}
