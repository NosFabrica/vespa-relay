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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * WHERE EACH PRIME RELAY STANDS — one row per relay this router is allowed to
 * dial, and what its sync of that relay has actually reached.
 *
 * ## The question no card could answer
 *
 * Everything the mirror published was an AGGREGATE. `roster` counted the prime
 * relays, the coverage card charted their bands folded into per-stream groups,
 * and the in-flight tables named the handful a worker happened to be holding at
 * that instant. So "is this relay synced" — the question an operator actually
 * arrives with, usually about ONE relay somebody complained about — could be
 * answered for a relay only while it was being visited, and never afterwards.
 * Worse, the two states that matter most were the same absence: a relay the
 * pool has never reached and a relay it reaches every few minutes and is
 * refused by both have no band, and the coverage card draws neither, because
 * its denominator is *relays this stream has touched*.
 *
 * ## The four answers, and what each rests on
 *
 * The unit is the pool's own — a (relay, stream) PAIR — because that is what
 * has a state: one relay can be complete for `indexers` and have never started
 * for `contentViaOutbox`, and a row per relay would have to pick one of those
 * to report.
 *
 *  - **`complete`** — every band this unit holds is complete, which is quartz's
 *    word for *the past below this is settled*: a paged leg that DRAINED (the
 *    relay EOSEd an empty page) or a finished negentropy reconcile. `verifiedAt`
 *    beside it is the last reconcile's own stamp, which is the closest thing
 *    this router has to a *last synced* time; where no reconcile has ever run,
 *    `coveredTo` — the newest event we hold from it — is what there is.
 *  - **`paging`** — bands exist and at least one is not settled, so the walk is
 *    still working backwards. `coveredFrom` is how far back it has REACHED, and
 *    it is the number to read: a relay whose `coveredFrom` has not moved
 *    between two polls is a relay whose walk is not advancing.
 *  - **`refused`** — the pool visited it and could write no band, with the
 *    reason and the relay's own sentence from [com.nosfabrica.vespa.relay.sync.VisitAborts].
 *    This is the row that did not exist before: it is indistinguishable from
 *    `notStarted` in the band file and it is the opposite finding.
 *  - **`notStarted`** — on the roster, no band, and no abort recorded. Either
 *    the queue has not reached it yet or the process has only just booted.
 *
 * A unit that HAS bands and has also aborted keeps the band's status and
 * carries the refusal beside it, because both are true and the pair is the
 * useful reading: *complete, and the last visit was turned away* is a relay
 * that has stopped being maintained.
 *
 * ## Why it folds the band SNAPSHOT rather than asking the bands
 *
 * The obvious implementation — ask [com.nosfabrica.vespa.relay.sync.SyncBands]
 * for each ask's band — costs a `Filter.toJson()` per ask that misses quartz's
 * fingerprint cache, and that cache holds a thousand entries against a roster
 * whose author-bound asks run to tens of thousands. It would be a fresh
 * serialisation of a 141-kind filter per row per poll.
 *
 * The snapshot is already built for [SyncCoverageReport] on the same tick and
 * is keyed the way this needs — stream, then filter, then relay — so one extra
 * walk of a map that is already in hand answers every row at once. That is also
 * why the join is on the relay's canonical url ([canonicalRelay]) rather than
 * on a filter: the roster's asks and the file's keys agree on the URL and need
 * not agree on anything else.
 *
 * Public where [SyncCoverageReport] beside it is internal, and the difference
 * is which way the data flows: that one is handed maps this package already
 * holds, and this one is handed [Unit]s the POOL builds — so its type crosses
 * `SyncStatus`'s constructor exactly as `SyncBands` does, and for the same
 * reason.
 */
object RelayStatusReport {
    /**
     * One prime (relay, stream) unit as the POOL knows it — the half of a row
     * that no file holds.
     *
     * Built by `VisitPool.primeUnits`, which is the only thing that can: the
     * roster is a live rebuild off the monitor's verdicts, and a relay that
     * leaves it stops having a row rather than becoming a stale one.
     */
    class Unit(
        val relay: String,
        val stream: String,
        /** How many asks this unit owes — one per bound author on a scanning stream. */
        val asks: Int,
        /** A worker is inside this unit's visit right now. */
        val visiting: Boolean,
        /** …and it is holding a live subscription. */
        val live: Boolean,
        /** The last time this unit ended early, if it ever has. */
        val abortReason: String? = null,
        val abortSaid: String? = null,
        val abortAtSec: Long = 0,
    )

    /**
     * Fold [units] against [bandsDoc] — `SyncBands.snapshot()`, stream → filter
     * → relay → band.
     *
     * Null when there are no units at all, which is a router with no visit
     * streams rather than one with nothing to say — the section is then absent,
     * on the same rule the store section follows.
     */
    fun build(
        bandsDoc: JsonObject?,
        units: List<Unit>,
        nowSeconds: Long,
    ): JsonObject? {
        if (units.isEmpty()) return null
        val bands = foldBands(bandsDoc)
        val rows = units.map { row(it, bands[key(it.stream, it.relay)]) }
        val counts = rows.groupingBy { it.status }.eachCount()
        // WORST FIRST, and the order is the whole usability of a list this
        // long: an operator opens it because something is wrong, and a table
        // sorted by relay name would put the four broken rows on page nine.
        // Inside a status, the least-covered first for the same reason —
        // and the url last so two polls of an unchanged router produce the
        // same document.
        val ordered =
            rows.sortedWith(
                compareBy<Row> { STATUS_ORDER.indexOf(it.status) }
                    .thenBy { it.coveredFrom ?: Long.MAX_VALUE }
                    .thenBy { it.relay }
                    .thenBy { it.stream },
            )
        return buildJsonObject {
            put("pairs", rows.size)
            // THE PARTITION AS ROWS, not as four members of its own.
            //
            // `complete` is already a BAND's member in this document and
            // `counted` is already the root's sentence about what it counts, so
            // publishing the statuses as members would have put one word on two
            // quantities twice over — the exact overload `StatusVocabularyTest`
            // exists to catch. As rows the vocabulary is `syncStatus` and
            // `pairs`, each glossed once and meaning one thing.
            //
            // Published WHOLE even where the row list below is cut: the counts
            // are what close, and a truncated list must never be the only place
            // a status is counted.
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
            putJsonArray("rows") {
                for (r in ordered.take(MAX_ROWS)) {
                    add(
                        buildJsonObject {
                            put("relay", r.relay)
                            put("stream", r.stream)
                            put("syncStatus", r.status)
                            put("asks", r.asks)
                            // Absent rather than zero: a unit with no band has
                            // no edges, and a 1970 in either column would read
                            // as a walk that reached the epoch.
                            r.coveredFrom?.let { put("coveredFrom", it) }
                            r.coveredTo?.let { put("coveredTo", it) }
                            // The AGE and not the stamp. One number where two
                            // would say the same thing, and the age is the one
                            // a reader judges — `verifiedAt` alone needs the
                            // document's clock found and subtracted before it
                            // means anything.
                            r.verifiedAt?.let { put("verifiedAgoSec", (nowSeconds - it).coerceAtLeast(0)) }
                            if (r.bands > 0) put("bands", r.bands)
                            if (r.visiting) put("visiting", true)
                            // `tailed`, not `live`: this document's `live` is
                            // the root's list of every held tail, and a boolean
                            // of the same name on a row would be a second
                            // meaning for it.
                            if (r.live) put("tailed", true)
                            r.abortReason?.let { put("refusedFor", it) }
                            r.abortSaid?.let { put("relaySaid", it) }
                            r.abortAtSec.takeIf { it > 0 }?.let { put("refusedAgoSec", (nowSeconds - it).coerceAtLeast(0)) }
                        },
                    )
                }
            }
            // Never silent about a cut, on the rule every list in this document
            // follows.
            put("omitted", (rows.size - MAX_ROWS).coerceAtLeast(0))
        }
    }

    private class Row(
        val relay: String,
        val stream: String,
        val status: String,
        val asks: Int,
        val bands: Int,
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
        unit: Unit,
        band: Folded?,
    ): Row {
        val status =
            when {
                band == null || band.bands == 0 -> if (unit.abortReason != null) REFUSED else NOT_STARTED
                band.allComplete -> COMPLETE
                else -> PAGING
            }
        return Row(
            relay = unit.relay,
            stream = unit.stream,
            status = status,
            asks = unit.asks,
            bands = band?.bands ?: 0,
            coveredFrom = band?.min,
            coveredTo = band?.max,
            verifiedAt = band?.verifiedAt,
            visiting = unit.visiting,
            live = unit.live,
            abortReason = unit.abortReason,
            abortSaid = unit.abortSaid,
            abortAtSec = unit.abortAtSec,
        )
    }

    /** Every band one unit holds, reduced to the four facts a row needs. */
    private class Folded(
        var bands: Int = 0,
        var min: Long? = null,
        var max: Long? = null,
        var verifiedAt: Long? = null,
        var allComplete: Boolean = true,
    )

    /**
     * One walk of the snapshot, gathering per (stream, relay).
     *
     * Best-effort exactly as [SyncCoverageReport] is, and for the same reason:
     * this runs inside the status tick, and a band entry a future build writes
     * differently must cost a thinner table rather than the whole document.
     */
    private fun foldBands(doc: JsonObject?): Map<String, Folded> {
        val out = HashMap<String, Folded>()
        if (doc == null) return out
        for ((stream, byFilter) in doc) {
            val filters = byFilter as? JsonObject ?: continue
            for ((_, byRelay) in filters) {
                val relays = byRelay as? JsonObject ?: continue
                for ((relay, entry) in relays) {
                    val band = entry as? JsonObject ?: continue
                    val folded = out.getOrPut(key(stream, canonicalRelay(relay))) { Folded() }
                    folded.bands++
                    // A band whose `complete` is absent is one this build does
                    // not understand; read as NOT settled, which is the claim
                    // that costs a re-walk rather than the one that skips
                    // history.
                    val complete = band["complete"]?.jsonPrimitive?.booleanOrNull ?: false
                    if (!complete) folded.allComplete = false
                    band["min"]?.jsonPrimitive?.longOrNull?.takeIf { it > 0 }?.let { m ->
                        folded.min = minOf(folded.min ?: m, m)
                    }
                    band["max"]?.jsonPrimitive?.longOrNull?.takeIf { it > 0 }?.let { m ->
                        folded.max = maxOf(folded.max ?: m, m)
                    }
                    // The NEWEST reconcile across the unit's asks. `min` would
                    // be the safer-sounding choice and is the wrong one: asks
                    // are audited on independent clocks, so the oldest is
                    // whichever ask happens to be furthest from its turn, not
                    // a statement about the relay.
                    band["verifiedAt"]?.jsonPrimitive?.longOrNull?.takeIf { it > 0 }?.let { v ->
                        folded.verifiedAt = maxOf(folded.verifiedAt ?: v, v)
                    }
                }
            }
        }
        return out
    }

    private fun key(
        stream: String,
        relay: String,
    ) = "$stream $relay"

    /** A unit the pool has visited and could write no band for — see the class header. */
    const val REFUSED = "refused"

    /** …and one it has not reached at all. */
    const val NOT_STARTED = "notStarted"

    /** Bands exist, and at least one of them is not settled. */
    const val PAGING = "paging"

    /** Every band this unit holds is settled. */
    const val COMPLETE = "complete"

    /**
     * The order the statuses are counted and drawn in, worst first.
     *
     * A list, not the map's iteration order: it is both the published member
     * order and the row sort, and those two agreeing is what makes the counts
     * above the table read as a key to it.
     */
    val STATUS_ORDER = listOf(REFUSED, NOT_STARTED, PAGING, COMPLETE)

    /**
     * How many rows are published.
     *
     * A four-stream router on a 700-relay roster is 2,800 units, and the whole
     * set on every poll is a document nobody can open for a table whose first
     * screen is the whole answer. The counts above it are complete whatever
     * this is, and the sort puts every row that names a fault before the cut.
     */
    const val MAX_ROWS = 1_000
}
