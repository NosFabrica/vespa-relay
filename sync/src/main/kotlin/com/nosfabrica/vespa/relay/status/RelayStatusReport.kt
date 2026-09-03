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
 * **The PAGE is not where a single relay is looked up, and should not pretend
 * to be.** The rows are ordered worst-first and cut at [MAX_ROWS] precisely
 * because the table's job is *what is wrong on this mirror*, and the per-relay
 * table this replaces was 10,462 rows behind a filter box that nobody could
 * read. One named relay is a question for the document, which is public and is
 * the artifact the page is only one reader of:
 *
 * ```
 * curl -s localhost:7778/stats.json |
 *   jq '.sync.data.relays.rows[] | select(.relay == "wss://relay.example/")'
 * ```
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
 * ## The join is on the ASKS, and getting that wrong is not a detail
 *
 * A unit owes one ask PER BOUND AUTHOR where a `relaySource` select pairs
 * providers with relays, so a `contentViaOutbox` unit on a busy relay owes
 * dozens. The first version of this counted the bands a (stream, relay) pair
 * held and called the pair complete when all of THOSE were settled — so a unit
 * owing forty asks with one drained band read `complete`, which is the worst
 * answer a table whose whole job is *is this relay synced* can give, and it
 * gave it silently. `RelayStatusReportTest` pins that case by name.
 *
 * The denominator has to be what the unit OWES, and it is free:
 * [RosterBuilder.UnitAsks.identity] is already each ask's filter as JSON —
 * computed for the roster's own change detection — and that is the very string
 * [SyncBands.snapshot] keys a band under. So `askKeys` joins exactly, a band
 * for an ask the roster no longer makes is ignored rather than counted against
 * the unit, and `settled == askKeys.size` is the only thing that earns
 * `complete`.
 *
 * Asking [com.nosfabrica.vespa.relay.sync.SyncBands] per ask instead would cost
 * a `Filter.toJson()` that misses quartz's thousand-entry fingerprint cache —
 * a fresh serialisation of a 141-kind filter per row per poll. The snapshot is
 * already built for [SyncCoverageReport] on the same tick, so one walk of a map
 * already in hand answers every row.
 *
 * The relay is joined on `url.url` VERBATIM, not canonicalised: both sides are
 * the same normalized string by construction ([SyncBands] writes the pool's own
 * url), so normalising would be ~10,000 re-parses per tick to produce the
 * strings we started with — and, worse, a canonical form that is not the file's
 * key would join nothing at all.
 *
 * Public where [SyncCoverageReport] beside it is internal, and the difference
 * is which way the data flows: that one is handed maps this package already
 * holds, and this one is handed [PrimeUnit]s the POOL builds — so its type
 * crosses `SyncStatus`'s constructor exactly as `SyncBands` does, and for the
 * same reason.
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
    class PrimeUnit(
        /** The relay's normalized url, VERBATIM — see the class header on the join. */
        val relay: String,
        val stream: String,
        /**
         * EVERY ASK THIS UNIT OWES, as the filter JSON the band file keys by —
         * `RosterBuilder.UnitAsks.identity`, handed over by reference.
         *
         * The denominator of the whole row, and the reason it is this and not a
         * count: `settled == askKeys.size` is what earns `complete`, and a
         * count could only ever be compared against the bands that HAPPEN to
         * exist, which is the bug the class header names.
         */
        val askKeys: Set<String>,
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
        units: List<PrimeUnit>,
        nowSeconds: Long,
    ): JsonObject? {
        if (units.isEmpty()) return null
        val folded = fold(bandsDoc, units)
        val rows = units.mapIndexed { at, unit -> row(unit, folded[at]) }
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
                            // THE THREE THAT READ AS A FRACTION: what the unit
                            // owes, how much of it has any coverage, and how
                            // much is finished. `settled` against `asks` is the
                            // completeness of the pair and the only pair of
                            // numbers `complete` may be claimed from.
                            put("asks", r.asks)
                            if (r.bands > 0) put("bands", r.bands)
                            if (r.settled > 0) put("settled", r.settled)
                            // Absent rather than zero: a unit with no band has
                            // no edges, and a 1970 in either column would read
                            // as a walk that reached the epoch.
                            r.coveredFrom?.let { put("coveredFrom", it) }
                            r.coveredTo?.let { put("coveredTo", it) }
                            // The AGE and not the stamp. One number where two
                            // would say the same thing, and the age is the one
                            // a reader judges — a stamp alone needs the
                            // document's clock found and subtracted before it
                            // means anything.
                            r.verifiedAt?.let { put("verifiedAgoSec", (nowSeconds - it).coerceAtLeast(0)) }
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
    ): Row {
        val status =
            when {
                // An ask set this unit does not own any coverage for. The two
                // readings of that are the whole reason this table exists, and
                // only the abort tells them apart.
                band.bands == 0 -> if (unit.abortReason != null) REFUSED else NOT_STARTED

                // EVERY ask settled, and never merely every band: see the class
                // header. A unit that owes nothing cannot be complete either —
                // that is a roster this report cannot describe, not a finished
                // relay.
                unit.askKeys.isNotEmpty() && band.settled >= unit.askKeys.size -> COMPLETE

                else -> PAGING
            }
        return Row(
            relay = unit.relay,
            stream = unit.stream,
            status = status,
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

    /** What one unit's OWED asks have covered, gathered off the snapshot. */
    private class Folded {
        /** Owed asks with any coverage at all… */
        var bands = 0

        /** …and the share of those whose past is settled. */
        var settled = 0
        var min: Long? = null
        var max: Long? = null
        var verifiedAt: Long? = null
    }

    /**
     * ONE WALK of the snapshot, accumulating straight into the units.
     *
     * Indexed by the unit's position rather than gathered into a map first: the
     * only question this walk asks of a band is *does some unit own this
     * (stream, relay), and is this filter one of its asks*, so an intermediate
     * keyed by every (stream, relay) in the file would build rows for the
     * thousands of urls the roster no longer admits and then throw them away.
     *
     * Best-effort exactly as [SyncCoverageReport] is, and for the same reason:
     * this runs inside the status tick, and a band entry a future build writes
     * differently must cost a thinner table rather than the whole document.
     */
    private fun fold(
        doc: JsonObject?,
        units: List<PrimeUnit>,
    ): List<Folded> {
        val out = List(units.size) { Folded() }
        if (doc == null) return out
        val at = HashMap<String, Int>(units.size * 2)
        units.forEachIndexed { i, u -> at[key(u.stream, u.relay)] = i }
        for ((stream, byFilter) in doc) {
            val filters = byFilter as? JsonObject ?: continue
            for ((filter, byRelay) in filters) {
                val relays = byRelay as? JsonObject ?: continue
                for ((relay, entry) in relays) {
                    val band = entry as? JsonObject ?: continue
                    val i = at[key(stream, relay)] ?: continue
                    // The ask gate. A band for a filter this unit no longer
                    // asks — a provider pairing a scan has since dropped — is
                    // another unit's history, and counting it here would move
                    // a row's edges and its denominator alike.
                    if (filter !in units[i].askKeys) continue
                    val f = out[i]
                    f.bands++
                    // A band whose `complete` is absent is one this build does
                    // not understand; read as NOT settled, which is the claim
                    // that costs a re-walk rather than the one that skips
                    // history.
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
                    // The NEWEST reconcile across the unit's asks. `min` would
                    // be the safer-sounding choice and is the wrong one: asks
                    // are audited on independent clocks, so the oldest is
                    // whichever ask happens to be furthest from its turn, not
                    // a statement about the relay.
                    band["verifiedAt"]?.jsonPrimitive?.longOrNull?.takeIf { it > 0 }?.let { v ->
                        f.verifiedAt = maxOf(f.verifiedAt ?: v, v)
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
