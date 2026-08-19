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

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.putJsonArray

/**
 * THE LAST HOUR OF THE FOUR PROCESS GAUGES, appended one sample per rollup.
 *
 * The one thing the status document cannot state from a single tick. Everything
 * else on the mirror's card is a reading `SyncProgress` already holds; this is
 * the reading BESIDE the ones before it, and it is the whole reason the status
 * rollup has state at all.
 *
 * It is what survives of `SyncProgressReport`, which used to rebuild the entire
 * progress document member by member. That defence was owed to a FILE — the
 * mirror wrote one, the serving relay read it off a shared volume, and a
 * hand-edited or half-migrated file must not be able to put arbitrary JSON into
 * a page served under the relay's name. The mirror serves its own page now, so
 * the writer and the reader are one object on one heap and there is nothing to
 * defend against: ~700 lines of re-copying our own members went with the
 * boundary that justified them. This did not, because it was never a copy.
 */
internal object GaugeSeries {
    /**
     * The series this rollup should serve: the previous one with this tick's
     * sample appended.
     *
     * ## Why an instant was not enough
     *
     * Every gauge on this card is a level, and not one operator question about
     * a level is answerable from one reading. `heap 45%` says nothing; heap 45%
     * and climbing three points a minute says everything. A queue at 4,101 of
     * 4,096 is the constraint if it has been there for ten minutes and is noise
     * if it filled this second. "Is it stuck" is a derivative, and the card was
     * answering it with thresholds — which is the wrong instrument, and is why
     * the thresholds always felt arbitrary.
     *
     * ## Where it is kept
     *
     * In the DOCUMENT, appended to whatever the previously served one carried.
     * Nothing new holds it: `StatsSnapshot` already merges each tier into the
     * document it is serving and already persists that document to `STATS_FILE`,
     * so a series that lives there is carried across rollups by the merge and
     * across restarts by the file, with no ring buffer, no scheduler and no
     * second lifetime to reason about. A relay that has just started serves the
     * history its last run wrote.
     *
     * ## What is in it, and what is deliberately not
     *
     * The four PROCESS gauges, because they are the ones that decide whether
     * there is a problem at all, and because they are four scalars rather than
     * a shape that changes with the deployment. Per-stream and per-processor
     * series are not here and are not an oversight: the alias fold runs on a
     * six-hour clock, so at this cadence an hour of samples would not contain
     * one of its passes. That needs a different window and is a different
     * feature.
     *
     * `at` is published beside the values rather than an interval being
     * assumed. The rollup cadence is an operator's env var, a restart leaves a
     * hole, and a reader drawing evenly spaced points over an uneven series
     * would draw a smooth line through a gap.
     */
    fun next(
        previous: JsonObject?,
        health: JsonObject?,
        doc: JsonObject,
        nowSeconds: Long,
    ): JsonObject? {
        val prior = previous?.get("series") as? JsonObject
        // The one thing that must not happen: a sample per REQUEST rather than
        // per rollup. `build` is called once per counters tick, but a document
        // republished without a new reading would still append — so a sample
        // whose clock has not moved past the last one is the same instant and
        // is dropped.
        val lastAt = (prior?.get("at") as? JsonArray)?.lastOrNull()?.let { num(it) }
        if (lastAt != null && nowSeconds <= lastAt) return prior
        val sample =
            SERIES.associateWith { member ->
                when (member) {
                    "heapPct" -> {
                        val used = num(health?.get("heapUsedMb"))
                        val max = num(health?.get("heapMaxMb"))
                        if (used != null && max != null && max > 0) used * 100 / max else null
                    }

                    "queued" -> {
                        num(
                            (doc["processors"] as? JsonArray)
                                ?.filterIsInstance<JsonObject>()
                                ?.firstOrNull { text(it["name"]) == "ingest" }
                                ?.get("queued"),
                        )
                    }

                    else -> {
                        num(health?.get(member))
                    }
                }
            }
        // Nothing to sample is not a zero sample. A router too old to publish
        // health, or one whose first health tick has not fired, would otherwise
        // lay down an hour of flat zeroes that read as a dead mirror.
        if (sample.values.all { it == null } && prior == null) return null
        return buildJsonObject {
            putJsonArray("at") {
                for (t in tail((prior?.get("at") as? JsonArray).orEmpty().mapNotNull { num(it) } + nowSeconds)) add(t)
            }
            for (member in SERIES) {
                putJsonArray(member) {
                    // A gap is published as a NULL, never as a zero or a
                    // carried-forward value: the reader has to be able to tell
                    // "the router said nothing" from "the router said none".
                    val kept = (prior?.get(member) as? JsonArray).orEmpty().map { num(it) }
                    for (v in tail(kept + sample[member])) {
                        if (v == null) add(JsonNull) else add(v)
                    }
                }
            }
        }
    }

    /** The last [MAX_SAMPLES] of a series, so the ring is bounded by the document rather than by a clock. */
    private fun <T> tail(values: List<T>): List<T> = if (values.size <= MAX_SAMPLES) values else values.takeLast(MAX_SAMPLES)

    /**
     * The gauges kept as a series, in draw order — see [next].
     *
     * `heapPct` is derived rather than copied: a percentage is what a reader
     * compares across samples, and publishing both halves of the pair sixty
     * times over to let the page divide them would triple the cost of the
     * feature for nothing.
     */
    internal val SERIES = listOf("eventsPerSec", "queued", "heapPct", "sockets")

    /**
     * How many samples the ring holds — an hour at the stock 60s counters
     * cadence, and however long that many ticks is at any other.
     *
     * Bounded by COUNT rather than by age on purpose: the bound has to hold
     * whatever an operator sets `STATS_COUNTERS_INTERVAL_SECONDS` to, and this
     * one costs the document about 1.2KB whatever that is.
     */
    internal const val MAX_SAMPLES = 60

    /** Read as a number, or null — a member the document does not carry is not a zero. */
    private fun num(value: JsonElement?): Long? = (value as? JsonPrimitive)?.longOrNull

    /** Read as a non-blank string, or null. */
    private fun text(value: JsonElement?): String? = (value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotBlank() }
}
