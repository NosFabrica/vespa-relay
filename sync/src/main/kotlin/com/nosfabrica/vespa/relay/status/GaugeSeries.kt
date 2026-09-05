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
 * The last [MAX_SAMPLES] readings of the process gauges, one sample per rollup. The series
 * lives in the served document, carried across rollups by the merge and across restarts by
 * the stats file; nothing else holds it.
 */
internal object GaugeSeries {
    /**
     * The previous series with this tick's sample appended, or null when there is nothing to
     * sample and no history to keep. `at` is published because the cadence is configurable
     * and a restart leaves a hole.
     */
    fun next(
        previous: JsonObject?,
        health: JsonObject?,
        doc: JsonObject,
        nowSeconds: Long,
    ): JsonObject? {
        val prior = previous?.get("series") as? JsonObject
        // One sample per rollup: a republish without a new reading appends nothing.
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
        // No health yet is not a zero sample.
        if (sample.values.all { it == null } && prior == null) return null
        return buildJsonObject {
            putJsonArray("at") {
                for (t in tail((prior?.get("at") as? JsonArray).orEmpty().mapNotNull { num(it) } + nowSeconds)) add(t)
            }
            for (member in SERIES) {
                putJsonArray(member) {
                    // A gap is a null, never a zero or a carried-forward value.
                    val kept = (prior?.get(member) as? JsonArray).orEmpty().map { num(it) }
                    for (v in tail(kept + sample[member])) {
                        if (v == null) add(JsonNull) else add(v)
                    }
                }
            }
        }
    }

    private fun <T> tail(values: List<T>): List<T> = if (values.size <= MAX_SAMPLES) values else values.takeLast(MAX_SAMPLES)

    /** The gauges kept as a series, in draw order. `heapPct` is derived; the rest are copied from `health`. */
    internal val SERIES = listOf("arrivingPerSec", "eventsPerSec", "queued", "heapPct", "sockets")

    /** Bounded by count rather than age, so the document's size holds whatever the rollup cadence is. */
    internal const val MAX_SAMPLES = 60

    private fun num(value: JsonElement?): Long? = (value as? JsonPrimitive)?.longOrNull

    private fun text(value: JsonElement?): String? = (value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotBlank() }
}
