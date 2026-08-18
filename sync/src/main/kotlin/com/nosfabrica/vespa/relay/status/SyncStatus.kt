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

import com.nosfabrica.vespa.relay.progress.SyncProgress
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.time.Instant

/**
 * WHAT THIS MIRROR IS DOING, as its own `/stats.json` publishes it.
 *
 * ## Why this is not a section of the relay's document
 *
 * It was, and the relay could not honestly build it. The mirror wrote three
 * JSON files to a shared volume, and the serving relay read them back, re-parsed
 * them against an allowlist, and re-narrated them — about 2,500 lines whose only
 * job was to re-derive what the writer already knew. Two things were wrong with
 * that beyond the cost.
 *
 * A file cannot say whether the process writing it is alive. That is why the
 * progress document had to carry a `writtenAt` heartbeat and the reader had to
 * turn it into a `staleForSec`: without them "a mirror that has been down for a
 * day" and "a mirror mid-cycle" published the identical card. An HTTP request
 * answers that question by whether it answers, so the heartbeat and every
 * inference built on it are gone.
 *
 * And the re-parse was a defence against a boundary that no longer exists. The
 * relay was right to distrust another process's file — a hand-edited or
 * half-migrated one must not be able to put arbitrary JSON into a page served
 * under the relay's name. Here the writer and the reader are one object on one
 * heap, so `SyncProgress.latest` is served as it is built.
 *
 * ## What is left, and why each part earns it
 *
 * [SyncCoverageReport] stays whole: folding bands and sweep cursors into
 * per-stream groups and depth buckets is real computation, not a re-copy, and
 * it is the same computation wherever it runs. [GaugeSeries] stays because a
 * series is the one thing a single tick cannot state. [SyncVocabulary] ships
 * with the numbers it defines, so a chip can never describe a member in words
 * the router would not use.
 *
 * The envelope is deliberately the relay's: same `schema`/`generatedAt`/`tiers`
 * shape, same per-section `status`/`generatedAt`/`data`. The two pages share a
 * rendering engine, and a second envelope would be a second thing to keep in
 * step for no reader's benefit.
 */
class SyncStatus(
    private val bands: SyncBands,
    private val sweeps: SweepState,
    private val progress: SyncProgress,
    private val snapshot: StatsSnapshot,
    /**
     * How often [publish] is called, so the page can poll on the cadence the
     * document states rather than on a guess — see `everySeconds`.
     */
    private val everySeconds: Long,
) {
    /**
     * Build the document and hand it to [snapshot].
     *
     * Never throws. This runs on a timer beside the mirror's own work, and a
     * status page that takes the router down with it would be worse than no
     * status page — the failure is published INTO the document instead, under
     * the same `errors` key every section of the relay's document uses.
     */
    fun publish(nowSeconds: Long = System.currentTimeMillis() / 1000) {
        val startedMs = System.currentTimeMillis()
        val errors = LinkedHashMap<String, String>()

        val coverage =
            runCatching { SyncCoverageReport.build(bands.snapshot(), sweeps.snapshot(), nowSeconds) }
                .onFailure { errors["sync"] = it.message ?: it::class.simpleName.orEmpty() }
                .getOrNull()

        // The previously served series, so this tick appends to it rather than
        // restarting it. The document is where state that outlives one pass is
        // kept — see [GaugeSeries] for why nothing else holds it.
        val servedProgress = (served()?.get("data") as? JsonObject)?.get("progress") as? JsonObject
        val latest = progress.latest
        val withSeries =
            latest?.let { doc ->
                val series =
                    runCatching { GaugeSeries.next(servedProgress, doc["health"] as? JsonObject, doc, nowSeconds) }
                        .onFailure { errors["series"] = it.message ?: it::class.simpleName.orEmpty() }
                        .getOrNull()
                if (series == null) doc else JsonObject(doc + ("series" to series))
            }

        val data =
            if (coverage == null && withSeries == null) {
                null
            } else {
                buildJsonObject {
                    coverage?.forEach { (member, value) -> put(member, value) }
                    withSeries?.let { put("progress", it) }
                    // What every number above MEANS, in the document that
                    // carries them. Last, because it is the largest member here
                    // and the least likely to be read first.
                    put("terms", SyncVocabulary.TERMS)
                }
            }

        snapshot.publish(
            buildJsonObject {
                put("schema", SCHEMA_VERSION)
                put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                put(
                    "scope",
                    "What this mirror is doing right now, and how far it has walked. NOT the relay's corpus: " +
                        "for what the store holds, read the relay's own /stats.html.",
                )
                put("timezone", "UTC")
                putJsonObject("tiers") {
                    putJsonObject(TIER) {
                        put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                        put("tookMs", System.currentTimeMillis() - startedMs)
                        if (everySeconds > 0) put("everySeconds", everySeconds)
                        putJsonArray("sections") { if (data != null) add(JsonPrimitive("sync")) }
                    }
                }
                if (data != null || errors.isNotEmpty()) {
                    putJsonObject("sync") {
                        put(
                            "status",
                            if (errors.isEmpty()) {
                                "ok"
                            } else if (data == null) {
                                "failed"
                            } else {
                                "partial"
                            },
                        )
                        put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                        put("tookMs", System.currentTimeMillis() - startedMs)
                        put("data", data ?: JsonObject(emptyMap()))
                        if (errors.isNotEmpty()) putJsonObject("errors") { errors.forEach { (k, v) -> put(k, v) } }
                    }
                }
            },
        )
    }

    private fun served(): JsonObject? = snapshot.served()?.doc?.get("sync") as? JsonObject

    companion object {
        /**
         * The one tier this document has.
         *
         * The relay's document is computed in two passes on two cadences,
         * because a grouping over its whole corpus costs minutes. Nothing here
         * queries anything — it is a fold over maps this process already holds —
         * so there is one pass and it is named for what it is.
         */
        const val TIER = "status"

        /**
         * Bumped when a RELEASED member of this document changes meaning or
         * leaves, so the page can say it was written for another one rather
         * than quietly mis-drawing it. Its own number, not the relay's: the two
         * documents are published by different processes and version
         * independently.
         */
        const val SCHEMA_VERSION = 1
    }
}
