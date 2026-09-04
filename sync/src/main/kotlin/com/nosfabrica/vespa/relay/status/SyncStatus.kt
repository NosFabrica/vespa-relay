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

import com.nosfabrica.vespa.relay.progress.StatusVocabulary
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
 * The mirror's own `/stats.json`: what it is doing and how far it has walked.
 *
 * The writer and the reader share one heap, so `SyncProgress.latest` is served
 * as built, and liveness is answered by whether the request answers. The
 * envelope matches the relay's document (`schema`, `generatedAt`, `tiers`, a
 * `status`/`generatedAt`/`data` section) because the two pages share a
 * rendering engine.
 */
class SyncStatus(
    private val bands: SyncBands,
    private val sweeps: SweepState,
    private val progress: SyncProgress,
    private val snapshot: StatsSnapshot,
    /** How often [publish] is called; published so the page polls on the stated cadence. */
    private val everySeconds: Long,
    /**
     * Every prime (relay, stream) unit the pool holds, read once per tick
     * because the roster is rebuilt on its own clock. Empty publishes no
     * `relays` section at all.
     */
    private val primeUnits: () -> List<RelayStatusReport.PrimeUnit> = { emptyList() },
) {
    /**
     * Build the document and hand it to [snapshot]. Never throws: a failed
     * part is published under `errors` instead.
     */
    fun publish(nowSeconds: Long = System.currentTimeMillis() / 1000) {
        val startedMs = System.currentTimeMillis()
        val errors = LinkedHashMap<String, String>()

        // The band snapshot is the expensive part of the tick; both reports
        // walk it, so it is built once.
        val bandsDoc =
            runCatching { bands.snapshot() }
                .onFailure { errors["bands"] = it.message ?: it::class.simpleName.orEmpty() }
                .getOrNull()

        val coverage =
            runCatching { SyncCoverageReport.build(bandsDoc, sweeps.snapshot(), nowSeconds) }
                .onFailure { errors["sync"] = it.message ?: it::class.simpleName.orEmpty() }
                .getOrNull()

        // Its own member: the coverage fold groups by stream over relays a
        // stream has touched, and this one's subject is the roster.
        val relays =
            runCatching { RelayStatusReport.build(bandsDoc, primeUnits(), nowSeconds) }
                .onFailure { errors["relays"] = it.message ?: it::class.simpleName.orEmpty() }
                .getOrNull()

        // The previously served series, so this tick appends rather than restarts it.
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
            if (coverage == null && withSeries == null && relays == null) {
                null
            } else {
                buildJsonObject {
                    coverage?.forEach { (member, value) -> put(member, value) }
                    relays?.let { put("relays", it) }
                    withSeries?.let { put("progress", it) }
                }
            }

        // Only the vocabulary this document's members use; see [StatusVocabulary.termsFor].
        val withTerms = data?.let { JsonObject(it + ("terms" to StatusVocabulary.termsFor(it))) }

        snapshot.publish(
            buildJsonObject {
                put("schema", SCHEMA_VERSION)
                // One markup file serves all three services, so the title, scope
                // and what the numbers cover come from the document.
                put("title", "Mirror status")
                put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                put(
                    "scope",
                    "What this mirror is doing right now, and how far it has walked. NOT the relay's corpus: " +
                        "for what the store holds, read the relay's own /stats.html.",
                )
                put("timezone", "UTC")
                put("counted", "Counted against this mirror's own state, not the relay's corpus.")
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
                        put("data", withTerms ?: JsonObject(emptyMap()))
                        if (errors.isNotEmpty()) putJsonObject("errors") { errors.forEach { (k, v) -> put(k, v) } }
                    }
                }
            },
        )
    }

    private fun served(): JsonObject? = snapshot.served()?.doc?.get("sync") as? JsonObject

    companion object {
        /** The one tier: everything here is a fold over maps this process already holds. */
        const val TIER = "status"

        /** Bumped when a released member changes meaning or leaves. Versioned apart from the relay's document. */
        const val SCHEMA_VERSION = 1
    }
}
