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
package com.nosfabrica.vespa.relay.pulse

import com.nosfabrica.vespa.eventstore.TrustHealth
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.DegradedReads
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.time.Instant

/**
 * What `/trust.html` charts: coverage, repairs in flight, and why reads come
 * back short.
 *
 * UNGATED, unlike the pulse. Every member is a count, a phase or a query
 * SHAPE — never a search term or an observer key, which is what the pulse's
 * admin gate exists for. These numbers being reachable only from inside the
 * process is why an incomplete projection stayed invisible for days.
 */
object TrustDocument {
    const val SCHEMA = 1

    fun reader(
        store: VespaEventStore,
        scope: String,
    ): () -> JsonObject = { of(store.trustHealth(), DegradedReads.snapshot(), scope) }

    fun of(
        health: TrustHealth,
        degraded: List<DegradedReads.Reading>,
        scope: String,
        nowMillis: Long = System.currentTimeMillis(),
    ): JsonObject =
        buildJsonObject {
            put("schema", SCHEMA)
            put("scope", scope)
            put("generatedAt", Instant.ofEpochMilli(nowMillis).toString())
            // Said rather than implied: an unmeasured coverage drawn as 0%
            // reads as an outage, and a reconcile that has not run is not one.
            put("measured", health.measured)
            put("measuredAtMs", health.measuredAtMs)
            put("servicesNamed", health.servicesNamed)
            put("servicesProjected", health.servicesProjected)
            put("lensesTotal", health.lensesTotal)
            put("lensesResolvable", health.lensesResolvable)
            putJsonArray("steps") {
                health.steps.forEach { s ->
                    add(
                        buildJsonObject {
                            put("op", s.op)
                            put("phase", s.phase)
                            put("done", s.done)
                            put("total", s.total)
                            put("elapsedSec", s.elapsedSec)
                            put("finished", s.finished)
                        },
                    )
                }
            }
            putJsonArray("degradedReads") {
                degraded.forEach { r ->
                    add(
                        buildJsonObject {
                            put("profile", r.profile)
                            put("shape", r.shape)
                            put("flags", r.flags)
                            put("count", r.count)
                            put("refused", r.refused)
                            put("lastCoverage", r.lastCoverage)
                            put("lastDocuments", r.lastDocuments)
                        },
                    )
                }
            }
        }

    /** One pubkey's explanation, or the reason it could not be given. */
    suspend fun explain(
        store: VespaEventStore,
        pubkey: String,
    ): JsonObject =
        buildJsonObject {
            if (!HEX64.matches(pubkey)) {
                put("ok", false)
                put("error", "not a 64-hex pubkey")
                return@buildJsonObject
            }
            val line = runCatching { store.explainTrust(pubkey) }
            put("ok", line.isSuccess)
            line.fold({ put("line", it) }, { put("error", it.message?.take(200) ?: it::class.simpleName ?: "failed") })
        }

    private val HEX64 = Regex("^[0-9a-fA-F]{64}$")
}
