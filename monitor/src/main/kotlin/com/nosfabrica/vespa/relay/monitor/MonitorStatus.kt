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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.time.Instant

/**
 * What the monitor has decided, as its own `/stats.json` publishes it: the pass rows only, in
 * the same envelope as the relay's and the mirror's documents so all three share one renderer.
 */
class MonitorStatus(
    private val processors: Processors,
    /** How often [document] is rebuilt, published so the page polls on it. */
    private val everySeconds: Long,
    /** The served relay's ws url for the verdict panel to dial; `location` here is the monitor's port. */
    private val relayUrl: String? = null,
) {
    /** The document, pure so it can be asserted without a server. Never throws. */
    fun document(nowSeconds: Long = System.currentTimeMillis() / 1000): JsonObject {
        val startedMs = System.currentTimeMillis()
        val rows = processors.snapshot()
        val progress =
            buildJsonObject {
                putJsonArray("processors") { rows.forEach { add(Processors.published(it)) } }
            }
        // Absent, not empty, when no pass has registered a row: a card of zeroes reads as a failing monitor.
        val data =
            progress.takeIf { rows.isNotEmpty() }?.let {
                buildJsonObject {
                    put("progress", it)
                    put("terms", StatusVocabulary.termsFor(it))
                }
            }

        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            // One markup file serves three services, so the heading is the document's to state.
            put("title", "Relay monitor")
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put(
                "scope",
                "What this router has decided about the relay urls it discovers — which are one server wearing " +
                    "several addresses, which cannot answer the same question twice, which do not answer the " +
                    "question that was asked, which are graded prime, and which are unreachable. NOT what the mirror is doing with them: that is the sync service's page.",
            )
            put("timezone", "UTC")
            put("counted", "Counted over the relay urls this router discovers, not over its corpus.")
            relayUrl?.let { put("relay", it) }
            putJsonObject("tiers") {
                putJsonObject(TIER) {
                    put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                    put("tookMs", System.currentTimeMillis() - startedMs)
                    if (everySeconds > 0) put("everySeconds", everySeconds)
                    putJsonArray("sections") { if (data != null) add(JsonPrimitive("monitor")) }
                }
            }
            data?.let {
                putJsonObject("monitor") {
                    put("status", "ok")
                    put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                    put("tookMs", System.currentTimeMillis() - startedMs)
                    put("data", it)
                }
            }
        }
    }

    companion object {
        /** The passes' own rows, read from atomics; nothing here queries or dials. */
        const val TIER = "status"

        /** Bumped when a released member changes meaning or leaves. */
        const val SCHEMA_VERSION = 1
    }
}
