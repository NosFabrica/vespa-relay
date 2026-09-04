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
 * WHAT THE MONITOR HAS DECIDED, as its own `/stats.json` publishes it.
 *
 * ## Why the monitor has a page at all
 *
 * Because it answers a different question from the mirror's, on a different
 * clock, in a different unit. Sync coverage answers "is the mirror keeping up",
 * measured in events; this answers "what is out there, and how much of it can we
 * use", measured in relay urls. They were two cards on one page and the page
 * asked both at once — an operator arrives with one of the two questions, and
 * the split is what lets them stop reading at the answer.
 *
 * It is also what the eventual process split needs. The monitor's rows already
 * come from its own [Processors]; this document and the page over it mean that
 * when the plane moves into its own container, nothing about how it is read
 * changes — the port is already its own.
 *
 * ## What is in it
 *
 * The four pass rows and nothing else. There is no `streams` half and no health
 * gauges: the mirror owns the ingest queue, the heap and the sockets, and a
 * second reading of them here would be a second number for one fact.
 *
 * The envelope is deliberately the same as the relay's and the mirror's — same
 * `schema`/`generatedAt`/`tiers`, same per-section `status`/`data` — because all
 * three pages run the same rendering engine, and a third envelope would be a
 * third thing to keep in step for no reader's benefit.
 */
class MonitorStatus(
    private val processors: Processors,
    /** How often [document] is rebuilt, published so the page polls on what the document states. */
    private val everySeconds: Long,
    /**
     * THE SERVED RELAY'S OWN WS URL — `RELAY_URL`, published because the page
     * cannot derive it.
     *
     * The verdict panel reads kind-30166 records over a websocket, and it used
     * to open one against `location`: correct on the relay's own page, which is
     * where the panel lived, and wrong the moment it moved here, because this
     * page is served on the MONITOR's port. It would have dialled the status
     * site and found no relay there — a panel that reads empty on a store
     * holding thousands of records, which is the exact confusion the panel
     * exists to end.
     *
     * Null in a deployment with no relay beside it; the page then says so
     * rather than guessing at an origin.
     */
    private val relayUrl: String? = null,
) {
    /**
     * The document, pure, so it can be asserted without a server.
     *
     * Never throws: a `Processors` snapshot is a read of atomics the passes
     * already keep, so there is nothing here that can fail in a way worth
     * reporting into the document.
     */
    fun document(nowSeconds: Long = System.currentTimeMillis() / 1000): JsonObject {
        val startedMs = System.currentTimeMillis()
        val rows = processors.snapshot()
        val progress =
            buildJsonObject {
                putJsonArray("processors") { rows.forEach { add(Processors.published(it)) } }
            }
        // ABSENT, not empty, when no pass has registered a row. A deployment
        // with no signer runs no monitor at all, and a card of zeroes there
        // reads as a monitor that is failing rather than one nobody configured.
        val data =
            progress.takeIf { rows.isNotEmpty() }?.let {
                buildJsonObject {
                    put("progress", it)
                    // What every number above MEANS — the subset of the shared
                    // vocabulary this document actually publishes. See
                    // [StatusVocabulary.termsFor] for why it is a subset.
                    put("terms", StatusVocabulary.termsFor(it))
                }
            }

        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            // See SyncStatus: one markup file, three services, so the heading
            // and the tab are the document's to state.
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
            // See [relayUrl]: this page is not served by the relay, so the
            // websocket the verdict panel opens has to be named for it.
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
        /**
         * The one tier this document has — the passes' own rows, read from
         * atomics. Nothing here queries or dials, so there is one pass and it is
         * named for what it is.
         */
        const val TIER = "status"

        /**
         * Bumped when a RELEASED member changes meaning or leaves. Its own
         * number: the three documents are published by different planes and
         * version independently.
         */
        const val SCHEMA_VERSION = 1
    }
}
