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
package com.nosfabrica.vespa.relay.maintenance

import com.nosfabrica.vespa.relay.web.StatsSnapshot
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.io.File
import java.time.Instant

/**
 * The relay's `GET /stats.json`: who this relay is and what its router mirrors. It asks the engine
 * nothing; `stats.html` counts the store itself over the relay's websocket, so every number on the
 * page is as fresh as the moment it was asked.
 */
internal class StatsRollup(
    private val relayUrl: String,
    /** The router's manifest on the shared volume; null in a serve-only deployment. */
    private val syncManifestFile: File? = null,
) {
    /** The document. Never throws: an unreadable manifest is a failed `sync` section. */
    fun compute(everySeconds: Long = 0): JsonObject {
        val startedMs = System.currentTimeMillis()
        // Absent, not empty, without a router: "0 relays" reads as a broken mirror.
        val sync = syncSection()
        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            put("title", "Relay stats")
            put("relay", relayUrl)
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put(
                "scope",
                "This relay's own store — the events it has mirrored and serves. NOT the Nostr network: " +
                    "a total below a network-wide dashboard's is this mirror's coverage, not a fault.",
            )
            put("countedAs", "anonymous")
            put("counted", "Counted live over this relay's websocket, anonymously, against the whole store.")
            put("timezone", "UTC")
            putJsonObject("tiers") {
                putJsonObject(TIER) {
                    put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
                    put("tookMs", System.currentTimeMillis() - startedMs)
                    if (everySeconds > 0) put("everySeconds", everySeconds)
                    putJsonArray("sections") { if (sync != null) add("sync") }
                }
            }
            sync?.let { put("sync", it) }
        }
    }

    /** The kind set this relay mirrors, read off the shared volume; null when no router ever wrote one. */
    private fun syncSection(): JsonObject? {
        if (syncManifestFile == null) return null
        val startedMs = System.currentTimeMillis()
        val mirrors =
            try {
                MirrorReport.build(syncManifestFile.takeIf { it.isFile }?.readText())
            } catch (e: Exception) {
                return envelope("failed", startedMs, buildJsonObject { }, e.message ?: e.toString())
            }
        return mirrors?.let { envelope("ok", startedMs, buildJsonObject { put("mirrors", it) }) }
    }

    /** The section envelope every service's panels read: status, timing, data, and the error if any. */
    private fun envelope(
        status: String,
        startedMs: Long,
        data: JsonObject,
        error: String? = null,
    ): JsonObject =
        buildJsonObject {
            put("status", status)
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            put("data", data)
            error?.let { putJsonObject("errors") { put("mirrors", it) } }
        }

    companion object {
        /** Bumped when a released field changes meaning or leaves, not when one is added. */
        const val SCHEMA_VERSION = 3

        /** The one cadence, as the document's `tiers` names it. */
        const val TIER = "counters"

        /** The sections this rollup owns; one missing from a pass leaves the served document. */
        val SECTIONS = setOf("sync")
    }
}

/**
 * Recompute the document into [snapshot] every [everySeconds]. Published even without a `sync`
 * section: the page's live panels need only the headers, and a serve-only relay has nothing else.
 */
internal fun launchStatsRollup(
    scope: CoroutineScope,
    rollup: StatsRollup,
    snapshot: StatsSnapshot,
    everySeconds: Long,
) {
    scope.launch {
        while (true) {
            runCatching { rollup.compute(everySeconds) }
                .onSuccess { doc ->
                    snapshot.publish(doc, owns = StatsRollup.SECTIONS, tier = StatsRollup.TIER)
                    if ((doc["sync"] as? JsonObject)?.get("status") == JsonPrimitive("failed")) {
                        println("stats: the sync manifest could not be read — see /stats.json")
                    }
                }.onFailure { e ->
                    if (e is CancellationException) throw e
                    System.err.println("stats: rollup failed (${e.message}) — serving the previous document")
                    snapshot.markStale(
                        "the last ${StatsRollup.TIER} rollup failed: ${e.message ?: e.javaClass.simpleName}",
                        tier = StatsRollup.TIER,
                    )
                }
            delay(everySeconds * 1000)
        }
    }
}
