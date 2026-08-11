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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * WHAT THE ROUTER IS DOING, as `/stats.json` publishes it — the progress file
 * (`SyncProgress`, written by the sync process on its progress tick) read off
 * the shared volume and reduced to what a reader can act on.
 *
 * ## The three things this answers that nothing else could
 *
 * **Is it alive.** `writtenAt` is a heartbeat: the router rewrites it every tick
 * whatever its streams are doing. Beside the rollup's own clock it becomes
 * `staleForSec` — how long the file has gone unwritten — which is the one number
 * that separates a quiet mirror from a stopped one. A mirror that has been down
 * for a day used to publish exactly the same card as one mid-cycle.
 *
 * **What happened to everything it took on.** The cycle's `urls`/`taken` objects
 * are a PARTITION, and this object republishes them as one: a production run
 * reported 16,752 discovered against 5,323 band-bearing and nothing accounted
 * for the ~11,400 in between. `accountedFor` is this side's own check that they
 * still sum after the read — the writer's `balanced` says the router thought so,
 * this says the document does.
 *
 * **Whether the last cycle worked.** `outcome` is `running`, `completed` or
 * `failed`. A cycle that aborted at 80% and one that finished left the identical
 * trace before it existed: both simply stopped saying anything.
 *
 * ## Read, not relayed
 *
 * Rebuilt member by member, never passed through, for the reason [MirrorReport]
 * gives at length: the file is another process's, and a hand-edited or
 * half-migrated one must not be able to put arbitrary JSON into a document
 * served under this relay's name. Everything unreadable is simply absent, and no
 * failure here may cost the relay its rollup.
 */
internal object SyncProgressReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /**
     * The eight terminal outcomes of a url the cycle took on, in the order the
     * card reads them: what worked, then what did not, then what is still going.
     *
     * A fixed list, spelled here rather than taken from the file's key order.
     * The document is a partition and a reader adds these up; taking the members
     * from whatever the writer happened to emit would let a future router widen
     * the sum silently, which is exactly the failure — counts that do not add up
     * to their own total — this whole object exists to end.
     */
    private val OUTCOMES =
        listOf(
            "delivered",
            "nothingNew",
            "unreachable",
            "transferFailed",
            "noRoute",
            "hostStruckOut",
            "torUnavailable",
            "pending",
        )

    /**
     * Fold the progress file into the `progress` object of the `sync` section,
     * or null when there is nothing to say.
     *
     * [nowSeconds] is the rollup's clock, not the file's, and that asymmetry is
     * the point: the staleness of the file is measured against the reader, so a
     * router that stopped writing an hour ago says so however recent its own
     * last timestamp looked.
     */
    fun build(
        progressJson: String?,
        nowSeconds: Long,
    ): JsonObject? {
        val doc = parse(progressJson) ?: return null
        val writtenAt = (doc["writtenAt"] as? JsonPrimitive)?.longOrNull
        val streams = (doc["streams"] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        // A file with neither a heartbeat nor a stream is not a router being
        // quiet, it is a document this parser could not read — and publishing an
        // empty `progress` object would be a claim that the router said nothing,
        // which is a different and stronger statement.
        if (writtenAt == null && streams.isEmpty()) return null

        return buildJsonObject {
            writtenAt?.let {
                put("writtenAt", it)
                // Never negative. The two processes share a host clock but not a
                // guarantee, and a file stamped a second into the future would
                // otherwise publish "-1s stale", which reads as a bug in the
                // relay rather than as skew.
                put("staleForSec", (nowSeconds - it).coerceAtLeast(0))
            }
            putJsonArray("streams") {
                for (s in streams) stream(s)?.let { add(it) }
            }
        }
    }

    /** One stream's line, or null when it carries no name — without which it says nothing. */
    private fun stream(o: JsonObject): JsonObject? {
        val name = text(o["name"]) ?: return null
        return buildJsonObject {
            put("name", name)
            text(o["phase"])?.let { put("phase", it) }
            (o["phaseForSec"] as? JsonPrimitive)?.longOrNull?.let { put("phaseForSec", it) }
            (o["cycle"] as? JsonObject)?.let { c -> cycle(c)?.let { put("cycle", it) } }
        }
    }

    /**
     * One cycle's disposition, re-derived rather than copied.
     *
     * The sums are recomputed HERE from the members this object names, so that
     * `accountedFor` is a statement about the document being served rather than
     * a boolean forwarded from a file nobody on this side has checked. A
     * mismatch is published, not hidden: the counts are still worth having, and
     * the flag is what stops a reader treating a broken partition as a whole
     * one.
     */
    private fun cycle(o: JsonObject): JsonObject? {
        val urls = o["urls"] as? JsonObject ?: return null
        val discovered = num(urls["discovered"]) ?: return null
        val folded = num(urls["foldedOntoAnother"]) ?: 0
        val taken = num(urls["taken"]) ?: (discovered - folded)
        val outcomes = o["taken"] as? JsonObject ?: JsonObject(emptyMap())
        val byOutcome = OUTCOMES.associateWith { num(outcomes[it]) ?: 0L }
        val settled = byOutcome.values.sum()
        return buildJsonObject {
            num(o["startedAt"])?.let { put("startedAt", it) }
            num(o["endedAt"])?.let { put("endedAt", it) }
            text(o["outcome"])?.let { put("outcome", it) }
            put(
                "urls",
                buildJsonObject {
                    put("discovered", discovered)
                    put("foldedOntoAnother", folded)
                    put("taken", taken)
                },
            )
            // Beside the urls, never instead of them. 3,272 urls resolved to 850
            // hosts in the run that motivated this: every count taken over urls
            // is inflated by whatever the alias fold has not decided yet, and the
            // gap between the two numbers IS the disclosure.
            num(o["hosts"])?.let { put("hosts", it) }
            put("taken", buildJsonObject { for ((k, v) in byOutcome) put(k, v) })
            put("received", num(o["received"]) ?: 0L)
            // Both halves of the partition, checked on this side.
            put("accountedFor", folded + taken == discovered && settled == taken)
            // What the WRITER thought, kept separately. The two disagreeing
            // localises the fault to the read or to the router, which one
            // merged flag could never do.
            (o["balanced"] as? JsonPrimitive)?.booleanOrNull?.let { put("balanced", it) }
        }
    }

    private fun num(value: JsonElement?): Long? = (value as? JsonPrimitive)?.longOrNull

    /**
     * A member's text, or null when absent, blank — or not a primitive at all.
     *
     * The cast is load-bearing for the reason [MirrorReport.text] spells out:
     * `jsonPrimitive` throws on an object, and one `"name": {}` in a file this
     * process did not write would cost the whole sync section.
     */
    private fun text(value: JsonElement?): String? = (value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotBlank() }

    /** A corrupt or half-written file costs this object, not the rollup. `Exception`, never `Throwable` — see [SyncCoverageReport.parse]. */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return try {
            lenient.parseToJsonElement(text).jsonObject
        } catch (e: Exception) {
            null
        }
    }
}
