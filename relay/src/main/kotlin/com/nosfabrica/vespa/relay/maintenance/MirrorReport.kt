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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * WHAT THIS RELAY MIRRORS, as `/stats.json` publishes it — the router's manifest
 * (`SyncManifest`, written by the sync process) read off the shared volume and
 * reduced to the one set a client actually needs.
 *
 * ## The comparison this makes possible
 *
 * "How much of my stuff is here yet" is answered by counting our events for an
 * author against the author's own relay's count for them, and done bare that is
 * a filtered mirror over an unfiltered total: a quotient that cannot reach 100%
 * however complete the mirror is. Measured, 31,118 here of 89,485 there — *35%*
 * — on a mirror missing nothing it was ever asked to hold. The gap was kinds 3,
 * 4, 5, 6, 7 and 1059, none of which this mirror asks for, two of which are
 * encrypted DMs and gift wraps it must never hold.
 *
 * With `kinds` published, both sides of that comparison can carry the same
 * `kinds` and the number means something. Nothing here computes the comparison —
 * this relay has no count for somebody else's relay — it publishes the one fact
 * the caller cannot get anywhere else.
 *
 * ## Read, not relayed
 *
 * The document is rebuilt member by member rather than passed through. The file
 * is another process's, the same trust boundary [SyncCoverageReport] describes,
 * and a hand-edited or half-migrated manifest must not be able to put arbitrary
 * JSON into a document served under this relay's name. Everything unparseable is
 * simply absent — the same best-effort contract as the coverage card, whose one
 * hard rule is that a router file may never cost the relay its rollup.
 *
 * ## `allKinds`, and why a partial union is not published
 *
 * A stream whose filter names no kinds mirrors everything its relays serve, so
 * there IS no kind bound to apply and a union taken over the streams that do
 * name kinds would be a smaller set than the truth — a client scoping a COUNT to
 * it would under-count the very denominator this exists to fix. That case
 * publishes `allKinds: true` and NO `kinds`: an absent member is a question a
 * reader has to answer, where a wrong list is one it will not think to ask.
 *
 * Only `down`/`both` streams count toward the set. An `up` stream publishes our
 * events at somebody else; it holds nothing here. They stay in `streams` anyway,
 * with their direction, because "we push kind 62 up there" is exactly the fact
 * that explains why 62 is not in the mirrored set.
 */
internal object MirrorReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /**
     * Fold the manifest into the `mirrors` object of the `sync` section, or null
     * when there is nothing to say — no file, no parse, or a manifest naming no
     * streams. Null rather than an empty object, for the reason the sync section
     * itself is absent on a serve-only relay: "this relay does not mirror" and
     * "this relay mirrors nothing" are different claims.
     */
    fun build(manifestJson: String?): JsonObject? {
        val doc = parse(manifestJson) ?: return null
        val streams =
            (doc["streams"] as? JsonArray ?: return null)
                .filterIsInstance<JsonObject>()
                .mapNotNull { stream(it) }
        if (streams.isEmpty()) return null

        // The union, over the streams that pull events DOWN. `allKinds` the
        // moment one of them names no kinds — see the class header for why that
        // suppresses the list rather than shrinking it.
        val mirroring = streams.filter { it.mirrors }
        val unbounded = mirroring.any { it.kinds == null }
        val union = mirroring.flatMap { it.kinds.orEmpty() }.distinct().sorted()

        return buildJsonObject {
            // Straight from the writer: this document outlives the process that
            // wrote it, so a reader comparing against a mirror that was turned
            // off last month needs to be able to see that it was.
            doc["writtenAt"]?.jsonPrimitive?.longOrNull?.let { put("writtenAt", it) }
            if (unbounded) {
                put("allKinds", true)
            } else if (union.isNotEmpty()) {
                putJsonArray("kinds") { for (kind in union) add(kind) }
            }
            putJsonArray("streams") {
                for (s in streams) {
                    add(
                        buildJsonObject {
                            put("name", s.name)
                            put("dir", s.dir)
                            s.kinds?.let { kinds -> putJsonArray("kinds") { for (kind in kinds) add(kind) } }
                            s.since?.let { put("since", it) }
                        },
                    )
                }
            }
        }
    }

    /** One stream of the manifest, after everything unreadable has been dropped. */
    private class Stream(
        val name: String,
        val dir: String,
        /** Null means "no kind bound", which is NOT the same as an empty list. */
        val kinds: List<Int>?,
        val since: Long?,
    ) {
        /** Whether this stream puts events in our store — the only ones the union is over. */
        val mirrors: Boolean get() = dir == "down" || dir == "both"
    }

    /**
     * A stream entry, or null when it carries no name — the one member without
     * which it says nothing at all.
     *
     * A missing `dir` reads as `down`. Every manifest this repo writes carries
     * one; an entry that lost it is far likelier to be a mirroring stream than a
     * push, and guessing the other way would silently drop kinds OUT of the set,
     * which is the failure this whole report exists to end.
     */
    private fun stream(o: JsonObject): Stream? {
        val name = o["name"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() } ?: return null
        val kinds =
            when (val raw = o["kinds"]) {
                // Absent is the writer saying "no kind bound on this stream".
                null -> null

                is JsonArray -> raw.mapNotNull { it.jsonPrimitive.intOrNull }.distinct().sorted()

                // PRESENT and unreadable is not evidence of an unbounded stream.
                // It is a bound this build could not read, so the stream
                // contributes nothing to the union rather than widening it to
                // everything — the one direction in which being wrong here puts
                // kinds we do not hold into a set a client counts against.
                else -> emptyList()
            }
        return Stream(
            name = name,
            dir = o["dir"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() } ?: "down",
            kinds = kinds,
            since = o["since"]?.jsonPrimitive?.longOrNull,
        )
    }

    /**
     * A corrupt or half-written manifest costs this object, not the rollup. Same
     * `Exception`-not-`Throwable` rule as [SyncCoverageReport.parse], for the
     * same reason — this file is small, but the discipline is the point.
     */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return try {
            lenient.parseToJsonElement(text).jsonObject
        } catch (e: Exception) {
            null
        }
    }
}
