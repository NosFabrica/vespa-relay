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
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * What this relay mirrors, as `/stats.json` publishes it: the router's `SyncManifest`, reduced to
 * the kind set a client needs to scope a count against us. Rebuilt member by member, never passed
 * through: the file is another process's, and nothing unreadable in it may cost the rollup.
 */
internal object MirrorReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /** The `mirrors` object of the `sync` section, or null when there is nothing to say. */
    fun build(manifestJson: String?): JsonObject? {
        val doc = parse(manifestJson) ?: return null
        val streams =
            (doc["streams"] as? JsonArray ?: return null)
                .filterIsInstance<JsonObject>()
                .mapNotNull { stream(it) }
        if (streams.isEmpty()) return null

        val mirroring = streams.filter { it.mirrors }
        val unbounded = mirroring.any { it.kinds == null }
        val union = mirroring.flatMap { it.kinds.orEmpty() }.distinct().sorted()

        return buildJsonObject {
            // The writer's own stamp: this document outlives the process that wrote it.
            (doc["writtenAt"] as? JsonPrimitive)?.longOrNull?.let { put("writtenAt", it) }
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
        /** Null is "no kind bound", which is not an empty list. */
        val kinds: List<Int>?,
        val since: Long?,
    ) {
        /** Whether this stream puts events in our store. */
        val mirrors: Boolean get() = dir == "down" || dir == "both"
    }

    /** A stream entry, or null when it carries no name. A missing `dir` reads as `down`, so no kind is dropped. */
    private fun stream(o: JsonObject): Stream? {
        val name = text(o["name"]) ?: return null
        val kinds =
            when (val raw = o["kinds"]) {
                null -> null

                // Cast per element: `jsonPrimitive` throws on an object or an array.
                is JsonArray -> raw.mapNotNull { (it as? JsonPrimitive)?.intOrNull }.distinct().sorted()

                // Present and unreadable is a bound, not an unbounded stream: it contributes nothing.
                else -> emptyList()
            }
        return Stream(
            name = name,
            dir = text(o["dir"]) ?: "down",
            kinds = kinds,
            since = (o["since"] as? JsonPrimitive)?.longOrNull,
        )
    }

    /** A member's text, or null when absent, blank or not a primitive; the cast never throws. */
    private fun text(value: JsonElement?): String? = (value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotBlank() }

    /** A corrupt manifest costs this object, not the rollup. */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return try {
            lenient.parseToJsonElement(text).jsonObject
        } catch (e: Exception) {
            null
        }
    }
}
