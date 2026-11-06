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
package com.nosfabrica.vespa.relay.web

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicReference

/**
 * What a service's `GET /stats.json` serves: the last computed document, held in memory with its
 * ETag. Nothing is kept on disk, so a restarted service answers 503 until its first rollup.
 */
class StatsSnapshot(
    /**
     * Why this service will never publish, when it computes no statistics by choice (a rollup turned
     * off); the route says so instead of "not computed yet". Null for a service that will publish.
     */
    val offBecause: String? = null,
) {
    /** Bytes and ETag swapped as one reference, so a request never pairs new bytes with the old tag. */
    private val current = AtomicReference<Served?>(null)

    /** The current document's bytes and ETag, or null before the first rollup. */
    fun served(): Served? = current.get()

    /** Replaces the served document with [doc], clearing any `stale` notice. */
    @Synchronized
    fun publish(doc: JsonObject) {
        val bytes = JSON.encodeToString(JsonObject.serializer(), doc).toByteArray(Charsets.UTF_8)
        current.set(Served(doc, bytes, etagOf(bytes)))
    }

    /**
     * Re-publishes the served document with a `stale` member naming the reason and the moment. In
     * the document rather than a header, since anything that caches or forwards `/stats.json` would
     * drop a header. A no-op before the first rollup.
     */
    @Synchronized
    fun markStale(
        reason: String,
        atSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        val existing = current.get() ?: return
        runCatching {
            val doc = existing.doc
            // Rebuilt so `stale` goes on the outside and an earlier notice is replaced, not doubled.
            val marked =
                buildJsonObject {
                    for ((member, value) in doc) if (member != "stale") put(member, value)
                    put(
                        "stale",
                        buildJsonObject {
                            put("reason", reason)
                            put("since", atSeconds)
                            (doc["generatedAt"] as? JsonPrimitive)?.contentOrNull?.let { put("generatedAt", it) }
                        },
                    )
                }
            val bytes = JSON.encodeToString(JsonObject.serializer(), marked).toByteArray(Charsets.UTF_8)
            current.set(Served(marked, bytes, etagOf(bytes)))
        }.onFailure {
            // The served document stays as it was; losing the notice is the smaller harm.
            System.err.println("stats: could not mark the served document stale (${it.message})")
        }
    }

    /**
     * One published document: the exact bytes served, the ETag over them, and the parsed form. The
     * ETag is over [bytes]; re-encoding [doc] could mint different bytes.
     */
    class Served(
        val doc: JsonObject,
        val bytes: ByteArray,
        val etag: String,
    )

    private companion object {
        /** Not pretty-printed: the document is machine-read first, and polled. */
        val JSON = Json
    }
}
