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
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicReference

/**
 * What a service's `GET /stats.json` serves: the last computed document, held in memory with its
 * ETag and written through to a file so a restart answers with the previous one. Served from
 * memory, never from the file; the write is atomic so a half-written file is never read back.
 */
class StatsSnapshot(
    /** Where the document is persisted; null/blank keeps it purely in memory. */
    private val path: String? = null,
) {
    /** Bytes and ETag swapped as one reference, so a request never pairs new bytes with the old tag. */
    private val current = AtomicReference<Served?>(null)

    /** The current document's bytes and ETag, or null before the first rollup. */
    fun served(): Served? = current.get()

    /**
     * Merges [members] into the served document and persists the result: members in [members]
     * replace, members in [owns] but not in [members] are removed, everything else stays as the
     * other tier left it. With no [owns] the whole document is replaced.
     */
    @Synchronized
    fun publish(
        members: JsonObject,
        owns: Set<String> = emptySet(),
        tier: String? = null,
    ) {
        val doc = merged(members, owns, tier)
        val bytes = JSON.encodeToString(JsonObject.serializer(), doc).toByteArray(Charsets.UTF_8)
        current.set(Served(doc, bytes, etagOf(bytes)))
        save(bytes)
    }

    /**
     * [members] folded onto the served document. `tiers` is a union, and a `stale` notice survives
     * another tier's success: a writer clears only its own notice and an unattributed one.
     */
    private fun merged(
        members: JsonObject,
        owns: Set<String>,
        tier: String?,
    ): JsonObject {
        val base = current.get()?.doc
        if (base == null || owns.isEmpty()) return members
        // Another schema's document is replaced, not merged onto: its dropped members are owned by nobody.
        if (base["schema"] != members["schema"]) return members
        val out = LinkedHashMap<String, JsonElement>(base)
        owns.forEach { out.remove(it) }
        out.remove("stale")
        members.forEach { (member, value) -> out[member] = value }
        val wasTiers = base["tiers"] as? JsonObject
        val nowTiers = members["tiers"] as? JsonObject
        if (wasTiers != null && nowTiers != null) out["tiers"] = JsonObject(wasTiers + nowTiers)
        val stale = base["stale"] as? JsonObject
        val staleTier = (stale?.get("tier") as? JsonPrimitive)?.contentOrNull
        if (stale != null && staleTier != null && staleTier != tier) out["stale"] = stale
        return JsonObject(out)
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
        /** Which cadence failed, or null for the whole document; [publish] clears only its own notice. */
        tier: String? = null,
        /** False on the seed path, where a boot must not rewrite the bytes it just read. */
        persist: Boolean = true,
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
                            tier?.let { put("tier", it) }
                            // The failing tier's own timestamp: the healthy tier touched the
                            // document seconds ago.
                            (tierGeneratedAt(doc, tier) ?: (doc["generatedAt"] as? JsonPrimitive)?.contentOrNull)
                                ?.let { put("generatedAt", it) }
                        },
                    )
                }
            val bytes = JSON.encodeToString(JsonObject.serializer(), marked).toByteArray(Charsets.UTF_8)
            current.set(Served(marked, bytes, etagOf(bytes)))
            if (persist) save(bytes)
        }.onFailure {
            // The served document stays as it was; losing the notice is the smaller harm.
            System.err.println("stats: could not mark the served document stale (${it.message})")
        }
    }

    /**
     * Seeds from [path] if it holds a parseable document, marked stale until the first rollup. A
     * missing or corrupt file is not an error; the page's own "not computed yet" state is right.
     */
    fun loadFromFile() {
        val file = File(path ?: return)
        if (path.isBlank() || !file.exists()) return
        runCatching {
            val bytes = file.readBytes()
            // Parsed, as the only check on a hand-edited or truncated file, and kept for the next
            // publish to merge onto.
            val doc = Json.parseToJsonElement(bytes.decodeToString()).jsonObject
            current.set(Served(doc, bytes, etagOf(bytes)))
            markStale(
                "served from $path after a restart; no rollup has completed in this process yet",
                persist = false,
            )
        }.onFailure { e ->
            System.err.println("stats: could not read $path (${e.message}) — serving nothing until the first rollup")
        }
    }

    @Synchronized
    private fun save(bytes: ByteArray) {
        val target = File(path?.takeIf { it.isNotBlank() } ?: return)
        target.absoluteFile.parentFile?.mkdirs()
        val tmp = File(target.path + ".tmp")
        runCatching {
            tmp.writeBytes(bytes)
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }.recoverCatching {
            // Some filesystems do not support ATOMIC_MOVE.
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }.onFailure { e ->
            // Not fatal: the in-memory document is already serving.
            System.err.println("stats: could not persist to $path: ${e.message}")
        }
    }

    /**
     * One published document: the exact bytes served, the ETag over them, and the parsed form the
     * next publish merges onto. The ETag is over [bytes]; re-encoding [doc] could mint different bytes.
     */
    class Served(
        val doc: JsonObject,
        val bytes: ByteArray,
        val etag: String,
    )

    private companion object {
        /** When the tier named [tier] last computed anything, per the document's own `tiers` member. */
        fun tierGeneratedAt(
            doc: JsonObject,
            tier: String?,
        ): String? =
            ((doc["tiers"] as? JsonObject)?.get(tier ?: return null) as? JsonObject)
                ?.get("generatedAt")
                ?.let { (it as? JsonPrimitive)?.contentOrNull }

        /** Not pretty-printed: the document is machine-read first, and polled. */
        val JSON = Json

        /** The same content-derived strong ETag the pages and modules use. */
        fun etagOf(bytes: ByteArray): String =
            MessageDigest
                .getInstance("SHA-256")
                .digest(bytes)
                .take(8)
                .joinToString("") { "%02x".format(it) }
                .let { "\"$it\"" }
    }
}
