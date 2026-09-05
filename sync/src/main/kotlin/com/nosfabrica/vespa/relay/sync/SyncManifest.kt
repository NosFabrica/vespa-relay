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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.SyncStream
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * What this router is configured to mirror, written as JSON where the relay can read it, so a
 * client comparing our count against an upstream's total can scope both sides to the kinds the
 * mirror asks for. Written once at boot by the router alone; the relay's read is best-effort.
 */
class SyncManifest(
    /** Where the manifest is written; null publishes nothing. */
    private val file: File?,
) {
    /**
     * Whether `SYNC_MANIFEST_FILE` named a path at all. Separate from [write]'s result: no path
     * and a refused write need different fixes.
     */
    val publishes: Boolean get() = file != null

    /** Publishes [streams]; returns whether anything was written. Never throws: the mirror does not need this file. */
    fun write(
        streams: List<SyncStream>,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val f = file ?: return false
        return runCatching {
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), document(streams, nowSeconds)))
            // Temp file plus an atomic move, so the relay never reads half a document.
            try {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: AtomicMoveNotSupportedException) {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        }.onFailure {
            System.err.println("router: could not write the sync manifest to ${f.path} (${it.message}) — the relay cannot publish which kinds this mirror holds")
        }.isSuccess
    }

    companion object {
        // Pretty-printed for a human reader.
        private val json = Json { prettyPrint = true }

        /** The document, pure, so it can be asserted without a filesystem. */
        fun document(
            streams: List<SyncStream>,
            nowSeconds: Long,
        ): JsonObject =
            buildJsonObject {
                put("writtenAt", nowSeconds)
                putJsonArray("streams") {
                    for (stream in streams) {
                        add(
                            buildJsonObject {
                                put("name", stream.name)
                                put("dir", stream.dir.wire)
                                // Sorted and de-duplicated: a set, so a reorder in the config is not a change.
                                stream.filter.kinds?.takeIf { it.isNotEmpty() }?.let { kinds ->
                                    putJsonArray("kinds") { for (kind in kinds.distinct().sorted()) add(kind) }
                                }
                                // Only when the stream has a floor; a written `since` is a bound the reader applies.
                                stream.filter.since?.let { put("since", it) }
                            },
                        )
                    }
                }
            }

        /** `SYNC_MANIFEST_FILE`: where the manifest is written. Unset publishes nothing; there is no `ROUTER_*` spelling. */
        fun fromEnv(env: Map<String, String>): SyncManifest =
            SyncManifest(
                env["SYNC_MANIFEST_FILE"]
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
            )
    }
}
