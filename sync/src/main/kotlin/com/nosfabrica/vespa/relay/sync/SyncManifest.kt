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
 * What this router is configured to mirror, written as JSON where the relay
 * can read it.
 *
 * A client comparing our count for an author against the author's own relay
 * compares a filtered mirror against an unfiltered total unless both sides
 * are scoped to the kinds the mirror asks for, and `router.conf` is the only
 * place that list exists. The router is the only writer, once at boot (a
 * config edit is a restart, never a reload), and the relay's read is
 * best-effort. The streams are the ones this process is running, not every
 * stream in the config; a stream with no `kinds` gets no `kinds` member;
 * `since` rides along where a stream has one; `writtenAt` lets a reader spot
 * a document that outlived its writer. No union is computed here: the relay's
 * `MirrorReport` does that, so two places cannot disagree about it.
 */
class SyncManifest(
    /** Where the manifest is written; null publishes nothing. */
    private val file: File?,
) {
    /**
     * Whether `SYNC_MANIFEST_FILE` named a path at all. Separate from [write]'s
     * result: a router never given a path and one whose disk refused the write
     * need different fixes.
     */
    val publishes: Boolean get() = file != null

    /**
     * Publishes [streams]; returns whether anything was written. Never throws:
     * a router that cannot write this file still mirrors, so the failure is
     * logged loudly and swallowed.
     */
    fun write(
        streams: List<SyncStream>,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val f = file ?: return false
        return runCatching {
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), document(streams, nowSeconds)))
            // Temp file plus an atomic move: the relay reads on its own schedule and must never see half a document.
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
        // Pretty-printed: an operator reads it, and the relay reads it from disk on its own rollup.
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
