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
package com.nosfabrica.vespa.relay.server

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicReference

/**
 * What `GET /stats.json` serves: the last computed statistics document, held in
 * memory with a validator, and written through to a file so a restart answers
 * with the previous one instead of nothing.
 *
 * ## Why the file and the route are not the same thing
 *
 * The obvious shape is to write the JSON somewhere public and let a static
 * handler serve it. Two reasons it is not that here.
 *
 * The relay has no public static directory. `/web/…` is served from
 * [WebAssets] off the classpath rather than Ktor's `staticResources`,
 * specifically so it can mint content-derived validators — a jar entry's mtime
 * is the build's, not the file's. Adding a disk-backed static root for one file
 * would be a second serving mechanism, with a path-traversal surface, for a
 * document already in memory.
 *
 * And the two have different jobs. The file exists for DURABILITY: a full
 * rollup is minutes of grouping over the corpus, so a deploy must not blank the
 * page, and the operator gets a copy under `/var/lib/vespa-relay` that is
 * readable from the host (the same bind mount the FTS cursor and the parse
 * audit land in). Serving is a different question and is answered from memory,
 * where the bytes and their ETag are already computed.
 *
 * The write is atomic (temp file + move), same as [BanListFile] and for the
 * same reason: a reader that catches a half-written document gets a parse error
 * on the next boot, and the recovery for that is silently starting empty.
 */
class StatsSnapshot(
    /** Where the document is persisted; null/blank keeps it purely in memory. */
    private val path: String? = null,
) {
    /**
     * The served bytes plus the validator over them, swapped as one reference.
     *
     * Atomic because the rollup publishes from a background coroutine while
     * Netty threads serve: two fields would let a request pair new bytes with
     * the old tag, which is the one combination a cache never recovers from.
     */
    private val current = AtomicReference<Served?>(null)

    /** The current document's bytes and ETag, or null before the first rollup. */
    fun served(): Served? = current.get()

    /** Replace the served document and persist it. */
    fun publish(doc: JsonObject) {
        val bytes = JSON.encodeToString(JsonObject.serializer(), doc).toByteArray(Charsets.UTF_8)
        current.set(Served(bytes, etagOf(bytes)))
        save(bytes)
    }

    /**
     * Seed from [path] if it holds a parseable document. A missing or corrupt
     * file is not an error: the next rollup overwrites it, and the page's own
     * "not computed yet" state is the correct thing to show meanwhile. Corrupt
     * still gets a line — silently serving nothing looks exactly like a relay
     * whose rollup never runs.
     */
    fun loadFromFile() {
        val file = File(path ?: return)
        if (path.isBlank() || !file.exists()) return
        runCatching {
            val bytes = file.readBytes()
            // Parsed, not trusted as-is: serving whatever the file holds would
            // let a hand-edited or truncated document out under this relay's
            // name, and the parse is the only check available.
            Json.parseToJsonElement(bytes.decodeToString()).jsonObject
            current.set(Served(bytes, etagOf(bytes)))
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
            // Some filesystems don't support ATOMIC_MOVE; fall back to a plain replace.
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }.onFailure { e ->
            // Not fatal — the in-memory document is already serving. But an
            // unwritable state file means every restart recomputes from
            // scratch, which on this corpus is minutes of blank page.
            System.err.println("stats: could not persist to $path: ${e.message}")
        }
    }

    /** One published document: the exact bytes served, and the validator over them. */
    class Served(
        val bytes: ByteArray,
        val etag: String,
    )

    private companion object {
        /**
         * Not pretty-printed. This document is machine-read first — the page
         * fetches it, and anyone charting our coverage against a network-wide
         * dashboard fetches it too — and indentation on a 50-kind table is
         * bytes across the wire on every poll.
         */
        val JSON = Json

        /** The same content-derived strong ETag the pages and modules use — see `etagOf` in HttpServer. */
        fun etagOf(bytes: ByteArray): String =
            MessageDigest
                .getInstance("SHA-256")
                .digest(bytes)
                .take(8)
                .joinToString("") { "%02x".format(it) }
                .let { "\"$it\"" }
    }
}
