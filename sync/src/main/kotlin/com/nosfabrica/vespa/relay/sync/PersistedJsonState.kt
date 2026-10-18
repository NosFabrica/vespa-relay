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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.atomic.AtomicLong

/**
 * One state file's persistence: a change counter, a document built at most once per change, and
 * a flush that writes only while the counter is ahead of the file. A null [file] keeps memory only.
 */
internal class PersistedJsonState(
    private val file: File?,
    /** What the log lines call the file's contents. */
    private val what: String,
    private val flusherName: String,
    /** The whole document from the owner's maps; called outside every lock but the build's own. */
    private val build: () -> JsonObject,
) : AutoCloseable {
    /** Bumped after every change lands, so a document built at a generation holds every change up to it. */
    private val generation = AtomicLong()

    /** The generation the file holds; behind [generation], the next flush writes. */
    @Volatile private var savedAt = NEVER_SAVED

    private class Built(
        val generation: Long,
        val doc: JsonObject,
    )

    @Volatile private var built: Built? = null

    private val buildLock = Any()

    /** Held for the disk write alone, so a status read never waits on the file. */
    private val writeLock = Any()

    @Volatile private var flusher: Thread? = null

    /** Every mutation of persisted state ends here, after the mutation itself. */
    fun changed() {
        generation.incrementAndGet()
    }

    /** Takes what memory holds as what the file holds, or with [dirty] as owed to it. Called once, after [read]. */
    fun loaded(dirty: Boolean = false) {
        savedAt = if (dirty) NEVER_SAVED else generation.get()
    }

    /** Parses the file and hands its root to [restore]; an absent file is an empty start. */
    fun read(restore: (JsonObject) -> Unit) {
        val f = file ?: return
        if (!f.isFile) return
        runCatching { restore(Json.parseToJsonElement(f.readText()).jsonObject) }
            .onFailure {
                // A lost state file costs one re-walk; refusing to start costs the mirror.
                System.err.println("router: could not read $what from ${f.path} (${it.message}); starting fresh")
            }
    }

    /** The document at the current generation, shared by the status page and the flush. */
    fun current(): JsonObject = currentBuilt().doc

    private fun currentBuilt(): Built {
        built?.takeIf { it.generation == generation.get() }?.let { return it }
        synchronized(buildLock) {
            built?.takeIf { it.generation == generation.get() }?.let { return it }
            // Read before the build: a change landing during it leaves the result marked stale.
            val at = generation.get()
            return Built(at, build()).also { built = it }
        }
    }

    /** Writes the document every [intervalSec] on a daemon thread. Unchanged intervals write nothing. */
    fun startPeriodicFlush(intervalSec: Long) {
        if (file == null) return
        flusher =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(intervalSec * 1000)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                    flush()
                }
            }.apply {
                isDaemon = true
                name = flusherName
                start()
            }
    }

    /** Writes the document if anything changed since the last write. */
    fun flush() {
        synchronized(writeLock) {
            if (file == null || generation.get() == savedAt) return
            val current = currentBuilt()
            // A failed write leaves [savedAt] behind, so the next tick retries it.
            if (save(current.doc)) savedAt = current.generation
        }
    }

    /** Stops the periodic flush and writes anything outstanding. */
    override fun close() {
        flusher?.interrupt()
        flush()
    }

    private fun save(doc: JsonObject): Boolean {
        val f = file ?: return true
        return runCatching { writeJsonFile(f, doc) }
            .onFailure { System.err.println("router: could not write $what to ${f.path}: ${it.message}") }
            .isSuccess
    }

    companion object {
        /** Below every generation, so a store that has never written always has something to write. */
        private const val NEVER_SAVED = -1L

        /** How often the owners' flushers write by default. */
        const val DEFAULT_FLUSH_SECONDS = 30L
    }
}

// Pretty-printed for a human reader.
private val prettyJson = Json { prettyPrint = true }

/** Replaces [target] with [doc] via a temp file and an atomic move, so a reader never sees half a document. */
internal fun writeJsonFile(
    target: File,
    doc: JsonObject,
) {
    val dir = target.absoluteFile.parentFile
    dir.mkdirs()
    val tmp = File(dir, "${target.name}.tmp")
    tmp.writeText(prettyJson.encodeToString(JsonObject.serializer(), doc))
    // ATOMIC_MOVE asked for explicitly; without it the JVM may fall back to copy+delete.
    try {
        Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    } catch (_: AtomicMoveNotSupportedException) {
        Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }
}
