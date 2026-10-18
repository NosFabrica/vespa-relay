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

import com.nosfabrica.vespa.relay.util.quarantine
import com.nosfabrica.vespa.relay.util.writeDurably
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import java.io.File
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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
    /** Reads the file's text; a seam for tests, since a root test JVM can read any file. */
    private val readText: (File) -> String = { it.readText() },
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

    /** Ends the flusher's wait. Never an interrupt: one landing in the write would close the channel mid-fsync. */
    private val stop = CountDownLatch(1)

    /** Every mutation of persisted state ends here, after the mutation itself. */
    fun changed() {
        generation.incrementAndGet()
    }

    /** Takes what memory holds as what the file holds, or with [dirty] as owed to it. Called once, after [read]. */
    fun loaded(dirty: Boolean = false) {
        savedAt = if (dirty) NEVER_SAVED else generation.get()
    }

    /**
     * Hands the file's root to [restore]. A file whose content fails part way is moved aside and [discard]
     * empties what [restore] had filled; a file that cannot be read at all stops the boot and is left in place.
     */
    fun read(
        restore: (JsonObject) -> Unit,
        discard: () -> Unit,
    ): Boolean {
        val f = file ?: return true
        if (!f.isFile) return true
        val text =
            try {
                readText(f)
            } catch (e: IOException) {
                // The file may be fine and the disk or the permissions not: quarantining it would throw a good state away.
                throw IOException(
                    "router: cannot read $what from ${f.path} (${e.message}); the file is left as it is. If the process " +
                        "user changed (the image runs as uid 10001), give it the directory: chown -R 10001:10001 ${f.absoluteFile.parent}",
                    e,
                )
            }
        // Restore reads only the parsed document, so whatever it throws short of an Error is about the file's shape.
        val failure =
            try {
                restore(Json.parseToJsonElement(text).jsonObject)
                return true
            } catch (e: RuntimeException) {
                e
            }
        discard()
        // A lost state file costs one re-walk; refusing to start costs the mirror.
        val aside = quarantine(f)
        System.err.println(
            "router: could not read $what from ${f.path} (${failure.message}); starting from an empty state" +
                (aside?.let { ", the unreadable file kept as ${it.name}" } ?: ", and the unreadable file could not be moved aside"),
        )
        return false
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
                try {
                    while (!stop.await(intervalSec, TimeUnit.SECONDS)) flush()
                } catch (_: InterruptedException) {
                    // Nothing here interrupts it; leave the final write to close().
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

    /** Stops the periodic flush, waits out a write it has in progress, then writes anything outstanding. */
    override fun close() {
        stop.countDown()
        try {
            flusher?.join()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
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

/** Replaces [target] with [doc] so a crash leaves the old document or the new one, never half of either. */
internal fun writeJsonFile(
    target: File,
    doc: JsonObject,
) = writeDurably(target, prettyJson.encodeToString(JsonObject.serializer(), doc))
