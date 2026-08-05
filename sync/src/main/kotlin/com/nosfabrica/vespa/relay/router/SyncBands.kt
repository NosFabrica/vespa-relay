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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.config.syncEnv
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * The router's sync bands: file persistence around quartz's [SyncCoverage].
 *
 * The band arithmetic — what a (relay, filter) pair already covers, which legs
 * are still outstanding, how wide a shared negentropy snapshot has to be — USED
 * TO LIVE HERE, as ~150 lines this file owned outright. It is upstream now, and
 * this class is the half that never was: where the map is written, when, and
 * which environment variable names it.
 *
 * That fork had already cost something. Upstream fixed two things this copy
 * never picked up — [SyncCoverage.coveringWindow] stopped letting a relay that
 * needs NOTHING widen the shared snapshot to the full filter, and
 * [SyncCoverage.legs] learned that a COMPLETE band still owes an older leg when
 * the caller's floor now reaches below it — and both bugs sat here, in a file
 * whose comments still described them as solved. `SYNC_STATE_FILE`'s on-disk
 * shape is identical either way (`{key: {min, max, complete, fullAt}}`), so an
 * existing deployment's bands load across this change untouched.
 *
 * Persistence is deliberately the CALLER's in quartz: [SyncCoverage.export] and
 * [SyncCoverage.restore] hand over the whole map, and `onChange` fires when a
 * band moves so a writer can mark itself dirty without polling. Amethyst's own
 * `SyncCoverageFile` is the same wrapper for geode; this one differs only where
 * the router needs it to — a null [file] for the in-memory mode the engine
 * defaults to, and a failed write that re-arms rather than being dropped.
 */
class SyncBands(
    private val file: File?,
    fullResyncSeconds: Long = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    private val coverage = SyncCoverage(fullResyncSeconds, onChange = { dirty = true })

    init {
        load()
        // restore() does not fire onChange, but stay defensive: reopening a
        // file must never count as a change, or every boot rewrites it.
        dirty = false
    }

    // ---- the band arithmetic, upstream's ------------------------------------
    // Delegated rather than exposing `coverage` directly: these five calls are
    // the entire surface the router uses, and naming them keeps that visible.

    fun legs(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> = coverage.legs(url, filter)

    fun record(
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
    ) = coverage.record(url, filter, observedMin, observedMax, paged, reconciledThrough)

    fun coveringWindow(
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter = coverage.coveringWindow(urls, filter)

    /** Whether ANY of [urls] still has work outside its band. */
    fun anyOutstanding(
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Boolean = urls.any { coverage.legs(it, filter).isNotEmpty() }

    fun band(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): SyncCoverage.Band? = coverage.band(url, filter)

    fun size(): Int = coverage.size()

    // ---- the file ------------------------------------------------------------

    /**
     * Write the map every [intervalSec] on a daemon thread, so progress
     * survives a hard kill between the milestone flushes (which are minutes to
     * hours apart). Unchanged intervals write nothing.
     */
    fun startPeriodicFlush(intervalSec: Long = DEFAULT_FLUSH_SECONDS): SyncBands {
        if (file == null) return this
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
                name = "sync-cursor-flush"
                start()
            }
        return this
    }

    /** Stop the periodic flush and write anything outstanding. */
    override fun close() {
        flusher?.interrupt()
        flush()
    }

    /** Write the map if anything changed since the last write. */
    @Synchronized
    fun flush() {
        if (!dirty) return
        dirty = false
        // A failed write re-arms the flag: "write anything outstanding" is this
        // method's contract, and a transiently unwritable disk should be retried
        // on the next tick, not on the next band mutation.
        if (!save()) dirty = true
    }

    private fun load() {
        val f = file ?: return
        if (!f.isFile) return
        runCatching {
            val root = Json.parseToJsonElement(f.readText()).jsonObject
            coverage.restore(
                root.mapValues { (_, v) ->
                    val o = v.jsonObject
                    SyncCoverage.Band(
                        o.getValue("min").jsonPrimitive.long,
                        o.getValue("max").jsonPrimitive.long,
                        // Absent in files written before coverage was tracked:
                        // reads as "span only" and stale, so the first run after
                        // an upgrade re-walks once and records the real thing.
                        o["complete"]?.jsonPrimitive?.boolean ?: false,
                        o["fullAt"]?.jsonPrimitive?.long ?: 0L,
                    )
                },
            )
        }.onFailure {
            // A corrupt cursor file costs one re-sync; exiting costs the mirror.
            System.err.println("router: could not read sync bands from ${f.path} (${it.message}); starting fresh")
        }
    }

    /** Persist via a temp file and an atomic move, so a reader never sees a half-written map. */
    @Synchronized
    private fun save(): Boolean {
        val f = file ?: return true
        return runCatching {
            val snapshot: JsonObject =
                buildJsonObject {
                    coverage.export().forEach { (k, band) ->
                        put(
                            k,
                            buildJsonObject {
                                put("min", band.minCreatedAt)
                                put("max", band.maxCreatedAt)
                                put("complete", band.complete)
                                put("fullAt", band.fullAt)
                            },
                        )
                    }
                }
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), snapshot))
            // ATOMIC_MOVE asked for explicitly: without it the JVM may legally
            // fall back to copy+delete, and a reader could see a half map.
            try {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: AtomicMoveNotSupportedException) {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        }.onFailure {
            System.err.println("router: could not write sync bands to ${f.path}: ${it.message}")
        }.isSuccess
    }

    companion object {
        // Pretty-printed: this file is read by a human debugging why a relay
        // re-synced.
        private val json = Json { prettyPrint = true }

        // Often enough that a kill costs little, rare enough to be free.
        private const val DEFAULT_FLUSH_SECONDS = 30L

        /**
         * `SYNC_STATE_FILE` — where the bands live. Unset keeps them in memory,
         * which is the same as not having them.
         */
        fun fromEnv(env: Map<String, String>): SyncBands =
            SyncBands(
                env
                    .syncEnv("SYNC_STATE_FILE", "ROUTER_SYNC_STATE_FILE")
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
                env
                    .syncEnv("SYNC_FULL_RESYNC_SECONDS", "ROUTER_FULL_RESYNC_SECONDS")
                    ?.trim()
                    ?.toLongOrNull()
                    ?.takeIf { it > 0 } ?: SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
