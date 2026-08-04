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
 * File persistence around quartz's [SyncCoverage]. The band arithmetic —
 * which `created_at` spans a paged relay already served, the legs outside
 * them, the shared snapshot window — graduated upstream and lives there;
 * this keeps only what is deployment-specific: the JSON state file
 * (`SYNC_STATE_FILE`), the periodic flush, and the env wiring.
 *
 * Keyed by the WHOLE filter deliberately: any edit to a filter is a new key
 * with no band, so the next run starts over — the safe direction to be wrong
 * in, and the intended way to force a re-walk.
 */
class SyncBands(
    private val file: File?,
    fullResyncSeconds: Long = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    private val coverage =
        SyncCoverage(
            fullResyncSeconds = fullResyncSeconds,
            onChange = {
                // Marked dirty, not written: a dynamic stream records once per
                // leg per relay, and every write serializes the whole map —
                // saving here would be thousands of full-file rewrites per
                // cycle. [flush] does it once; losing the last unflushed
                // window to a hard kill costs one partial re-fetch, not
                // correctness.
                dirty = true
            },
        )

    init {
        load()
        // restore() bypasses onChange, but stay defensive: reopening a file
        // must never count as a change, or every boot rewrites it.
        dirty = false
    }

    /** The filters to actually run now, given what is already covered. See [SyncCoverage.legs]. */
    fun legs(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> = coverage.legs(url, filter)

    /** Widen the band for (url, filter) to include what a completed fetch saw. See [SyncCoverage.record]. */
    fun record(
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
    ) = coverage.record(url, filter, observedMin, observedMax, paged, reconciledThrough)

    /** The narrowest single filter covering what every one of [urls] needs. See [SyncCoverage.coveringWindow]. */
    fun coveringWindow(
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter = coverage.coveringWindow(urls, filter)

    /** What is currently covered, for logging and tests. */
    fun band(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): SyncCoverage.Band? = coverage.band(url, filter)

    fun size(): Int = coverage.size()

    /**
     * Write the map every [intervalSec] on a daemon thread, so progress
     * survives a hard kill between the milestone flushes (which are minutes
     * to hours apart). Unchanged intervals write nothing.
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
        // A failed write re-arms the flag: "write anything outstanding" is
        // this method's contract, and a transiently unwritable disk should be
        // retried on the next tick, not on the next band mutation.
        if (!save()) dirty = true
    }

    private fun load() {
        val f = file ?: return
        if (!f.isFile) return
        runCatching {
            val restored = LinkedHashMap<String, SyncCoverage.Band>()
            Json.parseToJsonElement(f.readText()).jsonObject.forEach { (k, v) ->
                val o = v.jsonObject
                restored[k] =
                    SyncCoverage.Band(
                        o.getValue("min").jsonPrimitive.long,
                        o.getValue("max").jsonPrimitive.long,
                        // Absent in files written before coverage was tracked:
                        // reads as "span only" and stale, so the first run
                        // after an upgrade re-walks once and records the real
                        // thing.
                        o["complete"]?.jsonPrimitive?.boolean ?: false,
                        o["fullAt"]?.jsonPrimitive?.long ?: 0L,
                    )
            }
            coverage.restore(restored)
        }.onFailure {
            // A corrupt cursor file costs one re-sync; exiting costs the relay.
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

        /** See [SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS]. */
        const val DEFAULT_FULL_RESYNC_SECONDS = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS

        /** See [SyncCoverage.PLAUSIBLE_FLOOR]. */
        const val PLAUSIBLE_FLOOR = SyncCoverage.PLAUSIBLE_FLOOR

        /**
         * Whether a `created_at` can be believed as evidence of coverage.
         * Filter with this per EVENT, not over a leg's aggregate: one misdated
         * event once discarded a whole upstream's 700k-event band.
         */
        fun isPlausible(createdAt: Long): Boolean = SyncCoverage.isPlausible(createdAt)

        /**
         * `SYNC_STATE_FILE` — where the bands live. Unset keeps them
         * in memory, which is the same as not having them.
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
                    ?.takeIf { it > 0 } ?: DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
