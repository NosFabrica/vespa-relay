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
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap

/**
 * Two things [NegentropyPager] must not forget: how big a window a peer will
 * reconcile, and how far down the timeline the current sweep already got.
 *
 * Both are per PEER, and neither is derivable from anything else we hold —
 * which is exactly why they are written down.
 *
 *  - **The window size** (`target`) is learned. A peer's `max_sync_events` is
 *    not advertised anywhere in NIP-11, so the only way to know it is to be
 *    refused once — quartz parses the number out of that refusal and hands it
 *    back as `NegentropySyncResult.peerCap`. Re-learning it from scratch on
 *    every boot means walking the same ladder again, against the same relay,
 *    for nothing.
 *  - **The cursor** is the thing count-based paging fundamentally cannot give
 *    you. [SyncBands] records a band only once a relay's whole filter has been
 *    walked; a sweep killed at 80% leaves nothing behind, and on a corpus large
 *    enough to need paging at all, that is the whole cost of the sync. The
 *    cursor is written per finished window, so a crash costs one window.
 *
 * Both live in one file with one flusher because they share a lifetime: they
 * are per (peer, filter shape) knowledge about a sync that is still running.
 * The on-disk shape is `{"peers": {...}, "sweeps": {...}}`, separate from
 * `SYNC_STATE_FILE` — bands are the long-lived record of what a relay has
 * given us, these are working state a completed sweep throws away.
 */
class SweepState(
    private val file: File?,
    // A cursor claims a range was compared against the peer AT A POINT IN TIME.
    // Past this age the claim is not worth acting on — the same reasoning (and
    // the same default) as a band's full-resync horizon.
    private val staleAfterSeconds: Long = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    /** What one peer will reconcile: the window size we use, and its cap if it told us. */
    data class Peer(
        val target: Int,
        val cap: Int? = null,
    )

    /**
     * The contiguous slice of one sweep that is already reconciled: everything
     * from [downTo] up to [upTo] inclusive. Contiguous by construction — the
     * pager walks a leg strictly newest-first, so the done region only ever
     * grows downward from the leg's ceiling (see [NegentropyPager.sweep]).
     */
    data class Reconciled(
        val downTo: Long,
        val upTo: Long,
        val at: Long,
    )

    private val peers = ConcurrentHashMap<String, Peer>()
    private val sweeps = ConcurrentHashMap<String, Reconciled>()

    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    init {
        load()
        dirty = false
    }

    // ---- what a peer will reconcile -----------------------------------------

    fun peer(url: NormalizedRelayUrl): Peer? = peers[url.url]

    /** The window size to try on this peer, or [fallback] for one we have never asked. */
    fun target(
        url: NormalizedRelayUrl,
        fallback: Int,
    ): Int = peers[url.url]?.target ?: fallback

    fun setTarget(
        url: NormalizedRelayUrl,
        target: Int,
    ) {
        if (peers[url.url]?.target == target) return
        // compute(), so a cap learned on the reader coroutine at the same moment
        // as a target update on a sweep coroutine cannot be dropped.
        peers.compute(url.url) { _, before -> Peer(target, before?.cap) }
        dirty = true
    }

    /**
     * The peer's own `max_sync_events`, straight from its rejection. Recorded
     * once and kept: it is a property of their config, not of this sweep.
     */
    fun learnCap(
        url: NormalizedRelayUrl,
        cap: Int,
        target: Int,
    ) {
        val before = peers[url.url]
        if (before?.cap == cap && before.target == target) return
        peers.compute(url.url) { _, _ -> Peer(target, cap) }
        dirty = true
    }

    // ---- how far the current sweep got --------------------------------------

    /**
     * What this (peer, filter shape) already reconciled, or null when there is
     * nothing usable — no cursor, or one old enough that re-comparing is the
     * honest answer.
     */
    fun reconciled(key: String): Reconciled? {
        val mark = sweeps[key] ?: return null
        return if (nowSeconds() - mark.at > staleAfterSeconds) null else mark
    }

    /** Widen the reconciled slice to include a window that just finished. */
    fun advance(
        key: String,
        downTo: Long,
        upTo: Long,
    ) {
        // compute(), not read-then-write: the flusher reads this map on another
        // thread, and two sweeps sharing a key (a relay in two streams with the
        // same filter) would otherwise be able to drop one's progress.
        //
        // Only ever widened, never replaced: windows land newest-first, so the
        // low edge is the one that moves, and a `downTo` that jumped BACKWARD
        // would silently claim an un-compared hole.
        sweeps.compute(key) { _, before ->
            Reconciled(
                downTo = minOf(before?.downTo ?: downTo, downTo),
                upTo = maxOf(before?.upTo ?: upTo, upTo),
                at = nowSeconds(),
            )
        }
        dirty = true
    }

    /**
     * Drop the cursor for a finished leg. The band [SyncBands] records at the
     * same moment is the durable statement; keeping the cursor too would let a
     * later, narrower leg inherit a claim it never earned.
     */
    fun finish(key: String) {
        if (sweeps.remove(key) != null) dirty = true
    }

    fun size(): Int = sweeps.size

    // ---- the file ------------------------------------------------------------

    fun startPeriodicFlush(intervalSec: Long = DEFAULT_FLUSH_SECONDS): SweepState {
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
                name = "sync-sweep-flush"
                start()
            }
        return this
    }

    override fun close() {
        flusher?.interrupt()
        flush()
    }

    @Synchronized
    fun flush() {
        if (!dirty) return
        dirty = false
        // Re-arm on a failed write, like SyncBands: a transiently unwritable
        // disk should be retried on the next tick, not on the next mutation.
        if (!save()) dirty = true
    }

    private fun load() {
        val f = file ?: return
        if (!f.isFile) return
        runCatching {
            val root = Json.parseToJsonElement(f.readText()).jsonObject
            root["peers"]?.jsonObject?.forEach { (url, v) ->
                val o = v.jsonObject
                peers[url] = Peer(o.getValue("target").jsonPrimitive.int, o["cap"]?.jsonPrimitive?.int)
            }
            root["sweeps"]?.jsonObject?.forEach { (k, v) ->
                val o = v.jsonObject
                sweeps[k] =
                    Reconciled(
                        o.getValue("downTo").jsonPrimitive.long,
                        o.getValue("upTo").jsonPrimitive.long,
                        o["at"]?.jsonPrimitive?.long ?: 0L,
                    )
            }
        }.onFailure {
            // Same trade as a corrupt band file: losing this costs one re-sweep,
            // refusing to start costs the mirror.
            System.err.println("router: could not read sweep state from ${f.path} (${it.message}); starting fresh")
        }
    }

    @Synchronized
    private fun save(): Boolean {
        val f = file ?: return true
        return runCatching {
            val snapshot: JsonObject =
                buildJsonObject {
                    put(
                        "peers",
                        buildJsonObject {
                            peers.forEach { (url, p) ->
                                put(
                                    url,
                                    buildJsonObject {
                                        put("target", p.target)
                                        p.cap?.let { put("cap", it) }
                                    },
                                )
                            }
                        },
                    )
                    put(
                        "sweeps",
                        buildJsonObject {
                            sweeps.forEach { (k, r) ->
                                put(
                                    k,
                                    buildJsonObject {
                                        put("downTo", r.downTo)
                                        put("upTo", r.upTo)
                                        put("at", r.at)
                                    },
                                )
                            }
                        },
                    )
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
            System.err.println("router: could not write sweep state to ${f.path}: ${it.message}")
        }.isSuccess
    }

    companion object {
        /**
         * The cursor key: the peer, plus the filter with its time bounds removed.
         *
         * Time is what the sweep VARIES, so a key that included `since`/`until`
         * would mint a fresh cursor per window and never resume anything.
         * Everything else — kinds, authors, tags — changes what is being asked
         * for, and a different ask has not been reconciled just because this one
         * was.
         *
         * Taken ONCE per sweep and passed back in, because building it
         * serialises the filter: a discovery stream's filter carries thousands
         * of authors, and re-deriving the key per finished window would re-render
         * that JSON for every window, for nothing.
         */
        fun keyFor(
            url: NormalizedRelayUrl,
            shape: Filter,
        ): String = "${url.url}|${shape.copy(since = null, until = null, limit = null).toJson()}"

        // Read by a human asking why a peer is being asked for 12,500 events at
        // a time, so it is written to be read.
        private val json = Json { prettyPrint = true }

        // A window takes minutes; flushing more often than this would write the
        // same numbers back repeatedly.
        private const val DEFAULT_FLUSH_SECONDS = 30L

        /**
         * `SYNC_SWEEP_STATE_FILE` — where the learned caps and cursors live.
         * Unset, they are kept in memory and a restart re-learns them, which is
         * correct but pays the whole ladder again.
         */
        fun fromEnv(env: Map<String, String>): SweepState =
            SweepState(
                env
                    .syncEnv("SYNC_SWEEP_STATE_FILE", "ROUTER_SWEEP_STATE_FILE")
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
