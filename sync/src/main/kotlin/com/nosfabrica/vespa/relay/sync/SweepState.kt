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

import com.nosfabrica.vespa.relay.config.syncEnv
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
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap

/**
 * What [NegentropyPager] must not forget between calls: how big a window a
 * peer will reconcile (learned from its refusal; NIP-11 does not advertise
 * it) and how far down the timeline the current sweep got (written per
 * finished window, so a crash costs one window). Working state a completed
 * sweep throws away, separate from the long-lived bands.
 *
 * ```json
 * {
 *   "peers":  { "wss://relay/": { "target": 12500, "cap": 500000 } },
 *   "sweeps": { "<stream>": { "<filter>": { "wss://relay/": { "downTo": …, "upTo": …, "at": … } } } }
 * }
 * ```
 *
 * `peers` stays flat because a cap is a property of the peer alone.
 */
class SweepState(
    private val file: File?,
    /**
     * Past this age an interrupted sweep restarts instead of resuming. Not a
     * stream's `refetchThePastSeconds`: this schedules nothing, so it keeps a default.
     */
    private val staleAfterSeconds: Long = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    /** What one peer will reconcile: the window size we use, and its cap if it told us. */
    data class Peer(
        val target: Int,
        val cap: Int? = null,
    )

    /** The contiguous slice of one sweep already reconciled, [downTo]..[upTo] inclusive. */
    data class Reconciled(
        val downTo: Long,
        val upTo: Long,
        val at: Long,
    )

    /**
     * A cursor's identity. [filter] is the serialised shape, taken once per
     * sweep by [keyFor]. [stream] is part of the identity: two streams asking
     * one relay the same filter are two sweeps, and one may not inherit the
     * other's claim.
     */
    data class Cursor(
        val stream: String,
        val filter: String,
        val relay: String,
    )

    private val peers = ConcurrentHashMap<String, Peer>()
    private val sweeps = ConcurrentHashMap<Cursor, Reconciled>()

    /**
     * Migration shim: cursors from a file written before the format nested,
     * keyed by (filter, relay). Claimed by the first stream to ask and
     * re-written flat until then. Delete with [claim] and the flat branches
     * in [load] and [snapshot] together.
     */
    private val preStream = ConcurrentHashMap<Pair<String, String>, Reconciled>()

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
        // compute(), so a cap learned on another coroutine at the same moment is not dropped.
        peers.compute(url.url) { _, before -> Peer(target, before?.cap) }
        dirty = true
    }

    /** The peer's own `max_sync_events`, from its rejection. A property of their config, so kept. */
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

    /** What this cursor already reconciled, or null when there is no usable claim. */
    fun reconciled(key: Cursor): Reconciled? {
        val mark = sweeps[key] ?: claim(key) ?: return null
        return if (nowSeconds() - mark.at > staleAfterSeconds) null else mark
    }

    /** Widen the reconciled slice to include a window that just finished. */
    fun advance(
        key: Cursor,
        downTo: Long,
        upTo: Long,
    ) {
        // Before the compute(), so a pre-stream cursor is widened rather than overwritten.
        claim(key)
        // Merged only when the window touches the claim: the claim is one range
        // and must stay compared throughout. A disjoint window keeps the
        // standing claim and only moves the liveness stamp.
        sweeps.compute(key) { _, before ->
            if (before == null || (downTo <= before.upTo + 1 && upTo >= before.downTo - 1)) {
                Reconciled(
                    downTo = minOf(before?.downTo ?: downTo, downTo),
                    upTo = maxOf(before?.upTo ?: upTo, upTo),
                    at = nowSeconds(),
                )
            } else {
                Reconciled(downTo = before.downTo, upTo = before.upTo, at = nowSeconds())
            }
        }
        dirty = true
    }

    /** Drop the cursor for a finished leg; the band recorded at the same moment is the durable statement. */
    fun finish(key: Cursor) {
        val had = sweeps.remove(key) != null
        // The pre-stream cursor for the same pair goes with it, or the next stream to ask would claim it.
        val hadOld = preStream.remove(key.filter to key.relay) != null
        if (had || hadOld) dirty = true
    }

    fun size(): Int = sweeps.size + preStream.size

    /** Migration shim: adopt a pre-stream cursor for this pair, once, into the stream that asked. */
    private fun claim(key: Cursor): Reconciled? {
        if (preStream.isEmpty()) return null
        val mark = preStream.remove(key.filter to key.relay) ?: return null
        dirty = true
        // Staleness is absolute: a claim this old is worth nothing to any stream.
        if (nowSeconds() - mark.at > staleAfterSeconds) return null
        // merge(), not put: a sweep may be advancing this cursor on another coroutine right now.
        return sweeps.merge(key, mark) { held, old ->
            Reconciled(
                downTo = minOf(held.downTo, old.downTo),
                upTo = maxOf(held.upTo, old.upTo),
                at = maxOf(held.at, old.at),
            )
        }
    }

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
        // A failed write re-arms the flag, so the next tick retries it.
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
            root["sweeps"]?.jsonObject?.forEach { (streamOrFlatKey, v) ->
                val o = v.jsonObject
                // Migration shim, told apart by shape: a filter is serialised JSON and can never be named `downTo`.
                if (o["downTo"] != null) {
                    val at = streamOrFlatKey.indexOf('|')
                    if (at <= 0 || at == streamOrFlatKey.length - 1) return@forEach
                    val relay = streamOrFlatKey.substring(0, at)
                    val filter = streamOrFlatKey.substring(at + 1)
                    preStream[filter to relay] = mark(o) ?: return@forEach
                    return@forEach
                }
                o.forEach { (filter, byRelay) ->
                    byRelay.jsonObject.forEach { (relay, m) ->
                        sweeps[Cursor(streamOrFlatKey, filter, relay)] = mark(m.jsonObject) ?: return@forEach
                    }
                }
            }
        }.onFailure {
            // Losing this costs one re-sweep; refusing to start costs the mirror.
            System.err.println("router: could not read sweep state from ${f.path} (${it.message}); starting fresh")
        }
    }

    /** One cursor, as it is written. */
    private fun mark(r: Reconciled): JsonObject =
        buildJsonObject {
            put("downTo", r.downTo)
            put("upTo", r.upTo)
            put("at", r.at)
        }

    /** One cursor's edges, or null for an entry too damaged to be a claim. */
    private fun mark(o: JsonObject): Reconciled? {
        val downTo = o["downTo"]?.jsonPrimitive?.longOrNull ?: return null
        val upTo = o["upTo"]?.jsonPrimitive?.longOrNull ?: return null
        return Reconciled(downTo, upTo, o["at"]?.jsonPrimitive?.longOrNull ?: 0L)
    }

    /** Every cursor and peer cap, in the one shape [save] writes and the status page reads. */
    @Synchronized
    internal fun snapshot(): JsonObject =
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
                    // Grouped here rather than held grouped: one flat map needs no lock.
                    val byStream = LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, Reconciled>>>()
                    sweeps.forEach { (k, r) ->
                        byStream
                            .getOrPut(k.stream) { LinkedHashMap() }
                            .getOrPut(k.filter) { LinkedHashMap() }[k.relay] = r
                    }
                    byStream.forEach { (stream, byFilter) ->
                        put(
                            stream,
                            buildJsonObject {
                                byFilter.forEach { (filter, byRelay) ->
                                    put(
                                        filter,
                                        buildJsonObject {
                                            byRelay.forEach { (relay, r) -> put(relay, mark(r)) }
                                        },
                                    )
                                }
                            },
                        )
                    }
                    // Migration shim: unclaimed pre-stream cursors go back out flat.
                    preStream.forEach { (pair, r) -> put("${pair.second}|${pair.first}", mark(r)) }
                },
            )
        }

    @Synchronized
    private fun save(): Boolean {
        val f = file ?: return true
        return runCatching {
            val snapshot: JsonObject = snapshot()
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
         * The cursor's identity: the stream, the filter with its time bounds
         * removed (time is what the sweep varies), and the peer. Taken once
         * per sweep because it serialises the filter.
         */
        fun keyFor(
            stream: String,
            url: NormalizedRelayUrl,
            shape: Filter,
        ): Cursor = Cursor(stream, shape.copy(since = null, until = null, limit = null).toJson(), url.url)

        // Pretty-printed: read by a human asking why a peer gets the window size it does.
        private val json = Json { prettyPrint = true }

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /**
         * `SYNC_SWEEP_STATE_FILE`, unset for in-memory (a restart re-learns
         * every cap). `SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS` is how old a
         * resume cursor may be and still be resumed from.
         */
        fun fromEnv(env: Map<String, String>): SweepState =
            SweepState(
                env
                    .syncEnv("SYNC_SWEEP_STATE_FILE", "ROUTER_SWEEP_STATE_FILE")
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
                env["SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS"]
                    ?.trim()
                    ?.toLongOrNull()
                    ?.takeIf { it > 0 } ?: SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
