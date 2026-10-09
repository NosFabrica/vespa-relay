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

import com.nosfabrica.vespa.relay.util.nowSeconds
import com.nosfabrica.vespa.relay.util.strictLong
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
import java.util.concurrent.atomic.AtomicLong

/**
 * What [NegentropyPager] must not forget between calls: how big a window a peer will reconcile,
 * learned from its refusal, and how far down the timeline the current sweep got, written per
 * finished window. Working state a completed sweep throws away, separate from the bands.
 */
class SweepState(
    private val file: File?,
    /** Past this age an interrupted sweep restarts instead of resuming. Schedules nothing, so it has a default. */
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
     * A cursor's identity, built by [keyFor]. [stream] is part of it: two streams asking one
     * relay the same filter are two sweeps, and one may not inherit the other's claim.
     */
    data class Cursor(
        val stream: String,
        val filter: String,
        val relay: String,
    )

    private val peers = ConcurrentHashMap<String, Peer>()
    private val sweeps = ConcurrentHashMap<Cursor, Reconciled>()

    /** Per peer url, when it last left the roster. */
    private val unowned = UnownedClock<String>()

    /**
     * Migration shim: cursors from a file written before the format nested, claimed by the
     * first stream to ask. Delete with [claim] and the flat branches in [load] and [snapshot].
     */
    private val preStream = ConcurrentHashMap<Pair<String, String>, Reconciled>()

    /** Bumped after every change lands, so a snapshot built at a generation holds every change up to it. */
    private val generation = AtomicLong()

    /** The generation the file holds; behind [generation], the next flush writes. */
    @Volatile private var savedAt = NEVER_SAVED

    /** The last snapshot and the generation it was built at. */
    private class Built(
        val generation: Long,
        val doc: JsonObject,
    )

    @Volatile private var built: Built? = null

    private val buildLock = Any()

    /** Held for the disk write alone, so a status read never waits on the file. */
    private val writeLock = Any()

    /** Every mutation of persisted state ends here, after the mutation itself. */
    private fun changed() {
        generation.incrementAndGet()
    }

    @Volatile private var flusher: Thread? = null

    init {
        load()
        savedAt = generation.get()
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
        changed()
    }

    /** The peer's own `max_sync_events`, from its rejection. */
    fun learnCap(
        url: NormalizedRelayUrl,
        cap: Int,
        target: Int,
    ) {
        val before = peers[url.url]
        if (before?.cap == cap && before.target == target) return
        peers.compute(url.url) { _, _ -> Peer(target, cap) }
        changed()
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
        // Merged only when the window touches the claim, which must stay one compared range;
        // a disjoint window only moves the liveness stamp.
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
        changed()
    }

    /** Drop the cursor for a finished leg; the band recorded at the same moment is the durable statement. */
    fun finish(key: Cursor) {
        val had = sweeps.remove(key) != null
        // The pre-stream cursor for the same pair goes with it, or the next stream to ask would claim it.
        val hadOld = preStream.remove(key.filter to key.relay) != null
        if (had || hadOld) changed()
    }

    fun size(): Int = sweeps.size + preStream.size

    /**
     * Drops cursors too old to resume from and the learned sizes of peers [relays] has not held
     * for [UnownedClock.UNOWNED_TTL_SECONDS]. [relays] is every url on the current roster.
     */
    fun retain(
        relays: Set<String>,
        now: Long = nowSeconds(),
    ) {
        val stale = sweeps.keys.removeIf { key -> sweeps[key]?.let { now - it.at > staleAfterSeconds } == true }
        val staleOld = preStream.keys.removeIf { key -> preStream[key]?.let { now - it.at > staleAfterSeconds } == true }
        val clocksBefore = unowned.stamps()
        val gone = unowned.expired(peers.keys.toSet(), relays, now)
        peers.keys.removeAll(gone)
        if (stale || staleOld || gone.isNotEmpty() || unowned.stamps() != clocksBefore) changed()
    }

    /** Migration shim: adopt a pre-stream cursor for this pair, once, into the stream that asked. */
    private fun claim(key: Cursor): Reconciled? {
        if (preStream.isEmpty()) return null
        val mark = preStream.remove(key.filter to key.relay) ?: return null
        changed()
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

    fun flush() {
        synchronized(writeLock) {
            if (file == null || generation.get() == savedAt) return
            val current = current()
            // A failed write leaves [savedAt] behind, so the next tick retries it.
            if (save(current.doc)) savedAt = current.generation
        }
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
            root[UNOWNED_SINCE]?.jsonObject?.forEach { (url, ts) -> ts.jsonPrimitive.longOrNull?.let { unowned.restore(url, it) } }
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
    internal fun snapshot(): JsonObject = current().doc

    private fun current(): Built {
        built?.takeIf { it.generation == generation.get() }?.let { return it }
        synchronized(buildLock) {
            built?.takeIf { it.generation == generation.get() }?.let { return it }
            // Read before the build: a change landing during it leaves the result marked stale.
            val at = generation.get()
            return Built(at, build()).also { built = it }
        }
    }

    private fun build(): JsonObject =
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
            val stamps = unowned.stamps()
            if (stamps.isNotEmpty()) put(UNOWNED_SINCE, buildJsonObject { stamps.forEach { (url, at) -> put(url, at) } })
        }

    private fun save(doc: JsonObject): Boolean {
        val f = file ?: return true
        return runCatching {
            val text = json.encodeToString(JsonObject.serializer(), doc)
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(text)
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
        /** Below every generation, so a store that has never written always has something to write. */
        private const val NEVER_SAVED = -1L

        /**
         * The cursor's identity: the stream, the filter with its time bounds removed, and the
         * peer. Taken once per sweep because it serialises the filter.
         */
        fun keyFor(
            stream: String,
            url: NormalizedRelayUrl,
            shape: Filter,
        ): Cursor = Cursor(stream, shape.copy(since = null, until = null, limit = null).toJson(), url.url)

        // Pretty-printed for a human reader.
        private val json = Json { prettyPrint = true }

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /** Beside `peers` and `sweeps`, which are all the status page reads. */
        private const val UNOWNED_SINCE = "unownedSince"

        /**
         * `SYNC_SWEEP_STATE_FILE`, unset for in-memory; `SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS`
         * is how old a resume cursor may be and still be resumed from.
         */
        fun fromEnv(env: Map<String, String>): SweepState =
            SweepState(
                env["SYNC_SWEEP_STATE_FILE"]
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
                env.strictLong("SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS", 1L..Long.MAX_VALUE) ?: SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
            ).startPeriodicFlush()
    }
}
