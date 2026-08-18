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
 * are per (stream, filter shape, peer) knowledge about a sync that is still
 * running. Separate from `SYNC_STATE_FILE` — bands are the long-lived record of
 * what a relay has given us, these are working state a completed sweep throws
 * away.
 *
 * The on-disk shape nests the three parts of a cursor's identity rather than
 * concatenating them:
 *
 * ```json
 * {
 *   "peers":  { "wss://relay/": { "target": 12500, "cap": 500000 } },
 *   "sweeps": { "<stream>": { "<filter>": { "wss://relay/": { "downTo": …, "upTo": …, "at": … } } } }
 * }
 * ```
 *
 * `peers` stays flat because it is keyed by the peer ALONE — `max_sync_events`
 * is a property of their config, not of anything we ask for.
 */
class SweepState(
    private val file: File?,
    // A cursor claims a range was compared against the peer AT A POINT IN TIME.
    // Past this age the claim is not worth acting on and the sweep starts over.
    //
    // NOT the same knob as a stream's `refetchThePastSeconds`, though it read
    // the same env var until that one became per-stream. This decides whether
    // an INTERRUPTED sweep resumes or restarts — work already scheduled, and
    // at most one sweep's worth either way — so it keeps a default. The other
    // decides whether a whole history is downloaded again on a clock, which is
    // why nothing may schedule that without being asked to.
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

    /**
     * Who is asking, for what, and of whom — the three parts of a cursor's
     * identity, held apart instead of joined into a string.
     *
     * [filter] is the serialised shape, not the [Filter]: it is taken ONCE per
     * sweep ([keyFor]) because serialising a discovery stream's filter renders
     * thousands of authors, and it is what the file nests under, so keeping the
     * rendered form is also what makes a save free of that cost.
     *
     * [stream] is part of the identity, not a label. Two streams that happen to
     * ask one relay the same filter are two sweeps: they start at different
     * moments and stop at different depths, and a cursor one of them wrote is a
     * claim about ITS walk. Sharing them let a stream inherit ground it had not
     * compared; keeping them apart costs at most one window walked twice, which
     * is the cheap side of that trade.
     */
    data class Cursor(
        val stream: String,
        val filter: String,
        val relay: String,
    )

    private val peers = ConcurrentHashMap<String, Peer>()
    private val sweeps = ConcurrentHashMap<Cursor, Reconciled>()

    /**
     * MIGRATION SHIM — cursors read from a file written before the format
     * nested, keyed by (filter, relay) because the flat key never said which
     * stream wrote them.
     *
     * They are claimed by the first stream to ask for that pair (see
     * [reconciled]) and re-written flat until then, so a router that restarts
     * before its slower streams have got to their first window does not lose
     * them. Delete this map, [claim], the flat branch in [load] and the flat
     * branch in [save] together once every deployment has started once on a
     * build that writes the nested shape.
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
     * What this (stream, filter shape, peer) already reconciled, or null when
     * there is nothing usable — no cursor, or one old enough that re-comparing
     * is the honest answer.
     */
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
        // Before the compute(), so a pre-stream cursor this sweep is resuming
        // is widened rather than overwritten by its first finished window.
        claim(key)
        // compute(), not read-then-write: the flusher reads this map on another
        // thread, and two coroutines advancing one cursor (the same stream's
        // legs against one relay) would otherwise be able to drop one's
        // progress.
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
    fun finish(key: Cursor) {
        val had = sweeps.remove(key) != null
        // The pre-stream cursor for the same pair goes with it, or the next
        // stream to ask would claim a slice this leg has already superseded.
        val hadOld = preStream.remove(key.filter to key.relay) != null
        if (had || hadOld) dirty = true
    }

    fun size(): Int = sweeps.size + preStream.size

    /**
     * MIGRATION SHIM — adopt a pre-stream cursor for this pair, once, into the
     * stream that asked for it.
     *
     * The stream that asks IS the stream that wrote it, in every case but two
     * streams sharing a filter against one relay — where first-ask-wins is the
     * behaviour the flat file already had, since they shared the one key.
     */
    private fun claim(key: Cursor): Reconciled? {
        if (preStream.isEmpty()) return null
        val mark = preStream.remove(key.filter to key.relay) ?: return null
        dirty = true
        // Staleness is absolute, not per stream: a claim this old is not worth
        // acting on for ANY stream, so it is dropped here rather than moved
        // into one stream's map to sit there being ignored — and dropping it
        // leaves it claimable by nobody, which is what it is worth.
        if (nowSeconds() - mark.at > staleAfterSeconds) return null
        // merge(), not put: a sweep may be advancing this cursor on another
        // coroutine right now (claim runs from advance() too), and a put would
        // drop the window it has just finished. Same widening rule as
        // [advance] — the low edge only ever falls.
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
            root["sweeps"]?.jsonObject?.forEach { (streamOrFlatKey, v) ->
                val o = v.jsonObject
                // MIGRATION SHIM. Told apart by SHAPE, not by the key: a
                // pre-stream entry is the mark itself, a stream is filters all
                // the way down. A filter can never be named `downTo` — it is
                // serialised JSON and starts with `{`.
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
            // Same trade as a corrupt band file: losing this costs one re-sweep,
            // refusing to start costs the mirror.
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

    /**
     * Every cursor and peer cap, in the shape [save] writes and the status page
     * reads.
     *
     * Extracted from [save] rather than duplicated for the page: the status
     * site renders this state in the SAME process that holds it, so a second
     * construction here would be a second format that could drift from the one
     * on disk — and the file is what a restart reloads.
     */
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
                    // Grouped once here rather than held grouped: the
                    // maps are read and written by sweep coroutines,
                    // and one flat ConcurrentHashMap is the shape that
                    // needs no lock to stay consistent.
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
                    // MIGRATION SHIM: unclaimed pre-stream cursors go
                    // back out flat, because there is still no stream to
                    // file them under. They drain as their streams reach
                    // them; goes when the reader above does.
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
         * The cursor's identity: the stream asking, the filter with its time
         * bounds removed, and the peer.
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
            stream: String,
            url: NormalizedRelayUrl,
            shape: Filter,
        ): Cursor = Cursor(stream, shape.copy(since = null, until = null, limit = null).toJson(), url.url)

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
         *
         * `SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS` is how old a resume cursor
         * may be and still be resumed from. It used to read the same env var a
         * band's re-walk period did, which is how one number came to mean two
         * things; that one is `refetchThePastSeconds` on a stream now, and this
         * kept the env because it schedules nothing — see [staleAfterSeconds].
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
