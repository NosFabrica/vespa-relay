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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap

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
 * whose comments still described them as solved. What a band CONTAINS
 * (`{min, max, complete, fullAt, spans}`) is identical either way; only how
 * `SYNC_STATE_FILE` keys them has changed since, and a file written before that
 * still loads (see the shim below).
 *
 * `complete` is written at BOTH levels and read from the inner one. It belongs
 * per kind — a paged leg that drained `kinds: [10002]` says nothing about kind 0
 * — but the band-level copy stays for a rollback, and a span that carries none
 * of its own inherits it, which is exactly what the flag used to mean for every
 * kind at once.
 *
 * Persistence is deliberately the CALLER's in quartz: [SyncCoverage.export] and
 * [SyncCoverage.restore] hand over the whole map, and `onChange` fires when a
 * band moves so a writer can mark itself dirty without polling. Amethyst's own
 * `SyncCoverageFile` is the same wrapper for geode; this one differs only where
 * the router needs it to — a null [file] for the in-memory mode the engine
 * defaults to, and a failed write that re-arms rather than being dropped.
 *
 * ## One coverage per stream
 *
 * quartz keys a band by (relay, filter) and knows nothing about streams, so the
 * stream is expressed HERE, as one [SyncCoverage] per stream name. That is what
 * lets the file nest by stream, and it makes the identity honest: two streams
 * that ask one relay the same filter walk it separately, at their own moments,
 * and neither may resume from the other's claim.
 *
 * The file follows the same three levels:
 *
 * ```json
 * { "<stream>": { "<filter>": { "wss://relay/": {
 *     "min": …, "max": …, "complete": …, "fullAt": …,
 *     "spans": { "<kind>": { "min": …, "max": …, "complete": … } }
 * } } } }
 * ```
 *
 * The two inner levels are the two halves of quartz's [SyncCoverage.BandKey],
 * which `export`/`restore` hand over as a pair. They used to arrive joined —
 * `"<relay-url> <filter.toJson()>"` — and this class split them at the first
 * space, on a separator it could only learn by reading `SyncCoverage`. That is
 * upstream's job now (amethyst#3877), which is why nothing here knows how a key
 * is spelled any more.
 */
class SyncBands(
    private val file: File?,
    private val fullResyncSeconds: Long = SyncCoverage.DEFAULT_FULL_RESYNC_SECONDS,
) : AutoCloseable {
    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    private val streams = ConcurrentHashMap<String, SyncCoverage>()

    /**
     * MIGRATION SHIM — bands read from a file written before the format nested,
     * held by quartz's flat key because it never said which stream wrote them.
     *
     * Kept as the raw JSON rather than parsed: an unclaimed band is re-written
     * verbatim, which is also how a member this build does not know about
     * survives the round trip. They are claimed by the first stream to ask
     * about that (relay, filter) — which is the stream that wrote them, bar two
     * streams sharing a filter, where first-ask-wins is what the flat file did
     * anyway. Delete this map, [claim], the flat branch in [load] and the flat
     * branch in [save] together once every deployment has started once on a
     * build that writes the nested shape.
     */
    private val preStream = ConcurrentHashMap<String, JsonObject>()

    /**
     * MIGRATION SHIM — the relays [preStream] holds anything for, so an ask
     * about any other relay never renders its filter.
     *
     * Without it, every `legs()` in a cycle serialises a discovery filter's
     * thousands of authors just to look for a leftover that is not there —
     * thousands of times per cycle, and forever, because an entry no live
     * stream ever asks about never drains. Membership is set once at load and
     * not pruned: a stale hit costs one key that misses, never a wrong answer.
     */
    private val preStreamRelays = HashSet<String>()

    init {
        load()
        // restore() does not fire onChange, but stay defensive: reopening a
        // file must never count as a change, or every boot rewrites it.
        dirty = false
    }

    /**
     * The bands of one stream. Created on first use: a stream that never syncs
     * costs nothing, and the engine does not announce its stream list here.
     */
    private fun coverage(stream: String): SyncCoverage = streams.computeIfAbsent(stream) { SyncCoverage(fullResyncSeconds, onChange = { dirty = true }) }

    // ---- the band arithmetic, upstream's ------------------------------------
    // Delegated rather than exposing `coverage` directly: these five calls are
    // the entire surface the router uses, and naming them keeps that visible.

    fun legs(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> {
        claim(stream, url, filter)
        return coverage(stream).legs(url, filter)
    }

    fun record(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
        // Required for a multi-kind filter on the PAGED path: without it quartz
        // records no band rather than let one interval speak for every kind,
        // and every multi-kind stream here would stop resuming. A reconcile
        // ignores it — it compares the whole filter in one pass.
        observedByKind: Map<Int, SyncCoverage.Span>? = null,
        // The relay EOSEd on an empty page, so there is nothing below what this
        // walk saw and the band may finally close its older leg. Gate every call
        // site on [drainSettlesThePast] — a drain on the NEWER leg is not a claim
        // about history.
        drained: Boolean = false,
    ) {
        // Before the record, so what this walk observed WIDENS the pre-stream
        // band instead of replacing it — quartz widens, and a claim that landed
        // after would be the older, wider claim overwriting the newer one.
        claim(stream, url, filter)
        coverage(stream).record(url, filter, observedMin, observedMax, paged, reconciledThrough, observedByKind, drained)
    }

    fun coveringWindow(
        stream: String,
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter {
        urls.forEach { claim(stream, it, filter) }
        return coverage(stream).coveringWindow(urls, filter)
    }

    /** Whether ANY of [urls] still has work outside its band. */
    fun anyOutstanding(
        stream: String,
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Boolean = urls.any { legs(stream, it, filter).isNotEmpty() }

    fun band(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): SyncCoverage.Band? {
        claim(stream, url, filter)
        return coverage(stream).band(url, filter)
    }

    fun size(): Int = streams.values.sumOf { it.size() } + preStream.size

    /**
     * MIGRATION SHIM — adopt a pre-stream band for this pair, once, into the
     * stream that asked for it.
     *
     * [SyncCoverage.restore] sets one key at a time rather than replacing the
     * map, so a single-entry restore is what an insert would be if quartz had
     * one — which is why claiming is affordable per ask rather than needing the
     * stream list up front.
     */
    private fun claim(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ) {
        // The overwhelming case, once a deployment has migrated: no file to
        // read, or one already drained. Both checks come before the key is
        // built, because building it renders the filter.
        if (preStream.isEmpty() || url.url !in preStreamRelays) return
        val key = SyncCoverage.BandKey(url.url, filter.toJson())
        val raw = preStream.remove(key.encode()) ?: return
        bandOf(raw)?.let {
            coverage(stream).restore(mapOf(key to it))
            dirty = true
        }
    }

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
            root.forEach { (streamOrFlatKey, v) ->
                val o = v.jsonObject
                // MIGRATION SHIM. Told apart by SHAPE, not by the key: a
                // pre-stream entry is the band itself, a stream is filters all
                // the way down. A filter can never be named `min` — it is
                // serialised JSON and starts with `{`.
                if (o["min"] != null) {
                    // Decoded by the class that mints the key, so even the shim
                    // no longer spells the separator out. A key that names no
                    // pair is not one any ask can ever claim, so it is dropped
                    // rather than held for a relay nothing can look up.
                    val key = SyncCoverage.BandKey.decode(streamOrFlatKey) ?: return@forEach
                    preStream[streamOrFlatKey] = o
                    preStreamRelays += key.relay
                    return@forEach
                }
                val restored = LinkedHashMap<SyncCoverage.BandKey, SyncCoverage.Band>()
                o.forEach { (filter, byRelay) ->
                    byRelay.jsonObject.forEach { (relay, band) ->
                        // Straight back into the pair, which is what the two
                        // inner levels have always been.
                        bandOf(band.jsonObject)?.let { restored[SyncCoverage.BandKey(relay, filter)] = it }
                    }
                }
                if (restored.isNotEmpty()) coverage(streamOrFlatKey).restore(restored)
            }
        }.onFailure {
            // A corrupt cursor file costs one re-sync; exiting costs the mirror.
            System.err.println("router: could not read sync bands from ${f.path} (${it.message}); starting fresh")
        }
    }

    /** One band, as it is written. */
    private fun bandOf(band: SyncCoverage.Band): JsonObject =
        buildJsonObject {
            // min/max are the outer edges across every kind, written for two
            // readers: a human debugging why a relay re-synced, and a ROLLBACK
            // — a build from before per-kind spans reads these and behaves as
            // it always did rather than failing to parse.
            put("min", band.minCreatedAt)
            put("max", band.maxCreatedAt)
            put("complete", band.complete)
            put("fullAt", band.fullAt)
            put(
                "spans",
                buildJsonObject {
                    band.spans.forEach { (kind, span) ->
                        put(
                            kind.toString(),
                            buildJsonObject {
                                put("min", span.min)
                                put("max", span.max)
                                put("complete", span.complete)
                            },
                        )
                    }
                },
            )
        }

    /** One band as it is written, or null for an entry too damaged to restore. */
    private fun bandOf(o: JsonObject): SyncCoverage.Band? {
        val spans = runCatching { spansOf(o) }.getOrNull() ?: return null
        return SyncCoverage.Band(
            spans,
            o["fullAt"]?.jsonPrimitive?.longOrNull ?: 0L,
        )
    }

    /**
     * The per-kind spans, or the single pre-split span read as covering every
     * kind under [SyncCoverage.ALL_KINDS].
     *
     * A file written before coverage was tracked per kind carries only
     * `min`/`max` — the wider claim per-kind spans exist to stop. It is loaded
     * as what it always meant rather than discarded, because discarding it
     * would re-walk every upstream's corpus once on upgrade, which is the cost
     * bands exist to avoid. The first paged walk that reports per kind
     * replaces it.
     *
     * Completeness rides one level down for the same reason and reads the same
     * way: a span written before it was per kind carries no `complete` of its
     * own, so it inherits the BAND's — which is exactly what that flag used to
     * mean for every kind at once. Absent there too it is false, which is what
     * a file older than coverage tracking should claim: nothing.
     */
    private fun spansOf(o: JsonObject): Map<Int, SyncCoverage.Span> {
        val bandComplete = o["complete"]?.jsonPrimitive?.booleanOrNull ?: false
        o["spans"]?.jsonObject?.let { spans ->
            return spans.entries.associate { (kind, v) ->
                val span = v.jsonObject
                kind.toInt() to
                    SyncCoverage.Span(
                        span.getValue("min").jsonPrimitive.long,
                        span.getValue("max").jsonPrimitive.long,
                        span["complete"]?.jsonPrimitive?.booleanOrNull ?: bandComplete,
                    )
            }
        }
        return mapOf(
            SyncCoverage.ALL_KINDS to
                SyncCoverage.Span(
                    o.getValue("min").jsonPrimitive.long,
                    o.getValue("max").jsonPrimitive.long,
                    bandComplete,
                ),
        )
    }

    /** Persist via a temp file and an atomic move, so a reader never sees a half-written map. */
    @Synchronized
    private fun save(): Boolean {
        val f = file ?: return true
        return runCatching {
            val snapshot: JsonObject =
                buildJsonObject {
                    streams.forEach { (stream, coverage) ->
                        // The key's own two halves become the two inner levels.
                        val byFilter = LinkedHashMap<String, LinkedHashMap<String, SyncCoverage.Band>>()
                        coverage.export().forEach { (k, band) ->
                            byFilter.getOrPut(k.filter) { LinkedHashMap() }[k.relay] = band
                        }
                        // A stream that has only ASKED holds no bands — its
                        // coverage exists because `legs()` created it — and an
                        // empty group is a stream the card would list as having
                        // walked nothing, which is a fact the file should not
                        // be asserting.
                        if (byFilter.isEmpty()) return@forEach
                        put(
                            stream,
                            buildJsonObject {
                                byFilter.forEach { (filter, byRelay) ->
                                    put(
                                        filter,
                                        buildJsonObject {
                                            byRelay.forEach { (relay, band) -> put(relay, bandOf(band)) }
                                        },
                                    )
                                }
                            },
                        )
                    }
                    // MIGRATION SHIM: unclaimed pre-stream bands go back out
                    // flat and verbatim, because there is still no stream to
                    // file them under and losing one costs a re-walked corpus.
                    // They drain as their streams reach them; goes when the
                    // reader above does.
                    preStream.forEach { (k, raw) -> put(k, raw) }
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

/**
 * Whether a drained leg says anything about HISTORY — the guard every paged call
 * site must put between [PagedFetchResult] and [SyncBands.record].
 *
 * A null [walk] is "this leg was not paged at all" — the negentropy branches pass
 * it, and it settles nothing.
 *
 * `fetchAllPages` reports a drain when the relay EOSEs on an empty page: there is
 * nothing below where this walk stopped. Whether that settles the PAST depends on
 * how deep the leg was allowed to reach, and quartz cannot tell — [SyncBands.record]
 * is handed the STREAM's filter, because that is what the band is keyed by, so the
 * leg's own bounds never reach it.
 *
 * [SyncCoverage.legs] gives the OLDER leg the filter's own `since`, so draining it
 * means the relay holds nothing at all below the filter's floor — exactly the claim
 * that lets the band stop re-asking. The NEWER leg starts at the band's CEILING, so
 * draining that one only means "nothing below the ceiling we already had": true,
 * useless, and actively dangerous if recorded, because the band would then claim a
 * settled past it never walked and skip its history forever.
 *
 * Compared as FLOORS, not for equality, because the two paged call sites build
 * their leg differently and equality silently excluded one of them. [SyncCoverage.legs]
 * hands the older leg the filter's own `since` — null for every stream in
 * `router.conf.example` — but the sweep fallback pages [NegentropyPager]'s
 * `outstanding()`, which materialises that null as [SyncCoverage.PLAUSIBLE_FLOOR].
 * `null == 1577836800L` is false, so an equality test made the drain unreachable on
 * the whole NIP-77-less backfill path while looking correct at both call sites.
 *
 * A null floor reads as "as deep as anything can go" on the leg side, and as the
 * plausible floor on the filter side, which is the deepest a band may ever claim —
 * quartz's own `isPlausible` refuses anything below it, so a leg that reaches it has
 * reached the bottom of what this filter can ever be asked for.
 *
 * KNOWN LIMIT, worth reading before trusting a `since`: for a BOUNDED filter this
 * returning true still does not close the leg. `SyncCoverage.windows` re-opens the
 * older leg whenever `filter.since < span.min` even on a complete band — deliberately,
 * so a caller reaching deeper than the band's floor gets its history back — and it
 * cannot tell that floor apart from one a drain already proved empty. Every stream
 * here is unbounded, so nothing in this deployment hits it; closing it needs `complete`
 * to carry the floor it was earned at, which is an upstream change.
 */
internal fun drainSettlesThePast(
    walk: PagedFetchResult?,
    leg: Filter,
    filter: Filter,
): Boolean {
    if (walk == null || !walk.drained) return false
    val legFloor = leg.since ?: Long.MIN_VALUE
    val filterFloor = filter.since ?: SyncCoverage.PLAUSIBLE_FLOOR
    return legFloor <= filterFloor
}
