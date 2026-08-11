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

    /**
     * stream -> the relays it currently folds away, whose bands are left out of
     * the file. See [dropFolded] for why they go, why they are not merged onto
     * the url that stands in for them, and why each pass REPLACES this rather
     * than adding to it.
     *
     * A filter on the way OUT rather than a deletion, because quartz's
     * [SyncCoverage] has no way to remove a key — `restore` and `record` are the
     * whole write surface — and rebuilding a stream's coverage to drop one
     * would race every leg recording into the object it replaced. Left in
     * memory the band costs a map entry nothing reads: a folded url reaches no
     * ask, so no [legs] or [band] call can find it. What matters is the file,
     * which is what the next boot loads and what `/stats.json` charts — and
     * keeping the band in memory is what lets a url whose verdict expires
     * resume from where it was instead of re-walking.
     *
     * The value is REPLACED, never mutated, so the flusher reading it on
     * another thread sees one pass's answer or the next one's, never half of
     * either.
     */
    private val folded = ConcurrentHashMap<String, Set<String>>()

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
     * Stop keeping band state for urls the alias fold proved are another url's
     * relay. Returns how many of them this call actually took out of the file —
     * the urls this stream was holding a band for, nested or flat — so a caller
     * can log the pass that changed something and stay quiet on the thousands
     * that did not.
     *
     * **What it returns is not "how many are folded", and the difference is the
     * whole of every restart.** The fold is re-applied from the store on the
     * first cycle after boot, so the naive count is the entire verdict set —
     * thousands of urls, on a file the previous process already wrote without
     * them. Reported that way it reads as a mass deletion at every boot, and it
     * marks the map dirty, so the first flush rewrites a multi-megabyte file to
     * produce the bytes it already had. Both are avoided by asking what this
     * stream is actually holding rather than what the verdicts say.
     *
     * **Why the state has to go at all.** A folded url is never dialled by this
     * stream again ([RelayAliases]), so its bands can never advance and nothing
     * will ever ask about them. They are not inert: this file is what
     * `SyncCoverageReport` charts on `/stats.json`, so every one of them is a
     * relay the coverage card keeps listing as walked while no sync exists for
     * it — which is how a fold that is working reads as a fold that never
     * happened. On the production corpus that is 5,514 urls' worth of bands, in
     * a file the relay re-parses on every rollup.
     *
     * **Dropped, not moved onto the canonical.** A band is a claim about a url
     * we walked, and the fold's evidence is a containment measurement over one
     * window ([RelayAliases.sameRelay]) — enough to stop dialling a duplicate,
     * which costs a re-download if it is wrong, and NOT enough to hand the
     * alias's claim to a url whose own legs would then close over ground it was
     * never walked for. What dropping costs is bounded and already being paid:
     * the canonical was being walked in parallel the whole time — that
     * duplication is the thing the fold exists to remove — and anything
     * re-downloaded meets the ingest's dedup.
     *
     * **[urls] REPLACES what this stream held, it does not add to it**, because
     * a fold is not permanent: a verdict carries a TTL and
     * [RelayAliases.forget] drops it when the store stops standing behind it, at
     * which point the url is back in the fan-out and walking again. Accumulating
     * would have kept suppressing the bands it earns after that — dialled every
     * cycle, written to no file, and re-walked from nothing on every restart,
     * with no error anywhere. So the set is whatever the current verdicts say,
     * and a url that comes back writes its bands again on the next flush.
     *
     * **Per stream**, because that is the scope the decision has: the fold is
     * applied to a dynamic stream's discovered set, and another stream dialling
     * the same url keeps its own bands. The name is not the whole of it, though
     * — which is what [keep] is for.
     *
     * **[keep] is the url a stream name cannot protect, and it is why this takes
     * one at all.** `urls` and `relaySource` may name ONE stream — nothing in
     * `RouterConfig` separates them, and `downUpstreams()` hands a configured
     * url to `StaticBackfill` whether or not the same stream also discovers —
     * so a configured upstream that this stream's fan-out folds away would go
     * on being dialled and recorded under this very stream name while every one
     * of those bands was filtered back out of the file. Nothing about that is
     * visible: the relay syncs, the file stays silent, and each restart re-walks
     * its whole corpus. Pass every url something static is subscribed to.
     */
    fun dropFolded(
        stream: String,
        urls: Collection<NormalizedRelayUrl>,
        keep: Set<NormalizedRelayUrl> = emptySet(),
    ): Int {
        val before = folded[stream].orEmpty()
        val now = urls.mapNotNullTo(HashSet()) { if (it in keep) null else it.url }
        if (now.isEmpty() && before.isEmpty()) return 0
        if (now.isEmpty()) folded.remove(stream) else folded[stream] = now
        // Only a url whose verdict CHANGED this pass can change the file: one
        // newly folded hides its bands, one whose verdict expired shows them
        // again. Every steady-state cycle hands over the same set it did last
        // time and stops here, which is what keeps this affordable on a call
        // made once per stream per cycle.
        val hidden = now.filterTo(HashSet()) { it !in before }
        val shown = before.filterTo(HashSet()) { it !in now }
        if (hidden.isEmpty() && shown.isEmpty()) return 0
        // Which of them this stream is actually holding. `export()` copies the
        // map, so it is asked only on a pass that learned something — and
        // asking is the difference between reporting what changed and
        // reporting the whole verdict set back at every boot.
        val held =
            streams[stream]
                ?.export()
                ?.keys
                ?.mapTo(HashSet()) { it.relay }
                .orEmpty()
        val flat = dropPreStream(hidden)
        val gone = hidden.count { it in held || it in flat }
        // A url whose verdict expired is the mirror image: its band is in
        // memory, suppressed until now, and belongs back in the file.
        val back = shown.count { it in held }
        if (gone > 0 || back > 0) dirty = true
        return gone
    }

    /**
     * MIGRATION SHIM — drop the unclaimed flat entries of urls that have just
     * been folded away, and say whose went.
     *
     * This is the one place an unclaimed pre-stream band is deleted rather than
     * written back, against the rule the rest of the shim keeps: an unclaimed
     * entry is normally re-written verbatim, because the stream that wrote it
     * may not have reached that relay yet and losing it costs a corpus. A
     * FOLDED url is the case where no such stream can be coming — nothing dials
     * it, so nothing will ever claim it. The exposure is a static upstream
     * naming a url some dynamic stream folded, which loses at most one
     * unclaimed band and re-walks it. Unlike the filter [dropFolded] applies,
     * this cannot be undone by a verdict expiring; an unclaimed pre-stream band
     * is worth exactly that one re-walk.
     *
     * Guarded by [preStreamRelays] the way [claim] is, and reached only for
     * urls folded on THIS pass: entries are only ever removed here, so a url
     * whose flat bands went last cycle has nothing left to find, and without
     * both guards a migrated deployment would decode every leftover key on
     * every cycle forever.
     */
    private fun dropPreStream(relays: Set<String>): Set<String> {
        if (preStream.isEmpty() || relays.none { it in preStreamRelays }) return emptySet()
        val gone = HashSet<String>()
        // Iterated live: the map is a ConcurrentHashMap, whose iterator
        // tolerates the removals below and [claim] running beside them.
        for (key in preStream.keys) {
            val relay = SyncCoverage.BandKey.decode(key)?.relay ?: continue
            if (relay in relays && preStream.remove(key) != null) gone += relay
        }
        return gone
    }

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
                        // A url this stream folded away is skipped rather than
                        // written: see [dropFolded]. Filtered per entry, so a
                        // filter whose every relay was folded contributes no
                        // empty husk to the file.
                        val skip = folded[stream].orEmpty()
                        coverage.export().forEach { (k, band) ->
                            if (k.relay in skip) return@forEach
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
 * The leg as it goes ON THE WIRE: floored at [SyncCoverage.PLAUSIBLE_FLOOR] when it
 * carries no `since` of its own. Apply this to every filter handed to
 * `fetchAllPages`, and only to those — a paged walk is the one thing the floor
 * protects, and putting it on a negentropy leg would narrow the remote set while the
 * local id snapshot stayed wide, which on the `deleteMissing` path is a retraction.
 *
 * **This is not tidiness. Without it a paged walk against a real relay never ends.**
 * `fetchAllPages` cursors newest-first by `until = oldest created_at seen`, so a
 * single event stamped `created_at = 0` drives the cursor to zero. purplepag.es holds
 * twelve of them — kind 10002, `created_at = 0` — and treats `until <= 0` as *no*
 * `until`, so it answers that page with its five hundred NEWEST events. None of them
 * matches the filter's own `until` client-side, so quartz sees a page that delivered
 * nothing, steps strictly past the boundary to `until = -1`, and asks again. And
 * again. Measured against the live relay: ~5.5 pages a second, 500 events fetched and
 * discarded on each, `until` marching one second further negative every time, EOSE on
 * every single page — for as long as the process runs. The stream never returns, so
 * its band is never recorded and the next boot re-walks the whole relay from the top.
 *
 * Flooring the REQ ends it where the data ends: below the floor purplepag.es answers
 * an empty page with an EOSE, which is a DRAIN, which closes the leg. Every paged
 * call site already spelled this floor out for its progress line
 * (`leg.since ?: PLAUSIBLE_FLOOR`) and [drainSettlesThePast] already accepts a leg
 * that carries it — the walk itself was the one place the floor was assumed and never
 * actually sent.
 *
 * Nothing is given up: [SyncCoverage.isPlausible] already refuses everything below
 * this floor, so those events could never widen a band or count as coverage.
 *
 * **This floor is not the whole fix, and it never was — keep it anyway.** Quartz now
 * guards its own cursor (amethyst#3889, merged as `a5507f9a`): it floors at epoch 0
 * and calls a relay answering ABOVE the boundary `UNPAGEABLE` instead of stepping
 * past it. That is the structural half, because a `since` does NOT bound the step
 * path — `until = boundary - 1` ignores it, so against a cursor-ignoring relay an
 * unguarded walk descends from the window floor to 0 whatever `since` says, ~1.5
 * billion pages. What this floor buys is different and still worth having: the walk
 * stops at real data rather than at a guard, so purplepag.es DRAINS at the floor and
 * the leg closes, where quartz's guard alone would leave it UNPAGEABLE — which
 * settles nothing, records no coverage, and re-walks 1.49M events on the next boot.
 * We are now ON that quartz (pin `a5507f9a4d`), so this is no longer load-bearing
 * against the loop itself. It is load-bearing for the leg CLOSING, which is the
 * thing the `indexers` stream needed.
 *
 * KNOWN HOLE, and why it is left open: a filter carrying its OWN `since` is passed
 * through untouched, so a config that writes `since = 0` — which means the same as
 * omitting it — walks unfloored. On the pinned quartz that no longer runs past zero
 * (the cursor floors there), but it is not harmless: the walk ends UNPAGEABLE
 * against a relay like purplepag.es, which records no coverage, so the leg re-walks
 * every boot. Clamping it HERE is the wrong fix — [drainSettlesThePast] compares the
 * leg's floor against the FILTER's, and a leg clamped above the floor its filter
 * asked for has not reached bottom and may not settle history. The right place is
 * the config loader, normalising `since = 0` to absent before a Filter is built.
 * Not done yet; no configuration here writes it, and a real `since` is always well
 * above this floor, where this function is correctly a no-op.
 */
internal fun Filter.flooredForPaging(): Filter = if (since != null) this else copy(since = SyncCoverage.PLAUSIBLE_FLOOR)

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
