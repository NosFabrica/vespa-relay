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

import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.config.syncEnv
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
 * The router's sync bands: file persistence around quartz's [SyncCoverage], one coverage per
 * stream so two streams asking one relay the same filter never resume from each other's claim.
 * The file nests stream, filter, relay; a null [file] is the in-memory mode.
 */
class SyncBands(
    private val file: File?,
    /** The re-fetch period for streams that name none; a seam for tests. */
    internal val refetchThePastSeconds: Long = NEVER,
    /** Per-stream re-fetch periods. Fixed at construction: a coverage carries its period for the process's life. */
    private val perStream: Map<String, Long> = emptyMap(),
) : AutoCloseable {
    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    private val coverageByStream = ConcurrentHashMap<String, SyncCoverage>()

    /**
     * stream -> the relays it currently folds away, left out of the file but kept in memory so
     * an expired verdict resumes where it was. Replaced whole, never mutated, for the flusher.
     */
    private val folded = ConcurrentHashMap<String, Set<String>>()

    /**
     * The audit's own clock, advanced on every `reconciledThrough` record; quartz's `fullAt`
     * is kept across merges and only serves before the first one.
     */
    private data class VerifiedKey(
        val stream: String,
        val filter: String,
        val relay: String,
    )

    private val verified = ConcurrentHashMap<VerifiedKey, Long>()

    /** When each ask's audit was last claimed, complete or not. In memory only. */
    private val attempts = ConcurrentHashMap<VerifiedKey, Long>()

    /**
     * The audit gate: due by [auditDueAt] and outside the attempt spacing. True claims the
     * attempt, so an audit that cannot complete is not retried on every visit.
     */
    fun claimAudit(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        negentropySyncThePastSeconds: Long,
        now: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val dueAt = auditDueAt(stream, url, filter, negentropySyncThePastSeconds)
        if (dueAt != null && now < dueAt) return false
        val key = VerifiedKey(stream, filter.toJson(), url.url)
        if (now - (attempts[key] ?: 0L) < attemptSpacingSeconds(negentropySyncThePastSeconds)) return false
        attempts[key] = now
        return true
    }

    /**
     * When this ask's audit comes due, the one arithmetic [claimAudit] gates on and the status
     * page certifies by. Null is never audited, which is always due. Stamps nothing.
     */
    fun auditDueAt(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        negentropySyncThePastSeconds: Long,
    ): Long? {
        val clock = verified[VerifiedKey(stream, filter.toJson(), url.url)] ?: band(stream, url, filter)?.fullAt ?: 0L
        return if (clock <= 0L) null else clock + negentropySyncThePastSeconds
    }

    /** When the band expires onto the re-fetch. Null before a first full pass or where the stream sets no period. */
    fun refetchDueAt(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Long? {
        val period = refetchThePastSecondsFor(stream)
        if (period == NEVER) return null
        val fullAt = band(stream, url, filter)?.fullAt ?: 0L
        return if (fullAt <= 0L) null else fullAt + period
    }

    /** When this ask's history was last verified by a completed reconcile, or null before its first. */
    fun verifiedAt(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Long? = verified[VerifiedKey(stream, filter.toJson(), url.url)]

    init {
        val pruned = load()
        // Reopening a file is not a change; a prune is, and only a write takes the keys off disk.
        dirty = pruned > 0
    }

    /** The bands of one stream, created on first use. */
    private fun coverage(stream: String): SyncCoverage =
        coverageByStream.computeIfAbsent(stream) {
            SyncCoverage(refetchThePastSecondsFor(stream), onChange = { dirty = true })
        }

    /** What [stream]'s bands are trusted for: its own period, else the router's. */
    internal fun refetchThePastSecondsFor(stream: String): Long = perStream[stream] ?: refetchThePastSeconds

    /**
     * Names the streams with neither a negentropy audit nor a re-fetch period at boot. A
     * forward-only mirror is legitimate; being in one by accident is not.
     */
    private fun announceUncheckedPasts(streams: List<SyncStream>) {
        val blind = streams.filter { it.negentropySyncThePastSeconds == null && refetchThePastSecondsFor(it.name) == NEVER }
        if (blind.isEmpty()) return
        System.err.println(
            "router: stream(s) ${blind.joinToString(", ") { it.name }} have neither `negentropySyncThePastSeconds` nor " +
                "`refetchThePastSeconds` — they page forward only, and nothing will re-read the history they " +
                "have already walked. Set one if a relay of theirs can back-fill",
        )
    }

    // ---- the band arithmetic, upstream's ------------------------------------

    fun legs(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> = coverage(stream).legs(url, filter)

    fun record(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        observedMin: Long?,
        observedMax: Long?,
        paged: Boolean,
        reconciledThrough: Long? = null,
        /** Required for a multi-kind filter on the paged path; quartz records no band without it. */
        observedByKind: Map<Int, SyncCoverage.Span>? = null,
        /** The relay EOSEd on an empty page. Gate every call site on [drainSettlesThePast]. */
        drained: Boolean = false,
    ) {
        coverage(stream).record(url, filter, observedMin, observedMax, paged, reconciledThrough, observedByKind, drained)
        if (reconciledThrough != null) {
            verified[VerifiedKey(stream, filter.toJson(), url.url)] = reconciledThrough
            dirty = true
        }
    }

    fun coveringWindow(
        stream: String,
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Filter = coverage(stream).coveringWindow(urls, filter)

    /** Whether any of [urls] still has work outside its band. */
    fun anyOutstanding(
        stream: String,
        urls: List<NormalizedRelayUrl>,
        filter: Filter,
    ): Boolean = urls.any { legs(stream, it, filter).isNotEmpty() }

    fun band(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): SyncCoverage.Band? = coverage(stream).band(url, filter)

    fun size(): Int = coverageByStream.values.sumOf { it.size() }

    /**
     * Hides the bands of urls the alias fold proved are another relay's from the file. [urls]
     * replaces the stream's folded set, since a verdict expires; [keep] protects configured
     * upstreams. Returns how many held bands this call hid.
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
        // Only a url whose verdict changed this pass can change the file.
        val hidden = now.filterTo(HashSet()) { it !in before }
        val shown = before.filterTo(HashSet()) { it !in now }
        if (hidden.isEmpty() && shown.isEmpty()) return 0
        val held =
            coverageByStream[stream]
                ?.export()
                ?.keys
                ?.mapTo(HashSet()) { it.relay }
                .orEmpty()
        val gone = hidden.count { it in held }
        val back = shown.count { it in held }
        if (gone > 0 || back > 0) dirty = true
        return gone
    }

    // ---- the file ------------------------------------------------------------

    /** Writes the map every [intervalSec] on a daemon thread. Unchanged intervals write nothing. */
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
        // A failed write re-arms the flag, so the next tick retries it.
        if (!save()) dirty = true
    }

    /**
     * Reads the file and prunes the flat pre-stream keys, returning how many were dropped. A
     * flat key names no stream, so nothing can ever claim it.
     */
    private fun load(): Int {
        val f = file ?: return 0
        if (!f.isFile) return 0
        var pruned = 0
        runCatching {
            val root = Json.parseToJsonElement(f.readText()).jsonObject
            root.forEach { (streamOrFlatKey, v) ->
                val o = v.jsonObject
                // Told apart by shape: a filter is serialised JSON and can never be named `min`.
                if (o["min"] != null) {
                    pruned++
                    return@forEach
                }
                val restored = LinkedHashMap<SyncCoverage.BandKey, SyncCoverage.Band>()
                o.forEach { (filter, byRelay) ->
                    byRelay.jsonObject.forEach { (relay, band) ->
                        bandOf(band.jsonObject)?.let { restored[SyncCoverage.BandKey(relay, filter)] = it }
                        band.jsonObject["verifiedAt"]?.jsonPrimitive?.longOrNull?.let {
                            verified[VerifiedKey(streamOrFlatKey, filter, relay)] = it
                        }
                    }
                }
                if (restored.isNotEmpty()) coverage(streamOrFlatKey).restore(restored)
            }
        }.onFailure {
            // A corrupt cursor file costs one re-sync; exiting costs the mirror.
            System.err.println("router: could not read sync bands from ${f.path} (${it.message}); starting fresh")
            // A count from a parse that stopped is not a fact about the file.
            pruned = 0
        }
        if (pruned > 0) {
            System.err.println("router: dropped $pruned pre-stream band(s) from ${f.path} — flat keys name no stream, so nothing can ever claim them")
        }
        return pruned
    }

    /** One band, as it is written. */
    private fun bandOf(
        band: SyncCoverage.Band,
        verifiedAt: Long?,
    ): JsonObject =
        buildJsonObject {
            // The outer edges across every kind, so a build without per-kind spans still parses it.
            put("min", band.minCreatedAt)
            put("max", band.maxCreatedAt)
            put("complete", band.complete)
            put("fullAt", band.fullAt)
            verifiedAt?.let { put("verifiedAt", it) }
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
     * The per-kind spans. A band written before spans existed is read as one span over
     * [SyncCoverage.ALL_KINDS], and a span without `complete` inherits the band's.
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

    /** Every band this router holds, in the one shape [save] writes and the status page reads. */
    @Synchronized
    internal fun snapshot(): JsonObject =
        buildJsonObject {
            coverageByStream.forEach { (stream, coverage) ->
                val byFilter = LinkedHashMap<String, LinkedHashMap<String, SyncCoverage.Band>>()
                // Folded urls are skipped per entry, so a filter with every relay folded leaves no empty husk.
                val skip = folded[stream].orEmpty()
                coverage.export().forEach { (k, band) ->
                    if (k.relay in skip) return@forEach
                    byFilter.getOrPut(k.filter) { LinkedHashMap() }[k.relay] = band
                }
                // A stream that has only asked holds no bands and gets no group.
                if (byFilter.isEmpty()) return@forEach
                put(
                    stream,
                    buildJsonObject {
                        byFilter.forEach { (filter, byRelay) ->
                            put(
                                filter,
                                buildJsonObject {
                                    byRelay.forEach { (relay, band) ->
                                        put(relay, bandOf(band, verified[VerifiedKey(stream, filter, relay)]))
                                    }
                                },
                            )
                        }
                    },
                )
            }
        }

    /** Persist via a temp file and an atomic move, so a reader never sees a half-written map. */
    @Synchronized
    private fun save(): Boolean {
        val f = file ?: return true
        return runCatching {
            val snapshot: JsonObject = snapshot()
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), snapshot))
            // ATOMIC_MOVE asked for explicitly; without it the JVM may fall back to copy+delete.
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
        // Pretty-printed for a human reader.
        private val json = Json { prettyPrint = true }

        /** The dueness rule as a predicate, for tests. A clock of zero is always due. */
        internal fun auditDue(
            fullAt: Long,
            now: Long,
            negentropySyncThePastSeconds: Long,
        ): Boolean = fullAt <= 0L || now - fullAt >= negentropySyncThePastSeconds

        /**
         * How long after an audit ran before the same ask may try again, whatever the outcome.
         * Floored above the revisit floor, capped so a weekly audit still retries within a shift.
         */
        internal fun attemptSpacingSeconds(negentropySyncThePastSeconds: Long): Long = (negentropySyncThePastSeconds / 4).coerceIn(900L, 21_600L)

        private const val DEFAULT_FLUSH_SECONDS = 30L

        /** No period, as a number quartz's `isStale` can hold. Zero would mean always stale. */
        internal const val NEVER = Long.MAX_VALUE

        /** `SYNC_STATE_FILE`, unset for in-memory. [streams] are the only source of a re-fetch period. */
        fun fromEnv(
            env: Map<String, String>,
            streams: List<SyncStream> = emptyList(),
        ): SyncBands =
            refuseRemovedEnv(env).let {
                SyncBands(
                    env
                        .syncEnv("SYNC_STATE_FILE", "ROUTER_SYNC_STATE_FILE")
                        ?.trim()
                        ?.takeIf { it.isNotEmpty() }
                        ?.let(::File),
                    perStream = streams.mapNotNull { stream -> stream.refetchThePastSeconds?.let { stream.name to it } }.toMap(),
                ).also { it.announceUncheckedPasts(streams) }.startPeriodicFlush()
            }

        /** Retired env names, refused by name so an upgrade cannot silently drop a schedule. */
        private fun refuseRemovedEnv(env: Map<String, String>) {
            val set =
                listOf("SYNC_REFETCH_THE_PAST_SECONDS", "SYNC_FULL_RESYNC_SECONDS", "ROUTER_FULL_RESYNC_SECONDS")
                    .filter { env[it]?.isNotBlank() == true }
            require(set.isEmpty()) {
                "router: ${set.joinToString(", ")} is set — one number used to mean two things and now means " +
                    "neither. Re-walking a relay's whole history is per STREAM (`refetchThePastSeconds` in " +
                    "router.conf, unset meaning never, because one period cannot be right for a 130-kind content " +
                    "mirror and a five-relay bootstrap at once); how old an INTERRUPTED sweep's cursor may be and " +
                    "still resume is `SYNC_SWEEP_CURSOR_STALE_AFTER_SECONDS`. Set whichever you meant and unset this"
            }
        }
    }
}

/**
 * The leg as it goes on the wire, floored at [SyncCoverage.PLAUSIBLE_FLOOR] when it carries no
 * `since`. Apply to every filter handed to `fetchAllPages` and only those: on a negentropy leg
 * it would narrow the remote set while the local snapshot stayed wide.
 */
internal fun Filter.flooredForPaging(): Filter = if (since != null) this else copy(since = SyncCoverage.PLAUSIBLE_FLOOR)

/**
 * Whether a drained leg says anything about history: the guard between [PagedFetchResult] and
 * [SyncBands.record]. Only the older leg, which reaches the filter's own floor, settles the past.
 * Compared as floors, not for equality: the sweep fallback materialises a null `since`.
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
