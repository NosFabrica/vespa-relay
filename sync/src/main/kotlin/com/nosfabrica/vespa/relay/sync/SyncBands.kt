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
import com.nosfabrica.vespa.relay.config.SyncTier
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
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
        /**
         * The age band this clock is for, empty for a stream that audits its past as one. A
         * band's window slides with `now`, so the window can never be its identity: a key that
         * moved with it would orphan the record of having walked the band, and the band would
         * read as never verified on the very next visit.
         */
        val band: String = "",
    )

    private val verified = ConcurrentHashMap<VerifiedKey, Long>()

    /**
     * A band negentropy CANNOT verify, whatever the clock says: the relay would not open a
     * NEG-OPEN for this ask. Neither complete nor incomplete — those both describe a walk that
     * happened, and this one cannot. The re-fetch plane covers the band meanwhile.
     */
    data class CannotReconcile(
        /** First windows refused in a row. `UNAVAILABLE` is also a failed dial, so one is not a verdict. */
        val strikes: Int,
        /** When the last was measured, so the verdict lapses and the band is tried again. */
        val at: Long,
        /** What the relay, or the transport, said. */
        val why: String,
    )

    private val cannot = ConcurrentHashMap<VerifiedKey, CannotReconcile>()

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
        band: String = "",
    ): Boolean {
        val dueAt = auditDueAt(stream, url, filter, negentropySyncThePastSeconds, band)
        if (dueAt != null && now < dueAt) return false
        val key = VerifiedKey(stream, filter.toJson(), url.url, band)
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
        band: String = "",
    ): Long? {
        // Only an unbanded stream falls back to the coverage's `fullAt`. A band's coverage is
        // the stream's whole filter, so the first band audited sets a `fullAt` every other band
        // would then inherit — and each would sit out its whole period before its FIRST walk,
        // the tail for a year. A band with no clock of its own has never been verified.
        val own = verified[VerifiedKey(stream, filter.toJson(), url.url, band)]
        val clock = own ?: (if (band.isEmpty()) band(stream, url, filter)?.fullAt else null) ?: 0L
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

    /**
     * One sweep of this band whose first window never reconciled. Returns the strikes so far;
     * the band only reads as impossible once they reach [STRIKES_BEFORE_IMPOSSIBLE].
     */
    fun noteCannotReconcile(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        band: String,
        why: String,
        at: Long = System.currentTimeMillis() / 1000,
    ): Int {
        val key = VerifiedKey(stream, filter.toJson(), url.url, band)
        val after = cannot.compute(key) { _, was -> CannotReconcile((was?.strikes ?: 0) + 1, at, why) }!!
        dirty = true
        return after.strikes
    }

    /** A window reconciled: the band is ordinary work again, whatever the last attempts looked like. */
    fun clearCannotReconcile(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        band: String,
    ) {
        if (cannot.remove(VerifiedKey(stream, filter.toJson(), url.url, band)) != null) dirty = true
    }

    /**
     * Whether negentropy cannot verify this band right now. Lapses on its own, so a relay that
     * gains NIP-77 — or was merely unreachable for an hour — is tried again without an operator.
     */
    fun cannotReconcile(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        band: String = "",
        at: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val state = cannot[VerifiedKey(stream, filter.toJson(), url.url, band)] ?: return false
        if (state.strikes < STRIKES_BEFORE_IMPOSSIBLE) return false
        return at - state.at < CANNOT_RECONCILE_TTL_SECONDS
    }

    /**
     * Every (stream, relay) with at least one band negentropy cannot walk, to what the relay
     * said about it. Built in one pass: a per-row lookup would scan this map once per roster
     * row, and the roster is thousands of rows wide.
     */
    fun cannotReconcileByUnit(at: Long = System.currentTimeMillis() / 1000): Map<Pair<String, String>, String> {
        val out = HashMap<Pair<String, String>, String>()
        cannot.forEach { (key, state) ->
            if (state.strikes < STRIKES_BEFORE_IMPOSSIBLE) return@forEach
            if (at - state.at >= CANNOT_RECONCILE_TTL_SECONDS) return@forEach
            out[key.stream to key.relay] = state.why
        }
        return out
    }

    /** What the relay said, for a band that reads as impossible; null for any other band. */
    fun cannotReconcileWhy(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        band: String = "",
        at: Long = System.currentTimeMillis() / 1000,
    ): String? {
        if (!cannotReconcile(stream, url, filter, band, at)) return null
        return cannot[VerifiedKey(stream, filter.toJson(), url.url, band)]?.why
    }

    /** When this ask's history was last verified by a completed reconcile, or null before its first. */
    fun verifiedAt(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        band: String = "",
    ): Long? = verified[VerifiedKey(stream, filter.toJson(), url.url, band)]

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
        val blind =
            streams.filter { stream ->
                val refetchTiers = stream.refetchSchedule
                stream.negentropySchedule.isEmpty() &&
                    // A banded re-fetch checks the past unless every band of it is NEVER.
                    if (refetchTiers.isEmpty()) {
                        refetchThePastSecondsFor(stream.name) == NEVER
                    } else {
                        refetchTiers.all { it.everySeconds == NEVER }
                    }
            }
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
        /** The age band [reconciledThrough] verified. Coverage is always recorded against the whole [filter]. */
        band: String = "",
    ) {
        coverage(stream).record(url, filter, observedMin, observedMax, paged, reconciledThrough, observedByKind, drained)
        if (reconciledThrough != null) {
            verified[VerifiedKey(stream, filter.toJson(), url.url, band)] = reconciledThrough
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
                if (streamOrFlatKey == BAND_CLOCKS) {
                    o.forEach { (stream, byBand) ->
                        byBand.jsonObject.forEach { (band, byFilter) ->
                            byFilter.jsonObject.forEach { (filter, byRelay) ->
                                byRelay.jsonObject.forEach { (relay, ts) ->
                                    ts.jsonPrimitive.longOrNull?.let { verified[VerifiedKey(stream, filter, relay, band)] = it }
                                }
                            }
                        }
                    }
                    return@forEach
                }
                if (streamOrFlatKey == CANNOT_RECONCILE) {
                    o.forEach { (stream, byBand) ->
                        byBand.jsonObject.forEach { (band, byFilter) ->
                            byFilter.jsonObject.forEach { (filter, byRelay) ->
                                byRelay.jsonObject.forEach { (relay, entry) ->
                                    cannotOf(entry.jsonObject)?.let { cannot[VerifiedKey(stream, filter, relay, band)] = it }
                                }
                            }
                        }
                    }
                    return@forEach
                }
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
            putBandClocks()
            putCannotReconcile()
        }

    /**
     * The tiered audits' clocks, in a section of their own. A band records its coverage against
     * the stream's whole filter — the window slides, and a coverage band per window per day
     * would grow without bound — so there is no per-band band object to hang a `verifiedAt` on
     * the way an untiered stream does. Without this, a band's schedule would be memory-only and
     * every restart would re-walk the oldest, most expensive band.
     */
    private fun JsonObjectBuilder.putBandClocks() {
        val banded = verified.entries.filter { it.key.band.isNotEmpty() }
        if (banded.isEmpty()) return
        put(
            BAND_CLOCKS,
            buildJsonObject {
                banded.groupBy { it.key.stream }.forEach { (stream, ofStream) ->
                    put(
                        stream,
                        buildJsonObject {
                            ofStream.groupBy { it.key.band }.forEach { (band, ofBand) ->
                                put(
                                    band,
                                    buildJsonObject {
                                        ofBand.groupBy { it.key.filter }.forEach { (filter, ofFilter) ->
                                            put(filter, buildJsonObject { ofFilter.forEach { put(it.key.relay, it.value) } })
                                        }
                                    },
                                )
                            }
                        },
                    )
                }
            },
        )
    }

    /** One impossible band, as it is written. */
    private fun cannotOf(c: CannotReconcile): JsonObject =
        buildJsonObject {
            put("strikes", c.strikes)
            put("at", c.at)
            put("why", c.why)
        }

    /** One impossible band read back, or null for an entry too damaged to stand for one. */
    private fun cannotOf(o: JsonObject): CannotReconcile? {
        val strikes = o["strikes"]?.jsonPrimitive?.intOrNull ?: return null
        val at = o["at"]?.jsonPrimitive?.longOrNull ?: return null
        return CannotReconcile(strikes, at, o["why"]?.jsonPrimitive?.contentOrNull ?: "")
    }

    /**
     * The bands negentropy cannot walk, in the same stream/band/filter/relay nesting as the
     * clocks. Written apart from the coverage so a band with no coverage at all — which is
     * exactly what one of these is — still has somewhere to say so.
     */
    private fun JsonObjectBuilder.putCannotReconcile() {
        if (cannot.isEmpty()) return
        put(
            CANNOT_RECONCILE,
            buildJsonObject {
                cannot.entries.groupBy { it.key.stream }.forEach { (stream, ofStream) ->
                    put(
                        stream,
                        buildJsonObject {
                            ofStream.groupBy { it.key.band }.forEach { (band, ofBand) ->
                                put(
                                    band,
                                    buildJsonObject {
                                        ofBand.groupBy { it.key.filter }.forEach { (filter, ofFilter) ->
                                            put(filter, buildJsonObject { ofFilter.forEach { put(it.key.relay, cannotOf(it.value)) } })
                                        }
                                    },
                                )
                            }
                        },
                    )
                }
            },
        )
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

        /**
         * The band-clock section's key in the state file. `#` cannot start a HOCON key that
         * reaches us as a stream name, so it can never collide with one.
         */
        private const val BAND_CLOCKS = "#bandClocks"

        /** Where the impossible bands are written, beside [BAND_CLOCKS] and out of the url namespace. */
        private const val CANNOT_RECONCILE = "#cannotReconcile"

        /**
         * First windows a band must lose in a row before it reads as impossible. More than one
         * because quartz raises the same `UNAVAILABLE` for no NIP-77, a failed dial and a
         * mid-reconcile silence, and cannot tell them apart — a relay without NIP-77 usually
         * just ignores the NEG-OPEN and goes quiet. A first-window failure costs no window work.
         */
        const val STRIKES_BEFORE_IMPOSSIBLE = 3

        /** How long that reading stands before the band is swept again. */
        const val CANNOT_RECONCILE_TTL_SECONDS = 7L * 24 * 60 * 60

        /** `SYNC_STATE_FILE`, unset for in-memory. [streams] are the only source of a re-fetch period. */
        fun fromEnv(
            env: Map<String, String>,
            streams: List<SyncStream> = emptyList(),
        ): SyncBands =
            refuseRemovedEnv(env).let {
                SyncBands(
                    env["SYNC_STATE_FILE"]
                        ?.trim()
                        ?.takeIf { it.isNotEmpty() }
                        ?.let(::File),
                    // One entry per band: a coverage carries its period for the process's life,
                    // so a banded re-fetch needs a coverage — and therefore a key — per band.
                    perStream =
                        streams
                            .flatMap { stream ->
                                val tiers = stream.refetchSchedule
                                tiers.map { tier -> SyncTier.keyFor(stream.name, tiers, tier) to tier.everySeconds }
                            }.toMap(),
                ).also { it.announceUncheckedPasts(streams) }.startPeriodicFlush()
            }

        /** Retired env names, refused by name so an upgrade cannot silently drop a schedule. */
        private fun refuseRemovedEnv(env: Map<String, String>) {
            val set =
                listOf("SYNC_REFETCH_THE_PAST_SECONDS", "SYNC_FULL_RESYNC_SECONDS")
                    .filter { env[it]?.isNotBlank() == true }
            require(set.isEmpty()) {
                "router: ${set.joinToString(", ")} is set — one number used to mean two things and now means " +
                    "neither. Re-walking a relay's whole history is per STREAM (`refetchThePastSeconds` in " +
                    "sync.conf, unset meaning never, because one period cannot be right for a 130-kind content " +
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
