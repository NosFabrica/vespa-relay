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
 * `SYNC_STATE_FILE` keys them has changed since, and the flat keys a file
 * written before that carries are PRUNED on load (see [load]).
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
    /**
     * The period for streams that name none — [NEVER] for the router, which
     * has no other source: `fromEnv` passes nothing here, and the only
     * production answer comes from [perStream]. A seam for tests and
     * embedders, kept because forcing staleness is otherwise unexpressible.
     */
    internal val refetchThePastSeconds: Long = NEVER,
    /**
     * Streams whose re-walk runs on a period of their own, by name. Fixed at
     * construction rather than registered later: a [SyncCoverage] is built on
     * a stream's first band and carries its period for the life of the
     * process, so a value learned after that would be silently ignored — and
     * the symptom, a stream re-walking on the wrong clock, is invisible for a
     * week.
     */
    private val perStream: Map<String, Long> = emptyMap(),
) : AutoCloseable {
    @Volatile private var dirty = false

    @Volatile private var flusher: Thread? = null

    private val coverageByStream = ConcurrentHashMap<String, SyncCoverage>()

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

    /**
     * THE AUDIT'S OWN CLOCK, because quartz's `fullAt` is not it.
     *
     * `Band.widen` keeps the OLD `fullAt` on every non-stale merge — upstream
     * defines it as "when the last pass that started from nothing finished",
     * and only a stale replace (the 7-day full resync) restarts it. Read as
     * "last verified", it freezes: the moment a band aged past
     * `negentropySyncThePastSeconds`, every visit's audit was due again, and one relay was
     * measured taking 13 full history sweeps in 40 minutes. So the router
     * keeps its own stamp, advanced by every `reconciledThrough` record and
     * persisted beside the band it belongs to. Callers fall back to `fullAt`
     * when no stamp exists yet — a fresh band's paged full walk still defers
     * the first audit one `negentropySyncThePastSeconds`, exactly as before.
     */
    private data class VerifiedKey(
        val stream: String,
        val filter: String,
        val relay: String,
    )

    private val verified = ConcurrentHashMap<VerifiedKey, Long>()

    /**
     * When each ask's audit was last CLAIMED, complete or not — the spacing
     * half of [claimAudit]. In-memory on purpose: a restart retrying once is
     * fine, a revisit-floor retry loop is not.
     */
    private val attempts = ConcurrentHashMap<VerifiedKey, Long>()

    /**
     * THE AUDIT GATE, both halves in one place: is this ask's history due —
     * the [verifiedAt] clock aged past [negentropySyncThePastSeconds], falling back to the
     * band's `fullAt` so a fresh catch-up still defers the first audit — and
     * is the ask outside its attempt spacing? TRUE CLAIMS THE ATTEMPT: the
     * caller is expected to run the audit, and an audit that cannot complete
     * (negentropy refused, sweep interrupted) advances no clock, so the
     * claim itself is what stands between that and a retry on every visit.
     * The clock chain and the spacing map each used to be spelled twice —
     * once per audit path — which is exactly how they would have drifted.
     */
    fun claimAudit(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
        negentropySyncThePastSeconds: Long,
        now: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val key = VerifiedKey(stream, filter.toJson(), url.url)
        val clock = verified[key] ?: band(stream, url, filter)?.fullAt ?: 0L
        if (!auditDue(clock, now, negentropySyncThePastSeconds)) return false
        if (now - (attempts[key] ?: 0L) < attemptSpacingSeconds(negentropySyncThePastSeconds)) return false
        attempts[key] = now
        return true
    }

    /**
     * WHEN this ask's negentropy audit comes due — the same arithmetic
     * [claimAudit] gates on, exposed as a TIME rather than a yes/no.
     *
     * Null means never audited, which [auditDue] treats as always due: a
     * relay's first audit happens on its first visit rather than a period
     * later. That answer is the one worth publishing separately, because it is
     * the only way a fresh deployment's audit storm reads as scheduled work
     * instead of a rule being broken — every ask is due at once, exactly as
     * designed, and it never happens again for the same ask.
     *
     * Read-only and stamps nothing. [claimAudit] takes the attempt clock as a
     * side effect of returning true, so asking IT what is due would push every
     * ask it was asked about hours into the future.
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

    /**
     * …and when its bands expire onto the re-fetch, on the stream's
     * `refetchThePastSeconds`. Null where the band has never completed a full
     * pass — there is nothing recorded to re-fetch, so the walk it gets is a
     * first catch-up and not a re-walk — or where the stream sets no period,
     * which is a stream whose history is never re-fetched at all.
     */
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

    /** When this ask's history was last VERIFIED by a completed reconcile, or null before its first. */
    fun verifiedAt(
        stream: String,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Long? = verified[VerifiedKey(stream, filter.toJson(), url.url)]

    init {
        val pruned = load()
        // restore() does not fire onChange, but stay defensive: reopening a
        // file must never count as a change, or every boot rewrites it. A
        // PRUNE is the one thing a load can change: the file on disk still
        // carries keys this build refuses to hold, and only a write takes them
        // out of it.
        dirty = pruned > 0
    }

    /**
     * The bands of one stream. Created on first use: a stream that never syncs
     * costs nothing, and the engine does not announce its stream list here.
     */
    private fun coverage(stream: String): SyncCoverage =
        coverageByStream.computeIfAbsent(stream) {
            SyncCoverage(refetchThePastSecondsFor(stream), onChange = { dirty = true })
        }

    /** What [stream]'s bands are trusted for: its own period, else the router's. */
    internal fun refetchThePastSecondsFor(stream: String): Long = perStream[stream] ?: refetchThePastSeconds

    /**
     * Say WHICH streams have no way back into their own past, at boot.
     *
     * A stream re-reads history two ways: [SyncStream.negentropySyncThePastSeconds] reconciles
     * the covered past and downloads the difference, and
     * [SyncStream.refetchThePastSeconds] expires the band so the past is walked
     * again. With neither, a walk that missed a window — a relay that
     * back-filled after we passed it, a leg that recorded a band on a page it
     * should not have — is never revisited, and the band file says nothing
     * about it because a band only ever widens.
     *
     * Printed rather than refused: a mirror that only ever moves forward is a
     * legitimate deployment, and the router's job is to make sure nobody is in
     * one by accident.
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
    // Delegated rather than exposing `coverage` directly: these five calls are
    // the entire surface the router uses, and naming them keeps that visible.

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
    ): SyncCoverage.Band? = coverage(stream).band(url, filter)

    fun size(): Int = coverageByStream.values.sumOf { it.size() }

    /**
     * Stop keeping band state for urls the alias fold proved are another url's
     * relay. Returns how many of them this call actually took out of the file —
     * the urls this stream was holding a band for — so a caller can log the
     * pass that changed something and stay quiet on the thousands that did not.
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
            coverageByStream[stream]
                ?.export()
                ?.keys
                ?.mapTo(HashSet()) { it.relay }
                .orEmpty()
        val gone = hidden.count { it in held }
        // A url whose verdict expired is the mirror image: its band is in
        // memory, suppressed until now, and belongs back in the file.
        val back = shown.count { it in held }
        if (gone > 0 || back > 0) dirty = true
        return gone
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

    /**
     * Read the file, and PRUNE the flat keys left by a build that wrote bands
     * before the format nested. Returns how many were dropped, so the first
     * flush is what finally takes them off disk.
     *
     * They used to be held aside and handed to the first stream that asked
     * about that (relay, filter) — the stream that wrote them. That shim could
     * never finish draining, because a claim needs a live stream to ask and the
     * keys still there are precisely the ones no stream asks about. Measured on
     * staging four days after the format nested: 2,624 of 2,628 top-level keys
     * were flat — 2.5MB of a 13.8MB file — and the newest `max` across all of
     * them was 90 minutes after that build landed, so not one had been written
     * to since. Every one was a subpath alias `RelayAliases` had folded out of the
     * fan-out (`wss://espelho.girino.org/dynamo-yankee`), so nothing dialled it
     * and nothing could claim it: of the 1,578 flat urls under one filter, ZERO
     * appeared among the 6,578 relays that stream walked in the cycle running
     * at the time, against a control of 2,858 nested urls from the same file
     * that all did. Meanwhile `SyncCoverageReport` charted them as three
     * unnamed groups, `reconciled=0` and frozen, which a reader cannot tell
     * apart from streams failing to reconcile.
     *
     * The prune costs one band for any flat key a stream WOULD still have
     * claimed, which is one re-walk of that pair — bounded, one-time, and on
     * staging zero, since every one of them is a folded alias of a host that is
     * walked anyway and returns byte-identical data. This is the deletion the
     * shim's own exit condition sanctioned: every deployment has long since
     * booted on a build that writes the nested shape.
     *
     * [dropFolded] keeps folded bands out of the file, so nothing writes a flat
     * key again once these are gone.
     */
    private fun load(): Int {
        val f = file ?: return 0
        if (!f.isFile) return 0
        var pruned = 0
        runCatching {
            val root = Json.parseToJsonElement(f.readText()).jsonObject
            root.forEach { (streamOrFlatKey, v) ->
                val o = v.jsonObject
                // Told apart by SHAPE, not by the key: a pre-stream entry is
                // the band itself, a stream is filters all the way down. A
                // filter can never be named `min` — it is serialised JSON and
                // starts with `{`.
                if (o["min"] != null) {
                    pruned++
                    return@forEach
                }
                val restored = LinkedHashMap<SyncCoverage.BandKey, SyncCoverage.Band>()
                o.forEach { (filter, byRelay) ->
                    byRelay.jsonObject.forEach { (relay, band) ->
                        // Straight back into the pair, which is what the two
                        // inner levels have always been.
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
            // The count describes a parse that stopped somewhere, so it is not
            // a fact about the file and must not be reported as one. This does
            // NOT save the damaged file: the first band recorded marks the map
            // dirty and [save] rewrites it from the partial read anyway, flat
            // keys and all. What it buys is that the BOOT is not the thing that
            // does it, so a router that reads a damaged file and then records
            // nothing leaves it there to be looked at.
            pruned = 0
        }
        // Said once, at the boot that does it, because it is a deletion: the
        // next flush is where these stop existing, and a silent one is a
        // coverage card that loses thousands of rows with nothing to point at.
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
            // min/max are the outer edges across every kind, written for two
            // readers: a human debugging why a relay re-synced, and a ROLLBACK
            // — a build from before per-kind spans reads these and behaves as
            // it always did rather than failing to parse.
            put("min", band.minCreatedAt)
            put("max", band.maxCreatedAt)
            put("complete", band.complete)
            put("fullAt", band.fullAt)
            // The router's own audit clock, absent until the ask's first
            // completed reconcile — see [verified]. An old build ignores it.
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

    /**
     * Every band this router holds, in the shape [save] writes and the status
     * page reads.
     *
     * Extracted from [save] rather than duplicated for the page: the status
     * site renders this state in the SAME process that holds it, so a second
     * construction here would be a second format that could drift from the one
     * on disk — and the file is what a restart reloads.
     */
    @Synchronized
    internal fun snapshot(): JsonObject =
        buildJsonObject {
            coverageByStream.forEach { (stream, coverage) ->
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

        /**
         * Is the ask's history due its audit? A clock of zero is an ask that
         * has NEVER had a verified pass — always due, which is what makes a
         * fresh relay's first audit happen on its first visit rather than a
         * week later.
         */
        internal fun auditDue(
            fullAt: Long,
            now: Long,
            negentropySyncThePastSeconds: Long,
        ): Boolean = fullAt <= 0L || now - fullAt >= negentropySyncThePastSeconds

        /**
         * How long after an audit RAN before the same ask may try again,
         * whatever the outcome — the backstop for audits that cannot
         * complete and so never advance the verified-at clock. A quarter of
         * the knob, floored so a flaky relay is not re-swept on the revisit
         * floor and capped so a weekly audit still retries within the shift
         * an operator is watching.
         */
        internal fun attemptSpacingSeconds(negentropySyncThePastSeconds: Long): Long = (negentropySyncThePastSeconds / 4).coerceIn(900L, 21_600L)

        // Often enough that a kill costs little, rare enough to be free.
        private const val DEFAULT_FLUSH_SECONDS = 30L

        /**
         * No period at all, spelled as one quartz can hold: `isStale` is
         * `now - fullAt >= period`, so a period no clock reaches is a band that
         * is trusted for as long as the process lives. Not zero — that is
         * "always stale", the opposite — and not a magic null, because
         * [SyncCoverage] takes a number.
         */
        internal const val NEVER = Long.MAX_VALUE

        /**
         * `SYNC_STATE_FILE` — where the bands live. Unset keeps them in memory,
         * which is the same as not having them.
         *
         * [streams] are the ONLY source of a re-walk period, which is why this
         * is built after the config is parsed. There is no environment default
         * and no built-in one: re-reading a relay's whole history is the most
         * expensive thing this router does on a schedule — the content mirror
         * is ~130 kinds against every certified relay — and it was running on
         * quartz's week in every deployment that had never heard of the knob,
         * beside reconciles that already covered the same ground for the
         * difference alone.
         *
         * A schedule that costs that much belongs to the stream that pays it,
         * written in the same file as the filter it re-reads. One number for
         * every stream could only ever be wrong for most of them: the content
         * mirror and a five-relay bootstrap do not want the same period, and
         * the streams that reconcile want none at all.
         *
         * The env names it used to answer to are REFUSED rather than ignored —
         * see [refuseRemovedEnv] — and the streams left with no re-check of
         * any kind are named at boot rather than left to be inferred.
         */
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

        /**
         * The three env names that used to carry a re-walk period, refused by
         * name rather than ignored.
         *
         * Ignoring one would take a deployment's schedule away on an upgrade
         * and say nothing — the exact failure the rename machinery exists to
         * prevent, arriving through the other door. This is the same posture
         * `recycleSeconds` and the per-stream `concurrency` got when the cycle
         * engine went: a setting that no longer has a meaning is an error, and
         * the message says where the meaning moved.
         */
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
