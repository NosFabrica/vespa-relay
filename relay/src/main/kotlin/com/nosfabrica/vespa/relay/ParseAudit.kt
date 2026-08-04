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
package com.nosfabrica.vespa.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import com.vitorpamplona.quartz.utils.LogSink
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Collects every event quartz cannot fully parse, with the raw event JSON, so the
 * gaps can be fixed in quartz instead of silently degrading this relay's search
 * index.
 *
 * ## Why this exists
 *
 * [SearchableEvent.indexableContent] is what turns an event into NIP-50 search
 * text, and for kind 0 it runs the whole `content` through quartz's `UserMetadata`
 * deserializer. Mirroring profiles from a dozen upstream relays therefore replays
 * every malformed kind 0 ever published through that deserializer, and quartz
 * reports what it could not read through [Log]:
 *
 *     [MetadataEvent] Content Parse Error: nostr:naddr1… Expected start of the object '{', …
 *     [TolerantStringSerializer] Ignoring non-primitive string field (JsonObject)
 *
 * Those two lines are very different in severity. The `MetadataEvent` one means the
 * content was not a JSON object at all, so there is no metadata to index and that
 * profile is not findable by name. The tolerant-serializer ones mean the parse
 * *succeeded* and one field of the wrong JSON type was skipped by design.
 *
 * ## Why a log sink alone is not enough
 *
 * [LogSink] receives only `(level, tag, message, throwable)` — no event. The
 * `MetadataEvent` message embeds the event's own `nostr:naddr1…` URI, so those are
 * self-identifying, but the tolerant-serializer messages carry no identifier at
 * all: on their own they say a field somewhere in some profile had the wrong type.
 *
 * So [inspect] parses each event *itself*, on a thread it controls, with the event
 * parked in a [ThreadLocal]. Everything quartz logs during that call is therefore
 * attributable to exactly one event, and the report can carry the raw JSON needed
 * to write a quartz regression test.
 *
 * The cost is one extra parse per inspected event, which is why this is opt-in
 * (`PARSE_AUDIT_FILE`). It is a diagnostic run, not a production default.
 *
 * ## What lands in the report
 *
 * Findings are grouped by signature — the tag plus the message with event URIs,
 * hex ids and offsets stripped — so "the same quartz gap" collapses to one entry
 * with a count, however many events hit it. Each keeps up to
 * `PARSE_AUDIT_SAMPLES` raw events.
 */
class ParseAudit(
    private val outFile: File,
    private val maxSamplesPerFinding: Int = 5,
    private val delegate: LogSink = Log.sink,
) : LogSink,
    AutoCloseable {
    /**
     * The event whose parse this thread is currently inside, so [log] can attribute
     * what quartz reports, plus whether anything was reported for it. The flag has
     * to live here rather than being inferred from the findings map: a second event
     * hitting an *already-known* defect adds no map entry, so comparing map sizes
     * across the parse counts only the events that introduced a new signature.
     */
    private class InFlight(
        val event: Event,
    ) {
        var reported = false
    }

    private val inFlight = ThreadLocal<InFlight?>()

    private val findings = ConcurrentHashMap<String, Finding>()

    // Parse reports that arrived outside [inspect] — the store's own indexing pass
    // on its Vespa feed threads, for instance. Counted, not attributed: there is no
    // event to tie them to. A high number here means inspection is missing a path.
    private val unattributed = ConcurrentHashMap<String, AtomicLong>()

    private val inspected = AtomicLong()
    private val withFindings = AtomicLong()

    @Volatile private var flusher: Thread? = null

    /**
     * Run the event through quartz's search-indexing parse and record whatever it
     * reports. Only [SearchableEvent]s are inspected — nothing else reaches a
     * content deserializer on the ingest path.
     *
     * Never throws: an audit must not be able to reject an event the relay would
     * otherwise have stored.
     */
    fun inspect(event: Event) {
        if (event !is SearchableEvent) return
        inspected.incrementAndGet()
        val ctx = InFlight(event)
        inFlight.set(ctx)
        try {
            event.indexableContent()
        } catch (e: Exception) {
            // A parse that throws rather than logging is itself a finding — quartz
            // is expected to degrade gracefully here, not propagate.
            record(ctx, "thrown:${e.javaClass.simpleName}", LogLevel.ERROR, e.message ?: "")
        } finally {
            inFlight.remove()
        }
        if (ctx.reported) withFindings.incrementAndGet()
    }

    override fun log(
        level: LogLevel,
        tag: String,
        message: String,
        throwable: Throwable?,
    ) {
        val ctx = inFlight.get()
        if (ctx != null) {
            record(ctx, tag, level, message)
            // Swallowed: the report is the output, and one line per malformed
            // profile across a multi-year backfill is what buried the real logs.
            return
        }
        if (isParseReport(tag)) {
            unattributed.computeIfAbsent(tag) { AtomicLong() }.incrementAndGet()
            return
        }
        delegate.log(level, tag, message, throwable)
    }

    private fun record(
        ctx: InFlight,
        tag: String,
        level: LogLevel,
        message: String,
    ) {
        ctx.reported = true
        val finding = findings.computeIfAbsent(signature(tag, message)) { Finding(tag, level, normalize(message)) }
        finding.hit(message, ctx.event, maxSamplesPerFinding)
    }

    /** Everything seen so far, most frequent first. */
    fun snapshot(): List<Finding> = findings.values.sortedByDescending { it.count.get() }

    /**
     * Total reports recorded. Higher than [withFindings] whenever one event trips
     * more than one defect — a profile with two wrongly-typed fields reports twice.
     */
    private fun totalReports(): Long = findings.values.sumOf { it.count.get() }

    /** A one-line summary for the periodic progress log. */
    fun summary(): String =
        "parse audit: ${findings.size} distinct issue(s), ${totalReports()} report(s) over " +
            "${withFindings.get()} affected of ${inspected.get()} event(s)" +
            unattributed.entries.sumOf { it.value.get() }.let { if (it > 0) ", $it unattributed" else "" }

    /**
     * Write the report. Goes to a sibling temp file and is then moved over the
     * target, so a reader (or a `docker cp` mid-backfill) never sees a half file.
     */
    fun writeReport() {
        val report =
            buildJsonObject {
                put("inspected", JsonPrimitive(inspected.get()))
                // Events that reported at least once, and the report total across
                // them — one event can trip more than one defect.
                put("eventsWithFindings", JsonPrimitive(withFindings.get()))
                put("totalReports", JsonPrimitive(totalReports()))
                put("distinctFindings", JsonPrimitive(findings.size))
                put(
                    "unattributedByTag",
                    buildJsonObject {
                        unattributed.entries.sortedBy { it.key }.forEach { put(it.key, JsonPrimitive(it.value.get())) }
                    },
                )
                put("findings", JsonArray(snapshot().map { it.toJson() }))
            }
        val tmp = File(outFile.parentFile ?: File("."), "${outFile.name}.tmp")
        tmp.parentFile?.mkdirs()
        tmp.writeText(json.encodeToString(JsonObject.serializer(), report))
        if (!tmp.renameTo(outFile)) {
            tmp.copyTo(outFile, overwrite = true)
            tmp.delete()
        }
    }

    /**
     * Rewrite the report every [intervalSec] on a daemon thread, so a long backfill
     * can be read while it runs rather than only after shutdown.
     */
    fun startPeriodicFlush(intervalSec: Long): ParseAudit {
        flusher =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(intervalSec * 1000)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                    runCatching { writeReport() }
                    if (findings.isNotEmpty()) System.err.println(summary())
                }
            }.apply {
                isDaemon = true
                name = "parse-audit-flush"
                start()
            }
        return this
    }

    /** Flush the final report. */
    override fun close() {
        flusher?.interrupt()
        runCatching { writeReport() }
            .onFailure { System.err.println("parse audit: could not write ${outFile.path}: ${it.message}") }
    }

    /** One distinct parse gap, plus the events that hit it. */
    class Finding(
        val tag: String,
        val level: LogLevel,
        val normalizedMessage: String,
    ) {
        val count = AtomicLong()
        private val samples = ArrayList<Sample>()

        fun hit(
            rawMessage: String,
            event: Event,
            max: Int,
        ) {
            count.incrementAndGet()
            synchronized(samples) {
                if (samples.size < max && samples.none { it.eventId == event.id }) {
                    samples.add(Sample(event.id, event.pubKey, event.kind, rawMessage, event.toJson()))
                }
            }
        }

        fun toJson(): JsonObject =
            buildJsonObject {
                put("tag", JsonPrimitive(tag))
                put("level", JsonPrimitive(level.name))
                put("message", JsonPrimitive(normalizedMessage))
                put("count", JsonPrimitive(count.get()))
                put(
                    "samples",
                    buildJsonArray {
                        synchronized(samples) { samples.toList() }.forEach { s ->
                            add(
                                buildJsonObject {
                                    put("eventId", JsonPrimitive(s.eventId))
                                    put("pubkey", JsonPrimitive(s.pubkey))
                                    put("kind", JsonPrimitive(s.kind))
                                    put("quartzMessage", JsonPrimitive(s.rawMessage))
                                    // The whole event, so a quartz test can replay it verbatim.
                                    put("event", json.parseToJsonElement(s.eventJson))
                                },
                            )
                        }
                    },
                )
            }

        class Sample(
            val eventId: String,
            val pubkey: String,
            val kind: Int,
            val rawMessage: String,
            val eventJson: String,
        )
    }

    companion object {
        private val json = Json { prettyPrint = true }

        // Tags whose messages are parse reports rather than relay logging. Used only
        // to decide what to count-and-drop when it arrives without an event context.
        private val PARSE_TAGS =
            setOf(
                "MetadataEvent",
                "TolerantStringSerializer",
                "TolerantIntSerializer",
                "TolerantLongSerializer",
                "TolerantBooleanSerializer",
                "BirthdayTolerantSerializer",
            )

        private fun isParseReport(tag: String): Boolean = tag in PARSE_TAGS || tag.endsWith("Serializer")

        // Event URIs, 64-hex ids and byte offsets make every occurrence unique; strip
        // them so the same underlying gap groups into one finding.
        private val NOSTR_URI = Regex("""nostr:[a-z0-9]+""", RegexOption.IGNORE_CASE)
        private val HEX64 = Regex("""\b[0-9a-f]{64}\b""", RegexOption.IGNORE_CASE)
        private val OFFSET = Regex("""offset \d+""")

        // kotlinx.serialization appends the offending document ("JSON input: …") to
        // its exception messages. That is the single most useful part of a sample and
        // is kept there verbatim — but in a signature it makes every event unique, so
        // two profiles failing for the identical reason would never group.
        private val JSON_INPUT = Regex("""JSON input:.*""", RegexOption.DOT_MATCHES_ALL)

        private fun normalize(message: String): String =
            message
                .replace(NOSTR_URI, "<event>")
                .replace(HEX64, "<hex>")
                .replace(OFFSET, "offset N")
                .replace(JSON_INPUT, "")
                .trim()

        private fun signature(
            tag: String,
            message: String,
        ): String = "$tag|${normalize(message)}"

        /**
         * Install the audit from the environment, or return null when it is off.
         *
         *   PARSE_AUDIT_FILE     where to write the report; unset ⇒ audit disabled
         *   PARSE_AUDIT_SAMPLES  raw events kept per distinct finding (default 5)
         *   QUARTZ_LOG_LEVEL     DEBUG / INFO / WARN / ERROR — quartz's own floor,
         *                        which defaults to DEBUG (that is why the parse
         *                        reports are so loud). Applies with or without the
         *                        audit, so it is also the plain "quiet it down" knob.
         */
        fun installFromEnv(env: Map<String, String>): ParseAudit? {
            env["QUARTZ_LOG_LEVEL"]?.trim()?.uppercase()?.takeIf { it.isNotEmpty() }?.let { name ->
                val level = LogLevel.entries.firstOrNull { it.name == name }
                if (level == null) {
                    System.err.println("QUARTZ_LOG_LEVEL '$name' is not one of ${LogLevel.entries.joinToString("/") { it.name }} — ignored")
                } else {
                    Log.minLevel = level
                }
            }

            val path = env["PARSE_AUDIT_FILE"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val samples = env["PARSE_AUDIT_SAMPLES"]?.trim()?.toIntOrNull()?.coerceIn(1, 100) ?: 5

            // The audit needs to see what quartz reports, so it must not be filtered
            // out before reaching the sink.
            if (Log.minLevel > LogLevel.WARN) Log.minLevel = LogLevel.WARN

            val flushSec = env["PARSE_AUDIT_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(5L) ?: 60L
            val audit = ParseAudit(File(path), samples)
            Log.sink = audit
            System.err.println("parse audit: on — report at $path (up to $samples sample event(s) per issue, flushed every ${flushSec}s)")
            return audit.startPeriodicFlush(flushSec)
        }
    }
}
