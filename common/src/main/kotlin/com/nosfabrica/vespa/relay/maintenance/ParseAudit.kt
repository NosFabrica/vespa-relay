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
package com.nosfabrica.vespa.relay.maintenance

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
 * Collects every event quartz cannot fully parse, with the raw event JSON, so
 * the gaps can be fixed in quartz instead of silently degrading this relay's
 * search index.
 *
 * Quartz reports parse problems through [Log] with no event attached, so
 * [inspect] re-runs each event's search-indexing parse itself with the event
 * parked in a [ThreadLocal] — everything quartz logs during that call is then
 * attributable to exactly one event. That costs one extra parse per event,
 * which is why the audit is opt-in (`PARSE_AUDIT_FILE`).
 *
 * Findings are grouped by signature (tag + message with event URIs, hex ids
 * and offsets stripped), each keeping up to `PARSE_AUDIT_SAMPLES` raw events.
 */
class ParseAudit(
    private val outFile: File,
    private val maxSamplesPerFinding: Int = 5,
    private val delegate: LogSink = Log.sink,
) : LogSink,
    AutoCloseable {
    /**
     * The event whose parse this thread is currently inside. The `reported`
     * flag lives here because a second event hitting an already-known defect
     * adds no map entry.
     */
    private class InFlight(
        val event: Event,
    ) {
        var reported = false
    }

    private val inFlight = ThreadLocal<InFlight?>()

    private val findings = ConcurrentHashMap<String, Finding>()

    // Parse reports that arrived outside [inspect] — e.g. the store's own
    // indexing pass. Counted, not attributed: there is no event to tie them
    // to. A high number here means inspection is missing a path.
    private val unattributed = ConcurrentHashMap<String, AtomicLong>()

    private val inspected = AtomicLong()
    private val withFindings = AtomicLong()

    @Volatile private var flusher: Thread? = null

    /**
     * Run the event through quartz's search-indexing parse and record whatever
     * it reports. Never throws: an audit must not be able to reject an event
     * the relay would otherwise have stored.
     */
    fun inspect(event: Event) {
        if (event !is SearchableEvent) return
        inspected.incrementAndGet()
        val ctx = InFlight(event)
        inFlight.set(ctx)
        try {
            event.indexableContent()
        } catch (e: Exception) {
            // A parse that throws rather than logging is itself a finding.
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
            // Swallowed: the report is the output — one line per malformed
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

    /** Total reports recorded — one event can trip more than one defect. */
    private fun totalReports(): Long = findings.values.sumOf { it.count.get() }

    /** A one-line summary for the periodic progress log. */
    fun summary(): String =
        "parse audit: ${findings.size} distinct issue(s), ${totalReports()} report(s) over " +
            "${withFindings.get()} affected of ${inspected.get()} event(s)" +
            unattributed.entries.sumOf { it.value.get() }.let { if (it > 0) ", $it unattributed" else "" }

    /**
     * Write the report via a sibling temp file and a move, so a reader never
     * sees a half file. Synchronized because the flusher thread and [close]
     * both write, and both use the SAME temp path.
     */
    @Synchronized
    fun writeReport() {
        val report =
            buildJsonObject {
                put("inspected", JsonPrimitive(inspected.get()))
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
     * Rewrite the report every [intervalSec] on a daemon thread, so a long
     * backfill can be read while it runs rather than only after shutdown.
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
        // Joined, not just interrupted: a flusher caught mid-writeReport()
        // would race this thread on the same temp file, and the loser's
        // half-written tmp could be renamed over the report.
        runCatching { flusher?.join(FLUSHER_JOIN_MS) }
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

        // Bounded: shutdown must not hang on a flusher stuck in disk I/O.
        private const val FLUSHER_JOIN_MS = 5_000L

        // Tags whose messages are parse reports rather than relay logging.
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

        // Event URIs, 64-hex ids and byte offsets make every occurrence unique;
        // strip them so the same underlying gap groups into one finding.
        private val NOSTR_URI = Regex("""nostr:[a-z0-9]+""", RegexOption.IGNORE_CASE)
        private val HEX64 = Regex("""\b[0-9a-f]{64}\b""", RegexOption.IGNORE_CASE)
        private val OFFSET = Regex("""offset \d+""")

        // kotlinx.serialization appends the offending document ("JSON input: …")
        // to its messages. Kept verbatim in samples, stripped from signatures —
        // otherwise two profiles failing identically would never group.
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
         *   QUARTZ_LOG_LEVEL     DEBUG / INFO / WARN / ERROR — quartz's own log
         *                        floor, which defaults to DEBUG
         */
        fun installFromEnv(env: Map<String, String>): ParseAudit? {
            applyQuartzLogLevel(env)

            val path = env["PARSE_AUDIT_FILE"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val samples = env["PARSE_AUDIT_SAMPLES"]?.trim()?.toIntOrNull()?.coerceIn(1, 100) ?: 5

            // The audit needs to see what quartz reports, so it must not be
            // filtered out before reaching the sink.
            if (Log.minLevel > LogLevel.WARN) Log.minLevel = LogLevel.WARN

            val flushSec = env["PARSE_AUDIT_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(5L) ?: 60L
            val audit = ParseAudit(File(path), samples)
            Log.sink = audit
            System.err.println("parse audit: on — report at $path (up to $samples sample event(s) per issue, flushed every ${flushSec}s)")
            return audit.startPeriodicFlush(flushSec)
        }

        /**
         * `QUARTZ_LOG_LEVEL` alone, without the audit. The relay process wants
         * the floor but must NOT install the audit: nothing on the serving
         * side calls [inspect] since the mirror moved to its own process, and
         * an installed-but-unfed audit is precisely the silently-inert
         * configured component this codebase forbids.
         */
        fun applyQuartzLogLevel(env: Map<String, String>) {
            env["QUARTZ_LOG_LEVEL"]?.trim()?.uppercase()?.takeIf { it.isNotEmpty() }?.let { name ->
                val level = LogLevel.entries.firstOrNull { it.name == name }
                if (level == null) {
                    System.err.println("QUARTZ_LOG_LEVEL '$name' is not one of ${LogLevel.entries.joinToString("/") { it.name }} — ignored")
                } else {
                    Log.minLevel = level
                }
            }
        }
    }
}
