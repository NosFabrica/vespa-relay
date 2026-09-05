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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.relay.maintenance.applyQuartzLogLevel
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
 * Collects every event quartz cannot fully parse, with the raw event JSON. Quartz reports
 * parse problems through [Log] with no event attached, so [inspect] re-runs each event's
 * parse with the event parked in a [ThreadLocal]; everything logged during it is that event's.
 */
class ParseAudit(
    private val outFile: File,
    private val maxSamplesPerFinding: Int = 5,
    private val delegate: LogSink = Log.sink,
) : LogSink,
    AutoCloseable {
    /** The event whose parse this thread is currently inside. */
    private class InFlight(
        val event: Event,
    ) {
        var reported = false
    }

    private val inFlight = ThreadLocal<InFlight?>()

    private val findings = ConcurrentHashMap<String, Finding>()

    // Parse reports that arrived outside [inspect].
    private val unattributed = ConcurrentHashMap<String, AtomicLong>()

    private val inspected = AtomicLong()
    private val withFindings = AtomicLong()

    @Volatile private var flusher: Thread? = null

    /** Run the event through quartz's search-indexing parse and record what it reports. */
    fun inspect(event: Event) {
        if (event !is SearchableEvent) return
        inspected.incrementAndGet()
        val ctx = InFlight(event)
        inFlight.set(ctx)
        try {
            event.indexableContent()
        } catch (e: Exception) {
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
            // Swallowed on purpose: the report is the output.
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

    /** One event can trip more than one defect. */
    private fun totalReports(): Long = findings.values.sumOf { it.count.get() }

    /** A one-line summary for the periodic progress log. */
    fun summary(): String =
        "parse audit: ${findings.size} distinct issue(s), ${totalReports()} report(s) over " +
            "${withFindings.get()} affected of ${inspected.get()} event(s)" +
            unattributed.entries.sumOf { it.value.get() }.let { if (it > 0) ", $it unattributed" else "" }

    /** Write the report via a sibling temp file and a move. Synchronized: the flusher and [close] share the temp path. */
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

    /** Rewrite the report every [intervalSec] on a daemon thread. */
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
        // Joined, not just interrupted: a flusher mid-writeReport() shares the temp file.
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

        private const val FLUSHER_JOIN_MS = 5_000L

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

        // Stripped from signatures so one gap groups into one finding.
        private val NOSTR_URI = Regex("""nostr:[a-z0-9]+""", RegexOption.IGNORE_CASE)
        private val HEX64 = Regex("""\b[0-9a-f]{64}\b""", RegexOption.IGNORE_CASE)
        private val OFFSET = Regex("""offset \d+""")

        // kotlinx.serialization appends the offending document to its messages.
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

        /** Install the audit from the environment, or null when `PARSE_AUDIT_FILE` is unset. */
        fun installFromEnv(env: Map<String, String>): ParseAudit? {
            applyQuartzLogLevel(env)

            val path = env["PARSE_AUDIT_FILE"]?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            val samples = env["PARSE_AUDIT_SAMPLES"]?.trim()?.toIntOrNull()?.coerceIn(1, 100) ?: 5

            // The sink must see what quartz reports, whatever the configured floor.
            if (Log.minLevel > LogLevel.WARN) Log.minLevel = LogLevel.WARN

            val flushSec = env["PARSE_AUDIT_INTERVAL_SECONDS"]?.trim()?.toLongOrNull()?.coerceAtLeast(5L) ?: 60L
            val audit = ParseAudit(File(path), samples)
            Log.sink = audit
            System.err.println("parse audit: on — report at $path (up to $samples sample event(s) per issue, flushed every ${flushSec}s)")
            return audit.startPeriodicFlush(flushSec)
        }
    }
}
