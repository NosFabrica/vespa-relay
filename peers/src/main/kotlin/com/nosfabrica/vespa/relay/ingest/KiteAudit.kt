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

import com.nosfabrica.vespa.relay.util.strictLong
import com.nosfabrica.vespa.relay.util.writeDurably
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.JsonEngine
import com.vitorpamplona.quartz.nip01Core.core.OptimizedJsonMapper
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.jackson.JacksonMapper
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MessageDecoder
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
 * The Kite trial's evidence: a frame Kite rejects, or an event it reads wrong, re-read with Jackson
 * and kept whole when Jackson reads it right, as a quartz test case. Valid traffic pays only for
 * holding each EVENT frame until the pipeline has checked that event's id.
 */
class KiteAudit(
    private val outFile: File?,
    private val frameBudgetBytes: Long = DEFAULT_FRAMES_MB * MB,
    private val maxSamplesPerFinding: Int = MAX_SAMPLES,
) : AutoCloseable {
    // Two generations, rotated on quartz's age-out tick or once the newer holds half the budget.
    @Volatile private var current = ConcurrentHashMap<String, String>()

    @Volatile private var previous = ConcurrentHashMap<String, String>()
    private val currentBytes = AtomicLong()

    private val findings = ConcurrentHashMap<String, Finding>()
    private val overflow = AtomicLong()

    private val eventFrames = AtomicLong()
    private val badFrames = AtomicLong()
    private val badEvents = AtomicLong()
    private val genuinelyBad = AtomicLong()
    private val notHeld = AtomicLong()
    private val notReproduced = AtomicLong()

    @Volatile private var flusher: Thread? = null

    /** [delegate] (Kite) for every frame, Jackson for one Kite throws on, and each EVENT frame held for [onBadEvent]. */
    fun decoder(delegate: MessageDecoder = MessageDecoder.Default): MessageDecoder =
        object : MessageDecoder {
            override fun decode(text: String): Message {
                val msg =
                    try {
                        delegate.decode(text)
                    } catch (kiteError: Exception) {
                        return rescue(text, kiteError)
                    }
                if (msg is EventMessage) hold(msg.event.id, text)
                return msg
            }

            override fun ageOutCache() {
                delegate.ageOutCache()
                ageOut()
            }

            override fun clearCache() {
                delegate.clearCache()
                ageOut()
                ageOut()
            }
        }

    private fun rescue(
        text: String,
        kiteError: Exception,
    ): Message {
        val jackson =
            try {
                JacksonMapper.fromJsonToMessage(text)
            } catch (_: Exception) {
                badFrames.incrementAndGet()
                throw kiteError
            }
        val eventId = (jackson as? EventMessage)?.event?.id
        record(KITE_THREW, normalize("${kiteError.javaClass.simpleName}: ${kiteError.message}"), Sample(text, eventId, kiteError.message, null, null))
        return jackson
    }

    private fun hold(
        id: String,
        frame: String,
    ) {
        eventFrames.incrementAndGet()
        current[id] = frame
        if (currentBytes.addAndGet(frame.length * 2L) > frameBudgetBytes / 2) rotateIfFull()
    }

    @Synchronized
    private fun rotateIfFull() {
        if (currentBytes.get() > frameBudgetBytes / 2) ageOut()
    }

    @Synchronized
    private fun ageOut() {
        previous = current
        current = ConcurrentHashMap()
        currentBytes.set(0)
    }

    /**
     * An event the pipeline refused as a bad id or signature. Never throws: it runs inside a
     * batch's bookkeeping.
     */
    fun onBadEvent(event: Event) {
        try {
            badEvents.incrementAndGet()
            val frame = current.remove(event.id) ?: previous.remove(event.id)
            if (frame == null) {
                notHeld.incrementAndGet()
                return
            }
            val kite = eventIn(frame) { OptimizedJsonMapper.fromJsonToMessage(it) }
            val jackson = eventIn(frame) { JacksonMapper.fromJsonToMessage(it) }
            when {
                jackson == null || !jackson.verify() -> genuinelyBad.incrementAndGet()
                kite != null && kite.verify() -> notReproduced.incrementAndGet()
                else -> record(KITE_MISREAD, fieldsThatDiffer(kite, jackson), Sample(frame, jackson.id, null, kite?.toJson(), jackson.toJson()))
            }
        } catch (e: Exception) {
            System.err.println("kite audit: could not check event ${event.id} (${e.javaClass.simpleName}: ${e.message})")
        }
    }

    private fun eventIn(
        frame: String,
        read: (String) -> Message,
    ): Event? = runCatching { (read(frame) as? EventMessage)?.event }.getOrNull()

    private fun record(
        kind: String,
        key: String,
        sample: Sample,
    ) {
        val id = "$kind|$key"
        val finding =
            findings[id] ?: synchronized(findings) {
                findings[id] ?: if (findings.size >= MAX_FINDINGS) null else Finding(kind, key).also { findings[id] = it }.also { announce(it, sample) }
            }
        if (finding == null) overflow.incrementAndGet() else finding.hit(sample, maxSamplesPerFinding)
    }

    private fun announce(
        finding: Finding,
        sample: Sample,
    ) {
        val where = outFile?.let { "sample in ${it.path}" } ?: "frame: ${sample.frame.take(STDERR_FRAME_CHARS)}"
        System.err.println("kite audit: new finding ${finding.kind} (${finding.key}); $where")
    }

    /** A one-line summary for the periodic log. */
    fun summary(): String =
        "kite audit: ${count(KITE_THREW)} frame(s) Kite rejected and Jackson read, ${count(KITE_MISREAD)} event(s) Kite misread; " +
            "both engines refused ${badFrames.get()} frame(s) and ${genuinelyBad.get()} event(s); " +
            "${notHeld.get()} refused event(s) unchecked (frame no longer held), of ${eventFrames.get()} EVENT frame(s)"

    private fun count(kind: String): Long = findings.values.filter { it.kind == kind }.sumOf { it.count.get() }

    /** Write the report durably. Synchronized: the flusher and [close] share the temp path. */
    @Synchronized
    fun writeReport() {
        val file = outFile ?: return
        val report =
            buildJsonObject {
                put("eventFrames", JsonPrimitive(eventFrames.get()))
                put("kiteRejectedJacksonRead", JsonPrimitive(count(KITE_THREW)))
                put("kiteMisread", JsonPrimitive(count(KITE_MISREAD)))
                put("badFramesBothRefuse", JsonPrimitive(badFrames.get()))
                put("badEvents", JsonPrimitive(badEvents.get()))
                put("badEventsBothRefuse", JsonPrimitive(genuinelyBad.get()))
                put("badEventsFrameNotHeld", JsonPrimitive(notHeld.get()))
                put("badEventsKiteVerifies", JsonPrimitive(notReproduced.get()))
                put("findingsPastCap", JsonPrimitive(overflow.get()))
                put("findings", JsonArray(findings.values.sortedByDescending { it.count.get() }.map { it.toJson() }))
            }
        writeDurably(file, json.encodeToString(JsonObject.serializer(), report))
    }

    /** Rewrite the report every [intervalSec] on a daemon thread, and say the summary when there is anything to say. */
    fun startPeriodicFlush(intervalSec: Long): KiteAudit {
        flusher =
            Thread {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(intervalSec * 1000)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                    runCatching { writeReport() }.onFailure { System.err.println("kite audit: could not write ${outFile?.path}: ${it.message}") }
                    if (findings.isNotEmpty() || badFrames.get() > 0 || badEvents.get() > 0) System.err.println(summary())
                }
            }.apply {
                isDaemon = true
                name = "kite-audit-flush"
                start()
            }
        return this
    }

    override fun close() {
        flusher?.interrupt()
        // Joined, not just interrupted: a flusher mid-writeReport() shares the temp file.
        runCatching { flusher?.join(FLUSHER_JOIN_MS) }
        runCatching { writeReport() }.onFailure { System.err.println("kite audit: could not write ${outFile?.path}: ${it.message}") }
    }

    /** One distinct divergence, and the frames that show it. */
    class Finding(
        val kind: String,
        val key: String,
    ) {
        val count = AtomicLong()
        private val samples = ArrayList<Sample>()

        fun hit(
            sample: Sample,
            max: Int,
        ) {
            count.incrementAndGet()
            synchronized(samples) { if (samples.size < max && samples.none { it.sameEvent(sample) }) samples.add(sample) }
        }

        fun toJson(): JsonObject =
            buildJsonObject {
                put("kind", JsonPrimitive(kind))
                put("key", JsonPrimitive(key))
                put("count", JsonPrimitive(count.get()))
                put(
                    "samples",
                    buildJsonArray {
                        synchronized(samples) { samples.toList() }.forEach { s ->
                            add(
                                buildJsonObject {
                                    put("frame", JsonPrimitive(s.frame.take(MAX_FRAME_CHARS)))
                                    if (s.frame.length > MAX_FRAME_CHARS) put("frameChars", JsonPrimitive(s.frame.length))
                                    s.kiteError?.let { put("kiteError", JsonPrimitive(it.take(MAX_ERROR_CHARS))) }
                                    s.kiteEvent?.let { put("kiteRead", JsonPrimitive(it)) }
                                    s.jacksonEvent?.let { put("jacksonRead", JsonPrimitive(it)) }
                                },
                            )
                        }
                    },
                )
            }
    }

    /** The frame verbatim, and what each engine made of it. */
    class Sample(
        val frame: String,
        /** Jackson's reading of the id, so one event arriving on many subscriptions is one sample. */
        val eventId: String?,
        val kiteError: String?,
        val kiteEvent: String?,
        val jacksonEvent: String?,
    ) {
        fun sameEvent(other: Sample) = if (eventId != null) eventId == other.eventId else frame == other.frame
    }

    companion object {
        /** Kite threw on a frame Jackson reads. */
        const val KITE_THREW = "kite-threw"

        /** Kite read an event whose id or signature then failed, where Jackson's reading verifies. */
        const val KITE_MISREAD = "kite-misread"

        const val DEFAULT_FRAMES_MB = 64L
        private const val MB = 1024L * 1024
        private const val MAX_FINDINGS = 64
        private const val MAX_SAMPLES = 5
        private const val MAX_FRAME_CHARS = 1_000_000
        private const val MAX_ERROR_CHARS = 500
        private const val STDERR_FRAME_CHARS = 2_000
        private const val FLUSHER_JOIN_MS = 5_000L
        private const val FLUSH_SECONDS = 60L

        private val json = Json { prettyPrint = true }
        private val DIGITS = Regex("""\d+""")

        private fun normalize(message: String): String = message.replace(DIGITS, "N").take(MAX_ERROR_CHARS)

        /** Which NIP-01 fields the two readings disagree on: what a quartz fix has to change. */
        private fun fieldsThatDiffer(
            kite: Event?,
            jackson: Event,
        ): String {
            if (kite == null) return "kite cannot re-read the frame"
            val differ =
                listOfNotNull(
                    "id".takeIf { kite.id != jackson.id },
                    "pubkey".takeIf { kite.pubKey != jackson.pubKey },
                    "created_at".takeIf { kite.createdAt != jackson.createdAt },
                    "kind".takeIf { kite.kind != jackson.kind },
                    "tags".takeIf { !kite.tags.contentDeepEquals(jackson.tags) },
                    "content".takeIf { kite.content != jackson.content },
                    "sig".takeIf { kite.sig != jackson.sig },
                )
            return if (differ.isEmpty()) "no field differs" else differ.joinToString(",")
        }

        /** On exactly while the engine is Kite: `KITE_AUDIT_FILE` says where the report goes, not whether there is one. */
        fun installFromEnv(env: Map<String, String>): KiteAudit? {
            if (!JsonEngine.isKite) return null
            val path = env["KITE_AUDIT_FILE"]?.trim()?.takeIf { it.isNotEmpty() }
            val mb = env.strictLong("KITE_AUDIT_FRAMES_MB", 1L..4096L) ?: DEFAULT_FRAMES_MB
            val where = path?.let { "report at $it" } ?: "findings on stderr only (set KITE_AUDIT_FILE to keep whole frames)"
            System.err.println("kite audit: on, $where, up to $mb MB of frames held for the id check")
            return KiteAudit(path?.let(::File), mb * MB).startPeriodicFlush(FLUSH_SECONDS)
        }
    }
}
