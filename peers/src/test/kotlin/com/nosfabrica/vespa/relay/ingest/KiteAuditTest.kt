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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.JsonEngine
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.io.File
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** What Kite gets wrong is kept whole, as a quartz test case; what is simply bad input is only counted. */
class KiteAuditTest {
    private val booted = JsonEngine.current
    private val signer = NostrSignerSync()
    private val dir = Files.createTempDirectory("kite-audit").toFile()
    private val report = File(dir, "kite-audit.json")

    @BeforeTest
    fun kite() {
        JsonEngine.current = JsonEngine.KITE
    }

    @AfterTest
    fun restore() {
        JsonEngine.current = booted
        dir.deleteRecursively()
    }

    private fun note(text: String = "hello"): Event = signer.sign(1_700_000_000L, 1, emptyArray(), text)

    /** [event] as a relay frame, with [extra] spliced in after its last field. */
    private fun frame(
        event: Event,
        extra: String = "",
    ) = """["EVENT","s",${event.toJson().dropLast(1)}$extra}]"""

    /** A signed event whose content Kite refuses as a number and Jackson reads back as the signed "5". */
    private fun coerced(): String = frame(note("5")).replace("\"content\":\"5\"", "\"content\":5")

    private fun read(audit: KiteAudit): JsonObject {
        audit.writeReport()
        return Json.parseToJsonElement(report.readText()).jsonObject
    }

    private fun JsonObject.count(field: String) = this[field]!!.jsonPrimitive.long

    private fun JsonObject.findings(kind: String) = this["findings"]!!.jsonArray.map { it.jsonObject }.filter { it["kind"]!!.jsonPrimitive.content == kind }

    @Test
    fun `a frame Kite rejects and Jackson reads is kept and handed on as Jackson's`() {
        val audit = KiteAudit(report)
        val text = coerced()
        val msg = audit.decoder().decode(text)
        assertEquals("5", assertIs<EventMessage>(msg).event.content)
        val finding = read(audit).findings(KiteAudit.KITE_THREW).single()
        assertEquals(
            text,
            finding["samples"]!!
                .jsonArray
                .single()
                .jsonObject["frame"]!!
                .jsonPrimitive.content,
        )
    }

    @Test
    fun `one event on many subscriptions is one sample and many counts`() {
        val audit = KiteAudit(report)
        val text = coerced()
        listOf("a", "b", "c").forEach { sub -> audit.decoder().decode(text.replace("[\"EVENT\",\"s\"", "[\"EVENT\",\"$sub\"")) }
        val finding = read(audit).findings(KiteAudit.KITE_THREW).single()
        assertEquals(3, finding.count("count"))
        assertEquals(1, finding["samples"]!!.jsonArray.size)
    }

    @Test
    fun `Kite's strictness on an event that is bad anyway is counted, not kept`() {
        val audit = KiteAudit(report)
        // Signed as "hello": Jackson's coerced "5" does not verify, so Kite refusing it is no bug.
        val msg = audit.decoder().decode(frame(note()).replace("\"content\":\"hello\"", "\"content\":5"))
        assertEquals("5", assertIs<EventMessage>(msg).event.content)
        val doc = read(audit)
        assertEquals(1, doc.count("kiteStricterOnBadEvents"))
        assertTrue(doc["findings"]!!.jsonArray.isEmpty())
    }

    @Test
    fun `a frame both engines refuse is counted, kept nowhere, and still refused`() {
        val audit = KiteAudit(report)
        assertFails { audit.decoder().decode("""["EVENT","s",{"id":""") }
        val doc = read(audit)
        assertEquals(1, doc.count("badFramesBothRefuse"))
        assertTrue(doc["findings"]!!.jsonArray.isEmpty())
    }

    @Test
    fun `an event Kite misreads is kept with both readings`() {
        val audit = KiteAudit(report)
        val signed = note()
        // Kite matches a key by length and its first and last characters, so this reads as the content.
        val text = frame(signed, ",\"comment\":\"spoof\"")
        val misread = assertIs<EventMessage>(audit.decoder().decode(text)).event
        assertEquals("spoof", misread.content)
        audit.onBadEvent(misread)
        val finding = read(audit).findings(KiteAudit.KITE_MISREAD).single()
        assertEquals("content", finding["key"]!!.jsonPrimitive.content)
        val sample = finding["samples"]!!.jsonArray.single().jsonObject
        assertEquals(text, sample["frame"]!!.jsonPrimitive.content)
        assertEquals(signed.toJson(), sample["jacksonRead"]!!.jsonPrimitive.content)
    }

    @Test
    fun `a genuinely bad event is counted, not kept`() {
        val audit = KiteAudit(report)
        val signed = note()
        val text = frame(signed).replace(signed.sig, "f".repeat(128))
        audit.onBadEvent(assertIs<EventMessage>(audit.decoder().decode(text)).event)
        val doc = read(audit)
        assertEquals(1, doc.count("badEventsBothRefuse"))
        assertTrue(doc["findings"]!!.jsonArray.isEmpty())
    }

    @Test
    fun `each copy of an event is judged on the frame it came in`() {
        val audit = KiteAudit(report)
        val decoder = audit.decoder()
        val signed = note()
        val misread = assertIs<EventMessage>(decoder.decode(frame(signed, ",\"comment\":\"spoof\""))).event
        // The same event from another relay, read right, arrives after the misread copy.
        decoder.decode(frame(signed))
        audit.onBadEvent(misread)
        assertEquals(
            "content",
            read(audit)
                .findings(KiteAudit.KITE_MISREAD)
                .single()["key"]!!
                .jsonPrimitive.content,
        )
    }

    @Test
    fun `a refused event whose frame verifies is kept as a failure past the frame`() {
        val audit = KiteAudit(report)
        val text = frame(note())
        audit.onBadEvent(assertIs<EventMessage>(audit.decoder().decode(text)).event)
        val finding = read(audit).findings(KiteAudit.REFUSED_VERIFIES).single()
        assertEquals(
            text,
            finding["samples"]!!
                .jsonArray
                .single()
                .jsonObject["frame"]!!
                .jsonPrimitive.content,
        )
    }

    @Test
    fun `quartz's age-out tick drops no frame, only the budget does`() {
        val audit = KiteAudit(report)
        val decoder = audit.decoder()
        val misread = assertIs<EventMessage>(decoder.decode(frame(note(), ",\"comment\":\"spoof\""))).event
        repeat(3) { decoder.ageOutCache() }
        audit.onBadEvent(misread)
        assertEquals(1, read(audit).findings(KiteAudit.KITE_MISREAD).size)
    }

    @Test
    fun `Jackson's reading is checked with Jackson's id preimage, whatever the engine`() {
        val tricky = listOf("plain", "emoji \uD83D\uDE00", "controls \u0001\u000b\u001f", "line sep \u2028", "lone \uD83D surrogate", "quote \" back \\ slash")
        JsonEngine.current = JsonEngine.DEFAULT
        val events = tricky.map { signer.sign<Event>(1_700_000_000L, 1, arrayOf(arrayOf("t", it)), it) }
        JsonEngine.current = JsonEngine.KITE
        events.forEach { assertTrue(KiteAudit.jacksonIdMatches(it), it.content) }
        val tampered = events.first().let { Event(it.id, it.pubKey, it.createdAt, it.kind, it.tags, it.content + " ", it.sig) }
        assertTrue(!KiteAudit.jacksonIdMatches(tampered))
    }

    @Test
    fun `the frame budget bounds what is held`() {
        // 4 KB: each generation rotates past 2 KB, so at most two of these frames are held.
        val audit = KiteAudit(report, frameBudgetBytes = 4096)
        val decoder = audit.decoder()
        val misread = assertIs<EventMessage>(decoder.decode(frame(note(), ",\"comment\":\"spoof\""))).event
        repeat(20) { decoder.decode(frame(note("filler $it ${"x".repeat(400)}"))) }
        audit.onBadEvent(misread)
        assertEquals(1, read(audit).count("badEventsFrameNotHeld"))
    }

    @Test
    fun `the pipeline shows the audit each event it refuses as forged`() =
        runBlocking {
            val audit = KiteAudit(report)
            val decoder = audit.decoder()
            val misread = assertIs<EventMessage>(decoder.decode(frame(note("misread"), ",\"comment\":\"spoof\""))).event
            val valid = assertIs<EventMessage>(decoder.decode(frame(note("valid")))).event
            val scope = CoroutineScope(Job())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("wss://here.example"))
            val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 8), null, null, scope, kiteAudit = audit)
            val heard = ConcurrentHashMap<String, IEventStore.InsertOutcome>()
            listOf(misread, valid).forEach { e -> pipeline.submit(e, skipVerify = false) { heard[e.id] = it } }
            pipeline.start()
            var waitedMs = 0
            while (heard.size < 2 && waitedMs < 10_000) {
                delay(5)
                waitedMs += 5
            }
            scope.cancel()
            pipeline.close()
            assertEquals(IEventStore.InsertOutcome.Accepted, heard[valid.id])
            assertIs<IEventStore.InsertOutcome.Rejected>(heard[misread.id])
            assertEquals(
                "content",
                read(audit)
                    .findings(KiteAudit.KITE_MISREAD)
                    .single()["key"]!!
                    .jsonPrimitive.content,
            )
        }

    @Test
    fun `on exactly while the engine is kite`() {
        JsonEngine.current = JsonEngine.DEFAULT
        assertNull(KiteAudit.installFromEnv(mapOf("KITE_AUDIT_FILE" to report.path)))
        JsonEngine.current = JsonEngine.KITE
        KiteAudit.installFromEnv(mapOf("KITE_AUDIT_FILE" to report.path))!!.close()
        assertTrue(report.exists())
    }
}
