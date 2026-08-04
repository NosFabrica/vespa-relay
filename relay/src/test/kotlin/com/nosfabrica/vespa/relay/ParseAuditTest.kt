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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel
import com.vitorpamplona.quartz.utils.LogSink
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ParseAuditTest {
    private val originalSink = Log.sink
    private val originalLevel = Log.minLevel

    @AfterTest
    fun restoreLog() {
        Log.sink = originalSink
        Log.minLevel = originalLevel
    }

    private fun metadata(
        id: String,
        content: String,
    ) = MetadataEvent(id.repeat(64).take(64), "a1".repeat(32), 1_700_000_000L, emptyArray(), content, "")

    private fun audit(dir: File): ParseAudit {
        val a = ParseAudit(File(dir, "report.json"), maxSamplesPerFinding = 5)
        Log.sink = a
        Log.minLevel = LogLevel.DEBUG
        return a
    }

    @Test
    fun `attributes an unparseable kind 0 content to its event`() {
        val dir = createTempDir()
        audit(dir).use { a ->
            // A JSON array where an object belongs — still unparseable.
            a.inspect(metadata("1", "[]"))

            val findings = a.snapshot()
            assertEquals(1, findings.size, "expected exactly one finding, got ${findings.map { it.normalizedMessage }}")
            assertEquals(1L, findings.single().count.get())
        }
    }

    @Test
    fun `empty content is no longer a parse failure`() {
        // It was: "Content Parse Error: … Expected start of the object '{', but
        // had 'EOF' instead" was the second-largest class in a live audit, 3,783
        // of 77,753 reports. quartz cdef4e9658 reads an empty content as an empty
        // profile instead of a failed parse, which is what it always meant.
        val dir = createTempDir()
        audit(dir).use { a ->
            a.inspect(metadata("1", ""))
            assertEquals(emptyList(), a.snapshot().map { it.normalizedMessage })
        }
    }

    @Test
    fun `groups the same failure across different events into one finding`() {
        val dir = createTempDir()
        audit(dir).use { a ->
            // Same defect (content is a JSON array, not an object), different events.
            a.inspect(metadata("1", "[]"))
            a.inspect(metadata("2", "[1,2,3]"))
            a.inspect(metadata("3", "[]"))

            val findings = a.snapshot()
            assertEquals(1, findings.size, "the same defect should collapse to one finding")
            assertEquals(3L, findings.single().count.get())
        }
    }

    @Test
    fun `counts every affected event, not just the ones that introduce a new defect`() {
        val dir = createTempDir()
        val out = File(dir, "report.json")
        val a = ParseAudit(out, maxSamplesPerFinding = 5)
        Log.sink = a
        Log.minLevel = LogLevel.DEBUG

        // Three events, one shared defect. Only the first adds a map entry, so a
        // counter derived from the findings map would report 1 affected event here.
        a.inspect(metadata("1", "[]"))
        a.inspect(metadata("2", "[1,2,3]"))
        a.inspect(metadata("3", "[]"))
        // ...plus one clean profile, which must not be counted as affected.
        a.inspect(metadata("4", """{"name":"alice"}"""))
        a.close()

        val report = Json.parseToJsonElement(out.readText()).jsonObject
        assertEquals(4, report["inspected"]!!.jsonPrimitive.content.toInt())
        assertEquals(1, report["distinctFindings"]!!.jsonPrimitive.content.toInt())
        assertEquals(
            3,
            report["eventsWithFindings"]!!.jsonPrimitive.content.toInt(),
            "all three malformed events are affected, not only the one that introduced the defect",
        )
        assertEquals(3, report["totalReports"]!!.jsonPrimitive.content.toInt())
    }

    @Test
    fun `one event tripping two defects counts once but reports twice`() {
        val dir = createTempDir()
        val out = File(dir, "report.json")
        val a = ParseAudit(out, maxSamplesPerFinding = 5)
        Log.sink = a
        Log.minLevel = LogLevel.DEBUG

        // A wrongly-typed name AND a wrongly-typed birthday in one profile.
        a.inspect(metadata("1", """{"name":{"nested":"obj"},"birthday":"1990-01-01"}"""))
        a.close()

        val report = Json.parseToJsonElement(out.readText()).jsonObject
        assertEquals(1, report["eventsWithFindings"]!!.jsonPrimitive.content.toInt())
        assertEquals(2, report["totalReports"]!!.jsonPrimitive.content.toInt())
        assertEquals(2, report["distinctFindings"]!!.jsonPrimitive.content.toInt())
    }

    @Test
    fun `a well-formed profile produces no findings`() {
        val dir = createTempDir()
        audit(dir).use { a ->
            a.inspect(metadata("1", """{"name":"alice","about":"hi"}"""))
            assertEquals(emptyList(), a.snapshot().map { it.normalizedMessage })
        }
    }

    @Test
    fun `report carries the raw event so a quartz test can replay it`() {
        val dir = createTempDir()
        val out = File(dir, "report.json")
        val a = ParseAudit(out, maxSamplesPerFinding = 5)
        Log.sink = a
        Log.minLevel = LogLevel.DEBUG
        a.inspect(metadata("7", "not json at all"))
        a.close()

        val report = Json.parseToJsonElement(out.readText()).jsonObject
        assertEquals(1, report["inspected"]!!.jsonPrimitive.content.toInt())
        val finding = report["findings"]!!.jsonArray.single().jsonObject
        val sample = finding["samples"]!!.jsonArray.single().jsonObject

        // The embedded event must be the real thing, not a summary of it.
        val event = sample["event"]!!.jsonObject
        assertEquals(0, event["kind"]!!.jsonPrimitive.content.toInt())
        assertEquals("not json at all", event["content"]!!.jsonPrimitive.content)
        assertContains(sample.keys, "pubkey")
        assertTrue(finding["count"]!!.jsonPrimitive.content.toInt() >= 1)
    }

    @Test
    fun `unrelated logging still reaches the delegate sink`() {
        val dir = createTempDir()
        val seen = mutableListOf<String>()
        val collector =
            object : LogSink {
                override fun log(
                    level: LogLevel,
                    tag: String,
                    message: String,
                    throwable: Throwable?,
                ) {
                    seen.add("$tag:$message")
                }
            }
        val a = ParseAudit(File(dir, "report.json"), maxSamplesPerFinding = 5, delegate = collector)
        Log.sink = a
        Log.minLevel = LogLevel.DEBUG

        Log.w("SomeOtherSubsystem", "a real warning")
        assertEquals(listOf("SomeOtherSubsystem:a real warning"), seen)

        // Parse chatter with no event context is counted, not forwarded.
        Log.w("TolerantStringSerializer", "Ignoring non-primitive string field (JsonObject)")
        assertEquals(1, seen.size, "parse chatter should not reach the delegate")
        a.close()
    }

    @Test
    fun `installFromEnv is off without a file and sets the quartz log floor`() {
        assertEquals(null, ParseAudit.installFromEnv(emptyMap()))

        assertEquals(null, ParseAudit.installFromEnv(mapOf("QUARTZ_LOG_LEVEL" to "ERROR")))
        assertEquals(LogLevel.ERROR, Log.minLevel)

        val dir = createTempDir()
        val a = ParseAudit.installFromEnv(mapOf("PARSE_AUDIT_FILE" to File(dir, "r.json").path))!!
        // The audit must be able to see WARN-level reports, whatever the floor was.
        assertEquals(LogLevel.WARN, Log.minLevel)
        a.close()
    }

    private fun createTempDir(): File {
        val f = File.createTempFile("parse-audit", "")
        f.delete()
        f.mkdirs()
        return f
    }
}
