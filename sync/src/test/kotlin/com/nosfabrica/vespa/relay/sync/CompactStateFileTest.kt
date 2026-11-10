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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The band file is written compact, and an indented one an older build wrote still loads to the same state. */
class CompactStateFileTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")
    private val b = RelayUrlNormalizer.normalize("wss://b.example")
    private val content = Filter(kinds = listOf(1, 6, 7, 30023))

    private fun now() = System.currentTimeMillis() / 1000

    private fun spans(
        min: Long,
        max: Long,
    ) = content.kinds!!.associateWith { SyncCoverage.Span(min, max) }

    private fun written(f: File): SyncBands =
        SyncBands(f).apply {
            val now = now()
            record("content", a, content, now - 86_400, now - 60, paged = true, reconciledThrough = now - 120, observedByKind = spans(now - 86_400, now - 60))
            record("content", b, content, now - 3_600, now - 600, paged = true, observedByKind = spans(now - 3_600, now - 600))
            flush()
        }

    @Test
    fun `the band file is written without indentation`() {
        val dir = Files.createTempDirectory("compact-bands").toFile()
        try {
            val f = File(dir, "sync-cursors.json")
            written(f).close()
            val text = f.readText()
            assertTrue(text.contains("wss://a.example") && text.contains("wss://b.example"), "the premise: the file holds both bands")
            val compact = Json.encodeToString(JsonObject.serializer(), Json.parseToJsonElement(text) as JsonObject)
            assertEquals(compact, text, "byte for byte the compact encoding of what it holds")
        } finally {
            dir.deleteRecursively()
        }
    }

    @Test
    fun `a pretty file from an older build loads to the same bands`() {
        val dir = Files.createTempDirectory("compact-bands").toFile()
        try {
            val compact = File(dir, "compact.json")
            val bands = written(compact)
            val expected = bands.snapshot()
            bands.close()
            assertTrue(expected.toString().contains("wss://b.example"), "the premise: there is state to compare")

            val pretty = File(dir, "pretty.json")
            val root = Json.parseToJsonElement(compact.readText()) as JsonObject
            pretty.writeText(Json { prettyPrint = true }.encodeToString(JsonObject.serializer(), root))
            assertTrue(pretty.readText().contains("\n    "), "the premise: the old format, indented")

            SyncBands(pretty).use { assertEquals(expected, it.snapshot(), "the same state, whatever the whitespace") }
            assertEquals(1, dir.listFiles().orEmpty().count { it.name.startsWith("pretty.json") }, "and nothing was quarantined")
        } finally {
            dir.deleteRecursively()
        }
    }
}
