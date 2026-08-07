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
package com.nosfabrica.vespa.relay.server

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The statistics snapshot: what it serves, what it refuses to serve, and what
 * survives a restart.
 *
 * The routes are exercised in [StatsPageTest]; this covers the HOLDER, which is
 * where the two mistakes live that a route test would not catch — publishing
 * bytes with a stale validator, and treating "no document yet" as a document
 * full of zeros.
 */
class StatsSnapshotTest {
    private fun doc(events: Int) =
        buildJsonObject {
            put("schema", 1)
            put("corpus", buildJsonObject { put("events", events) })
        }

    @Test
    fun `nothing is served before the first rollup`() {
        // The distinction the page depends on: "not computed yet" is not
        // "this relay holds nothing", and a snapshot must never invent the
        // second while waiting for the first.
        assertNull(StatsSnapshot().served())
    }

    @Test
    fun `publishing swaps the bytes and the validator together`() {
        val snap = StatsSnapshot()
        snap.publish(doc(1))
        val first = assertNotNull(snap.served())
        assertTrue(first.etag.matches(Regex("^\"[0-9a-f]{16}\"$")), "a quoted strong etag, got ${first.etag}")
        assertEquals(
            1,
            Json
                .parseToJsonElement(first.bytes.decodeToString())
                .jsonObject["corpus"]!!
                .jsonObject["events"]!!
                .jsonPrimitive.content
                .toInt(),
        )

        snap.publish(doc(2))
        val second = assertNotNull(snap.served())
        // The property that makes the ETag worth having: different content can
        // never share a validator, or a poller caches the first document
        // forever and the page silently stops advancing.
        assertTrue(first.etag != second.etag, "new content must mint a new validator")

        snap.publish(doc(1))
        assertEquals(first.etag, assertNotNull(snap.served()).etag, "same content, same validator")
    }

    @Test
    fun `a published document survives a restart`() {
        val file = Files.createTempDirectory("stats").resolve("stats.json").toFile()
        StatsSnapshot(file.path).publish(doc(7))

        // A fresh holder over the same path — the restart.
        val reopened = StatsSnapshot(file.path).also { it.loadFromFile() }
        val served = assertNotNull(reopened.served(), "the file is why a deploy does not blank the page")
        assertEquals(
            7,
            Json
                .parseToJsonElement(served.bytes.decodeToString())
                .jsonObject["corpus"]!!
                .jsonObject["events"]!!
                .jsonPrimitive.content
                .toInt(),
        )
        file.parentFile.deleteRecursively()
    }

    @Test
    fun `a corrupt or missing state file leaves the snapshot empty rather than failing`() {
        val dir = Files.createTempDirectory("stats").toFile()

        // Missing: normal on a first boot, and says nothing.
        val absent = StatsSnapshot(File(dir, "nope.json").path).also { it.loadFromFile() }
        assertNull(absent.served())

        // Truncated: the recovery is an empty page and a loud line, NOT
        // serving half a document under this relay's name.
        val bad = File(dir, "bad.json").also { it.writeText("""{"schema":1,"corpus":{"eve""") }
        val corrupt = StatsSnapshot(bad.path).also { it.loadFromFile() }
        assertNull(corrupt.served())

        // …and the next rollup simply overwrites it.
        corrupt.publish(doc(3))
        assertNotNull(corrupt.served())
        assertTrue(bad.readText().contains("\"events\":3"))
        dir.deleteRecursively()
    }

    @Test
    fun `an unwritable path still serves from memory`() {
        // The file is for durability, not for serving — losing it must not cost
        // the endpoint. A directory where the file should be makes every write
        // fail without making the document unavailable.
        val dir = Files.createTempDirectory("stats").toFile()
        val blocked = File(dir, "stats.json").also { it.mkdirs() }
        val snap = StatsSnapshot(blocked.path)
        snap.publish(doc(5))
        assertNotNull(snap.served(), "the in-memory document is unaffected by a failed write")
        dir.deleteRecursively()
    }
}
