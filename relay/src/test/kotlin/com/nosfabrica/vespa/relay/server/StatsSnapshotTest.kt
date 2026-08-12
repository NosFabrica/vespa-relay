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
import kotlinx.serialization.json.putJsonObject
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

    // ---- two writers, one document ------------------------------------------

    /** One tier's pass: the members it computed, stamped as that tier's. */
    private fun pass(
        tier: String,
        vararg members: Pair<String, Int>,
    ) = buildJsonObject {
        put("schema", 1)
        put("generatedAt", "$tier-now")
        putJsonObject("tiers") { putJsonObject(tier) { put("generatedAt", "$tier-now") } }
        members.forEach { (member, value) -> put(member, buildJsonObject { put("events", value) }) }
    }

    private fun StatsSnapshot.doc() = Json.parseToJsonElement(assertNotNull(served()).bytes.decodeToString()).jsonObject

    /**
     * A tier publishes its own half and leaves the other alone.
     *
     * This is what makes two cadences possible at all: the counters pass runs
     * fifteen times for every charts pass, and each of those must not blank the
     * charts it did not compute. Both tiers' `tiers` entries survive too — that
     * member is the one thing neither owns, and a plain replacement would leave
     * the document only ever admitting to whichever cadence wrote last.
     */
    @Test
    fun `a tier publishes its own sections and keeps the other tier's`() {
        val snap = StatsSnapshot()
        snap.publish(pass("charts", "kinds" to 1, "activity" to 2), owns = setOf("kinds", "activity"), tier = "charts")
        snap.publish(pass("counters", "corpus" to 3), owns = setOf("corpus", "sync"), tier = "counters")

        val doc = snap.doc()
        assertEquals(
            3,
            doc["corpus"]!!
                .jsonObject["events"]!!
                .jsonPrimitive.content
                .toInt(),
        )
        assertEquals(
            1,
            doc["kinds"]!!
                .jsonObject["events"]!!
                .jsonPrimitive.content
                .toInt(),
            "the charts pass is not lost to the counters pass",
        )
        assertEquals(
            2,
            doc["activity"]!!
                .jsonObject["events"]!!
                .jsonPrimitive.content
                .toInt(),
        )
        assertEquals(setOf("charts", "counters"), doc["tiers"]!!.jsonObject.keys, "both halves state when they ran")
        assertEquals("counters-now", doc["generatedAt"]!!.jsonPrimitive.content, "the document was last touched by the counters")
    }

    /**
     * A section its own tier stops computing DISAPPEARS.
     *
     * `sync` is the case: a relay whose router state files are gone publishes no
     * sync section, and leaving the last one behind would show a coverage card
     * that keeps its numbers forever while the section header claims this pass
     * computed them. Absent is a fact the page draws; stale is one it cannot.
     */
    @Test
    fun `a section the owning tier no longer computes is removed, not left behind`() {
        val snap = StatsSnapshot()
        snap.publish(pass("counters", "corpus" to 1, "sync" to 9), owns = setOf("corpus", "sync"), tier = "counters")
        assertNotNull(snap.doc()["sync"])

        snap.publish(pass("counters", "corpus" to 2), owns = setOf("corpus", "sync"), tier = "counters")
        assertNull(snap.doc()["sync"], "the tier that owns it computed nothing for it")
    }

    /**
     * A staleness notice belongs to the tier that failed, and survives the other
     * tier's success.
     *
     * The failure this ends: charts that have been failing for hours, beside
     * counters forty seconds old, publishing every minute — and every one of
     * those publishes clearing the notice that said so. The page would then draw
     * a document indistinguishable from a healthy one, which is exactly what
     * `stale` exists to prevent.
     */
    @Test
    fun `one tier's staleness notice survives the other tier's publish`() {
        val snap = StatsSnapshot()
        snap.publish(pass("charts", "kinds" to 1), owns = setOf("kinds"), tier = "charts")
        snap.markStale("the last charts rollup failed: boom", tier = "charts")
        assertEquals("charts", assertNotNull(snap.doc()["stale"]).jsonObject["tier"]!!.jsonPrimitive.content)

        snap.publish(pass("counters", "corpus" to 2), owns = setOf("corpus"), tier = "counters")
        val kept = assertNotNull(snap.doc()["stale"], "the counters are current; the charts are still not")
        assertEquals("charts", kept.jsonObject["tier"]!!.jsonPrimitive.content)

        // …and the tier that left it is the one that clears it.
        snap.publish(pass("charts", "kinds" to 3), owns = setOf("kinds"), tier = "charts")
        assertNull(snap.doc()["stale"])
    }

    /**
     * A document from another schema is replaced rather than merged onto.
     *
     * Its members can mean different things, and the ones this schema dropped
     * are owned by nobody now — so merging would leave them in the document
     * forever, beside the numbers that superseded them. The cost is the other
     * tier's sections until its next pass, which is the first minutes after an
     * upgrade and is stated by their absence.
     */
    @Test
    fun `a seeded document from an older schema is not merged onto`() {
        val snap = StatsSnapshot()
        // What a schema-1 state file held: `pubkeys` inside `corpus`, and a
        // top-level `tookMs` that schema 2 does not write.
        snap.publish(
            buildJsonObject {
                put("schema", 1)
                put("tookMs", 41000)
                put("corpus", buildJsonObject { put("pubkeys", 7) })
                put("kinds", buildJsonObject { put("events", 1) })
            },
        )
        snap.publish(pass("counters", "corpus" to 2).let { buildJsonObject { it.forEach { (k, v) -> if (k == "schema") put(k, 2) else put(k, v) } } }, owns = setOf("corpus"), tier = "counters")

        val doc = snap.doc()
        assertNull(doc["tookMs"], "a member this schema does not write cannot be left over from one that did")
        assertNull(doc["kinds"], "the charts sections come back on the next charts pass, stated as absent until then")
        assertEquals(2, doc["schema"]!!.jsonPrimitive.content.toInt())
    }
}
