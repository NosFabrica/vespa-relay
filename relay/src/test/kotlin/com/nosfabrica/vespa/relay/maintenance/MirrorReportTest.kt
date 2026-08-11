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

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The router's manifest, as `/stats.json` publishes it.
 *
 * What is being pinned is a DENOMINATOR: a client scopes its NIP-45 COUNT to
 * `mirrors.kinds`, so a kind that goes missing here silently shrinks somebody
 * else's total, and a kind that appears here without being mirrored inflates it
 * — which is the 31,118-of-89,485 bug this exists to end, in the other
 * direction.
 */
class MirrorReportTest {
    /** A manifest in the shape `SyncManifest` writes, built rather than pasted. */
    private fun manifest(
        vararg streams: String,
        writtenAt: Long? = 1_800_000_000L,
    ): String =
        buildString {
            append("{")
            writtenAt?.let { append("\"writtenAt\":$it,") }
            append("\"streams\":[").append(streams.joinToString(",")).append("]}")
        }

    private fun stream(
        name: String,
        dir: String? = "down",
        kinds: String? = null,
        since: Long? = null,
    ): String =
        buildString {
            append("{\"name\":\"$name\"")
            dir?.let { append(",\"dir\":\"$it\"") }
            kinds?.let { append(",\"kinds\":$it") }
            since?.let { append(",\"since\":$it") }
            append("}")
        }

    private fun kindsOf(doc: JsonObject?) = doc?.get("kinds")?.jsonArray?.map { it.jsonPrimitive.int }

    private fun streamsOf(doc: JsonObject?) =
        doc
            ?.get("streams")
            ?.jsonArray
            ?.map { it.jsonObject }
            .orEmpty()

    @Test
    fun `the mirrored set is the union over the streams that pull down`() {
        val doc =
            MirrorReport.build(
                manifest(
                    stream("content", kinds = "[1,0,30023]"),
                    stream("assertions", kinds = "[30382]"),
                ),
            )
        assertEquals(listOf(0, 1, 30023, 30382), kindsOf(doc))
        assertEquals(1_800_000_000L, doc?.get("writtenAt")?.jsonPrimitive?.longOrNull)
    }

    /** A push stream holds nothing here, so its kinds are not in the set — but it is still named. */
    @Test
    fun `an up stream contributes no kinds and is still published`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), stream("push", dir = "up", kinds = "[62]")))
        assertEquals(listOf(1), kindsOf(doc))
        assertEquals(listOf("content", "push"), streamsOf(doc).map { it.getValue("name").jsonPrimitive.content })
        assertEquals(listOf(62), streamsOf(doc)[1].getValue("kinds").jsonArray.map { it.jsonPrimitive.int })
    }

    @Test
    fun `a both stream mirrors, so its kinds count`() {
        assertEquals(listOf(1, 62), kindsOf(MirrorReport.build(manifest(stream("content", kinds = "[1]"), stream("heal", dir = "both", kinds = "[62]")))))
    }

    /**
     * The case a partial union would answer WRONGLY. A stream with no kind bound
     * mirrors whatever its relays serve, so no list is the set — and a client
     * scoping a COUNT to a union taken over the other streams would under-count
     * the denominator it came here to fix.
     */
    @Test
    fun `an unbounded stream suppresses the list rather than shrinking it`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), stream("everything")))
        assertNull(doc?.get("kinds"))
        assertEquals(true, doc?.get("allKinds")?.jsonPrimitive?.booleanOrNull)
    }

    @Test
    fun `a bounded mirror says nothing about allKinds`() {
        assertNull(MirrorReport.build(manifest(stream("content", kinds = "[1]")))?.get("allKinds"))
    }

    /** An `up` stream with no kinds is not this relay mirroring everything. */
    @Test
    fun `an unbounded up stream does not widen the set`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), stream("push", dir = "up")))
        assertEquals(listOf(1), kindsOf(doc))
        assertNull(doc?.get("allKinds"))
    }

    /** A mirror bounded in time breaks a count comparison the way a kind bound does. */
    @Test
    fun `a stream's floor is carried through`() {
        val doc = MirrorReport.build(manifest(stream("recent", kinds = "[1]", since = 1_700_000_000L)))
        assertEquals(1_700_000_000L, streamsOf(doc).single()["since"]?.jsonPrimitive?.longOrNull)
    }

    @Test
    fun `a missing dir reads as down`() {
        assertEquals(listOf(1), kindsOf(MirrorReport.build(manifest(stream("content", dir = null, kinds = "[1]")))))
    }

    /**
     * A `kinds` member this build cannot read is a bound it failed to parse, not
     * the absence of one: the stream contributes nothing, and the document does
     * not claim the mirror holds every kind on the strength of a typo.
     */
    @Test
    fun `an unreadable kinds member neither widens nor is invented`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), stream("broken", kinds = "\"0,1\"")))
        assertEquals(listOf(1), kindsOf(doc))
        assertNull(doc?.get("allKinds"))
        assertEquals(emptyList(), streamsOf(doc)[1].getValue("kinds").jsonArray.map { it.jsonPrimitive.int })
    }

    @Test
    fun `entries that name no stream are dropped, not published half-read`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), "{\"dir\":\"down\",\"kinds\":[7]}"))
        assertEquals(listOf("content"), streamsOf(doc).map { it.getValue("name").jsonPrimitive.content })
        assertEquals(listOf(1), kindsOf(doc))
    }

    /**
     * The seam. Every other fixture here is this test's own idea of what the
     * router writes; this one is CAPTURED — the exact bytes `SyncManifest` put
     * on disk for a three-stream config, pasted verbatim. The two halves live in
     * different modules and cannot import each other, so a rename on the writing
     * side is otherwise a change that passes both test suites and publishes an
     * empty set. Re-capture it, do not hand-edit it, if the format moves.
     */
    @Test
    fun `a manifest the router actually wrote reads back whole`() {
        val captured =
            """
            {
                "writtenAt": 1800000000,
                "streams": [
                    {
                        "name": "indexers",
                        "dir": "down",
                        "kinds": [
                            0,
                            10002
                        ]
                    },
                    {
                        "name": "assertions",
                        "dir": "down",
                        "kinds": [
                            30382
                        ],
                        "since": 1700000000
                    },
                    {
                        "name": "monitor",
                        "dir": "up",
                        "kinds": [
                            30166
                        ]
                    }
                ]
            }
            """.trimIndent()
        val doc = MirrorReport.build(captured)
        assertEquals(listOf(0, 10002, 30382), kindsOf(doc))
        assertEquals(1_800_000_000L, doc?.get("writtenAt")?.jsonPrimitive?.longOrNull)
        assertEquals(listOf("indexers", "assertions", "monitor"), streamsOf(doc).map { it.getValue("name").jsonPrimitive.content })
        assertEquals(1_700_000_000L, streamsOf(doc)[1]["since"]?.jsonPrimitive?.longOrNull)
    }

    // ---- nothing to say -----------------------------------------------------

    @Test
    fun `no manifest is not an empty mirror`() {
        assertNull(MirrorReport.build(null))
        assertNull(MirrorReport.build("   "))
    }

    @Test
    fun `a corrupt or half-written manifest costs this object, not the rollup`() {
        assertNull(MirrorReport.build("{\"streams\":[{\"name\":\"content\","))
        assertNull(MirrorReport.build("not json at all"))
    }

    @Test
    fun `a manifest naming no streams says nothing`() {
        assertNull(MirrorReport.build(manifest()))
        assertNull(MirrorReport.build("{\"writtenAt\":1800000000}"))
    }

    /**
     * A manifest written before the timestamp existed still parses — nothing
     * here is load-bearing except the streams, and refusing the document over a
     * missing member would lose the kind set to a missing disclosure.
     */
    @Test
    fun `a manifest with no timestamp still publishes its kinds`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), writtenAt = null))
        assertNotNull(doc)
        assertNull(doc["writtenAt"])
        assertEquals(listOf(1), kindsOf(doc))
    }

    /** The document is rebuilt member by member, so a manifest cannot smuggle its own JSON into ours. */
    @Test
    fun `unknown members of the manifest are not relayed`() {
        val doc = MirrorReport.build("{\"streams\":[{\"name\":\"content\",\"dir\":\"down\",\"kinds\":[1],\"note\":\"hello\"}],\"extra\":{\"x\":1}}")
        assertNull(doc?.get("extra"))
        assertTrue(streamsOf(doc).single().keys.all { it in setOf("name", "dir", "kinds", "since") })
    }
}
