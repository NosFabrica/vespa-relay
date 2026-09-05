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
 * The router's manifest as `/stats.json` publishes it: the denominator a
 * client scopes its NIP-45 COUNT to.
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

    /** A stream with no kind bound mirrors everything, so a union over the others would under-count. */
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

    /** An unreadable `kinds` member is a bound that failed to parse, not the absence of one. */
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
     * The exact bytes `SyncManifest` wrote. The writer lives in a module this one cannot
     * import, so a rename there would otherwise pass both suites. Re-capture, never hand-edit.
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

    /** [StatsRollup] catches per section, so a throw here takes the coverage card down with it. */
    @Test
    fun `a member of the wrong type costs that member, never a throw`() {
        val doc =
            MirrorReport.build(
                """
                {
                  "writtenAt": {"nope": 1},
                  "streams": [
                    {"name": {"nope": 1}, "dir": "down", "kinds": [7]},
                    {"name": "content", "dir": ["down"], "kinds": [[1], 2, "3"], "since": {"nope": 1}}
                  ]
                }
                """.trimIndent(),
            )
        assertNotNull(doc)
        assertEquals(listOf("content"), streamsOf(doc).map { it.getValue("name").jsonPrimitive.content })
        // An unreadable `dir` reads as `down`, the direction that cannot drop kinds.
        assertEquals(
            "down",
            streamsOf(doc)
                .single()
                .getValue("dir")
                .jsonPrimitive.content,
        )
        // Element by element: the nested array is dropped, the numeric string is read.
        assertEquals(listOf(2, 3), kindsOf(doc))
        assertNull(doc["writtenAt"])
        assertNull(streamsOf(doc).single()["since"])
    }

    @Test
    fun `no shape of manifest throws`() {
        listOf(
            "{\"streams\":{}}",
            "{\"streams\":[[]]}",
            "{\"streams\":[null]}",
            "{\"streams\":[{\"name\":\"a\",\"kinds\":{}}]}",
            "{\"streams\":[{\"name\":\"a\",\"kinds\":[null]}]}",
            "{\"streams\":[{\"name\":true,\"dir\":7}]}",
            "[]",
            "\"a string\"",
            "3",
        ).forEach { MirrorReport.build(it) }
    }

    @Test
    fun `a manifest naming no streams says nothing`() {
        assertNull(MirrorReport.build(manifest()))
        assertNull(MirrorReport.build("{\"writtenAt\":1800000000}"))
    }

    /** A manifest written before the timestamp existed still parses. */
    @Test
    fun `a manifest with no timestamp still publishes its kinds`() {
        val doc = MirrorReport.build(manifest(stream("content", kinds = "[1]"), writtenAt = null))
        assertNotNull(doc)
        assertNull(doc["writtenAt"])
        assertEquals(listOf(1), kindsOf(doc))
    }

    /** The document is rebuilt member by member, so a manifest cannot smuggle JSON into ours. */
    @Test
    fun `unknown members of the manifest are not relayed`() {
        val doc = MirrorReport.build("{\"streams\":[{\"name\":\"content\",\"dir\":\"down\",\"kinds\":[1],\"note\":\"hello\"}],\"extra\":{\"x\":1}}")
        assertNull(doc?.get("extra"))
        assertTrue(streamsOf(doc).single().keys.all { it in setOf("name", "dir", "kinds", "since") })
    }
}
