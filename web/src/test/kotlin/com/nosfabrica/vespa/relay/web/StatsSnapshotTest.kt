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
package com.nosfabrica.vespa.relay.web

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The holder behind the stats routes: bytes and validator move together, and "no document yet"
 * is not a document full of zeros.
 */
class StatsSnapshotTest {
    private fun doc(events: Int) =
        buildJsonObject {
            put("schema", 1)
            put("corpus", buildJsonObject { put("events", events) })
        }

    @Test
    fun `nothing is served before the first rollup`() {
        // "Not computed yet" is not "this relay holds nothing".
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
        // Different content never shares a validator, or a poller caches the first document forever.
        assertTrue(first.etag != second.etag, "new content must mint a new validator")

        snap.publish(doc(1))
        assertEquals(first.etag, assertNotNull(snap.served()).etag, "same content, same validator")
    }

    private fun StatsSnapshot.doc() = Json.parseToJsonElement(assertNotNull(served()).bytes.decodeToString()).jsonObject

    @Test
    fun `a failed rollup marks the document stale, and the next publish clears it`() {
        val snap = StatsSnapshot()
        snap.markStale("boom")
        assertNull(snap.served(), "nothing to mark before the first rollup")

        snap.publish(
            buildJsonObject {
                put("schema", 1)
                put("generatedAt", "then")
            },
        )
        snap.markStale("the last rollup failed: boom", atSeconds = 42)
        val stale = assertNotNull(snap.doc()["stale"]).jsonObject
        assertEquals("the last rollup failed: boom", stale["reason"]!!.jsonPrimitive.content)
        assertEquals("then", stale["generatedAt"]!!.jsonPrimitive.content, "when the numbers on screen were computed")

        snap.publish(doc(3))
        assertNull(snap.doc()["stale"], "a fresh document is the whole answer")
    }
}
