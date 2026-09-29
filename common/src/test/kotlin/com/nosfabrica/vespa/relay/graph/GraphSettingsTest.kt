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
package com.nosfabrica.vespa.relay.graph

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class GraphSettingsTest {
    @Test
    fun `off unless asked for`() {
        assertNull(GraphSettings.fromEnv(emptyMap()))
        assertNull(GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "off")))
    }

    @Test
    fun `on needs a password`() {
        assertFailsWith<IllegalStateException> { GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "on")) }
    }

    @Test
    fun `a misspelled switch stops the boot rather than reading as off`() {
        assertFailsWith<IllegalStateException> { GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "yes")) }
        assertFailsWith<IllegalStateException> { GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "on", "NEO4J_PASSWORD" to "x", "GRAPH_CYPHER" to "everyone")) }
    }

    @Test
    fun `defaults keep cypher closed`() {
        val s = GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "on", "NEO4J_PASSWORD" to "x"))!!
        assertEquals(CypherAccess.OFF, s.cypher)
        assertEquals("bolt://neo4j:7687", s.url)
        assertEquals(emptySet(), s.excludedKinds)
        assertNull(s.tagNodes)
    }

    @Test
    fun `kinds and tag names parse from lists`() {
        val s =
            GraphSettings.fromEnv(
                mapOf("GRAPH_PROJECTION" to "on", "NEO4J_PASSWORD" to "x", "GRAPH_CYPHER" to "Admin", "GRAPH_EXCLUDE_KINDS" to "4, 1059", "GRAPH_TAG_NODES" to "t,i"),
            )!!
        assertEquals(CypherAccess.ADMIN, s.cypher)
        assertEquals(setOf(4, 1059), s.excludedKinds)
        assertEquals(setOf("t", "i"), s.tagNodes)
    }

    @Test
    fun blankTagNodesMeansTheDefaultListNotNone() {
        // docker-compose passes `GRAPH_TAG_NODES: ${GRAPH_TAG_NODES:-}` — an empty string.
        val s = GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "on", "NEO4J_PASSWORD" to "x", "GRAPH_TAG_NODES" to ""))!!
        assertNull(s.tagNodes)
    }

    @Test
    fun aQueueOrTickBelowOneStopsTheBoot() {
        for (key in listOf("GRAPH_QUEUE", "GRAPH_RECONCILE_SECONDS")) {
            for (bad in listOf("0", "-1")) {
                assertFailsWith<IllegalArgumentException>("$key=$bad") {
                    GraphSettings.fromEnv(mapOf("GRAPH_PROJECTION" to "on", "NEO4J_PASSWORD" to "x", key to bad))
                }
            }
        }
    }
}
