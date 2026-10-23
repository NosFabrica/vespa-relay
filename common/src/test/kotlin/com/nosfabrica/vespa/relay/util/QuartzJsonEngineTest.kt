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
package com.nosfabrica.vespa.relay.util

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.JsonEngine
import com.vitorpamplona.quartz.nip01Core.core.OptimizedJsonMapper
import com.vitorpamplona.quartz.nip01Core.crypto.EventHasher
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The engine is chosen at boot, and choosing it changes no byte either process keys on or hashes. */
class QuartzJsonEngineTest {
    private val booted = JsonEngine.current

    @AfterTest
    fun restore() {
        JsonEngine.current = booted
    }

    @Test
    fun `unset keeps quartz's default, a name switches, a typo stops the boot`() {
        assertEquals(booted, applyQuartzJsonEngine(emptyMap()))
        assertEquals(JsonEngine.KITE, applyQuartzJsonEngine(mapOf("QUARTZ_JSON_ENGINE" to " Kite ")))
        assertEquals(JsonEngine.KITE, JsonEngine.current)
        assertEquals(JsonEngine.DEFAULT, applyQuartzJsonEngine(mapOf("QUARTZ_JSON_ENGINE" to "default")))
        val refused = assertFailsWith<IllegalStateException> { applyQuartzJsonEngine(mapOf("QUARTZ_JSON_ENGINE" to "kit")) }
        assertTrue("QUARTZ_JSON_ENGINE='kit'" in refused.message.orEmpty())
    }

    @Test
    fun `a filter serializes to the same key under either engine`() {
        // SyncBands and SweepState persist their cursors keyed by Filter.toJson().
        for (filter in FILTERS) {
            assertEquals(under(JsonEngine.DEFAULT) { filter.toJson() }, under(JsonEngine.KITE) { filter.toJson() })
        }
    }

    @Test
    fun `an event reads, writes and hashes the same under either engine`() {
        for (json in EVENTS) {
            val jackson = under(JsonEngine.DEFAULT) { OptimizedJsonMapper.fromJson(json).let { it to it.toJson() } }
            val kite = under(JsonEngine.KITE) { OptimizedJsonMapper.fromJson(json).let { it to it.toJson() } }
            assertEquals(jackson.second, kite.second)
            assertEquals(jackson.first.tags.map { it.toList() }, kite.first.tags.map { it.toList() })
            assertEquals(jackson.first.content, kite.first.content)
            assertEquals(under(JsonEngine.DEFAULT) { hashOf(jackson.first) }, under(JsonEngine.KITE) { hashOf(kite.first) })
        }
    }

    private fun hashOf(event: Event) = EventHasher.hashId(event.pubKey, event.createdAt, event.kind, event.tags, event.content)

    private fun <T> under(
        engine: JsonEngine,
        block: () -> T,
    ): T {
        JsonEngine.current = engine
        return block()
    }

    private companion object {
        const val PK = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        val FILTERS =
            listOf(
                Filter(kinds = listOf(1), limit = 20),
                Filter(kinds = listOf(30382), authors = listOf(PK), tags = mapOf("d" to listOf(PK)), since = 1_700_000_000, until = 1_800_000_000),
                Filter(kinds = listOf(1985), search = "include:spam \"exact\" -word é😀", tagsAll = mapOf("l" to listOf("prime"))),
                Filter(ids = listOf(PK), tags = mapOf("t" to listOf("quote\"back\\slash", "tab\tnew\nline"))),
            )

        val EVENTS =
            listOf(
                """{"id":"$PK","pubkey":"$PK","created_at":1700000000,"kind":1,"tags":[["e","$PK","wss://r.example","reply"],["t","nostr"]],"content":"hello é 😀 \"q\" \\ \n\t\u0001\u000b\u001f ","sig":"${PK}$PK"}""",
                """{"kind":0,"content":"{\"name\":\"x\"}","tags":[],"created_at":1,"pubkey":"$PK","id":"$PK","sig":"${PK}$PK"}""",
                """{"id":"$PK","pubkey":"$PK","created_at":1700000000,"kind":30382,"tags":[["d","$PK"],["rank","87"]],"content":"","sig":"${PK}$PK"}""",
            )
    }
}
