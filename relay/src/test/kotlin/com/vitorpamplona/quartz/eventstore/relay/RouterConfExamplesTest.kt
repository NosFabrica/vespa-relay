/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The shipped example configs are the router's documentation, so they have to
 * parse into what they claim to be — a broken example is a broken feature.
 */
class RouterConfExamplesTest {
    /** Tests run from the module dir; the examples sit at the repo root. */
    private fun load(name: String): RouterConfig {
        val file = listOf(File("../$name"), File(name)).firstOrNull { it.isFile }
        return RouterConfigLoader.parse(requireNotNull(file) { "missing example config $name" }.readText())
    }

    @Test
    fun `the plain example parses`() {
        assertTrue(load("router.conf.example").streams.isNotEmpty())
    }

    @Test
    fun `the outbox example is a seed stream plus a 10002 relaySource`() {
        val cfg = load("router.conf.outbox.example")
        val outbox = cfg.dynamicStreams().single()
        assertEquals("outbox", outbox.name)
        assertEquals(RelayListKind.OUTBOX, outbox.relaySource?.kind)
        assertEquals(RelayRole.WRITE, outbox.relaySource?.role)
        // The seed stream is what fills the store the outbox stream then reads.
        assertTrue(cfg.downUpstreams().any { it.streamName == "indexers" })
    }

    @Test
    fun `the assertions example is a seed stream plus a 10040 relaySource`() {
        val cfg = load("router.conf.assertions.example")
        val assertions = cfg.dynamicStreams().single()
        assertEquals("assertions", assertions.name)
        assertEquals(RelayListKind.TRUST_PROVIDERS, assertions.relaySource?.kind)
        // 10040 has no read/write sides, so every url it names is in scope.
        assertEquals(RelayRole.ANY, assertions.relaySource?.role)
        assertTrue(cfg.downUpstreams().any { it.streamName == "indexers" })
    }
}
