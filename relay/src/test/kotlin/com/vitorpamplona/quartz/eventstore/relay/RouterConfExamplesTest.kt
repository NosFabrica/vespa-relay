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
 * The shipped example config is the router's documentation, so it has to parse
 * into what it claims to be — a broken example is a broken feature.
 */
class RouterConfExamplesTest {
    /** Tests run from the module dir; the example sits at the repo root. */
    private val example: RouterConfig =
        RouterConfigLoader.parse(
            requireNotNull(
                listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile },
            ) { "missing router.conf.example" }.readText(),
        )

    @Test
    fun `the static streams parse and seed the dynamic ones`() {
        // The dynamic streams can only fan out over relay lists these mirror in.
        assertTrue(example.downUpstreams().any { it.streamName == "indexers" })
        assertTrue(example.downUpstreams().any { it.filter.kinds?.contains(10002) == true })
        assertTrue(example.downUpstreams().any { it.filter.kinds?.contains(10040) == true })
    }

    @Test
    fun `the outbox stream reads write-marked relays out of the 10002s`() {
        val outbox = example.dynamicStreams().first { it.name == "outbox" }
        assertEquals(RelayListKind.OUTBOX, outbox.relaySource?.kind)
        assertEquals(RelayRole.WRITE, outbox.relaySource?.role)
        assertTrue(outbox.urls.isEmpty(), "a relaySource stream carries no static urls")
    }

    @Test
    fun `the assertions stream reads every relay out of the 10040s`() {
        val assertions = example.dynamicStreams().first { it.name == "assertions" }
        assertEquals(RelayListKind.TRUST_PROVIDERS, assertions.relaySource?.kind)
        // 10040 has no read/write sides, so every url it names is in scope.
        assertEquals(RelayRole.ANY, assertions.relaySource?.role)
        assertTrue(assertions.urls.isEmpty(), "a relaySource stream carries no static urls")
    }
}
