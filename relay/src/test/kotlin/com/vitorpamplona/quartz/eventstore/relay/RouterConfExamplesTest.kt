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
import kotlin.test.assertNotNull
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
    fun `the outbox stream merges relay lists, monitor reports and hints`() {
        val outbox = example.dynamicStreams().first { it.name == "outbox" }
        val sources = outbox.dynamic!!.sources
        assertTrue(outbox.urls.isEmpty(), "a relaySource stream carries no static urls")

        // One scan covers every relay-list kind; the selects sort them out.
        val lists = sources.first { it.filter.kinds?.contains(10002) == true }
        val nip65 = lists.selects.first { it.kind == 10002 }
        assertEquals("r", nip65.tag)
        assertEquals(RelayRole.WRITE, nip65.role)
        assertTrue(lists.selects.any { it.kind == 30166 && it.tag == "d" }, "NIP-66 monitors")
        // The ["relay", "<url>"] family rides on one kind-less select.
        assertTrue(lists.selects.any { it.kind == null && it.tag == "relay" })
        assertTrue(lists.filter.kinds!!.containsAll(listOf(10050, 30002)))

        // Relay hints: a regular kind, so its scan must be bounded, and the urls
        // sit after the id/pubkey.
        val hints = sources.first { it.filter.kinds == listOf(1) }
        assertNotNull(hints.filter.limit, "a kind-1 scan has to bound itself")
        assertTrue(hints.selects.isNotEmpty())
        assertTrue(hints.selects.all { it.index == 2 })
        assertTrue(hints.selects.any { it.tag == "e" })
    }

    @Test
    fun `the assertions stream names the NIP-85 services it wants`() {
        val assertions = example.dynamicStreams().first { it.name == "assertions" }
        val source = assertions.dynamic!!.sources.single()
        assertTrue(assertions.urls.isEmpty(), "a relaySource stream carries no static urls")
        assertEquals(listOf(10040), source.filter.kinds)
        // Every select is a service tag with the url after the provider pubkey.
        assertTrue(source.selects.all { it.index == 2 })
        assertTrue(source.selects.any { it.tag == "30382:rank" })
    }
}
