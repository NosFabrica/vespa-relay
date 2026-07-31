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
    fun `a static stream seeds the store the dynamic scans read from`() {
        // A relaySource stream reads its relays out of events we already hold, so
        // something with hand-written urls has to put the first ones there. Only
        // static streams can: a store with no relay lists gives every dynamic
        // stream an empty fan-out, forever.
        val static = example.streams.filter { it.dynamic == null }
        assertTrue(static.isNotEmpty(), "the example needs at least one statically-addressed stream")
        assertTrue(static.any { it.urls.isNotEmpty() }, "a static stream must name real urls")
        assertEquals(
            example
                .downUpstreams()
                .map { it.streamName }
                .distinct()
                .sorted(),
            static
                .filter { it.urls.isNotEmpty() }
                .map { it.name }
                .distinct()
                .sorted(),
            "downUpstreams() is the static streams only — a dynamic one resolves its relays at run time",
        )
    }

    @Test
    fun `every dynamic scan reads a kind some stream actually mirrors`() {
        // The chain in the example is static(10002) -> outbox(10040) -> assertions.
        // A scan for a kind nothing mirrors is a stream that can never fan out,
        // and it fails silently — there is no error, just no relays.
        val mirrored = example.streams.flatMap { it.filter.kinds.orEmpty() }.toSet()
        example.dynamicStreams().forEach { stream ->
            stream.dynamic!!.sources.forEach { source ->
                source.filter.kinds.orEmpty().forEach { kind ->
                    assertTrue(kind in mirrored, "stream '${stream.name}' scans kind $kind, which no stream mirrors")
                }
            }
        }
    }

    @Test
    fun `the outbox stream fans out over NIP-65 write relays`() {
        val outbox = example.dynamicStreams().first { it.name == "dataViaOutbox" }
        assertTrue(outbox.urls.isEmpty(), "a relaySource stream carries no static urls")

        val source = outbox.dynamic!!.sources.single()
        assertEquals(listOf(10002), source.filter.kinds, "the scan reads NIP-65 relay lists")

        val nip65 = source.selects.single()
        assertEquals(10002, nip65.kind)
        assertEquals("r", nip65.tag)
        // 10002 puts the url first and its marker after it; only the write side
        // is where a user's own events land, which is what an outbox mirror wants.
        assertEquals(1, nip65.index)
        assertEquals(RelayRole.WRITE, nip65.role)
    }

    @Test
    fun `the assertions stream names the NIP-85 services it wants`() {
        val assertions = example.dynamicStreams().first { it.name == "assertions" }
        val source = assertions.dynamic!!.sources.single()
        assertTrue(assertions.urls.isEmpty(), "a relaySource stream carries no static urls")
        assertEquals(listOf(10040), source.filter.kinds)
        // Every select is a service tag with the url AFTER the provider pubkey.
        assertTrue(source.selects.all { it.index == 2 })
        assertTrue(source.selects.any { it.tag == "30382:rank" })
        assertTrue(source.selects.any { it.tag == "30382:followers" })
        // Mirroring 30382 is the point: the scores those services publish.
        assertTrue(assertions.filter.kinds?.contains(30382) == true)
    }

    @Test
    fun `a dynamic cycle paces its fan-out without deadlining a relay`() {
        // A dead relay is caught by the client's idle timeout in seconds, so what
        // the example has to state is how the cycle repeats and how wide it runs —
        // NOT a wall clock, which could only ever cut off a relay still sending.
        example.dynamicStreams().forEach { stream ->
            val d = stream.dynamic!!
            assertTrue(d.refreshSeconds > 0, "'${stream.name}' needs a refresh period")
            assertTrue(d.concurrency > 0, "'${stream.name}' needs a fan-out width")
        }
    }
}
