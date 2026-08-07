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
package com.nosfabrica.vespa.relay.router.config

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
    fun `the outbox streams fan out over NIP-65 write relays`() {
        // Found by SHAPE, not by name. There is more than one outbox stream now
        // (profiles and content are split so they can sync differently), and
        // they have been renamed once already — a test that pins the name fails
        // on a rename while the thing it checks is still correct.
        val outboxes =
            example.dynamicStreams().filter { s ->
                s.dynamic!!.sources.any { it.filter.kinds == listOf(10002) }
            }
        assertTrue(outboxes.isNotEmpty(), "no stream discovers relays from NIP-65 lists")

        for (outbox in outboxes) {
            assertTrue(outbox.urls.isEmpty(), "a relaySource stream carries no static urls")
            val source = outbox.dynamic!!.sources.single()
            assertEquals(listOf(10002), source.filter.kinds, "the scan reads NIP-65 relay lists")

            val nip65 = source.selects.single()
            assertEquals(10002, nip65.kind)
            assertEquals("r", nip65.tag)
            // 10002 puts the url first and its marker after it; only the write side
            // is where a user's own events land, which is what an outbox mirror wants.
            // The example says `marker = "write"`, which is sugar for exactly this:
            // marked write, marked empty, or too short to carry a marker at all.
            assertEquals(1, nip65.index)
            assertEquals(
                listOf(
                    TagCondition(index = 2, equals = "write"),
                    TagCondition(index = 2, equals = ""),
                    TagCondition(maxSize = 2),
                ),
                nip65.where,
            )
        }
    }

    @Test
    fun `a stream that mirrors content mirrors the retractions too`() {
        // By SHAPE again: a stream carrying kind 1 is mirroring what people
        // write, and one that takes the notes without the kind 5 (NIP-09) and
        // kind 62 (NIP-62) that retract them goes on serving what its authors
        // deleted. The store enforces both at insert, so mirroring them is the
        // whole mechanism — and the STORED request is what the next cycle's
        // re-download is checked against, without which the erase is undone on
        // the following walk.
        val content = example.streams.filter { it.filter.kinds?.contains(1) == true }
        assertTrue(content.isNotEmpty(), "the example mirrors no user-written content at all")
        content.forEach { stream ->
            val kinds = stream.filter.kinds.orEmpty()
            assertTrue(5 in kinds, "stream '${stream.name}' mirrors notes but not the kind 5 that deletes them")
            assertTrue(62 in kinds, "stream '${stream.name}' mirrors notes but not the kind 62 that vanishes their author")
        }
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
        // ...and only from the services the SAME tag paired with each relay. The
        // service sits at slot 1 of the tag whose slot 2 named the url, so the
        // two travel together; binding it from anywhere else would be the cross
        // product wearing the right shape.
        assertTrue(
            source.selects.all { it.bindings["authors"] == Slot.OfTag(1) },
            "each service tag binds its own provider as the authors to ask for",
        )
        // A band per (relay, service) rather than per relay, so a new provider
        // list does not invalidate the ones already walked.
        assertEquals(1, assertions.dynamic!!.authorsPerLeg)
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
