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
            // Scans only: a verdict source reads the monitor's own kind-30166
            // records, which the monitor WRITES into this store — no stream
            // mirrors them, and none needs to.
            stream.dynamic!!.scanSources.forEach { source ->
                source.filter.kinds.orEmpty().forEach { kind ->
                    assertTrue(kind in mirrored, "stream '${stream.name}' scans kind $kind, which no stream mirrors")
                }
            }
        }
    }

    @Test
    fun `the monitor fans out over NIP-65 write relays`() {
        // Found by SHAPE, not by name — and in the MONITOR block now: relay
        // list parsing moved off the streams and onto the monitor's own
        // sources, whose verdicts the streams then select on. The shape checks
        // survive the move because they were never about which block the
        // source lives in.
        val sources = example.monitor!!.sources.filter { it.filter.kinds == listOf(10002) }
        assertTrue(sources.isNotEmpty(), "the monitor does not read NIP-65 lists")

        for (source in sources) {
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
    fun `the monitor also fans out over NIP-29 group hosts`() {
        // By shape again: a scan of kind 10009. The `group` tag is the one
        // relay list in the protocol that does not put the url at element 1 —
        // it is ["group", <id>, <relay url>, <name?>] — so the whole point of
        // this test is that the example says 2 and not 1. Reading element 1
        // would hand the fan-out a set of GROUP IDS to dial, which normalize
        // rejects one at a time and silently: no error, no relays, and a
        // `group:` search with nothing behind it.
        val hosts =
            example.monitor!!
                .sources
                .filter { it.filter.kinds == listOf(10009) }
                .map { "monitor" to it }
        assertTrue(hosts.isNotEmpty(), "the monitor does not read NIP-29 group lists")

        for ((stream, source) in hosts) {
            val select = source.selects.single()
            assertEquals(10009, select.kind, "the scan is narrowed to the group list kind")
            assertEquals("group", select.tag)
            assertEquals(2, select.index, "a `group` tag carries the host relay at element 2, after the id")
            assertTrue(select.where.isEmpty(), "a group tag has no marker to test — every entry names a host")
            assertTrue(
                select.bindings.isEmpty(),
                "the select is deliberately unbound: binding the id would ask each host only for the listed " +
                    "groups, and give up the tag projection for a paging scan over whole events",
            )
            assertTrue(
                10009 in example.streams.flatMap { it.filter.kinds.orEmpty() },
                "$stream scans kind 10009, so some stream has to mirror it",
            )
        }

        // The half that makes it worth doing: a host discovered this way is
        // only useful if something asks it for what a group actually holds.
        // NIP-29 posts are kinds 9 (chat) and 11 (thread) with replies in 1111,
        // and the group's own record is 39000 — the one the `group:` picker
        // resolves a name against.
        // The hosts the monitor certifies are only useful if a visit-mode
        // stream then asks them for what a group actually holds.
        assertTrue(
            example.streams.any {
                it.dynamic?.verdictSources?.isNotEmpty() == true &&
                    it.filter.kinds
                        .orEmpty()
                        .containsAll(listOf(9, 11, 1111, 39000))
            },
            "the monitor certifies group hosts but no verdict-built stream asks for group posts or the group record",
        )
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
        // A band per (relay, service) rather than per relay — the pool makes
        // one ask per bound author, structurally.
        // GATED on the monitor's verdicts: a 10040 is as writable as a 10002,
        // and at millions of provider lists the spammed dead urls in them must
        // cost the monitor one probe each — never this stream a dial and a
        // timeout per cycle forever.
        assertTrue(source.certified != null, "the assertions scan dials only relays the monitor certifies")
        // ...which only works if those urls EARN verdicts: the monitor must
        // read the same 10040 tags as candidates.
        val monitor10040 = example.monitor!!.sources.filter { it.filter.kinds == listOf(10040) }
        assertTrue(monitor10040.isNotEmpty(), "the assertions scan is gated on verdicts no monitor source would ever take")
        assertTrue(
            monitor10040.flatMap { it.selects }.map { it.tag }.containsAll(listOf("30382:rank", "30382:followers")),
            "the monitor reads the service tags the assertions stream scans",
        )
        assertTrue(
            monitor10040.flatMap { it.selects }.all { it.index == 2 },
            "the service tag carries the url at element 2 — reading 1 would probe provider pubkeys as urls",
        )
    }

    @Test
    fun `every dynamic stream states how often its list is re-derived`() {
        // A dead relay is caught by the client's idle timeout in seconds; the
        // one clock the config owes discovery is how often a scan's list is
        // re-read from the store.
        example.dynamicStreams().forEach { stream ->
            assertTrue(stream.dynamic!!.refreshSeconds > 0, "'${stream.name}' needs a refresh period")
        }
    }
}
