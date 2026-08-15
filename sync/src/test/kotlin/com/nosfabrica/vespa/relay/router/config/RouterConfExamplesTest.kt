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
    private val exampleText: String =
        requireNotNull(
            listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile },
        ) { "missing router.conf.example" }.readText()

    private val example: RouterConfig = RouterConfigLoader.parse(exampleText)

    /**
     * The example's `presence` streams, uncommented and parsed.
     *
     * They ship commented out because they need two settings that have no
     * defaults, on two services — but a commented-out example is prose, and
     * prose does not parse. This is what makes them documentation that is
     * checked: an operator uncomments those exact lines, and a select the
     * loader would refuse (a bound slot that moved, a knob presence rejects, a
     * `marker` spelling) fails here rather than at their boot.
     *
     * Read from the first block's own name to the end of the file and stripped
     * of the `    #` this file indents comments with — literal on purpose, so a
     * block that stops being commented the same way fails loudly rather than
     * being silently skipped.
     */
    private val presenceExample: RouterConfig =
        exampleText
            .lines()
            .dropWhile { !it.contains("# $FIRST_PRESENCE_STREAM {") }
            .filter { it.startsWith("    #") }
            .map { it.removePrefix("    #").removePrefix(" ") }
            .joinToString("\n")
            .also { require(it.contains("$FIRST_PRESENCE_STREAM {")) { "the example no longer carries a commented `presence` stream" } }
            .let { RouterConfigLoader.parse("streams {\n$it\n}") }

    @Test
    fun `the presence streams in the example parse when uncommented`() {
        val presence = presenceExample.presenceStreams()

        // As a SET: HOCON hands back its object keys in its own order, which
        // is not the file's, and pinning that order would be pinning the
        // library rather than the example.
        assertEquals(setOf(FIRST_PRESENCE_STREAM, "authedScores"), presence.map { it.name }.toSet())
        // Both halves of the outbox model, which is the pair the block is for:
        // their own posts from their write relays, and mentions of them from
        // their read relays. A binding that moved slot would still parse and
        // would ask an inbox for the reader's own events, which returns nothing.
        val content = presence.first { it.name == FIRST_PRESENCE_STREAM }
        assertEquals(
            listOf(mapOf("authors" to Slot.EventPubkey), mapOf("#p" to Slot.EventPubkey)),
            content.dynamic!!.sources.map { it.selects.single().bindings },
        )
        // It mirrors the same corpus the wide fan-out does, reached from the
        // other end — so a kind list that drifted from contentViaOutbox's is a
        // presence stream quietly holding less for the person who is waiting.
        val wide = example.streams.first { it.name == "contentViaOutbox" }
        assertEquals(wide.filter.kinds, content.filter.kinds, "the presence stream mirrors contentViaOutbox's kinds")
        // …and the scores one is the assertions select, scoped to one reader.
        val scores = presence.first { it.name == "authedScores" }
        assertEquals(listOf(30382), scores.filter.kinds)
        assertEquals(
            listOf(10040),
            scores.dynamic!!
                .sources
                .single()
                .filter.kinds,
        )
    }

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
            // One source PER KIND of relay list, not one source. An outbox
            // stream reads NIP-65 write relays and NIP-29 group hosts, and they
            // are separate entries because they scan different kinds — this
            // used to be `.single()`, which pinned "there is exactly one way to
            // find a relay" rather than anything about NIP-65.
            val source = outbox.dynamic!!.sources.single { it.filter.kinds == listOf(10002) }

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
    fun `the outbox streams also fan out over NIP-29 group hosts`() {
        // By shape again: a scan of kind 10009. The `group` tag is the one
        // relay list in the protocol that does not put the url at element 1 —
        // it is ["group", <id>, <relay url>, <name?>] — so the whole point of
        // this test is that the example says 2 and not 1. Reading element 1
        // would hand the fan-out a set of GROUP IDS to dial, which normalize
        // rejects one at a time and silently: no error, no relays, and a
        // `group:` search with nothing behind it.
        val hosts =
            example.dynamicStreams().mapNotNull { s ->
                s.dynamic!!
                    .sources
                    .singleOrNull { it.filter.kinds == listOf(10009) }
                    ?.let { s to it }
            }
        assertTrue(hosts.isNotEmpty(), "no stream discovers relays from NIP-29 group lists")

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
                "stream '${stream.name}' scans kind 10009, so some stream has to mirror it",
            )
        }

        // The half that makes it worth doing: a host discovered this way is
        // only useful if something asks it for what a group actually holds.
        // NIP-29 posts are kinds 9 (chat) and 11 (thread) with replies in 1111,
        // and the group's own record is 39000 — the one the `group:` picker
        // resolves a name against.
        val content = example.streams.filter { it.dynamic?.sources?.any { s -> s.filter.kinds == listOf(10009) } == true }
        assertTrue(
            content.any {
                it.filter.kinds
                    .orEmpty()
                    .containsAll(listOf(9, 11, 1111, 39000))
            },
            "a stream dials group hosts but none of them asks for group posts or the group record",
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

    private companion object {
        const val FIRST_PRESENCE_STREAM = "authedContentViaOutbox"
    }
}
