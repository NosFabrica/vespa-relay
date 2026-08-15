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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `presence { }` as a SCOPE over a `relaySource`, and the combinations it
 * refuses.
 *
 * Most of what is here is refusals, and each of them is a configuration that
 * would have RUN — mirroring the wrong thing, or the right thing at a cost
 * nobody signed up for — rather than failed. The loader is the one place a human
 * types this, so it is the one place those can be named.
 */
class PresenceStreamConfigTest {
    private val outbox =
        """{ select = [ { kind = 10002, tag = "r", marker = "write", authors = "pubkey" } ], filter = { "kinds": [10002] } }"""

    private val inbox =
        """{ select = [ { kind = 10002, tag = "r", marker = "read", "#p" = "pubkey" } ], filter = { "kinds": [10002] } }"""

    private fun parse(body: String) = RouterConfigLoader.parse("streams { $body }")

    private fun stream(body: String) = parse(body).streams.single()

    private fun normalized(url: String) = RelayUrlNormalizer.normalizeOrNull(url)!!

    @Test
    fun `a presence stream is an ordinary relaySource, paced by presence`() {
        val s =
            stream(
                """
                authedContent {
                    dir      = "down"
                    sync     = "fetch"
                    filter   = { "kinds": [1, 1111, 30023] }
                    presence = { pollSeconds = 45, concurrency = 6, maxRelaysPerReader = 4 }
                    exclude  = [ "wss://ours.example" ]
                    relaySource = [ $outbox, $inbox ]
                }
                """.trimIndent(),
            )

        val presence = requireNotNull(s.presence)
        assertEquals(45L, presence.pollSeconds)
        assertEquals(6, presence.concurrency)
        assertEquals(4, presence.maxRelaysPerReader)
        // The sources are the ordinary ones, read by the ordinary code.
        assertEquals(2, s.dynamic!!.sources.size)
        assertEquals(
            listOf(Slot.EventPubkey),
            s.dynamic!!
                .sources[0]
                .selects
                .single()
                .bindings.values
                .toList(),
        )
        assertEquals(
            setOf("#p"),
            s.dynamic!!
                .sources[1]
                .selects
                .single()
                .bindings.keys,
        )
        assertTrue(normalized("wss://ours.example/") in s.dynamic!!.exclude)
    }

    @Test
    fun `a presence stream is not ALSO run as a dynamic one`() {
        // It carries a relaySource, so `dynamicStreams()` would take it — and
        // `DynamicSync` would walk every stored relay list through selects
        // written to be scoped to one reader, fanning the `authors` binding out
        // over the whole corpus.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    corpus  { filter = { "kinds": [0] }, relaySource = [ $outbox ] }
                    authed  { filter = { "kinds": [1] }, sync = "live", presence = { }, relaySource = [ $outbox ] }
                }
                """.trimIndent(),
            )

        assertEquals(listOf("corpus"), cfg.dynamicStreams().map { it.name })
        assertEquals(listOf("authed"), cfg.presenceStreams().map { it.name })
    }

    @Test
    fun `the defaults are the ones the class documents`() {
        val presence =
            requireNotNull(stream("""a { filter = { "kinds": [1] }, sync = "live", presence = { }, relaySource = [ $outbox ] }""").presence)

        assertEquals(PresenceConfig.DEFAULT_POLL_SECONDS, presence.pollSeconds)
        assertEquals(PresenceConfig.DEFAULT_MAX_RELAYS_PER_READER, presence.maxRelaysPerReader)
    }

    @Test
    fun `the poll interval has a floor, so a typo cannot turn the feed into load`() {
        val presence =
            requireNotNull(
                stream(
                    """a { filter = { "kinds": [1] }, sync = "live", presence = { pollSeconds = 0 }, relaySource = [ $outbox ] }""",
                ).presence,
            )

        assertEquals(PresenceConfig.MIN_POLL_SECONDS, presence.pollSeconds)
    }

    @Test
    fun `presence with no relaySource is refused, and the message shows one`() {
        // Presence scopes a source; it is not one. A stream with only the block
        // names no tag to read a url out of.
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { filter = { "kinds": [1] }, sync = "live", presence = { } }""")
            }
        assertTrue(e.message!!.contains("SCOPES a relaySource"))
        assertTrue(e.message!!.contains("""authors = "pubkey""""), "the message carries a source that works")
    }

    @Test
    fun `the rotation's knobs are refused rather than left inert beside presence`() {
        // A `refreshSeconds = 21600` copied down from the stream above would sit
        // there meaning nothing while the operator believed they had set the
        // period.
        for (knob in PresenceConfig.ROTATION_ONLY) {
            val e =
                assertFailsWith<IllegalArgumentException>("$knob should be refused") {
                    parse("""a { filter = { "kinds": [1] }, sync = "live", presence = { }, $knob = 60, relaySource = [ $outbox ] }""")
                }
            assertTrue(e.message!!.contains(knob), "the message names $knob")
            assertTrue(e.message!!.contains("pollSeconds"), "…and says what paces it instead")
        }
    }

    @Test
    fun `a presence source may not pin its own authors`() {
        // Presence SETS that, to the reader it is resolving. Left alone it would
        // be silently overwritten — the stream would run, discover the
        // right-looking relays, and resolve them for somebody else.
        val pinned =
            """{ select = [ { kind = 10002, tag = "r" } ], filter = { "kinds": [10002], "authors": ["${"a".repeat(64)}"] } }"""
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { filter = { "kinds": [1] }, sync = "live", presence = { }, relaySource = [ $pinned ] }""")
            }

        assertTrue(e.message!!.contains("presence sets that itself"))
    }

    @Test
    fun `negentropy on a presence stream is refused, with the cost spelled out`() {
        // A reconcile snapshots OUR side of the filter, and a presence filter is
        // per reader — so this would be one full store walk per signed-in person
        // per poll. Not a slow version of the feature; a different one.
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { filter = { "kinds": [1] }, sync = "negentropy", presence = { }, relaySource = [ $outbox ] }""")
            }
        assertTrue(e.message!!.contains("once per signed-in reader"))
    }

    @Test
    fun `auto is refused too, because what it would decide is negentropy`() {
        assertFailsWith<IllegalArgumentException> {
            parse("""a { filter = { "kinds": [1] }, presence = { }, relaySource = [ $outbox ] }""")
        }
    }

    @Test
    fun `a relaySource still only pulls down, presence or not`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { dir = "up", filter = { "kinds": [1] }, sync = "live", presence = { }, relaySource = [ $outbox ] }""")
            }
        assertTrue(e.message!!.contains("only pulls down"))
    }

    // ---- the mode itself, which is usable without presence ------------------

    @Test
    fun `a live static upstream is subscribed and never backfilled`() {
        // The mode is not presence-only: `live` on a configured upstream means
        // "tail it, walk no history", which used to need a `since` on the filter
        // that then also bounded the tail.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    tailed { dir = "down", sync = "live", filter = { "kinds": [1] }, urls = ["wss://a.example"] }
                    filled { dir = "down", sync = "fetch", filter = { "kinds": [1] }, urls = ["wss://b.example"] }
                }
                """.trimIndent(),
            )

        assertEquals(setOf("tailed", "filled"), cfg.downUpstreams().map { it.streamName }.toSet())
        assertEquals(listOf("filled"), cfg.backfillUpstreams().map { it.streamName })
    }

    @Test
    fun `the mode list in the parse error is the mode list`() {
        // The message enumerates the enum rather than a hand-written list, so a
        // fifth mode cannot leave it stale — this is what proves it.
        val e = assertFailsWith<IllegalStateException> { SyncMode.parse("nope") }

        SyncMode.entries.forEach { assertTrue(e.message!!.contains(it.wire), "the error names ${it.wire}") }
    }
}
