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
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The third way a stream gets its relays, and the combinations it refuses.
 *
 * Most of what is here is refusals, and each of them is a configuration that
 * would have RUN — mirroring the wrong thing, or the right thing at a cost
 * nobody signed up for — rather than failed. The loader is the one place a
 * human types this, so it is the one place those can be named.
 */
class PresenceStreamConfigTest {
    private fun parse(body: String) = RouterConfigLoader.parse("streams { $body }")

    /** The normalizer's own spelling, so an exclude entry is compared the way the router compares it. */
    private fun normalized(url: String) = RelayUrlNormalizer.normalizeOrNull(url)!!

    private fun stream(body: String) = parse(body).streams.single()

    @Test
    fun `a presence stream reads its source, its pacing and its bounds`() {
        val s =
            stream(
                """
                authedOutbox {
                    dir      = "down"
                    sync     = "fetch"
                    filter   = { "kinds": [1, 30023] }
                    presence = {
                        source             = "outbox"
                        pollSeconds        = 45
                        concurrency        = 6
                        maxRelaysPerReader = 4
                        exclude            = [ "wss://ours.example" ]
                    }
                }
                """.trimIndent(),
            )

        val presence = requireNotNull(s.presence)
        assertEquals(PresenceSource.OUTBOX, presence.source)
        assertEquals(45L, presence.pollSeconds)
        assertEquals(6, presence.concurrency)
        assertEquals(4, presence.maxRelaysPerReader)
        assertTrue(normalized("wss://ours.example/") in presence.exclude)
        assertEquals(SyncMode.FETCH, s.sync)
        assertTrue(s.urls.isEmpty())
        assertNull(s.dynamic)
    }

    @Test
    fun `the defaults are the ones the class documents`() {
        val presence =
            requireNotNull(
                stream("""a { filter = { "kinds": [30382] }, sync = "live", presence = { source = "scores" } }""").presence,
            )

        assertEquals(PresenceSource.SCORES, presence.source)
        assertEquals(PresenceConfig.DEFAULT_POLL_SECONDS, presence.pollSeconds)
        assertEquals(PresenceConfig.DEFAULT_MAX_RELAYS_PER_READER, presence.maxRelaysPerReader)
    }

    @Test
    fun `the poll interval has a floor, so a typo cannot turn the feed into load`() {
        val presence =
            requireNotNull(
                stream("""a { filter = { "kinds": [1] }, sync = "live", presence = { source = "outbox", pollSeconds = 0 } }""").presence,
            )

        assertEquals(PresenceConfig.MIN_POLL_SECONDS, presence.pollSeconds)
    }

    @Test
    fun `a presence block with no source is refused rather than guessed`() {
        // The two sources ask completely different questions of completely
        // different relays. A stream that guessed would mirror the wrong one
        // perfectly quietly.
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { filter = { "kinds": [1] }, sync = "live", presence = { } }""")
            }
        assertTrue(e.message!!.contains("no `source`"))
    }

    @Test
    fun `an unknown source names the ones that exist`() {
        val e =
            assertFailsWith<IllegalStateException> {
                parse("""a { filter = { "kinds": [1] }, sync = "live", presence = { source = "friends" } }""")
            }
        assertTrue(e.message!!.contains("outbox"))
        assertTrue(e.message!!.contains("scores"))
    }

    @Test
    fun `negentropy on a presence stream is refused, with the cost spelled out`() {
        // A reconcile snapshots OUR side of the filter, and a presence filter is
        // per reader — so this would be one full store walk per signed-in person
        // per poll. Not a slow version of the feature; a different one.
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { filter = { "kinds": [1] }, sync = "negentropy", presence = { source = "outbox" } }""")
            }
        assertTrue(e.message!!.contains("once per signed-in reader"))
    }

    @Test
    fun `auto is refused too, because what it would decide is negentropy`() {
        assertFailsWith<IllegalArgumentException> {
            parse("""a { filter = { "kinds": [1] }, presence = { source = "outbox" } }""")
        }
    }

    @Test
    fun `presence cannot share a stream with urls or a relaySource`() {
        // `urls` and `relaySource` may share one name — each half gets its own
        // pass — and that rests on both walking the same filter. A presence
        // stream narrows per reader, so sharing a name would put two different
        // questions under one band key.
        val withUrls =
            assertFailsWith<IllegalArgumentException> {
                parse(
                    """a { filter = { "kinds": [1] }, sync = "live", urls = ["wss://a.example"], presence = { source = "outbox" } }""",
                )
            }
        assertTrue(withUrls.message!!.contains("per signed-in reader"))

        assertFailsWith<IllegalArgumentException> {
            parse(
                """
                a {
                    filter = { "kinds": [1] }
                    sync = "live"
                    presence = { source = "outbox" }
                    relaySource = [ { select = [ { kind = 10002, tag = "r" } ], filter = { "kinds": [10002] } } ]
                }
                """.trimIndent(),
            )
        }
    }

    @Test
    fun `presence only pulls down`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                parse("""a { dir = "up", filter = { "kinds": [1] }, sync = "live", presence = { source = "outbox" } }""")
            }
        assertTrue(e.message!!.contains("only pulls down"))
    }

    @Test
    fun `a stream with no source of relays at all names all three`() {
        val e = assertFailsWith<IllegalArgumentException> { parse("""a { filter = { "kinds": [1] } }""") }

        assertTrue(e.message!!.contains("urls"))
        assertTrue(e.message!!.contains("relaySource"))
        assertTrue(e.message!!.contains("presence"))
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
