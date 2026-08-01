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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RouterConfigTest {
    /** What `marker = "write"` desugars to on a url-at-1 select. */
    private val nip65Write =
        listOf(
            TagCondition(index = 2, equals = "write"),
            TagCondition(index = 2, equals = ""),
            TagCondition(maxSize = 2),
        )

    // The exact strfry-style routerConfigOverride an operator would drop in.
    private val streamsConfig =
        """
        connectionTimeout = 20

        streams {
            popular {
                dir = "down"
                filter = { "kinds": [0,3,5,1984,10000,30000] }
                urls = [
                    "wss://relay.primal.net",
                    "wss://relay.damus.io",
                    "wss://purplepag.es",
                    "wss://nos.lol",
                    "wss://nostr-pub.wellorder.net"
                ]
            }
            mirrors {
                dir = "down"
                filter = { "kinds": [0,3,5,1984,10000,30000] }
                urls = [
                    "wss://brainstorm.nostr1.com",
                    "wss://primus.nostr1.com",
                    "wss://profiles.nostr1.com",
                    "wss://indexer.coracle.social",
                    "wss://user.kindpag.es",
                    "wss://directory.yabu.me",
                    "wss://relay.ditto.pub"
                ]
            }
        }
        """.trimIndent()

    @Test
    fun `parses the strfry streams config`() {
        val cfg = RouterConfigLoader.parse(streamsConfig)

        assertEquals(20L, cfg.connectionTimeoutSec)
        assertEquals(2, cfg.streams.size)

        val popular = cfg.streams.first { it.name == "popular" }
        val mirrors = cfg.streams.first { it.name == "mirrors" }

        assertEquals(MirrorDirection.DOWN, popular.dir)
        assertEquals(listOf(0, 3, 5, 1984, 10000, 30000), popular.filter.kinds)
        assertEquals(5, popular.urls.size)
        assertEquals(7, mirrors.urls.size)
        assertEquals(false, popular.trusted)
        // No `since`: NIP-01 reads that as unbounded, so the stream reaches the
        // upstream's whole history rather than only its live tail.
        assertEquals(null, popular.filter.since)
    }

    @Test
    fun `expands to one upstream per url, all down, nothing skipped`() {
        val cfg = RouterConfigLoader.parse(streamsConfig)
        val ups = cfg.downUpstreams()

        assertEquals(12, ups.size)
        assertTrue(ups.all { it.filter.kinds == listOf(0, 3, 5, 1984, 10000, 30000) })
        // All streams are down, so nothing to push up.
        assertTrue(cfg.upUpstreams().isEmpty())
        // Every configured url normalized and survived.
        assertTrue(ups.any { it.url.url.contains("relay.primal.net") })
        assertTrue(ups.any { it.url.url.contains("directory.yabu.me") })
    }

    @Test
    fun `down up and both expand into the right direction sets`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    pushUp {
                        dir = "up"
                        filter = { "kinds": [1] }
                        urls = ["wss://a.example"]
                    }
                    twoWay {
                        dir = "both"
                        filter = { "kinds": [1] }
                        urls = ["wss://b.example"]
                    }
                    pullDown {
                        dir = "down"
                        filter = { "kinds": [1] }
                        urls = ["wss://c.example"]
                    }
                }
                """.trimIndent(),
            )

        val downUrls = cfg.downUpstreams().map { it.url.url }
        val upUrls = cfg.upUpstreams().map { it.url.url }

        // both counts in each direction; up-only and down-only in one each.
        assertTrue(downUrls.any { it.contains("b.example") }, "both should mirror down")
        assertTrue(downUrls.any { it.contains("c.example") })
        assertTrue(downUrls.none { it.contains("a.example") }, "pure up should not mirror down")

        assertTrue(upUrls.any { it.contains("b.example") }, "both should mirror up")
        assertTrue(upUrls.any { it.contains("a.example") })
        assertTrue(upUrls.none { it.contains("c.example") }, "pure down should not mirror up")
    }

    @Test
    fun `default connection timeout and optional fields`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        filter = { "kinds": [0] }
                        urls = ["wss://x.example"]
                        trusted = true
                    }
                }
                """.trimIndent(),
            )
        val s = cfg.streams.single()
        assertEquals(20L, cfg.connectionTimeoutSec) // default when unset
        assertEquals(MirrorDirection.DOWN, s.dir) // default dir
        assertEquals(true, s.trusted)
    }

    @Test
    fun `a filter's since and until bound how far the stream reaches`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    bounded {
                        filter = { "kinds": [0], "since": 1700000000, "until": 1800000000 }
                        urls = ["wss://x.example"]
                    }
                    unbounded {
                        filter = { "kinds": [0] }
                        urls = ["wss://y.example"]
                    }
                }
                """.trimIndent(),
            )
        val bounded = cfg.streams.first { it.name == "bounded" }
        assertEquals(1_700_000_000L, bounded.filter.since)
        assertEquals(1_800_000_000L, bounded.filter.until)

        // The absent case is the one that matters: nothing substitutes a window
        // for it, so the stream asks the upstream for everything it has.
        val unbounded = cfg.streams.first { it.name == "unbounded" }
        assertEquals(null, unbounded.filter.since)
        assertEquals(null, unbounded.filter.until)
    }

    @Test
    fun `parses authors, tag, and search filter fields`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir = "down"
                        filter = { "kinds": [1], "authors": ["abc"], "#t": ["nostr","bitcoin"], "search": "hello" }
                        urls = ["wss://x.example"]
                    }
                }
                """.trimIndent(),
            )
        val f = cfg.streams.single().filter
        assertEquals(listOf(1), f.kinds)
        assertEquals(listOf("abc"), f.authors)
        assertEquals("hello", f.search)
        assertEquals(listOf("nostr", "bitcoin"), f.tags?.get("t"))
    }

    @Test
    fun `parses a dynamic stream with a list of relay sources`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    outbox {
                        dir            = "down"
                        filter         = { "kinds": [0, 3, 10002] }
                        refreshSeconds     = 3600
                        concurrency        = 4
                        exclude            = [ "wss://skip.example" ]
                        relaySource = [
                            {
                                select = [
                                    { kind = 10002, tag = "r", marker = "write" }
                                    { kind = 10040, tag = "30382:rank", index = 2 }
                                    { tag = "relay" }
                                ]
                                filter = { "kinds": [10002, 10040, 10050] }
                            }
                            {
                                select = [ { tag = "e", index = 2 } ]
                                filter = { "kinds": [1], "limit": 1000, "authors": ["abc"] }
                            }
                        ]
                    }
                    assertions {
                        filter = { "kinds": [30382] }
                        relaySource = [
                            {
                                filter = { "kinds": [10040] }
                                select = [ { index = 2 } ]
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )

        val outbox = cfg.streams.first { it.name == "outbox" }.dynamic!!
        assertEquals(3600L, outbox.refreshSeconds)
        assertEquals(4, outbox.concurrency)
        assertEquals(listOf("wss://skip.example/"), outbox.exclude.map { it.url })
        assertEquals(2, outbox.sources.size)

        // One scan over three kinds, three selects sorting them out.
        val lists = outbox.sources[0]
        assertEquals(listOf(10002, 10040, 10050), lists.filter.kinds)
        assertEquals(3, lists.selects.size)

        // Defaults: index 1, no where filter, and a kind-less select applies to all.
        val nip65 = lists.selects[0]
        assertEquals(10002, nip65.kind)
        assertEquals("r", nip65.tag)
        assertEquals(1, nip65.index)
        assertEquals(nip65Write, nip65.where)

        // A NIP-85 service tag, named exactly, with the url after the pubkey.
        val provider = lists.selects[1]
        assertEquals("30382:rank", provider.tag)
        assertEquals(2, provider.index)
        assertTrue(provider.where.isEmpty())

        assertNull(lists.selects[2].kind, "no kind = every event the filter collected")

        // The second scan is a bounded sweep of a regular kind, filter fields and all.
        val hints = outbox.sources[1]
        assertEquals(listOf(1), hints.filter.kinds)
        assertEquals(1000, hints.filter.limit)
        assertEquals(listOf("abc"), hints.filter.authors)

        val assertions = cfg.streams.first { it.name == "assertions" }.dynamic!!
        assertNull(
            assertions.sources
                .single()
                .selects
                .single()
                .tag,
            "no tag = every tag in the event",
        )
        assertEquals(21_600L, assertions.refreshSeconds) // the built-in defaults

        // Dynamic streams have no static urls, so they are not down/up upstreams.
        assertEquals(2, cfg.dynamicStreams().size)
        assertTrue(cfg.downUpstreams().isEmpty())
        assertTrue(cfg.upUpstreams().isEmpty())
    }

    /** A one-source `relaySource` list, with [filter] as the scan. */
    private fun sourced(
        filter: String,
        select: String = """{ tag = "r" }""",
    ) = stream(
        """
        relaySource = [
            {
                select = [ $select ]
                filter = $filter
            }
        ]
        """.trimIndent(),
    )

    @Test
    fun `a regular kind must bound its scan, a replaceable one need not`() {
        // Scanning all of kind 1 would load every note in the store.
        assertFailsWith<IllegalArgumentException> { RouterConfigLoader.parse(sourced("""{ "kinds": [1] }""")) }
        // Any of limit / since / authors narrows it enough to be safe.
        RouterConfigLoader.parse(sourced("""{ "kinds": [1], "limit": 1000 }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [1], "since": 1750000000 }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [1], "authors": ["abc"] }"""))
        // `until` alone doesn't: it caps the top and leaves all of history below.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [1], "until": 1750000000 }"""))
        }
        // Replaceable and addressable kinds are one event per author — safe whole.
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002] }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [30166] }"""))
    }

    @Test
    fun `index 0 is the tag name, never the url`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [10002] }""", """{ tag = "r", index = 0 }"""))
        }
    }

    @Test
    fun `where entries parse with their fields`() {
        val cfg =
            RouterConfigLoader.parse(
                sourced(
                    """{ "kinds": [1], "limit": 1000 }""",
                    """
                    {
                        tag = "e"
                        index = 2
                        where = [
                            { index = 3, equals = "root" }
                            { index = 3, equals = "reply", maxSize = 4 }
                            { minSize = 4 }
                            { maxSize = 3 }
                        ]
                    }
                    """.trimIndent(),
                ),
            )
        val select =
            cfg
                .dynamicStreams()
                .single()
                .dynamic!!
                .sources
                .single()
                .selects
                .single()
        assertEquals(
            listOf(
                TagCondition(index = 3, equals = "root"),
                TagCondition(index = 3, equals = "reply", maxSize = 4),
                TagCondition(minSize = 4),
                TagCondition(maxSize = 3),
            ),
            select.where,
        )
    }

    @Test
    fun `marker is sugar for the where that spells the NIP-65 rule`() {
        fun whereOf(select: String) =
            RouterConfigLoader
                .parse(sourced("""{ "kinds": [10002] }""", select))
                .dynamicStreams()
                .single()
                .dynamic!!
                .sources
                .single()
                .selects
                .single()
                .where

        assertEquals(nip65Write, whereOf("""{ tag = "r", marker = "write" }"""))
        // The desugar follows the url, so hint-style tags get the slot after theirs.
        assertEquals(
            listOf(
                TagCondition(index = 3, equals = "read"),
                TagCondition(index = 3, equals = ""),
                TagCondition(maxSize = 3),
            ),
            whereOf("""{ tag = "e", index = 2, marker = "read" }"""),
        )
        // `any` keeps everything, which is what no conditions means.
        assertEquals(emptyList(), whereOf("""{ tag = "r", marker = "any" }"""))
        assertFailsWith<IllegalStateException> { whereOf("""{ tag = "r", marker = "nonsense" }""") }
    }

    @Test
    fun `a where entry must be satisfiable and whole`() {
        fun parse(select: String) = RouterConfigLoader.parse(sourced("""{ "kinds": [10002] }""", select))

        // marker is sugar for a where; saying both is saying it twice.
        assertFailsWith<IllegalArgumentException> {
            parse("""{ tag = "r", marker = "write", where = [ { maxSize = 2 } ] }""")
        }
        // No predicate at all, and the index/equals halves apart.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { } ] }""") }
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { index = 2 } ] }""") }
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { equals = "write" } ] }""") }
        // Bounds no tag can meet: below the url the select itself demands, or crossed.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { maxSize = 1 } ] }""") }
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { minSize = 4, maxSize = 3 } ] }""") }
        // An equals whose element can't exist under the entry's own maxSize.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { index = 2, equals = "write", maxSize = 2 } ] }""") }
        // A minSize the url guard already guarantees holds for every tag — and one
        // always-true entry in an OR list silently disables the others.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { minSize = 2 } ] }""") }
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", where = [ { minSize = 0 } ] }""") }
        // The smallest minSize that can actually filter is one past the url slot.
        parse("""{ tag = "r", where = [ { minSize = 3 } ] }""")
    }

    @Test
    fun `a relaySource entry needs both a filter with kinds and a select`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(stream("""relaySource = [ { select = [ { tag = "r" } ] } ]"""))
        }
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(stream("""relaySource = [ { filter = { "kinds": [10002] } } ]"""))
        }
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "authors": ["abc"] }"""))
        }
    }

    @Test
    fun `relaySource defaults come from the env`() {
        val cfg =
            RouterConfigLoader.fromEnv(
                mapOf(
                    "ROUTER_CONFIG" to
                        """
                        streams {
                            outbox {
                                filter = { "kinds": [1] }
                                relaySource = [
                                    {
                                        select = [ { tag = "r" } ]
                                        filter = { "kinds": [10002] }
                                    }
                                ]
                            }
                        }
                        """.trimIndent(),
                    "ROUTER_DYNAMIC_REFRESH_SECONDS" to "900",
                    "ROUTER_DYNAMIC_CONCURRENCY" to "16",
                ),
            )
        val dynamic = cfg!!.dynamicStreams().single().dynamic!!
        assertEquals(900L, dynamic.refreshSeconds)
        assertEquals(16, dynamic.concurrency)
    }

    /** A one-stream config, with [body] as the stream's keys. */
    private fun stream(body: String) =
        """
        streams {
            s {
                filter = { "kinds": [1] }
                $body
            }
        }
        """.trimIndent()

    @Test
    fun `a stream must have either urls or a relaySource, never both`() {
        assertFailsWith<IllegalArgumentException> { RouterConfigLoader.parse(stream("")) }
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(
                stream(
                    """
                    urls = ["wss://a.example"]
                    relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]
                    """.trimIndent(),
                ),
            )
        }
        assertFailsWith<IllegalArgumentException> { RouterConfigLoader.parse(stream("relaySource = []")) }
    }

    @Test
    fun `a relaySource stream can only pull down`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(
                stream(
                    """
                    dir = "up"
                    relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]
                    """.trimIndent(),
                ),
            )
        }
    }

    @Test
    fun `no router config env yields null`() {
        assertNull(RouterConfigLoader.fromEnv(emptyMap()))
    }

    @Test
    fun `inline ROUTER_CONFIG env is parsed`() {
        val cfg = RouterConfigLoader.fromEnv(mapOf("ROUTER_CONFIG" to streamsConfig))
        assertEquals(12, cfg!!.downUpstreams().size)
    }

    @Test
    fun `ROUTER_STREAMS runs only the streams it names`() {
        val cfg =
            RouterConfigLoader.fromEnv(
                mapOf("ROUTER_CONFIG" to streamsConfig, "ROUTER_STREAMS" to " mirrors "),
            )

        assertEquals(listOf("mirrors"), cfg!!.streams.map { it.name })
    }

    @Test
    fun `ROUTER_STREAMS naming a stream the config lacks is an error rather than an empty run`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.fromEnv(
                mapOf("ROUTER_CONFIG" to streamsConfig, "ROUTER_STREAMS" to "mirrorz"),
            )
        }
    }

    @Test
    fun `no ROUTER_STREAMS runs everything`() {
        val cfg = RouterConfigLoader.fromEnv(mapOf("ROUTER_CONFIG" to streamsConfig, "ROUTER_STREAMS" to "  "))

        assertEquals(2, cfg!!.streams.size)
    }
}
