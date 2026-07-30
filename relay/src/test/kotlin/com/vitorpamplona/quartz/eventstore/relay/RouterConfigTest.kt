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
        assertEquals(0L, popular.backfillSeconds)
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
                        backfillSeconds = 86400
                    }
                }
                """.trimIndent(),
                backfillDefault = 10L,
            )
        val s = cfg.streams.single()
        assertEquals(20L, cfg.connectionTimeoutSec) // default when unset
        assertEquals(MirrorDirection.DOWN, s.dir) // default dir
        assertEquals(true, s.trusted)
        assertEquals(86400L, s.backfillSeconds) // explicit beats the default
    }

    @Test
    fun `backfillSeconds falls back to the env default`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        filter = { "kinds": [0] }
                        urls = ["wss://x.example"]
                    }
                }
                """.trimIndent(),
                backfillDefault = 3600L,
            )
        assertEquals(3600L, cfg.streams.single().backfillSeconds)
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
    fun `parses a dynamic relaySource stream`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    outbox {
                        dir    = "down"
                        filter = { "kinds": [0, 3, 10002] }
                        relaySource {
                            kind           = 10002
                            marker         = "write"
                            refreshSeconds = 3600
                            concurrency    = 4
                            exclude        = [ "wss://skip.example" ]
                        }
                    }
                    assertions {
                        filter = { "kinds": [30382] }
                        relaySource { kind = 10040 }
                    }
                }
                """.trimIndent(),
            )

        val outbox = cfg.streams.first { it.name == "outbox" }.relaySource!!
        assertEquals(RelayListKind.OUTBOX, outbox.kind)
        assertEquals(RelayRole.WRITE, outbox.role)
        assertEquals(3600L, outbox.refreshSeconds)
        assertEquals(4, outbox.concurrency)
        assertEquals(listOf("wss://skip.example/"), outbox.exclude.map { it.url })

        // 10040 has no read/write markers, so it defaults to every url it names.
        val assertions = cfg.streams.first { it.name == "assertions" }.relaySource!!
        assertEquals(RelayListKind.TRUST_PROVIDERS, assertions.kind)
        assertEquals(RelayRole.ANY, assertions.role)
        assertEquals(21_600L, assertions.refreshSeconds) // the built-in default

        // Dynamic streams have no static urls, so they are not down/up upstreams.
        assertEquals(2, cfg.dynamicStreams().size)
        assertTrue(cfg.downUpstreams().isEmpty())
        assertTrue(cfg.upUpstreams().isEmpty())
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
                                relaySource { kind = 10002 }
                            }
                        }
                        """.trimIndent(),
                    "ROUTER_DYNAMIC_REFRESH_SECONDS" to "900",
                    "ROUTER_DYNAMIC_CONCURRENCY" to "16",
                ),
            )
        val source = cfg!!.dynamicStreams().single().relaySource!!
        assertEquals(900L, source.refreshSeconds)
        assertEquals(16, source.concurrency)
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
                    relaySource { kind = 10002 }
                    """.trimIndent(),
                ),
            )
        }
    }

    @Test
    fun `a relaySource stream can only pull down, and only from a known list kind`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(
                stream(
                    """
                    dir = "up"
                    relaySource { kind = 10002 }
                    """.trimIndent(),
                ),
            )
        }
        assertFailsWith<IllegalStateException> {
            RouterConfigLoader.parse(stream("relaySource { kind = 3 }"))
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
}
