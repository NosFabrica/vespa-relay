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
import kotlin.test.assertFalse
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

        assertEquals(SyncDirection.DOWN, popular.dir)
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
        assertEquals(SyncDirection.DOWN, s.dir) // default dir
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
                        filter = { "kinds": [1], "authors": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"], "#t": ["nostr","bitcoin"], "search": "hello" }
                        urls = ["wss://x.example"]
                    }
                }
                """.trimIndent(),
            )
        val f = cfg.streams.single().filter
        assertEquals(listOf(1), f.kinds)
        assertEquals(listOf("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), f.authors)
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
                        exclude            = [ "wss://skip.example" ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
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
                                filter = { "kinds": [1], "limit": 1000, "authors": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] }
                            }
                        ]
                    }
                    assertions {
                        filter = { "kinds": [30382] }
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
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

        val outbox = cfg.streams.first { it.name == "outbox" }.discovery!!
        assertEquals(3600L, outbox.refreshSeconds)
        assertEquals(listOf("wss://skip.example/"), outbox.exclude.urls.map { it.url })
        assertEquals(2, outbox.sources.size)

        // One scan over three kinds, three selects sorting them out.
        val lists = outbox.sources[0]
        assertEquals(listOf(10002, 10040, 10050), lists.filter.kinds)
        assertEquals(3, lists.selects.size)

        // Defaults: index 1, no where filter, and a kind-less select applies to all.
        val nip65 = lists.selects[0]
        assertEquals(10002, nip65.kind)
        assertEquals("r", nip65.tag)
        assertEquals(1, nip65.urlIndex)
        assertEquals(nip65Write, nip65.where)

        // A NIP-85 service tag, named exactly, with the url after the pubkey.
        val provider = lists.selects[1]
        assertEquals("30382:rank", provider.tag)
        assertEquals(2, provider.urlIndex)
        assertTrue(provider.where.isEmpty())

        assertNull(lists.selects[2].kind, "no kind = every event the filter collected")

        // The second scan is a bounded sweep of a regular kind, filter fields and all.
        val hints = outbox.sources[1]
        assertEquals(listOf(1), hints.filter.kinds)
        assertEquals(1000, hints.filter.limit)
        assertEquals(listOf("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), hints.filter.authors)

        val assertions = cfg.streams.first { it.name == "assertions" }.discovery!!
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
        assertEquals(2, cfg.discoveryStreams().size)
        assertTrue(cfg.downUpstreams().isEmpty())
        assertTrue(cfg.upUpstreams().isEmpty())
    }

    @Test
    fun `a plain exclude url matches by normalized equality and a regex entry matches the whole url`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    outbox {
                        filter  = { "kinds": [10002] }
                        exclude = [ "PURPLEPAG.ES", "wss://DIRECTORY.YABU.ME:443", "wss://filter.nostr.wine/npub.*" ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        relaySource = [
                            {
                                select = [ { tag = "r" } ]
                                filter = { "kinds": [10002] }
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val exclude =
            cfg.streams
                .single()
                .discovery!!
                .exclude
        // No regex metacharacter (a dot is not one), so the first two entries
        // are plain urls: normalized like a `urls` entry — covering the
        // scheme-less uppercase spelling a pre-regex config could carry, and
        // an uppercase host with a redundant :443, which discovery also
        // strips from every url before the exclude check — and excluding
        // exactly one relay each...
        assertTrue(RelayUrlNormalizer.normalize("wss://purplepag.es") in exclude)
        assertTrue(RelayUrlNormalizer.normalize("wss://directory.yabu.me") in exclude)
        // ...never a longer url it sits inside, nor the look-alike host its
        // dots would reach as a regex.
        assertFalse(RelayUrlNormalizer.normalize("wss://purplepag.es.evil.example") in exclude)
        assertFalse(RelayUrlNormalizer.normalize("wss://purplepagXes") in exclude)
        // The `.*` makes the second entry a regex, and it reaches only what
        // it names: the per-user urls the host mints, not the relay itself.
        assertTrue(RelayUrlNormalizer.normalize("wss://filter.nostr.wine/npub1xyz") in exclude)
        assertFalse(RelayUrlNormalizer.normalize("wss://filter.nostr.wine") in exclude)
        assertFalse(RelayUrlNormalizer.normalize("wss://nostr.wine") in exclude)
    }

    @Test
    fun `a broken exclude regex refuses the config naming the stream and the entry`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(
                    """
                    streams {
                        outbox {
                            filter  = { "kinds": [10002] }
                            exclude = [ "wss://filter.nostr.wine/[" ]
                            gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                            relaySource = [
                                {
                                    select = [ { tag = "r" } ]
                                    filter = { "kinds": [10002] }
                                }
                            ]
                        }
                    }
                    """.trimIndent(),
                )
            }
        assertTrue("outbox" in e.message!!, "the error names the stream: ${e.message}")
        assertTrue("wss://filter.nostr.wine/[" in e.message!!, "the error names the entry: ${e.message}")
    }

    /** A one-source `relaySource` list, with [filter] as the scan. */
    private fun sourced(
        filter: String,
        select: String = """{ tag = "r" }""",
    ) = stream(
        """
        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
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
        RouterConfigLoader.parse(sourced("""{ "kinds": [1], "authors": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] }"""))
        // `until` alone doesn't: it caps the top and leaves all of history below.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [1], "until": 1750000000 }"""))
        }
        // Replaceable and addressable kinds are one event per author — safe whole.
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002] }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [30000] }"""))
    }

    @Test
    fun `a negative since, until or limit is refused at parse time`() {
        // None of the three can be negative under NIP-01, and every way a relay
        // reacts to one is a failure that never names the config behind it:
        // strfry CLOSEs the subscription, three of the five `indexers` answer a
        // NOTICE and then never EOSE (so each page burns a whole idle timeout),
        // and purplepag.es drops the bound and serves its NEWEST page instead —
        // the opposite end of the relay from the one asked for.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": -1 }"""))
        }
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "until": -1 }"""))
        }
        // The quietest of the three: quartz drops a filter whose limit is already
        // met before the first REQ, so the stream downloads nothing and reports
        // LIMIT_REACHED every cycle, reading as a relay that simply has no events.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "limit": -1 }"""))
        }
        // Zero is not negative and stays legal, but for a DIFFERENT reason in
        // each of the three — see the two tests below.
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 0 }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "until": 0 }"""))
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "limit": 0 }"""))
    }

    @Test
    fun `a zero limit stays legal, it is the live-only idiom, not a mistake`() {
        // Worth pinning with its reason, because the paged path makes zero look
        // broken from one side: quartz's `stillNeedsMore` is
        // `matchCountPerFilter[i] < filter.limit`, so `0 < 0` drops the filter
        // before the first REQ and the walk reports LIMIT_REACHED having
        // downloaded nothing.
        //
        // That is the correct outcome, not a bug to validate away. `limit = 0`
        // is the NIP-01 way to say "send me no stored events, just stream the
        // live ones", and this router honours it: `SyncEngine`'s down tail
        // subscribes with this same filter, overriding `since` but NOT `limit`,
        // so the live subscription still runs. LIMIT_REACHED is not DRAINED, so
        // no band claims coverage from it either. A stream configured to want
        // no history downloading no history is the truth.
        val cfg = RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "limit": 0 }"""))
        assertEquals(
            0,
            cfg
                .discoveryStreams()
                .single()
                .discovery!!
                .sources
                .single()
                .filter.limit,
        )
    }

    @Test
    fun `a zero since is the absence of a floor, so it is normalised to absent`() {
        // `created_at` is unsigned: the epoch IS the bottom, so `since = 0` asks
        // for exactly what omitting `since` asks for. Two places read
        // `since != null` as "bounded" and were both fooled by the long
        // spelling — `flooredForPaging` passed it through unfloored (leaving the
        // leg to end UNPAGEABLE and re-walk every boot), and the `narrowed`
        // check below counted it as narrowing a regular-kind scan.
        val cfg = RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 0 }"""))
        assertNull(
            cfg
                .discoveryStreams()
                .single()
                .discovery!!
                .sources
                .single()
                .filter.since,
        )
        // A real floor is untouched — this normalises the epoch, nothing else.
        val real = RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 1577836800 }"""))
        assertEquals(
            1577836800L,
            real
                .discoveryStreams()
                .single()
                .discovery!!
                .sources
                .single()
                .filter.since,
        )
        // And it is not a back door into the unbounded-scan guard: kind 1 is a
        // regular kind, and `since = 0` no longer counts as narrowing it.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [1], "since": 0 }"""))
        }
    }

    @Test
    fun `an inverted window is refused rather than recorded as settled history`() {
        // since > until matches nothing, and nothing downstream notices: the
        // relay EOSEs an empty page, the walk reports DRAINED, and
        // `drainSettlesThePast` compares the leg's floor to the filter's — the
        // same value — and says yes. The band then records a settled past for a
        // window that could not have returned an event.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 1700000000, "until": 1600000000 }"""))
        }
        // Equal bounds are a real one-second window, not an inversion — a
        // band's re-read edge leg is exactly that shape.
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 1600000000, "until": 1600000000 }"""))
        // And normalising `since = 0` must not turn a legal filter into an
        // inverted one: null since, bounded until, still fine.
        RouterConfigLoader.parse(sourced("""{ "kinds": [10002], "since": 0, "until": 1600000000 }"""))
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
                .discoveryStreams()
                .single()
                .discovery!!
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
                .discoveryStreams()
                .single()
                .discovery!!
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
    fun `deleteMissing needs the pool's audit, a relay source and its clock`() {
        // The comparison runs as the history audit over asks a scan paired
        // with their owners, so the config must supply both halves: the
        // relaySource (a static stream has no discovery to pair authors
        // with) and negentropySyncThePastSeconds (the reconcile's clock).
        fun stream(extra: String) =
            RouterConfigLoader
                .parse(
                    """
                    streams {
                      s {
                        dir = "down"
                        filter = { "kinds": [30382] }
                        $extra
                      }
                    }
                    """.trimIndent(),
                ).streams
                .single()

        val gated = """
            gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
            relaySource = [
                {
                    select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                    filter = { "kinds": [10040] }
                }
            ]
        """
        assertEquals(
            DeleteMissing.ON,
            stream(
                """ownedKinds = [30382]
                deleteMissing = true
                negentropySyncThePastSeconds = 86400
                $gated""",
            ).deleteMissing,
        )
        assertEquals(
            DeleteMissing.DRY_RUN,
            stream(
                """ownedKinds = [30382]
                deleteMissing = "dryRun"
                negentropySyncThePastSeconds = 86400
                $gated""",
            ).deleteMissing,
        )
        // No relaySource: nothing pairs the owned kinds with their owners.
        assertFailsWith<IllegalArgumentException> {
            stream(
                """urls = [ "wss://a.example" ]
                ownedKinds = [30382]
                deleteMissing = true""",
            )
        }
        // No negentropySyncThePastSeconds: the one decision that destroys data
        // has no clock.
        assertFailsWith<IllegalArgumentException> {
            stream(
                """ownedKinds = [30382]
                deleteMissing = true
                $gated""",
            )
        }
        assertFailsWith<IllegalArgumentException> {
            stream(
                """ownedKinds = [30382]
                deleteMissing = "sometimes"
                negentropySyncThePastSeconds = 86400
                $gated""",
            )
        }
    }

    @Test
    fun `a stream may set its own re-walk period, floored and read per stream`() {
        // The re-walk is the coarse twin of the audit — the audit reconciles
        // the covered history, this re-downloads it — and a visit pages before
        // it audits, so a stream left on the same period as its audit re-pages
        // everything and then reconciles the same ground.
        fun stream(extra: String) =
            RouterConfigLoader
                .parse(
                    """
                    streams {
                      s {
                        dir = "down"
                        filter = { "kinds": [1] }
                        urls = [ "wss://a.example" ]
                        $extra
                      }
                    }
                    """.trimIndent(),
                ).streams
                .single()

        assertEquals(2_592_000L, stream("refetchThePastSeconds = 2592000").refetchThePastSeconds)
        // Unset is not zero: it means the router's default, which is the env
        // knob and quartz's week under that.
        assertNull(stream("").refetchThePastSeconds)
        // Same floor as the audit's, and for the same reason — under an hour
        // it is a re-walk loop rather than a period.
        assertEquals(3600L, stream("refetchThePastSeconds = 60").refetchThePastSeconds)
    }

    @Test
    fun `the renamed knobs still answer to their old names, loudly`() {
        // verifySeconds -> auditSeconds -> negentropySyncThePastSeconds,
        // newUrlSeconds -> fastLaneSeconds, concurrency -> dialConcurrency. A
        // renamed key must never silently turn a deployment's reconciles or
        // fast lane off — every old spelling parses, warns, and means the same
        // thing. Two generations of the same knob, because the second rename
        // is what put the transport in the name.
        val cfg =
            RouterConfigLoader.parse(
                """
                monitor {
                    sources = [ { select = [ { tag = "r", relay = 1 } ], filter = { "kinds": [10002] } } ]
                    newUrlSeconds = 90
                    concurrency = 7
                }
                streams {
                    s {
                        dir = "down"
                        filter = { "kinds": [1] }
                        relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        verifySeconds = 604800
                    }
                }
                """.trimIndent(),
            )
        assertEquals(604_800L, cfg.streams.single().negentropySyncThePastSeconds)
        assertEquals(90L, cfg.monitor!!.fastLaneSeconds)
        assertEquals(7, cfg.monitor!!.dialConcurrency)
    }

    @Test
    fun `a retracting stream's every ask must be author-bound, the delete's whole licence is per provider`() {
        // An UNBOUND ask on a retracting stream would reconcile EVERY
        // provider's owned records against one relay and delete whatever that
        // relay happens not to hold — one config mistake away from store-wide
        // destruction. Two shapes produce one: a verdict source (fans the
        // stream's single filter to every certified relay), and a scan whose
        // select binds no `authors`. Both refused by name.
        fun stream(source: String) =
            RouterConfigLoader.parse(
                """
                streams {
                  s {
                    dir = "down"
                    filter = { "kinds": [30382] }
                    ownedKinds = [30382]
                    deleteMissing = true
                    negentropySyncThePastSeconds = 86400
                    gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                    relaySource = [ $source ]
                  }
                }
                """.trimIndent(),
            )
        assertFailsWith<IllegalArgumentException>("verdict source") {
            stream("""{ filter = { "kinds": [30166], "#l": ["prime"] } }""")
        }
        assertFailsWith<IllegalArgumentException>("select without authors") {
            stream(
                """{
                    select = [ { tag = "30382:rank", relay = 2 } ]
                    filter = { "kinds": [10040] }
                }""",
            )
        }
        // The bound shape stays parseable — the refusals are about binding,
        // not about retracting streams as such.
        stream(
            """{
                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                filter = { "kinds": [10040] }
            }""",
        )
    }

    @Test
    fun `deleting requires naming what it may delete`() {
        fun stream(
            kinds: String,
            extra: String,
        ) = RouterConfigLoader
            .parse(
                """
                streams {
                  s {
                    dir = "down"
                    filter = { $kinds }
                    negentropySyncThePastSeconds = 86400
                    gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                    relaySource = [
                        {
                            select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                            filter = { "kinds": [10040] }
                        }
                    ]
                    $extra
                  }
                }
                """.trimIndent(),
            ).streams
            .single()

        // The whole point: deletion is refused until the config says which
        // kinds the upstream owns. Without it every other kind in the filter
        // gets deleted for being absent from a relay that never held it —
        // measured, no NIP-85 provider relay serves its own key's kind 0.
        assertFailsWith<IllegalArgumentException> {
            stream(""""kinds": [0, 10002, 30382]""", """deleteMissing = true""")
        }
        assertFailsWith<IllegalArgumentException> {
            stream(
                """"kinds": [0, 10002, 30382]""",
                """deleteMissing = true
                ownedKinds = []""",
            )
        }
        assertEquals(
            setOf(30382),
            stream(
                """"kinds": [0, 10002, 30382]""",
                """deleteMissing = true
                ownedKinds = [30382]""",
            ).ownedKinds,
        )

        // Owning a kind the stream never asks for compares nothing.
        assertFailsWith<IllegalArgumentException> {
            stream(
                """"kinds": [0, 10002]""",
                """deleteMissing = true
                ownedKinds = [30382]""",
            )
        }
        // "Every kind" leaves the protected set open-ended.
        assertFailsWith<IllegalArgumentException> {
            stream(
                """"authors": ["aa"]""",
                """deleteMissing = true
                ownedKinds = [30382]""",
            )
        }
        // A licence with no deletion is a trap for whoever turns deletion on.
        assertFailsWith<IllegalArgumentException> {
            stream(""""kinds": [30382]""", """ownedKinds = [30382]""")
        }
        assertEquals(emptySet(), stream(""""kinds": [30382]""", "").ownedKinds)
    }

    @Test
    fun `a select binds filter fields from its own tag`() {
        fun selectOf(select: String) =
            RouterConfigLoader
                .parse(sourced("""{ "kinds": [10040] }""", select))
                .discoveryStreams()
                .single()
                .discovery!!
                .sources
                .single()
                .selects
                .single()

        val nip85 = selectOf("""{ tag = "30382:rank", relay = 2, authors = 1 }""")
        assertEquals(2, nip85.urlIndex, "`relay` names the slot `index` used to")
        assertEquals(mapOf("authors" to BindingSlot.OfTag(1)), nip85.bindings)

        // The scanned event's own author — the outbox model, where the author is
        // nowhere in the tag.
        assertEquals(
            mapOf("authors" to BindingSlot.EventPubkey),
            selectOf("""{ tag = "r", relay = 1, authors = "pubkey" }""").bindings,
        )
        // Tag filters, for any single letter NIP-01 allows.
        assertEquals(
            mapOf("#p" to BindingSlot.OfTag(1)),
            selectOf("""{ tag = "p", relay = 2, "#p" = 1 }""").bindings,
        )
        // No bindings is the shape every config had before they existed.
        assertEquals(emptyMap(), selectOf("""{ tag = "r", index = 1 }""").bindings)
    }

    @Test
    fun `a binding must name a slot that could hold a value`() {
        fun parse(select: String) = RouterConfigLoader.parse(sourced("""{ "kinds": [10040] }""", select))

        // Element 0 is the tag name, never a value.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "30382:rank", relay = 2, authors = 0 }""") }
        // Neither a slot number nor one of the two names for something outside
        // the tag. Rejected rather than ignored: a binding that silently bound
        // nothing would show up as a stream quietly syncing the wrong thing.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "30382:rank", relay = 2, authors = "author" }""") }
        // `relay` and `index` are the same slot under two names.
        assertFailsWith<IllegalArgumentException> { parse("""{ tag = "r", relay = 1, index = 1 }""") }
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
            RouterConfigLoader.parse(sourced("""{ "authors": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] }"""))
        }
    }

    @Test
    fun `relaySource defaults come from the env`() {
        val cfg =
            RouterConfigLoader.fromEnv(
                mapOf(
                    "SYNC_CONFIG" to
                        """
                        streams {
                            outbox {
                                filter = { "kinds": [1] }
                                gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                                relaySource = [
                                    {
                                        select = [ { tag = "r" } ]
                                        filter = { "kinds": [10002] }
                                    }
                                ]
                            }
                        }
                        """.trimIndent(),
                    "SYNC_DYNAMIC_REFRESH_SECONDS" to "900",
                ),
            )
        val discovery = cfg!!.discoveryStreams().single().discovery!!
        assertEquals(900L, discovery.refreshSeconds)
    }

    @Test
    fun `the cycle engine's knobs are refused by name, with the migration note`() {
        // Silently ignoring a legacy knob is a silently different deployment.
        // Each one is refused naming its replacement.
        fun gated(extra: String) =
            """
            streams {
                s {
                    dir    = "down"
                    filter = { "kinds": [1] }
                    $extra
                    gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                    relaySource = [
                        {
                            select = [ { tag = "r" } ]
                            filter = { "kinds": [10002] }
                        }
                    ]
                }
            }
            """.trimIndent()
        for (knob in listOf("authorsPerLeg = 1", "concurrency = 8", "recycleSeconds = 30", """sync = "fetch"""")) {
            val e = assertFailsWith<IllegalArgumentException> { RouterConfigLoader.parse(gated(knob)) }
            assertTrue(knob.substringBefore(" ") in e.message!!, "the error names the knob: ${e.message}")
        }
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
    fun `negentropy paging is on by default and sized from the env`() {
        val stock = RouterConfigLoader.fromEnv(mapOf("SYNC_CONFIG" to streamsConfig))
        assertEquals(100_000, stock!!.negPageTarget)
        assertEquals(60L, stock.negPageSlackSec)

        val tuned =
            RouterConfigLoader.fromEnv(
                mapOf(
                    "SYNC_CONFIG" to streamsConfig,
                    "SYNC_NEG_PAGE_TARGET" to "25000",
                    "SYNC_NEG_PAGE_MIN" to "500",
                    "SYNC_NEG_PAGE_MAX" to "250000",
                    "SYNC_NEG_PAGE_SLACK_SECONDS" to "120",
                ),
            )
        assertEquals(25_000, tuned!!.negPageTarget)
        assertEquals(500, tuned.negPageMin)
        assertEquals(250_000, tuned.negPageMax)
        assertEquals(120L, tuned.negPageSlackSec)
    }

    @Test
    fun `a zero paging target turns windowed reconciliation off`() {
        // The escape hatch back to one shared snapshot per stream — correct,
        // and the only way to get the old memory profile back.
        val cfg = RouterConfigLoader.fromEnv(mapOf("SYNC_CONFIG" to streamsConfig, "SYNC_NEG_PAGE_TARGET" to "0"))
        assertEquals(0, cfg!!.negPageTarget)
    }

    @Test
    fun `a paging ceiling below the floor is raised to it`() {
        // Nonsense config must not produce a window size that can never be met.
        val cfg =
            RouterConfigLoader.fromEnv(
                mapOf("SYNC_CONFIG" to streamsConfig, "SYNC_NEG_PAGE_MIN" to "10000", "SYNC_NEG_PAGE_MAX" to "100"),
            )
        assertEquals(10_000, cfg!!.negPageMin)
        assertEquals(10_000, cfg.negPageMax)
    }

    @Test
    fun `no router config env yields null`() {
        assertNull(RouterConfigLoader.fromEnv(emptyMap()))
    }

    @Test
    fun `legacy ROUTER_ spellings still load, and the SYNC_ name wins when both are set`() {
        // The env vars were renamed; a deployment still exporting the old
        // names must keep mirroring rather than silently serve-only.
        val legacy =
            RouterConfigLoader.fromEnv(
                mapOf("ROUTER_CONFIG" to streamsConfig, "ROUTER_INGEST_BATCH" to "77"),
            )
        assertEquals(77, legacy?.ingestBatch)
        val both =
            RouterConfigLoader.fromEnv(
                mapOf("ROUTER_INGEST_BATCH" to "77", "SYNC_INGEST_BATCH" to "88", "SYNC_CONFIG" to streamsConfig),
            )
        assertEquals(88, both?.ingestBatch)
    }

    @Test
    fun `inline SYNC_CONFIG env is parsed`() {
        val cfg = RouterConfigLoader.fromEnv(mapOf("SYNC_CONFIG" to streamsConfig))
        assertEquals(12, cfg!!.downUpstreams().size)
    }

    @Test
    fun `SYNC_STREAMS runs only the streams it names`() {
        val cfg =
            RouterConfigLoader.fromEnv(
                mapOf("SYNC_CONFIG" to streamsConfig, "SYNC_STREAMS" to " mirrors "),
            )

        assertEquals(listOf("mirrors"), cfg!!.streams.map { it.name })
    }

    @Test
    fun `SYNC_STREAMS naming a stream the config lacks is an error rather than an empty run`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.fromEnv(
                mapOf("SYNC_CONFIG" to streamsConfig, "SYNC_STREAMS" to "mirrorz"),
            )
        }
    }

    @Test
    fun `sync is refused, the pool has one shape`() {
        // It chose the transport of a stream the legacy backfill walked once
        // per process. Every stream rides the pool now — page forward,
        // live-tail, reconcile the past, re-fetch the past — so the knob has
        // nothing left to decide, and accepting it would claim otherwise.
        for (mode in listOf("fetch", "negentropy", "auto")) {
            assertFailsWith<IllegalArgumentException>("sync = \"$mode\" must be refused") {
                RouterConfigLoader.parse(
                    stream(
                        """
                        sync = "$mode"
                        urls = [ "wss://a.example" ]
                        """.trimIndent(),
                    ),
                )
            }
        }
        // …and a stream that says nothing about transport still parses.
        assertEquals(1, RouterConfigLoader.parse(stream("""urls = [ "wss://a.example" ]""")).streams.size)
    }

    @Test
    fun `no SYNC_STREAMS runs everything`() {
        val cfg = RouterConfigLoader.fromEnv(mapOf("SYNC_CONFIG" to streamsConfig, "SYNC_STREAMS" to "  "))

        assertEquals(2, cfg!!.streams.size)
    }

    @Test
    fun `a relaySource asking for kind 30166 is the monitor's verdicts, verified`() {
        // The verdict-built list IS a relaySource — the whole point of the
        // monitor split is that this stream never scans a 10002 again, and
        // there is no second config shape to learn: the same filter spelling,
        // routed to the verified read instead of the tag scan.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    content {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#l": ["prime"] }
                                maxAgeSeconds = 7200
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val source =
            cfg.streams
                .single()
                .discovery!!
                .sources
                .single()
        assertEquals(7200L, source.maxAgeSeconds)
        assertEquals(listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)), source.selects, "the `d` tag NIP-66 fixes")
    }

    @Test
    fun `a verdict query is an ordinary source with a default select`() {
        fun stream(source: String) =
            """
            streams {
                s {
                    dir    = "down"
                    filter = { "kinds": [1] }
                    relaySource = [ $source ]
                }
            }
            """.trimIndent()
        // The "#l" left off entirely takes the default age, and the `d`-tag
        // select NIP-66 fixes for us.
        val bare =
            RouterConfigLoader
                .parse(stream("""{ filter = { "kinds": [30166] } }"""))
                .streams
                .single()
                .discovery!!
                .sources
                .single()
        assertEquals(null, bare.maxAgeSeconds, "no bound is inferred from the kind, or from any tag in the filter")
        assertEquals(listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)), bare.selects)
        // A WIDE CONFIG STAYS WIDE. This used to have `#l: ["prime"]` put on
        // it — first by a read that hardcoded the value, then by the loader
        // making that explicit — so a filter asking for every verdict quietly
        // asked for one. Narrowing what the operator wrote is the same mistake
        // whichever layer does it, and this filter now means what it says: all
        // of them, `dead` included.
        assertEquals(null, bare.filter.tags, "no tag predicate is added to a filter that carries none")
        // …and a select written by hand is honoured rather than refused: there
        // is no verified read left for it to be incompatible with.
        assertEquals(
            listOf(RelaySelect(kind = null, tag = "d", urlIndex = 1)),
            RouterConfigLoader
                .parse(stream("""{ select = [ { tag = "d" } ], filter = { "kinds": [30166] } }"""))
                .streams
                .single()
                .discovery!!
                .sources
                .single()
                .selects,
        )
        // Any tag value parses. `#s: ["dead"]` as a SOURCE is a stream that
        // syncs from relays our monitor called dead, which is odd but is the
        // operator's odd; refusing it would mean this loader having an opinion
        // about what values in what tag mean, which is the opinion the whole
        // shape exists to avoid holding.
        RouterConfigLoader.parse(stream("""{ filter = { "kinds": [30166], "#l": ["dead"] } }"""))
        // Mixing 30166 with another kind still fails, and for a reason that is
        // not about semantics: the `d`-tag default is a NIP-66 fact about kind
        // 30166 and says nothing about where kind 10002 keeps its urls.
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(stream("""{ filter = { "kinds": [30166, 10002] } }"""))
        }
    }

    @Test
    fun `a filter's authors are raw hex, because a filter block is a NIP-01 filter`() {
        fun stream(source: String) =
            """
            streams {
                s {
                    dir    = "down"
                    filter = { "kinds": [1] }
                    relaySource = [ $source ]
                }
            }
            """.trimIndent()
        // The split-monitor deployment: the operator who set the monitor's
        // nsec knows its npub and writes it here; the loader hands the read
        // the hex the store speaks.
        val named =
            RouterConfigLoader.parse(
                stream(
                    """{ filter = { "kinds": [30166], "#l": ["prime"],
                         "authors": ["0000000000000000000000000000000000000000000000000000000000000001"] } }""",
                ),
            )
        val namedSource =
            named.streams
                .single()
                .discovery!!
                .sources
                .single()
        // Carried through untouched: what the operator wrote IS what goes on
        // the wire. Copy a filter out of a REQ and it works here; paste one
        // from here into a REQ and it works there.
        assertEquals(listOf("0".repeat(63) + "1"), namedSource.filter.authors)
        // Absent is the unscoped read, and stays absent on the filter: an
        // EMPTY authors list would be a predicate nothing satisfies.
        assertEquals(
            null,
            RouterConfigLoader
                .parse(stream("""{ filter = { "kinds": [30166] } }"""))
                .streams
                .single()
                .discovery!!
                .sources
                .single()
                .filter.authors,
        )
        // Bech32 is refused rather than decoded: accepting it would make the
        // block "mostly NIP-01", which is the worst of both. An nsec is called
        // out by name — a PRIVATE key in a file people commit. And a value
        // that is neither is refused for shape, since NIP-01 matches these
        // exactly and a malformed one selects nothing and says nothing.
        // …and hex is simply accepted.
        RouterConfigLoader.parse(stream("""{ filter = { "kinds": [30166], "authors": ["${"a".repeat(64)}"] } }"""))
        for (bad in listOf(
            "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w2",
            "nsec1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
            "abc",
            "z".repeat(64),
        )) {
            assertFailsWith<IllegalArgumentException>(bad) {
                RouterConfigLoader.parse(stream("""{ filter = { "kinds": [30166], "authors": ["$bad"] } }"""))
            }
        }
        // Uppercase is lowercased, not refused: unambiguous, and nothing
        // downstream cares which case the operator's clipboard held.
        assertEquals(
            listOf("a".repeat(64)),
            RouterConfigLoader
                .parse(stream("""{ filter = { "kinds": [30166], "authors": ["${"A".repeat(64)}"] } }"""))
                .streams
                .single()
                .discovery!!
                .sources
                .single()
                .filter.authors,
        )
    }

    @Test
    fun `the monitor block parses its sources with the stream-side select syntax`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                monitor {
                    sources = [
                        {
                            select = [ { kind = 10002, tag = "r", marker = "write" } ]
                            filter = { "kinds": [10002] }
                        }
                    ]
                    exclude       = [ "wss://skip.example" ]
                    sweepSeconds  = 3600
                    fastLaneSeconds = 60
                    concurrency   = 32
                }
                streams {
                    content {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#l": ["prime"] }
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val m = cfg.monitor!!
        assertEquals(1, m.sources.size)
        assertEquals(3600L, m.sweepSeconds)
        assertEquals(60L, m.fastLaneSeconds)
        assertEquals(32, m.dialConcurrency)
    }

    @Test
    fun `monitor concurrency defaults when absent and cannot be set to a monitor that never dials`() {
        val block = """
            streams {
                s {
                    dir    = "down"
                    filter = { "kinds": [1] }
                    urls   = ["wss://a.example"]
                }
            }
        """
        val absent = RouterConfigLoader.parse("monitor { sweepSeconds = 3600 }\n$block")
        assertEquals(MonitorConfig.DEFAULT_DIAL_CONCURRENCY, absent.monitor!!.dialConcurrency)
        // Zero dials is an off switch wearing a tuning knob's name — floored,
        // not honored.
        val floored = RouterConfigLoader.parse("monitor { concurrency = 0 }\n$block")
        assertEquals(1, floored.monitor!!.dialConcurrency)
    }

    @Test
    fun `fastLaneSeconds zero turns the fast lane off, absent takes the default`() {
        val off =
            RouterConfigLoader.parse(
                """
                monitor { fastLaneSeconds = 0 }
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        urls   = ["wss://a.example"]
                    }
                }
                """.trimIndent(),
            )
        assertEquals(null, off.monitor!!.fastLaneSeconds)
        val defaulted =
            RouterConfigLoader.parse(
                """
                monitor {}
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        urls   = ["wss://a.example"]
                    }
                }
                """.trimIndent(),
            )
        assertEquals(MonitorConfig.DEFAULT_FAST_LANE_SECONDS, defaulted.monitor!!.fastLaneSeconds)
    }

    @Test
    fun `the pool's two socket knobs parse, default, and refuse zero`() {
        val block = """
            streams {
                s {
                    dir    = "down"
                    filter = { "kinds": [1] }
                    urls   = ["wss://a.example"]
                }
            }
        """
        val tuned = RouterConfigLoader.parse("visitConcurrency = 64\ntailBudget = 900\n$block")
        assertEquals(64, tuned.visitConcurrency)
        assertEquals(900, tuned.tailBudget)
        val defaulted = RouterConfigLoader.parse(block)
        assertEquals(RouterConfig.DEFAULT_VISIT_CONCURRENCY, defaulted.visitConcurrency)
        assertEquals(RouterConfig.DEFAULT_TAIL_BUDGET, defaulted.tailBudget)
        // Zero of either is an off switch wearing a tuning knob's name —
        // floored, not honored, same as the monitor's dial gate.
        val floored = RouterConfigLoader.parse("visitConcurrency = 0\ntailBudget = 0\n$block")
        assertEquals(1, floored.visitConcurrency)
        assertEquals(1, floored.tailBudget)
    }

    @Test
    fun `gatedBy is a stream-level filter over any kind`() {
        // The 10040 shape: the scan supplies the (relay, provider) pairing,
        // `gatedBy` supplies the right to be dialled at all — and it sits on
        // the stream, beside `exclude`, because it is not a property of how a
        // url was discovered.
        val cfg =
            RouterConfigLoader.parse(
                stream(
                    """
                    relaySource = [ {
                        select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                        filter = { "kinds": [10040] }
                    } ]
                    gatedBy = [
                        { filter = { "kinds": [30166], "#l": ["prime"] }, maxAgeSeconds = 7200 }
                    ]
                    """.trimIndent(),
                ),
            )
        val gate =
            cfg.streams
                .single()
                .discovery!!
                .gatedBy
                .single()
        assertEquals(7200L, gate.maxAgeSeconds)
        assertEquals(null, gate.filter.authors, "absent authors is the unscoped read, not a substituted identity")
        // NIP-66 fixes the url in the `d` tag, so a 30166 gate needs no select.
        assertEquals(listOf(RelaySelect(kind = 30166, tag = "d", urlIndex = 1)), gate.selects)
        // …and NOTHING is inferred where none is written: which filters
        // describe a measurement that goes stale is the operator's knowledge.
        assertEquals(
            null,
            RouterConfigLoader
                .parse(
                    stream(
                        """
                        relaySource = [ { select = [ { tag = "30382:rank", relay = 2 } ], filter = { "kinds": [10040] } } ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        """.trimIndent(),
                    ),
                ).streams
                .single()
                .discovery!!
                .gatedBy
                .single()
                .maxAgeSeconds,
        )
    }

    @Test
    fun `a source or gate over anything but kind 30166 has to say where its urls sit`() {
        // Only NIP-66 fixes the position. Every other relay list puts the url
        // somewhere of its own choosing, so guessing an index would read the
        // wrong slot in silence.
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(
                    stream(
                        """
                        relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]
                        gatedBy = [ { filter = { "kinds": [10002] } } ]
                        """.trimIndent(),
                    ),
                )
            }
        assertTrue("select" in e.message!!, e.message!!)
        // Said explicitly, it parses — and a gate need not be about monitors
        // at all: a curated relay list is a perfectly good one.
        val ok =
            RouterConfigLoader.parse(
                stream(
                    """
                    relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]
                    gatedBy = [
                        { select = [ { kind = 10002, tag = "r", marker = "write" } ],
                          filter = { "kinds": [10002], "authors": ["0000000000000000000000000000000000000000000000000000000000000001"] } }
                    ]
                    """.trimIndent(),
                ),
            )
        assertEquals(
            1,
            ok.streams
                .single()
                .discovery!!
                .gatedBy
                .single()
                .selects.size,
        )
    }

    @Test
    fun `an absolute and a relative bound cannot both be written`() {
        // Two spellings of one bound, and the relative one wins at read time —
        // so the absolute one would be read by a human and by nothing else.
        // Either alone is fine: `since` is a static floor a corpus may really
        // want, `maxAgeSeconds` is the one that keeps meaning what it said.
        for (body in listOf(
            """relaySource = [ { filter = { "kinds": [30166], "since": 1700000000 }, maxAgeSeconds = 3600 } ]""",
            """relaySource = [ { filter = { "kinds": [30166] } } ]
               gatedBy = [ { filter = { "kinds": [30166], "since": 1700000000 }, maxAgeSeconds = 3600 } ]""",
        )) {
            val e = assertFailsWith<IllegalArgumentException>(body) { RouterConfigLoader.parse(stream(body)) }
            assertTrue("maxAgeSeconds" in e.message!!, e.message!!)
        }
        RouterConfigLoader.parse(stream("""relaySource = [ { filter = { "kinds": [30166], "since": 1700000000 } } ]"""))
        RouterConfigLoader.parse(stream("""relaySource = [ { filter = { "kinds": [30166] }, maxAgeSeconds = 3600 } ]"""))
    }

    @Test
    fun `an ungated stream parses, because nothing here can tell a gate from a scan`() {
        // This was a parse error, on the rule "unless every source is a verdict
        // query". Stating that rule meant the loader deciding which tag, and
        // which value in it, constitutes a vouching — the operator's choice and
        // another monitor's spelling. A monitor writing `["status", "live"]` is
        // as good a gate as ours and the config is the only thing that knows,
        // so an ungated stream parses and says so at boot instead.
        val ungated =
            RouterConfigLoader.parse(
                stream("""relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]"""),
            )
        assertEquals(
            emptyList(),
            ungated.streams
                .single()
                .discovery!!
                .gatedBy,
        )
        // …and a gate spelled somebody else's way is an ordinary filter here.
        val foreign =
            RouterConfigLoader.parse(
                stream(
                    """
                    relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] } } ]
                    gatedBy = [ { filter = { "kinds": [30166], "#l": ["live"] }, maxAgeSeconds = 3600 } ]
                    """.trimIndent(),
                ),
            )
        assertEquals(
            mapOf("l" to listOf("live")),
            foreign.streams
                .single()
                .discovery!!
                .gatedBy
                .single()
                .filter.tags,
            "no tag and no value is privileged; NIP-01 only indexes single-letter tags, which bounds the " +
                "spellings a monitor can pick but not which one this router understands",
        )
    }

    @Test
    fun `the old spellings name their replacements`() {
        // Both shipped in configs people are running, so the errors have to
        // say what to write instead, not merely that a key is unknown.
        for ((body, expect) in listOf(
            """relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] }, certified = {} } ]""" to "gatedBy",
            """relaySource = [ { select = [ { tag = "r" } ], filter = { "kinds": [10002] },
                 resultsFilteredBy = [ { filter = { "kinds": [30166] } } ] } ]""" to "gatedBy",
        )) {
            val e = assertFailsWith<IllegalArgumentException>(body) { RouterConfigLoader.parse(stream(body)) }
            assertTrue(expect in e.message!!, e.message!!)
        }
    }

    @Test
    fun `a verdict query and a scan ride in one stream, under one gate`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    content {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#l": ["prime"] }
                                maxAgeSeconds = 7200
                            },
                            {
                                select = [ { kind = 10009, tag = "group", index = 2 } ]
                                filter = { "kinds": [10009] }
                            }
                        ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                    }
                }
                """.trimIndent(),
            )
        val discovery = cfg.streams.single().discovery!!
        assertEquals(2, discovery.sources.size, "two ways of finding urls, one list")
        assertEquals(7200L, discovery.sources.first().maxAgeSeconds)
        // One gate over both. The scan needs it; the verdict query does not,
        // and gets it anyway — which is the point of hoisting it: permission
        // is a question about the stream, not about how a url was found.
        assertEquals(1, discovery.gatedBy.size)
        assertEquals(
            listOf(listOf(30166), listOf(10009)),
            discovery.sources.map { it.filter.kinds },
            "two ways of finding urls; the gate above does not care which is which",
        )
    }
}
