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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.peers.DiscoveredRelay
import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The visit model's pure arithmetic: which streams ride the pool, how a
 * revisit is paced, and which walk endings are refusals.
 */
class VisitPoolTest {
    @Test
    fun `more content lately means a sooner revisit, on both bases`() {
        // Yield divides the wait, and the tailed base stays above the untailed one at every score.
        val quietTailed = VisitPool.revisitDelayMs(0.0, tailed = true)
        val quietUntailed = VisitPool.revisitDelayMs(0.0, tailed = false)
        assertEquals(VisitPool.REVISIT_TAILED_MS, quietTailed)
        assertEquals(VisitPool.REVISIT_UNTAILED_MS, quietUntailed)
        assertEquals(quietTailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = true))
        assertEquals(quietUntailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = false))
        // Five hundred takes the untailed base under the floor; the tailed base, six times longer, still divides.
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(500.0, tailed = false))
        assertTrue(VisitPool.revisitDelayMs(500.0, tailed = true) > VisitPool.revisitDelayMs(500.0, tailed = false))
    }

    @Test
    fun `a firehose relay is a frequent guest, never a busy loop`() {
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = false))
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = true))
    }

    @Test
    fun `every dynamic stream rides the pool, verdicts, gated scans, retracting streams`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    pure {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#l": ["prime"] }
                            }
                        ]
                        negentropySyncThePastSeconds = 604800
                    }
                    gatedScan {
                        dir    = "down"
                        filter = { "kinds": [30382] }
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                            }
                        ]
                    }
                    retracting {
                        dir    = "down"
                        filter = { "kinds": [0, 30382] }
                        deleteMissing = "dryRun"
                        ownedKinds = [30382]
                        negentropySyncThePastSeconds = 86400
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        val visit = cfg.streams.filter { VisitPool.ridesThePool(it) }
        // A set: HOCON hands back a map, so block order is not a promise.
        assertEquals(setOf("pure", "gatedScan", "retracting"), visit.map { it.name }.toSet())
        assertEquals(604_800L, visit.single { it.name == "pure" }.negentropySyncThePastSeconds)
    }

    @Test
    fun `a declared-urls stream rides the pool too, and an up stream does not`(): Unit =
        runBlocking {
            val cfg =
                RouterConfigLoader.parse(
                    """
                    streams {
                        declared {
                            dir    = "down"
                            filter = { "kinds": [0, 10002] }
                            urls   = [ "wss://a.example", "wss://b.example" ]
                            negentropySyncThePastSeconds = 604800
                            refetchThePastSeconds = 2592000
                        }
                        pushed {
                            dir    = "up"
                            filter = { "kinds": [1] }
                            urls   = [ "wss://c.example" ]
                        }
                    }
                    """.trimIndent(),
                )
            val visit = cfg.streams.filter { VisitPool.ridesThePool(it) }
            assertEquals(listOf("declared"), visit.map { it.name }, "an up stream pushes; it has no past to re-check")

            val roster =
                RosterBuilder(store = NostrSemanticsStore(InMemoryEventIndex(), relay = null), streams = visit, bands = SyncBands(null)).rebuild()
            assertEquals(
                setOf("wss://a.example/", "wss://b.example/"),
                roster.asks.keys
                    .map { it.url }
                    .toSet(),
                "a declared url is an ask like any other",
            )
            assertEquals(
                listOf(0, 10002),
                roster.asks.values
                    .first()
                    .values
                    .single()
                    .asks
                    .single()
                    .filter.kinds,
                "and it carries the stream's own filter, unnarrowed — one stream, one ask, under its own name",
            )
        }

    @Test
    fun `a retracting pool stream must say when its comparison runs`() {
        assertFailsWith<IllegalArgumentException> {
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [0, 30382] }
                        deleteMissing = "dryRun"
                        ownedKinds = [30382]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ]
                        relaySource = [
                            {
                                select = [ { tag = "30382:rank", relay = 2, authors = 1 } ]
                                filter = { "kinds": [10040] }
                            }
                        ]
                    }
                }
                """.trimIndent(),
            )
        }
    }

    @Test
    fun `a walk that was refused with nothing delivered ends the relay's visit`() {
        for (end in listOf(
            PagedFetchResult.End.IDLE,
            PagedFetchResult.End.CLOSED,
            PagedFetchResult.End.AUTH_REQUIRED,
            PagedFetchResult.End.CANNOT_CONNECT,
            PagedFetchResult.End.UNPAGEABLE,
        )) {
            assertTrue(VisitPool.refusedOutright(PagedFetchResult(0, end)), "$end with nothing delivered is a refusal")
        }
    }

    @Test
    fun `a drained or self-limited walk is not a refusal, and neither is one that delivered`() {
        // DRAINED proves absence, LIMIT_REACHED was our own instruction, and a CLOSED after 4,000 events is a rate limit.
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.DRAINED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.LIMIT_REACHED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(4_000, PagedFetchResult.End.CLOSED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(1, PagedFetchResult.End.IDLE)))
    }

    @Test
    fun `one ask per bound author, the pairing the tags already made`() {
        val base = Filter(kinds = listOf(0, 30382))
        val url = RelayUrlNormalizer.normalize("wss://provider.example")
        val p1 = "a".repeat(64)
        val p2 = "b".repeat(64)
        val paired =
            RosterBuilder.asksOf(
                base,
                DiscoveredRelay(url, bindings = mapOf("authors" to setOf(p2, p1), "kinds" to setOf("30382"))),
            )
        assertEquals(2, paired.size)
        assertEquals(listOf(listOf(p1), listOf(p2)), paired.map { it.authors }, "sorted, so the band's serialized key is stable")
        assertTrue(paired.all { it.kinds == listOf(30382) }, "the other narrow keys ride along in each split")
        // A select that binds nothing keeps one ask: the stream's own filter.
        val unbound = RosterBuilder.asksOf(base, DiscoveredRelay(url))
        assertEquals(listOf(base), unbound)
    }

    @Test
    fun `a tail asks once per shape, not once per provider`() {
        // Merged by shape: bound asks union their authors, and an unbound ask absorbs its shape's bound ones.
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    a { dir = "down", filter = { "kinds": [30382] }
                        relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
                }
                """.trimIndent(),
            )
        val s = cfg.streams.single()
        val p1 = "a".repeat(64)
        val p2 = "b".repeat(64)

        fun ask(filter: Filter) = RosterBuilder.Ask(s, filter)

        val merged =
            VisitPool.tailFilters(
                listOf(
                    ask(Filter(kinds = listOf(30382), authors = listOf(p2))),
                    ask(Filter(kinds = listOf(30382), authors = listOf(p1))),
                    ask(Filter(kinds = listOf(0))),
                ),
                since = 1_000L,
            )
        assertEquals(2, merged.size, "two shapes, however many providers")
        val bound = merged.single { it.kinds == listOf(30382) }
        assertEquals(listOf(p1, p2), bound.authors, "authors union, sorted — the band key discipline")
        assertEquals(1_000L, bound.since)
        val unbound = merged.single { it.kinds == listOf(0) }
        assertNull(unbound.authors)

        val absorbed =
            VisitPool.tailFilters(
                listOf(
                    ask(Filter(kinds = listOf(30382), authors = listOf(p1))),
                    ask(Filter(kinds = listOf(30382))),
                ),
                since = 1_000L,
            )
        assertNull(absorbed.single().authors, "an unbound ask already asks for every author")
    }

    @Test
    fun `a rebuilt roster with the same asks is not news, one more ask is`() {
        // Asserted on the filter's own json, which is what the roster builds and `openTail` compares.
        fun wants(vararg authors: List<String>?) = authors.mapTo(mutableSetOf()) { Filter(kinds = listOf(30382), authors = it).toJson() }

        val p1 = "a".repeat(64)
        val p2 = "b".repeat(64)
        assertEquals(wants(listOf(p1)), wants(listOf(p1)), "same shape, fresh instances — not news")
        assertTrue(wants(listOf(p1)) != wants(listOf(p1), listOf(p2)), "a new bound author is news")
        assertEquals(wants(null), wants(null), "two streams' identical asks are one string, kept apart by the map key")
    }

    @Test
    fun `negentropySyncThePastSeconds has an hour floor, a reconcile is not a re-walk loop`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    s {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        relaySource = [
                            {
                                filter = { "kinds": [30166], "#l": ["prime"] }
                            }
                        ]
                        negentropySyncThePastSeconds = 5
                    }
                }
                """.trimIndent(),
            )
        assertEquals(3600L, cfg.streams.single().negentropySyncThePastSeconds)
    }

    /** A band recorded over [min, max], as `refetchThePastSeconds` finds one before expiring it. */
    private fun band(
        min: Long,
        max: Long,
    ) = SyncCoverage.Band(mapOf(1 to SyncCoverage.Span(min, max, true)), max)

    @Test
    fun `a leg walking time the band already covers is the re-fetch, not the catch-up`() {
        // Real seconds: an unfloored leg walks from PLAUSIBLE_FLOOR, and a band never starts below it.
        val covered = band(min = 1_600_000_000, max = 1_700_000_000)

        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), covered))
        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1), since = 1_650_000_000, until = 1_660_000_000), covered))

        // The two ordinary legs touch the band exactly at its edges; a `<=` would file every catch-up as a re-walk.
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(1), since = 1_700_000_000), covered),
            "forward from the band's edge",
        )
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(1), until = 1_600_000_000), covered),
            "deeper than the band reaches",
        )
        assertTrue(
            VisitPool.rewalksCovered(Filter(kinds = listOf(1), since = 1_699_999_999), covered),
            "one second inside the band is inside it",
        )
    }

    @Test
    fun `each leg is judged against its OWN kinds, not the whole band's edges`() {
        val mixed =
            SyncCoverage.Band(
                mapOf(
                    // kind 30023's span sits inside kind 1's.
                    1 to SyncCoverage.Span(1_600_000_000, 1_700_000_000, true),
                    30023 to SyncCoverage.Span(1_650_000_000, 1_660_000_000, true),
                ),
                1_700_000_000,
            )

        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), since = 1_660_000_000), mixed),
            "forward from this kind's own edge is a catch-up, whatever the other kinds cover",
        )
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), until = 1_650_000_000), mixed),
            "deeper than this kind reaches is a catch-up too",
        )
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(9735)), mixed),
            "a kind with no span of its own has not been walked, band or no band",
        )

        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1, 30023)), mixed))
        assertTrue(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), since = 1_655_000_000), mixed),
            "inside this kind's own span is a re-walk",
        )
    }

    @Test
    fun `nothing recorded yet is a first walk, and a first walk is a catch-up`() {
        assertFalse(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), null))
        // A band with no spans is what a restored file with an unreadable entry leaves behind.
        assertFalse(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), SyncCoverage.Band(emptyMap(), 0)))
    }

    @Test
    fun `the unit of work is a relay AND a stream, so one relay is many units`() {
        val a = VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "content")
        val b = VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "indexers")
        assertTrue(a != b, "same relay, different stream, different unit")
        assertEquals(a, VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "content"))
        // Value semantics: every queue and pool collection is keyed by it.
        assertEquals(a.hashCode(), VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example/"), "content").hashCode())
        assertTrue(setOf(a, b).size == 2)
    }

    @Test
    fun `the pool's width is the sum of what its streams allow themselves`() {
        val cfg =
            RouterConfigLoader.parse(
                """
                streams {
                    a {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        urls   = ["wss://a.example"]
                        visitConcurrency = 8
                    }
                    b {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        urls   = ["wss://b.example"]
                        visitConcurrency = 32
                    }
                }
                """.trimIndent(),
            )
        assertEquals(40, VisitPool.workersFor(cfg.streams))

        // A stream naming no width gets the router-wide default.
        val silent =
            RouterConfigLoader.parse(
                """
                streams {
                    a {
                        dir    = "down"
                        filter = { "kinds": [1] }
                        urls   = ["wss://a.example"]
                    }
                }
                """.trimIndent(),
            )
        assertEquals(RouterConfig.DEFAULT_VISIT_CONCURRENCY, VisitPool.workersFor(silent.streams))
        assertEquals(1, VisitPool.workersFor(emptyList()))
    }

    @Test
    fun `the four pool words are the wire's, and the glossary defines every one of them`() {
        // `poolsOf` in `web/shared/sync.js` groups the page's tables by these strings.
        val words = listOf(VisitPool.POOL_LIVE, VisitPool.POOL_CATCHING_UP, VisitPool.POOL_REFETCHING, VisitPool.POOL_NEGENTROPY)
        assertEquals(listOf("live", "catching-up", "re-fetching", "negentropy"), words)

        val defined = StatusVocabulary.TERMS["pool"]!!.jsonPrimitive.content
        for (word in words) {
            assertTrue("`$word`" in defined, "the glossary's `pool` entry does not name $word, so a reader meets it undefined")
        }
    }
}
