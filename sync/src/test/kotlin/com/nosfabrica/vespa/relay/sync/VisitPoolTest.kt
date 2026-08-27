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
 * The visit model's two pieces of pure arithmetic: which streams ride the
 * rotating pool, and when a relay's history is due its audit. Everything else
 * in [VisitPool] is sockets and clocks, which the probes cover.
 */
class VisitPoolTest {
    @Test
    fun `more content lately means a sooner revisit, on both bases`() {
        // The priority rule: yield divides the wait. Fifty decayed events
        // halves it, five hundred pins it near the floor — and the tailed
        // base stays above the untailed one at every score, because a tail
        // is already carrying that relay's present.
        val quietTailed = VisitPool.revisitDelayMs(0.0, tailed = true)
        val quietUntailed = VisitPool.revisitDelayMs(0.0, tailed = false)
        assertEquals(VisitPool.REVISIT_TAILED_MS, quietTailed)
        assertEquals(VisitPool.REVISIT_UNTAILED_MS, quietUntailed)
        assertEquals(quietTailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = true))
        assertEquals(quietUntailed / 2, VisitPool.revisitDelayMs(VisitPool.YIELD_HALVES_THE_WAIT, tailed = false))
        // Five hundred decayed events takes an untailed relay all the way to
        // the floor — base/11 sits under it — while the tailed base, six times
        // longer, still has room to divide.
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(500.0, tailed = false))
        assertTrue(VisitPool.revisitDelayMs(500.0, tailed = true) > VisitPool.revisitDelayMs(500.0, tailed = false))
    }

    @Test
    fun `a firehose relay is a frequent guest, never a busy loop`() {
        // The floor: however much a relay delivers, its revisit never drops
        // under a minute — the queue wait and the visit itself are the real
        // pacing below that, and a zero delay would be a spin on one relay.
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = false))
        assertEquals(VisitPool.REVISIT_FLOOR_MS, VisitPool.revisitDelayMs(1e9, tailed = true))
    }

    @Test
    fun `every dynamic stream rides the pool, verdicts, gated scans, retracting streams`() {
        // The fork is gone with the engine it forked to: the loader refuses
        // an ungated scan outright, so every dynamic stream that parses
        // rides the pool. Three shapes, one per rule: a verdict source; a
        // gated scan; a retracting stream whose deleteMissing comparison
        // runs as its negentropySyncThePastSeconds reconcile.
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
        // As a SET: HOCON hands back a map, and the block order is not a promise.
        assertEquals(setOf("pure", "gatedScan", "retracting"), visit.map { it.name }.toSet())
        assertEquals(604_800L, visit.single { it.name == "pure" }.negentropySyncThePastSeconds)
    }

    @Test
    fun `a declared-urls stream rides the pool too, and an up stream does not`(): Unit =
        runBlocking {
            // The crossing: `urls` used to mean the legacy backfill, which
            // walked each relay once per process and then live-tailed — so
            // neither clock that re-checks the past could mean anything there.
            // Where a relay came from is the only difference between the two
            // kinds of stream now; everything after the roster is one policy.
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

            // …and its urls reach the roster, which is what makes the two
            // clocks above real: the pool visits them like any other relay.
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
                    .single()
                    .filter.kinds,
                "and it carries the stream's own filter, unnarrowed",
            )
        }

    @Test
    fun `a retracting pool stream must say when its comparison runs`() {
        // The deleteMissing comparison IS that reconcile, so a
        // pool-shaped retracting stream without the knob has no clock for the
        // one decision that destroys data — refused where it is typed.
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
        // quartz already names why each walk ended; this is believing it. A
        // refusal ends the whole visit rather than re-opening the same
        // conversation once per remaining ask — an idle window of silence
        // apiece.
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
        // DRAINED is the relay honestly EOSEing an empty page — the one ending
        // that proves absence — and LIMIT_REACHED stopped on our own
        // instruction. Neither says the next ask is futile. And a walk that
        // carried events did real work whatever ended it: a CLOSED after 4,000
        // events is a rate limit, not a dead relay.
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.DRAINED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(0, PagedFetchResult.End.LIMIT_REACHED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(4_000, PagedFetchResult.End.CLOSED)))
        assertFalse(VisitPool.refusedOutright(PagedFetchResult(1, PagedFetchResult.End.IDLE)))
    }

    @Test
    fun `one ask per bound author, the pairing the tags already made`() {
        // authorsPerLeg = 1 made structural: a relay two providers name
        // yields two single-author filters — each its own immortal band —
        // and a third provider later is a third ask beside them, never an
        // invalidation. Other narrow keys ride along in every split.
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
        // quartz's Filter has no equals, so the old `.distinct()` kept every
        // per-author filter and a relay paired with N providers got an
        // N-filter REQ — tens of KB that filter-capped relays refuse whole.
        // Merged by shape: bound asks union their authors, an unbound ask
        // absorbs its shape's bound ones, and different shapes stay apart.
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
        // The pool compares one stream's want set on a url across rebuilds —
        // `wantsAtOpen == wantsNow` in `openTail` — and both directions of
        // that comparison carry a failure mode. quartz's Filter has no equals,
        // so the fresh-but-identical filters every rebuild derives MUST
        // compare equal, or each roster tick re-opens every tail and the
        // revisit pacing collapses. And one more bound author MUST compare
        // different, or the new ask waits out the tailed revisit base for its
        // first catch-up, its tail filter and its retraction audit — which is
        // how a staged phantom sat undeleted for half an hour on a relay
        // another stream already tailed.
        //
        // Asserted on the EXPRESSION the roster builds and the pool compares,
        // which is the filter's own JSON. It used to run against a companion
        // helper that prefixed the stream name — a shape nothing built once
        // the want map became url → stream → filters, so the test went on
        // passing about code no caller had.
        fun wants(vararg authors: List<String>?) = authors.mapTo(mutableSetOf()) { Filter(kinds = listOf(30382), authors = it).toJson() }

        val p1 = "a".repeat(64)
        val p2 = "b".repeat(64)
        assertEquals(wants(listOf(p1)), wants(listOf(p1)), "same shape, fresh instances — not news")
        assertTrue(wants(listOf(p1)) != wants(listOf(p1), listOf(p2)), "a new bound author is news")
        // …and the stream is not in the string because it does not have to be:
        // a want set is stored under its stream's own key, so one stream's set
        // is only ever compared with its own predecessor. Two streams asking
        // the identical filter is the case that proves it — the sets are equal
        // and they are still two tails, one per (relay, stream) pair.
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
        // THE READING THIS SEPARATES. Both walks are `fetchAllPages` over a
        // REQ, both fill the same rows, and one is a mirror keeping up while
        // the other is the same mirror re-downloading years of history because
        // `refetchThePastSeconds` expired the band. `visiting: 100` counted
        // them as one number, and so did the in-flight row's stage word.
        // Real seconds, because one of the edges IS a real second: an unfloored
        // leg is walked as `flooredForPaging`, from `PLAUSIBLE_FLOOR`, and a
        // recorded band can never start below that — quartz refuses to observe
        // an implausible `created_at` in the first place. A band written with
        // toy numbers would sit UNDER the floor and every case below would
        // answer backwards.
        val covered = band(min = 1_600_000_000, max = 1_700_000_000)

        // The expired band's leg: the whole filter again, unfloored at both
        // ends, straight through everything already recorded.
        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), covered))
        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1), since = 1_650_000_000, until = 1_660_000_000), covered))

        // …and the two ordinary legs, which TOUCH the band exactly at its
        // edges: quartz asks for the newer one from the band's max and the
        // older one down to its min. A `<=` on either side here would file
        // every routine catch-up on this deployment as a re-walk of everything.
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
        // THE BUG THIS PINS. A band's `minCreatedAt`/`maxCreatedAt` are
        // aggregates over every kind in it, but quartz emits one leg per kind
        // GROUP, each windowed on that group's own span. On a stream over many
        // kinds — contentViaOutbox rides ~130 — the kinds do not cover the same
        // time, so a leg walking forward from its own kind's edge lands inside
        // the aggregate and reads as a re-walk of everything.
        //
        // That is not a labelling nit: a leg classified as a re-fetch takes a
        // `refetchConcurrency` permit and is SKIPPED when the cap is full, and
        // that cap is small on purpose (4 against 96 visits in the shipped
        // example). So ordinary catch-up on a multi-kind stream was throttled
        // to the re-fetch budget and silently dropped past it.
        val mixed =
            SyncCoverage.Band(
                mapOf(
                    // kind 1 walked back to 2020 and forward to 2023…
                    1 to SyncCoverage.Span(1_600_000_000, 1_700_000_000, true),
                    // …kind 30023 only ever seen in a window inside that.
                    30023 to SyncCoverage.Span(1_650_000_000, 1_660_000_000, true),
                ),
                1_700_000_000,
            )

        // The ordinary forward leg for 30023: from ITS OWN edge, which is a
        // hundred million seconds below the band's aggregate max.
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), since = 1_660_000_000), mixed),
            "forward from this kind's own edge is a catch-up, whatever the other kinds cover",
        )
        // …and the older leg for it, likewise inside the aggregate.
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), until = 1_650_000_000), mixed),
            "deeper than this kind reaches is a catch-up too",
        )
        // A kind with nothing recorded at all has never been walked, so its
        // whole range is new — even though the band it shares is full.
        assertFalse(
            VisitPool.rewalksCovered(Filter(kinds = listOf(9735)), mixed),
            "a kind with no span of its own has not been walked, band or no band",
        )

        // …and the re-walk still reads as one: the expired band's leg carries
        // the whole ask again, over kinds whose spans it genuinely re-covers.
        assertTrue(VisitPool.rewalksCovered(Filter(kinds = listOf(1, 30023)), mixed))
        assertTrue(
            VisitPool.rewalksCovered(Filter(kinds = listOf(30023), since = 1_655_000_000), mixed),
            "inside this kind's own span is a re-walk",
        )
    }

    @Test
    fun `nothing recorded yet is a first walk, and a first walk is a catch-up`() {
        // An ask with no band has never been walked, so its whole range is new
        // — reading that as a re-fetch would put every fresh relay on the
        // deployment into the pool that means "re-downloading history we have".
        assertFalse(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), null))
        // …and the same for a band carrying no spans, which is the shape a
        // restored file with an unreadable entry leaves behind. It has no min
        // or max to compare against either, which is the other reason this
        // answers before reading them.
        assertFalse(VisitPool.rewalksCovered(Filter(kinds = listOf(1)), SyncCoverage.Band(emptyMap(), 0)))
    }

    @Test
    fun `the unit of work is a relay AND a stream, so one relay is many units`() {
        // THE INVARIANT. Many streams may work one relay at once — they share
        // its socket and touch disjoint bands — while each stream sees that
        // relay in one state at a time. So the pair is what is queued,
        // visited and revisited, and a relay two streams want is TWO units on
        // two independent clocks.
        val a = VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "content")
        val b = VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "indexers")
        assertTrue(a != b, "same relay, different stream, different unit")
        assertEquals(a, VisitPool.VisitKey(RelayUrlNormalizer.normalize("wss://a.example"), "content"))
        // Value semantics, because every collection in the queue and the pool
        // is keyed by it — a unit rebuilt from a roster read must find the
        // tail and the timer the last one left.
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
        // Fewer workers than the shares add up to would leave a configured
        // share unreachable: a stream allowed 32 visits cannot have them if
        // only 8 workers exist to draw its relays.
        assertEquals(40, VisitPool.workersFor(cfg.streams))

        // A stream that names no width stands for the number the router-wide
        // setting used to default to, so a deployment that configures nothing
        // runs exactly the pool it always did.
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
        // …and a router with no visit streams still has a pool it can start.
        assertEquals(1, VisitPool.workersFor(emptyList()))
    }

    @Test
    fun `the four pool words are the wire's, and the glossary defines every one of them`() {
        // These four strings ARE the contract: the page groups its four tables
        // by them (`poolsOf` in `web/shared/sync.js`) and a reader looks them
        // up in the document's own glossary. Renaming one here without the
        // other two would empty a table on the page and leave the word it drew
        // undefined — the same silent break the pool/stage split exists to
        // stop, one level up.
        val words = listOf(VisitPool.POOL_LIVE, VisitPool.POOL_CATCHING_UP, VisitPool.POOL_REFETCHING, VisitPool.POOL_NEGENTROPY)
        assertEquals(listOf("live", "catching-up", "re-fetching", "negentropy"), words)

        val defined = StatusVocabulary.TERMS["pool"]!!.jsonPrimitive.content
        for (word in words) {
            assertTrue("`$word`" in defined, "the glossary's `pool` entry does not name $word, so a reader meets it undefined")
        }
    }
}
