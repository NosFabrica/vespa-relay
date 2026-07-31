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

import com.vitorpamplona.quartz.eventstore.store.NostrEventStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The dynamic relay list: what [RelayDiscovery] reads out of the store, driven
 * entirely by a [RelaySource]'s filter and its [RelaySelect]s rather than by
 * per-kind code.
 */
class RelayDiscoveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun select(
        tag: String? = null,
        index: Int = 1,
        kind: Int? = null,
        where: List<TagCondition> = emptyList(),
    ) = RelaySelect(kind = kind, tag = tag, index = index, where = where)

    /** What `marker = <value>` desugars to: marked that side, marked "", or no marker slot. */
    private fun marker(
        value: String,
        urlIndex: Int = 1,
    ) = listOf(
        TagCondition(index = urlIndex + 1, equals = value),
        TagCondition(index = urlIndex + 1, equals = ""),
        TagCondition(maxSize = urlIndex + 1),
    )

    private fun source(
        vararg kinds: Int,
        selects: List<RelaySelect>,
        limit: Int? = null,
    ) = RelaySource(selects = selects, filter = Filter(kinds = kinds.toList(), limit = limit))

    private fun dynamic(
        vararg sources: RelaySource,
        exclude: Set<String> = emptySet(),
    ) = DynamicRelayList(
        sources = sources.toList(),
        refreshSeconds = 3_600,
        concurrency = 4,
        exclude = exclude.map { RelayUrlNormalizer.normalize(it) }.toSet(),
    )

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = NostrSignerSync().sign(1_700_000_000L, kind, arrayOf(*tags), "")

    private fun urls(
        event: Event,
        select: RelaySelect,
    ) = RelayDiscovery.urlsIn(event, select).map { it.url }

    // ---- extraction --------------------------------------------------------

    @Test
    fun `NIP-65 keeps write and unmarked relays and drops read-only ones`() {
        val list =
            event(
                10002,
                arrayOf("r", "wss://write.example", "write"),
                arrayOf("r", "wss://read.example", "read"),
                arrayOf("r", "wss://both.example"),
            )

        val write = urls(list, select(tag = "r", where = marker("write")))
        assertTrue(write.any { it.contains("write.example") })
        assertTrue(write.any { it.contains("both.example") }, "an unmarked r tag is read AND write")
        assertFalse(write.any { it.contains("read.example") })

        val read = urls(list, select(tag = "r", where = marker("read")))
        assertTrue(read.any { it.contains("read.example") })
        assertTrue(read.any { it.contains("both.example") })
        assertFalse(read.any { it.contains("write.example") })

        // No where at all: every relay the tag names.
        assertEquals(3, urls(list, select(tag = "r")).size)
    }

    @Test
    fun `index reads NIP-85 service tags, which put the pubkey first`() {
        val list =
            event(
                10040,
                arrayOf("30382:rank", "a".repeat(64), "wss://scores.example"),
                arrayOf("30382:followers", "b".repeat(64), "wss://graph.example"),
            )

        // A named tag picks exactly one service...
        assertEquals(listOf("wss://scores.example/"), urls(list, select(tag = "30382:rank", index = 2)))
        // ...and no tag name takes the whole <kind>:<type> family.
        assertEquals(
            listOf("wss://scores.example/", "wss://graph.example/"),
            urls(list, select(index = 2)),
        )
    }

    @Test
    fun `relay hints on e and p tags are just another select`() {
        val note =
            event(
                1,
                arrayOf("e", "f".repeat(64), "wss://hint.example", "root"),
                arrayOf("p", "a".repeat(64), "wss://inbox.example"),
                arrayOf("t", "nostr"),
            )

        assertEquals(listOf("wss://hint.example/"), urls(note, select(tag = "e", index = 2)))
        assertEquals(listOf("wss://inbox.example/"), urls(note, select(tag = "p", index = 2)))
    }

    @Test
    fun `the relay tag family used by every NIP-51 list needs no special casing`() {
        val dmRelays = event(10050, arrayOf("relay", "wss://dm.example"))
        val relaySet = event(30002, arrayOf("relay", "wss://set.example"))
        val monitor = event(30166, arrayOf("d", "wss://monitored.example"))

        assertEquals(listOf("wss://dm.example/"), urls(dmRelays, select(tag = "relay")))
        assertEquals(listOf("wss://set.example/"), urls(relaySet, select(tag = "relay")))
        assertEquals(listOf("wss://monitored.example/"), urls(monitor, select(tag = "d")))
    }

    @Test
    fun `a named tag tolerates junk, repeats, and scheme-less hosts`() {
        val list =
            event(
                10002,
                // `equals` is exact, so "WRITE" is not a write marker — NIP-65
                // specifies lowercase. The url survives via its lowercase twin.
                arrayOf("r", "wss://write.example", "WRITE"),
                arrayOf("r", "wss://write.example", "write"),
                arrayOf("r", "relay.example"), // scheme-less: the normalizer fixes it
                arrayOf("r"),
                arrayOf("p", "wss://not-an-r-tag.example"),
                // The normalizer would turn these into `wss://not/`; we don't let it.
                arrayOf("r", "not a url at all"),
                arrayOf("r", "   "),
            )

        assertEquals(
            listOf("wss://write.example/", "wss://relay.example/"),
            urls(list, select(tag = "r", where = marker("write"))),
        )
    }

    @Test
    fun `where entries OR together and each ANDs its own fields`() {
        val note =
            event(
                1,
                arrayOf("e", "a".repeat(64), "wss://root.example", "root"),
                arrayOf("e", "b".repeat(64), "wss://reply.example", "reply"),
                arrayOf("e", "c".repeat(64), "wss://mention.example", "mention"),
                arrayOf("e", "d".repeat(64), "wss://bare.example"),
            )

        // Two OR-ed alternatives keep root and reply hints, dropping the rest.
        val threaded =
            select(
                tag = "e",
                index = 2,
                where =
                    listOf(
                        TagCondition(index = 3, equals = "root"),
                        TagCondition(index = 3, equals = "reply"),
                    ),
            )
        assertEquals(listOf("wss://root.example/", "wss://reply.example/"), urls(note, threaded))

        // Fields inside one entry AND: the marker must say root AND the tag must
        // stop right there, so a root tag with anything after it is out.
        val strictRoot =
            select(
                tag = "e",
                index = 2,
                where = listOf(TagCondition(index = 3, equals = "root", maxSize = 4)),
            )
        assertEquals(listOf("wss://root.example/"), urls(note, strictRoot))
    }

    @Test
    fun `minSize and maxSize split marked from unmarked tags`() {
        val note =
            event(
                1,
                arrayOf("e", "a".repeat(64), "wss://marked.example", "root"),
                arrayOf("e", "b".repeat(64), "wss://bare.example"),
            )

        // Has a marker, whatever it says.
        assertEquals(
            listOf("wss://marked.example/"),
            urls(note, select(tag = "e", index = 2, where = listOf(TagCondition(minSize = 4)))),
        )
        // Structurally has no marker slot.
        assertEquals(
            listOf("wss://bare.example/"),
            urls(note, select(tag = "e", index = 2, where = listOf(TagCondition(maxSize = 3)))),
        )
    }

    @Test
    fun `equals never matches an element that does not exist`() {
        val list =
            event(
                10002,
                arrayOf("r", "wss://bare.example"),
                arrayOf("r", "wss://empty.example", ""),
            )

        // An absent marker slot is not an empty string: `equals = ""` demands the
        // slot exists. The structural case is maxSize's job.
        assertEquals(
            listOf("wss://empty.example/"),
            urls(list, select(tag = "r", where = listOf(TagCondition(index = 2, equals = "")))),
        )
        assertEquals(
            listOf("wss://bare.example/"),
            urls(list, select(tag = "r", where = listOf(TagCondition(maxSize = 2)))),
        )
    }

    @Test
    fun `with no tag name only values that say they are relays are taken`() {
        val list =
            event(
                10040,
                arrayOf("30382:rank", "a".repeat(64), "wss://scores.example"),
                // Without a tag name to filter on, a pet name would otherwise
                // normalize to `wss://petname/` and enter the fan-out.
                arrayOf("p", "b".repeat(64), "petname"),
            )

        assertEquals(listOf("wss://scores.example/"), urls(list, select(index = 2)))
    }

    // ---- discovery ---------------------------------------------------------

    @Test
    fun `discovery keeps every relay, ordered by how many tags name it`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            // popular: 3 lists, quiet: 2, lonely: 1.
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://quiet.example", "write")))
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://quiet.example")))
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://lonely.example", "write")))

            // Nothing is dropped for being unpopular — the one-list relay is synced
            // like the rest; the count only decides who goes first.
            val all =
                RelayDiscovery.discover(
                    store,
                    dynamic(source(10002, selects = listOf(select(tag = "r", where = marker("write"))))),
                )
            assertEquals(
                listOf("wss://popular.example/" to 3, "wss://quiet.example/" to 2, "wss://lonely.example/" to 1),
                all.map { it.url.url to it.references },
            )
        }

    @Test
    fun `one scan feeds several selects, and their counts add up`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, arrayOf("r", "wss://shared.example", "write")))
            store.insert(event(10040, arrayOf("30382:rank", "a".repeat(64), "wss://shared.example")))
            store.insert(event(10050, arrayOf("relay", "wss://dm.example")))

            // One filter over all three kinds; the selects sort out which is which.
            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(
                        source(
                            10002,
                            10040,
                            10050,
                            selects =
                                listOf(
                                    select(kind = 10002, tag = "r", where = marker("write")),
                                    select(kind = 10040, tag = "30382:rank", index = 2),
                                    // No kind: applies to everything the scan collected.
                                    select(tag = "relay"),
                                ),
                        ),
                    ),
                )

            // A relay two events name outranks one only a single event names.
            assertEquals(
                listOf("wss://shared.example/" to 2, "wss://dm.example/" to 1),
                found.map { it.url.url to it.references },
            )
        }

    @Test
    fun `a select bound to a kind never reads another kind's tags`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            // Both carry an `r` tag, but only the 10002 is a relay list.
            store.insert(event(10002, arrayOf("r", "wss://list.example", "write")))
            store.insert(event(10006, arrayOf("r", "wss://blocked.example")))

            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(source(10002, 10006, selects = listOf(select(kind = 10002, tag = "r")))),
                )
            assertEquals(listOf("wss://list.example/"), found.map { it.url.url })
        }

    @Test
    fun `exclude and the caller's skip set are the only things dropped`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(
                event(
                    10002,
                    arrayOf("r", "wss://popular.example", "write"),
                    arrayOf("r", "wss://quiet.example", "write"),
                    arrayOf("r", "wss://lonely.example", "write"),
                ),
            )

            val kept =
                RelayDiscovery.discover(
                    store,
                    dynamic(
                        source(10002, selects = listOf(select(tag = "r", where = marker("write")))),
                        exclude = setOf("wss://popular.example"),
                    ),
                    skip = setOf(RelayUrlNormalizer.normalize("wss://quiet.example")),
                )
            assertEquals(listOf("wss://lonely.example/"), kept.map { it.url.url })
        }

    @Test
    fun `a paged scan sees every event, across page boundaries and repeated timestamps`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            // 12 lists over 4 distinct timestamps, so pages of 2 land mid-run of
            // events sharing a created_at — the case `until` being inclusive makes
            // awkward, and the case a naive cursor either loops on or skips.
            repeat(12) { i ->
                store.insert(
                    NostrSignerSync().sign<Event>(
                        1_700_000_000L + (i % 4),
                        10002,
                        arrayOf(arrayOf("r", "wss://relay$i.example", "write")),
                        "",
                    ),
                )
            }

            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(source(10002, selects = listOf(select(tag = "r")))),
                    pageSize = 2,
                )
            assertEquals(12, found.size, "every event must be seen exactly once, whatever the page size")
            assertEquals(List(12) { 1 }, found.map { it.references }, "no event counted twice across a page boundary")
        }

    @Test
    fun `a scan limit is a budget for the whole scan, not per page`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            repeat(10) { i ->
                store.insert(
                    NostrSignerSync().sign<Event>(
                        1_700_000_000L + i,
                        10002,
                        arrayOf(arrayOf("r", "wss://relay$i.example", "write")),
                        "",
                    ),
                )
            }

            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(source(10002, selects = listOf(select(tag = "r")), limit = 4)),
                    pageSize = 2,
                )
            assertEquals(4, found.size, "the limit bounds the scan across pages, it does not multiply by them")
        }

    @Test
    fun `an empty store discovers nothing`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            assertTrue(RelayDiscovery.discover(store, dynamic(source(10002, selects = listOf(select(tag = "r"))))).isEmpty())
        }
}
