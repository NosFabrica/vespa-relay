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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.BindingSlot
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.config.TagCondition
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What [RelayDiscovery] reads out of the store, driven entirely by a
 * [RelaySource]'s filter and its [RelaySelect]s rather than by per-kind code.
 */
class RelayDiscoveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun select(
        tag: String? = null,
        index: Int = 1,
        kind: Int? = null,
        where: List<TagCondition> = emptyList(),
    ) = RelaySelect(kind = kind, tag = tag, urlIndex = index, where = where)

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
        maxRelaysPerList: Int? = null,
    ) = RelayDiscoveryConfig(
        sources = sources.toList(),
        refreshSeconds = 3_600,
        // Parsed the way the loader does, so the entry and case rules are production's.
        exclude = RelayExcludes.parse(exclude.toList()),
        maxRelaysPerList = maxRelaysPerList,
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

        assertEquals(listOf("wss://scores.example/"), urls(list, select(tag = "30382:rank", index = 2)))
        // No tag name takes the whole <kind>:<type> family.
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
    fun `a named tag tolerates junk and repeats, and now demands a ws scheme`() {
        val list =
            event(
                10002,
                // `equals` is exact and NIP-65 specifies lowercase; the url survives via its twin.
                arrayOf("r", "wss://write.example", "WRITE"),
                arrayOf("r", "wss://write.example", "write"),
                // Scheme-less: the normalizer would coerce it to wss://, and a relay list holds whatever its author typed.
                arrayOf("r", "relay.example"),
                arrayOf("r", "http://relay.example"), // right host, wrong protocol
                arrayOf("r"),
                arrayOf("p", "wss://not-an-r-tag.example"),
                // The normalizer would turn these into `wss://not/`.
                arrayOf("r", "not a url at all"),
                arrayOf("r", "   "),
            )

        assertEquals(
            listOf("wss://write.example/"),
            urls(list, select(tag = "r", where = marker("write"))),
        )
    }

    @Test
    fun `urls we could never dial are dropped rather than discovered`() {
        // No Tor transport here, and a loopback host in someone else's list means their
        // machine. A certain failure kept in the list burns a timeout and a permit every cycle.
        val list =
            event(
                10002,
                arrayOf("r", "wss://real.example"),
                arrayOf("r", "wss://sc7l4cy2s3sfxbqiz4ntxdpsjfsrijgcdpwcsuqxpwkyoqgnzcvfmuad.onion"),
                arrayOf("r", "ws://localhost:4869"),
                arrayOf("r", "ws://127.0.0.1:7777"),
            )

        assertEquals(listOf("wss://real.example/"), urls(list, select(tag = "r")))
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
                // NIP-10 also allows a pubkey after the marker: the tag that proves maxSize cuts inside an AND.
                arrayOf("e", "e".repeat(64), "wss://deep.example", "root", "f".repeat(64)),
            )

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
        assertEquals(listOf("wss://root.example/", "wss://reply.example/", "wss://deep.example/"), urls(note, threaded))

        // The marker must say root and the tag must stop there, so the root tag with a pubkey after it is out.
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

        // An absent slot is not an empty string: `equals = ""` demands the slot exists.
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
                // Without a tag name to filter on, a pet name would normalize to `wss://petname/`.
                arrayOf("p", "b".repeat(64), "petname"),
            )

        assertEquals(listOf("wss://scores.example/"), urls(list, select(index = 2)))
    }

    @Test
    fun `a default port is dropped, so one relay is not dialled twice`() {
        val list =
            event(
                10002,
                arrayOf("r", "wss://relay.example:443"),
                arrayOf("r", "wss://relay.example"),
                arrayOf("r", "ws://plain.example:80/alpha"),
                // Not a default for its scheme: a different listener.
                arrayOf("r", "wss://relay.example:4443"),
            )

        val found = urls(list, select(tag = "r"))

        assertEquals(listOf("wss://relay.example/", "ws://plain.example/alpha", "wss://relay.example:4443/"), found)
    }

    @Test
    fun `an ipv6 literal keeps the colons that are not a port`() {
        val list = event(10002, arrayOf("r", "ws://[2001:db8::1]:80/alpha"), arrayOf("r", "ws://[2001:db8::2]/beta"))

        assertEquals(listOf("ws://[2001:db8::1]/alpha", "ws://[2001:db8::2]/beta"), urls(list, select(tag = "r")))
    }

    // ---- oversized lists ---------------------------------------------------

    @Test
    fun `a relay list too long to be one is dropped whole`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, *Array(12) { arrayOf("r", "wss://bulk$it.example") }))
            store.insert(event(10002, arrayOf("r", "wss://ordinary.example")))

            val found =
                RelayDiscovery
                    .discover(store, dynamic(source(10002, selects = listOf(select(tag = "r"))), maxRelaysPerList = 10))
                    .map { it.url.url }

            // None of the bulk list, not its first ten: a prefix lets the author choose which relays we see.
            assertEquals(listOf("wss://ordinary.example/"), found)
        }

    @Test
    fun `the cap counts the relays a select reads, not every tag on the event`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // A NIP-51 set: dozens of `p` tags around two relays.
            store.insert(
                event(
                    30002,
                    *Array(50) { arrayOf("p", "%064d".format(it)) },
                    arrayOf("relay", "wss://one.example"),
                    arrayOf("relay", "wss://two.example"),
                ),
            )

            val found =
                RelayDiscovery
                    .discover(store, dynamic(source(30002, selects = listOf(select(tag = "relay"))), maxRelaysPerList = 10))
                    .map { it.url.url }

            assertEquals(listOf("wss://one.example/", "wss://two.example/"), found)
        }

    @Test
    fun `the cap does not count relays the select's where rejects`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // 200 read-only relays and three write ones: judged on the relays it names to us, an ordinary list.
            store.insert(
                event(
                    10002,
                    *Array(200) { arrayOf("r", "wss://read$it.example", "read") },
                    arrayOf("r", "wss://w1.example", "write"),
                    arrayOf("r", "wss://w2.example", "write"),
                    arrayOf("r", "wss://w3.example", "write"),
                ),
            )

            val found =
                RelayDiscovery
                    .discover(
                        store,
                        dynamic(
                            source(10002, selects = listOf(select(tag = "r", where = marker("write")))),
                            maxRelaysPerList = 50,
                        ),
                    ).map { it.url.url }

            assertEquals(listOf("wss://w1.example/", "wss://w2.example/", "wss://w3.example/"), found.sorted())
        }

    @Test
    fun `no cap configured reads every list however long`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, *Array(12) { arrayOf("r", "wss://bulk$it.example") }))

            val found = RelayDiscovery.discover(store, dynamic(source(10002, selects = listOf(select(tag = "r")))))

            assertEquals(12, found.size)
        }

    // ---- bindings ----------------------------------------------------------

    @Test
    fun `a bound author stays with the relay from its own tag, not every relay`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val rankSvc = "a".repeat(64)
            val followSvc = "b".repeat(64)
            // One observer, two dimensions, two different relays: the case a cross product gets wrong.
            store.insert(
                event(
                    10040,
                    arrayOf("30382:rank", rankSvc, "wss://rank.example"),
                    arrayOf("30382:followers", followSvc, "wss://follow.example"),
                ),
            )

            val found =
                RelayDiscovery
                    .discover(
                        store,
                        dynamic(
                            source(
                                10040,
                                selects =
                                    listOf(
                                        RelaySelect(kind = null, tag = "30382:rank", urlIndex = 2, bindings = mapOf("authors" to BindingSlot.OfTag(1))),
                                        RelaySelect(kind = null, tag = "30382:followers", urlIndex = 2, bindings = mapOf("authors" to BindingSlot.OfTag(1))),
                                    ),
                            ),
                        ),
                    ).associateBy { it.url.url }

            assertEquals(setOf(rankSvc), found["wss://rank.example/"]?.bindings?.get("authors"))
            assertEquals(setOf(followSvc), found["wss://follow.example/"]?.bindings?.get("authors"))
        }

    @Test
    fun `a relay named by several tags collects every author it was paired with`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val one = "1".repeat(64)
            val two = "2".repeat(64)
            store.insert(event(10040, arrayOf("30382:rank", one, "wss://shared.example")))
            store.insert(event(10040, arrayOf("30382:rank", two, "wss://shared.example")))

            val found =
                RelayDiscovery
                    .discover(
                        store,
                        dynamic(
                            source(
                                10040,
                                selects = listOf(RelaySelect(kind = null, tag = "30382:rank", urlIndex = 2, bindings = mapOf("authors" to BindingSlot.OfTag(1)))),
                            ),
                        ),
                    ).single()

            assertEquals(setOf(one, two), found.bindings["authors"])
            // Sorted into the filter, because the band is keyed on the filter's serialized form.
            assertEquals(listOf(one, two), found.narrowed(Filter(kinds = listOf(30382))).authors)
        }

    @Test
    fun `pubkey binds the scanned event's own author, which is the outbox model`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val list = event(10002, arrayOf("r", "wss://mine.example", "write"))
            store.insert(list)

            val found =
                RelayDiscovery
                    .discover(
                        store,
                        dynamic(
                            source(
                                10002,
                                selects =
                                    listOf(
                                        RelaySelect(
                                            kind = 10002,
                                            tag = "r",
                                            urlIndex = 1,
                                            where = marker("write"),
                                            bindings = mapOf("authors" to BindingSlot.EventPubkey),
                                        ),
                                    ),
                            ),
                        ),
                    ).single()

            // The author is nowhere in the tag.
            assertEquals(setOf(list.pubKey), found.bindings["authors"])
        }

    @Test
    fun `a tag that cannot fill a binding is dropped whole, not half-applied`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // No service slot, and a service that is not a key: half-applied, either
            // would widen the ask back to every author on that relay.
            store.insert(event(10040, arrayOf("30382:rank", "wss://short.example")))
            store.insert(event(10040, arrayOf("30382:rank", "not-a-pubkey", "wss://bogus.example")))

            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(
                        source(
                            10040,
                            selects = listOf(RelaySelect(kind = null, tag = "30382:rank", urlIndex = 2, bindings = mapOf("authors" to BindingSlot.OfTag(1)))),
                        ),
                    ),
                )

            assertTrue(found.none { it.url.url.contains("short.example") }, "a tag too short to carry the service names no relay")
            assertTrue(found.none { it.url.url.contains("bogus.example") }, "a service that is not 64 hex names no relay")
        }

    @Test
    fun `a select that binds nothing narrows nothing`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, arrayOf("r", "wss://plain.example", "write")))

            val found =
                RelayDiscovery
                    .discover(
                        store,
                        dynamic(source(10002, selects = listOf(select(tag = "r", where = marker("write"))))),
                    ).single()

            assertTrue(found.bindings.isEmpty())
            val base = Filter(kinds = listOf(0, 10002))
            assertEquals(base, found.narrowed(base))
        }

    // ---- discovery ---------------------------------------------------------

    @Test
    fun `discovery keeps every relay, ordered by how many tags name it`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // popular: 3 lists, quiet: 2, lonely: 1.
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://quiet.example", "write")))
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://quiet.example")))
            store.insert(event(10002, arrayOf("r", "wss://popular.example", "write"), arrayOf("r", "wss://lonely.example", "write")))

            val all =
                RelayDiscovery.discover(
                    store,
                    dynamic(source(10002, selects = listOf(select(tag = "r", where = marker("write"))))),
                )
            // Every named relay once, in name order: the scan answers with a set, so there is no count to order by.
            assertEquals(
                listOf("wss://lonely.example/", "wss://popular.example/", "wss://quiet.example/"),
                all.map { it.url.url },
            )
        }

    @Test
    fun `one scan feeds several selects, and their relays are unioned`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, arrayOf("r", "wss://shared.example", "write")))
            store.insert(event(10040, arrayOf("30382:rank", "a".repeat(64), "wss://shared.example")))
            store.insert(event(10050, arrayOf("relay", "wss://dm.example")))

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

            assertEquals(
                listOf("wss://dm.example/", "wss://shared.example/"),
                found.map { it.url.url },
            )
        }

    @Test
    fun `a select bound to a kind never reads another kind's tags`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
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
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
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
    fun `one exclude pattern drops every per-user url a host mints`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // The shape filter.nostr.wine produces: a url per user, so no literal exclude list could be complete.
            store.insert(
                event(
                    10002,
                    arrayOf("r", "wss://filter.nostr.wine/npub1aaa", "write"),
                    arrayOf("r", "wss://filter.nostr.wine/npub1bbb", "write"),
                    arrayOf("r", "wss://filter.nostr.wine", "write"),
                    arrayOf("r", "wss://keep.example", "write"),
                ),
            )

            val kept =
                RelayDiscovery.discover(
                    store,
                    dynamic(
                        source(10002, selects = listOf(select(tag = "r", where = marker("write")))),
                        exclude = setOf("wss://filter\\.nostr\\.wine/npub.*"),
                    ),
                )
            assertEquals(listOf("wss://filter.nostr.wine/", "wss://keep.example/"), kept.map { it.url.url })
        }

    @Test
    fun `a paged scan sees every event, across page boundaries and repeated timestamps`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            // 12 lists over 4 timestamps, so pages of 2 land mid-run of a shared
            // created_at: the case an inclusive `until` makes a naive cursor loop on or skip.
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
            assertEquals(
                12,
                found.map { it.url.url }.distinct().size,
                "no relay repeated across a page boundary",
            )
        }

    @Test
    fun `a scan limit is a budget for the whole scan, not per page`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
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

    /**
     * Without a Tor transport an onion url asks the local resolver for a name
     * only Tor can answer, which fails and tells the resolver who we sync with.
     */
    @Test
    fun `an onion relay is kept only when a Tor transport exists`() {
        val list =
            event(
                10002,
                arrayOf("r", "wss://clearnet.example"),
                arrayOf("r", "ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion"),
            )
        val select = select(tag = "r")

        assertEquals(
            listOf("wss://clearnet.example/"),
            RelayDiscovery.urlsIn(list, select).map { it.url },
        )
        assertEquals(
            listOf(
                "wss://clearnet.example/",
                "ws://vespa7iexampleonionaddressthatisnotreal7abcdefghijklmn.onion/",
            ),
            RelayDiscovery.urlsIn(list, select, allowOnion = true).map { it.url },
            "with Tor configured the onion url is an ordinary upstream",
        )
    }

    /** `ws://localhost` in someone else's relay list names their machine, and Tor changes nothing about that. */
    @Test
    fun `a Tor transport does not make loopback dialable`() {
        val list = event(10002, arrayOf("r", "ws://localhost:7777"))
        assertTrue(RelayDiscovery.urlsIn(list, select(tag = "r"), allowOnion = true).isEmpty())
    }

    @Test
    fun `an empty store discovers nothing`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            assertTrue(RelayDiscovery.discover(store, dynamic(source(10002, selects = listOf(select(tag = "r"))))).isEmpty())
        }

    /**
     * The fast lane asks about a handful of urls every tick. Bounded by `#d` the
     * read must give the same answer over that subset as the unbounded one.
     */
    @Test
    fun `the hold-out read can be bounded to the urls a caller is asking about`() =
        runBlocking {
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val record = RelayVerdictRecord(store, monitor)
            val dead = RelayUrlNormalizer.normalize("wss://corpse.example")
            val alsoDead = RelayUrlNormalizer.normalize("wss://elsewhere.example")
            val alive = RelayUrlNormalizer.normalize("wss://answering.example")
            for (url in listOf(dead, alsoDead)) {
                record.publishFitness(url, "dead", "nothing answered", pageable = null, nip77 = null)
            }
            record.publishFitness(alive, "prime", "answered at a settled anchor", pageable = null, nip77 = null)

            val authors = listOf(monitor.pubKey)
            val whole = RelayDiscovery.undialable(store, authors, maxAgeSeconds = 86_400)
            assertEquals(setOf(dead, alsoDead), whole, "unbounded, it is the whole dead set")

            // The dead url outside the ask is not returned: a bound that leaked it would
            // make the two reads disagree about a url the caller never mentioned.
            assertEquals(
                setOf(dead),
                RelayDiscovery.undialable(store, authors, maxAgeSeconds = 86_400, among = listOf(dead, alive)),
                "bounded, it answers about exactly the urls it was handed",
            )
            // Only `dead` holds a url out; `alias`, `inconsistent` and the rest were earned by answering.
            assertTrue(RelayDiscovery.undialable(store, authors, maxAgeSeconds = 86_400, among = listOf(alive)).isEmpty())
            // An empty ask is not an unbounded one: the lane's quiet tick must not read the whole dead set.
            assertTrue(RelayDiscovery.undialable(store, authors, maxAgeSeconds = 86_400, among = emptyList()).isEmpty())
        }
}
