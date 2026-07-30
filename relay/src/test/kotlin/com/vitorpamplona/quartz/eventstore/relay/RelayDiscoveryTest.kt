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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The dynamic relay list: what [RelayDiscovery] reads out of the store, driven
 * entirely by a [RelaySource]'s kind/tag/urlIndex rather than per-kind code.
 */
class RelayDiscoveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun source(
        kind: Int,
        tag: String? = null,
        urlIndex: Int = 1,
        role: RelayRole? = null,
    ) = RelaySource(kind = kind, tag = tag, urlIndex = urlIndex, role = role, sinceSeconds = 0)

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
        source: RelaySource,
    ) = RelayDiscovery.urlsIn(event, source).map { it.url }

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

        val write = urls(list, source(10002, tag = "r", role = RelayRole.WRITE))
        assertTrue(write.any { it.contains("write.example") })
        assertTrue(write.any { it.contains("both.example") }, "an unmarked r tag is read AND write")
        assertFalse(write.any { it.contains("read.example") })

        val read = urls(list, source(10002, tag = "r", role = RelayRole.READ))
        assertTrue(read.any { it.contains("read.example") })
        assertTrue(read.any { it.contains("both.example") })
        assertFalse(read.any { it.contains("write.example") })

        // No marker configured at all: every relay the tag names.
        assertEquals(3, urls(list, source(10002, tag = "r")).size)
    }

    @Test
    fun `urlIndex reads NIP-85 service tags, which put the pubkey first`() {
        val list =
            event(
                10040,
                arrayOf("30382:rank", "a".repeat(64), "wss://scores.example"),
                arrayOf("30382:followers", "b".repeat(64), "wss://graph.example"),
            )

        // A named tag picks exactly one service...
        assertEquals(listOf("wss://scores.example/"), urls(list, source(10040, tag = "30382:rank", urlIndex = 2)))
        // ...and no tag name takes the whole <kind>:<type> family.
        assertEquals(
            listOf("wss://scores.example/", "wss://graph.example/"),
            urls(list, source(10040, urlIndex = 2)),
        )
    }

    @Test
    fun `relay hints on e and p tags are just another source`() {
        val note =
            event(
                1,
                arrayOf("e", "f".repeat(64), "wss://hint.example", "root"),
                arrayOf("p", "a".repeat(64), "wss://inbox.example"),
                arrayOf("t", "nostr"),
            )

        assertEquals(listOf("wss://hint.example/"), urls(note, source(1, tag = "e", urlIndex = 2)))
        assertEquals(listOf("wss://inbox.example/"), urls(note, source(1, tag = "p", urlIndex = 2)))
    }

    @Test
    fun `the relay tag family used by every NIP-51 list needs no special casing`() {
        val dmRelays = event(10050, arrayOf("relay", "wss://dm.example"))
        val relaySet = event(30002, arrayOf("relay", "wss://set.example"))
        val monitor = event(30166, arrayOf("d", "wss://monitored.example"))

        assertEquals(listOf("wss://dm.example/"), urls(dmRelays, source(10050, tag = "relay")))
        assertEquals(listOf("wss://set.example/"), urls(relaySet, source(30002, tag = "relay")))
        assertEquals(listOf("wss://monitored.example/"), urls(monitor, source(30166, tag = "d")))
    }

    @Test
    fun `a named tag tolerates junk, repeats, and scheme-less hosts`() {
        val list =
            event(
                10002,
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
            urls(list, source(10002, tag = "r", role = RelayRole.WRITE)),
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

        assertEquals(listOf("wss://scores.example/"), urls(list, source(10040, urlIndex = 2)))
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
            val all = RelayDiscovery.discover(store, dynamic(source(10002, tag = "r", role = RelayRole.WRITE)))
            assertEquals(
                listOf("wss://popular.example/" to 3, "wss://quiet.example/" to 2, "wss://lonely.example/" to 1),
                all.map { it.url.url to it.references },
            )
        }

    @Test
    fun `several sources merge into one fan-out and their counts add up`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(event(10002, arrayOf("r", "wss://shared.example", "write")))
            store.insert(event(10040, arrayOf("30382:rank", "a".repeat(64), "wss://shared.example")))
            store.insert(event(10050, arrayOf("relay", "wss://dm.example")))

            val found =
                RelayDiscovery.discover(
                    store,
                    dynamic(
                        source(10002, tag = "r", role = RelayRole.WRITE),
                        source(10040, tag = "30382:rank", urlIndex = 2),
                        source(10050, tag = "relay"),
                    ),
                )

            // A relay two sources agree on outranks one only a single source names.
            assertEquals(
                listOf("wss://shared.example/" to 2, "wss://dm.example/" to 1),
                found.map { it.url.url to it.references },
            )
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
                    dynamic(source(10002, tag = "r", role = RelayRole.WRITE), exclude = setOf("wss://popular.example")),
                    skip = setOf(RelayUrlNormalizer.normalize("wss://quiet.example")),
                )
            assertEquals(listOf("wss://lonely.example/"), kept.map { it.url.url })
        }

    @Test
    fun `an empty store discovers nothing`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            assertTrue(RelayDiscovery.discover(store, dynamic(source(10002, tag = "r"))).isEmpty())
        }
}
