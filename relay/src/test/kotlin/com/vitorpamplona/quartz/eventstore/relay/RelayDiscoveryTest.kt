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
 * The dynamic relay list: what [RelayDiscovery] reads out of the 10002s and
 * 10040s a store already holds, and how it ranks them.
 */
class RelayDiscoveryTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun source(
        kind: RelayListKind,
        role: RelayRole = RelayRole.WRITE,
        exclude: Set<String> = emptySet(),
    ) = RelaySource(
        kind = kind,
        role = role,
        refreshSeconds = 3_600,
        concurrency = 4,
        exclude = exclude.map { RelayUrlNormalizer.normalize(it) }.toSet(),
    )

    /** A kind-10002 relay list. Each tag is `r, url, [marker]`. */
    private fun outboxList(vararg relays: Pair<String, String?>): Event =
        NostrSignerSync().sign(
            1_700_000_000L,
            10002,
            relays
                .map { (url, marker) -> if (marker == null) arrayOf("r", url) else arrayOf("r", url, marker) }
                .toTypedArray(),
            "",
        )

    /** A kind-10040 trusted-assertion list: `<kind>:<type>, pubkey, url`. */
    private fun trustList(vararg providers: Pair<String, String>): Event =
        NostrSignerSync().sign(
            1_700_000_000L,
            10040,
            providers
                .map { (type, url) -> arrayOf(type, "a".repeat(64), url) }
                .toTypedArray(),
            "",
        )

    private fun urls(
        event: Event,
        source: RelaySource,
    ) = RelayDiscovery.urlsIn(event, source).map { it.url }

    @Test
    fun `outbox keeps write and unmarked relays and drops read-only ones`() {
        val list =
            outboxList(
                "wss://write.example" to "write",
                "wss://read.example" to "read",
                "wss://both.example" to null,
            )

        val write = urls(list, source(RelayListKind.OUTBOX, RelayRole.WRITE))
        assertTrue(write.any { it.contains("write.example") })
        assertTrue(write.any { it.contains("both.example") }, "an unmarked r tag is read AND write")
        assertFalse(write.any { it.contains("read.example") })

        val read = urls(list, source(RelayListKind.OUTBOX, RelayRole.READ))
        assertTrue(read.any { it.contains("read.example") })
        assertTrue(read.any { it.contains("both.example") })
        assertFalse(read.any { it.contains("write.example") })

        assertEquals(3, urls(list, source(RelayListKind.OUTBOX, RelayRole.ANY)).size)
    }

    @Test
    fun `outbox tolerates upper-case markers, junk tags, and repeats`() {
        val list =
            NostrSignerSync().sign<Event>(
                1_700_000_000L,
                10002,
                arrayOf(
                    arrayOf("r", "wss://write.example", "WRITE"),
                    arrayOf("r", "wss://write.example", "write"),
                    arrayOf("r"),
                    arrayOf("p", "wss://not-a-relay-tag.example"),
                    // The normalizer would turn this into `wss://not/`; we don't let it.
                    arrayOf("r", "not a url at all"),
                    arrayOf("r", "   "),
                ),
                "",
            )

        // The repeat collapses; the malformed and non-`r` tags are ignored.
        assertEquals(listOf("wss://write.example/"), urls(list, source(RelayListKind.OUTBOX)))
    }

    @Test
    fun `trust providers read the relay out of NIP-85 service tags`() {
        val list =
            NostrSignerSync().sign<Event>(
                1_700_000_000L,
                10040,
                arrayOf(
                    arrayOf("30382:rank", "a".repeat(64), "wss://scores.example"),
                    arrayOf("30382:followers", "b".repeat(64), "wss://graph.example"),
                    // Not a `kind:type` tag, and a service tag with no relay.
                    arrayOf("alt", "a trusted assertion list"),
                    arrayOf("30382:rank", "c".repeat(64)),
                ),
                "",
            )

        assertEquals(
            listOf("wss://scores.example/", "wss://graph.example/"),
            urls(list, source(RelayListKind.TRUST_PROVIDERS, RelayRole.ANY)),
        )
    }

    @Test
    fun `discovery keeps every relay, ordered by how many lists name it`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            // popular: 3 lists, quiet: 2, lonely: 1.
            store.insert(outboxList("wss://popular.example" to "write", "wss://quiet.example" to "write"))
            store.insert(outboxList("wss://popular.example" to "write", "wss://quiet.example" to null))
            store.insert(outboxList("wss://popular.example" to "write", "wss://lonely.example" to "write"))

            // Nothing is dropped for being unpopular — the one-list relay is synced
            // like the rest; the count only decides who goes first.
            val all = RelayDiscovery.discover(store, source(RelayListKind.OUTBOX))
            assertEquals(
                listOf("wss://popular.example/" to 3, "wss://quiet.example/" to 2, "wss://lonely.example/" to 1),
                all.map { it.url.url to it.references },
            )
        }

    @Test
    fun `exclude and the caller's skip set are the only things dropped`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(
                outboxList(
                    "wss://popular.example" to "write",
                    "wss://quiet.example" to "write",
                    "wss://lonely.example" to "write",
                ),
            )

            val kept =
                RelayDiscovery.discover(
                    store,
                    source(RelayListKind.OUTBOX, exclude = setOf("wss://popular.example")),
                    skip = setOf(RelayUrlNormalizer.normalize("wss://quiet.example")),
                )
            assertEquals(listOf("wss://lonely.example/"), kept.map { it.url.url })
        }

    @Test
    fun `discovery reads 10040 lists out of the same store`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            store.insert(trustList("30382:rank" to "wss://scores.example"))
            store.insert(trustList("30382:rank" to "wss://scores.example"))
            // A 10002 in the same store must not leak into the 10040 source.
            store.insert(outboxList("wss://outbox.example" to "write"))

            val found = RelayDiscovery.discover(store, source(RelayListKind.TRUST_PROVIDERS, RelayRole.ANY))
            assertEquals(listOf("wss://scores.example/" to 2), found.map { it.url.url to it.references })
        }

    @Test
    fun `an empty store discovers nothing`() =
        runBlocking {
            val store = NostrEventStore(InMemoryEventIndex(), relay = relayUrl)
            assertTrue(RelayDiscovery.discover(store, source(RelayListKind.OUTBOX)).isEmpty())
        }
}
