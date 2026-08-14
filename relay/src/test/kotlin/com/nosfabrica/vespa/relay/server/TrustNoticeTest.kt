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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What a reader is told when they sign in, and — the half that is easy to lose
 * — what they are NOT told.
 *
 * The decision is asserted here rather than over the wire because silence is
 * one of its answers: `RelayProtocolTest` can prove a NOTICE arrived, but
 * "nothing was said" is only ever a wait that has not finished yet. This runs
 * the walk to completion and looks at the list.
 */
class TrustNoticeTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val notice = TrustNotice(store, CoroutineScope(SupervisorJob()))

    private val reader = NostrSignerSync()
    private val service = NostrSignerSync()
    private val stranger = NostrSignerSync()

    /** The reader's NIP-85 list, naming [service] for the dimension that ranks. */
    private suspend fun providerList(
        by: NostrSignerSync = reader,
        tag: Array<String> = arrayOf("30382:rank", service.pubKey, relayUrl.url),
    ): Event = providerList(by, listOf(tag))

    private suspend fun providerList(
        by: NostrSignerSync,
        tags: List<Array<String>>,
    ): Event = by.sign(1_700_000_000L, 10040, tags.toTypedArray(), "")

    /** One of [by]'s score cards. Its `d` is whoever it is about; what ranks is who SIGNED it. */
    private suspend fun scoreCard(
        by: NostrSignerSync = service,
        about: String = stranger.pubKey,
    ): Event = by.sign(1_700_000_000L, 30382, arrayOf(arrayOf("d", about), arrayOf("rank", "42")), "")

    @Test
    fun `a reader this relay knows nothing about is told about the list`() =
        runBlocking {
            assertEquals(listOf(TrustNotice.NO_PROVIDER), notice.notices(reader.pubKey))
        }

    @Test
    fun `a reader whose provider has not been mirrored is told which one`() =
        runBlocking {
            store.insert(providerList())
            assertEquals(listOf(TrustNotice.noScores(listOf(service.pubKey))), notice.notices(reader.pubKey))
        }

    @Test
    fun `a reader whose provider's cards are here hears nothing at all`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard())
            assertEquals(emptyList(), notice.notices(reader.pubKey), "the failure mode of a status channel is nagging people who are fine")
        }

    /**
     * The assertion the whole chain rests on: it is the SIGNER of a card that
     * ranks, not its subject. A relay holding a million cards from services
     * this reader never named can rank nothing for them, and a filter keyed on
     * anything but the `30382:rank` service would call that ready.
     */
    @Test
    fun `another service's cards are not this reader's scores`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard(by = stranger, about = reader.pubKey))
            assertEquals(
                listOf(TrustNotice.noScores(listOf(service.pubKey))),
                notice.notices(reader.pubKey),
                "a card ABOUT the reader, signed by a service they did not name, ranks nothing for them",
            )
        }

    /**
     * Three ways a stored 10040 still names nothing this relay can rank
     * through, told apart from having no list at all because the reader's fix
     * is different — and read the same way the store's own provider map reads
     * it, off the public tag array.
     */
    @Test
    fun `a list that names no usable rank service is its own answer`() =
        runBlocking {
            val cases =
                mapOf(
                    "orders a list, ranks nothing" to arrayOf("30382:followers", service.pubKey, relayUrl.url),
                    "no relay hint, so quartz drops the entry" to arrayOf("30382:rank", service.pubKey),
                    "private (NIP-44), unreadable here" to arrayOf("p", service.pubKey),
                )
            for ((why, tag) in cases) {
                val own = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
                own.insert(providerList(tag = tag))
                assertEquals(
                    listOf(TrustNotice.NO_RANK_SERVICE),
                    TrustNotice(own, CoroutineScope(SupervisorJob())).notices(reader.pubKey),
                    why,
                )
            }
        }

    /**
     * A 10040 may name more than one rank service, and the store's provider
     * map credits the reader through EVERY one of them — so either service's
     * cards being here means ranking works. Reading only the first told a
     * reader whose second provider is fully mirrored that their scores were
     * missing, on every login.
     */
    @Test
    fun `either of two named rank services is enough`() =
        runBlocking {
            val second = NostrSignerSync()
            store.insert(
                providerList(
                    by = reader,
                    tags =
                        listOf(
                            arrayOf("30382:rank", service.pubKey, relayUrl.url),
                            arrayOf("30382:rank", second.pubKey, relayUrl.url),
                        ),
                ),
            )
            assertEquals(
                listOf(TrustNotice.noScores(listOf(service.pubKey, second.pubKey))),
                notice.notices(reader.pubKey),
                "neither one's cards are here, and the notice names both",
            )

            // Only the SECOND is mirrored, which is the case a first-tag-only
            // read reported as missing forever.
            store.insert(scoreCard(by = second))
            assertEquals(emptyList(), notice.notices(reader.pubKey))
        }

    @Test
    fun `somebody else's list is not yours`() =
        runBlocking {
            store.insert(providerList(by = stranger))
            store.insert(scoreCard())
            assertEquals(listOf(TrustNotice.NO_PROVIDER), notice.notices(reader.pubKey))
        }

    /**
     * A store that cannot answer is not a store that answered "no". A notice
     * here tells a reader their own list is missing and sends them off to
     * republish it; only the store may make that claim, and a Vespa that is
     * briefly unreachable has made none.
     */
    @Test
    fun `an unanswerable check says nothing rather than guessing`() =
        runBlocking {
            store.insert(providerList())
            val blindToLists = TrustNotice(FailingKind(store, kind = 10040), CoroutineScope(SupervisorJob()))
            assertEquals(emptyList(), blindToLists.notices(reader.pubKey), "the 10040 read threw; nothing is claimed about it")

            // The second leg the same way: the list was read, the cards were
            // not, and "your provider has not been mirrored" is unsupported.
            val blindToCards = TrustNotice(FailingKind(store, kind = 30382), CoroutineScope(SupervisorJob()))
            assertEquals(emptyList(), blindToCards.notices(reader.pubKey))
        }

    @Test
    fun `each ask is addressed to exactly one key`() {
        val provider = TrustNotice.providerListFilter(reader.pubKey)
        assertEquals(listOf(10040), provider.kinds)
        assertEquals(listOf(reader.pubKey), provider.authors, "their OWN list — a 10040 someone else signed says nothing about them")
        assertEquals(1, provider.limit, "the current version; nothing here reads history")

        val scores = TrustNotice.scoreCardFilter(listOf(service.pubKey))
        assertEquals(listOf(30382), scores.kinds)
        assertEquals(listOf(service.pubKey), scores.authors, "the services the 10040 named, not the reader")
        assertEquals(null, scores.tags, "not narrowed to cards about the reader: it is the signer that ranks")
        assertEquals(1, scores.limit)
    }

    @Test
    fun `every notice names the kind it is about, and stops`() {
        val all = listOf(TrustNotice.NO_PROVIDER, TrustNotice.NO_RANK_SERVICE, TrustNotice.noScores(listOf(service.pubKey)))
        assertTrue(all.any { "10040" in it } && all.any { "30382" in it }, "the reader who can act on this reads kinds, not prose")
        assertTrue(all.all { it.length <= 120 }, "a NOTICE is a line in somebody's console: $all")
        assertTrue(
            all.all { MachineReadablePrefix.parse(it) == MachineReadablePrefix.RESTRICTED },
            "NIP-01's single-word prefix so a client can react to it, not a word of ours: $all",
        )
    }

    /** A store whose reads for one kind throw; everything else is the real thing. */
    private class FailingKind(
        private val inner: IEventStore,
        private val kind: Int,
    ) : IEventStore by inner {
        override suspend fun <T : Event> query(filter: Filter): List<T> {
            if (filter.kinds?.contains(kind) == true) throw IllegalStateException("vespa unreachable")
            return inner.query(filter)
        }
    }
}
