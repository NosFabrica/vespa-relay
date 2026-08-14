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
 * the two reads to completion and looks at the list.
 */
class TrustNoticeTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val notice = TrustNotice(store, CoroutineScope(SupervisorJob()))

    private val reader = NostrSignerSync()
    private val service = NostrSignerSync()
    private val stranger = NostrSignerSync()

    /** The reader's NIP-85 list, naming [service] as the one whose scores rank for them. */
    private suspend fun providerList(by: NostrSignerSync = reader): Event = by.sign(1_700_000_000L, 10040, arrayOf(arrayOf("30382:rank", service.pubKey, relayUrl.url)), "")

    /** A score card ABOUT [about] — NIP-85 puts the scored pubkey in `d`. */
    private suspend fun scoreCard(about: String): Event = service.sign(1_700_000_000L, 30382, arrayOf(arrayOf("d", about), arrayOf("rank", "42")), "")

    @Test
    fun `a reader this relay knows nothing about hears about both links`() =
        runBlocking {
            assertEquals(
                listOf(TrustNotice.NO_PROVIDER, TrustNotice.NO_SCORES),
                notice.notices(reader.pubKey),
                "both missing, in chain order: whose scores rank for you, then whether anyone has scored you",
            )
        }

    @Test
    fun `a reader with a provider list is only told about the scores`() =
        runBlocking {
            store.insert(providerList())
            assertEquals(listOf(TrustNotice.NO_SCORES), notice.notices(reader.pubKey))
        }

    @Test
    fun `a reader who has been scored is only told about the provider list`() =
        runBlocking {
            store.insert(scoreCard(about = reader.pubKey))
            assertEquals(listOf(TrustNotice.NO_PROVIDER), notice.notices(reader.pubKey))
        }

    @Test
    fun `a reader with both hears nothing at all`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard(about = reader.pubKey))
            assertEquals(emptyList(), notice.notices(reader.pubKey), "the failure mode of a status channel is nagging people who are fine")
        }

    /**
     * The assertion that makes the score check a `d` ask rather than an author
     * ask. A relay mirroring a provider holds millions of its cards, so a
     * filter keyed on the card's SIGNER answers "yes" for every reader alive
     * and the notice silently stops being about them.
     */
    @Test
    fun `somebody else's card does not count as yours, however many of them there are`() =
        runBlocking {
            store.insert(scoreCard(about = stranger.pubKey))
            store.insert(providerList(by = stranger))
            assertEquals(
                listOf(TrustNotice.NO_PROVIDER, TrustNotice.NO_SCORES),
                notice.notices(reader.pubKey),
                "a store full of a provider's cards about other people still holds nothing about this reader",
            )
        }

    /**
     * A store that cannot answer is not a store that answered "no". `false`
     * here tells a reader their own list is missing and sends them off to
     * republish it; only the store may make that claim, and a Vespa that is
     * briefly unreachable has made none.
     */
    @Test
    fun `an unanswerable check says nothing rather than guessing`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard(about = reader.pubKey))
            val blind = TrustNotice(FailingKind(store, kind = 10040), CoroutineScope(SupervisorJob()))
            assertEquals(emptyList(), blind.notices(reader.pubKey), "the 10040 read threw; nothing is claimed about it")

            // …and the OTHER check still answers. The two are independent asks
            // and one failing must not take the working one down with it.
            val emptyStore = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val half = TrustNotice(FailingKind(emptyStore, kind = 10040), CoroutineScope(SupervisorJob()))
            assertEquals(listOf(TrustNotice.NO_SCORES), half.notices(reader.pubKey))
        }

    @Test
    fun `the two filters ask exactly one question each`() {
        val provider = TrustNotice.providerListFilter(reader.pubKey)
        assertEquals(listOf(10040), provider.kinds)
        assertEquals(listOf(reader.pubKey), provider.authors, "their OWN list — a 10040 someone else signed says nothing about them")
        assertEquals(1, provider.limit, "existence, not a count")

        val scores = TrustNotice.scoreCardFilter(reader.pubKey)
        assertEquals(listOf(30382), scores.kinds)
        assertEquals(mapOf("d" to listOf(reader.pubKey)), scores.tags, "cards ABOUT them; the author is whichever service signed it")
        assertEquals(null, scores.authors)
        assertEquals(1, scores.limit)
    }

    @Test
    fun `both notices name the kind they are about`() {
        assertTrue("10040" in TrustNotice.NO_PROVIDER, "the reader who can act on this reads kinds, not prose")
        assertTrue("30382" in TrustNotice.NO_SCORES)
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
