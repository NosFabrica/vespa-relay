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

import com.nosfabrica.vespa.eventstore.LensedEventStore
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.TrustChain
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What a reader is told at sign-in, and what they are not. Asserted over the
 * finished check rather than the wire, because silence is one of the answers.
 */
class TrustNoticeTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun newStore() = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = relayUrl)

    private val store = newStore()
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

    /** One of [by]'s score cards. Its `d` is whoever it is about; what ranks is who signed it. */
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
            assertEquals(listOf(TrustNotice.noScores(service.pubKey)), notice.notices(reader.pubKey))
        }

    @Test
    fun `a reader whose provider's cards are here hears nothing at all`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard())
            assertEquals(emptyList(), notice.notices(reader.pubKey), "the failure mode of a status channel is nagging people who are fine")
        }

    /** It is the signer of a card that ranks, not its subject. */
    @Test
    fun `another service's cards are not this reader's scores`() =
        runBlocking {
            store.insert(providerList())
            store.insert(scoreCard(by = stranger, about = reader.pubKey))
            assertEquals(
                listOf(TrustNotice.noScores(service.pubKey)),
                notice.notices(reader.pubKey),
                "a card ABOUT the reader, signed by a service they did not name, ranks nothing for them",
            )
        }

    /**
     * Told apart from having no list because the reader's fix is different, and
     * read off the public tag array the way the store's provider map reads it.
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
                val own = newStore()
                own.insert(providerList(tag = tag))
                assertEquals(
                    listOf(TrustNotice.NO_RANK_SERVICE),
                    TrustNotice(own, CoroutineScope(SupervisorJob())).notices(reader.pubKey),
                    why,
                )
            }
        }

    /** The store's reads rank through the first named rank service, so its cards are the ones that matter. */
    @Test
    fun `of two named rank services the notice follows the one the reads use`() =
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
            // Only the second is mirrored: the reader's ranked page is still empty.
            store.insert(scoreCard(by = second))
            assertEquals(listOf(TrustNotice.noScores(service.pubKey)), notice.notices(reader.pubKey))

            store.insert(scoreCard(by = service))
            assertEquals(emptyList(), notice.notices(reader.pubKey))
        }

    @Test
    fun `somebody else's list is not yours`() =
        runBlocking {
            store.insert(providerList(by = stranger))
            store.insert(scoreCard())
            assertEquals(listOf(TrustNotice.NO_PROVIDER), notice.notices(reader.pubKey))
        }

    /** A notice sends a reader off to republish their list; an unreachable store has not claimed it is missing. */
    @Test
    fun `an unanswerable check says nothing rather than guessing`() =
        runBlocking {
            val blind =
                object : LensedEventStore by store {
                    override suspend fun lensOf(pubkey: String): TrustChain = throw IllegalStateException("vespa unreachable")
                }
            assertEquals(emptyList(), TrustNotice(blind, CoroutineScope(SupervisorJob())).notices(reader.pubKey))
        }

    /** Every fresh key that signs in starts a store read; a burst of them may not start a burst of reads. */
    @Test
    fun `a burst of sign-ins runs a bounded number of checks`() =
        runBlocking {
            val stalled = Stalled(store)
            val bounded = TrustNotice(stalled, CoroutineScope(SupervisorJob()), maxInFlight = 2)
            val told = Collections.synchronizedList(mutableListOf<String>())
            val keys = List(4) { NostrSignerSync().pubKey }

            fun signIn(key: String) = bounded.check(key) { told += key }

            keys.take(3).forEach(::signIn)
            withTimeout(5_000) { while (stalled.entered.get() < 2) delay(10) }
            stalled.open.complete(Unit)
            withTimeout(5_000) { while (told.size < 2) delay(10) }

            // The finished checks hand their places back.
            signIn(keys[3])
            withTimeout(5_000) { while (keys[3] !in told) delay(10) }
            assertEquals(setOf(keys[0], keys[1], keys[3]), told.toSet(), "the third sign-in arrived with both places taken, and was not queued")
        }

    @Test
    fun `every notice names the kind it is about, and stops`() {
        val all = listOf(TrustNotice.NO_PROVIDER, TrustNotice.NO_RANK_SERVICE, TrustNotice.noScores(service.pubKey))
        assertTrue(all.any { "10040" in it } && all.any { "30382" in it }, "the reader who can act on this reads kinds, not prose")
        assertTrue(all.all { it.length <= 120 }, "a NOTICE is a line in somebody's console: $all")
        assertTrue(
            all.all { MachineReadablePrefix.parse(it) == MachineReadablePrefix.RESTRICTED },
            "NIP-01's single-word prefix so a client can react to it, not a word of ours: $all",
        )
    }

    /** A store whose trust-chain reads wait for [open]; [entered] counts the reads that started. */
    private class Stalled(
        private val inner: LensedEventStore,
    ) : LensedEventStore by inner {
        val open = CompletableDeferred<Unit>()
        val entered = AtomicInteger()

        override suspend fun lensOf(pubkey: String): TrustChain {
            entered.incrementAndGet()
            open.await()
            return inner.lensOf(pubkey)
        }
    }
}
