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
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * An absent `authors` on a verdict source is the unscoped read it is on any
 * NIP-01 filter; the router does not substitute its own signer.
 */
class MonitorScopeTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val ours = NostrSignerInternal(KeyPair())
    private val stranger = NostrSignerInternal(KeyPair())

    private val ourRelay = RelayUrlNormalizer.normalize("wss://ours.example")
    private val theirRelay = RelayUrlNormalizer.normalize("wss://theirs.example")

    /** Both monitors' verdicts in one store, which is what mirroring a foreign 30166 amounts to. */
    private suspend fun storeWithBothMonitors(): NostrSemanticsStore {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        RelayVerdictRecord(store, ours)
            .publishFitness(ourRelay, "prime", "answered at a settled anchor", pageable = null, nip77 = null)
        RelayVerdictRecord(store, stranger)
            .publishFitness(theirRelay, "prime", "somebody else measured this one", pageable = null, nip77 = null)
        return store
    }

    private fun rosterOf(
        store: NostrSemanticsStore,
        authors: String = "",
    ) = RosterBuilder(
        store = store,
        streams =
            RouterConfigLoader
                .parse(
                    """
                    streams {
                        a { dir = "down", filter = { "kinds": [1] }
                            relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"]$authors } } ] }
                    }
                    """.trimIndent(),
                ).streams,
        bands = SyncBands(null),
    )

    @Test
    fun `a source naming no authors admits every monitor in the store`() =
        runBlocking {
            val roster = rosterOf(storeWithBothMonitors()).rebuild()
            assertEquals(
                setOf(ourRelay, theirRelay),
                roster.asks.keys,
                "an absent `authors` is unscoped, not a silent narrowing to our own signer",
            )
        }

    @Test
    fun `naming authors keeps every other monitor out`() =
        runBlocking {
            // Hex: a `filter { }` block is a NIP-01 filter, and NIP-01 speaks hex.
            val roster = rosterOf(storeWithBothMonitors(), authors = ""","authors": ["${ours.pubKey}"]""").rebuild()
            assertEquals(
                setOf(ourRelay),
                roster.asks.keys,
                "the scoped read is still the trust boundary it always was",
            )
        }

    @Test
    fun `a rotated signer does not empty an unscoped roster`() =
        runBlocking {
            // The store holds the old key's verdicts; the process runs under a new one.
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            RelayVerdictRecord(store, ours)
                .publishFitness(ourRelay, "prime", "signed before the rotation", pageable = null, nip77 = null)
            assertEquals(setOf(ourRelay), rosterOf(store).rebuild().asks.keys)
        }

    @Test
    fun `the unscoped read still applies the freshness and epoch rules`() =
        runBlocking {
            // Unscoped widens who may admit and nothing else; a stranger's `dead` is not an admission.
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            RelayVerdictRecord(store, stranger)
                .publishFitness(theirRelay, "dead", "no TCP answer at the pre-probe", pageable = null, nip77 = null)
            RelayVerdictRecord(store, ours)
                .publishFitness(ourRelay, "prime", "answers and pages", pageable = null, nip77 = null)
            assertEquals(setOf(ourRelay), rosterOf(store).rebuild().asks.keys)
        }

    @Test
    fun `the roster is unchanged by a second rebuild`() =
        runBlocking {
            // The second rebuild is served from each source's cache and must answer the same.
            val builder = rosterOf(storeWithBothMonitors())
            assertEquals(builder.rebuild().asks.keys, builder.rebuild().asks.keys)
        }
}
