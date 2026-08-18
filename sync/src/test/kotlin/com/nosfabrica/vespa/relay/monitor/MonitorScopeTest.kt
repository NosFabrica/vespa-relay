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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.shared.RelayVerdictRecord
import com.nosfabrica.vespa.relay.sync.RosterBuilder
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * **WHOSE 30166 VERDICTS BUILD THE ROSTER WHEN THE CONFIG NAMES NOBODY?**
 *
 * Everybody's. An absent `authors` is the unscoped read it is on any other
 * NIP-01 filter, and the router does not substitute its own signer for it.
 *
 * The substitution used to be there (`RosterBuilder`'s `ifEmpty`) and cost
 * more than it bought. It welded the trust anchor to `RELAY_NSEC`: rotating
 * the key re-pointed every verdict source at an identity that had signed
 * nothing, so all of them selected zero records and every roster emptied
 * until the new key finished a sweep — with no warning, because a rotated key
 * IS a monitor identity, just one with no history. And the only deployment
 * where the substitution changed the answer at all was the one that had
 * deliberately mirrored somebody else's 30166s, i.e. the one that meant the
 * union.
 *
 * Unscoped is safe for THIS read because admitting is a positive claim that
 * still has to survive a dial. The hold-out read is not symmetric with it and
 * stays author-bound — see `RelayDiscovery.undialable` and
 * [discovery.StreamWorld].
 */
class MonitorScopeTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val ours = NostrSignerInternal(KeyPair())
    private val stranger = NostrSignerInternal(KeyPair())

    private val ourRelay = RelayUrlNormalizer.normalize("wss://ours.example")
    private val theirRelay = RelayUrlNormalizer.normalize("wss://theirs.example")

    /** Both monitors' verdicts in one store — mirroring a foreign 30166 amounts to exactly this. */
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
            // Hex, because a `filter { }` block is a NIP-01 filter and NIP-01
            // speaks hex. Bech32 stays in the settings that are ours to define.
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
            // The migration the substitution used to break: the store holds the
            // OLD key's verdicts, the process now runs under a new one. Nothing
            // in the read depends on which key this process happens to hold, so
            // the roster is whatever the store can prove — not empty.
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            RelayVerdictRecord(store, ours)
                .publishFitness(ourRelay, "prime", "signed before the rotation", pageable = null, nip77 = null)
            assertEquals(setOf(ourRelay), rosterOf(store).rebuild().asks.keys)
        }

    @Test
    fun `the unscoped read still applies the freshness and epoch rules`() =
        runBlocking {
            // Unscoped widens WHO may admit, and nothing else: a stranger's
            // `dead` verdict is not an admission, and never was an exclusion
            // either — that asymmetry is the dead set's, not this read's.
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
            // Each source's read is cached for its own refreshSeconds, so the
            // second rebuild is served from that cache — and must answer the
            // same thing the read did.
            val builder = rosterOf(storeWithBothMonitors())
            assertEquals(builder.rebuild().asks.keys, builder.rebuild().asks.keys)
        }
}
