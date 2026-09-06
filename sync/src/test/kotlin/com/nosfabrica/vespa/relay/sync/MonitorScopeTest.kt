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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** An absent `authors` on a verdict source is an unscoped read; the router never substitutes its own signer. */
class MonitorScopeTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val ours = NostrSignerInternal(KeyPair())
    private val stranger = NostrSignerInternal(KeyPair())

    private val ourRelay = RelayUrlNormalizer.normalize("wss://ours.example")
    private val theirRelay = RelayUrlNormalizer.normalize("wss://theirs.example")

    /** Both monitors' verdicts in one store, as after mirroring a foreign 30166. */
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
        watching: Boolean = false,
        verdicts: RelayVerdictRecord? = null,
    ) = RosterBuilder(
        store = store,
        watching = watching,
        verdicts = { urls -> verdicts?.load(urls) ?: RelayVerdictRecord.Verdicts() },
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
    fun `a relay on the roster that our own monitor has not graded is named`() =
        runBlocking {
            // Both relays are on the roster; only one carries a verdict of ours. The other is
            // exactly the drift `unwatched` exists for: mirrored by us, graded by nobody here.
            val store = storeWithBothMonitors()
            val roster = rosterOf(store, watching = true, verdicts = RelayVerdictRecord(store, ours)).rebuild()

            assertEquals(setOf(ourRelay, theirRelay), roster.asks.keys, "the fixture has to hold both for this to mean anything")
            assertEquals(setOf(ourRelay), roster.measured, "a stranger's verdict is not ours, and grades nothing for us")
            assertTrue(roster.watches(ourRelay))
            assertFalse(roster.watches(theirRelay))
        }

    @Test
    fun `a router that measures nothing reports no relay as unwatched`() =
        runBlocking {
            // Not the same absence: nothing here was ever going to grade these, so there is no
            // drift to report, and a mark on every row would be noise rather than a finding.
            val store = storeWithBothMonitors()
            val roster = rosterOf(store).rebuild()

            assertEquals(emptySet(), roster.measured)
            assertTrue(roster.watches(ourRelay))
            assertTrue(roster.watches(theirRelay), "a deployment with no monitor is not one whose every relay is ungraded")
        }

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
            // Hex, because a `filter { }` block is a NIP-01 filter.
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
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            RelayVerdictRecord(store, ours)
                .publishFitness(ourRelay, "prime", "signed before the rotation", pageable = null, nip77 = null)
            assertEquals(setOf(ourRelay), rosterOf(store).rebuild().asks.keys)
        }

    @Test
    fun `the unscoped read still applies the freshness and epoch rules`() =
        runBlocking {
            // Unscoped widens who may admit and nothing else.
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
            // The second rebuild is served from each source's cache.
            val builder = rosterOf(storeWithBothMonitors())
            assertEquals(builder.rebuild().asks.keys, builder.rebuild().asks.keys)
        }
}
