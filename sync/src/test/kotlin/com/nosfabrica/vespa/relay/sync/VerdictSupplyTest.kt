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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A stream that rosters from verdicts nobody writes says so, instead of syncing nothing in silence. */
class VerdictSupplyTest {
    private val streams =
        RouterConfigLoader
            .parse(
                """
                streams {
                    graded { dir = "down", filter = { "kinds": [1] }, relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
                    gated { dir = "down", filter = { "kinds": [0] }
                        relaySource = [ { filter = { "kinds": [10002] }, select = [ { kind = 10002, tag = "r" } ] } ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
                    lists { dir = "down", filter = { "kinds": [3] }, relaySource = [ { filter = { "kinds": [10002] }, select = [ { kind = 10002, tag = "r" } ] } ] }
                    pinned { dir = "down", filter = { "kinds": [7] }, urls = [ "wss://pinned.example" ] }
                }
                """.trimIndent(),
            ).streams

    @Test
    fun `a verdict-sourced stream with no verdict in the store is named, and stops being named once one lands`(): Unit =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val supply = VerdictSupply(store, streams)

            val starved = supply.starved()
            assertEquals(2, starved.size, "the rostered and the gated stream, never the list-sourced or pinned ones: $starved")
            assertTrue(starved.any { "`graded`" in it } && starved.any { "`gated`" in it }, starved.toString())

            RelayVerdictRecord(store, NostrSignerInternal(KeyPair()))
                .publishFitness(RelayUrlNormalizer.normalize("wss://graded.example"), "prime", "answered", pageable = null, nip77 = null)
            assertEquals(emptyList(), supply.starved())
        }
}
